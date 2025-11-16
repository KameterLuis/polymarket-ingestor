from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Dict, Iterable

try:
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    boto3 = None
    pa = None
    pq = None


UTC = timezone.utc


async def archive_and_prune_minutes(
    sb,
    *,
    table: str,
    older_than: datetime,
    bucket: str | None,
    region: str | None,
    prefix: str,
):
    """Archive rows older than `older_than` to AWS S3 (Parquet) then delete them from Supabase.
    Works in batches to keep memory low.
    """
    if not (boto3 and pa and pq and bucket and region):
        return

    # Page through selects, upload each page, then delete all lt older_than once.
    from_idx = 0
    page_size = 50_000
    all_rows: list[Dict] = []
    start_iso = older_than.replace(tzinfo=UTC).isoformat()

    while True:
        to_idx = from_idx + page_size - 1
        res = (
            sb.table(table)
            .select("market_id,ts,price,volume")
            .lt("ts", start_iso)
            .order("ts", desc=False)
            .range(from_idx, to_idx)
            .execute()
        )
        items = getattr(res, "data", []) or []
        all_rows.extend(items)
        if len(items) < page_size:
            break
        from_idx += page_size

    if not all_rows:
        return

    # Write one partition per earliest day for simplicity
    first_ts = _parse(all_rows[0]["ts"]).astimezone(UTC)
    key = f"{prefix}{table}/year={first_ts:%Y}/month={first_ts:%m}/day={first_ts:%d}/{table}-{first_ts.isoformat()}.parquet"
    local = f"/tmp/{os.path.basename(key)}"

    table_pa = pa.Table.from_pylist(all_rows)
    pq.write_table(table_pa, local, compression="zstd")

    s3 = boto3.client("s3", region_name=region)
    s3.upload_file(local, bucket, key)
    os.remove(local)

    # Prune all rows older than threshold in one go
    sb.table(table).delete().lt("ts", start_iso).execute()


def _parse(x: str) -> datetime:
    return datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(UTC)
