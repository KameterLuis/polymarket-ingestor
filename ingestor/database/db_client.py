# db_bulk.py
import os, io, csv, psycopg2
import json
from psycopg2 import sql

# Read these from your .env (same names you already use)
DB_USER = os.getenv("user")
DB_PASS = os.getenv("password")
DB_HOST = os.getenv("host")
DB_PORT = int(os.getenv("port", "5432"))
DB_NAME = os.getenv("dbname", "postgres")

def _connect():
    return psycopg2.connect(
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        sslmode="require",
    )

def _pg_array_literal(seq):
    """Convert a Python list/tuple into a Postgres array literal."""
    def enc(el):
        if el is None:
            return 'NULL'  # unquoted NULL inside arrays
        if isinstance(el, (int, float)):
            return str(el)
        if isinstance(el, bool):
            return 'TRUE' if el else 'FALSE'
        s = str(el).replace('\\', '\\\\').replace('"', '\\"')
        return f'"{s}"'
    return '{' + ','.join(enc(x) for x in seq) + '}'

def _cell_for_copy(col, val, json_cols):
    """
    Convert Python value to a CSV cell for COPY, with column-aware encoding.
    - None -> empty field (NULL)
    - if col in json_cols -> JSON text (lists/dicts become JSON arrays/objects)
    - list/tuple -> Postgres array literal
    - dict -> JSON text
    - others -> as-is (csv will quote as needed)
    """
    if val is None:
        return ""
    if col in json_cols:
        # if you *already* have a JSON string, pass it through
        if isinstance(val, (dict, list)):
            return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
        return val
    if isinstance(val, (list, tuple)):
        return _pg_array_literal(val)
    if isinstance(val, dict):
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    return val

def _rows_to_csv_buffer(rows, cols, json_cols=frozenset()):
    """
    rows: list[dict] or list[tuple]
    cols: sequence[str] in target order
    json_cols: set[str] of columns that are json/jsonb in the table
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    if rows:
        if isinstance(rows[0], dict):
            for r in rows:
                w.writerow([_cell_for_copy(c, r.get(c), json_cols) for c in cols])
        else:
            for r in rows:
                w.writerow([_cell_for_copy(c, v, json_cols) for c, v in zip(cols, r)])
    buf.seek(0)
    return buf

def replace_table_atomic(schema, table, cols, rows, lock_timeout_ms=2000, json_cols=frozenset()):
    stg = f"_stg_{table}"
    col_ident_list = sql.SQL(", ").join(map(sql.Identifier, cols))
    with _connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = %s", (f"{lock_timeout_ms}ms",))
            cur.execute("SET LOCAL statement_timeout = %s", ("5min",))
            cur.execute("SET LOCAL synchronous_commit = OFF")

            cur.execute(sql.SQL(
                "CREATE TEMP TABLE {stg} (LIKE {tbl} INCLUDING DEFAULTS) ON COMMIT DROP"
            ).format(stg=sql.Identifier(stg), tbl=sql.Identifier(schema, table)))

            copy_stmt = sql.SQL(
                "COPY {stg} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
            ).format(stg=sql.Identifier(stg), cols=col_ident_list)

            buf = _rows_to_csv_buffer(rows, cols, json_cols=json_cols)
            cur.copy_expert(copy_stmt.as_string(cur), buf)

            cur.execute(sql.SQL("TRUNCATE {tbl}").format(tbl=sql.Identifier(schema, table)))
            cur.execute(sql.SQL(
                "INSERT INTO {tbl} ({cols}) SELECT {cols} FROM {stg}"
            ).format(tbl=sql.Identifier(schema, table), cols=col_ident_list, stg=sql.Identifier(stg)))


def copy_upsert_timeseries(schema, table, cols, key_cols, rows, lock_timeout_ms=0):
    """
    Fast upsert for time-series tables via COPY->staging->INSERT ON CONFLICT.
    Set lock_timeout_ms>0 if you want to avoid waiting on locks (optional).
    """
    if not rows:
        return
    stg = f"_stg_{table}"
    col_ident_list = sql.SQL(", ").join(map(sql.Identifier, cols))
    key_ident_list = sql.SQL(", ").join(map(sql.Identifier, key_cols))
    set_updates = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
        for c in cols if c not in key_cols
    )
    with _connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            if lock_timeout_ms:
                cur.execute("SET LOCAL lock_timeout = %s", (f"{lock_timeout_ms}ms",))
            cur.execute("SET LOCAL statement_timeout = %s", ("5min",))
            cur.execute("SET LOCAL synchronous_commit = OFF")

            cur.execute(sql.SQL(
                "CREATE TEMP TABLE {stg} (LIKE {tbl} INCLUDING DEFAULTS) ON COMMIT DROP"
            ).format(stg=sql.Identifier(stg),
                     tbl=sql.Identifier(schema, table)))

            copy_stmt = sql.SQL(
                "COPY {stg} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
            ).format(stg=sql.Identifier(stg), cols=col_ident_list)

            buf = _rows_to_csv_buffer(rows, cols)
            cur.copy_expert(copy_stmt.as_string(cur), buf)

            cur.execute(sql.SQL("""
                INSERT INTO {tbl} ({cols})
                SELECT {cols} FROM {stg}
                ON CONFLICT ({key_cols}) DO UPDATE SET {updates}
            """).format(
                tbl=sql.Identifier(schema, table),
                cols=col_ident_list,
                stg=sql.Identifier(stg),
                key_cols=key_ident_list,
                updates=set_updates
            ))
    # done
