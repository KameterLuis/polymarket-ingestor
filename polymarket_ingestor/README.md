- Put env vars in /etc/default/polymarket-ingestor
  DATABASE_URL=postgresql://user:pass@host:5432/db
  GAMMA_BASE=https://gamma-api.polymarket.com
  S3_BUCKET=your-bucket
  S3_REGION=eu-central-1
  S3_PREFIX=polybloom/

- Create venv + install deps
  python3 -m venv /opt/polymarket-ingestor-venv
  source /opt/polymarket-ingestor-venv/bin/activate
  pip install asyncpg httpx aiolimiter boto3 pyarrow

- Initialize schema
  psql "$DATABASE_URL" -f sql/schema.sql

- Enable service
  sudo systemctl daemon-reload
  sudo systemctl enable --now polymarket-ingestor

Notes

- Minute table holds only last 60 minutes; hourly last 7 days; daily indefinitely (adjust as desired).
- For archival to S3, wire a small cronjob or extend the loop to select rows older than retention, write Parquet, then DELETE.
- Consider TimescaleDB later for automatic retention + continuous aggs.
