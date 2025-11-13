import os
import io
import csv
import json
import psycopg
from psycopg import sql

# --- Connection ---

DB_USER = os.getenv("user")
DB_PASS = os.getenv("password")
DB_HOST = os.getenv("host")
DB_PORT = int(os.getenv("port", "5432"))
DB_NAME = os.getenv("dbname", "postgres")

DB_CONN_STRING = f"user={DB_USER} password={DB_PASS} host={DB_HOST} port={DB_PORT} dbname={DB_NAME} sslmode=require"

async def get_async_connection():
    """
    Creates and returns an async database connection.
    """
    try:
        conn = await psycopg.AsyncConnection.connect(DB_CONN_STRING)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

# --- CSV/COPY Helper Functions (from your original code) ---

def _pg_array_literal(seq):
    """Convert a Python list/tuple into a Postgres array literal."""
    def enc(el):
        if el is None:
            return 'NULL'
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
    """
    if val is None:
        return ""
    if col in json_cols:
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
    Converts list[dict] to a CSV buffer for COPY.
    Note: psycopg v3 COPY needs a bytes buffer, so we encode to UTF-8.
    """
    buf = io.StringIO() # Write to string buffer
    w = csv.writer(buf)
    w.writerow(cols) # Write header
    
    if rows:
        # This handles your list[dict] payload directly
        for r in rows:
            w.writerow([_cell_for_copy(c, r.get(c), json_cols) for c in cols])
    
    buf.seek(0)
    # Return as a bytes buffer, which async copy needs
    return io.BytesIO(buf.read().encode('utf-8'))

# --- Main Async DB Functions (Re-implemented) ---

async def replace_rows(conn, schema, table, cols, rows, json_cols=frozenset()):
    """
    Fast, async, pooler-friendly table replacement using TRUNCATE + COPY.
    Rows should be a list of DICTIONARIES.
    """
    stg = f"_stg_{table}" # Temporary table name
    col_ident_list = sql.SQL(", ").join(map(sql.Identifier, cols))

    await conn.set_autocommit(False)
    try:
        async with conn.cursor() as cur:
            # 1. Create a temporary staging table (transaction-local)
            await cur.execute(sql.SQL(
                "CREATE TEMP TABLE {stg} (LIKE {tbl} INCLUDING DEFAULTS) ON COMMIT DROP"
            ).format(stg=sql.Identifier(stg), tbl=sql.Identifier(schema, table)))

            # 2. Prep the COPY statement for the temp table
            copy_stmt = sql.SQL(
                "COPY {stg} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
            ).format(stg=sql.Identifier(stg), cols=col_ident_list)

            # 3. Get CSV data as a bytes buffer
            buf = _rows_to_csv_buffer(rows, cols, json_cols=json_cols)

            # 4. Execute the async COPY
            async with cur.copy(copy_stmt.as_string(cur)) as copy:
                await copy.write(buf.read())

            # 5. Atomically replace the old table with the new one
            await cur.execute(sql.SQL("TRUNCATE {tbl}").format(tbl=sql.Identifier(schema, table)))
            await cur.execute(sql.SQL(
                "INSERT INTO {tbl} ({cols}) SELECT {cols} FROM {stg}"
            ).format(tbl=sql.Identifier(schema, table), cols=col_ident_list, stg=sql.Identifier(stg)))
        
        await conn.commit() # Commit the transaction
    except Exception as e:
        await conn.rollback()
        raise e

async def upsert_rows(conn, schema, table, cols, key_cols, rows, json_cols=frozenset()):
    """
    Fast, async, pooler-friendly upsert using COPY -> Staging -> INSERT ON CONFLICT.
    Rows should be a list of DICTIONARIES.
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

    await conn.set_autocommit(False)
    try:
        async with conn.cursor() as cur:
            # 1. Create temp staging table
            await cur.execute(sql.SQL(
                "CREATE TEMP TABLE {stg} (LIKE {tbl} INCLUDING DEFAULTS) ON COMMIT DROP"
            ).format(stg=sql.Identifier(stg), tbl=sql.Identifier(schema, table)))

            # 2. Prep the COPY statement
            copy_stmt = sql.SQL(
                "COPY {stg} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
            ).format(stg=sql.Identifier(stg), cols=col_ident_list)
            
            # 3. Get CSV data as a bytes buffer
            buf = _rows_to_csv_buffer(rows, cols, json_cols=json_cols)
            
            # 4. Execute async COPY
            async with cur.copy(copy_stmt.as_string(cur)) as copy:
                await copy.write(buf.read())

            # 5. Execute the INSERT... ON CONFLICT
            await cur.execute(sql.SQL("""
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
            
        await conn.commit()
    except Exception as e:
        await conn.rollback()
        raise e