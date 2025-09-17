"""
- Connects to a MongoDB collection.
- Infers a SQL Server table schema (scans sample documents).
- Creates a table (if doesn't exist) with same name as collection. Setting to allow 'drop and recreating' or using existing table.
- Inserts documents (converting nested objects to JSON, ObjectId -> str, Decimal128 -> Decimal).
"""

import pymongo
import pyodbc
import json
import decimal
from bson import ObjectId, Decimal128, Int64
import datetime
from collections import defaultdict

# ---------------- CONFIG ----------------
COLLECTION_NAMES = "coll_1;coll_2;coll_3;coll_4" 

ReCreateIfExists = 1        # 0 = keep table, 1 = drop & recreate

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "TestMongoData"
SQL_CONN = "SERVER=localhost;DATABASE=TestMongoToSql;Trusted_Connection=yes;"
#SQL_CONN = "SERVER=localhost\\SQLEXPRESS;DATABASE=TestMongoToSql;Trusted_Connection=yes;"
#SQL_CONN = "SERVER=localhost;DATABASE=TestMongoToSql;UID=XXXX;PWD=XXXXX;"

SAMPLE_SIZE = 1000    # number of docs to scan for schema inference; set 0 or None to scan entire collection
BATCH_INSERT = 500    # commit every N rows
DECIMAL_PRECISION = 38
DECIMAL_SCALE = 18
# ----------------------------------------

# helper ranges for int and bigint
INT32_MIN = -2**31
INT32_MAX = 2**31 - 1
INT64_MIN = -2**63
INT64_MAX = 2**63 - 1

def analyze_collection_schema(collection, sample_size=1000):
    """Scan documents and gather type statistics per field."""
    stats = {}
    total_docs = 0

    cursor = collection.find({})
    for i, doc in enumerate(cursor):
        if sample_size and sample_size > 0 and i >= sample_size:
            break
        total_docs += 1
        # ensure keys in stats
        for key in doc.keys():
            if key not in stats:
                stats[key] = {
                    "types": set(),
                    "count": 0,
                    "int_min": None,
                    "int_max": None,
                    "max_str_len": 0,
                }
        # update counts for seen fields
        for key, value in doc.items():
            st = stats[key]
            st["count"] += 1
            # detect types
            if value is None:
                st["types"].add("null")
            elif isinstance(value, bool):
                st["types"].add("bool")
            elif isinstance(value, (Int64,)) or (isinstance(value, int) and not isinstance(value, bool)):
                st["types"].add("int")
                v = int(value)
                if st["int_min"] is None or v < st["int_min"]:
                    st["int_min"] = v
                if st["int_max"] is None or v > st["int_max"]:
                    st["int_max"] = v
            elif isinstance(value, float):
                st["types"].add("float")
            elif isinstance(value, (Decimal128, decimal.Decimal)):
                st["types"].add("decimal")
            elif isinstance(value, datetime.datetime):
                st["types"].add("datetime")
            elif isinstance(value, str):
                st["types"].add("str")
                st["max_str_len"] = max(st["max_str_len"], len(value))
            elif isinstance(value, ObjectId):
                st["types"].add("objectid")
            elif isinstance(value, (list, dict)):
                st["types"].add("json")
            elif isinstance(value, (bytes, bytearray, memoryview)):
                st["types"].add("bytes")
            else:
                # fallback, treat as string
                st["types"].add("str")
                try:
                    st["max_str_len"] = max(st["max_str_len"], len(str(value)))
                except Exception:
                    pass

    # fields that never appeared (shouldn't happen) -> ignore
    # determine nullable fields (if occurrence < total_docs)
    for key, st in stats.items():
        st["nullable"] = st["count"] < total_docs

    return stats, total_docs

def sql_type_from_stats(st):
    """Return SQL Server column type (string) given stats for field."""
    types = st["types"]
    # if only nulls
    if types == {"null"}:
        return f"NVARCHAR(MAX)"

    # if JSON present or mixed containing json -> NVARCHAR(MAX)
    if "json" in types:
        return "NVARCHAR(MAX)"

    # bytes
    if types == {"bytes"}:
        return "VARBINARY(MAX)"

    # object id
    if types == {"objectid"} or ("objectid" in types and len(types) == 1):
        # store as hex string e.g. "60b8..."
        return "NVARCHAR(24)"

    # boolean
    if types == {"bool"}:
        return "BIT"

    # datetime
    if types == {"datetime"}:
        return "DATETIME2"

    # decimal related
    if "decimal" in types and types.issubset({"decimal", "int"}):
        # choose DECIMAL to preserve precision (use default precision/scale)
        return f"DECIMAL({DECIMAL_PRECISION},{DECIMAL_SCALE})"

    # float (or int+float)
    if "float" in types:
        return "FLOAT"

    # integer-only
    if types == {"int"} or types == {"int", "null"}:
        # decide INT vs BIGINT based on min/max
        lo = st["int_min"]
        hi = st["int_max"]
        if lo is None or hi is None:
            return "BIGINT"
        # if fits in 32-bit
        if INT32_MIN <= lo <= hi <= INT32_MAX:
            return "INT"
        # if fits in signed 64-bit
        if INT64_MIN <= lo <= hi <= INT64_MAX:
            return "BIGINT"
        # otherwise fallback to DECIMAL
        return f"DECIMAL({DECIMAL_PRECISION},0)"

    # string rules: if only strings (or str + null)
    if types.issubset({"str", "null"}):
        maxlen = st.get("max_str_len", 0) or 0
        if maxlen <= 1:
            return "NCHAR(1)"
        if maxlen <= 255:
            return "NVARCHAR(255)"
        # longer -> NVARCHAR(MAX)
        return "NVARCHAR(MAX)"

    # if mixture of types (e.g., int + str) -> NVARCHAR(MAX)
    return "NVARCHAR(MAX)"

def build_create_table_statement(table_name, schema_map):
    """schema_map: dict column -> sql_type"""
    cols = []
    for col, sql_type in schema_map.items():
        cols.append(f"[{col}] {sql_type} NULL")
    cols_sql = ",\n  ".join(cols)

    if ReCreateIfExists == 1:
        create_stmt = f"""
IF OBJECT_ID(N'dbo.{table_name}', 'U') IS NOT NULL
    DROP TABLE [dbo].[{table_name}];
CREATE TABLE [dbo].[{table_name}] (
  {cols_sql}
);
"""
    else:
        create_stmt = f"""
IF OBJECT_ID(N'dbo.{table_name}', 'U') IS NULL
BEGIN
  CREATE TABLE [dbo].[{table_name}] (
  {cols_sql}
  );
END
"""
    return create_stmt

def convert_value_for_sql(value):
    """Convert BSON/python value into something pyodbc  friendly for insertion."""
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)   # 24-char hex
    if isinstance(value, Decimal128):
        # Decimal128 -> decimal.Decimal
        try:
            return value.to_decimal()
        except Exception:
            return decimal.Decimal(str(value))
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return bytes(value)
    # datetime is ok
    if isinstance(value, datetime.datetime):
        return value
    # booleans, ints, floats, strings are fine
    return value

def create_table_and_insert(mongo_collection, sql_conn, table_name, sample_size=1000):
    stats, total = analyze_collection_schema(mongo_collection, sample_size=sample_size)
    # ensure _id exists in schema (if present)
    if "_id" not in stats:
        # maybe collection empty; choose to create empty table if needed
        print("Warning: _id not present in scanned docs (collection may be empty). Creating table with only _id as NVARCHAR(24).")
        stats["_id"] = {"types": {"objectid"}, "count":0, "max_str_len":24, "int_min":None, "int_max":None, "nullable": True}

    # compute SQL types
    schema_map = {}
    # keep deterministic order: put _id first then rest sorted
    ordered_fields = ["_id"] + sorted([k for k in stats.keys() if k != "_id"])
    for field in ordered_fields:
        sql_type = sql_type_from_stats(stats[field])
        schema_map[field] = sql_type

    # create table
    cursor = sql_conn.cursor()
    create_stmt = build_create_table_statement(table_name, schema_map)
    cursor.execute(create_stmt)
    sql_conn.commit()
    print(f"Table [{table_name}] ensured (created if missing).")

    # Insert data
    insert_cols = ordered_fields
    placeholders = ", ".join(["?"] * len(insert_cols))
    columns_sql = ", ".join([f"[{c}]" for c in insert_cols])
    insert_stmt = f"INSERT INTO [dbo].[{table_name}] ({columns_sql}) VALUES ({placeholders})"

    batch = 0
    rows = 0
    for doc in mongo_collection.find({}):
        rows += 1
        values = []
        for c in insert_cols:
            values.append(convert_value_for_sql(doc.get(c)))
        try:
            cursor.execute(insert_stmt, tuple(values))
        except Exception as ex:
            # On insert error, try a safer conversion: dump complex to JSON strings
            safe_vals = []
            for v in values:
                if isinstance(v, (dict, list)):
                    safe_vals.append(json.dumps(v, default=str))
                else:
                    safe_vals.append(v)
            cursor.execute(insert_stmt, tuple(safe_vals))
        batch += 1
        if batch >= BATCH_INSERT:
            sql_conn.commit()
            batch = 0
            print(f"Inserted {rows} rows...")

    # final commit
    sql_conn.commit()
    print(f"Insert finished. Total rows processed: {rows}")

def main():
    # connect mongo
    mongo_client = pymongo.MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]

    # connect SQL Server
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"{SQL_CONN}"
    )
    sql_conn = pyodbc.connect(conn_str, autocommit=False)

    try:
        # split multiple collections by ;
        for coll_name in COLLECTION_NAMES.split(";"):
            coll_name = coll_name.strip()
            if not coll_name:
                continue
            print(f"\n Processing collection: {coll_name}")
            collection = mongo_db[coll_name]
            create_table_and_insert(collection, sql_conn, coll_name, sample_size=SAMPLE_SIZE)
    finally:
        sql_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    main()
