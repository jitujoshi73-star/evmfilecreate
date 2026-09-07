import os
import glob
import time
import shutil
import duckdb

# ==========================================
# CONFIGURATION (Windows Safe Paths)
# ==========================================
BASE_DIR = r"D:/bsc/BSC_Addresses"
PATTERN = f"{BASE_DIR}/bnb_blocks_*.csv.gz"

OUTPUT_PARQUET = f"{BASE_DIR}/bsc_addresses_master_unique.parquet"
OUTPUT_CSV = f"{BASE_DIR}/bsc_master_unique.csv.gz"

TEMP_DIR = f"{BASE_DIR}/duckdb_temp"
STAGE_DIR = f"{BASE_DIR}/_stage_partitions"

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(STAGE_DIR, exist_ok=True)

BATCH_SIZE = 1

# ==========================================
# FILE DISCOVERY
# ==========================================
files = sorted(glob.glob(PATTERN))
print(f"[{time.strftime('%H:%M:%S')}] Target directory: {BASE_DIR}")
print(f"[{time.strftime('%H:%M:%S')}] Found {len(files)} compressed archives matching pattern.")

if not files:
    raise FileNotFoundError(
        f"No files matching '{PATTERN}' found. Ensure your files are placed in '{BASE_DIR}'."
    )

total_start = time.time()

# ==========================================
# STAGE 1: BATCHED LOCAL DEDUPLICATION
# ==========================================
print(f"\n[{time.strftime('%H:%M:%S')}] Starting Stage 1: Partitioned extraction ({BATCH_SIZE} files/batch)...")

total_batches = (len(files) + BATCH_SIZE - 1) // BATCH_SIZE
stage_partitions = []

for idx in range(total_batches):
    batch = files[idx * BATCH_SIZE : (idx + 1) * BATCH_SIZE]
    partition_file = f"{STAGE_DIR}/part_{idx:03d}.parquet"
    stage_partitions.append(partition_file)

    if os.path.exists(partition_file) and os.path.getsize(partition_file) > 0:
        print(f"[{time.strftime('%H:%M:%S')}]  -> Batch {idx + 1}/{total_batches} already exists. Skipping.")
        continue

    t_batch = time.time()

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET memory_limit = '7GB';")
    con.execute("SET threads = 2;")
    con.execute(f"SET temp_directory = '{TEMP_DIR}';")

    batch_sql_list = "[" + ", ".join([f"'{f.replace(os.sep, '/')}'" for f in batch]) + "]"

    con.execute(f"""
        COPY (
            SELECT DISTINCT LOWER(TRIM(address)) AS address
            FROM read_csv({batch_sql_list}, header=True, columns={{'address': 'VARCHAR'}}, parallel=true)
            WHERE address IS NOT NULL
              AND LENGTH(TRIM(address)) = 42
        ) TO '{partition_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    con.close()

    mb_size = os.path.getsize(partition_file) / (1024 * 1024)
    print(f"[{time.strftime('%H:%M:%S')}]  -> Batch {idx + 1}/{total_batches} generated ({mb_size:.1f} MB) in {time.time() - t_batch:.1f}s")

# ==========================================
# STAGE 2: GLOBAL DEDUPLICATION
# ==========================================
print(f"\n[{time.strftime('%H:%M:%S')}] Starting Stage 2: Merging {len(stage_partitions)} partitions globally...")

con = duckdb.connect()
con.execute("SET preserve_insertion_order = false;")
con.execute("SET memory_limit = '8.5GB';")
con.execute("SET threads = 2;")
con.execute(f"SET temp_directory = '{TEMP_DIR}';")

stage_pattern_sql = f"{STAGE_DIR}/part_*.parquet"

con.execute(f"""
    COPY (
        SELECT DISTINCT address
        FROM read_parquet('{stage_pattern_sql}')
    ) TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

parquet_size_mb = os.path.getsize(OUTPUT_PARQUET) / (1024 * 1024)
print(f"[{time.strftime('%H:%M:%S')}] -> Master Parquet generated: {OUTPUT_PARQUET} ({parquet_size_mb:.2f} MB)")

total_unique = con.execute(f"SELECT COUNT(*) FROM '{OUTPUT_PARQUET}'").fetchone()[0]

print("\n" + "=" * 60)
print(f"TOTAL GLOBALLY UNIQUE bsc ADDRESSES: {total_unique:,}")
print("=" * 60 + "\n")

print(f"[{time.strftime('%H:%M:%S')}] Generating final compressed CSV...")
con.execute(f"""
    COPY (
        SELECT address
        FROM '{OUTPUT_PARQUET}'
    ) TO '{OUTPUT_CSV}' (FORMAT CSV, HEADER, COMPRESSION GZIP);
""")

con.close()

csv_size_mb = os.path.getsize(OUTPUT_CSV) / (1024 * 1024)
print(f"[{time.strftime('%H:%M:%S')}] -> Master CSV.GZ generated: {OUTPUT_CSV} ({csv_size_mb:.2f} MB)")

# ==========================================
# CLEANUP
# ==========================================
print(f"[{time.strftime('%H:%M:%S')}] Purging intermediate cache...")
shutil.rmtree(TEMP_DIR, ignore_errors=True)
shutil.rmtree(STAGE_DIR, ignore_errors=True)

print(f"[{time.strftime('%H:%M:%S')}] Completed successfully in {time.time() - total_start:.1f}s.")
