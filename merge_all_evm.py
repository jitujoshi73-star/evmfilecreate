import os
import time
import shutil
import duckdb

# ==========================================
# CONFIGURATION
# ==========================================
# Jaha aapki 4 csv.gz files rakhi hain
DATA_DIR = r"D:/bsc/BSC_Addresses"

INPUT_FILES = [
    f"{DATA_DIR}/bsc_master_unique.csv.gz",
    f"{DATA_DIR}/arbitrum_addresses_master_unique.csv.gz",
    f"{DATA_DIR}/eth_addresses_master_unique.csv.gz",
    f"{DATA_DIR}/polygon_addresses_master_unique.csv.gz"
]

OUTPUT_PARQUET = f"{DATA_DIR}/all_chains_master_unique.parquet"
OUTPUT_CSV = f"{DATA_DIR}/all_chains_master_unique.csv.gz"

TEMP_DIR = f"{DATA_DIR}/duckdb_temp"
STAGE_DIR = f"{DATA_DIR}/_evm_merge_buckets"

os.makedirs(STAGE_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# Format file list for SQL
files_sql = "[" + ", ".join([f"'{f}'" for f in INPUT_FILES]) + "]"

# 0x0 se 0xf tak ke 16 prefixes
HEX_CHARS = [f"0x{c:x}" for c in range(16)]

total_start = time.time()
print(f"[{time.strftime('%H:%M:%S')}] Starting multi-chain EVM deduplication across 4 networks...")

# ==========================================
# STAGE 1: BUCKET-WISE GLOBAL DISTINCT
# ==========================================
for idx, prefix in enumerate(HEX_CHARS, 1):
    bucket_file = f"{STAGE_DIR}/bucket_{prefix}.parquet"
    
    if os.path.exists(bucket_file) and os.path.getsize(bucket_file) > 0:
        print(f"[{time.strftime('%H:%M:%S')}] -> Prefix {prefix} ({idx}/16) already done. Skipping.")
        continue

    t_b = time.time()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET memory_limit = '6GB';")
    con.execute("SET threads = 2;")
    con.execute(f"SET temp_directory = '{TEMP_DIR}';")

    # Sabhi 4 files me se sirf current prefix filter karke deduplicate karega
    con.execute(f"""
        COPY (
            SELECT DISTINCT address
            FROM read_csv({files_sql}, header=True, columns={{'address': 'VARCHAR'}}, parallel=true)
            WHERE starts_with(address, '{prefix}')
        ) TO '{bucket_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    con.close()

    mb_size = os.path.getsize(bucket_file) / (1024 * 1024)
    print(f"[{time.strftime('%H:%M:%S')}] -> Prefix {prefix} ({idx}/16) completed ({mb_size:.1f} MB) in {time.time() - t_b:.1f}s")

# ==========================================
# STAGE 2: FINAL MASTER PARQUET & CSV
# ==========================================
print(f"\n[{time.strftime('%H:%M:%S')}] Combining deduplicated buckets into master files...")

con = duckdb.connect()
con.execute("SET preserve_insertion_order = false;")
con.execute("SET memory_limit = '6GB';")
con.execute("SET threads = 2;")
con.execute(f"SET temp_directory = '{TEMP_DIR}';")

bucket_pattern = f"{STAGE_DIR}/bucket_*.parquet"

# Sabhi buckets already internally unique aur disjoint hain
con.execute(f"""
    COPY (
        SELECT address FROM read_parquet('{bucket_pattern}')
    ) TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

parquet_size_mb = os.path.getsize(OUTPUT_PARQUET) / (1024 * 1024)
print(f"[{time.strftime('%H:%M:%S')}] -> Master Parquet generated: {parquet_size_mb:.2f} MB")

# Count verification
total_unique = con.execute(f"SELECT COUNT(*) FROM '{OUTPUT_PARQUET}'").fetchone()[0]

print("\n" + "=" * 60)
print(f"TOTAL UNIQUE EVM ADDRESSES (ALL CHAINS): {total_unique:,}")
print("=" * 60 + "\n")

# Stream directly to final compressed CSV
print(f"[{time.strftime('%H:%M:%S')}] Generating final all_chains_master_unique.csv.gz...")
con.execute(f"""
    COPY (
        SELECT address FROM '{OUTPUT_PARQUET}'
    ) TO '{OUTPUT_CSV}' (FORMAT CSV, HEADER, COMPRESSION GZIP);
""")
con.close()

csv_size_mb = os.path.getsize(OUTPUT_CSV) / (1024 * 1024)
print(f"[{time.strftime('%H:%M:%S')}] -> Master CSV.GZ generated: {csv_size_mb:.2f} MB")

# ==========================================
# CLEANUP
# ==========================================
print(f"[{time.strftime('%H:%M:%S')}] Cleaning intermediate buckets and cache...")
shutil.rmtree(TEMP_DIR, ignore_errors=True)
shutil.rmtree(STAGE_DIR, ignore_errors=True)

print(f"[{time.strftime('%H:%M:%S')}] Process finished successfully in {time.time() - total_start:.1f}s.")
