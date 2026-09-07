import os
import duckdb

BASE_DIR = r"D:/bsc/BSC_Addresses"
MASTER_PARQUET = f"{BASE_DIR}/all_chains_master_unique.parquet"
FAST_DIR = f"{BASE_DIR}/fast_partitions"

os.makedirs(FAST_DIR, exist_ok=True)

con = duckdb.connect()
con.execute("SET preserve_insertion_order = false;")
con.execute("SET threads = 4;")

print("Splitting into 16 optimized prefix files...")

# 0x0 se 0xf tak 16 alag sorted files banegi
for c in range(16):
    pfx = f"0x{c:x}"
    out_file = f"{FAST_DIR}/part_{pfx}.parquet"
    
    if os.path.exists(out_file):
        continue
        
    print(f"Writing {pfx}...")
    con.execute(f"""
        COPY (
            SELECT address 
            FROM '{MASTER_PARQUET}' 
            WHERE starts_with(address, '{pfx}')
            ORDER BY address
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

con.close()
print("Done! Fast partitions ready at:", FAST_DIR)
