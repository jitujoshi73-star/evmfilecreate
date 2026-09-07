import time
import duckdb

DATA_DIR = r"D:/bsc/BSC_Addresses"
OUTPUT_PARQUET = f"{DATA_DIR}/all_chains_master_unique.parquet"
OUTPUT_CSV = f"{DATA_DIR}/all_chains_master_unique.csv.gz"

print(f"[{time.strftime('%H:%M:%S')}] Converting Master Parquet to CSV.GZ...")
t0 = time.time()

con = duckdb.connect()
# Threads badha kar 4 ya 6 kar sakte hain taaki export thoda jaldi ho
con.execute("SET threads = 4;")
con.execute("SET memory_limit = '6GB';")

con.execute(f"""
    COPY (
        SELECT address FROM '{OUTPUT_PARQUET}'
    ) TO '{OUTPUT_CSV}' (FORMAT CSV, HEADER, COMPRESSION GZIP);
""")
con.close()

print(f"[{time.strftime('%H:%M:%S')}] CSV.GZ generated successfully in {time.time() - t0:.1f}s.")
