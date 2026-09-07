# Sirf pehle 10 lakh (1M) addresses ka CSV banana
duckdb.query("""
    COPY (
        SELECT address 
        FROM 'D:/bsc/BSC_Addresses/all_chains_master_unique.parquet' 
        LIMIT 1000000
    ) TO 'D:/bsc/sample.csv' (FORMAT CSV, HEADER);
""")
