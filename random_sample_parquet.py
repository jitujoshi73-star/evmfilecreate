import duckdb

# 10 random addresses nikalne ke liye
duckdb.query("""
    SELECT address 
    FROM 'D:/bsc/BSC_Addresses/all_chains_master_unique.parquet' 
    LIMIT 10
""").show()
