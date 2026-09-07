import duckdb

target_addr = "0x1234...5678"  # lower case me

result = duckdb.query(f"""
    SELECT address 
    FROM 'D:/bsc/BSC_Addresses/all_chains_master_unique.parquet'
    WHERE address = '{target_addr}'
""").fetchall()

if result:
    print("Address Found!")
else:
    print("Not Found")
