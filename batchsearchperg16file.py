def check_bulk_addresses(address_list: list) -> list:
    df_input = [{"address": a.strip().lower()} for a in address_list]
    con.register("input_batch", duckdb.from_df(df_input))

    # Sabhi 16 partitions ke sath parallel match
    matches = con.execute(f"""
        SELECT i.address 
        FROM input_batch i
        INNER JOIN '{FAST_DIR}/part_*.parquet' p
          ON i.address = p.address
    """).fetchall()

    return [row[0] for row in matches]
