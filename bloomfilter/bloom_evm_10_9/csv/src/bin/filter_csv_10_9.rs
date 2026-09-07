use duckdb::Connection;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, BufWriter, Write};
use std::time::Instant;
use twox_hash::XxHash64;

// ==========================================
// 10^-9 CONFIGURATION CONSTANTS
// ==========================================
const TOTAL_BITS: u64 = 46_128_984_000;
const NUM_HASHES: u64 = 30;

const BLOOM_PATH: &str = "D:/bsc/BSC_Addresses/evm_10_9.bloom";
const PARQUET_PATH: &str = "D:/bsc/BSC_Addresses/all_chains_master_unique.parquet";

// Input & Output paths (Apni zaroorat ke hisab se badal lein)
const INPUT_CSV: &str = "D:/bsc/input_seeds.csv";
const OUTPUT_CSV: &str = "D:/bsc/matched_output.csv";

const BATCH_SIZE: usize = 100_000;

#[inline(always)]
fn get_two_hashes(data: &[u8]) -> (u64, u64) {
    let mut h1 = XxHash64::with_seed(0);
    h1.write(data);
    let hash1 = h1.finish();

    let mut h2 = XxHash64::with_seed(hash1);
    h2.write(data);
    let hash2 = h2.finish();

    (hash1, hash2)
}

#[inline(always)]
fn bloom_contains(mmap: &[u8], addr: &str) -> bool {
    let bytes = addr.trim().to_ascii_lowercase();
    let (h1, h2) = get_two_hashes(bytes.as_bytes());

    for i in 0..NUM_HASHES {
        let bit_idx = h1.wrapping_add(i.wrapping_mul(h2)) % TOTAL_BITS;
        let byte_idx = (bit_idx / 8) as usize;
        let bit_mask = 1u8 << (bit_idx % 8);

        if (mmap[byte_idx] & bit_mask) == 0 {
            return false;
        }
    }
    true
}

// Parquet fallback: Bloom ke yes bolne par 100% confirmation ke liye
fn verify_parquet(conn: &Connection, addr: &str) -> bool {
    let query = format!(
        "SELECT 1 FROM '{}' WHERE address = '{}' LIMIT 1;",
        PARQUET_PATH, addr
    );
    let mut stmt = conn.prepare(&query).unwrap();
    let mut rows = stmt.query([]).unwrap();
    rows.next().unwrap().is_some()
}

fn main() {
    println!("Loading 5.76 GB Bloom Filter (10^-9) via Memory Map...");
    let bloom_file = File::open(BLOOM_PATH)
        .expect("evm_10_9.bloom nahi mili! Pehle generator script run karein.");
    let mmap = unsafe { Mmap::map(&bloom_file).expect("Failed to mmap filter file") };

    println!("Initializing DuckDB for Parquet fallback confirmation...");
    let duckdb_conn = Connection::open_in_memory().expect("DuckDB initialization failed");

    println!("Opening Input CSV: {}", INPUT_CSV);
    let infile = File::open(INPUT_CSV).expect("Input CSV file nahi mili!");
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(BufReader::with_capacity(16 * 1024 * 1024, infile));

    let headers = rdr.headers().expect("CSV Header missing").clone();
    let seed_idx = headers.iter().position(|h| h.trim().eq_ignore_ascii_case("seed"))
        .expect("CSV me 'seed' column nahi mila!");
    let addr_idx = headers.iter().position(|h| h.trim().eq_ignore_ascii_case("addresses") || h.trim().eq_ignore_ascii_case("address"))
        .expect("CSV me 'addresses' column nahi mila!");

    println!("Writing matches to: {}", OUTPUT_CSV);
    let outfile = File::create(OUTPUT_CSV).expect("Output file create nahi ho saki");
    let mut writer = BufWriter::with_capacity(16 * 1024 * 1024, outfile);

    writeln!(writer, "seed,addresses").unwrap();

    let start_time = Instant::now();
    let mut batch: Vec<(String, String)> = Vec::with_capacity(BATCH_SIZE);
    let mut total_scanned: u64 = 0;
    let mut total_matched: u64 = 0;

    for result in rdr.records() {
        let record = match result {
            Ok(rec) => rec,
            Err(_) => continue,
        };

        let seed = record.get(seed_idx).unwrap_or("").trim().to_string();
        let addr = record.get(addr_idx).unwrap_or("").trim().to_lowercase();

        if addr.len() == 42 && addr.starts_with("0x") {
            batch.push((seed, addr));
        }

        if batch.len() >= BATCH_SIZE {
            total_scanned += batch.len() as u64;

            // Stage 1: Parallel Bloom check across CPU threads
            let potential_matches: Vec<&(String, String)> = batch
                .par_iter()
                .filter(|(_, addr)| bloom_contains(&mmap, addr))
                .collect();

            // Stage 2: Parquet verification sirf candidate matches ke liye
            for (s, a) in potential_matches {
                if verify_parquet(&duckdb_conn, a) {
                    writeln!(writer, "{},{}", s, a).unwrap();
                    total_matched += 1;
                }
            }

            println!(
                "Scanned: {:>10} rows | Verified Matches: {:>6} | Speed: {:.0} rows/s",
                total_scanned,
                total_matched,
                total_scanned as f64 / start_time.elapsed().as_secs_f64()
            );

            batch.clear();
        }
    }

    // Remaining rows
    if !batch.is_empty() {
        total_scanned += batch.len() as u64;
        let potential_matches: Vec<&(String, String)> = batch
            .par_iter()
            .filter(|(_, addr)| bloom_contains(&mmap, addr))
            .collect();

        for (s, a) in potential_matches {
            if verify_parquet(&duckdb_conn, a) {
                writeln!(writer, "{},{}", s, a).unwrap();
                total_matched += 1;
            }
        }
    }

    writer.flush().unwrap();

    println!("\n" + "=" * 50);
    println!("PROCESSING COMPLETED!");
    println!("Total Rows Scanned : {}", total_scanned);
    println!("Verified Matches   : {}", total_matched);
    println!("Time Taken         : {:.2}s", start_time.elapsed().as_secs_f64());
    println!("Output File        : {}", OUTPUT_CSV);
    println!("=" * 50);
}
