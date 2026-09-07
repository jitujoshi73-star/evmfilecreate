use duckdb::Connection;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use twox_hash::XxHash64;
use std::hash::Hasher;

// Mathematical constants for 1.07B items & 10^-15 FP Rate
const TOTAL_BITS: u64 = 76_884_517_000;
const U64_WORDS: usize = ((TOTAL_BITS + 63) / 64) as usize;
const NUM_HASHES: u64 = 50;

const PARQUET_PATH: &str = "D:/bsc/BSC_Addresses/all_chains_master_unique.parquet";
const OUTPUT_FILE: &str = "D:/bsc/BSC_Addresses/evm_10_15.bloom";
const CHUNK_SIZE: usize = 500_000;

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

fn main() {
    println!("Allocating ~9.61 GB RAM for Atomic Bitset...");
    let start_time = Instant::now();

    // Allocate 0-initialized atomic buffer
    let mut bitset: Vec<AtomicU64> = Vec::with_capacity(U64_WORDS);
    bitset.resize_with(U64_WORDS, || AtomicU64::new(0));
    let bitset = Arc::new(bitset);

    println!("Connecting to DuckDB and streaming from Parquet...");
    let conn = Connection::open_in_memory().expect("Failed to create DuckDB memory instance");

    let query = format!("SELECT address FROM '{}'", PARQUET_PATH);
    let mut stmt = conn.prepare(&query).expect("Failed to prepare query");
    let mut rows = stmt.query([]).expect("Failed to execute query");

    let mut batch: Vec<String> = Vec::with_capacity(CHUNK_SIZE);
    let mut total_processed: u64 = 0;
    let mut batch_timer = Instant::now();

    while let Some(row) = rows.next().expect("Error reading row") {
        let addr: String = row.get(0).expect("Invalid address string");
        batch.push(addr);

        if batch.len() >= CHUNK_SIZE {
            let b_ref = Arc::clone(&bitset);
            
            // Parallel insertion across all CPU threads
            batch.par_iter().for_each(|addr| {
                let bytes = addr.trim().to_ascii_lowercase();
                let (h1, h2) = get_two_hashes(bytes.as_bytes());

                for i in 0..NUM_HASHES {
                    let bit_idx = h1.wrapping_add(i.wrapping_mul(h2)) % TOTAL_BITS;
                    let word_idx = (bit_idx / 64) as usize;
                    let mask = 1u64 << (bit_idx % 64);
                    b_ref[word_idx].fetch_or(mask, Ordering::Relaxed);
                }
            });

            total_processed += batch.len() as u64;
            let elapsed = batch_timer.elapsed().as_secs_f64();
            println!(
                "Processed: {:>12} addresses | Batch Time: {:.2}s | Overall: {:.1}s",
                total_processed,
                elapsed,
                start_time.elapsed().as_secs_f64()
            );

            batch.clear();
            batch_timer = Instant::now();
        }
    }

    // Process leftover records
    if !batch.is_empty() {
        let b_ref = Arc::clone(&bitset);
        batch.par_iter().for_each(|addr| {
            let bytes = addr.trim().to_ascii_lowercase();
            let (h1, h2) = get_two_hashes(bytes.as_bytes());

            for i in 0..NUM_HASHES {
                let bit_idx = h1.wrapping_add(i.wrapping_mul(h2)) % TOTAL_BITS;
                let word_idx = (bit_idx / 64) as usize;
                let mask = 1u64 << (bit_idx % 64);
                b_ref[word_idx].fetch_or(mask, Ordering::Relaxed);
            }
        });
        total_processed += batch.len() as u64;
    }

    println!("\nAll {} addresses added in {:.2}s", total_processed, start_time.elapsed().as_secs_f64());
    println!("Writing 9.61 GB bitset to '{}'...", OUTPUT_FILE);

    let file = File::create(OUTPUT_FILE).expect("Failed to create file");
    let mut writer = BufWriter::with_capacity(32 * 1024 * 1024, file); // 32MB buffer

    for word in bitset.iter() {
        let val = word.load(Ordering::Relaxed);
        writer.write_all(&val.to_le_bytes()).expect("Error writing bytes");
    }
    writer.flush().expect("Failed to flush buffer");

    println!("Bloom filter saved successfully! File size: ~9.61 GB");
}
