use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, BufWriter, Write};
use std::time::Instant;
use twox_hash::XxHash64;

// ==========================================
// 10^-15 CONFIGURATION CONSTANTS
// ==========================================
const TOTAL_BITS: u64 = 76_884_517_000;
const NUM_HASHES: u64 = 50;
const BLOOM_PATH: &str = "D:/bsc/BSC_Addresses/evm_10_15.bloom";

// Input aur Output CSV Paths
const INPUT_CSV: &str = "D:/bsc/input_seeds.csv";
const OUTPUT_CSV: &str = "D:/bsc/matched_output.csv";

const BATCH_SIZE: usize = 100_000; // Ek baar me 1 lakh rows parallel process hongi

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

fn main() {
    println!("Loading 9.61 GB Bloom Filter via Mmap...");
    let bloom_file = File::open(BLOOM_PATH)
        .expect("Bloom filter file nahi mili! Path check karein.");
    let mmap = unsafe { Mmap::map(&bloom_file).expect("Mmap failed") };

    println!("Opening Input CSV: {}", INPUT_CSV);
    let infile = File::open(INPUT_CSV).expect("Input CSV file nahi mili!");
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(BufReader::with_capacity(16 * 1024 * 1024, infile));

    // Headers check karein aur index dhoondhein
    let headers = rdr.headers().expect("CSV Header padhne me error").clone();
    let seed_idx = headers.iter().position(|h| h.trim().eq_ignore_ascii_case("seed"))
        .expect("CSV me 'seed' column nahi mila!");
    let addr_idx = headers.iter().position(|h| h.trim().eq_ignore_ascii_case("addresses") || h.trim().eq_ignore_ascii_case("address"))
        .expect("CSV me 'addresses' column nahi mila!");

    println!("Output ready at: {}", OUTPUT_CSV);
    let outfile = File::create(OUTPUT_CSV).expect("Output file create nahi hui");
    let mut writer = BufWriter::with_capacity(16 * 1024 * 1024, outfile);

    // Header write karein
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
        let addr = record.get(addr_idx).unwrap_or("").trim().to_string();

        if addr.len() == 42 && addr.starts_with("0x") {
            batch.push((seed, addr));
        }

        if batch.len() >= BATCH_SIZE {
            total_scanned += batch.len() as u64;

            // Multi-core parallel filtering
            let matches: Vec<&(String, String)> = batch
                .par_iter()
                .filter(|(_, addr)| bloom_contains(&mmap, addr))
                .collect();

            for (s, a) in &matches {
                writeln!(writer, "{},{}", s, a).unwrap();
            }

            total_matched += matches.len() as u64;
            println!(
                "Scanned: {:>10} rows | Matches: {:>6} | Speed: {:.0} rows/s",
                total_scanned,
                total_matched,
                total_scanned as f64 / start_time.elapsed().as_secs_f64()
            );

            batch.clear();
        }
    }

    // Remaining rows process karein
    if !batch.is_empty() {
        total_scanned += batch.len() as u64;
        let matches: Vec<&(String, String)> = batch
            .par_iter()
            .filter(|(_, addr)| bloom_contains(&mmap, addr))
            .collect();

        for (s, a) in &matches {
            writeln!(writer, "{},{}", s, a).unwrap();
        }
        total_matched += matches.len() as u64;
    }

    writer.flush().unwrap();

    println!("\n" + "=" * 50);
    println!("PROCESSING COMPLETED!");
    println!("Total Rows Scanned : {}", total_scanned);
    println!("Total Matches Found: {}", total_matched);
    println!("Total Time Taken   : {:.2}s", start_time.elapsed().as_secs_f64());
    println!("Output File Saved  : {}", OUTPUT_CSV);
    println!("=" * 50);
}
