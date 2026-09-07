use duckdb::Connection;
use memmap2::Mmap;
use std::fs::File;
use std::hash::Hasher;
use std::time::Instant;
use twox_hash::XxHash64;

// 10^-9 Configuration Constants
const TOTAL_BITS: u64 = 46_128_984_000;
const NUM_HASHES: u64 = 30;

const BLOOM_PATH: &str = "D:/bsc/BSC_Addresses/evm_10_9.bloom";
const PARQUET_PATH: &str = "D:/bsc/BSC_Addresses/all_chains_master_unique.parquet";

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

// Stage 1: RAM/Mmap Bloom Check (< 1 microsecond)
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

pub struct HybridAddressChecker {
    mmap: Mmap,
    duckdb_conn: Connection,
}

impl HybridAddressChecker {
    pub fn new() -> Self {
        println!("Initializing Memory-Mapped Bloom Filter...");
        let file = File::open(BLOOM_PATH).expect("Bloom filter file not found. Generate it first!");
        let mmap = unsafe { Mmap::map(&file).expect("Failed to memory-map bloom filter") };

        println!("Initializing DuckDB Engine for Parquet Verification...");
        let conn = Connection::open_in_memory().expect("Failed to open DuckDB");

        Self {
            mmap,
            duckdb_conn: conn,
        }
    }

    pub fn check_address(&self, address: &str) -> (bool, &'static str) {
        let clean_addr = address.trim().to_ascii_lowercase();

        // 1. Instant Bloom check
        if !bloom_contains(&self.mmap, &clean_addr) {
            return (false, "Rejected by Bloom Filter (100% Not Found)");
        }

        // 2. Parquet verification (Only runs if Bloom says YES)
        let query = format!(
            "SELECT 1 FROM '{}' WHERE address = '{}' LIMIT 1;",
            PARQUET_PATH, clean_addr
        );

        let mut stmt = self.duckdb_conn.prepare(&query).expect("Query error");
        let mut rows = stmt.query([]).expect("Execution error");

        if rows.next().unwrap().is_some() {
            (true, "Confirmed by Parquet (Verified Present)")
        } else {
            (false, "False Positive caught by Parquet")
        }
    }
}

fn main() {
    let checker = HybridAddressChecker::new();
    println!("Ready for queries!\n");

    // Test cases: Ek valid address aur ek random invalid address
    let test_addresses = vec![
        "0x8894e0a0c962cb723c1976a4421c95949be2d4e3",
        "0x0000000000000000000000000000000000000000",
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    ];

    for addr in test_addresses {
        let start = Instant::now();
        let (exists, reason) = checker.check_address(addr);
        let duration = start.elapsed();

        println!("Address : {}", addr);
        println!("Found   : {}", exists);
        println!("Source  : {}", reason);
        println!("Time    : {:?}\n", duration);
    }
}
