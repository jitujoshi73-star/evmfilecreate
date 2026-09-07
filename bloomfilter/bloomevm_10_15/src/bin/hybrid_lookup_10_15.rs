use duckdb::Connection;
use memmap2::Mmap;
use std::fs::File;
use std::hash::Hasher;
use std::time::Instant;
use twox_hash::XxHash64;

// ==========================================
// 10^-15 CONFIGURATION CONSTANTS
// ==========================================
const TOTAL_BITS: u64 = 76_884_517_000;
const NUM_HASHES: u64 = 50;

const BLOOM_PATH: &str = "D:/bsc/BSC_Addresses/evm_10_15.bloom";
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

// Stage 1: Ultra-fast Mmap Bitset Check (Sub-microsecond)
fn bloom_contains(mmap: &[u8], addr: &str) -> bool {
    let bytes = addr.trim().to_ascii_lowercase();
    let (h1, h2) = get_two_hashes(bytes.as_bytes());

    for i in 0..NUM_HASHES {
        let bit_idx = h1.wrapping_add(i.wrapping_mul(h2)) % TOTAL_BITS;
        let byte_idx = (bit_idx / 8) as usize;
        let bit_mask = 1u8 << (bit_idx % 8);

        // Agar ek bhi bit 0 mili, toh 100% address exist nahi karta
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
        println!("Opening 9.61 GB Bloom Filter via Memory Map...");
        let file = File::open(BLOOM_PATH)
            .expect("evm_10_15.bloom file nahi mili! Pehle 10^-15 wali generator script run karein.");
        let mmap = unsafe { Mmap::map(&file).expect("Mmap initialization failed") };

        println!("Initializing DuckDB Engine for Parquet Verification...");
        let conn = Connection::open_in_memory().expect("Failed to open DuckDB memory instance");

        Self {
            mmap,
            duckdb_conn: conn,
        }
    }

    pub fn check_address(&self, address: &str) -> (bool, &'static str) {
        let clean_addr = address.trim().to_ascii_lowercase();

        // 1. Bloom Filter Check (Nanoseconds)
        if !bloom_contains(&self.mmap, &clean_addr) {
            return (false, "Rejected by Bloom Filter (100% Not Found)");
        }

        // 2. Parquet Fallback Verification (Sirf tab chalega jab Bloom 'YES' bole)
        let query = format!(
            "SELECT 1 FROM '{}' WHERE address = '{}' LIMIT 1;",
            PARQUET_PATH, clean_addr
        );

        let mut stmt = self.duckdb_conn.prepare(&query).expect("SQL Prepare Error");
        let mut rows = stmt.query([]).expect("SQL Execution Error");

        if rows.next().unwrap().is_some() {
            (true, "Confirmed by Parquet (Verified Present)")
        } else {
            (false, "False Positive caught by Parquet")
        }
    }
}

fn main() {
    let checker = HybridAddressChecker::new();
    println!("Hybrid Engine ready for real-time queries!\n");

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
        println!("Latency : {:?}\n", duration);
    }
}
