use memmap2::Mmap;
use std::fs::File;
use std::hash::Hasher;
use std::time::Instant;
use twox_hash::XxHash64;

// Exactly matches 10^-9 configuration
const TOTAL_BITS: u64 = 46_128_984_000;
const NUM_HASHES: u64 = 30;
const FILTER_PATH: &str = "D:/bsc/BSC_Addresses/evm_10_9.bloom";

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

fn contains(mmap: &[u8], addr: &str) -> bool {
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
    let file = File::open(FILTER_PATH).expect("Bloom filter file not found");
    let mmap = unsafe { Mmap::map(&file).expect("Failed to mmap filter") };

    // Test addresses
    let test_addr = "0x8894e0a0c962cb723c1976a4421c95949be2d4e3";

    let t0 = Instant::now();
    let present = contains(&mmap, test_addr);
    let duration = t0.elapsed();

    println!("Address: {}", test_addr);
    println!("Present: {}", present);
    println!("Lookup Duration: {:?}", duration);
}
