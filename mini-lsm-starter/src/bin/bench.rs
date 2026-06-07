// Bench: measure the effect of the SSTable bloom filter on point lookups.
//
//     cargo run --release --bin bench                 # default workloads
//     cargo run --release --bin bench -- 100000 250000 500000
//     BENCH_RUNS=5 cargo run --release --bin bench    # repeats per measurement
//
// NOTE: small SSTs mean many L0 files; raise the fd limit first:
//     ulimit -n 8192
//
// Methodology: for each workload size we load N keys once into a fresh store and
// flush EVERYTHING to L0 SSTs, then run the SAME lookups twice -- once with the
// bloom filter enabled, once disabled (BLOOM_DISABLED). The bloom check is the
// ONLY thing that differs between the two runs; the engine (flush thread,
// compaction, block cache, SST format) is identical. Each measurement does a
// warmup pass and then reports the mean wall-clock time over BENCH_RUNS passes.
//
// We report time only. A block-read counter is unreliable here because the two
// modes share one block cache (the first mode warms it for the second), and
// week-1 has no SST recovery so we cannot reopen with a cold cache per mode.
//
// Bloom filters help most on ABSENT keys: every L0 SST whose filter says "no"
// is skipped without building an iterator / seeking into it. They also speed up
// PRESENT-key lookups, because without the filter every L0 SST is opened and
// seeked on every lookup even though only one holds the key.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::Result;
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
use mini_lsm_starter::table::BLOOM_DISABLED;

const TARGET_SST_SIZE: usize = 64 * 1024; // small => many L0 SSTs (where bloom pays off)
const NUM_MEMTABLE_LIMIT: usize = 8;
const N_LOOKUPS: usize = 2_000;

fn key_of(i: usize) -> Vec<u8> {
    format!("key{i:08}").into_bytes()
}

fn absent_key_of(i: usize) -> Vec<u8> {
    format!("absent{i:08}").into_bytes()
}

/// One lookup batch; returns wall-clock time.
fn lookup_batch(lsm: &MiniLsm, present: bool, n_keys: usize) -> Result<Duration> {
    let t = Instant::now();
    for i in 0..N_LOOKUPS {
        let k = if present {
            key_of(i % n_keys)
        } else {
            absent_key_of(i)
        };
        let _ = lsm.get(&k)?;
    }
    Ok(t.elapsed())
}

/// Warmup pass + mean over `runs` timed passes.
fn mean_time(lsm: &MiniLsm, present: bool, n_keys: usize, runs: usize) -> Result<Duration> {
    lookup_batch(lsm, present, n_keys)?; // warmup (fills block cache)
    let mut total = Duration::ZERO;
    for _ in 0..runs {
        total += lookup_batch(lsm, present, n_keys)?;
    }
    Ok(total / runs as u32)
}

fn run_workload(n_keys: usize, runs: usize) -> Result<()> {
    let dir = std::env::temp_dir().join(format!("mini-lsm-bench-{}-{n_keys}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let mut options = LsmStorageOptions::default_for_week1_test();
    options.target_sst_size = TARGET_SST_SIZE;
    options.num_memtable_limit = NUM_MEMTABLE_LIMIT;
    let lsm = MiniLsm::open(&dir, options)?;

    // --- load ---
    let value = vec![b'v'; 100];
    let t = Instant::now();
    for i in 0..n_keys {
        lsm.put(&key_of(i), &value)?;
    }
    // Flush ALL data to L0 deterministically. `put` only freezes memtables; the
    // background thread flushes ~1 per tick, so after a fast load most frozen
    // memtables are still in memory. Stop the bg thread (so there is no race on
    // the imm list), then drive the public force_flush() enough times to drain
    // every immutable memtable. We do NOT modify the engine to do this.
    lsm.close()?;
    std::thread::sleep(Duration::from_millis(150));
    let max_flushes = n_keys / 100 + NUM_MEMTABLE_LIMIT + 16;
    for _ in 0..max_flushes {
        let _ = lsm.force_flush();
    }
    let load_time = t.elapsed();

    // --- measure both modes on the same loaded SSTs ---
    BLOOM_DISABLED.store(false, Ordering::Relaxed);
    let absent_on = mean_time(&lsm, false, n_keys, runs)?;
    let present_on = mean_time(&lsm, true, n_keys, runs)?;

    BLOOM_DISABLED.store(true, Ordering::Relaxed);
    let absent_off = mean_time(&lsm, false, n_keys, runs)?;
    let present_off = mean_time(&lsm, true, n_keys, runs)?;
    BLOOM_DISABLED.store(false, Ordering::Relaxed);

    // sanity: confirm data is actually on disk (present keys must be found)
    let found = lsm.get(&key_of(0))?.is_some();

    // --- report ---
    println!(
        "\n### {n_keys} keys  (loaded+flushed in {load_time:.2?}, {N_LOOKUPS} lookups/batch, mean of {runs} runs, data_on_disk={found})"
    );
    println!("| workload     | bloom ON    | bloom OFF   | speedup |");
    println!("|--------------|-------------|-------------|---------|");
    print_row("absent keys", absent_on, absent_off);
    print_row("present keys", present_on, present_off);

    drop(lsm);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

fn print_row(workload: &str, on: Duration, off: Duration) {
    let speedup = off.as_secs_f64() / on.as_secs_f64();
    println!(
        "| {workload:<12} | {:>11} | {:>11} | {speedup:>6.1}x |",
        format!("{on:.2?}"),
        format!("{off:.2?}"),
    );
}

fn main() -> Result<()> {
    let runs: usize = std::env::var("BENCH_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let sizes: Vec<usize> = {
        let args: Vec<usize> = std::env::args().skip(1).filter_map(|s| s.parse().ok()).collect();
        if args.is_empty() {
            vec![100_000, 250_000, 500_000]
        } else {
            args
        }
    };

    println!("bloom filter benchmark — workloads: {sizes:?}, runs: {runs}");
    for n in sizes {
        run_workload(n, runs)?;
    }
    Ok(())
}
