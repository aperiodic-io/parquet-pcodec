//! What the decoder allocates (spec section 7, AGENTS.md: no unbounded allocation). The output
//! buffer is the caller's (`decompress_into`); everything else the decoder allocates must stay
//! a small constant, whatever the page size and whatever the payload header claims.
//!
//! Allocations through the Rust global allocator are counted per thread, so tests running in
//! parallel do not see each other's. (zstd allocates with C malloc; see zstd_memory.rs.)

mod common;
use common::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use tparquet_core::{Error, Mode, Options, PhysicalType, compress, decompress_into, mode_of};

struct Counting;

// signed: a thread may free what another allocated
thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn grow(n: usize) {
    let _ = LIVE.try_with(|l| {
        let v = l.get() + n as isize;
        l.set(v);
        let _ = PEAK.try_with(|p| p.set(p.get().max(v)));
    });
}

fn shrink(n: usize) {
    let _ = LIVE.try_with(|l| l.set(l.get() - n as isize));
}

// SAFETY: forwards every call to the system allocator unchanged; only counts sizes
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        grow(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        grow(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        shrink(l.size());
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        grow(n);
        shrink(l.size());
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// the most `f` had allocated at once on this thread, beyond what was live before
fn peak_of<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let start = LIVE.with(Cell::get);
    PEAK.with(|p| p.set(start));
    let r = f();
    (r, (PEAK.with(Cell::get) - start) as usize)
}

/// what the decoder may allocate besides the output, whatever the page size: pcodec's batch
/// buffers, bins and ANS tables
const OVERHEAD: usize = 1 << 20;

/// decodes into a fresh buffer: (result, peak allocation, the buffer)
fn decode_peak(payload: &[u8], len: usize) -> (Result<(), Error>, usize, Vec<u8>) {
    let mut dst = vec![0u8; len];
    let (r, peak) = peak_of(|| decompress_into(payload, &mut dst));
    (r, peak, dst)
}

#[test]
fn the_counter_counts() {
    let (v, peak) = peak_of(|| std::hint::black_box(vec![0u8; 3 << 20]));
    assert!(peak >= 3 << 20, "{peak}");
    drop(v);
}

#[test]
fn pco_pages_decode_without_a_copy_of_the_page() {
    for t in TYPES {
        let src = smooth(t, 1 << 20, 3);
        let p = compress(&src, &Options { decimal: false, ..Options::with_hint(t) }).unwrap();
        assert_eq!(mode_of(&p), Ok(Mode::Pco));
        let (r, peak, out) = decode_peak(&p, src.len());
        assert_eq!(r, Ok(()));
        assert!(out == src, "{t:?}: decoded bytes differ");
        assert!(peak < OVERHEAD, "{t:?}: {peak} bytes for a {} byte page", src.len());
    }
}

#[test]
fn decimal_pages_decode_without_a_copy_of_the_page() {
    let mut d: Vec<f64> = (0..1 << 20).map(|i| (i % 50_000) as f64 / 100.0).collect();
    for i in (5..d.len()).step_by(1_000) {
        d[i] = f64::from_bits(0x7FF8_0000_0000_0000 | i as u64);
    }
    let f: Vec<f32> = d.iter().map(|x| *x as f32).collect();
    for (t, src) in [(PhysicalType::Double, f64s(&d)), (PhysicalType::Float, f32s(&f))] {
        let p = compress(&src, &Options::with_hint(t)).unwrap();
        assert_eq!(mode_of(&p), Ok(Mode::Decimal), "{t:?}");
        let (r, peak, out) = decode_peak(&p, src.len());
        assert_eq!(r, Ok(()));
        assert!(out == src, "{t:?}: decoded bytes differ");
        assert!(peak < OVERHEAD, "{t:?}: {peak} bytes for a {} byte page", src.len());
    }
}

#[test]
fn a_lying_exception_count_allocates_nothing_for_it() {
    // k = n = 2^20 exceptions claimed by a 9 byte payload
    for (dtype, width) in [(4u8, 4usize), (5, 8)] {
        let n = 1u64 << 20;
        let mut p = vec![1, 2, dtype, 0];
        varint(n, &mut p);
        let (r, peak, _) = decode_peak(&p, n as usize * width);
        assert_eq!(r, Err(Error::Truncated));
        assert!(peak < 4096, "dtype {dtype}: {peak} bytes");
    }
}

#[test]
fn a_lying_chunk_count_allocates_nothing_for_it() {
    // a pcodec chunk of 2^20 values decoded into a page of 8 values
    let p = pco_payload(PhysicalType::Int64, &i64s(&vec![7; 1 << 20]));
    let (r, peak, _) = decode_peak(&p, 64);
    assert!(matches!(r, Err(Error::SizeMismatch { expected: 64, .. })), "{r:?}");
    assert!(peak < OVERHEAD, "{peak} bytes");
}

/// the most any single-bit flip in the first 128 bytes of pcodec files written in Dict mode and
/// with lookback deltas makes the decoder allocate
fn worst_bit_flip() -> usize {
    use pco::standalone::simple_compress;
    use pco::{ChunkConfig, DeltaSpec, ModeSpec};
    let dict: Vec<i64> = (0..5_000).map(|i| [5, 77, 1 << 40, -3][i % 4]).collect();
    let lookback: Vec<i64> = (0..5_000).map(|i| (i % 97 * (i % 13)) as i64).collect();
    let mut worst = 0;
    for (v, cfg) in [
        (dict, ChunkConfig::default().with_mode_spec(ModeSpec::TryDict)),
        (lookback, ChunkConfig::default().with_delta_spec(DeltaSpec::TryLookback)),
    ] {
        let mut p = vec![1, 1, 2];
        p.extend(simple_compress(&v, &cfg).unwrap());
        for bit in 0..p.len().min(128) * 8 {
            let mut q = p.clone();
            q[bit / 8] ^= 1 << (bit % 8);
            worst = worst.max(decode_peak(&q, v.len() * 8).1);
        }
    }
    worst
}

/// pcodec 1.0.4 sizes two buffers from chunk header fields without checking them against the
/// chunk's value count: a Dict mode dictionary (up to 2^25 entries) and a lookback window (up
/// to 2^24 values, twice). Both are valid pcodec, so tparquet-core cannot refuse them, and a
/// lying header makes the decoder allocate up to about 256 MB per decode, whatever the page.
/// This pins that ceiling (it is pcodec's, not the payload's); the ignored test below is the
/// bound to reach once pcodec checks the fields.
#[test]
fn pcodec_header_fields_cost_at_most_pcodecs_own_caps() {
    let worst = worst_bit_flip();
    assert!(worst < 1 << 29, "{worst} bytes");
}

#[test]
#[ignore = "pcodec 1.0.4 sizes Dict and lookback buffers from chunk header fields (see above)"]
fn pcodec_header_fields_cost_no_more_than_the_page() {
    let worst = worst_bit_flip();
    assert!(worst < OVERHEAD, "{worst} bytes for a 40000 byte page");
}
