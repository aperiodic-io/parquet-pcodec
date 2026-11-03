//! Any payload and any expected size: the decoder returns Ok or Err, never panics or overreads.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    // first two bytes: expected size (bounded, as a page header bounds it)
    let expected = usize::from(u16::from_le_bytes([data[0], data[1]]));
    let _ = parquet_pcodec_core::decompress(&data[2..], expected);
});
