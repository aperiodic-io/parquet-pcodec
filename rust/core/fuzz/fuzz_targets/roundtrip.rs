//! Any bytes with any hint, level and decimal flag decode back to themselves.
#![no_main]
use libfuzzer_sys::fuzz_target;
use pparquet_core::{Options, PhysicalType, compress, decompress};

fuzz_target!(|data: &[u8]| {
    let Some((&ctl, src)) = data.split_first() else { return };
    let hint = [
        None,
        Some(PhysicalType::Int32),
        Some(PhysicalType::Int64),
        Some(PhysicalType::Float),
        Some(PhysicalType::Double),
    ][usize::from(ctl % 5)];
    let opts = Options { level: (ctl >> 3) % 13, hint, decimal: ctl & 0x80 != 0 };
    let p = compress(src, &opts).expect("valid options");
    assert_eq!(decompress(&p, src.len()).expect("own payload"), src);
});
