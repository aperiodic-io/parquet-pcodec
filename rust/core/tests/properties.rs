//! Property tests: lossless on any bytes with any hint and level; the decoder never panics.

use parquet_pcodec_core::{Options, PhysicalType, compress, decompress};
use proptest::prelude::*;

fn any_hint() -> impl Strategy<Value = Option<PhysicalType>> {
    prop_oneof![
        Just(None),
        Just(Some(PhysicalType::Int32)),
        Just(Some(PhysicalType::Int64)),
        Just(Some(PhysicalType::Float)),
        Just(Some(PhysicalType::Double)),
    ]
}

/// decimal-looking doubles with occasional arbitrary bit patterns: the DECIMAL path's inputs
fn decimalish() -> impl Strategy<Value = Vec<u8>> {
    (0u8..6, prop::collection::vec((any::<i32>(), any::<u64>(), 0u8..100), 0..3_000)).prop_map(|(e, v)| {
        v.into_iter()
            .flat_map(|(q, bits, roll)| {
                let x = if roll == 0 { f64::from_bits(bits) } else { q as f64 / 10f64.powi(e as i32) };
                x.to_le_bytes()
            })
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 400, ..ProptestConfig::default() })]

    #[test]
    fn lossless_on_any_bytes(src in prop::collection::vec(any::<u8>(), 0..4_096), hint in any_hint(), level in 0u8..=12, decimal: bool) {
        let p = compress(&src, &Options { level, hint, decimal }).unwrap();
        prop_assert_eq!(decompress(&p, src.len()).unwrap(), src);
    }

    #[test]
    fn lossless_on_decimal_doubles(src in decimalish(), level in 0u8..=12) {
        let p = compress(&src, &Options { level, hint: Some(PhysicalType::Double), decimal: true }).unwrap();
        prop_assert_eq!(decompress(&p, src.len()).unwrap(), src.clone());
        // the same values read as f32 pairs are also lossless
        let p = compress(&src, &Options { level, hint: Some(PhysicalType::Float), decimal: true }).unwrap();
        prop_assert_eq!(decompress(&p, src.len()).unwrap(), src);
    }

    #[test]
    fn never_bigger_than_raw(src in prop::collection::vec(any::<u8>(), 0..2_048), hint in any_hint()) {
        let p = compress(&src, &Options { hint, ..Options::default() }).unwrap();
        prop_assert!(p.len() <= src.len() + 2);
    }

    #[test]
    fn decoder_never_panics(p in prop::collection::vec(any::<u8>(), 0..512), expected in 0usize..65_536) {
        let _ = decompress(&p, expected);
    }

    #[test]
    fn decoder_never_panics_on_valid_prefix(mode in 0u8..4, dtype in prop_oneof![Just(1u8), Just(2), Just(4), Just(5)],
                                            rest in prop::collection::vec(any::<u8>(), 0..512), expected in 0usize..65_536) {
        let mut p = vec![1, mode, dtype];
        p.extend(rest);
        let _ = decompress(&p, expected);
    }
}
