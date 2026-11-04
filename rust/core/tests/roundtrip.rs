//! Round trips and the encoder's mode choice, through the public API only.

use tparquet_core::{Error, Mode, Options, PhysicalType, compress, decompress, decompress_into, mode_of};

fn le<T: Copy, const W: usize>(vals: &[T], f: fn(T) -> [u8; W]) -> Vec<u8> {
    vals.iter().flat_map(|v| f(*v)).collect()
}

fn f64s(v: &[f64]) -> Vec<u8> {
    le(v, f64::to_le_bytes)
}

fn f32s(v: &[f32]) -> Vec<u8> {
    le(v, f32::to_le_bytes)
}

fn i64s(v: &[i64]) -> Vec<u8> {
    le(v, i64::to_le_bytes)
}

fn i32s(v: &[i32]) -> Vec<u8> {
    le(v, i32::to_le_bytes)
}

fn hint(t: PhysicalType) -> Options {
    Options { hint: Some(t), ..Options::default() }
}

/// encodes, checks the round trip both ways (owned and into a buffer), returns the mode used
fn roundtrip(src: &[u8], opts: &Options) -> (Mode, usize) {
    let payload = compress(src, opts).unwrap();
    assert_eq!(decompress(&payload, src.len()).unwrap(), src);
    let mut dst = vec![0xAB; src.len()];
    decompress_into(&payload, &mut dst).unwrap();
    assert_eq!(dst, src);
    (mode_of(&payload).unwrap(), payload.len())
}

/// a deterministic pseudo-random stream (xorshift), so tests need no RNG crate
fn noise(n: usize, mut s: u64) -> Vec<u64> {
    (0..n)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        })
        .collect()
}

#[test]
fn empty_input_is_raw_and_two_bytes() {
    for opts in [Options::default(), hint(PhysicalType::Double)] {
        let p = compress(&[], &opts).unwrap();
        assert_eq!(p, vec![1, 0]);
        assert_eq!(decompress(&p, 0).unwrap(), Vec::<u8>::new());
    }
}

#[test]
fn integers_use_pco() {
    let ts: Vec<i64> = (0..50_000).map(|i| 1_700_000_000_000 + i * 7 + (i % 3)).collect();
    let src = i64s(&ts);
    let (mode, len) = roundtrip(&src, &hint(PhysicalType::Int64));
    assert_eq!(mode, Mode::Pco);
    assert!(len * 20 < src.len(), "{len} vs {}", src.len());

    let small: Vec<i32> = (0..10_000).map(|i| (i % 100) - 50).collect();
    assert_eq!(roundtrip(&i32s(&small), &hint(PhysicalType::Int32)).0, Mode::Pco);
}

#[test]
fn decimal_prices_use_decimal_and_beat_pco() {
    let prices: Vec<f64> = (0..20_000).map(|i| (10_000 + (i * 37) % 500) as f64 / 100.0).collect();
    let src = f64s(&prices);
    let (mode, len) = roundtrip(&src, &hint(PhysicalType::Double));
    assert_eq!(mode, Mode::Decimal);
    let pco_only = compress(&src, &Options { decimal: false, ..hint(PhysicalType::Double) }).unwrap();
    assert_eq!(mode_of(&pco_only).unwrap(), Mode::Pco);
    assert!(len < pco_only.len(), "decimal {len} vs pco {}", pco_only.len());

    let p32: Vec<f32> = (0..20_000).map(|i| (1_000 + (i * 13) % 300) as f32 / 10.0).collect();
    assert_eq!(roundtrip(&f32s(&p32), &hint(PhysicalType::Float)).0, Mode::Decimal);
}

#[test]
fn decimal_keeps_rare_exceptions_exact() {
    let mut v: Vec<f64> = (0..10_000).map(|i| i as f64 * 0.25).collect();
    // 1 in 200 values is not a short decimal, NaN payloads and signed zero included
    v[3] = f64::from_bits(0x7FF8_0000_DEAD_BEEF);
    v[100] = -0.0;
    v[200] = f64::INFINITY;
    v[300] = std::f64::consts::PI;
    v[9_999] = f64::NEG_INFINITY;
    let (mode, _) = roundtrip(&f64s(&v), &hint(PhysicalType::Double));
    assert_eq!(mode, Mode::Decimal);
}

#[test]
fn random_floats_fall_back_to_pco() {
    let v: Vec<f64> = noise(10_000, 7).iter().map(|x| (*x >> 11) as f64 / (1u64 << 53) as f64).collect();
    let (mode, _) = roundtrip(&f64s(&v), &hint(PhysicalType::Double));
    assert_eq!(mode, Mode::Pco);
}

#[test]
fn float_bits_survive_pco_mode() {
    let specials = [
        0.0,
        -0.0,
        f64::NAN,
        -f64::NAN,
        f64::from_bits(0x7FF0_0000_0000_0001), // signalling NaN
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN_POSITIVE,
        f64::from_bits(1), // smallest subnormal
        f64::MAX,
        f64::MIN,
    ];
    let v: Vec<f64> = specials.iter().cycle().take(5_000).copied().collect();
    let opts = Options { decimal: false, ..hint(PhysicalType::Double) };
    roundtrip(&f64s(&v), &opts);
    let w: Vec<f32> = v.iter().map(|x| *x as f32).chain([f32::from_bits(0x7F80_0001)]).collect();
    roundtrip(&f32s(&w), &Options { decimal: false, ..hint(PhysicalType::Float) });
}

#[test]
fn no_hint_uses_zstd_on_compressible_bytes() {
    let src: Vec<u8> = b"bid,ask,bid,ask,".iter().cycle().take(64 * 1024).copied().collect();
    let (mode, len) = roundtrip(&src, &Options::default());
    assert_eq!(mode, Mode::Zstd);
    assert!(len < 1_000);
}

#[test]
fn width_that_does_not_divide_the_length_uses_zstd() {
    // a V1 page with levels in front of the values: not an array of the hinted type
    let mut src = vec![3u8, 0, 0, 0, 0x02, 0x01, 0x01];
    src.extend(i64s(&(0..1_000).collect::<Vec<_>>()));
    assert_eq!(src.len() % 8, 7);
    assert_eq!(roundtrip(&src, &hint(PhysicalType::Int64)).0, Mode::Zstd);
}

#[test]
fn incompressible_bytes_are_raw() {
    let src: Vec<u8> = noise(4_096, 99).iter().flat_map(|x| x.to_le_bytes()).collect();
    for opts in [Options::default(), hint(PhysicalType::Int64), hint(PhysicalType::Double)] {
        let (mode, len) = roundtrip(&src, &opts);
        assert_eq!(mode, Mode::Raw);
        assert_eq!(len, src.len() + 2);
    }
}

#[test]
fn every_level_round_trips_and_13_is_rejected() {
    let src = i64s(&(0..5_000).map(|i| i * i).collect::<Vec<_>>());
    for level in 0..=12 {
        roundtrip(&src, &Options { level, ..hint(PhysicalType::Int64) });
    }
    assert!(matches!(
        compress(&src, &Options { level: 13, ..hint(PhysicalType::Int64) }),
        Err(Error::InvalidLevel(13))
    ));
}

#[test]
fn higher_level_is_not_bigger_on_structured_data() {
    let src = i64s(&(0..50_000).map(|i| (i * 31) % 1_000 + i / 10).collect::<Vec<_>>());
    let lo = compress(&src, &Options { level: 0, ..hint(PhysicalType::Int64) }).unwrap();
    let hi = compress(&src, &Options { level: 12, ..hint(PhysicalType::Int64) }).unwrap();
    assert!(hi.len() <= lo.len(), "{} > {}", hi.len(), lo.len());
}

#[test]
fn default_options() {
    let o = Options::default();
    assert_eq!(o.level, 8);
    assert_eq!(o.hint, None);
    assert!(o.decimal);
    assert_eq!(Options::with_hint(PhysicalType::Float), hint(PhysicalType::Float));
}

#[test]
fn a_payload_one_byte_under_raw_is_kept() {
    // near-random integers of growing width: somewhere pcodec lands within 2 bytes of RAW, and
    // the encoder must still keep it, because it is smaller
    let mut s = 1u64;
    let mut near = 0;
    for n in [64usize, 128, 256] {
        for bits in 40..64 {
            let src: Vec<u8> = (0..n)
                .flat_map(|_| {
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    (s >> (64 - bits)).to_le_bytes()
                })
                .collect();
            let p = compress(&src, &hint(PhysicalType::Int64)).unwrap();
            assert!(p.len() <= src.len() + 2);
            if p.len() + 2 >= src.len() && p.len() < src.len() + 2 {
                assert_eq!(mode_of(&p).unwrap(), Mode::Pco, "n={n} bits={bits}");
                near += 1;
            }
        }
    }
    assert!(near > 0, "no input landed next to RAW; widen the search");
}
