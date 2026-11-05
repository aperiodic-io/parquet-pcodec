//! Payloads and sizes as an attacker chooses them (spec section 7): the decoder returns an
//! error, never panics, and never accepts what the spec does not allow.

mod common;
use common::*;
use tparquet_core::{Error, PhysicalType, decompress};

#[test]
fn an_unallocatable_expected_size_is_an_error_not_a_panic() {
    // a page header can claim any size; `decompress` allocates it, and must fail cleanly
    let pco = pco_payload(PhysicalType::Int64, &i64s(&[1, 2, 3]));
    for n in [usize::MAX, usize::MAX - 1, isize::MAX as usize + 1, isize::MAX as usize, 1 << 62] {
        for p in [&[1u8, 0][..], &[1, 0, 7], &pco, &[1, 3, 0x28, 0xB5, 0x2F, 0xFD]] {
            assert_eq!(decompress(p, n), Err(Error::AllocationFailed(n)), "{n} {p:?}");
        }
    }
    assert_eq!(Error::AllocationFailed(7).to_string(), "pcodec payload: cannot allocate the expected size of 7 bytes");
    // a malformed header is reported before anything is allocated
    assert_eq!(decompress(&[], usize::MAX), Err(Error::Truncated));
    assert_eq!(decompress(&[3, 0], usize::MAX), Err(Error::UnsupportedVersion(3)));
    assert_eq!(decompress(&[1, 9], usize::MAX), Err(Error::UnknownMode(9)));
}
