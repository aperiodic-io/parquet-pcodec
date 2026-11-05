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

/// a ZSTD payload: one frame without a content size, declaring a window of 2^`window_log`
/// bytes, holding one raw block of `data`
fn zstd_frame(window_log: u8, data: &[u8]) -> Vec<u8> {
    let mut p = vec![1, 3, 0x28, 0xB5, 0x2F, 0xFD, 0x00, (window_log - 10) << 3];
    let block = 1 | (data.len() as u32) << 3; // last block, raw
    p.extend(&block.to_le_bytes()[..3]);
    p.extend(data);
    p
}

#[test]
fn zstd_frames_decode_whatever_window_they_declare() {
    // RFC 8878 allows windows up to 2^41; one frame decoded straight into the page needs none
    for log in [10, 20, 27, 28, 30, 31] {
        assert_eq!(decompress(&zstd_frame(log, b"page"), 4).as_deref(), Ok(&b"page"[..]), "window 2^{log}");
    }
    // and the expected size still bounds the output
    let p = zstd_frame(31, b"page");
    assert_eq!(decompress(&p, 3), Err(Error::SizeMismatch { expected: 3, actual: 4 }));
    assert_eq!(decompress(&p, 5), Err(Error::SizeMismatch { expected: 5, actual: 4 }));
}

#[test]
fn a_zstd_body_needs_a_frame() {
    // spec 4.4: one frame, or several; zero frames is not a body, even for an empty page
    assert_eq!(decompress(&[1, 3], 0), Err(Error::Truncated));
    assert_eq!(decompress(&[1, 3], 5), Err(Error::Truncated));
    // a skippable frame is a frame (RFC 8878 3.1.2): an empty page
    let skippable = [1, 3, 0x50, 0x2A, 0x4D, 0x18, 2, 0, 0, 0, 0xAA, 0xBB];
    assert_eq!(decompress(&skippable, 0), Ok(vec![]));
    assert_eq!(decompress(&skippable, 1), Err(Error::SizeMismatch { expected: 1, actual: 0 }));
}
