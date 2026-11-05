//! The C ABI, called the way C and Go call it.

use std::ptr;
use tparquet_capi::*;

fn compress(src: &[u8], t: i32, level: i32, decimal: i32) -> (i32, Vec<u8>) {
    let mut dst = vec![0u8; pcp_compress_bound(src.len())];
    let mut written = 0usize;
    let rc =
        unsafe { pcp_compress(src.as_ptr(), src.len(), t, level, decimal, dst.as_mut_ptr(), dst.len(), &mut written) };
    dst.truncate(written);
    (rc, dst)
}

fn decompress(p: &[u8], n: usize) -> (i32, Vec<u8>) {
    let mut dst = vec![0u8; n];
    let rc = unsafe { pcp_decompress(p.as_ptr(), p.len(), dst.as_mut_ptr(), n) };
    (rc, dst)
}

fn last_error() -> String {
    let mut buf = [0i8; 256];
    let n = unsafe { pcp_last_error(buf.as_mut_ptr().cast(), buf.len()) };
    let s: Vec<u8> = buf.iter().take_while(|c| **c != 0).map(|c| *c as u8).collect();
    assert_eq!(n, s.len());
    String::from_utf8(s).unwrap()
}

fn doubles(n: usize) -> Vec<u8> {
    (0..n).flat_map(|i| ((10_000 + i) as f64 / 100.0).to_le_bytes()).collect()
}

#[test]
fn constants() {
    assert_eq!(pcp_codec_id(), 20547);
    assert_eq!(pcp_abi_version(), 1);
    assert_eq!(pcp_compress_bound(0), 2);
    assert_eq!(pcp_compress_bound(1000), 1002);
}

#[test]
fn round_trip_every_type_and_none() {
    let src = doubles(10_000);
    for t in [-1, 0, 1, 2, 4, 5, 6, 99] {
        for (level, decimal) in [(-1, 1), (0, 0), (12, 1)] {
            let (rc, p) = compress(&src, t, level, decimal);
            assert_eq!(rc, PCP_OK);
            assert!(p.len() <= src.len() + 2);
            if t == 5 {
                assert!(p.len() < src.len() / 4, "type {t}: {}", p.len());
            }
            assert_eq!(decompress(&p, src.len()), (PCP_OK, src.clone()));
        }
    }
}

#[test]
fn hint_and_flags_reach_the_encoder() {
    let src = doubles(10_000);
    // payload byte 1 is the mode, byte 2 the dtype (spec section 4)
    assert_eq!(compress(&src, 5, -1, 1).1[1..3], [2, 5], "DOUBLE + decimal: DECIMAL");
    assert_eq!(compress(&src, 5, -1, 0).1[1..3], [1, 5], "DOUBLE, no decimal: PCO");
    assert_eq!(compress(&src, 2, -1, 1).1[1..3], [1, 2], "as INT64: PCO i64");
    assert_eq!(compress(&src, -1, -1, 1).1[1], 3, "no hint: ZSTD");
    assert_eq!(compress(&src, 3, -1, 1).1[1], 3, "INT96 is no hint");
    // the level changes the pcodec stream
    let ints: Vec<u8> = (0..20_000i64).flat_map(|i| ((i * 37) % 1000 + i / 7).to_le_bytes()).collect();
    assert_ne!(compress(&ints, 2, 0, 1).1, compress(&ints, 2, 12, 1).1);
    assert_eq!(compress(&ints, 2, -1, 1).1, compress(&ints, 2, 8, 1).1, "-1 is level 8");
}

#[test]
fn empty_input_with_null_pointers() {
    let mut dst = [0u8; 2];
    let mut written = 9usize;
    let rc = unsafe { pcp_compress(ptr::null(), 0, 5, -1, 1, dst.as_mut_ptr(), 2, &mut written) };
    assert_eq!((rc, written, dst), (PCP_OK, 2, [1, 0]));
    let rc = unsafe { pcp_decompress(dst.as_ptr(), 2, ptr::null_mut(), 0) };
    assert_eq!(rc, PCP_OK);
}

#[test]
fn argument_errors() {
    let src = doubles(10);
    let mut dst = vec![0u8; 100];
    let mut written = 0usize;
    let call = |src: *const u8, n: usize, level: i32, dst: *mut u8, cap: usize, w: *mut usize| unsafe {
        pcp_compress(src, n, 5, level, 1, dst, cap, w)
    };
    assert_eq!(call(src.as_ptr(), src.len(), 13, dst.as_mut_ptr(), 100, &mut written), PCP_ERR_ARGUMENT);
    assert!(last_error().contains("level 13"), "{}", last_error());
    assert_eq!(call(src.as_ptr(), src.len(), -2, dst.as_mut_ptr(), 100, &mut written), PCP_ERR_ARGUMENT);
    assert_eq!(call(ptr::null(), 8, -1, dst.as_mut_ptr(), 100, &mut written), PCP_ERR_ARGUMENT);
    assert!(last_error().contains("NULL"), "{}", last_error());
    assert_eq!(call(src.as_ptr(), src.len(), -1, ptr::null_mut(), 100, &mut written), PCP_ERR_ARGUMENT);
    assert_eq!(call(src.as_ptr(), src.len(), -1, dst.as_mut_ptr(), 100, ptr::null_mut()), PCP_ERR_ARGUMENT);
    // dst smaller than the bound, even when the payload would fit
    assert_eq!(call(src.as_ptr(), src.len(), -1, dst.as_mut_ptr(), src.len() + 1, &mut written), PCP_ERR_ARGUMENT);
    assert!(last_error().contains("82"), "the message names the bound: {}", last_error());
    assert_eq!(unsafe { pcp_decompress(ptr::null(), 3, dst.as_mut_ptr(), 1) }, PCP_ERR_ARGUMENT);
    assert_eq!(unsafe { pcp_decompress([1u8, 0, 7].as_ptr(), 3, ptr::null_mut(), 1) }, PCP_ERR_ARGUMENT);
}

#[test]
fn decode_errors_and_messages() {
    assert_eq!(decompress(&[9, 0], 0).0, PCP_ERR_CORRUPT);
    assert_eq!(last_error(), "pcodec payload: unsupported version 9");
    assert_eq!(decompress(&[1, 0, 1, 2], 3).0, PCP_ERR_SIZE);
    assert_eq!(last_error(), "pcodec payload: decodes to 2 bytes, expected 3");
    let mut p = vec![1, 1, 2];
    p.extend([1, 2, 3]);
    assert_eq!(decompress(&p, 15).0, PCP_ERR_SIZE, "not a multiple of the width is a size error");
    assert_eq!(decompress(&p, 16).0, PCP_ERR_CORRUPT);
    // success clears the message
    assert_eq!(decompress(&[1, 0, 5], 1), (PCP_OK, vec![5]));
    assert_eq!(last_error(), "");
}

#[test]
fn last_error_truncates_to_the_buffer() {
    assert_eq!(decompress(&[9, 0], 0).0, PCP_ERR_CORRUPT);
    let mut buf = [0x55i8; 8];
    let n = unsafe { pcp_last_error(buf.as_mut_ptr().cast(), buf.len()) };
    assert_eq!(n, "pcodec payload: unsupported version 9".len());
    let s: Vec<u8> = buf.iter().map(|c| *c as u8).collect();
    assert_eq!(&s, b"pcodec \0");
    // cap 0 writes nothing; NULL buffer with cap 0 is allowed
    assert_eq!(unsafe { pcp_last_error(ptr::null_mut(), 0) }, n);
    let mut one = [0x55i8; 1];
    unsafe { pcp_last_error(one.as_mut_ptr().cast(), 1) };
    assert_eq!(one, [0]);
}

#[test]
fn errors_are_per_thread() {
    assert_eq!(decompress(&[9, 0], 0).0, PCP_ERR_CORRUPT);
    std::thread::spawn(|| assert_eq!(last_error(), "")).join().unwrap();
    assert!(!last_error().is_empty());
}

#[test]
fn the_bound_saturates() {
    // a bound that wrapped would let a caller allocate a tiny buffer for a huge input
    assert_eq!(pcp_compress_bound(usize::MAX), usize::MAX);
    assert_eq!(pcp_compress_bound(usize::MAX - 1), usize::MAX);
    assert_eq!(pcp_compress_bound(usize::MAX - 2), usize::MAX);
    assert_eq!(pcp_compress_bound(usize::MAX - 3), usize::MAX - 1);
}

#[test]
fn lengths_above_isize_max_are_refused() {
    // no object can be that large; a negative Go or C length cast to size_t looks like this
    let src = [1u8, 0, 5];
    let mut dst = [0u8; 8];
    let mut written = 0usize;
    for huge in [isize::MAX as usize + 1, usize::MAX] {
        assert_eq!(unsafe { pcp_decompress(src.as_ptr(), huge, dst.as_mut_ptr(), 1) }, PCP_ERR_ARGUMENT);
        assert!(last_error().contains("isize::MAX"), "{}", last_error());
        assert_eq!(unsafe { pcp_decompress(src.as_ptr(), 3, dst.as_mut_ptr(), huge) }, PCP_ERR_ARGUMENT);
        let rc = unsafe { pcp_compress(src.as_ptr(), huge, -1, -1, 0, dst.as_mut_ptr(), 8, &mut written) };
        assert_eq!(rc, PCP_ERR_ARGUMENT);
        let rc = unsafe { pcp_compress(src.as_ptr(), 3, -1, -1, 0, dst.as_mut_ptr(), huge, &mut written) };
        assert_eq!(rc, PCP_ERR_ARGUMENT);
    }
    assert_eq!(written, 0, "written is only set on success");
}

#[test]
fn overlapping_buffers_are_refused() {
    // the header forbids nothing, and Rust may not alias them: refuse them
    let mut buf = vec![0u8; 64];
    buf[..2].copy_from_slice(&[1, 0]);
    let p = buf.as_mut_ptr();
    let mut written = 0usize;
    unsafe {
        // the same buffer, and partial overlaps from either side
        assert_eq!(pcp_decompress(p, 10, p, 8), PCP_ERR_ARGUMENT);
        assert!(last_error().contains("overlap"), "{}", last_error());
        assert_eq!(pcp_decompress(p.add(2), 10, p, 8), PCP_ERR_ARGUMENT);
        assert_eq!(pcp_decompress(p, 10, p.add(9), 8), PCP_ERR_ARGUMENT);
        assert_eq!(pcp_compress(p, 10, -1, -1, 0, p.add(5), 20, &mut written), PCP_ERR_ARGUMENT);
        assert_eq!(pcp_compress(p.add(20), 10, -1, -1, 0, p, 21, &mut written), PCP_ERR_ARGUMENT);
        // adjacent is fine
        assert_eq!(pcp_decompress(p, 10, p.add(10), 8), PCP_OK);
        assert_eq!(pcp_compress(p, 10, -1, -1, 0, p.add(10), 12, &mut written), PCP_OK);
        assert_eq!(pcp_compress(p.add(12), 10, -1, -1, 0, p, 12, &mut written), PCP_OK);
        // an empty buffer overlaps nothing
        assert_eq!(pcp_decompress(p, 2, p, 0), PCP_OK);
    }
}

#[test]
fn calls_during_thread_teardown_do_not_abort() {
    // a thread-local destructor (C: a pthread key destructor) that calls the library after the
    // library's own thread-local error is gone
    use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
    static RC: AtomicI32 = AtomicI32::new(-1);
    static LEN: AtomicUsize = AtomicUsize::new(usize::MAX);
    struct CallOnExit;
    impl Drop for CallOnExit {
        fn drop(&mut self) {
            let rc = unsafe { pcp_decompress([9u8, 0].as_ptr(), 2, ptr::null_mut(), 0) };
            RC.store(rc, Ordering::SeqCst);
            let mut buf = [0x55i8; 4];
            LEN.store(unsafe { pcp_last_error(buf.as_mut_ptr().cast(), 4) }, Ordering::SeqCst);
            assert_eq!(buf[0], 0, "an empty message");
        }
    }
    thread_local! { static ON_EXIT: CallOnExit = const { CallOnExit }; }
    std::thread::spawn(|| {
        // registered first, so destroyed after the library's error (destructors run in reverse)
        ON_EXIT.with(|_| {});
        assert_eq!(decompress(&[9, 0], 0).0, PCP_ERR_CORRUPT);
    })
    .join()
    .unwrap();
    assert_eq!(RC.load(Ordering::SeqCst), PCP_ERR_CORRUPT, "the call still works");
    assert_eq!(LEN.load(Ordering::SeqCst), 0, "with no message to keep");
}
