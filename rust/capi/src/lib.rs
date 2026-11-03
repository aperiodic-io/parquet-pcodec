//! C ABI over `parquet-pcodec-core`; the contract is `include/parquet_pcodec.h`.
#![allow(clippy::missing_safety_doc)] // the header documents the contract

use parquet_pcodec_core::{self as core, Error, Options, PhysicalType};
use std::cell::RefCell;
use std::ffi::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub const PCP_OK: i32 = 0;
pub const PCP_ERR_ARGUMENT: i32 = 1;
pub const PCP_ERR_CORRUPT: i32 = 2;
pub const PCP_ERR_SIZE: i32 = 3;
pub const PCP_ERR_PANIC: i32 = 4;

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

fn set_error(msg: String) {
    LAST_ERROR.with(|e| *e.borrow_mut() = msg);
}

/// Runs `f`, records its error message (or clears it), and maps it to a return code.
fn guarded(f: impl FnOnce() -> Result<(), (i32, String)>) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => {
            set_error(String::new());
            PCP_OK
        }
        Ok(Err((code, msg))) => {
            set_error(msg);
            code
        }
        Err(_) => {
            set_error("internal panic in parquet-pcodec".into());
            PCP_ERR_PANIC
        }
    }
}

fn code(e: &Error) -> i32 {
    match e {
        Error::SizeMismatch { .. } | Error::NotMultipleOfWidth { .. } => PCP_ERR_SIZE,
        Error::InvalidLevel(_) => PCP_ERR_ARGUMENT,
        _ => PCP_ERR_CORRUPT,
    }
}

fn arg(msg: &str) -> (i32, String) {
    (PCP_ERR_ARGUMENT, msg.to_string())
}

/// A slice from a C pointer and length; NULL is only allowed for length 0.
unsafe fn slice<'a>(p: *const u8, len: usize) -> Result<&'a [u8], (i32, String)> {
    match (p.is_null(), len) {
        (_, 0) => Ok(&[]),
        (true, _) => Err(arg("NULL pointer with a non-zero length")),
        // SAFETY: the caller guarantees `p` points at `len` readable bytes (header contract)
        (false, _) => Ok(unsafe { std::slice::from_raw_parts(p, len) }),
    }
}

unsafe fn slice_mut<'a>(p: *mut u8, len: usize) -> Result<&'a mut [u8], (i32, String)> {
    match (p.is_null(), len) {
        (_, 0) => Ok(&mut []),
        (true, _) => Err(arg("NULL pointer with a non-zero length")),
        // SAFETY: the caller guarantees `p` points at `len` writable bytes (header contract)
        (false, _) => Ok(unsafe { std::slice::from_raw_parts_mut(p, len) }),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pcp_codec_id() -> i32 {
    core::CODEC_ID
}

#[unsafe(no_mangle)]
pub extern "C" fn pcp_abi_version() -> u32 {
    1
}

#[unsafe(no_mangle)]
pub extern "C" fn pcp_compress_bound(src_len: usize) -> usize {
    src_len + 2
}

#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn pcp_compress(
    src: *const u8,
    src_len: usize,
    physical_type: i32,
    level: i32,
    decimal: i32,
    dst: *mut u8,
    dst_cap: usize,
    written: *mut usize,
) -> i32 {
    guarded(|| {
        let src = unsafe { slice(src, src_len)? };
        if written.is_null() {
            return Err(arg("NULL `written`"));
        }
        let bound = pcp_compress_bound(src_len);
        if dst_cap < bound {
            return Err(arg(&format!("dst_cap {dst_cap} below the bound {bound}")));
        }
        let level = match level {
            -1 => core::DEFAULT_LEVEL,
            l => u8::try_from(l).map_err(|_| arg(&format!("pcodec payload: compression level {l} invalid")))?,
        };
        let opts = Options { level, hint: PhysicalType::from_parquet(physical_type), decimal: decimal != 0 };
        let p = core::compress(src, &opts).map_err(|e| (code(&e), e.to_string()))?;
        let dst = unsafe { slice_mut(dst, dst_cap)? };
        dst[..p.len()].copy_from_slice(&p);
        // SAFETY: checked non-NULL above
        unsafe { *written = p.len() };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pcp_decompress(src: *const u8, src_len: usize, dst: *mut u8, dst_len: usize) -> i32 {
    guarded(|| {
        let src = unsafe { slice(src, src_len)? };
        let dst = unsafe { slice_mut(dst, dst_len)? };
        core::decompress_into(src, dst).map_err(|e| (code(&e), e.to_string()))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pcp_last_error(buf: *mut c_char, cap: usize) -> usize {
    LAST_ERROR.with(|e| {
        let msg = e.borrow();
        if !buf.is_null() && cap > 0 {
            let n = msg.len().min(cap - 1);
            // SAFETY: the caller guarantees `buf` has `cap` writable bytes; n + 1 <= cap
            unsafe {
                std::ptr::copy_nonoverlapping(msg.as_ptr().cast::<c_char>(), buf, n);
                *buf.add(n) = 0;
            }
        }
        msg.len()
    })
}
