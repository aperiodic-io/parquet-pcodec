//! JNI binding of `tparquet-core`: the native methods of `io.aperiodic.tparquet.Pcodec`
//! (`java/tparquet-jni`). The Java class checks its arguments first; these functions check
//! them again (a range outside an array or buffer is an `ArrayIndexOutOfBoundsException`,
//! never a read or write out of bounds), and no panic crosses into the JVM: it becomes an
//! `IllegalStateException`.

use jni::JNIEnv;
use jni::objects::{JByteArray, JByteBuffer, JClass, JObject, JThrowable, JValue};
use jni::sys::{jboolean, jbyteArray, jint};
use std::panic::{AssertUnwindSafe, catch_unwind};
use tparquet_core::{self as core, Error, Mode, Options, PhysicalType};

/// The ABI version `NativeLibrary.ABI_VERSION` expects.
pub const ABI_VERSION: jint = 1;

const EXCEPTION: &str = "io/aperiodic/tparquet/PcodecException";
const OUT_OF_BOUNDS: &str = "java/lang/ArrayIndexOutOfBoundsException";
const ILLEGAL_ARGUMENT: &str = "java/lang/IllegalArgumentException";
const ILLEGAL_STATE: &str = "java/lang/IllegalStateException";

/// Why a native method did not return normally.
#[derive(Debug)]
enum Failure {
    /// A range outside its array or buffer.
    Bounds(String),
    /// An argument the codec refuses (a level above 12, a destination too small).
    Argument(String),
    /// A payload error: `PcodecException` with code 2 (corrupt) or 3 (size).
    Codec(jint, String),
    /// A JNI call failed; when it raised a Java exception, that exception stays pending.
    Jni(jni::errors::Error),
}

#[cfg(test)]
impl PartialEq for Failure {
    fn eq(&self, other: &Self) -> bool {
        format!("{self:?}") == format!("{other:?}")
    }
}

impl From<jni::errors::Error> for Failure {
    fn from(e: jni::errors::Error) -> Self {
        Failure::Jni(e)
    }
}

/// The `PcodecException` code of a core error (as `rust/capi`'s return codes).
fn code(e: &Error) -> jint {
    match e {
        Error::SizeMismatch { .. } | Error::NotMultipleOfWidth { .. } => 3,
        _ => 2,
    }
}

fn codec(e: Error) -> Failure {
    match e {
        Error::InvalidLevel(_) => Failure::Argument(e.to_string()),
        e => Failure::Codec(code(&e), e.to_string()),
    }
}

/// `(off, len)` as a range within `size` elements.
fn range(off: jint, len: jint, size: usize) -> Result<std::ops::Range<usize>, Failure> {
    let bad = || Failure::Bounds(format!("range [{off}, {off} + {len}) outside 0..{size}"));
    let (o, l) = (usize::try_from(off).map_err(|_| bad())?, usize::try_from(len).map_err(|_| bad())?);
    match o.checked_add(l) {
        Some(end) if end <= size => Ok(o..end),
        _ => Err(bad()),
    }
}

/// Throws the Java exception of a failure (none for a JNI error that left one pending).
fn throw(env: &mut JNIEnv, failure: Failure) {
    let _ = match failure {
        Failure::Bounds(msg) => env.throw_new(OUT_OF_BOUNDS, msg),
        Failure::Argument(msg) => env.throw_new(ILLEGAL_ARGUMENT, msg),
        Failure::Codec(code, msg) => throw_codec(env, code, &msg),
        Failure::Jni(jni::errors::Error::JavaException) => Ok(()),
        Failure::Jni(e) => env.throw_new(ILLEGAL_STATE, format!("tparquet JNI: {e}")),
    };
}

fn throw_codec(env: &mut JNIEnv, code: jint, msg: &str) -> jni::errors::Result<()> {
    let msg = env.new_string(msg)?;
    let e = env.new_object(EXCEPTION, "(ILjava/lang/String;)V", &[JValue::Int(code), JValue::Object(&msg)])?;
    env.throw(JThrowable::from(e))
}

/// Runs `f`; a failure or a panic becomes a Java exception and the method returns `fallback`.
fn run<'l, T>(env: &mut JNIEnv<'l>, fallback: T, f: impl FnOnce(&mut JNIEnv<'l>) -> Result<T, Failure>) -> T {
    match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(v)) => v,
        Ok(Err(failure)) => {
            throw(env, failure);
            fallback
        }
        Err(_) => {
            // a pending exception would make the next JNI call undefined behaviour
            let _ = env.exception_clear();
            let _ = env.throw_new(ILLEGAL_STATE, "internal panic in tparquet");
            fallback
        }
    }
}

/// `array[off, off + len)`, copied out of the JVM.
fn read(env: &JNIEnv, array: &JByteArray, off: jint, len: jint) -> Result<Vec<u8>, Failure> {
    let size = env.get_array_length(array)? as usize;
    let r = range(off, len, size)?;
    let mut buf = vec![0i8; r.len()];
    env.get_byte_array_region(array, off, &mut buf)?;
    Ok(buf.into_iter().map(|b| b as u8).collect())
}

/// The bytes `[pos, pos + len)` of a direct buffer.
fn direct<'b>(env: &JNIEnv, buf: &JByteBuffer, pos: jint, len: jint) -> Result<&'b mut [u8], Failure> {
    let addr = env.get_direct_buffer_address(buf)?;
    let r = range(pos, len, env.get_direct_buffer_capacity(buf)?)?;
    if r.is_empty() {
        return Ok(&mut []);
    }
    // SAFETY: a direct buffer's address points at `capacity` bytes that stay valid while the
    // buffer is reachable (the caller holds it for the duration of the call); `r` is within it
    Ok(unsafe { std::slice::from_raw_parts_mut(addr.add(r.start), r.len()) })
}

/// Refuses a source and a destination that share bytes: `src` would alias `dst`.
fn disjoint(src: &[u8], dst: &[u8]) -> Result<(), Failure> {
    let (s, d) = (src.as_ptr_range(), dst.as_ptr_range());
    if !src.is_empty() && !dst.is_empty() && s.start < d.end && d.start < s.end {
        return Err(Failure::Argument("source and destination overlap".into()));
    }
    Ok(())
}

fn options(physical_type: jint, level: jint, decimal: jboolean) -> Result<Options, Failure> {
    let level =
        u8::try_from(level).map_err(|_| Failure::Argument(format!("compression level {level} outside 0..12")))?;
    Ok(Options { level, hint: PhysicalType::from_parquet(physical_type), decimal: decimal != 0 })
}

/// `PCP_ABI_VERSION` of this library.
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_aperiodic_tparquet_Pcodec_abiVersion0(_env: JNIEnv, _class: JClass) -> jint {
    ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_aperiodic_tparquet_Pcodec_compress0<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass<'l>,
    src: JByteArray<'l>,
    off: jint,
    len: jint,
    physical_type: jint,
    level: jint,
    decimal: jboolean,
) -> jbyteArray {
    run(&mut env, JObject::null().into_raw(), |env| {
        let opts = options(physical_type, level, decimal)?;
        let src = read(env, &src, off, len)?;
        let payload = core::compress(&src, &opts).map_err(codec)?;
        Ok(env.byte_array_from_slice(&payload)?.into_raw())
    })
}

#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_aperiodic_tparquet_Pcodec_compressDirect0<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass<'l>,
    src: JByteBuffer<'l>,
    src_pos: jint,
    src_len: jint,
    physical_type: jint,
    level: jint,
    decimal: jboolean,
    dst: JByteBuffer<'l>,
    dst_pos: jint,
    dst_cap: jint,
) -> jint {
    run(&mut env, 0, |env| {
        let opts = options(physical_type, level, decimal)?;
        let src = direct(env, &src, src_pos, src_len)?;
        let dst = direct(env, &dst, dst_pos, dst_cap)?;
        disjoint(src, dst)?;
        let bound = src.len() + 2;
        if dst.len() < bound {
            return Err(Failure::Argument(format!("destination has {} bytes, needs {bound}", dst.len())));
        }
        let payload = core::compress(src, &opts).map_err(codec)?;
        dst[..payload.len()].copy_from_slice(&payload);
        Ok(payload.len() as jint)
    })
}

#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_aperiodic_tparquet_Pcodec_decompress0<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass<'l>,
    src: JByteArray<'l>,
    off: jint,
    len: jint,
    dst: JByteArray<'l>,
    dst_off: jint,
    dst_len: jint,
) {
    run(&mut env, (), |env| {
        // the destination range first: its length bounds the allocation below
        range(dst_off, dst_len, env.get_array_length(&dst)? as usize)?;
        let src = read(env, &src, off, len)?;
        let out = core::decompress(&src, dst_len as usize).map_err(codec)?;
        // SAFETY: i8 and u8 have the same size and alignment
        let out: &[i8] = unsafe { std::slice::from_raw_parts(out.as_ptr().cast(), out.len()) };
        Ok(env.set_byte_array_region(&dst, dst_off, out)?)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_aperiodic_tparquet_Pcodec_decompressDirect0<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass<'l>,
    src: JByteBuffer<'l>,
    src_pos: jint,
    src_len: jint,
    dst: JByteBuffer<'l>,
    dst_pos: jint,
    dst_len: jint,
) {
    run(&mut env, (), |env| {
        let src = direct(env, &src, src_pos, src_len)?;
        let dst = direct(env, &dst, dst_pos, dst_len)?;
        disjoint(src, dst)?;
        core::decompress_into(src, dst).map_err(codec)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_aperiodic_tparquet_Pcodec_mode0<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass<'l>,
    src: JByteArray<'l>,
    off: jint,
    len: jint,
) -> jint {
    run(&mut env, -1, |env| {
        let src = read(env, &src, off, len)?;
        let mode: Mode = core::mode_of(&src).map_err(codec)?;
        Ok(mode as jint)
    })
}

/// Test hook (`java/tparquet-jni` tests, class `NativeTestHooks`): panics inside [`run`], to
/// show that a panic becomes an `IllegalStateException` and the JVM carries on.
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_aperiodic_tparquet_NativeTestHooks_panic0<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass<'l>,
) -> jint {
    run(&mut env, -1, |_| panic!("test hook"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(range(0, 0, 0), Ok(0..0));
        assert_eq!(range(2, 3, 5), Ok(2..5));
        assert_eq!(range(5, 0, 5), Ok(5..5));
        for (off, len, size) in [(-1, 1, 5), (0, -1, 5), (3, 3, 5), (6, 0, 5), (1, 5, 5)] {
            assert!(matches!(range(off, len, size), Err(Failure::Bounds(_))), "{off} {len} {size}");
        }
    }

    #[test]
    fn overlap() {
        let buf = [0u8; 10];
        assert_eq!(disjoint(&buf[0..5], &buf[5..10]), Ok(()));
        assert_eq!(disjoint(&buf[5..10], &buf[0..5]), Ok(()));
        assert_eq!(disjoint(&buf[0..0], &buf[0..10]), Ok(()));
        assert_eq!(disjoint(&buf[0..10], &buf[3..3]), Ok(()));
        for (a, b) in [(0..6, 5..10), (5..10, 0..6), (2..3, 0..10), (0..10, 9..10)] {
            assert!(matches!(disjoint(&buf[a.clone()], &buf[b.clone()]), Err(Failure::Argument(_))), "{a:?} {b:?}");
        }
    }

    #[test]
    fn codes() {
        assert_eq!(code(&Error::SizeMismatch { expected: 1, actual: 2 }), 3);
        assert_eq!(code(&Error::NotMultipleOfWidth { len: 1, width: 8 }), 3);
        assert_eq!(code(&Error::Truncated), 2);
        assert_eq!(
            codec(Error::InvalidLevel(13)),
            Failure::Argument("pcodec payload: compression level 13 above 12".into())
        );
        assert_eq!(
            codec(Error::BadException),
            Failure::Codec(2, "pcodec payload: exception count or position out of range".into())
        );
    }

    #[test]
    fn option_values() {
        assert_eq!(options(5, 12, 1).unwrap(), Options { level: 12, hint: Some(PhysicalType::Double), decimal: true });
        assert_eq!(options(-1, 0, 0).unwrap(), Options { level: 0, hint: None, decimal: false });
        assert!(matches!(options(1, 256, 0), Err(Failure::Argument(_))));
        assert!(matches!(options(1, -1, 0), Err(Failure::Argument(_))));
    }
}
