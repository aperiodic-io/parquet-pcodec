//! The PCODEC Parquet compression codec: the page payload of `spec/FORMAT.md`.
//!
//! [`compress`] turns the bytes Parquet would compress into a payload; [`decompress`] turns a
//! payload back into exactly those bytes. Decoding never needs the column type: the payload
//! says how it was written. The type hint given to [`compress`] only affects the ratio.

mod decimal;
mod varint;

use decimal::Float;
use pco::ChunkConfig;
use pco::data_types::Number;
use pco::standalone::{simple_compress, simple_decompress_into};
use std::fmt;

/// `CompressionCodec` value of PCODEC in the Parquet footer.
pub const CODEC_ID: i32 = 20547;
/// Payload format version this crate writes and reads.
pub const FORMAT_VERSION: u8 = 1;
/// Footer key-value pair writers add to files with PCODEC columns.
pub const KV_KEY: &str = "parquet.pcodec.version";
/// Value of [`KV_KEY`].
pub const KV_VALUE: &str = "1";
/// pcodec compression level used when none is given.
pub const DEFAULT_LEVEL: u8 = 8;
/// Highest pcodec compression level.
pub const MAX_LEVEL: u8 = 12;
/// ZSTD level of the fallback mode.
const ZSTD_LEVEL: i32 = 3;

/// The Parquet physical types PCODEC models, with their Parquet `Type` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PhysicalType {
    Int32 = 1,
    Int64 = 2,
    Float = 4,
    Double = 5,
}

impl PhysicalType {
    /// From a Parquet `Type` value; `None` for types PCODEC does not model.
    pub fn from_parquet(t: i32) -> Option<Self> {
        match t {
            1 => Some(Self::Int32),
            2 => Some(Self::Int64),
            4 => Some(Self::Float),
            5 => Some(Self::Double),
            _ => None,
        }
    }

    /// Bytes per value in Parquet `PLAIN` encoding.
    pub fn width(self) -> usize {
        match self {
            Self::Int32 | Self::Float => 4,
            Self::Int64 | Self::Double => 8,
        }
    }

    fn from_dtype(b: u8) -> Result<Self, Error> {
        Self::from_parquet(i32::from(b)).ok_or(Error::UnknownDtype(b))
    }
}

/// How a payload was written (spec section 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mode {
    Raw = 0,
    Pco = 1,
    Decimal = 2,
    Zstd = 3,
}

impl Mode {
    fn from_byte(b: u8) -> Result<Self, Error> {
        match b {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Pco),
            2 => Ok(Self::Decimal),
            3 => Ok(Self::Zstd),
            _ => Err(Error::UnknownMode(b)),
        }
    }
}

/// Encoder settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// pcodec compression level, 0..=12.
    pub level: u8,
    /// The column's physical type; without it the encoder can only use ZSTD or RAW.
    pub hint: Option<PhysicalType>,
    /// Try DECIMAL for float columns.
    pub decimal: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self { level: DEFAULT_LEVEL, hint: None, decimal: true }
    }
}

impl Options {
    /// Default options for a column of type `t`.
    pub fn with_hint(t: PhysicalType) -> Self {
        Self { hint: Some(t), ..Self::default() }
    }
}

/// Why a payload could not be decoded (or an option was refused).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    Truncated,
    UnsupportedVersion(u8),
    UnknownMode(u8),
    UnknownDtype(u8),
    BadExponent(u8),
    BadException,
    SizeMismatch { expected: usize, actual: usize },
    NotMultipleOfWidth { len: usize, width: usize },
    InvalidLevel(u8),
    Pco(String),
    Zstd(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("pcodec payload: ")?;
        match self {
            Self::Truncated => f.write_str("truncated"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported version {v}"),
            Self::UnknownMode(m) => write!(f, "unknown mode {m}"),
            Self::UnknownDtype(d) => write!(f, "unknown dtype {d}"),
            Self::BadExponent(e) => write!(f, "exponent {e} above 18"),
            Self::BadException => f.write_str("exception count or position out of range"),
            Self::SizeMismatch { expected, actual } => write!(f, "decodes to {actual} bytes, expected {expected}"),
            Self::NotMultipleOfWidth { len, width } => {
                write!(f, "expected size {len} is not a multiple of the value width {width}")
            }
            Self::InvalidLevel(l) => write!(f, "compression level {l} above {MAX_LEVEL}"),
            Self::Pco(e) => write!(f, "pcodec: {e}"),
            Self::Zstd(e) => write!(f, "zstd: {e}"),
        }
    }
}

impl std::error::Error for Error {}

// ---------------------------------------------------------------- encode

/// Encodes `src` as a payload.
pub fn compress(src: &[u8], opts: &Options) -> Result<Vec<u8>, Error> {
    if opts.level > MAX_LEVEL {
        return Err(Error::InvalidLevel(opts.level));
    }
    let raw = || [&[FORMAT_VERSION, Mode::Raw as u8][..], src].concat();
    if src.is_empty() {
        return Ok(raw());
    }
    let cfg = ChunkConfig::default().with_compression_level(usize::from(opts.level));
    let best = match opts.hint.filter(|t| src.len().is_multiple_of(t.width())) {
        Some(PhysicalType::Int32) => pco_payload(PhysicalType::Int32, &values(src, i32::from_le_bytes), &cfg)?,
        Some(PhysicalType::Int64) => pco_payload(PhysicalType::Int64, &values(src, i64::from_le_bytes), &cfg)?,
        Some(PhysicalType::Float) => float_payload(PhysicalType::Float, &values(src, f32::from_le_bytes), opts, &cfg)?,
        Some(PhysicalType::Double) => {
            float_payload(PhysicalType::Double, &values(src, f64::from_le_bytes), opts, &cfg)?
        }
        None => {
            let mut p = vec![FORMAT_VERSION, Mode::Zstd as u8];
            p.extend(zstd::bulk::compress(src, ZSTD_LEVEL).map_err(|e| Error::Zstd(e.to_string()))?);
            p
        }
    };
    Ok(if best.len() < src.len() + 2 { best } else { raw() })
}

fn values<T, const W: usize>(src: &[u8], f: fn([u8; W]) -> T) -> Vec<T> {
    src.chunks_exact(W).map(|c| f(c.try_into().expect("chunks_exact"))).collect()
}

fn pco_bytes<T: Number>(v: &[T], cfg: &ChunkConfig) -> Result<Vec<u8>, Error> {
    simple_compress(v, cfg).map_err(|e| Error::Pco(e.to_string()))
}

fn pco_payload<T: Number>(t: PhysicalType, v: &[T], cfg: &ChunkConfig) -> Result<Vec<u8>, Error> {
    let mut p = vec![FORMAT_VERSION, Mode::Pco as u8, t as u8];
    p.extend(pco_bytes(v, cfg)?);
    Ok(p)
}

fn float_payload<F: Float + Plain>(
    t: PhysicalType,
    v: &[F],
    opts: &Options,
    cfg: &ChunkConfig,
) -> Result<Vec<u8>, Error> {
    let plain = pco_payload(t, v, cfg)?;
    if !opts.decimal {
        return Ok(plain);
    }
    let Some(e) = decimal::exponent(v) else { return Ok(plain) };
    let Some((qs, exc)) = decimal::split(v, e) else { return Ok(plain) };
    let mut p = vec![FORMAT_VERSION, Mode::Decimal as u8, t as u8, e];
    varint::put(&mut p, exc.len() as u64);
    let mut next = 0;
    for &i in &exc {
        varint::put(&mut p, (i - next) as u64);
        next = i + 1;
    }
    for &i in &exc {
        p.extend_from_slice(&le_bytes(v[i]));
    }
    p.extend(pco_bytes(&qs, cfg)?);
    Ok(if p.len() < plain.len() { p } else { plain })
}

/// little-endian bytes of a float (4 or 8)
fn le_bytes<F: Plain>(v: F) -> Vec<u8> {
    let mut out = Vec::with_capacity(8);
    v.write_le(&mut out);
    out
}

/// Writing numbers back as Parquet `PLAIN` bytes.
trait WriteLe {
    fn write_le(self, out: &mut Vec<u8>);
}

macro_rules! write_le {
    ($($t:ty),*) => {$(
        impl WriteLe for $t {
            fn write_le(self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_le_bytes());
            }
        }
    )*};
}
write_le!(i32, i64, f32, f64);

/// pcodec numbers this crate writes as `PLAIN` bytes.
trait Plain: Number + WriteLe {}
impl<T: Number + WriteLe> Plain for T {}

// ---------------------------------------------------------------- decode

/// The mode of a payload, after checking its version.
pub fn mode_of(payload: &[u8]) -> Result<Mode, Error> {
    match payload {
        [] | [_] => Err(Error::Truncated),
        [v, ..] if *v != FORMAT_VERSION => Err(Error::UnsupportedVersion(*v)),
        [_, m, ..] => Mode::from_byte(*m),
    }
}

/// Decodes a payload that must produce exactly `expected_len` bytes.
pub fn decompress(payload: &[u8], expected_len: usize) -> Result<Vec<u8>, Error> {
    let mut out = vec![0; expected_len];
    decompress_into(payload, &mut out)?;
    Ok(out)
}

/// Decodes a payload into `dst`, which it must fill exactly.
pub fn decompress_into(payload: &[u8], dst: &mut [u8]) -> Result<(), Error> {
    let mode = mode_of(payload)?;
    let body = &payload[2..];
    match mode {
        Mode::Raw => {
            check(dst.len(), body.len())?;
            dst.copy_from_slice(body);
            Ok(())
        }
        Mode::Pco => {
            let (&dtype, body) = body.split_first().ok_or(Error::Truncated)?;
            match PhysicalType::from_dtype(dtype)? {
                PhysicalType::Int32 => pco_into::<i32>(body, dst, 4),
                PhysicalType::Int64 => pco_into::<i64>(body, dst, 8),
                PhysicalType::Float => pco_into::<f32>(body, dst, 4),
                PhysicalType::Double => pco_into::<f64>(body, dst, 8),
            }
        }
        Mode::Decimal => {
            let [dtype, e, ..] = *body else { return Err(Error::Truncated) };
            let t = PhysicalType::from_dtype(dtype)?;
            if e > decimal::MAX_EXPONENT {
                return Err(Error::BadExponent(e));
            }
            let body = &body[2..];
            match t {
                PhysicalType::Float => decimal_into::<f32>(body, e, dst),
                PhysicalType::Double => decimal_into::<f64>(body, e, dst),
                PhysicalType::Int32 | PhysicalType::Int64 => Err(Error::UnknownDtype(dtype)),
            }
        }
        Mode::Zstd => zstd_into(body, dst),
    }
}

/// Zstandard frames into exactly `dst`, reading at most one byte past it
fn zstd_into(body: &[u8], dst: &mut [u8]) -> Result<(), Error> {
    use std::io::Read;
    let zerr = |e: std::io::Error| Error::Zstd(e.to_string());
    let mut d = zstd::stream::read::Decoder::with_buffer(body).map_err(zerr)?;
    let mut filled = 0;
    while filled < dst.len() {
        match d.read(&mut dst[filled..]).map_err(zerr)? {
            0 => return Err(Error::SizeMismatch { expected: dst.len(), actual: filled }),
            r => filled += r,
        }
    }
    let mut extra = [0u8; 1];
    match d.read(&mut extra).map_err(zerr)? {
        0 => Ok(()),
        _ => Err(Error::SizeMismatch { expected: dst.len(), actual: dst.len() + 1 }),
    }
}

fn check(expected: usize, actual: usize) -> Result<(), Error> {
    if expected == actual { Ok(()) } else { Err(Error::SizeMismatch { expected, actual }) }
}

/// `len / width`, refusing a length that is not whole values
fn count(len: usize, width: usize) -> Result<usize, Error> {
    if len.is_multiple_of(width) { Ok(len / width) } else { Err(Error::NotMultipleOfWidth { len, width }) }
}

/// exactly `n` pcodec numbers of type `T`; size errors in bytes of `width` per value
fn pco_numbers<T: Number>(src: &[u8], n: usize, width: usize) -> Result<Vec<T>, Error> {
    let mut nums = vec![T::default(); n];
    let progress = simple_decompress_into(src, &mut nums).map_err(|e| Error::Pco(e.to_string()))?;
    if !progress.finished {
        // more numbers than the page has room for: the decoder stops one value past it
        return Err(Error::SizeMismatch { expected: n * width, actual: (n + 1) * width });
    }
    check(n * width, progress.n_processed * width)?;
    Ok(nums)
}

fn pco_into<T: Plain>(src: &[u8], dst: &mut [u8], width: usize) -> Result<(), Error> {
    let nums = pco_numbers::<T>(src, count(dst.len(), width)?, width)?;
    write_plain(&nums, dst);
    Ok(())
}

fn write_plain<T: Plain>(nums: &[T], dst: &mut [u8]) {
    let mut out = Vec::with_capacity(dst.len());
    for v in nums {
        v.write_le(&mut out);
    }
    dst.copy_from_slice(&out);
}

fn decimal_into<F: Float + Plain>(mut body: &[u8], e: u8, dst: &mut [u8]) -> Result<(), Error> {
    let width = size_of::<F>();
    let n = count(dst.len(), width)?;
    let k = varint::get(&mut body)?;
    if k > n as u64 {
        return Err(Error::BadException);
    }
    let mut positions = Vec::with_capacity(k as usize);
    let mut next: u64 = 0;
    for _ in 0..k {
        let p = next.checked_add(varint::get(&mut body)?).ok_or(Error::BadException)?;
        if p >= n as u64 {
            return Err(Error::BadException);
        }
        positions.push(p as usize);
        next = p + 1;
    }
    let exc_len = k as usize * width;
    if body.len() < exc_len {
        return Err(Error::Truncated);
    }
    let (exc, pco) = body.split_at(exc_len);
    let qs = pco_numbers::<i64>(pco, n, width)?;
    let vals: Vec<F> = qs.into_iter().map(|q| F::decode(q, e)).collect();
    write_plain(&vals, dst);
    for (j, p) in positions.iter().enumerate() {
        dst[p * width..(p + 1) * width].copy_from_slice(&exc[j * width..(j + 1) * width]);
    }
    Ok(())
}
