//! DuckDB vectors to Arrow arrays, for `COPY ... TO`.
//!
//! The conversion reads DuckDB's documented flat vector layout (`duckdb.h`: data as an array of
//! the physical type, validity as a bitmask of `u64` words, strings as 16-byte `duckdb_string_t`,
//! lists as `duckdb_list_entry` with a child vector, structs and arrays as child vectors). It is
//! written against [`VectorView`] so that the tests can hand it vectors laid out in plain memory;
//! [`crate::write`] implements the trait over the C API.
//!
//! Every value is read through a *selection*: the rows to convert, `None` for a row whose parent
//! is NULL. A NULL row is never dereferenced (its string pointer or list entry may be garbage),
//! and list children are read only where an entry points.

use crate::types::{LIST_ELEMENT, MAP_ENTRIES, SqlType, UTC};
use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::types::*;
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, FixedSizeListArray, ListArray, MapArray, PrimitiveArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};
use std::sync::Arc;

/// A flat DuckDB vector.
pub trait VectorView: Sized {
    /// The start of the data array.
    fn data(&self) -> *const u8;
    /// The validity bitmask, or null when every row is valid.
    fn validity(&self) -> *const u64;
    /// The child vector of a LIST and its length.
    fn list_child(&self) -> (Self, usize);
    /// The `i`-th child vector of a STRUCT.
    fn struct_child(&self, i: usize) -> Self;
    /// The child vector of an ARRAY.
    fn array_child(&self) -> Self;
}

/// DuckDB's `duckdb_list_entry`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListEntry {
    pub offset: u64,
    pub length: u64,
}

/// Size of `duckdb_string_t`; strings of at most [`INLINE`] bytes are stored inside it.
pub const STRING_T: usize = 16;
pub const INLINE: usize = 12;

fn valid<V: VectorView>(v: &V, row: usize) -> bool {
    let mask = v.validity();
    mask.is_null() || unsafe { *mask.add(row / 64) } >> (row % 64) & 1 == 1
}

/// # Safety
/// `row` is within the vector and `T` is its physical type.
unsafe fn read<T: Copy, V: VectorView>(v: &V, row: usize) -> T {
    unsafe { (v.data() as *const T).add(row).read_unaligned() }
}

/// # Safety
/// `row` is a valid row of a VARCHAR or BLOB vector.
unsafe fn string_bytes<V: VectorView>(v: &V, row: usize) -> &[u8] {
    let base = unsafe { v.data().add(row * STRING_T) };
    let len = unsafe { (base as *const u32).read_unaligned() } as usize;
    let ptr = if len <= INLINE {
        unsafe { base.add(4) }
    } else {
        unsafe { (base.add(8) as *const *const u8).read_unaligned() }
    };
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// The rows of `sel` that are valid in `v`.
fn present<V: VectorView>(v: &V, sel: &[Option<usize>]) -> Vec<Option<usize>> {
    sel.iter().map(|r| r.filter(|&r| valid(v, r))).collect()
}

fn nulls(rows: &[Option<usize>]) -> Option<NullBuffer> {
    let n = NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<bool>>());
    (n.null_count() > 0).then_some(n)
}

fn primitive<P: ArrowPrimitiveType, V: VectorView>(
    v: &V,
    rows: &[Option<usize>],
    f: impl Fn(P::Native) -> Result<P::Native, String>,
) -> Result<PrimitiveArray<P>, String> {
    let values = rows
        .iter()
        .map(|r| match r {
            Some(r) => f(unsafe { read::<P::Native, V>(v, *r) }),
            None => Ok(P::Native::default()),
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(PrimitiveArray::<P>::new(values.into(), nulls(rows)))
}

fn plain<P: ArrowPrimitiveType, V: VectorView>(v: &V, rows: &[Option<usize>]) -> Result<ArrayRef, String> {
    Ok(Arc::new(primitive::<P, V>(v, rows, Ok)?))
}

/// How many bytes DuckDB uses for a decimal of this width.
pub fn decimal_bytes(width: u8) -> usize {
    match width {
        0..=4 => 2,
        5..=9 => 4,
        10..=18 => 8,
        _ => 16,
    }
}

/// How many bytes DuckDB uses for the index of an enum with this many values.
pub fn enum_bytes(values: usize) -> usize {
    if values <= u8::MAX as usize {
        1
    } else if values <= u16::MAX as usize {
        2
    } else {
        4
    }
}

fn decimal<V: VectorView>(v: &V, rows: &[Option<usize>], width: u8, scale: u8) -> Result<ArrayRef, String> {
    let values: Vec<i128> = rows
        .iter()
        .map(|r| match r {
            None => 0,
            Some(r) => unsafe {
                match decimal_bytes(width) {
                    2 => read::<i16, V>(v, *r) as i128,
                    4 => read::<i32, V>(v, *r) as i128,
                    8 => read::<i64, V>(v, *r) as i128,
                    // duckdb_hugeint is the lower u64, then the upper i64: on the little-endian
                    // targets DuckDB supports, the bytes of an i128
                    _ => read::<i128, V>(v, *r),
                }
            },
        })
        .collect();
    let a = Decimal128Array::new(values.into(), nulls(rows))
        .with_precision_and_scale(width, scale as i8)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(a))
}

/// DuckDB's `infinity` and `-infinity` of every timestamp unit.
pub const INFINITY: i64 = i64::MAX;
pub const NEG_INFINITY: i64 = -i64::MAX;

/// A `TIMESTAMP_S` value as `TIMESTAMP_MS`; the infinities stay infinite.
fn seconds_to_millis(s: i64) -> Result<i64, String> {
    if s == INFINITY || s == NEG_INFINITY {
        return Ok(s);
    }
    s.checked_mul(1000).ok_or_else(|| format!("TIMESTAMP_S value {s} does not fit in milliseconds"))
}

fn enum_index<V: VectorView>(v: &V, r: usize, values: usize) -> usize {
    unsafe {
        match enum_bytes(values) {
            1 => read::<u8, V>(v, r) as usize,
            2 => read::<u16, V>(v, r) as usize,
            _ => read::<u32, V>(v, r) as usize,
        }
    }
}

/// Converts the rows `sel` of the vector `v` of type `t`.
pub fn convert<V: VectorView>(v: &V, t: &SqlType, sel: &[Option<usize>]) -> Result<ArrayRef, String> {
    let rows = present(v, sel);
    let rows = rows.as_slice();
    Ok(match t {
        SqlType::Boolean => {
            let values: Vec<bool> =
                rows.iter().map(|r| r.is_some_and(|r| unsafe { read::<u8, V>(v, r) } != 0)).collect();
            Arc::new(BooleanArray::new(values.into(), nulls(rows)))
        }
        SqlType::TinyInt => plain::<Int8Type, V>(v, rows)?,
        SqlType::SmallInt => plain::<Int16Type, V>(v, rows)?,
        SqlType::Integer => plain::<Int32Type, V>(v, rows)?,
        SqlType::BigInt => plain::<Int64Type, V>(v, rows)?,
        SqlType::UTinyInt => plain::<UInt8Type, V>(v, rows)?,
        SqlType::USmallInt => plain::<UInt16Type, V>(v, rows)?,
        SqlType::UInteger => plain::<UInt32Type, V>(v, rows)?,
        SqlType::UBigInt => plain::<UInt64Type, V>(v, rows)?,
        SqlType::Float => plain::<Float32Type, V>(v, rows)?,
        SqlType::Double => plain::<Float64Type, V>(v, rows)?,
        SqlType::Decimal { width, scale } => decimal(v, rows, *width, *scale)?,
        SqlType::Date => plain::<Date32Type, V>(v, rows)?,
        SqlType::Time => plain::<Time64MicrosecondType, V>(v, rows)?,
        SqlType::TimeNs => plain::<Time64NanosecondType, V>(v, rows)?,
        SqlType::TimestampS => Arc::new(primitive::<TimestampMillisecondType, V>(v, rows, seconds_to_millis)?),
        SqlType::TimestampMs => plain::<TimestampMillisecondType, V>(v, rows)?,
        SqlType::Timestamp => plain::<TimestampMicrosecondType, V>(v, rows)?,
        SqlType::TimestampNs => plain::<TimestampNanosecondType, V>(v, rows)?,
        SqlType::TimestampTz => Arc::new(primitive::<TimestampMicrosecondType, V>(v, rows, Ok)?.with_timezone(UTC)),
        SqlType::TimestampTzNs => Arc::new(primitive::<TimestampNanosecondType, V>(v, rows, Ok)?.with_timezone(UTC)),
        SqlType::Varchar => {
            let mut b = StringBuilder::new();
            for r in rows {
                match r {
                    Some(r) => {
                        let bytes = unsafe { string_bytes(v, *r) };
                        b.append_value(
                            std::str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8 in VARCHAR: {e}"))?,
                        );
                    }
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        SqlType::Blob => {
            let mut b = BinaryBuilder::new();
            for r in rows {
                match r {
                    Some(r) => b.append_value(unsafe { string_bytes(v, *r) }),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        SqlType::Enum(values) => {
            let mut b = StringBuilder::new();
            for r in rows {
                match r {
                    Some(r) => {
                        let i = enum_index(v, *r, values.len());
                        b.append_value(values.get(i).ok_or_else(|| format!("enum index {i} out of range"))?);
                    }
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        SqlType::List(child_type) | SqlType::Map(child_type, _) => {
            let (child, child_len) = v.list_child();
            let mut offsets = vec![0i32];
            let mut child_sel = vec![];
            for r in rows {
                if let Some(r) = r {
                    let e = unsafe { read::<ListEntry, V>(v, *r) };
                    let end = e.offset.checked_add(e.length).filter(|&end| end <= child_len as u64);
                    let end = end.ok_or_else(|| format!("list entry {e:?} outside its child of {child_len} rows"))?;
                    child_sel.extend((e.offset..end).map(|i| Some(i as usize)));
                }
                let len = i32::try_from(child_sel.len()).map_err(|_| "list children exceed 2^31 values")?;
                offsets.push(len);
            }
            let offsets = OffsetBuffer::new(offsets.into());
            if let SqlType::Map(k, val) = t {
                let key = convert(&child.struct_child(0), k, &child_sel)?;
                let value = convert(&child.struct_child(1), val, &child_sel)?;
                if key.null_count() > 0 {
                    return Err("map keys must not be NULL".into());
                }
                let fields = Fields::from(vec![
                    Field::new("key", key.data_type().clone(), false),
                    Field::new("value", value.data_type().clone(), true),
                ]);
                let entries =
                    StructArray::try_new(fields.clone(), vec![key, value], None).map_err(|e| e.to_string())?;
                let field = Arc::new(Field::new(MAP_ENTRIES, DataType::Struct(fields), false));
                Arc::new(MapArray::try_new(field, offsets, entries, nulls(rows), false).map_err(|e| e.to_string())?)
            } else {
                let values = convert(&child, child_type, &child_sel)?;
                let field = Arc::new(Field::new(LIST_ELEMENT, values.data_type().clone(), true));
                Arc::new(ListArray::try_new(field, offsets, values, nulls(rows)).map_err(|e| e.to_string())?)
            }
        }
        SqlType::Array(child_type, n) => {
            let child = v.array_child();
            let child_sel: Vec<Option<usize>> =
                rows.iter().flat_map(|r| (0..*n).map(move |i| r.map(|r| r * n + i))).collect();
            let values = convert(&child, child_type, &child_sel)?;
            let field = Arc::new(Field::new(LIST_ELEMENT, values.data_type().clone(), true));
            let size = i32::try_from(*n).map_err(|_| format!("ARRAY length {n} is too large"))?;
            Arc::new(FixedSizeListArray::try_new(field, size, values, nulls(rows)).map_err(|e| e.to_string())?)
        }
        SqlType::Struct(fields) => {
            let columns = fields
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| convert(&v.struct_child(i), ft, rows))
                .collect::<Result<Vec<_>, String>>()?;
            let fields: Fields = fields
                .iter()
                .zip(&columns)
                .map(|((name, _), c)| Field::new(name, c.data_type().clone(), true))
                .collect();
            Arc::new(StructArray::try_new(fields, columns, nulls(rows)).map_err(|e| e.to_string())?)
        }
        SqlType::Interval | SqlType::Unsupported(_) => {
            return Err(format!("columns of type {t} cannot be written to Parquet by this extension"));
        }
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow_array::{
        Array, BinaryArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };

    /// A vector in plain memory.
    #[derive(Debug, Default, Clone)]
    pub struct Mem {
        pub data: Vec<u8>,
        pub validity: Option<Vec<u64>>,
        pub children: Vec<Mem>,
        pub child_len: usize,
        /// Keeps the bytes of long strings alive.
        pub heap: Vec<Box<[u8]>>,
    }

    impl VectorView for Mem {
        fn data(&self) -> *const u8 {
            self.data.as_ptr()
        }
        fn validity(&self) -> *const u64 {
            self.validity.as_ref().map_or(std::ptr::null(), |v| v.as_ptr())
        }
        fn list_child(&self) -> (Self, usize) {
            (self.children[0].clone(), self.child_len)
        }
        fn struct_child(&self, i: usize) -> Self {
            self.children[i].clone()
        }
        fn array_child(&self) -> Self {
            self.children[0].clone()
        }
    }

    impl Mem {
        pub fn of<T: Copy>(values: &[T]) -> Self {
            let mut data = vec![0u8; std::mem::size_of_val(values)];
            unsafe { std::ptr::copy_nonoverlapping(values.as_ptr() as *const u8, data.as_mut_ptr(), data.len()) };
            Self { data, ..Default::default() }
        }

        /// Marks `invalid` rows NULL.
        pub fn nulls(mut self, rows: usize, invalid: &[usize]) -> Self {
            let mut words = vec![u64::MAX; rows.div_ceil(64).max(1)];
            for &r in invalid {
                words[r / 64] &= !(1 << (r % 64));
            }
            self.validity = Some(words);
            self
        }

        /// A VARCHAR or BLOB vector; `None` rows hold a dangling pointer, as NULL rows may.
        pub fn strings(values: &[Option<&[u8]>]) -> Self {
            let mut m = Self::default();
            for v in values {
                let mut s = [0u8; STRING_T];
                match v {
                    Some(b) => {
                        s[..4].copy_from_slice(&(b.len() as u32).to_le_bytes());
                        if b.len() <= INLINE {
                            s[4..4 + b.len()].copy_from_slice(b);
                        } else {
                            s[4..8].copy_from_slice(&b[..4]);
                            let boxed: Box<[u8]> = b.to_vec().into_boxed_slice();
                            s[8..].copy_from_slice(&(boxed.as_ptr() as usize).to_le_bytes());
                            m.heap.push(boxed);
                        }
                    }
                    None => {
                        s[..4].copy_from_slice(&1000u32.to_le_bytes());
                        s[8..].copy_from_slice(&0xdead_beefusize.to_le_bytes());
                    }
                }
                m.data.extend_from_slice(&s);
            }
            let invalid: Vec<usize> = values.iter().enumerate().filter(|(_, v)| v.is_none()).map(|(i, _)| i).collect();
            m.nulls(values.len(), &invalid)
        }
    }

    fn all(n: usize) -> Vec<Option<usize>> {
        (0..n).map(Some).collect()
    }

    fn eq(a: &ArrayRef, b: impl Array) {
        assert_eq!(a.as_ref(), &b as &dyn Array);
    }

    #[test]
    fn integers_and_floats() {
        let sel = all(3);
        eq(&convert(&Mem::of(&[-1i8, 0, 1]), &SqlType::TinyInt, &sel).unwrap(), Int8Array::from(vec![-1, 0, 1]));
        eq(&convert(&Mem::of(&[-1i16, 2, 3]), &SqlType::SmallInt, &sel).unwrap(), Int16Array::from(vec![-1, 2, 3]));
        eq(
            &convert(&Mem::of(&[i32::MIN, 2, 3]), &SqlType::Integer, &sel).unwrap(),
            Int32Array::from(vec![i32::MIN, 2, 3]),
        );
        eq(
            &convert(&Mem::of(&[i64::MAX, 2, 3]), &SqlType::BigInt, &sel).unwrap(),
            Int64Array::from(vec![i64::MAX, 2, 3]),
        );
        eq(&convert(&Mem::of(&[255u8, 0, 1]), &SqlType::UTinyInt, &sel).unwrap(), UInt8Array::from(vec![255, 0, 1]));
        eq(
            &convert(&Mem::of(&[65535u16, 0, 1]), &SqlType::USmallInt, &sel).unwrap(),
            UInt16Array::from(vec![65535, 0, 1]),
        );
        eq(
            &convert(&Mem::of(&[u32::MAX, 0, 1]), &SqlType::UInteger, &sel).unwrap(),
            UInt32Array::from(vec![u32::MAX, 0, 1]),
        );
        eq(
            &convert(&Mem::of(&[u64::MAX, 0, 1]), &SqlType::UBigInt, &sel).unwrap(),
            UInt64Array::from(vec![u64::MAX, 0, 1]),
        );
        let nan = f64::from_bits(0x7ff8_dead_beef_0001);
        let d = convert(&Mem::of(&[nan, -0.0, 1.5]), &SqlType::Double, &sel).unwrap();
        let d = d.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(
            d.values().iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
            vec![nan.to_bits(), 1 << 63, 1.5f64.to_bits()]
        );
        let f = convert(&Mem::of(&[f32::from_bits(0x7fc0_beef), 2.0, 3.0]), &SqlType::Float, &sel).unwrap();
        assert_eq!(f.as_any().downcast_ref::<Float32Array>().unwrap().value(0).to_bits(), 0x7fc0_beef);
    }

    #[test]
    fn nulls_and_selections() {
        let v = Mem::of(&[10i64, 11, 12, 13, 14]).nulls(5, &[1, 3]);
        let a = convert(&v, &SqlType::BigInt, &all(5)).unwrap();
        eq(&a, Int64Array::from(vec![Some(10), None, Some(12), None, Some(14)]));
        // a selection reorders rows; None (a NULL parent) gives NULL
        let a = convert(&v, &SqlType::BigInt, &[Some(4), None, Some(0), Some(1)]).unwrap();
        eq(&a, Int64Array::from(vec![Some(14), None, Some(10), None]));
        let a = convert(&Mem::of(&[1i32, 2]), &SqlType::Integer, &all(2)).unwrap();
        assert!(a.nulls().is_none());
        // validity words past the first
        let v = Mem::of(&(0..130i32).collect::<Vec<_>>()).nulls(130, &[64, 129]);
        let a = convert(&v, &SqlType::Integer, &all(130)).unwrap();
        assert_eq!(a.null_count(), 2);
        assert!(a.is_null(64) && a.is_null(129) && a.is_valid(63) && a.is_valid(65));
    }

    #[test]
    fn booleans() {
        let v = Mem::of(&[1u8, 0, 1, 7]).nulls(4, &[2]);
        let a = convert(&v, &SqlType::Boolean, &all(4)).unwrap();
        eq(&a, BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]));
    }

    #[test]
    fn decimals_of_every_storage_width() {
        let sel = all(2);
        let check = |m: Mem, width: u8, values: Vec<i128>| {
            let a = convert(&m, &SqlType::Decimal { width, scale: 2 }, &sel).unwrap();
            eq(&a, Decimal128Array::from(values).with_precision_and_scale(width, 2).unwrap());
        };
        check(Mem::of(&[-9999i16, 12]), 4, vec![-9999, 12]);
        check(Mem::of(&[-999_999_999i32, 12]), 9, vec![-999_999_999, 12]);
        check(Mem::of(&[-999_999_999_999_999_999i64, 12]), 18, vec![-999_999_999_999_999_999, 12]);
        let big: i128 = -(10i128.pow(38) - 1);
        let parts = [big as u64, (big >> 64) as u64, 12u64, 0];
        check(Mem::of(&parts), 38, vec![big, 12]);
        assert_eq!(decimal_bytes(1), 2);
        assert_eq!(decimal_bytes(5), 4);
        assert_eq!(decimal_bytes(10), 8);
        assert_eq!(decimal_bytes(19), 16);
    }

    #[test]
    fn temporal_values_keep_their_bits() {
        let sel = all(2);
        eq(&convert(&Mem::of(&[20089i32, -1]), &SqlType::Date, &sel).unwrap(), Date32Array::from(vec![20089, -1]));
        eq(
            &convert(&Mem::of(&[1i64, 86_399_999_999]), &SqlType::Time, &sel).unwrap(),
            Time64MicrosecondArray::from(vec![1, 86_399_999_999]),
        );
        eq(&convert(&Mem::of(&[1i64, 5]), &SqlType::TimeNs, &sel).unwrap(), Time64NanosecondArray::from(vec![1, 5]));
        eq(
            &convert(&Mem::of(&[2i64, -3]), &SqlType::TimestampS, &sel).unwrap(),
            TimestampMillisecondArray::from(vec![2000, -3000]),
        );
        eq(
            &convert(&Mem::of(&[2i64, -3]), &SqlType::TimestampMs, &sel).unwrap(),
            TimestampMillisecondArray::from(vec![2, -3]),
        );
        eq(
            &convert(&Mem::of(&[2i64, -3]), &SqlType::Timestamp, &sel).unwrap(),
            TimestampMicrosecondArray::from(vec![2, -3]),
        );
        eq(
            &convert(&Mem::of(&[2i64, -3]), &SqlType::TimestampNs, &sel).unwrap(),
            TimestampNanosecondArray::from(vec![2, -3]),
        );
        eq(
            &convert(&Mem::of(&[2i64, -3]), &SqlType::TimestampTz, &sel).unwrap(),
            TimestampMicrosecondArray::from(vec![2, -3]).with_timezone("UTC"),
        );
        eq(
            &convert(&Mem::of(&[2i64, -3]), &SqlType::TimestampTzNs, &sel).unwrap(),
            TimestampNanosecondArray::from(vec![2, -3]).with_timezone("UTC"),
        );
        // infinity and -infinity are the same in every unit
        let a = convert(&Mem::of(&[i64::MAX, -i64::MAX]), &SqlType::TimestampS, &sel).unwrap();
        eq(&a, TimestampMillisecondArray::from(vec![i64::MAX, -i64::MAX]));
        for s in [i64::MAX - 1, i64::MIN, i64::MAX / 1000 + 1, -i64::MAX + 1] {
            let e = convert(&Mem::of(&[s, 0]), &SqlType::TimestampS, &sel).unwrap_err();
            assert_eq!(e, format!("TIMESTAMP_S value {s} does not fit in milliseconds"));
        }
        let edge = i64::MAX / 1000;
        let a = convert(&Mem::of(&[edge, -edge]), &SqlType::TimestampS, &sel).unwrap();
        eq(&a, TimestampMillisecondArray::from(vec![edge * 1000, -edge * 1000]));
        // a NULL row is not converted
        let a = convert(&Mem::of(&[i64::MAX - 1, 1]).nulls(2, &[0]), &SqlType::TimestampS, &sel).unwrap();
        eq(&a, TimestampMillisecondArray::from(vec![None, Some(1000)]));
    }

    #[test]
    fn strings_inline_and_on_the_heap() {
        let long = "a string longer than twelve bytes, ünïcode".as_bytes();
        let v = Mem::strings(&[Some(b"short"), None, Some(long), Some(b""), Some(b"exactly12byt")]);
        let a = convert(&v, &SqlType::Varchar, &all(5)).unwrap();
        let s = std::str::from_utf8(long).unwrap();
        eq(&a, StringArray::from(vec![Some("short"), None, Some(s), Some(""), Some("exactly12byt")]));
        let b = convert(&v, &SqlType::Blob, &all(5)).unwrap();
        eq(&b, BinaryArray::from(vec![Some(&b"short"[..]), None, Some(long), Some(b""), Some(b"exactly12byt")]));
        let bad = Mem::strings(&[Some(&[0xff, 0xfe])]);
        assert!(convert(&bad, &SqlType::Varchar, &all(1)).unwrap_err().starts_with("invalid UTF-8 in VARCHAR"));
        assert!(convert(&bad, &SqlType::Blob, &all(1)).is_ok());
    }

    #[test]
    fn enums_by_dictionary_size() {
        let small: Vec<String> = vec!["a".into(), "b".into()];
        let a = convert(&Mem::of(&[1u8, 0, 1]).nulls(3, &[2]), &SqlType::Enum(small.clone()), &all(3)).unwrap();
        eq(&a, StringArray::from(vec![Some("b"), Some("a"), None]));
        let e = convert(&Mem::of(&[2u8]), &SqlType::Enum(small), &all(1)).unwrap_err();
        assert_eq!(e, "enum index 2 out of range");
        let mid: Vec<String> = (0..300).map(|i| format!("v{i}")).collect();
        let a = convert(&Mem::of(&[299u16, 7]), &SqlType::Enum(mid), &all(2)).unwrap();
        eq(&a, StringArray::from(vec!["v299", "v7"]));
        let large: Vec<String> = (0..70_000).map(|i| format!("v{i}")).collect();
        let a = convert(&Mem::of(&[69_999u32, 1]), &SqlType::Enum(large), &all(2)).unwrap();
        eq(&a, StringArray::from(vec!["v69999", "v1"]));
        assert_eq!((enum_bytes(255), enum_bytes(256), enum_bytes(65535), enum_bytes(65536)), (1, 2, 2, 4));
    }

    fn entries(e: &[(u64, u64)]) -> Vec<ListEntry> {
        e.iter().map(|&(offset, length)| ListEntry { offset, length }).collect()
    }

    #[test]
    fn lists_follow_their_entries() {
        // rows: [1, 2], NULL (garbage entry), [], [5] taken out of order from the child
        let child = Mem::of(&[1i64, 2, 0, 5]).nulls(4, &[2]);
        let mut v = Mem::of(&entries(&[(0, 2), (999, 999), (2, 0), (3, 1)])).nulls(4, &[1]);
        v.children = vec![child];
        v.child_len = 4;
        let t = SqlType::List(Box::new(SqlType::BigInt));
        let a = convert(&v, &t, &all(4)).unwrap();
        let l = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(l.value_offsets(), &[0, 2, 2, 2, 3]);
        assert!(l.is_null(1) && l.is_valid(2));
        eq(l.values(), Int64Array::from(vec![1, 2, 5]));
        assert_eq!(l.value_type(), DataType::Int64);
        // a selection with a NULL parent skips the entry
        let a = convert(&v, &t, &[None, Some(3)]).unwrap();
        let l = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(l.value_offsets(), &[0, 0, 1]);
        // entries must stay inside the child
        let mut bad = Mem::of(&entries(&[(3, 2)]));
        bad.children = vec![Mem::of(&[1i64, 2, 3, 4])];
        bad.child_len = 4;
        let e = convert(&bad, &t, &all(1)).unwrap_err();
        assert_eq!(e, "list entry ListEntry { offset: 3, length: 2 } outside its child of 4 rows");
        let mut overflow = Mem::of(&entries(&[(u64::MAX, 2)]));
        overflow.children = vec![Mem::of(&[1i64])];
        overflow.child_len = 1;
        assert!(convert(&overflow, &t, &all(1)).is_err());
        // exactly up to the end is fine
        let mut edge = Mem::of(&entries(&[(2, 2)]));
        edge.children = vec![Mem::of(&[1i64, 2, 3, 4])];
        edge.child_len = 4;
        assert!(convert(&edge, &t, &all(1)).is_ok());
    }

    #[test]
    fn structs_pass_their_nulls_down() {
        // the child's row 1 is a dangling string under a NULL struct row
        let names = Mem::strings(&[Some(b"x"), Some(b"long enough to live on the heap")]);
        let mut names_garbage = names.clone();
        names_garbage.validity = None;
        names_garbage.data[16 + 8..32].copy_from_slice(&0x10usize.to_le_bytes());
        let mut v = Mem::of(&[0u8; 2]).nulls(2, &[1]);
        v.children = vec![Mem::of(&[7i32, 8]), names_garbage];
        let t = SqlType::Struct(vec![("id".into(), SqlType::Integer), ("name".into(), SqlType::Varchar)]);
        let a = convert(&v, &t, &all(2)).unwrap();
        let s = a.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(s.is_valid(0) && s.is_null(1));
        assert_eq!(s.fields()[0].name(), "id");
        assert_eq!(s.fields()[1].name(), "name");
        eq(s.column(0), Int32Array::from(vec![Some(7), None]));
        eq(s.column(1), StringArray::from(vec![Some("x"), None]));
    }

    #[test]
    fn arrays_and_maps() {
        let mut v = Mem::of(&[0u8; 3]).nulls(3, &[1]);
        v.children = vec![Mem::of(&[1i32, 2, 9, 9, 5, 6]).nulls(6, &[5])];
        let t = SqlType::Array(Box::new(SqlType::Integer), 2);
        let a = convert(&v, &t, &all(3)).unwrap();
        let f = a.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(f.value_length(), 2);
        assert!(f.is_null(1));
        eq(f.values(), Int32Array::from(vec![Some(1), Some(2), None, None, Some(5), None]));

        let keys = Mem::strings(&[Some(b"a"), Some(b"b"), Some(b"c")]);
        let values = Mem::of(&[1i64, 2, 3]).nulls(3, &[1]);
        let kv = Mem { children: vec![keys, values], ..Default::default() };
        let mut m = Mem::of(&entries(&[(0, 2), (0, 0), (2, 1)])).nulls(3, &[1]);
        m.children = vec![kv];
        m.child_len = 3;
        let t = SqlType::Map(Box::new(SqlType::Varchar), Box::new(SqlType::BigInt));
        let a = convert(&m, &t, &all(3)).unwrap();
        let map = a.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_offsets(), &[0, 2, 2, 3]);
        assert!(map.is_null(1));
        eq(map.keys(), StringArray::from(vec!["a", "b", "c"]));
        eq(map.values(), Int64Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(a.data_type(), &t.to_arrow().unwrap());

        let mut null_key = m.clone();
        null_key.children[0].children[0] = Mem::strings(&[Some(b"a"), None, Some(b"c")]);
        assert_eq!(convert(&null_key, &t, &all(3)).unwrap_err(), "map keys must not be NULL");
    }

    #[test]
    fn refuses_unsupported_types() {
        let e = convert(&Mem::of(&[0u8; 16]), &SqlType::Unsupported("UUID".into()), &all(1)).unwrap_err();
        assert_eq!(e, "columns of type UUID cannot be written to Parquet by this extension");
        assert!(convert(&Mem::of(&[0u8; 16]), &SqlType::Interval, &all(1)).is_err());
    }

    #[test]
    fn converted_types_match_the_schema() {
        // the arrays COPY builds have exactly the Arrow types its schema declares
        let cases: Vec<(SqlType, Mem)> = vec![
            (SqlType::TimestampS, Mem::of(&[1i64])),
            (SqlType::TimestampTz, Mem::of(&[1i64])),
            (SqlType::Decimal { width: 18, scale: 3 }, Mem::of(&[1i64])),
            (SqlType::Enum(vec!["a".into()]), Mem::of(&[0u8])),
            (SqlType::List(Box::new(SqlType::Integer)), {
                let mut l = Mem::of(&entries(&[(0, 1)]));
                l.children = vec![Mem::of(&[1i32])];
                l.child_len = 1;
                l
            }),
            (SqlType::Array(Box::new(SqlType::Integer), 1), {
                let mut a = Mem::of(&[0u8]);
                a.children = vec![Mem::of(&[1i32])];
                a
            }),
            (SqlType::Map(Box::new(SqlType::Integer), Box::new(SqlType::Integer)), {
                let kv = Mem { children: vec![Mem::of(&[1i32]), Mem::of(&[2i32])], ..Default::default() };
                let mut m = Mem::of(&entries(&[(0, 1)]));
                m.children = vec![kv];
                m.child_len = 1;
                m
            }),
            (SqlType::Struct(vec![("a".into(), SqlType::Integer)]), {
                let mut s = Mem::of(&[0u8]);
                s.children = vec![Mem::of(&[1i32])];
                s
            }),
        ];
        for (t, m) in cases {
            assert_eq!(convert(&m, &t, &all(1)).unwrap().data_type(), &t.to_arrow().unwrap(), "{t}");
        }
    }
}
