//! Canonical JSON of Arrow data (`testdata/README.md`): one object per row, compact, with
//! floats as the hex of their IEEE bits so that comparisons are bit-exact.

use crate::error::{Result, invalid};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, RecordBatch};
use arrow_schema::{DataType, Fields, TimeUnit};
use base64::Engine as _;
use std::fmt::Write as _;

/// Appends one line (with a trailing `\n`) per row of `batch`.
pub fn write_batch(batch: &RecordBatch, out: &mut String) -> Result<()> {
    for row in 0..batch.num_rows() {
        write_row(batch.schema().fields(), batch.columns(), row, out)?;
        out.push('\n');
    }
    Ok(())
}

/// The canonical JSON lines of `batch`.
pub fn batch_lines(batch: &RecordBatch) -> Result<Vec<String>> {
    (0..batch.num_rows())
        .map(|row| {
            let mut s = String::new();
            write_row(batch.schema().fields(), batch.columns(), row, &mut s)?;
            Ok(s)
        })
        .collect()
}

fn write_row(fields: &Fields, columns: &[ArrayRef], row: usize, out: &mut String) -> Result<()> {
    out.push('{');
    for (i, (f, c)) in fields.iter().zip(columns).enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_str(f.name(), out);
        out.push(':');
        write_value(c.as_ref(), row, out)?;
    }
    out.push('}');
    Ok(())
}

fn write_str(s: &str, out: &mut String) {
    out.push_str(&serde_json::to_string(s).expect("a str serializes"));
}

fn int<T: ArrowPrimitiveType>(a: &dyn Array, i: usize, out: &mut String)
where
    T::Native: std::fmt::Display,
{
    write!(out, "{}", a.as_primitive::<T>().value(i)).expect("write to String");
}

fn write_value(a: &dyn Array, i: usize, out: &mut String) -> Result<()> {
    if a.is_null(i) {
        out.push_str("null");
        return Ok(());
    }
    match a.data_type() {
        DataType::Null => out.push_str("null"),
        DataType::Boolean => out.push_str(if a.as_boolean().value(i) { "true" } else { "false" }),
        DataType::Int8 => int::<Int8Type>(a, i, out),
        DataType::Int16 => int::<Int16Type>(a, i, out),
        DataType::Int32 => int::<Int32Type>(a, i, out),
        DataType::Int64 => int::<Int64Type>(a, i, out),
        DataType::UInt8 => int::<UInt8Type>(a, i, out),
        DataType::UInt16 => int::<UInt16Type>(a, i, out),
        DataType::UInt32 => int::<UInt32Type>(a, i, out),
        DataType::UInt64 => int::<UInt64Type>(a, i, out),
        DataType::Float16 => {
            write!(out, "\"0x{:04x}\"", a.as_primitive::<Float16Type>().value(i).to_bits()).expect("write to String")
        }
        DataType::Float32 => {
            write!(out, "\"0x{:08x}\"", a.as_primitive::<Float32Type>().value(i).to_bits()).expect("write to String")
        }
        DataType::Float64 => {
            write!(out, "\"0x{:016x}\"", a.as_primitive::<Float64Type>().value(i).to_bits()).expect("write to String")
        }
        DataType::Date32 => int::<Date32Type>(a, i, out),
        DataType::Date64 => int::<Date64Type>(a, i, out),
        DataType::Time32(TimeUnit::Second) => int::<Time32SecondType>(a, i, out),
        DataType::Time32(_) => int::<Time32MillisecondType>(a, i, out),
        DataType::Time64(TimeUnit::Microsecond) => int::<Time64MicrosecondType>(a, i, out),
        DataType::Time64(_) => int::<Time64NanosecondType>(a, i, out),
        DataType::Timestamp(TimeUnit::Second, _) => int::<TimestampSecondType>(a, i, out),
        DataType::Timestamp(TimeUnit::Millisecond, _) => int::<TimestampMillisecondType>(a, i, out),
        DataType::Timestamp(TimeUnit::Microsecond, _) => int::<TimestampMicrosecondType>(a, i, out),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => int::<TimestampNanosecondType>(a, i, out),
        DataType::Duration(TimeUnit::Second) => int::<DurationSecondType>(a, i, out),
        DataType::Duration(TimeUnit::Millisecond) => int::<DurationMillisecondType>(a, i, out),
        DataType::Duration(TimeUnit::Microsecond) => int::<DurationMicrosecondType>(a, i, out),
        DataType::Duration(TimeUnit::Nanosecond) => int::<DurationNanosecondType>(a, i, out),
        DataType::Decimal32(..) => int::<Decimal32Type>(a, i, out),
        DataType::Decimal64(..) => int::<Decimal64Type>(a, i, out),
        DataType::Decimal128(..) => int::<Decimal128Type>(a, i, out),
        DataType::Decimal256(..) => int::<Decimal256Type>(a, i, out),
        DataType::Utf8 => write_str(a.as_string::<i32>().value(i), out),
        DataType::LargeUtf8 => write_str(a.as_string::<i64>().value(i), out),
        DataType::Utf8View => write_str(a.as_string_view().value(i), out),
        DataType::Binary => write_b64(a.as_binary::<i32>().value(i), out),
        DataType::LargeBinary => write_b64(a.as_binary::<i64>().value(i), out),
        DataType::BinaryView => write_b64(a.as_binary_view().value(i), out),
        DataType::FixedSizeBinary(_) => write_b64(a.as_fixed_size_binary().value(i), out),
        DataType::List(_) => write_list(a.as_list::<i32>().value(i).as_ref(), out)?,
        DataType::LargeList(_) => write_list(a.as_list::<i64>().value(i).as_ref(), out)?,
        DataType::FixedSizeList(..) => write_list(a.as_fixed_size_list().value(i).as_ref(), out)?,
        DataType::Struct(fields) => write_row(fields, a.as_struct().columns(), i, out)?,
        DataType::Map(..) => {
            let entries = a.as_map().value(i);
            out.push('[');
            for j in 0..entries.len() {
                if j > 0 {
                    out.push(',');
                }
                write_row(entries.fields(), entries.columns(), j, out)?;
            }
            out.push(']');
        }
        DataType::Dictionary(..) => {
            let d = a.as_any_dictionary();
            let key = d.normalized_keys()[i];
            write_value(d.values().as_ref(), key, out)?;
        }
        t => return Err(invalid(format!("canonical JSON does not support {t}"))),
    }
    Ok(())
}

fn write_b64(b: &[u8], out: &mut String) {
    out.push('"');
    out.push_str(&base64::engine::general_purpose::STANDARD.encode(b));
    out.push('"');
}

fn write_list(values: &dyn Array, out: &mut String) -> Result<()> {
    out.push('[');
    for j in 0..values.len() {
        if j > 0 {
            out.push(',');
        }
        write_value(values, j, out)?;
    }
    out.push(']');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::*;
    use arrow_array::*;
    use arrow_buffer::{OffsetBuffer, i256};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn one(a: ArrayRef) -> Vec<String> {
        let schema = Schema::new(vec![Field::new("v", a.data_type().clone(), true)]);
        let b = RecordBatch::try_new(Arc::new(schema), vec![a]).unwrap();
        batch_lines(&b)
            .unwrap()
            .into_iter()
            .map(|l| l.strip_prefix("{\"v\":").unwrap().strip_suffix('}').unwrap().to_string())
            .collect()
    }

    #[test]
    fn integers_are_numbers() {
        assert_eq!(one(Arc::new(Int8Array::from(vec![Some(-8), None]))), ["-8", "null"]);
        assert_eq!(one(Arc::new(Int16Array::from(vec![-16]))), ["-16"]);
        assert_eq!(one(Arc::new(Int32Array::from(vec![i32::MIN]))), ["-2147483648"]);
        assert_eq!(one(Arc::new(Int64Array::from(vec![i64::MIN]))), ["-9223372036854775808"]);
        assert_eq!(one(Arc::new(UInt8Array::from(vec![255]))), ["255"]);
        assert_eq!(one(Arc::new(UInt16Array::from(vec![65535]))), ["65535"]);
        assert_eq!(one(Arc::new(UInt32Array::from(vec![u32::MAX]))), ["4294967295"]);
        assert_eq!(one(Arc::new(UInt64Array::from(vec![u64::MAX]))), ["18446744073709551615"]);
    }

    #[test]
    fn floats_are_bits() {
        assert_eq!(
            one(Arc::new(Float64Array::from(vec![1.0, -0.0]))),
            ["\"0x3ff0000000000000\"", "\"0x8000000000000000\""]
        );
        assert_eq!(
            one(Arc::new(Float32Array::from(vec![1.0, f32::from_bits(1)]))),
            ["\"0x3f800000\"", "\"0x00000001\""]
        );
        assert_eq!(one(Arc::new(Float16Array::from(vec![half_one()]))), ["\"0x3c00\""]);
        assert_eq!(
            one(Arc::new(Float64Array::from(vec![f64::from_bits(0x7ff8_0000_0000_0001)]))),
            ["\"0x7ff8000000000001\""]
        );
    }

    fn half_one() -> <Float16Type as ArrowPrimitiveType>::Native {
        <Float16Type as ArrowPrimitiveType>::Native::from_bits(0x3c00)
    }

    #[test]
    fn temporal_and_decimal_are_numbers() {
        assert_eq!(one(Arc::new(Date32Array::from(vec![19000]))), ["19000"]);
        assert_eq!(one(Arc::new(Date64Array::from(vec![-1]))), ["-1"]);
        assert_eq!(one(Arc::new(Time32SecondArray::from(vec![1]))), ["1"]);
        assert_eq!(one(Arc::new(Time32MillisecondArray::from(vec![2]))), ["2"]);
        assert_eq!(one(Arc::new(Time64MicrosecondArray::from(vec![3]))), ["3"]);
        assert_eq!(one(Arc::new(Time64NanosecondArray::from(vec![4]))), ["4"]);
        assert_eq!(one(Arc::new(TimestampSecondArray::from(vec![5]))), ["5"]);
        assert_eq!(one(Arc::new(TimestampMillisecondArray::from(vec![6]).with_timezone("UTC"))), ["6"]);
        assert_eq!(one(Arc::new(TimestampMicrosecondArray::from(vec![7]))), ["7"]);
        assert_eq!(one(Arc::new(TimestampNanosecondArray::from(vec![8]))), ["8"]);
        assert_eq!(one(Arc::new(DurationSecondArray::from(vec![9]))), ["9"]);
        assert_eq!(one(Arc::new(DurationMillisecondArray::from(vec![10]))), ["10"]);
        assert_eq!(one(Arc::new(DurationMicrosecondArray::from(vec![11]))), ["11"]);
        assert_eq!(one(Arc::new(DurationNanosecondArray::from(vec![12]))), ["12"]);
        assert_eq!(one(Arc::new(Decimal32Array::from(vec![-13]).with_precision_and_scale(5, 2).unwrap())), ["-13"]);
        assert_eq!(one(Arc::new(Decimal64Array::from(vec![14]).with_precision_and_scale(12, 2).unwrap())), ["14"]);
        assert_eq!(
            one(Arc::new(Decimal128Array::from(vec![i128::MAX]).with_precision_and_scale(38, 0).unwrap())),
            ["170141183460469231731687303715884105727"]
        );
        assert_eq!(one(Arc::new(Decimal256Array::from(vec![i256::from_i128(-15)]))), ["-15"]);
    }

    #[test]
    fn strings_binary_booleans() {
        assert_eq!(one(Arc::new(StringArray::from(vec!["a\"b\\c\n\u{1}é"]))), ["\"a\\\"b\\\\c\\n\\u0001é\""]);
        assert_eq!(one(Arc::new(LargeStringArray::from(vec!["x"]))), ["\"x\""]);
        assert_eq!(
            one(Arc::new(StringViewArray::from(vec!["a string longer than twelve bytes"]))),
            ["\"a string longer than twelve bytes\""]
        );
        assert_eq!(one(Arc::new(BinaryArray::from(vec![&b"\x00\xff"[..]]))), ["\"AP8=\""]);
        assert_eq!(one(Arc::new(LargeBinaryArray::from(vec![&b"ab"[..]]))), ["\"YWI=\""]);
        assert_eq!(one(Arc::new(BinaryViewArray::from(vec![&b"abc"[..]]))), ["\"YWJj\""]);
        assert_eq!(one(Arc::new(FixedSizeBinaryArray::try_from_iter([[1u8, 2]].into_iter()).unwrap())), ["\"AQI=\""]);
        assert_eq!(one(Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]))), ["true", "false", "null"]);
        assert_eq!(one(Arc::new(NullArray::new(1))), ["null"]);
    }

    #[test]
    fn nested() {
        let mut lb = ListBuilder::new(Int32Builder::new());
        lb.append_value([Some(1), None, Some(3)]);
        lb.append_null();
        lb.append_value([]);
        assert_eq!(one(Arc::new(lb.finish())), ["[1,null,3]", "null", "[]"]);

        let large = LargeListArray::new(
            Arc::new(Field::new_list_field(DataType::Int64, false)),
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int64Array::from(vec![4, 5])),
            None,
        );
        assert_eq!(one(Arc::new(large)), ["[4,5]"]);
        let fixed = FixedSizeListArray::new(
            Arc::new(Field::new_list_field(DataType::Int64, false)),
            2,
            Arc::new(Int64Array::from(vec![6, 7])),
            None,
        );
        assert_eq!(one(Arc::new(fixed)), ["[6,7]"]);

        let s = StructArray::from(vec![
            (Arc::new(Field::new("b", DataType::Int32, false)), Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);
        assert_eq!(one(Arc::new(s)), ["{\"b\":1,\"a\":null}"]);

        let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        mb.keys().append_value("k");
        mb.values().append_value(1);
        mb.append(true).unwrap();
        assert_eq!(one(Arc::new(mb.finish())), ["[{\"key\":\"k\",\"value\":1}]"]);

        let d: DictionaryArray<Int8Type> = vec!["x", "y", "x"].into_iter().collect();
        assert_eq!(one(Arc::new(d)), ["\"x\"", "\"y\"", "\"x\""]);
    }

    #[test]
    fn rows_are_objects_in_schema_order() {
        let schema = Schema::new(vec![Field::new("z", DataType::Int32, false), Field::new("a", DataType::Utf8, true)]);
        let b = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(StringArray::from(vec![Some("x"), None]))],
        )
        .unwrap();
        let mut s = String::new();
        write_batch(&b, &mut s).unwrap();
        assert_eq!(s, "{\"z\":1,\"a\":\"x\"}\n{\"z\":2,\"a\":null}\n");
    }

    #[test]
    fn unsupported_types_are_errors() {
        let a: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![1]));
        let schema = Schema::new(vec![Field::new("v", a.data_type().clone(), true)]);
        let b = RecordBatch::try_new(Arc::new(schema), vec![a]).unwrap();
        assert_eq!(batch_lines(&b).unwrap_err().to_string(), "canonical JSON does not support Interval(YearMonth)");
    }
}
