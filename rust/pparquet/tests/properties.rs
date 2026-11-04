//! Random Arrow batches round-trip bit for bit through every policy.

mod common;

use arrow_array::builder::{Float64Builder, ListBuilder};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema};
use common::{batch_lines, lines, write};
use pparquet::{Policy, WriterOptions};
use proptest::prelude::*;
use std::sync::Arc;

/// Values that are often smooth or decimal (so PCO and DECIMAL are exercised), sometimes
/// arbitrary bits (NaN payloads, infinities, signed zeros).
fn f64_value() -> impl Strategy<Value = f64> {
    prop_oneof![
        (-100_000i64..100_000).prop_map(|v| v as f64 / 100.0),
        any::<u64>().prop_map(f64::from_bits),
        Just(-0.0),
        Just(f64::NAN),
        Just(f64::INFINITY),
    ]
}

fn f32_value() -> impl Strategy<Value = f32> {
    prop_oneof![(-10_000i32..10_000).prop_map(|v| v as f32 / 4.0), any::<u32>().prop_map(f32::from_bits)]
}

fn column(rows: usize) -> impl Strategy<Value = ArrayRef> {
    let n = rows;
    prop_oneof![
        prop::collection::vec(prop::option::weighted(0.9, any::<i32>()), n)
            .prop_map(|v| Arc::new(Int32Array::from(v)) as ArrayRef),
        prop::collection::vec(0i32..1000, n).prop_map(|v| Arc::new(Int32Array::from(v)) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.9, any::<i64>()), n)
            .prop_map(|v| Arc::new(Int64Array::from(v)) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.8, f32_value()), n)
            .prop_map(|v| Arc::new(Float32Array::from(v)) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.8, f64_value()), n)
            .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef),
        prop::collection::vec(1_700_000_000_000_000i64..1_700_000_100_000_000, n)
            .prop_map(|v| Arc::new(TimestampMicrosecondArray::from(v).with_timezone("UTC")) as ArrayRef),
        prop::collection::vec(-10_000_000i128..10_000_000, n)
            .prop_map(|v| Arc::new(Decimal128Array::from(v).with_precision_and_scale(18, 4).unwrap()) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.9, "[a-c]{0,3}"), n)
            .prop_map(|v| Arc::new(StringArray::from(v)) as ArrayRef),
        prop::collection::vec(
            prop::option::weighted(0.9, prop::collection::vec(prop::option::weighted(0.9, f64_value()), 0..4)),
            n
        )
        .prop_map(|v| {
            let mut b = ListBuilder::new(Float64Builder::new());
            for row in v {
                match row {
                    Some(items) => b.append_value(items),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }),
        (prop::collection::vec(any::<i64>(), n), prop::collection::vec(f64_value(), n)).prop_map(|(a, b)| {
            Arc::new(StructArray::from(vec![
                (Arc::new(Field::new("a", DataType::Int64, false)), Arc::new(Int64Array::from(a)) as ArrayRef),
                (Arc::new(Field::new("b", DataType::Float64, false)), Arc::new(Float64Array::from(b)) as ArrayRef),
            ])) as ArrayRef
        }),
    ]
}

fn batch() -> impl Strategy<Value = RecordBatch> {
    (0usize..300, 1usize..5).prop_flat_map(|(rows, cols)| prop::collection::vec(column(rows), cols)).prop_map(
        |columns| {
            let fields: Vec<Field> = columns
                .iter()
                .enumerate()
                .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), c.null_count() > 0 || i % 2 == 0))
                .collect();
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        },
    )
}

fn policy() -> impl Strategy<Value = Policy> {
    prop_oneof![Just(Policy::Auto), Just(Policy::Pcodec), Just(Policy::Base)]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn roundtrip_bit_exact(batch in batch(), policy in policy(), level in 0u32..=12, split in 0usize..300) {
        let options = WriterOptions::default().with_policy(policy).with_level(level).unwrap();
        let split = split.min(batch.num_rows());
        let parts = [batch.slice(0, split), batch.slice(split, batch.num_rows() - split)];
        let (file, _) = write(&parts, options);
        prop_assert_eq!(lines(&file), batch_lines(&[batch]));
    }
}
