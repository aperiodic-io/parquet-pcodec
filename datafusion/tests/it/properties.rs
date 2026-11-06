//! Random tables round-trip bit for bit through `COPY ... STORED AS TPARQUET` with every
//! policy and level.

use crate::common::*;
use crate::write::{copy, read_back};
use datafusion::arrow::array::builder::{Float64Builder, ListBuilder};
use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
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
        prop::collection::vec(0i32..1000, n).prop_map(|v| Arc::new(Date32Array::from(v)) as ArrayRef),
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

fn policy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("auto"), Just("pcodec"), Just("base")]
}

proptest! {
    // no regression files: the failures cargo mutants provokes are not regressions
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::with_cases(32) })]

    #[test]
    fn roundtrip_bit_exact(
        batch in batch(),
        policy in policy(),
        level in 0u32..=12,
        split in 0usize..300,
        row_group in 1usize..400,
        v2 in any::<bool>(),
    ) {
        let split = split.min(batch.num_rows());
        let parts = [batch.slice(0, split), batch.slice(split, batch.num_rows() - split)];
        let version = if v2 { "2.0" } else { "1.0" };
        let options =
            format!("policy '{policy}', pcodec_level {level}, max_row_group_size {row_group}, writer_version '{version}'");
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let rows = rt.block_on(async {
            let path = copy(&ctx(), dir.path(), &parts, &options).await;
            read_back(&path).await
        });
        prop_assert_eq!(rows, canonical_lines(&[batch]));
    }
}
