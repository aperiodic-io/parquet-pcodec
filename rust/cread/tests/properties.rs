//! Random tables round trip through the tparquet writer and the C stream reader, and random
//! corruption of a file's PCODEC pages is an error or other rows, never a crash.

mod common;

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{Field, Schema};
use common::*;
use proptest::prelude::*;
use std::sync::Arc;

fn f64_value() -> impl Strategy<Value = f64> {
    prop_oneof![(-100_000i64..100_000).prop_map(|v| v as f64 / 100.0), any::<u64>().prop_map(f64::from_bits)]
}

fn column(rows: usize) -> impl Strategy<Value = ArrayRef> {
    prop_oneof![
        prop::collection::vec(prop::option::weighted(0.9, any::<i32>()), rows)
            .prop_map(|v| Arc::new(Int32Array::from(v)) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.9, -1000i64..1000), rows)
            .prop_map(|v| Arc::new(Int64Array::from(v)) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.9, any::<u32>().prop_map(f32::from_bits)), rows)
            .prop_map(|v| Arc::new(Float32Array::from(v)) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.8, f64_value()), rows)
            .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.9, "[a-z\u{e9}\"\\\\]{0,4}"), rows)
            .prop_map(|v| Arc::new(StringArray::from(v)) as ArrayRef),
        prop::collection::vec(prop::option::weighted(0.9, prop::collection::vec(any::<i64>(), 0..3)), rows).prop_map(
            |v| {
                let mut b = ListBuilder::new(Int64Builder::new());
                for row in v {
                    match row {
                        Some(items) => b.append_value(items.into_iter().map(Some)),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }
        ),
    ]
}

fn table() -> impl Strategy<Value = Vec<RecordBatch>> {
    (1usize..4, 0usize..300).prop_flat_map(|(cols, rows)| {
        prop::collection::vec(column(rows), cols).prop_map(move |columns| {
            let fields: Vec<Field> = columns
                .iter()
                .enumerate()
                .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
                .collect();
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            // two batches, so that row groups and batches do not line up
            let half = batch.num_rows() / 2;
            vec![batch.slice(0, half), batch.slice(half, batch.num_rows() - half)]
        })
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn random_tables_round_trip(batches in table(), pcodec in any::<bool>(), group in 1usize..200, batch_size in 0i64..100) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        std::fs::write(&path, write(&batches, pcodec, group)).unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        prop_assert_eq!(file_info(&path).unwrap().0, rows as i64);
        let got: Vec<RecordBatch> = open(&path, None, batch_size).unwrap().collect::<Result<_, _>>().unwrap();
        prop_assert!(got.iter().all(|b| batch_size == 0 || b.num_rows() <= batch_size as usize));
        prop_assert_eq!(jsonl(&got), jsonl(&batches));
        // and the last column alone
        let last = format!("c{}", batches[0].num_columns() - 1);
        let got: Vec<RecordBatch> = open(&path, Some(&[last.as_str()]), batch_size).unwrap().collect::<Result<_, _>>().unwrap();
        let want: Vec<RecordBatch> = batches.iter().map(|b| b.project(&[b.num_columns() - 1]).unwrap()).collect();
        prop_assert_eq!(jsonl(&got), jsonl(&want));
    }

    #[test]
    fn corrupt_pages_never_crash(seed in any::<u64>(), flips in prop::collection::vec((any::<u32>(), 1u8..=255), 1..8)) {
        let file = std::fs::read(testdata().join("files/mixed.parquet")).unwrap();
        // corrupt only the data (the footer is at the end): the page headers and payloads
        let data_end = file.len() - 8 - u32::from_le_bytes(file[file.len() - 8..file.len() - 4].try_into().unwrap()) as usize;
        let mut bad = file.clone();
        for (pos, x) in &flips {
            let i = 4 + ((*pos as u64 ^ seed) as usize) % (data_end - 4);
            bad[i] ^= x;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.parquet");
        std::fs::write(&path, &bad).unwrap();
        // every outcome but a crash is fine: an error, or (rarely) rows
        if let Ok(reader) = open(&path, None, 0) {
            for b in reader {
                if b.is_err() {
                    break;
                }
            }
        }
    }
}
