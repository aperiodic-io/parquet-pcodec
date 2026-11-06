// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! tparquet fork: round trips of Arrow data written with [`Compression::PCODEC`].

use std::sync::Arc;

use arrow_array::builder::{Float64Builder, ListBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, StructArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Encoding, PageType, PcodecLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Every combination of page version, dictionary and level the tests write.
fn all_props() -> Vec<WriterProperties> {
    let mut props = vec![];
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        for dictionary in [false, true] {
            for level in [0, 8, 12] {
                let mut builder = WriterProperties::builder()
                    .set_writer_version(version)
                    .set_compression(Compression::PCODEC(PcodecLevel::try_new(level).unwrap()))
                    .set_dictionary_enabled(dictionary)
                    .set_data_page_row_count_limit(300)
                    .set_write_batch_size(100);
                if !dictionary {
                    builder = builder.set_encoding(Encoding::PLAIN);
                }
                props.push(builder.build());
            }
        }
    }
    props
}

fn write(batch: &RecordBatch, props: WriterProperties) -> Bytes {
    let mut file = vec![];
    let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    Bytes::from(file)
}

fn read(file: Bytes) -> RecordBatch {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
}

/// Writes `batch` with every property set, checks the file is marked as PCODEC and reads
/// back the same data, bit for bit.
fn roundtrip(batch: RecordBatch) {
    for props in all_props() {
        let file = write(&batch, props.clone());
        let reader = SerializedFileReader::new(file.clone()).unwrap();
        let metadata = reader.metadata();
        for column in metadata.row_group(0).columns() {
            assert!(
                matches!(column.compression(), Compression::PCODEC(_)),
                "{column:?}"
            );
        }
        let kv = metadata.file_metadata().key_value_metadata().unwrap();
        assert!(
            kv.iter()
                .any(|kv| kv.key == "parquet.pcodec.version" && kv.value.as_deref() == Some("1"))
        );
        let actual = read(file);
        assert_eq!(actual.schema(), batch.schema());
        for (a, b) in actual.columns().iter().zip(batch.columns()) {
            assert_eq!(a.to_data(), b.to_data(), "{props:?}");
        }
    }
}

fn one_column(name: &str, values: ArrayRef) -> RecordBatch {
    let field = Field::new(name, values.data_type().clone(), values.null_count() > 0);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![values]).unwrap()
}

const N: i64 = 1000;

#[test]
fn pcodec_int32() {
    let required = Int32Array::from_iter_values((0..N as i32).map(|v| v * 3 - 700));
    roundtrip(one_column("v", Arc::new(required)));
    let optional = Int32Array::from_iter((0..N as i32).map(|v| (v % 7 != 0).then_some(v)));
    roundtrip(one_column("v", Arc::new(optional)));
}

#[test]
fn pcodec_int64() {
    let required = Int64Array::from_iter_values((0..N).map(|v| v * 1_000_003));
    roundtrip(one_column("v", Arc::new(required)));
    let optional = Int64Array::from_iter((0..N).map(|v| (v % 5 != 0).then_some(v)));
    roundtrip(one_column("v", Arc::new(optional)));
}

#[test]
fn pcodec_float() {
    let required = Float32Array::from_iter_values((0..N).map(|v| v as f32 / 4.0));
    roundtrip(one_column("v", Arc::new(required)));
    let optional = Float32Array::from_iter((0..N).map(|v| (v % 3 != 0).then_some(v as f32 * 0.1)));
    roundtrip(one_column("v", Arc::new(optional)));
}

#[test]
fn pcodec_double() {
    let required = Float64Array::from_iter_values((0..N).map(|v| v as f64 / 100.0));
    roundtrip(one_column("v", Arc::new(required)));
    let optional =
        Float64Array::from_iter((0..N).map(|v| (v % 2 != 0).then_some((v as f64).sqrt())));
    roundtrip(one_column("v", Arc::new(optional)));
}

#[test]
fn pcodec_float_special_values_bit_exact() {
    let specials = [
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        f64::from_bits(0x7ff8_0000_dead_beef),
        f64::from_bits(0xfff0_0000_0000_0001),
        f64::MIN_POSITIVE / 2.0,
        1.25,
    ];
    let values: Vec<f64> = (0..N as usize)
        .map(|i| specials[i % specials.len()])
        .collect();
    for props in all_props() {
        let file = write(
            &one_column("v", Arc::new(Float64Array::from(values.clone()))),
            props,
        );
        let actual = read(file);
        let actual = actual
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let bits: Vec<u64> = actual.values().iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits, values.iter().map(|v| v.to_bits()).collect::<Vec<_>>());
    }
}

#[test]
fn pcodec_logical_types() {
    let dates = Date32Array::from_iter_values(19_000..19_000 + N as i32);
    roundtrip(one_column("d", Arc::new(dates)));
    let ts =
        TimestampMillisecondArray::from_iter_values((0..N).map(|v| 1_700_000_000_000 + v * 250));
    roundtrip(one_column("ts", Arc::new(ts.with_timezone("UTC"))));
    let dec = Decimal128Array::from_iter_values((0..N as i128).map(|v| v * 25));
    roundtrip(one_column(
        "dec",
        Arc::new(dec.with_precision_and_scale(18, 2).unwrap()),
    ));
}

#[test]
fn pcodec_other_types_fall_back() {
    // not modelled by pcodec: the payload is ZSTD or RAW, and still round trips
    let strings = StringArray::from_iter_values((0..N).map(|v| format!("value {v}")));
    roundtrip(one_column("s", Arc::new(strings)));
    let booleans = BooleanArray::from_iter((0..N).map(|v| Some(v % 3 == 0)));
    roundtrip(one_column("b", Arc::new(booleans)));
}

#[test]
fn pcodec_nested() {
    let mut levels = ListBuilder::new(Float64Builder::new());
    for i in 0..N {
        if i % 11 == 0 {
            levels.append_null();
        } else {
            levels.append_value((0..i % 5).map(|j| Some(100.0 + (i + j) as f64 / 100.0)));
        }
    }
    let levels: ArrayRef = Arc::new(levels.finish());
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..N));
    let nested = StructArray::from(vec![
        (Arc::new(Field::new("id", DataType::Int64, false)), ids),
        (
            Arc::new(Field::new("levels", levels.data_type().clone(), true)),
            levels,
        ),
    ]);
    roundtrip(one_column("book", Arc::new(nested)));
}

#[test]
fn pcodec_page_types() {
    let batch = one_column("v", Arc::new(Int64Array::from_iter_values(0..N)));
    for props in all_props() {
        let expected_data_page = match props.writer_version() {
            WriterVersion::PARQUET_1_0 => PageType::DATA_PAGE,
            WriterVersion::PARQUET_2_0 => PageType::DATA_PAGE_V2,
        };
        let dictionary = props.dictionary_enabled(&"v".into());
        let reader = SerializedFileReader::new(write(&batch, props)).unwrap();
        let pages = reader
            .get_row_group(0)
            .unwrap()
            .get_column_page_reader(0)
            .unwrap()
            .map(|p| p.unwrap().page_type())
            .collect::<Vec<_>>();
        assert_eq!(pages[0] == PageType::DICTIONARY_PAGE, dictionary);
        assert!(pages.len() > 2, "{pages:?}");
        assert!(
            pages[usize::from(dictionary)..]
                .iter()
                .all(|t| *t == expected_data_page)
        );
    }
}
