//! The conformance files: what each holds and how it is written.

use crate::rng::Rng;
use arrow_array::builder::{Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringDictionaryBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;
use tparquet::parquet::arrow::{ArrowSchemaConverter, ArrowWriter, add_encoded_arrow_schema_to_metadata};
use tparquet::parquet::basic::{Compression, Encoding, PcodecLevel, ZstdLevel};
use tparquet::parquet::file::properties::{WriterProperties, WriterVersion};
use tparquet::parquet::file::writer::SerializedFileWriter;
use tparquet::{Policy, Result, Writer, WriterOptions};

/// One conformance file.
pub struct Case {
    pub name: &'static str,
    pub description: &'static str,
    /// The batches written, one row group each.
    pub batches: Vec<RecordBatch>,
    pub how: How,
}

/// How a case is written.
pub enum How {
    /// The high-level writer with these options.
    Writer(WriterOptions),
    /// `ArrowWriter` with these properties.
    Arrow(WriterProperties),
    /// One row group without rows and PCODEC column chunks without pages.
    EmptyRowGroup,
}

fn pcodec() -> Compression {
    Compression::PCODEC(PcodecLevel::default())
}

fn zstd(level: i32) -> Compression {
    Compression::ZSTD(ZstdLevel::try_new(level).expect("valid zstd level"))
}

fn batch(columns: Vec<(&str, ArrayRef, bool)>) -> RecordBatch {
    let fields: Vec<Field> =
        columns.iter().map(|(n, a, nullable)| Field::new(*n, a.data_type().clone(), *nullable)).collect();
    let arrays = columns.into_iter().map(|(_, a, _)| a).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("valid batch")
}

fn arc(a: impl Array + 'static) -> ArrayRef {
    Arc::new(a)
}

/// `Some(v)` except every `every`-th row
fn holes<T>(v: Vec<T>, every: usize) -> Vec<Option<T>> {
    v.into_iter().enumerate().map(|(i, x)| (i % every != every - 1).then_some(x)).collect()
}

/// INT32, INT64, FLOAT, DOUBLE columns: smooth, random and decimal-like.
fn primitives(rng: &mut Rng, rows: usize) -> (Vec<i32>, Vec<i64>, Vec<f32>, Vec<f64>) {
    let mut acc = 0i64;
    let i32s = (0..rows).map(|i| (i as i32 * 37) % 1000 - 500).collect();
    let i64s = (0..rows)
        .map(|_| {
            acc += rng.below(1000) as i64;
            1_000_000_000_000 + acc
        })
        .collect();
    let f32s = (0..rows).map(|i| (i % 400) as f32 * 0.25 - 50.0).collect();
    let f64s = (0..rows).map(|_| rng.below(1_000_000) as f64 / 1000.0).collect();
    (i32s, i64s, f32s, f64s)
}

fn primitives_case(rng: &mut Rng, optional: bool) -> RecordBatch {
    let (a, b, c, d) = primitives(rng, 3000);
    if optional {
        batch(vec![
            ("i32", arc(Int32Array::from(holes(a, 7))), true),
            ("i64", arc(Int64Array::from(holes(b, 5))), true),
            ("f32", arc(Float32Array::from(holes(c, 3))), true),
            ("f64", arc(Float64Array::from(holes(d, 2))), true),
        ])
    } else {
        batch(vec![
            ("i32", arc(Int32Array::from(a)), false),
            ("i64", arc(Int64Array::from(b)), false),
            ("f32", arc(Float32Array::from(c)), false),
            ("f64", arc(Float64Array::from(d)), false),
        ])
    }
}

fn lists(rng: &mut Rng, rows: usize) -> RecordBatch {
    let mut ints = ListBuilder::new(Int64Builder::new());
    let mut levels = ListBuilder::new(Float64Builder::new());
    for i in 0..rows {
        match i % 9 {
            0 => ints.append_null(),
            1 => ints.append_value([]),
            _ => ints.append_value((0..rng.below(6)).map(|j| (j % 4 != 3).then_some((i as i64) * 10 + j as i64))),
        }
        if i % 11 == 0 {
            levels.append_null();
        } else {
            let mid = 25_000 + (i % 200) as i64;
            levels.append_value((0..10).map(|j| Some((mid - j) as f64 * 0.01)));
        }
    }
    batch(vec![("ints", arc(ints.finish()), true), ("levels", arc(levels.finish()), true)])
}

fn nested_struct(rng: &mut Rng, rows: usize) -> RecordBatch {
    let ids = Int32Array::from_iter_values(0..rows as i32);
    let bid: Vec<f64> = (0..rows).map(|i| 1000.0 + ((i * 3) % 500) as f64 * 0.5).collect();
    let ask: Vec<f64> = bid.iter().map(|b| b + 0.5).collect();
    let size = Int64Array::from(holes((0..rows).map(|_| rng.below(100) as i64 * 100).collect(), 6));
    let quote = StructArray::from(vec![
        (Arc::new(Field::new("bid", DataType::Float64, false)), arc(Float64Array::from(bid))),
        (Arc::new(Field::new("ask", DataType::Float64, false)), arc(Float64Array::from(ask))),
        (Arc::new(Field::new("size", DataType::Int64, true)), arc(size)),
    ]);
    let mut tags = ListBuilder::new(Int32Builder::new());
    for i in 0..rows {
        tags.append_value((0..i % 3).map(|j| Some(j as i32)));
    }
    let tags = tags.finish();
    let book = StructArray::from(vec![
        (Arc::new(Field::new("id", DataType::Int32, false)), arc(ids)),
        (Arc::new(Field::new("quote", quote.data_type().clone(), false)), arc(quote)),
        (Arc::new(Field::new("tags", tags.data_type().clone(), false)), arc(tags)),
    ]);
    batch(vec![("book", arc(book), false)])
}

fn temporal(rows: usize) -> RecordBatch {
    let base_ms = 1_735_689_600_000i64; // 2025-01-01T00:00:00Z
    let ms = TimestampMillisecondArray::from_iter_values((0..rows as i64).map(|i| base_ms + i * 1250));
    let us = TimestampMicrosecondArray::from_iter_values((0..rows as i64).map(|i| base_ms * 1000 + i * 999_983));
    let ns = TimestampNanosecondArray::from_iter_values((0..rows as i64).map(|i| base_ms * 1_000_000 + i * 7_000_001));
    let local = TimestampMillisecondArray::from_iter_values((0..rows as i64).map(|i| base_ms + i * 60_000));
    batch(vec![
        ("ts_ms_utc", arc(ms.with_timezone("UTC")), false),
        ("ts_us", arc(us), false),
        ("ts_ns_utc", arc(ns.with_timezone("UTC")), false),
        ("ts_ms_ny", arc(local.with_timezone("America/New_York")), false),
        ("date32", arc(Date32Array::from_iter_values((0..rows as i32).map(|i| 20_089 + i / 24))), false),
        (
            "time64_us",
            arc(Time64MicrosecondArray::from_iter_values((0..rows as i64).map(|i| i * 1_000_003 % 86_400_000_000))),
            false,
        ),
        (
            "dec18_4",
            arc(Decimal128Array::from_iter_values((0..rows as i128).map(|i| 1_234_500 + i * 25))
                .with_precision_and_scale(18, 4)
                .expect("valid decimal")),
            false,
        ),
        (
            "dec9_2",
            arc(Decimal128Array::from(holes((0..rows as i128).map(|i| -50_000 + i * 3).collect(), 10))
                .with_precision_and_scale(9, 2)
                .expect("valid decimal")),
            true,
        ),
    ])
}

fn decimal_floats(rng: &mut Rng, rows: usize) -> RecordBatch {
    let mut p = 6_500_000i64;
    let cents: Vec<f64> = (0..rows)
        .map(|_| {
            p += rng.below(21) as i64 - 10;
            p as f64 / 100.0
        })
        .collect();
    let btc: Vec<f64> = (0..rows).map(|_| (rng.below(500_000_000) + 1) as f64 / 1e8).collect();
    let halves: Vec<f32> = (0..rows).map(|i| ((i * 13) % 2000) as f32 * 0.5).collect();
    let tenths: Vec<f32> = (0..rows).map(|i| (i % 1000) as f32 / 10.0).collect();
    // mostly on the grid, a few values off it (exceptions)
    let mut exceptions = cents.clone();
    for i in (0..rows).step_by(250) {
        exceptions[i] = std::f64::consts::PI * i as f64;
    }
    batch(vec![
        ("price_cents", arc(Float64Array::from(cents)), false),
        ("amount_1e8", arc(Float64Array::from(btc)), false),
        ("f32_halves", arc(Float32Array::from(halves)), false),
        ("f32_tenths", arc(Float32Array::from(tenths)), false),
        ("with_exceptions", arc(Float64Array::from(exceptions)), false),
    ])
}

fn float_specials(rows: usize) -> RecordBatch {
    let d = [
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        f64::from_bits(0xfff8_0000_0000_0000),
        f64::from_bits(0x7ff0_0000_0000_0001),
        f64::from_bits(0x7ff8_dead_beef_0001),
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        f64::MAX,
        f64::MIN,
        1.5,
        -2.25,
    ];
    let f = [
        0.0,
        -0.0,
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::NAN,
        f32::from_bits(0xffc0_0000),
        f32::from_bits(0x7f80_0001),
        f32::from_bits(0x7fc0_beef),
        f32::MIN_POSITIVE,
        f32::from_bits(1),
        f32::MAX,
        f32::MIN,
        1.5,
    ];
    batch(vec![
        ("f64", arc(Float64Array::from_iter_values((0..rows).map(|i| d[i % d.len()]))), false),
        ("f32", arc(Float32Array::from_iter_values((0..rows).map(|i| f[i % f.len()]))), false),
        ("f64_opt", arc(Float64Array::from(holes((0..rows).map(|i| d[(i * 5) % d.len()]).collect(), 4))), true),
    ])
}

/// A trades-like table: PCODEC numbers next to dictionary-encoded ZSTD strings.
fn mixed(rng: &mut Rng, rows: usize, offset: usize) -> RecordBatch {
    let mut t = 1_735_689_600_000i64 + offset as i64 * 37;
    let ts = TimestampMillisecondArray::from_iter_values((0..rows).map(|_| {
        t += rng.below(40) as i64;
        t
    }));
    let mut p = 9_500_000i64;
    let price = Float64Array::from_iter_values((0..rows).map(|_| {
        p += rng.below(11) as i64 - 5;
        p as f64 / 100.0
    }));
    let qty = Float64Array::from_iter_values((0..rows).map(|_| (rng.below(5000) + 1) as f64 / 1000.0));
    let mut sym = StringDictionaryBuilder::<Int32Type>::new();
    let mut venue = Vec::with_capacity(rows);
    let symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"];
    for _ in 0..rows {
        sym.append_value(symbols[rng.below(4) as usize]);
        venue.push(["XNAS", "XNYS", "ARCX"][rng.below(3) as usize]);
    }
    let side = BooleanArray::from_iter((0..rows).map(|_| Some(rng.below(2) == 0)));
    let note = StringArray::from(holes((0..rows).map(|i| format!("trade #{i} ünïcode \"quoted\"\n")).collect(), 3));
    batch(vec![
        ("ts", arc(ts.with_timezone("UTC")), false),
        ("price", arc(price), false),
        ("qty", arc(qty), false),
        ("symbol", arc(sym.finish()), false),
        ("venue", arc(StringArray::from(venue)), false),
        ("is_buy", arc(side), false),
        ("note", arc(note), true),
    ])
}

fn v1_props() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(pcodec())
        .set_data_page_row_count_limit(1000)
        .set_write_batch_size(500)
        .build()
}

fn v1_pages(rng: &mut Rng) -> RecordBatch {
    let rows = 2500;
    let (a, b, _, d) = primitives(rng, rows);
    let random: Vec<i64> = (0..rows).map(|_| rng.next() as i64).collect();
    batch(vec![
        // required: PLAIN or dictionary pages of plain values (PCO, DECIMAL)
        ("i32", arc(Int32Array::from(a)), false),
        ("f64", arc(Float64Array::from(d)), false),
        // optional: levels in front of the values, the width no longer divides (ZSTD)
        ("i64_opt", arc(Int64Array::from(holes(b, 4))), true),
        // incompressible (RAW)
        ("random", arc(Int64Array::from(random)), false),
        // dictionary-encoded strings
        ("tag", arc(StringArray::from_iter_values((0..rows).map(|i| format!("t{}", i % 9)))), false),
    ])
}

fn v2_props() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(pcodec())
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_column_encoding("tag".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_data_page_row_count_limit(1000)
        .set_write_batch_size(500)
        .build()
}

/// Every case, in a fixed order.
pub fn all() -> Vec<Case> {
    let mut rng = Rng(0x5043_0001);
    let rng = &mut rng;
    let pcodec_policy = || WriterOptions::default().with_policy(Policy::Pcodec);
    vec![
        Case {
            name: "primitives_required",
            description: "INT32, INT64, FLOAT, DOUBLE, required, PCODEC",
            batches: vec![primitives_case(rng, false)],
            how: How::Writer(pcodec_policy()),
        },
        Case {
            name: "primitives_optional",
            description: "INT32, INT64, FLOAT, DOUBLE with nulls, PCODEC",
            batches: vec![primitives_case(rng, true)],
            how: How::Writer(pcodec_policy()),
        },
        Case {
            name: "lists",
            description: "list<int64> and list<double> with null lists, empty lists and null items, PCODEC",
            batches: vec![lists(rng, 1200)],
            how: How::Writer(pcodec_policy()),
        },
        Case {
            name: "nested_struct",
            description: "struct<id int32, quote struct<bid, ask double, size int64>, tags list<int32>>, PCODEC",
            batches: vec![nested_struct(rng, 2000)],
            how: How::Writer(pcodec_policy()),
        },
        Case {
            name: "temporal",
            description: "timestamps (ms, us, ns; UTC, none, America/New_York), date32, time64, decimals in INT64 and INT32, PCODEC",
            batches: vec![temporal(1200)],
            how: How::Writer(pcodec_policy()),
        },
        Case {
            name: "decimal_floats",
            description: "decimal-valued doubles and floats (prices, 1e-8 amounts), some off the grid; DECIMAL payloads",
            batches: vec![decimal_floats(rng, 1500)],
            how: How::Writer(pcodec_policy()),
        },
        Case {
            name: "float_specials",
            description: "NaN payloads (quiet, signalling, negative), +-0, +-inf, subnormals, extremes",
            batches: vec![float_specials(1000)],
            how: How::Writer(pcodec_policy()),
        },
        Case {
            name: "empty",
            description: "a row group with 0 rows and PCODEC column chunks without pages",
            batches: vec![primitives_case(rng, true).slice(0, 0)],
            how: How::EmptyRowGroup,
        },
        Case {
            name: "multiple_row_groups",
            description: "5 row groups of 400 rows, Auto policy",
            batches: (0..5).map(|i| mixed(rng, 400, i * 400)).collect(),
            how: How::Writer(WriterOptions::default()),
        },
        Case {
            name: "pages_v1",
            description: "PCODEC with DATA_PAGE (V1) pages and dictionary pages; RAW, PCO, DECIMAL and ZSTD payloads",
            batches: vec![v1_pages(rng)],
            how: How::Arrow(v1_props()),
        },
        Case {
            name: "pages_v2",
            description: "PCODEC with DATA_PAGE_V2 pages, PLAIN, several pages per chunk, a V2 page stored uncompressed",
            batches: vec![v1_pages(rng)],
            how: How::Arrow(v2_props()),
        },
        Case {
            name: "mixed",
            description: "trades: PCODEC numeric columns next to dictionary-encoded ZSTD strings and booleans (Auto)",
            batches: vec![mixed(rng, 1500, 0)],
            how: How::Writer(WriterOptions::default()),
        },
        Case {
            name: "standard_control",
            description: "control: the mixed table as standard Parquet (ZSTD level 3, no PCODEC)",
            batches: vec![mixed(rng, 1500, 0)],
            how: How::Arrow(WriterProperties::builder().set_compression(zstd(3)).build()),
        },
    ]
}

/// Writes a case to Parquet bytes.
pub fn write(case: &Case) -> Result<Vec<u8>> {
    let schema: SchemaRef = case.batches[0].schema();
    let mut out = Vec::new();
    match &case.how {
        How::Writer(options) => {
            let mut w = Writer::try_new(&mut out, schema, options.clone())?;
            for b in &case.batches {
                w.write(b)?;
                w.flush()?;
            }
            w.close()?;
        }
        How::Arrow(props) => {
            let mut w = ArrowWriter::try_new(&mut out, schema, Some(props.clone()))?;
            for b in &case.batches {
                w.write(b)?;
                w.flush()?;
            }
            w.close()?;
        }
        How::EmptyRowGroup => {
            let descr = ArrowSchemaConverter::new().convert(&schema)?;
            let mut props = WriterProperties::builder().set_compression(pcodec()).build();
            add_encoded_arrow_schema_to_metadata(&schema, &mut props);
            let mut w = SerializedFileWriter::new(&mut out, descr.root_schema_ptr(), Arc::new(props))?;
            let mut rg = w.next_row_group()?;
            while let Some(c) = rg.next_column()? {
                c.close()?;
            }
            rg.close()?;
            w.close()?;
        }
    }
    Ok(out)
}
