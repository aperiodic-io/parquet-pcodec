//! Acceptance tests (b) and (c) on deterministic synthetic market data:
//! - `Auto` is at most 0.8 times the bytes of ZSTD(9) Parquet on trades, and on trades and L2
//!   books together (not on the L2 books alone), and every value round-trips;
//! - transcoding standard -> PCODEC -> standard is logically identical.
//!
//! (a), upstream `parquet` failing on PCODEC files, is `acceptance/rust-upstream`.

mod common;

use arrow_array::builder::{Float64Builder, ListBuilder};
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, TimestampMillisecondArray};
use bytes::Bytes;
use common::{Rng, batch_lines, codecs, lines};
use parquet_pcodec::parquet::arrow::ArrowWriter;
use parquet_pcodec::parquet::basic::{Compression, ZstdLevel};
use parquet_pcodec::parquet::file::properties::WriterProperties;
use parquet_pcodec::verify::verify;
use parquet_pcodec::{Target, WriterOptions, transcode};
use std::sync::Arc;

const TRADES: usize = 100_000;
const BOOKS: usize = 10_000;
const BATCH: usize = 25_000;

/// Trades: ms timestamps in bursts, prices on a 0.01 tick grid (a random walk), amounts in
/// lots of 0.0001, a side flag and trade ids with gaps.
fn trades() -> Vec<RecordBatch> {
    let mut rng = Rng(0x7472_6164);
    let (mut t, mut tick, mut id) = (1_735_689_600_000i64, 6_500_000i64, 4_000_000_000i64);
    let mut out = vec![];
    for _ in 0..TRADES / BATCH {
        let mut ts = Vec::with_capacity(BATCH);
        let mut price = Vec::with_capacity(BATCH);
        let mut amount = Vec::with_capacity(BATCH);
        let mut buy = Vec::with_capacity(BATCH);
        let mut ids = Vec::with_capacity(BATCH);
        for _ in 0..BATCH {
            t += if rng.below(3) == 0 { rng.below(400) as i64 } else { 0 };
            tick += rng.below(7) as i64 - 3;
            id += 1 + i64::from(rng.below(50) == 0) * rng.below(5) as i64;
            let lots = 1 + rng.below(20) * rng.below(20) * (1 + rng.below(3) * 10);
            ts.push(t);
            price.push(tick as f64 / 100.0);
            amount.push(lots as f64 / 10_000.0);
            buy.push(rng.below(2) == 0);
            ids.push(id);
        }
        out.push(
            RecordBatch::try_from_iter([
                ("ts", Arc::new(TimestampMillisecondArray::from(ts).with_timezone("UTC")) as ArrayRef),
                ("price", Arc::new(Float64Array::from(price)) as ArrayRef),
                ("amount", Arc::new(Float64Array::from(amount)) as ArrayRef),
                ("is_buy", Arc::new(BooleanArray::from(buy)) as ArrayRef),
                ("trade_id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ])
            .unwrap(),
        );
    }
    out
}

/// L2 snapshots every 100 ms of a simulated order book: resting sizes (lots of 0.001, heavy
/// tailed) keyed by price tick; between snapshots the mid moves by a few ticks and about a
/// third of the visible levels change size. Each snapshot lists the best 20 levels per side
/// (prices on the 0.01 tick grid, a few ticks empty) and their sizes, as list<double>.
fn books() -> Vec<RecordBatch> {
    const LEVELS: usize = 20;
    let mut rng = Rng(0x626f_6f6b);
    let (mut t, mut mid) = (1_735_689_600_000i64, 6_500_000i64);
    let lot = |rng: &mut Rng| -> u64 {
        match rng.below(10) {
            0 => [100, 250, 500, 1000][rng.below(4) as usize],
            1..=3 => 1 + rng.below(10),
            _ => 1 + rng.below(40) * rng.below(8),
        }
    };
    // resting size per price tick (0: empty level)
    let mut book = std::collections::HashMap::<i64, u64>::new();
    let mut out = vec![];
    for _ in 0..BOOKS / (BATCH / 5) {
        let mut ts = Vec::new();
        let mut lists: Vec<ListBuilder<Float64Builder>> = (0..4).map(|_| ListBuilder::new(Float64Builder::new())).collect();
        for _ in 0..BATCH / 5 {
            t += 100;
            mid += rng.below(5) as i64 - 2;
            ts.push(t);
            for (side, sign) in [(0usize, -1i64), (1, 1)] {
                let mut px = mid + i64::from(side == 1);
                let mut level = 0;
                while level < LEVELS {
                    let size = book.entry(px).or_insert_with(|| if rng.below(8) == 0 { 0 } else { lot(&mut rng) });
                    if rng.below(3) == 0 {
                        *size = if rng.below(10) == 0 { 0 } else { lot(&mut rng) };
                    }
                    if *size > 0 {
                        lists[side].values().append_value(px as f64 / 100.0);
                        lists[2 + side].values().append_value(*size as f64 / 1000.0);
                        level += 1;
                    }
                    px += sign;
                }
            }
            for l in &mut lists {
                l.append(true);
            }
        }
        let mut columns = vec![("ts", Arc::new(TimestampMillisecondArray::from(ts).with_timezone("UTC")) as ArrayRef)];
        for (name, l) in ["bid_px", "ask_px", "bid_sz", "ask_sz"].into_iter().zip(&mut lists) {
            columns.push((name, Arc::new(l.finish()) as ArrayRef));
        }
        out.push(RecordBatch::try_from_iter(columns).unwrap());
    }
    out
}

fn zstd9(batches: &[RecordBatch]) -> Bytes {
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(9).unwrap())).build();
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, batches[0].schema(), Some(props)).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.close().unwrap();
    Bytes::from(out)
}

/// `(Auto, ZSTD(9), base-only)` file sizes of `batches`; checks that the Auto file holds
/// exactly the input.
fn sizes(name: &str, batches: &[RecordBatch]) -> (usize, usize, usize) {
    let z9 = zstd9(batches).len();
    let (auto, _) = common::write(batches, WriterOptions::default());
    let (base, _) = common::write(batches, WriterOptions::default().with_policy(parquet_pcodec::Policy::Base));
    println!(
        "{name}: Auto {} bytes, ZSTD(9) {z9} bytes, ratio {:.3}; base ZSTD(3) {} bytes; codecs {:?}",
        auto.len(),
        auto.len() as f64 / z9 as f64,
        base.len(),
        codecs(&auto)
    );
    assert_eq!(lines(&auto), batch_lines(batches), "{name}: values differ");
    (auto.len(), z9, base.len())
}

#[test]
fn auto_is_at_most_0_8_of_zstd9_on_market_data() {
    let (ta, tz, _) = sizes("trades", &trades());
    let (la, lz, lb) = sizes("l2", &books());
    let total = (ta + la) as f64 / (tz + lz) as f64;
    println!("trades + l2: Auto {} bytes, ZSTD(9) {} bytes, ratio {total:.3}", ta + la, tz + lz);
    assert!(ta as f64 <= 0.8 * tz as f64, "trades: {ta} vs {tz}");
    assert!(total <= 0.8, "trades + l2: ratio {total:.3}");
    // the L2 lists alone do not reach 0.8: PCODEC loses to dictionary + ZSTD on them, so
    // Auto keeps the base codec there (ZSTD level 3, which ZSTD level 9 beats)
    assert!(la <= lb, "l2: Auto {la} above its base {lb}");
}

#[test]
fn transcode_round_trip_is_identical() {
    for batches in [trades(), books()] {
        let standard = zstd9(&batches);
        for policy in [parquet_pcodec::Policy::Auto, parquet_pcodec::Policy::Pcodec] {
            let mut pcodec = Vec::new();
            let target = Target::Pcodec(WriterOptions::default().with_policy(policy));
            transcode(standard.clone(), &mut pcodec, &target).unwrap();
            let pcodec = Bytes::from(pcodec);
            if policy == parquet_pcodec::Policy::Pcodec {
                assert!(codecs(&pcodec).iter().any(|(_, c)| c == "PCODEC"));
            }
            let mut back = Vec::new();
            transcode(pcodec.clone(), &mut back, &Target::standard()).unwrap();
            let back = Bytes::from(back);
            assert!(codecs(&back).iter().all(|(_, c)| c != "PCODEC"));
            assert_eq!(verify(standard.clone(), pcodec).unwrap(), None);
            assert_eq!(verify(standard.clone(), back.clone()).unwrap(), None);
            assert_eq!(lines(&back), batch_lines(&batches));
        }
    }
}
