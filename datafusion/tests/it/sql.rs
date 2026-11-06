//! SQL over PCODEC files: the same queries over a standard file and its TPARQUET copy give
//! the same rows; pruning (row-group statistics, page index, bloom filters), projection,
//! filter pushdown and `LIMIT` keep working; directories, globs and hive partitions.

use crate::common::*;
use crate::write::{copy, mixed};
use datafusion::physical_plan::{ExecutionPlan, collect, execute_stream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::path::Path;
use std::sync::Arc;

/// `std`: the standard control file; `pc`: its TPARQUET copy (PCODEC for `ts`, `price`,
/// `qty`, ZSTD for the rest); `other`: the PCODEC vector `mixed`, same schema, other rows.
async fn tables(ctx: &SessionContext, dir: &Path) {
    let control = lit(&vector_file("standard_control"));
    sql(ctx, &format!("CREATE EXTERNAL TABLE std STORED AS PARQUET LOCATION {control}")).await;
    let pc = dir.join("pc.parquet");
    sql(ctx, &format!("COPY std TO {} STORED AS TPARQUET", lit(&pc))).await;
    let r = crate::common::inspect::inspect_file(&pc);
    assert_eq!(r.column("price").codecs, ["PCODEC"]);
    assert_eq!(r.column("symbol").codecs, ["ZSTD"]);
    sql(ctx, &format!("CREATE EXTERNAL TABLE pc STORED AS TPARQUET LOCATION {}", lit(&pc))).await;
    sql(ctx, &format!("CREATE EXTERNAL TABLE other STORED AS TPARQUET LOCATION {}", lit(&vector_file("mixed")))).await;
}

const QUERIES: &[&str] = &[
    "SELECT * FROM {t}",
    "SELECT price, ts FROM {t}",
    "SELECT qty FROM {t} WHERE price > 95000 AND is_buy",
    "SELECT * FROM {t} WHERE CAST(ts AS BIGINT) BETWEEN 1735689600100 AND 1735689601000 ORDER BY ts, price",
    "SELECT * FROM {t} WHERE note IS NULL AND qty < 1.5",
    "SELECT * FROM {t} WHERE price IN (SELECT max(price) FROM {t})",
    // (float sums depend on the order of the rows, which depends on how the files are split
    // between partitions: the sums are over integers)
    "SELECT count(*) AS n, count(note) AS notes, sum(CAST(qty * 1e6 AS BIGINT)) AS q, avg(CAST(price * 100 AS BIGINT)) AS p, \
     min(ts) AS t0, max(ts) AS t1, min(price) AS p0, max(qty) AS q1 FROM {t}",
    "SELECT symbol, venue, count(*) AS n, sum(CAST(qty * 1e6 AS BIGINT)) AS q, max(price) AS p FROM {t} GROUP BY symbol, venue ORDER BY symbol, venue",
    "SELECT symbol, count(DISTINCT price) AS d FROM {t} GROUP BY symbol ORDER BY symbol",
    "SELECT ts, price, row_number() OVER (PARTITION BY symbol ORDER BY ts, price) AS r FROM {t} ORDER BY ts, price, r",
    "SELECT * FROM {t} ORDER BY price DESC, ts LIMIT 7",
    "SELECT ts FROM {t} WHERE CAST(ts AS BIGINT) % 7 = 0 ORDER BY ts",
    "SELECT CAST(price * 100 AS BIGINT) AS cents, -qty AS neg FROM {t} ORDER BY ts, cents",
    "SELECT DISTINCT is_buy FROM {t} ORDER BY is_buy",
];

#[tokio::test]
async fn queries_give_the_same_rows_on_pcodec_and_standard_files() {
    let dir = tempfile::tempdir().unwrap();
    for ctx in [ctx(), parallel_ctx()] {
        tables(&ctx, dir.path()).await;
        for q in QUERIES {
            let want = canonical_sql(&ctx, &q.replace("{t}", "std")).await;
            let got = canonical_sql(&ctx, &q.replace("{t}", "pc")).await;
            assert!(!want.is_empty(), "{q}");
            assert!(got == want, "{q}\n{got}\n{want}");
        }
        let _ = std::fs::remove_file(dir.path().join("pc.parquet"));
    }
}

#[tokio::test]
async fn joins_between_pcodec_and_standard_tables() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx();
    tables(&ctx, dir.path()).await;
    // the same rows: every row joins itself
    let q = "SELECT count(*) FROM std JOIN pc ON std.ts = pc.ts AND std.price = pc.price AND std.qty = pc.qty";
    assert_eq!(scalar(&ctx, q).await, "1500");
    // other rows: a join on the symbol, as with two standard files
    let q = "SELECT a.symbol, count(*) AS n, sum(CAST(b.qty * 1e6 AS BIGINT)) AS q FROM {a} a JOIN {b} b ON a.symbol = b.symbol \
             AND a.venue = b.venue WHERE a.ts < b.ts GROUP BY a.symbol ORDER BY a.symbol";
    let want = canonical_sql(&ctx, &q.replace("{a}", "std").replace("{b}", "std")).await;
    assert!(!want.is_empty());
    assert_eq!(canonical_sql(&ctx, &q.replace("{a}", "pc").replace("{b}", "pc")).await, want);
    let mixed_join = canonical_sql(&ctx, &q.replace("{a}", "std").replace("{b}", "other")).await;
    assert_eq!(canonical_sql(&ctx, &q.replace("{a}", "pc").replace("{b}", "other")).await, mixed_join);
    // union of a PCODEC and a standard table
    let q = "SELECT count(*) FROM (SELECT ts FROM pc UNION ALL SELECT ts FROM other)";
    assert_eq!(scalar(&ctx, q).await, "3000");
}

/// The value of the metric `name` (summed over the plan), as DataFusion prints it.
fn metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Vec<String> {
    let mut out = vec![];
    if let Some(m) = plan.metrics() {
        for m in m.iter() {
            if m.value().name() == name {
                out.push(m.value().to_string());
            }
        }
    }
    for child in plan.children() {
        out.extend(metric(child, name));
    }
    out
}

/// The sum of the counter `name` over the plan.
fn counter(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let mine: usize = plan
        .metrics()
        .map(|m| m.iter().filter(|m| m.value().name() == name).map(|m| m.value().as_usize()).sum())
        .unwrap_or(0);
    mine + plan.children().into_iter().map(|c| counter(c, name)).sum::<usize>()
}

/// The pruning metric `name` of the scans that pruned anything.
fn pruned(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Vec<String> {
    metric(plan, name).into_iter().filter(|m| m != "0 total → 0 matched").collect()
}

/// Runs `query` and returns its plan (with its metrics) and its rows.
async fn run(ctx: &SessionContext, query: &str) -> (Arc<dyn ExecutionPlan>, String) {
    let plan = ctx.sql(query).await.unwrap().create_physical_plan().await.unwrap();
    let batches = collect(plan.clone(), ctx.task_ctx()).await.unwrap();
    (plan, canonical_lines(&batches))
}

/// A millisecond timestamp literal (in UTC when `utc`), of the type of the `ts` columns.
fn ts_lit(ms: i64, utc: bool) -> String {
    let tz = if utc { "Some(\"UTC\")" } else { "None" };
    format!("arrow_cast({ms}, 'Timestamp(Millisecond, {tz})')")
}

#[tokio::test]
async fn row_group_statistics_prune_pcodec_files() {
    // multiple_row_groups: 5 row groups of 400 rows, ts increasing
    let ctx = ctx();
    sql(
        &ctx,
        &format!("CREATE EXTERNAL TABLE t STORED AS TPARQUET LOCATION {}", lit(&vector_file("multiple_row_groups"))),
    )
    .await;
    let rows = expected_rows("multiple_row_groups");
    let ts: Vec<i64> =
        rows.lines().map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["ts"].as_i64().unwrap()).collect();
    assert!(ts.windows(2).all(|w| w[0] <= w[1]));
    let cut = ts[399];
    let (plan, got) = run(&ctx, &format!("SELECT * FROM t WHERE ts <= {}", ts_lit(cut, true))).await;
    let want: String = rows.lines().zip(&ts).filter(|(_, t)| **t <= cut).map(|(l, _)| format!("{l}\n")).collect();
    assert_eq!(got, want);
    // four of the five row groups are pruned by their statistics
    assert_eq!(pruned(&plan, "row_groups_pruned_statistics"), ["5 total → 1 matched -> 1 fully matched"]);

    let text = canonical_sql(&ctx, &format!("EXPLAIN ANALYZE SELECT * FROM t WHERE ts <= {}", ts_lit(cut, true))).await;
    assert!(text.contains("row_groups_pruned_statistics=5 total → 1 matched"), "{text}");
}

#[tokio::test]
async fn page_index_and_bloom_filters_prune_pcodec_files() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = mixed(20_000, 21);
    let path = copy(&ctx, dir.path(), &[batch], "data_page_row_count_limit 1000, bloom_filter_on_write 'true'").await;
    let r = crate::common::inspect::inspect_file(&path);
    assert_eq!(r.column("ts").pages["DATA_PAGE_V2"], 20);
    sql(&ctx, &format!("CREATE EXTERNAL TABLE p STORED AS TPARQUET LOCATION {}", lit(&path))).await;
    sql(&ctx, "SET datafusion.execution.parquet.pushdown_filters = true").await;
    sql(&ctx, "SET datafusion.execution.parquet.bloom_filter_on_read = true").await;

    // ts is 1_700_000_000_000 + i * 97: one page of 1000 rows holds ts of rows 3000..3999
    let lo = 1_700_000_000_000i64 + 3000 * 97;
    let hi = lo + 999 * 97;
    let (plan, got) =
        run(&ctx, &format!("SELECT count(*) FROM p WHERE ts >= {} AND ts <= {}", ts_lit(lo, false), ts_lit(hi, false)))
            .await;
    assert_eq!(got, "{\"count(*)\":1000}\n");
    // the page index leaves the pages that overlap the range (pages hold about 1000 rows)
    let pages = pruned(&plan, "page_index_rows_pruned");
    assert_eq!(pages.len(), 1);
    assert!(pages[0].starts_with("20.00 K total → ") && pages[0].ends_with(" K matched"), "{pages:?}");
    let matched: f64 = pages[0]["20.00 K total → ".len()..].trim_end_matches(" K matched").parse().unwrap();
    assert!((1.0..=3.1).contains(&matched), "{pages:?}");

    // a value inside the statistics' range that no row has: the bloom filter prunes the row group
    let (plan, got) = run(&ctx, "SELECT count(*) FROM p WHERE flag = 12345").await;
    assert_eq!(got, "{\"count(*)\":0}\n");
    assert_eq!(pruned(&plan, "row_groups_pruned_bloom_filter"), ["1 total → 0 matched"]);
}

#[tokio::test]
async fn limit_and_projection_are_pushed_into_the_scan() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let path =
        copy(&ctx, dir.path(), &[mixed(50_000, 22)], "data_page_row_count_limit 1000, max_row_group_size 10000").await;
    sql(&ctx, &format!("CREATE EXTERNAL TABLE p STORED AS TPARQUET LOCATION {}", lit(&path))).await;
    let plan = canonical_sql(&ctx, "EXPLAIN SELECT price FROM p LIMIT 5").await;
    assert!(plan.contains("projection=[price]") && plan.contains("limit=5"), "{plan}");
    let (plan, got) = run(&ctx, "SELECT price FROM p LIMIT 5").await;
    assert_eq!(got.lines().count(), 5);
    let all = canonical_sql(&ctx, "SELECT price FROM p").await;
    assert!(all.starts_with(&got));
    // a LIMIT reads one row group of five
    let scanned = counter(&plan, "bytes_scanned");
    let (all_plan, _) = run(&ctx, "SELECT price FROM p").await;
    let all_scanned = counter(&all_plan, "bytes_scanned");
    assert!(scanned > 0 && scanned * 3 < all_scanned, "{scanned} of {all_scanned}");
    // a projection reads only its column
    let (one, _) = run(&ctx, "SELECT name FROM p").await;
    let one = counter(&one, "bytes_scanned");
    let (every, _) = run(&ctx, "SELECT * FROM p").await;
    let every = counter(&every, "bytes_scanned");
    assert!(one * 2 < every, "{one} of {every}");
}

#[tokio::test]
async fn directories_globs_and_hive_partitions() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    // three files with one schema in one directory, plus a file of another format
    let flat = dir.path().join("flat");
    std::fs::create_dir(&flat).unwrap();
    for i in 0..3 {
        crate::write::register(&ctx, &[mixed(1000, 30 + i)]);
        sql(&ctx, &format!("COPY t TO {} STORED AS TPARQUET", lit(&flat.join(format!("part-{i}.parquet"))))).await;
    }
    std::fs::write(flat.join("README.txt"), "not parquet").unwrap();
    sql(&ctx, &format!("CREATE EXTERNAL TABLE d STORED AS TPARQUET LOCATION '{}/'", flat.display())).await;
    assert_eq!(scalar(&ctx, "SELECT count(*) FROM d").await, "3000");
    sql(&ctx, &format!("CREATE EXTERNAL TABLE g STORED AS TPARQUET LOCATION '{}/part-[01].parquet'", flat.display()))
        .await;
    assert_eq!(scalar(&ctx, "SELECT count(*) FROM g").await, "2000");
    let df = ctx.read_parquet(format!("{}/", flat.display()).as_str(), Default::default()).await.unwrap();
    assert_eq!(df.count().await.unwrap(), 3000);

    // hive-style partitions written by COPY, inferred on read, pruned by a filter on them
    let hive = dir.path().join("hive");
    crate::write::register(&ctx, &[mixed(4000, 40)]);
    sql(&ctx, &format!("COPY (SELECT *, CAST(ts AS BIGINT) % 3 AS bucket FROM t) TO '{}/' STORED AS TPARQUET PARTITIONED BY (bucket)", hive.display()))
        .await;
    sql(&ctx, &format!("CREATE EXTERNAL TABLE h STORED AS TPARQUET LOCATION '{}/'", hive.display())).await;
    let q =
        "SELECT bucket, count(*) AS n, sum(flag % 1000) AS f, max(price) AS p FROM {} GROUP BY bucket ORDER BY bucket";
    let want =
        canonical_sql(&ctx, &q.replace("{}", "(SELECT *, CAST(CAST(ts AS BIGINT) % 3 AS VARCHAR) AS bucket FROM t)"))
            .await;
    assert_eq!(canonical_sql(&ctx, &q.replace("{}", "h")).await, want);
    let (plan, got) = run(&ctx, "SELECT price FROM h WHERE bucket = '1'").await;
    assert_eq!(got.lines().count(), 1333);
    let plan_text = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("file_groups={1 group: [[") && plan_text.matches("bucket=1").count() == 1,
        "{plan_text}"
    );
}

#[tokio::test]
async fn streams_of_many_batches() {
    // a file read as a stream of small batches, as DataFusion's operators see it
    let ctx = ctx();
    sql(&ctx, "SET datafusion.execution.batch_size = 64").await;
    sql(&ctx, &format!("CREATE EXTERNAL TABLE t STORED AS TPARQUET LOCATION {}", lit(&vector_file("mixed")))).await;
    let plan = ctx.sql("SELECT * FROM t").await.unwrap().create_physical_plan().await.unwrap();
    let mut stream = execute_stream(plan, ctx.task_ctx()).unwrap();
    let (mut batches, mut lines) = (0, String::new());
    while let Some(b) = stream.next().await {
        let b = b.unwrap();
        assert!(b.num_rows() <= 64);
        lines.push_str(&canonical_lines(&[b]));
        batches += 1;
    }
    assert!(batches >= 1500 / 64);
    assert!(lines == expected_rows("mixed"));
}
