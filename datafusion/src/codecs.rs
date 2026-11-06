//! Which leaf columns of a file get PCODEC, and the writer properties that follow: a port of
//! `rust/tparquet/src/writer.rs` (`Writer::open`) to DataFusion's writer.

use crate::options::{Policy, TparquetOptions, covers, resolve};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use parquet::basic::{Compression, Encoding, PcodecLevel, Type};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

/// A leaf column of the file.
#[derive(Debug, Clone)]
struct Leaf {
    path: ColumnPath,
    /// `None` when the column is not eligible for PCODEC.
    policy: Option<Policy>,
}

/// Whether PCODEC models the physical type (spec section 3).
fn eligible(t: Type) -> bool {
    matches!(t, Type::INT32 | Type::INT64 | Type::FLOAT | Type::DOUBLE)
}

/// The codec choice of one file: its schema, the base writer properties (DataFusion's), and
/// the policy of every leaf.
#[derive(Debug, Clone)]
pub(crate) struct Codecs {
    schema: SchemaRef,
    base: WriterProperties,
    level: PcodecLevel,
    leaves: Vec<Leaf>,
}

impl Codecs {
    /// The codecs of a file of `schema`; fails when an override names no column.
    pub(crate) fn try_new(schema: SchemaRef, base: WriterProperties, options: &TparquetOptions) -> Result<Self> {
        let descr = ArrowSchemaConverter::new().with_coerce_types(base.coerce_types()).convert(&schema)?;
        let leaves: Vec<Leaf> = descr
            .columns()
            .iter()
            .map(|c| Leaf {
                path: c.path().clone(),
                policy: eligible(c.physical_type())
                    .then(|| resolve(&c.path().string(), &options.columns, options.policy)),
            })
            .collect();
        if let Some((name, _)) =
            options.columns.iter().find(|(name, _)| !leaves.iter().any(|l| covers(name, &l.path.string())))
        {
            return Err(DataFusionError::Configuration(format!(
                "tparquet: policy for {name:?}, but there is no such column"
            )));
        }
        Ok(Self { schema, base, level: options.level, leaves })
    }

    /// The schema the file is written with.
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The writer properties with PCODEC (`PLAIN`, no dictionary) on the leaves where `pcodec`
    /// is set, and V2 pages when any is (or when `v2`).
    fn properties(&self, pcodec: &[bool], v2: bool) -> WriterProperties {
        let mut b = self.base.clone().into_builder();
        for (leaf, _) in self.leaves.iter().zip(pcodec).filter(|(_, p)| **p) {
            b = b
                .set_column_compression(leaf.path.clone(), Compression::PCODEC(self.level))
                .set_column_encoding(leaf.path.clone(), Encoding::PLAIN)
                .set_column_dictionary_enabled(leaf.path.clone(), false);
        }
        if v2 || pcodec.contains(&true) {
            b = b.set_writer_version(WriterVersion::PARQUET_2_0);
        }
        b.build()
    }

    /// Per leaf, whether it gets PCODEC: the `Pcodec` leaves, and the `Auto` leaves that are
    /// smaller with PCODEC on `sample` (without one, the `Auto` leaves keep the base codec).
    ///
    /// Each Auto leaf is compared with V2 pages for both codecs. A file with PCODEC is written
    /// with V2 pages, which can make the base columns larger (V2 pages store levels
    /// uncompressed): when only Auto leaves would use PCODEC, the file keeps the base codec
    /// everywhere if that, with the base properties' page version, is smaller in total
    /// (spec section 6).
    pub(crate) fn choose(&self, sample: Option<&RecordBatch>) -> Result<Vec<bool>> {
        let forced: Vec<bool> = self.leaves.iter().map(|l| l.policy == Some(Policy::Pcodec)).collect();
        let auto: Vec<bool> = self.leaves.iter().map(|l| l.policy == Some(Policy::Auto)).collect();
        let Some(batch) = sample.filter(|_| auto.contains(&true)) else {
            return Ok(forced);
        };
        let none = vec![false; self.leaves.len()];
        let base_v2 = self.chunk_sizes(batch, &none, true)?;
        let pcodec = self.chunk_sizes(batch, &auto, true)?;
        let choice: Vec<bool> =
            (0..self.leaves.len()).map(|i| forced[i] || (auto[i] && pcodec[i] < base_v2[i])).collect();
        let v2_total: i64 = (0..choice.len()).map(|i| if choice[i] { pcodec[i] } else { base_v2[i] }).sum();
        if forced.contains(&true) || v2_total < self.chunk_sizes(batch, &none, false)?.iter().sum() {
            Ok(choice)
        } else {
            Ok(forced)
        }
    }

    /// The writer properties of the file, given `sample` (the first batch with rows, if any).
    pub(crate) fn writer_properties(&self, sample: Option<&RecordBatch>) -> Result<WriterProperties> {
        Ok(self.properties(&self.choose(sample)?, false))
    }

    /// Compressed bytes per leaf of `batch` written with PCODEC on the `pcodec` leaves.
    fn chunk_sizes(&self, batch: &RecordBatch, pcodec: &[bool], v2: bool) -> Result<Vec<i64>> {
        let mut w = ArrowWriter::try_new(Vec::new(), self.schema.clone(), Some(self.properties(pcodec, v2)))?;
        w.write(batch)?;
        let metadata = w.close()?;
        let mut sizes = vec![0; self.leaves.len()];
        for rg in metadata.row_groups() {
            for (size, c) in sizes.iter_mut().zip(rg.columns()) {
                *size += c.compressed_size();
            }
        }
        Ok(sizes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use std::sync::Arc;

    /// long runs of a few unrelated values: dictionary + RLE beats PCODEC
    const RUNS: [i64; 8] = [
        -6_101_966_131_243_003_385,
        8_434_092_517_718_611_329,
        -1_272_455_226_406_532_186,
        3_960_587_219_133_017_017,
        7_036_282_119_405_826_455,
        -4_874_470_612_221_018_871,
        151_771_802_640_377_413,
        -8_852_001_101_010_101_011,
    ];

    fn batch(rows: i64) -> RecordBatch {
        RecordBatch::try_from_iter([
            ("smooth", Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i * 3))) as ArrayRef),
            (
                "runs",
                Arc::new(Int64Array::from_iter_values((0..rows).map(|i| RUNS[(i as usize / 512) % 8]))) as ArrayRef,
            ),
            ("price", Arc::new(Float64Array::from_iter_values((0..rows).map(|i| i as f64 / 100.0))) as ArrayRef),
            ("name", Arc::new(StringArray::from_iter_values((0..rows).map(|i| format!("n{}", i % 5)))) as ArrayRef),
        ])
        .unwrap()
    }

    fn codecs(options: TparquetOptions) -> Codecs {
        Codecs::try_new(batch(0).schema(), WriterProperties::default(), &options).unwrap()
    }

    fn compression(p: &WriterProperties) -> Vec<String> {
        ["smooth", "runs", "price", "name"].map(|c| p.compression(&ColumnPath::from(c)).to_string()).to_vec()
    }

    #[test]
    fn leaves_get_their_policies() {
        let c = codecs(TparquetOptions::default().with_column("price", Policy::Auto).with_column("runs", Policy::Base));
        let policies: Vec<Option<Policy>> = c.leaves.iter().map(|l| l.policy).collect();
        assert_eq!(policies, [Some(Policy::Pcodec), Some(Policy::Base), Some(Policy::Auto), None]);
        assert_eq!(c.schema().fields().len(), 4);
        let err = Codecs::try_new(
            batch(0).schema(),
            WriterProperties::default(),
            &TparquetOptions::default().with_column("x", Policy::Base),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid or Unsupported Configuration: tparquet: policy for \"x\", but there is no such column"
        );
    }

    #[test]
    fn properties_set_pcodec_plain_without_dictionary_and_v2_pages() {
        let c = codecs(TparquetOptions::default());
        let p = c.properties(&[true, false, true, false], false);
        assert_eq!(
            compression(&p),
            ["PCODEC(PcodecLevel(8))", "UNCOMPRESSED", "PCODEC(PcodecLevel(8))", "UNCOMPRESSED"]
        );
        let smooth = ColumnPath::from("smooth");
        assert_eq!(p.encoding(&smooth), Some(Encoding::PLAIN));
        assert!(!p.dictionary_enabled(&smooth));
        assert!(p.dictionary_enabled(&ColumnPath::from("runs")));
        assert_eq!(p.encoding(&ColumnPath::from("runs")), None);
        assert_eq!(p.writer_version(), WriterVersion::PARQUET_2_0);
        assert_eq!(c.properties(&[false; 4], false).writer_version(), WriterVersion::PARQUET_1_0);
        assert_eq!(c.properties(&[false; 4], true).writer_version(), WriterVersion::PARQUET_2_0);
        let c = codecs(TparquetOptions::default().with_level(0).unwrap());
        assert_eq!(compression(&c.properties(&[true; 4], false))[0], "PCODEC(PcodecLevel(0))");
    }

    #[test]
    fn choose_follows_the_policies() {
        let b = batch(20_000);
        let pcodec = codecs(TparquetOptions::default());
        assert_eq!(pcodec.choose(None).unwrap(), [true, true, true, false]);
        assert_eq!(pcodec.choose(Some(&b)).unwrap(), [true, true, true, false]);
        let base = codecs(TparquetOptions::default().with_policy(Policy::Base));
        assert_eq!(base.choose(Some(&b)).unwrap(), [false; 4]);
        // auto: PCODEC where smaller (smooth ints, prices), the base codec on long runs
        let auto = codecs(TparquetOptions::default().with_policy(Policy::Auto));
        assert_eq!(auto.choose(Some(&b)).unwrap(), [true, false, true, false]);
        assert_eq!(auto.choose(None).unwrap(), [false; 4]);
        let mixed = codecs(TparquetOptions::default().with_policy(Policy::Auto).with_column("runs", Policy::Pcodec));
        assert_eq!(mixed.choose(Some(&b)).unwrap(), [true, true, true, false]);
        assert_eq!(mixed.choose(None).unwrap(), [false, true, false, false]);
        let p = auto.writer_properties(Some(&b)).unwrap();
        assert_eq!(compression(&p)[..2], ["PCODEC(PcodecLevel(8))".to_string(), "UNCOMPRESSED".to_string()]);
    }

    #[test]
    fn chunk_sizes_are_per_leaf_over_all_row_groups() {
        let base = WriterProperties::builder().set_max_row_group_row_count(Some(1000)).build();
        let c = Codecs::try_new(batch(0).schema(), base, &TparquetOptions::default()).unwrap();
        let b = batch(5000);
        let sizes = c.chunk_sizes(&b, &[false; 4], false).unwrap();
        let one = c.chunk_sizes(&b.slice(0, 1000), &[false; 4], false).unwrap();
        assert!(sizes.iter().zip(&one).all(|(all, first)| *all > 4 * *first), "{sizes:?} {one:?}");
        let v2 = c.chunk_sizes(&b, &[false; 4], true).unwrap();
        let pcodec = c.chunk_sizes(&b, &[true, false, false, false], false).unwrap();
        assert!(pcodec[0] < v2[0]);
        assert_eq!(pcodec[1..], v2[1..]);
    }
}
