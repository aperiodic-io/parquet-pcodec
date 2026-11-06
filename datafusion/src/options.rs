//! The writer options of the TPARQUET format: which numeric columns get PCODEC, and its level.

use datafusion::error::{DataFusionError, Result};
use parquet::basic::PcodecLevel;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

/// `OPTIONS` key of the policy of every eligible column: `'policy' 'auto'` in SQL.
pub const POLICY_KEY: &str = "format.policy";
/// `OPTIONS` key of the pcodec compression level, 0 to 12: `'pcodec_level' 8` in SQL.
pub const LEVEL_KEY: &str = "format.pcodec_level";

/// The codec choice for an eligible column (an `INT32`, `INT64`, `FLOAT` or `DOUBLE` leaf).
///
/// Other columns always use the base codec, DataFusion's `compression` option (`zstd(3)` by
/// default).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Policy {
    /// Write each column with PCODEC and with the base codec on the first batch and keep the
    /// smaller (spec section 6).
    Auto,
    /// Always PCODEC.
    #[default]
    Pcodec,
    /// Always the base codec.
    Base,
}

impl FromStr for Policy {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "pcodec" => Ok(Self::Pcodec),
            "base" => Ok(Self::Base),
            _ => Err(DataFusionError::Configuration(format!(
                "tparquet: unknown policy {s:?}: expected auto, pcodec or base"
            ))),
        }
    }
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Auto => "auto",
            Self::Pcodec => "pcodec",
            Self::Base => "base",
        })
    }
}

/// How the TPARQUET format writes: the policy of the eligible columns and the pcodec level.
///
/// Everything else (base codec, row group size, statistics, page index, bloom filters,
/// key-value metadata...) comes from DataFusion's own Parquet options. PCODEC columns always
/// use `PLAIN`, no dictionary, and the file is written with `DATA_PAGE_V2` pages (spec
/// section 3, writer recommendations).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TparquetOptions {
    /// Policy of the eligible columns that `columns` does not name.
    pub policy: Policy,
    /// Per-column policies: a top-level field name, or a dotted leaf path such as
    /// `book.levels.list.element`. The longest name that covers a leaf wins.
    pub columns: Vec<(String, Policy)>,
    /// pcodec compression level of the PCODEC columns.
    pub level: PcodecLevel,
}

impl TparquetOptions {
    /// Sets the policy of every column without its own.
    pub fn with_policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }

    /// Sets the policy of the column `name` (a top-level field or a dotted leaf path).
    pub fn with_column(mut self, name: impl Into<String>, policy: Policy) -> Self {
        self.columns.push((name.into(), policy));
        self
    }

    /// Sets the pcodec compression level (0..=12).
    pub fn with_level(mut self, level: u32) -> Result<Self> {
        self.level = PcodecLevel::try_new(level)?;
        Ok(self)
    }

    /// Applies the TPARQUET keys of `format_options` (`format.policy`,
    /// `format.policy::<column>`, `format.pcodec_level`, each with or without the `format.`
    /// prefix) and returns the other options, for DataFusion's Parquet format.
    pub fn apply(&mut self, format_options: &HashMap<String, String>) -> Result<HashMap<String, String>> {
        let mut rest = HashMap::new();
        // in a fixed order, so that the first bad option reported is always the same
        let mut keys: Vec<&String> = format_options.keys().collect();
        keys.sort();
        for key in keys {
            let value = &format_options[key];
            // SQL prefixes `format.` only to keys without a dot: `'policy::book.levels'`
            // arrives as is
            let name = key.strip_prefix("format.").unwrap_or(key);
            if name == "policy" {
                self.policy = value.parse()?;
            } else if let Some(column) = name.strip_prefix("policy::") {
                self.columns.push((column.to_string(), value.parse()?));
            } else if name == "pcodec_level" {
                let level = value.trim().parse::<u32>().map_err(|_| {
                    DataFusionError::Configuration(format!("tparquet: pcodec_level {value:?} is not a level 0 to 12"))
                })?;
                *self = std::mem::take(self).with_level(level)?;
            } else {
                rest.insert(key.clone(), value.clone());
            }
        }
        Ok(rest)
    }
}

/// The policy of the leaf at `path` (dotted, as `ColumnPath::string` gives it): the longest
/// override that names the leaf or one of its ancestors, else `default`.
pub(crate) fn resolve(path: &str, overrides: &[(String, Policy)], default: Policy) -> Policy {
    overrides
        .iter()
        .filter(|(name, _)| covers(name, path))
        .max_by_key(|(name, _)| name.len())
        .map_or(default, |&(_, p)| p)
}

/// Whether the override `name` applies to the leaf `path`.
pub(crate) fn covers(name: &str, path: &str) -> bool {
    path.strip_prefix(name).is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn policies_parse_and_display() {
        for p in [Policy::Auto, Policy::Pcodec, Policy::Base] {
            assert_eq!(p.to_string().parse::<Policy>().unwrap(), p);
            assert_eq!(p.to_string().to_uppercase().parse::<Policy>().unwrap(), p);
        }
        assert_eq!(Policy::default(), Policy::Pcodec);
        let e = "fast".parse::<Policy>().unwrap_err().to_string();
        assert_eq!(
            e,
            "Invalid or Unsupported Configuration: tparquet: unknown policy \"fast\": expected auto, pcodec or base"
        );
    }

    #[test]
    fn defaults_and_builders() {
        let o = TparquetOptions::default();
        assert_eq!((o.policy, o.columns.len(), o.level.compression_level()), (Policy::Pcodec, 0, 8));
        let o = o.with_policy(Policy::Auto).with_column("a", Policy::Base).with_level(3).unwrap();
        assert_eq!(o.policy, Policy::Auto);
        assert_eq!(o.columns, [("a".to_string(), Policy::Base)]);
        assert_eq!(o.level.compression_level(), 3);
        assert_eq!(
            o.with_level(13).unwrap_err().to_string(),
            "Parquet error: Parquet error: valid compression range 0..=12 exceeded."
        );
    }

    #[test]
    fn apply_takes_the_tparquet_keys_and_returns_the_rest() {
        let mut o = TparquetOptions::default();
        let rest = o
            .apply(&map(&[
                ("format.policy", "auto"),
                ("format.policy::price", "base"),
                ("format.policy::book.levels", "PCODEC"),
                ("format.pcodec_level", " 12 "),
                ("format.compression", "snappy"),
                ("format.policies", "x"),
                ("policy::book.levels.list.item", "auto"),
                ("pcodec", "x"),
            ]))
            .unwrap();
        assert_eq!(o.policy, Policy::Auto);
        let mut columns = o.columns.clone();
        columns.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            columns,
            [
                ("book.levels".to_string(), Policy::Pcodec),
                ("book.levels.list.item".to_string(), Policy::Auto),
                ("price".to_string(), Policy::Base)
            ]
        );
        assert_eq!(o.level.compression_level(), 12);
        assert_eq!(rest, map(&[("format.compression", "snappy"), ("format.policies", "x"), ("pcodec", "x")]));
        // without the prefix too (as a programmatic caller may pass them)
        let mut o = TparquetOptions::default();
        assert!(o.apply(&map(&[("policy", "base"), ("pcodec_level", "1")])).unwrap().is_empty());
        assert_eq!((o.policy, o.level.compression_level()), (Policy::Base, 1));
        // options apply on top of the factory's
        let mut o = TparquetOptions::default().with_column("a", Policy::Auto).with_level(2).unwrap();
        assert!(o.apply(&HashMap::new()).unwrap().is_empty());
        assert_eq!((o.columns.len(), o.level.compression_level()), (1, 2));
    }

    #[test]
    fn apply_refuses_bad_values() {
        for (key, value, error) in [
            ("format.policy", "fast", "unknown policy \"fast\""),
            ("format.policy::a", "", "unknown policy \"\""),
            ("format.pcodec_level", "13", "valid compression range 0..=12 exceeded"),
            ("format.pcodec_level", "-1", "pcodec_level \"-1\" is not a level 0 to 12"),
            ("format.pcodec_level", "eight", "pcodec_level \"eight\" is not a level 0 to 12"),
        ] {
            let err = TparquetOptions::default().apply(&map(&[(key, value)])).unwrap_err().to_string();
            assert!(err.contains(error), "{key}={value}: {err}");
        }
    }

    #[test]
    fn the_longest_covering_override_wins() {
        let o = [("book".to_string(), Policy::Base), ("book.levels".to_string(), Policy::Auto)];
        assert_eq!(resolve("book.levels.list.item", &o, Policy::Pcodec), Policy::Auto);
        assert_eq!(resolve("book.id", &o, Policy::Pcodec), Policy::Base);
        assert_eq!(resolve("bookkeeping", &o, Policy::Pcodec), Policy::Pcodec);
        assert_eq!(resolve("other", &[], Policy::Auto), Policy::Auto);
        assert!(covers("a", "a") && covers("a", "a.b") && !covers("a", "ab") && !covers("a.b", "a"));
    }
}
