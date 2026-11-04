//! Which codec a column gets.

use crate::error::{Error, Result, invalid};
use std::fmt;
use std::str::FromStr;

/// The codec choice for an eligible column (an `INT32`, `INT64`, `FLOAT` or `DOUBLE` leaf).
///
/// Other columns always use the base codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Policy {
    /// Try both on the first batch and keep the smaller (spec section 6).
    #[default]
    Auto,
    /// Always PCODEC.
    Pcodec,
    /// Always the base codec.
    Base,
}

impl FromStr for Policy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "auto" => Ok(Self::Auto),
            "pcodec" => Ok(Self::Pcodec),
            "base" => Ok(Self::Base),
            _ => Err(invalid(format!("unknown policy {s:?}: expected auto, pcodec or base"))),
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

/// Parses `NAME=policy`, as the CLI's `--column` takes it.
pub fn parse_column_policy(s: &str) -> Result<(String, Policy)> {
    match s.rsplit_once('=') {
        Some((name, policy)) if !name.is_empty() => Ok((name.to_string(), policy.parse()?)),
        _ => Err(invalid(format!("expected NAME=policy, got {s:?}"))),
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

    #[test]
    fn parses_and_displays() {
        for p in [Policy::Auto, Policy::Pcodec, Policy::Base] {
            assert_eq!(p.to_string().parse::<Policy>().unwrap(), p);
        }
        assert_eq!(Policy::default(), Policy::Auto);
        let e = "fast".parse::<Policy>().unwrap_err().to_string();
        assert_eq!(e, "unknown policy \"fast\": expected auto, pcodec or base");
    }

    #[test]
    fn column_policies() {
        assert_eq!(parse_column_policy("price=pcodec").unwrap(), ("price".to_string(), Policy::Pcodec));
        assert_eq!(parse_column_policy("a=b=base").unwrap(), ("a=b".to_string(), Policy::Base));
        assert_eq!(parse_column_policy("price").unwrap_err().to_string(), "expected NAME=policy, got \"price\"");
        assert!(parse_column_policy("=auto").is_err());
        assert!(parse_column_policy("x=nope").is_err());
    }

    #[test]
    fn longest_ancestor_wins() {
        let o = vec![
            ("s".to_string(), Policy::Pcodec),
            ("s.inner.x".to_string(), Policy::Base),
            ("t".to_string(), Policy::Base),
        ];
        assert_eq!(resolve("s.inner.x", &o, Policy::Auto), Policy::Base);
        assert_eq!(resolve("s.inner.y", &o, Policy::Auto), Policy::Pcodec);
        assert_eq!(resolve("s", &o, Policy::Auto), Policy::Pcodec);
        assert_eq!(resolve("st", &o, Policy::Auto), Policy::Auto);
        assert_eq!(resolve("u", &o, Policy::Base), Policy::Base);
        assert_eq!(resolve("t.list.element", &o, Policy::Pcodec), Policy::Base);
    }

    #[test]
    fn covers_whole_names_only() {
        assert!(covers("a", "a"));
        assert!(covers("a", "a.b"));
        assert!(!covers("a", "ab"));
        assert!(!covers("a.b", "a"));
    }
}
