//! Dataset addressing: the target every request names and the optional day range it spans.

use chrono::NaiveDate;

use crate::domain::error::SearchError;

/// Upper bound on the number of days one date range may span.
pub const MAX_DATE_RANGE_DAYS: usize = 366;

/// An inclusive calendar-day range over date-partitioned datasets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateRange {
    /// First day (inclusive).
    pub start: NaiveDate,
    /// Last day (inclusive).
    pub end: NaiveDate,
}

impl DateRange {
    /// Builds a validated range: `start <= end` and the span fits in [`MAX_DATE_RANGE_DAYS`].
    pub fn new(start: NaiveDate, end: NaiveDate) -> Result<Self, SearchError> {
        if start > end {
            return Err(SearchError::invalid_argument(
                "date_range start_date must not be after end_date",
            ));
        }
        let span = (end - start).num_days() as usize + 1;
        if span > MAX_DATE_RANGE_DAYS {
            return Err(SearchError::invalid_argument(format!(
                "date_range spans {span} days, the maximum is {MAX_DATE_RANGE_DAYS}"
            )));
        }
        Ok(Self { start, end })
    }

    /// Every day in the range, in ascending order.
    pub fn days(&self) -> Vec<NaiveDate> {
        let mut days = Vec::new();
        let mut current = self.start;
        while current <= self.end {
            days.push(current);
            match current.succ_opt() {
                Some(next) => current = next,
                None => break,
            }
        }
        days
    }
}

/// Addresses the dataset(s) a request operates on.
///
/// Without a date range the target names the single dataset at
/// `{base}/{org_id}/{tenant_id}/{namespace}.lance`. With one it names one date-partitioned
/// dataset per day at `{base}/{org_id}/{tenant_id}/{namespace}/{date}.lance`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetTarget {
    /// Organization id. Must match `[A-Za-z0-9_-]+`.
    pub org_id: String,
    /// Tenant id. Must match `[A-Za-z0-9_-]+`.
    pub tenant_id: String,
    /// Namespace. Must match `[A-Za-z0-9_-]+`.
    pub namespace: String,
    /// Optional day range selecting date-partitioned datasets.
    pub date_range: Option<DateRange>,
}

impl DatasetTarget {
    /// Builds a target without a date range, for tests and embedded callers.
    pub fn new(org_id: impl Into<String>, tenant_id: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            org_id: org_id.into(),
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
            date_range: None,
        }
    }

    /// Validates every path segment of the target.
    pub fn validate(&self) -> Result<(), SearchError> {
        validate_path_segment(&self.org_id, "org_id")?;
        validate_path_segment(&self.tenant_id, "tenant_id")?;
        validate_path_segment(&self.namespace, "namespace")
    }

    /// Resolves the single date of a target whose range must cover at most one day.
    ///
    /// Returns `None` for rangeless targets, the day for a single-day range, and an
    /// `InvalidArgument` error when the range spans several days. Used by RPCs that operate on
    /// exactly one dataset (Prewarm, Clusters).
    pub fn single_date(&self) -> Result<Option<NaiveDate>, SearchError> {
        match &self.date_range {
            None => Ok(None),
            Some(range) if range.start == range.end => Ok(Some(range.start)),
            Some(_) => Err(SearchError::invalid_argument(
                "this RPC operates on one dataset, the date_range must cover exactly one day",
            )),
        }
    }
}

/// Rejects path segments that are empty or contain characters outside `[A-Za-z0-9_-]`.
pub fn validate_path_segment(value: &str, field: &str) -> Result<(), SearchError> {
    let valid = !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if valid {
        Ok(())
    } else {
        Err(SearchError::invalid_argument(format!(
            "{field} must be non-empty and match [A-Za-z0-9_-]+"
        )))
    }
}

/// Parses a `YYYY-MM-DD` date string.
pub fn parse_date(raw: &str, field: &str) -> Result<NaiveDate, SearchError> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .map_err(|_| SearchError::invalid_argument(format!("{field} must be a YYYY-MM-DD date, got {raw:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a date or panics, for fixtures.
    fn day(raw: &str) -> NaiveDate {
        parse_date(raw, "test").unwrap()
    }

    #[test]
    fn range_days_are_inclusive_and_ordered() {
        let range = DateRange::new(day("2026-06-01"), day("2026-06-03")).unwrap();
        assert_eq!(
            range.days(),
            vec![day("2026-06-01"), day("2026-06-02"), day("2026-06-03")]
        );
        let single = DateRange::new(day("2026-06-01"), day("2026-06-01")).unwrap();
        assert_eq!(single.days(), vec![day("2026-06-01")]);
    }

    #[test]
    fn inverted_and_oversized_ranges_are_rejected() {
        let err = DateRange::new(day("2026-06-02"), day("2026-06-01")).unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
        let err = DateRange::new(day("2020-01-01"), day("2026-01-01")).unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
    }

    #[test]
    fn segment_validation_rejects_traversal_and_empties() {
        for bad in ["", "../escape", "a/b", "a b", "a.b"] {
            assert!(validate_path_segment(bad, "org_id").is_err(), "accepted {bad:?}");
        }
        assert!(validate_path_segment("org-1_A", "org_id").is_ok());
        let mut target = DatasetTarget::new("org1", "tenant1", "ns1");
        assert!(target.validate().is_ok());
        target.namespace = "../x".to_string();
        assert!(target.validate().is_err());
    }

    #[test]
    fn single_date_rules() {
        let mut target = DatasetTarget::new("o", "t", "n");
        assert_eq!(target.single_date().unwrap(), None);
        target.date_range = Some(DateRange::new(day("2026-06-01"), day("2026-06-01")).unwrap());
        assert_eq!(target.single_date().unwrap(), Some(day("2026-06-01")));
        target.date_range = Some(DateRange::new(day("2026-06-01"), day("2026-06-02")).unwrap());
        assert!(target.single_date().is_err());
    }

    #[test]
    fn date_parsing_accepts_iso_and_rejects_garbage() {
        assert_eq!(parse_date("2026-06-06", "d").unwrap(), day("2026-06-06"));
        for bad in ["06/06/2026", "2026-13-01", "today", ""] {
            assert!(parse_date(bad, "d").is_err(), "accepted {bad:?}");
        }
    }
}
