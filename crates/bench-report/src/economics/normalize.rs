//! The two normalizations that make one run's request costs comparable to
//! another's.
//!
//! Both return `None` rather than a placeholder when the inputs cannot support
//! the quotient, because a rate over an absent counter is not a rate of zero.

/// Returns `numerator / denominator` when both are present and the denominator
/// is not zero.
#[expect(
    clippy::cast_precision_loss,
    reason = "reporting statistic over counter values, not identity arithmetic"
)]
pub(super) fn ratio(numerator: Option<u64>, denominator: Option<u64>) -> Option<f64> {
    let denominator = denominator.filter(|value| *value > 0)?;
    Some(numerator? as f64 / denominator as f64)
}

/// Returns `count` scaled to a per-million-records rate.
#[expect(
    clippy::cast_precision_loss,
    reason = "reporting statistic over counter values, not identity arithmetic"
)]
pub(super) fn per_million(count: Option<u64>, acknowledged_records: u64) -> Option<f64> {
    if acknowledged_records == 0 {
        return None;
    }
    Some(count? as f64 * 1_000_000.0 / acknowledged_records as f64)
}
