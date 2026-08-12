//! The inline SVG interval bar the HTML report draws beside every pair.
//!
//! Inline for the same reason the stylesheet is: the page has to open from a
//! sealed bundle on a machine with no network. The bar is decoration over
//! numbers that are printed beside it, and it never rounds one of them away —
//! an interval that runs off the end of the fixed domain is clamped, and the
//! printed figures remain the authority.

use bench_schema::PairedRatio;

use crate::suite::{SuiteMetric, pair_passes, pair_regresses};

use super::format::format_ratio;

/// Width of a ratio bar, in user units.
const BAR_WIDTH: f64 = 200.0;

/// Height of a ratio bar, in user units.
const BAR_HEIGHT: f64 = 24.0;

/// Lowest ratio a bar can show; anything smaller is clamped to the left edge.
const BAR_LOW: f64 = 0.5;

/// Highest ratio a bar can show; anything larger is clamped to the right edge.
const BAR_HIGH: f64 = 1.5;

/// An inline SVG bar showing one interval against parity and the threshold.
///
/// The domain is fixed at [`BAR_LOW`, `BAR_HIGH`] so that bars in different
/// rows are directly comparable; an interval that runs off the end is clamped
/// and the printed numbers beside it remain the authority.
pub(super) fn ratio_bar(pair: &PairedRatio, metric: SuiteMetric, threshold: f64) -> String {
    let scale = |value: f64| {
        let clamped = value.clamp(BAR_LOW, BAR_HIGH);
        (clamped - BAR_LOW) / (BAR_HIGH - BAR_LOW) * BAR_WIDTH
    };
    let low = scale(pair.ci_low);
    let high = scale(pair.ci_high);
    let width = (high - low).max(1.0);
    let class = if pair_passes(pair, metric, threshold) {
        "good"
    } else if pair_regresses(pair, metric, threshold) {
        "bad"
    } else {
        "flat"
    };
    format!(
        "<svg class=\"bar\" viewBox=\"0 0 {BAR_WIDTH:.0} {BAR_HEIGHT:.0}\" \
         width=\"{BAR_WIDTH:.0}\" height=\"{BAR_HEIGHT:.0}\" role=\"img\" \
         aria-label=\"interval {} to {}\">\
         <line class=\"axis\" x1=\"0\" y1=\"{axis:.1}\" x2=\"{BAR_WIDTH:.0}\" y2=\"{axis:.1}\"/>\
         <line class=\"tick\" x1=\"{lower_tick:.1}\" y1=\"2\" x2=\"{lower_tick:.1}\" \
         y2=\"{BAR_HEIGHT:.0}\"/>\
         <line class=\"tick\" x1=\"{upper_tick:.1}\" y1=\"2\" x2=\"{upper_tick:.1}\" \
         y2=\"{BAR_HEIGHT:.0}\"/>\
         <line class=\"parity\" x1=\"{parity:.1}\" y1=\"0\" x2=\"{parity:.1}\" \
         y2=\"{BAR_HEIGHT:.0}\"/>\
         <rect class=\"interval {class}\" x=\"{low:.1}\" y=\"{top:.1}\" width=\"{width:.1}\" \
         height=\"8\" rx=\"2\"/>\
         <circle class=\"point {class}\" cx=\"{point:.1}\" cy=\"{axis:.1}\" r=\"3\"/>\
         </svg>",
        format_ratio(pair.ci_low),
        format_ratio(pair.ci_high),
        axis = BAR_HEIGHT / 2.0,
        lower_tick = scale(1.0 - threshold),
        upper_tick = scale(1.0 + threshold),
        parity = scale(1.0),
        top = BAR_HEIGHT / 2.0 - 4.0,
        point = scale(pair.ratio_of_medians),
    )
}
