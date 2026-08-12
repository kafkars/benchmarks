//! The one check a model-written summary has to pass.

use bench_schema::{AnalysisPacket, LlmSummary, SchemaResult};

/// Checks a model-written summary against the packet it claims to be about.
///
/// A thin delegation to
/// [`LlmSummary::validate_against`](bench_schema::LlmSummary::validate_against),
/// so that the control plane has one import point for the whole reporting
/// surface and the rule stays defined in exactly one place.
///
/// # Errors
///
/// Returns an error when the summary cites a metric or an evidence reference
/// the packet does not define, or when it states a verdict other than the
/// packet's.
pub fn validate_llm_summary(summary: &LlmSummary, packet: &AnalysisPacket) -> SchemaResult<()> {
    summary.validate_against(packet)
}
