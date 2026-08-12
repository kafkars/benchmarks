//! How one invocation of the fixture should misbehave, if at all.
//!
//! The behavior is chosen by a leading `--mode <mode>` argument, falling back to
//! the `FAKE_ADAPTER_MODE` environment variable and then to `ok`. The argument
//! form exists because integration tests run in parallel threads of one process,
//! where a per-test environment variable would be a race.
//!
//! `rate-slo:<threshold>` is the capacity-search fixture: it behaves exactly like
//! `ok` except that its latencies jump above every objective the experiment
//! declares as soon as `offered_records_per_second` exceeds the threshold. The
//! jump is a step function of the resolved experiment alone, so a capacity search
//! over this fixture converges on the threshold with no clock, no load, and no
//! run-to-run variation to bracket around.

/// How this invocation should misbehave, if at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Everything succeeds.
    Ok,
    /// `run` writes a failed status and exits non-zero.
    RunNonzero,
    /// `run` never returns.
    RunHang,
    /// `run` dies on `SIGABRT`.
    RunAbort,
    /// `run` writes its process id to `<output>/pid`, then never returns.
    RunWritePidThenHang,
    /// `verify` reports an invalid topic and exits zero.
    VerifierInvalid,
    /// `verify` exits non-zero without a report.
    VerifierFail,
    /// `describe` prints something that is not JSON.
    DescribeGarbage,
    /// `topics-create` exits non-zero.
    TopicsFail,
    /// `run` succeeds, but its latencies exceed every declared objective as soon
    /// as the offered rate is above this threshold.
    RateSlo(u64),
}

impl Mode {
    /// The prefix of the parameterised capacity mode.
    const RATE_SLO_PREFIX: &'static str = "rate-slo:";

    /// Parses a mode name, defaulting to [`Mode::Ok`] for anything unknown.
    ///
    /// An unparseable threshold in `rate-slo:<n>` is a usage mistake in a test,
    /// and falling back to [`Mode::Ok`] would turn it into a capacity search that
    /// never saturates. A threshold of zero saturates at every rate instead,
    /// which fails loudly and immediately.
    pub(crate) fn parse(text: &str) -> Self {
        if let Some(threshold) = text.strip_prefix(Self::RATE_SLO_PREFIX) {
            return Self::RateSlo(threshold.parse().unwrap_or(0));
        }
        match text {
            "run-nonzero" => Self::RunNonzero,
            "run-hang" => Self::RunHang,
            "run-abort" => Self::RunAbort,
            "run-write-pid-then-hang" => Self::RunWritePidThenHang,
            "verifier-invalid" => Self::VerifierInvalid,
            "verifier-fail" => Self::VerifierFail,
            "describe-garbage" => Self::DescribeGarbage,
            "topics-fail" => Self::TopicsFail,
            _ => Self::Ok,
        }
    }
}
