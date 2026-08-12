//! The usage text, and the one default a flagless invocation falls back to.
//!
//! The text is a constant rather than something rendered from the parser,
//! because it is quoted verbatim on every usage error and a reader comparing a
//! failed command line against it must be looking at the same words the parser
//! was written from.

/// The full usage text, printed for `--help` and quoted on a usage error.
pub const USAGE: &str = "\
usage:
  benchctl resolve  --experiment <toml> --subjects <toml> --cluster <toml>
                    --bootstrap <host:port,...> [--seed <n>] [--order <a,b>]
                    [--out <path>]
  benchctl run      <resolve flags> [--results <dir>] [--run-timeout-secs <n>]
                    [--tool-timeout-secs <n>] [--probe-timeout-secs <n>]
  benchctl suite    <run flags> --repetitions <n> [--reports <dir>]
  benchctl capacity <run flags> [--reports <dir>]
  benchctl pack     --manifest <pack.toml> --subjects <toml> --cluster <toml>
                    --bootstrap <host:port,...> [--results <dir>]
                    [--reports <dir>] [--seed <n>] [--run-timeout-secs <n>]
                    [--tool-timeout-secs <n>] [--probe-timeout-secs <n>]
  benchctl report   --bundle <dir> [--out <path>]
  benchctl packet   --suite <suite-summary.json> --llm-summary <file>

  resolve   probe the subjects and print the resolved experiment; seals nothing
  run       resolve, execute every subject, and always seal an evidence bundle
  suite     run the same experiment N times and summarize the repetitions
  capacity  search for the highest offered rate that still meets the objectives
  pack      run every entry of one cadence's manifest, in the order it states
  report    render one sealed bundle as markdown
  packet    check an LLM summary against the packet derived from a suite";

/// Default directory the evidence tree is created under.
pub const DEFAULT_RESULTS_ROOT: &str = "results";
