//! Entry point for the `benchctl` control plane.
//!
//! The binary is deliberately a shell: it hands the argument vector to
//! [`benchctl::cli::run`] and exits with the code that comes back. Everything
//! worth testing — parsing, resolution, supervision, sealing — lives in the
//! library, so an integration test drives exactly the same code path this
//! process does rather than an approximation of it.
//!
//! Nothing here decides an exit code. The table lives in `error.rs`, because a
//! caller reading `20` must be able to find one place that says what 20 means.
#![forbid(unsafe_code)]

fn main() {
    std::process::exit(benchctl::cli::run(&std::env::args().collect::<Vec<_>>()));
}
