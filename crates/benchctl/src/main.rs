//! Entry point for the `benchctl` control plane.
//!
//! **This binary is a stub.** The command surface is fixed by the design —
//! `benchctl resolve` and `benchctl run` — but no verb is implemented yet, so
//! every invocation is reported as a usage error. Exit code `64` is the usage
//! slot in this repository's exit-code table and is deliberately distinct from
//! the sealed-outcome codes, so a caller can already tell "you asked wrongly"
//! apart from "the attempt failed" before any attempt exists.
#![forbid(unsafe_code)]

fn main() {
    eprintln!("usage: benchctl <resolve|run> --experiment <path> [options] (not implemented yet)");
    std::process::exit(64);
}
