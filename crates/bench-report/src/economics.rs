//! Request economics: how much broker traffic a subject spent to deliver the
//! records it delivered.
//!
//! # What this is for
//!
//! Two clients can post the same goodput and the same p99 while spending very
//! different numbers of produce requests, wire bytes, and batches to do it. The
//! difference is invisible in a latency histogram and is exactly what an
//! operator pays for, so it is worth reporting on its own axis. These numbers
//! are *descriptive*: they say what the client did, not whether it was right to
//! do it, and a batching strategy that looks wasteful at one payload size may
//! be the correct one at another.
//!
//! # Where the numbers come from
//!
//! The librdkafka statistics stream (`client-metrics.jsonl`), one JSON snapshot
//! per line. Field meanings follow the legacy control plane's
//! `librdkafka-statistics.mjs`, which is the behavioral reference:
//!
//! - `tx` / `tx_bytes` — requests and request bytes put on the wire, cumulative;
//! - `rx_bytes` — response bytes taken off it, cumulative;
//! - `txmsgs` / `txmsg_bytes` — records and payload bytes transmitted, cumulative;
//! - `brokers.*.req.Produce` — produce requests per broker, cumulative;
//! - `brokers.*.txretries` / `req_timeouts` — retries and timeouts, cumulative;
//! - `topics.<topic>.batchcnt` / `batchsize` — *reset-on-emit* rolling windows,
//!   so they are aggregated across snapshots rather than differenced.
//!
//! Cumulative counters are differenced between the `baseline` snapshot and the
//! `final` one, which is what isolates the measured phase from warmup.
//!
//! # Lenient on purpose, absent rather than invented
//!
//! Unlike the legacy module, which fails closed on any deviation from the
//! pinned schema, this one reads what is there: snapshots are parsed as generic
//! JSON, unknown keys are ignored, and a counter that is missing yields `None`
//! rather than a zero. Kafkars' versioned sidecar supplies exact Produce request,
//! partition-batch, record, and encoded-record-byte deltas. It does not expose
//! total wire bytes, retries, or request timeouts, so those fields remain absent.
//! An absent measurement and a measurement of zero are different claims.
//!
//! # Layout
//!
//! - `snapshot` — reading counters out of one snapshot.
//! - `window` — the reset-on-emit batching windows.
//! - `stream` — the whole stream, and which part of it was measured.
//! - `totals` — one subject's costs, and how attempts of it add up.
//! - `normalize` — the two quotients that make runs comparable.

mod kafkars;
mod normalize;
mod snapshot;
mod stream;
mod totals;
mod window;

pub use stream::{STATISTICS_FILE_NAME, read_request_economics, request_economics_from_snapshots};
pub use totals::RequestEconomics;
pub use window::BatchWindow;
