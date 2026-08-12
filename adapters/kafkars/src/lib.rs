//! Benchmark-only orchestration over the unmodified public `kafkars` surface.

mod arguments;
mod histogram_vector;
#[cfg(test)]
mod histogram_vector_test;
mod payload;
#[cfg(test)]
mod payload_test;
mod producer;
mod protocol;
#[cfg(test)]
mod protocol_test;
mod report;
#[cfg(test)]
mod report_test;
mod schedule;
#[cfg(test)]
mod schedule_test;
mod topics;

pub use arguments::run;
