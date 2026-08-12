//! Reading counters out of one librdkafka statistics snapshot.
//!
//! Snapshots are parsed as generic JSON rather than against a pinned schema, so
//! unknown keys are ignored and a counter that is missing yields `None` rather
//! than a zero. Every accessor here is total: it answers "what did the client
//! say about this", and "nothing" is one of the answers.
//!
//! Cumulative counters are only ever read through a delta, and a delta that
//! runs backwards is `None`. librdkafka counters are monotonic within a client
//! instance, so a decrease means the two snapshots came from different
//! instances and their difference means nothing.

use serde_json::Value;

/// Returns the librdkafka statistics object inside a snapshot, whether it is
/// wrapped in the sealed envelope or not.
pub(super) fn statistics_of(snapshot: Option<&Value>) -> Option<&Value> {
    let snapshot = snapshot?;
    match snapshot.get("statistics") {
        Some(inner) if inner.is_object() => Some(inner),
        _ => snapshot.is_object().then_some(snapshot),
    }
}

/// Returns a non-negative integer field of an object.
pub(super) fn counter(value: Option<&Value>, key: &str) -> Option<u64> {
    value?.get(key)?.as_u64()
}

/// Returns the increase of a cumulative counter across the measured window.
///
/// A counter that moved backwards is reported as `None`: librdkafka counters
/// are monotonic, so a decrease means the two snapshots came from different
/// client instances and the difference means nothing.
pub(super) fn counter_delta(
    baseline: Option<&Value>,
    terminal: Option<&Value>,
    key: &str,
) -> Option<u64> {
    let start = counter(baseline, key)?;
    let finish = counter(terminal, key)?;
    finish.checked_sub(start)
}

/// Returns the brokers a snapshot describes, excluding the bootstrap
/// pseudo-broker, which carries a negative node id and no request accounting.
fn brokers(snapshot: Option<&Value>) -> Vec<&Value> {
    let Some(map) = snapshot
        .and_then(|value| value.get("brokers"))
        .and_then(Value::as_object)
    else {
        return Vec::new();
    };
    map.values()
        .filter(|broker| {
            broker
                .get("nodeid")
                .and_then(Value::as_i64)
                .is_some_and(|node| node >= 0)
        })
        .collect()
}

/// Returns the increase of a per-broker counter, summed across brokers.
pub(super) fn broker_counter_delta(
    baseline: Option<&Value>,
    terminal: Option<&Value>,
    key: &str,
) -> Option<u64> {
    let finish = broker_total(terminal, key)?;
    let start = broker_total(baseline, key).unwrap_or(0);
    finish.checked_sub(start)
}

/// Sums one counter across every broker in a snapshot.
fn broker_total(snapshot: Option<&Value>, key: &str) -> Option<u64> {
    let brokers = brokers(snapshot);
    if brokers.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut seen = false;
    for broker in brokers {
        if let Some(value) = counter(Some(broker), key) {
            total = total.saturating_add(value);
            seen = true;
        }
    }
    seen.then_some(total)
}

/// Returns the increase of one request-type counter, summed across brokers.
pub(super) fn request_type_delta(
    baseline: Option<&Value>,
    terminal: Option<&Value>,
    request: &str,
) -> Option<u64> {
    let finish = request_type_total(terminal, request)?;
    let start = request_type_total(baseline, request).unwrap_or(0);
    finish.checked_sub(start)
}

/// Sums one request-type counter across every broker in a snapshot.
fn request_type_total(snapshot: Option<&Value>, request: &str) -> Option<u64> {
    let brokers = brokers(snapshot);
    if brokers.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut seen = false;
    for broker in brokers {
        if let Some(value) = broker
            .get("req")
            .and_then(|requests| requests.get(request))
            .and_then(Value::as_u64)
        {
            total = total.saturating_add(value);
            seen = true;
        }
    }
    seen.then_some(total)
}
