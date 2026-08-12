//! Sealing a v2 run must report the measurement, not the shutdown after it.

use std::error::Error;

use super::v2::seal;

fn failure(message: &str) -> Box<dyn Error> {
    message.into()
}

fn message<T>(sealed: Result<T, Box<dyn Error>>) -> String {
    sealed.err().map_or_else(
        || "the seal reported success where a failure was expected".to_owned(),
        |error| error.to_string(),
    )
}

#[test]
fn a_clean_run_seals_its_document() {
    let sealed = seal(Ok("document"), Ok(()));

    assert_eq!(
        sealed.ok(),
        Some("document"),
        "a run that measured and closed cleanly is a result"
    );
}

#[test]
fn a_failed_close_after_a_clean_run_is_the_failure() {
    let sealed = seal(Ok("document"), Err(failure("producer is already closed")));

    assert_eq!(
        message(sealed),
        "producer is already closed",
        "a measurement whose client would not shut down did not finish"
    );
}

#[test]
fn a_measurement_failure_survives_a_clean_close() {
    let sealed = seal::<&str>(Err(failure("warmup acknowledged 0 of 100")), Ok(()));

    assert_eq!(message(sealed), "warmup acknowledged 0 of 100");
}

#[test]
fn a_measurement_failure_is_not_replaced_by_the_close_it_caused() {
    // The regression, and the shape of the run that found it: a phase gives up,
    // leaving a client whose own close call is then rejected, so both halves
    // fail together — and the close failure is the *consequence* of the first.
    // Reporting it alone replaced a named measurement failure with "producer is
    // already closed", a sentence about the adapter's shutdown that says
    // nothing whatever about the run, and no `result.json` to read instead.
    let sealed = seal::<&str>(
        Err(failure(
            "the client accepted no record of an offer group of 1, for a reason no retry \
             can clear: kind=State",
        )),
        Err(failure("producer is already closed")),
    );

    let error = message(sealed);
    assert!(
        error.starts_with("the client accepted no record of an offer group of 1"),
        "the measurement's diagnosis must lead, not the shutdown symptom: {error}"
    );
    assert!(
        error.contains("producer is already closed"),
        "the close failure stays as context rather than being dropped: {error}"
    );
}
