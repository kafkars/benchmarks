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
fn a_failed_close_after_a_clean_run_keeps_the_measurement() {
    // The regression this replaces: a complete, validated measurement was
    // thrown away because teardown hiccuped, and the run reported a sentence
    // about shutting down instead of the evidence it had already gathered.
    // Every offer was made, answered, and settled before `close` was ever
    // called; each attempt runs in a fresh process, so there is no state a
    // failed close carries anywhere. The warning goes to stderr, which the
    // control plane captures.
    let sealed = seal(Ok("document"), Err(failure("producer is already closed")));

    assert_eq!(
        sealed.ok(),
        Some("document"),
        "a finished measurement outranks the teardown that followed it"
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

#[test]
fn only_a_measurement_failure_can_fail_a_seal() {
    // The whole rule in one place: the close result decides nothing on its
    // own. It can add context to a failure and it can be warned about after a
    // success, but it can neither create a failure nor erase one.
    assert!(
        seal(Ok("document"), Ok(())).is_ok(),
        "a clean run is a result"
    );
    assert!(
        seal(Ok("document"), Err(failure("close hiccup"))).is_ok(),
        "a close failure after a good measurement is not the run's verdict"
    );
    assert!(
        seal::<&str>(Err(failure("the phase failed")), Ok(())).is_err(),
        "a measurement failure is the run's verdict"
    );
    assert!(
        seal::<&str>(
            Err(failure("the phase failed")),
            Err(failure("close hiccup"))
        )
        .is_err(),
        "and stays it when the close fails too"
    );
}
