//! Preserving a finished measurement when client teardown reports a failure.

use std::error::Error;

/// Reports what the measurement said, not what shutting down afterwards said.
///
/// The session is closed on every path. A close failure after a good
/// measurement cannot erase complete evidence; a close failure after a bad
/// measurement is appended as context rather than replacing the root cause.
pub(in crate::producer) fn seal<T>(
    outcome: Result<T, Box<dyn Error>>,
    closed: Result<(), Box<dyn Error>>,
) -> Result<T, Box<dyn Error>> {
    match (outcome, closed) {
        (Ok(document), Ok(())) => Ok(document),
        (Ok(document), Err(close)) => {
            eprintln!(
                "kafkars: the measurement completed and the session then failed to close: {close}"
            );
            Ok(document)
        }
        (Err(failure), Ok(())) => Err(failure),
        (Err(failure), Err(close)) => {
            Err(format!("{failure}; the session then failed to close: {close}").into())
        }
    }
}
