//! Contract tests for deterministic cross-caller admission order.

use std::error::Error;

use super::admission_turn::AdmissionTurn;

#[test]
fn retries_retain_the_same_turn_until_admission() -> Result<(), Box<dyn Error>> {
    let turn = AdmissionTurn::new();
    turn.wait(0)?.retry();
    turn.wait(0)?.complete()?;
    turn.wait(1)?.complete()?;
    Ok(())
}

#[test]
fn abandoned_turn_fails_later_callers() -> Result<(), Box<dyn Error>> {
    let turn = AdmissionTurn::new();
    drop(turn.wait(0)?);
    assert!(turn.wait(1).is_err());
    Ok(())
}
