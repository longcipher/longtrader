//! Session lifecycle states (server-enforced, design doc §6.3).
//!
//! Extracted from `session::mod` so the state machine has a single owner.
//! Valid transitions:
//! `ATTACHED -> SYNCING -> ACTIVE -> {KILL_SWITCH_TRIPPED | GRACEFUL_SHUTDOWN}`.

use crate::proto::worker;

/// Server-enforced session states mirroring `worker.v1.SessionState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionState {
    Attached = 1,
    Syncing = 2,
    Active = 3,
    KillSwitchTripped = 4,
    GracefulShutdown = 5,
}

impl SessionState {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Attached),
            2 => Some(Self::Syncing),
            3 => Some(Self::Active),
            4 => Some(Self::KillSwitchTripped),
            5 => Some(Self::GracefulShutdown),
            _ => None,
        }
    }

    #[must_use]
    pub fn to_proto(self) -> worker::SessionState {
        match self {
            Self::Attached => worker::SessionState::Attached,
            Self::Syncing => worker::SessionState::Syncing,
            Self::Active => worker::SessionState::Active,
            Self::KillSwitchTripped => worker::SessionState::KillSwitchTripped,
            Self::GracefulShutdown => worker::SessionState::GracefulShutdown,
        }
    }

    /// Returns `true` for terminal states that must never be resurrected.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::KillSwitchTripped | Self::GracefulShutdown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant, in lifecycle order.
    const ALL: [SessionState; 5] = [
        SessionState::Attached,
        SessionState::Syncing,
        SessionState::Active,
        SessionState::KillSwitchTripped,
        SessionState::GracefulShutdown,
    ];

    #[test]
    fn round_trips_u8() {
        for (v, s) in [
            (1, SessionState::Attached),
            (2, SessionState::Syncing),
            (3, SessionState::Active),
            (4, SessionState::KillSwitchTripped),
            (5, SessionState::GracefulShutdown),
        ] {
            assert_eq!(SessionState::from_u8(v), Some(s));
        }
        assert_eq!(SessionState::from_u8(0), None);
        assert_eq!(SessionState::from_u8(99), None);
    }

    /// Every discriminant must round trip through `u8`; the two functions are
    /// separate matches and a new variant added to one but not the other would
    /// silently become unreachable.
    #[test]
    fn every_state_round_trips_through_its_discriminant() {
        for state in ALL {
            let byte = state as u8;
            assert_eq!(SessionState::from_u8(byte), Some(state), "{state:?} did not round trip");
        }
    }

    /// Only the five assigned discriminants decode; everything else is corrupt
    /// and must fail closed rather than resolve to a default.
    #[test]
    fn only_the_assigned_discriminants_decode() {
        for byte in 0u8..=255 {
            let expected = (1..=5).contains(&byte);
            assert_eq!(
                SessionState::from_u8(byte).is_some(),
                expected,
                "discriminant {byte} decoded unexpectedly"
            );
        }
    }

    /// `to_proto` must project onto the wire enum without collapsing two states
    /// onto one value.
    #[test]
    fn every_state_maps_to_its_own_wire_value() {
        let mapped: Vec<worker::SessionState> =
            ALL.iter().copied().map(SessionState::to_proto).collect();
        assert_eq!(
            mapped,
            vec![
                worker::SessionState::Attached,
                worker::SessionState::Syncing,
                worker::SessionState::Active,
                worker::SessionState::KillSwitchTripped,
                worker::SessionState::GracefulShutdown,
            ]
        );
    }

    /// Terminal states must never be resurrected: a reconnecting session that
    /// lands on one has to be refused.
    #[test]
    fn only_the_two_end_states_are_terminal() {
        assert!(!SessionState::Attached.is_terminal());
        assert!(!SessionState::Syncing.is_terminal());
        assert!(!SessionState::Active.is_terminal());
        assert!(SessionState::KillSwitchTripped.is_terminal());
        assert!(SessionState::GracefulShutdown.is_terminal());
    }

    /// Every non-terminal state is one a reconnect is allowed to reuse.
    #[test]
    fn every_non_terminal_state_is_reconnectable() {
        for state in ALL {
            assert_eq!(
                state.is_terminal(),
                matches!(state, SessionState::KillSwitchTripped | SessionState::GracefulShutdown),
                "{state:?} classified wrongly"
            );
        }
    }
}
