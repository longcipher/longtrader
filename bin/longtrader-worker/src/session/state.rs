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
}
