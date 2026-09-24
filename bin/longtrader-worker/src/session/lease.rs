//! Lease-timeout parsing shared by attach / set-policy paths.
//!
//! Single owner for `google.protobuf.Duration -> std::time::Duration`
//! conversion with explicit bounds. Rejects negative / absurd values
//! instead of silently clamping them into a valid-looking lease.

use std::time::Duration;

use crate::ports::PortError;

/// Bounds for a session lease: 500ms (DoS floor) .. 3600s (sanity ceiling).
pub const MIN_LEASE: Duration = Duration::from_millis(500);
pub const MAX_LEASE: Duration = Duration::from_secs(3600);

/// Parse a proto `Duration` into a bounded std `Duration`.
///
/// # Errors
/// Returns `PortError::InvalidArgument` on negative values or overflow.
pub fn lease_timeout_from_proto(
    timeout: &buffa_types::google::protobuf::Duration,
) -> Result<Duration, PortError> {
    let millis = timeout
        .seconds
        .checked_mul(1000)
        .and_then(|s| s.checked_add(i64::from(timeout.nanos) / 1_000_000))
        .ok_or_else(|| {
            PortError::InvalidArgument(format!(
                "lease_timeout overflow: {}s {}ns",
                timeout.seconds, timeout.nanos
            ))
        })?;
    if millis < 0 {
        return Err(PortError::InvalidArgument(format!(
            "lease_timeout must be >= 0, got {millis}ms"
        )));
    }
    let dur = Duration::from_millis(millis.cast_unsigned());
    if dur < MIN_LEASE || dur > MAX_LEASE {
        return Err(PortError::InvalidArgument(format!(
            "lease_timeout {}ms out of bounds [{MIN_LEASE:?}, {MAX_LEASE:?}]",
            dur.as_millis()
        )));
    }
    Ok(dur)
}

/// Default lease: 3x heartbeat interval, clamped into bounds for safety.
#[must_use]
pub fn default_lease(heartbeat_ms: u64) -> Duration {
    Duration::from_millis(heartbeat_ms.saturating_mul(3)).clamp(MIN_LEASE, MAX_LEASE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dur(secs: i64, nanos: i32) -> buffa_types::google::protobuf::Duration {
        buffa_types::google::protobuf::Duration { seconds: secs, nanos, ..Default::default() }
    }

    #[test]
    fn accepts_normal_lease() {
        let d = lease_timeout_from_proto(&dur(30, 0)).expect("valid");
        assert_eq!(d, Duration::from_secs(30));
    }

    #[test]
    fn rejects_negative() {
        assert!(lease_timeout_from_proto(&dur(-1, 0)).is_err());
    }

    #[test]
    fn rejects_too_small_and_too_large() {
        assert!(lease_timeout_from_proto(&dur(0, 100_000)).is_err());
        assert!(lease_timeout_from_proto(&dur(7200, 0)).is_err());
    }
}
