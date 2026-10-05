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
    use proptest::prelude::*;

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

    // -----------------------------------------------------------------------
    // Bounds are inclusive at both ends
    // -----------------------------------------------------------------------

    /// The floor and ceiling are the documented DoS bound and sanity bound; both
    /// must be accepted exactly, and one step outside must be refused.
    #[test]
    fn the_bounds_are_inclusive_at_both_ends() {
        let min = lease_timeout_from_proto(&dur(0, 500_000_000)).expect("the floor is valid");
        assert_eq!(min, MIN_LEASE);
        let max = lease_timeout_from_proto(&dur(3600, 0)).expect("the ceiling is valid");
        assert_eq!(max, MAX_LEASE);

        let below = lease_timeout_from_proto(&dur(0, 499_999_999)).expect_err("below the floor");
        assert!(below.to_string().contains("out of bounds"), "{below}");
        // `nanos` truncate to whole milliseconds, so 3600s + 1ns is still 3600s;
        // only a larger `seconds` crosses the ceiling.
        assert_eq!(
            lease_timeout_from_proto(&dur(3600, 999_999)).expect("still the ceiling"),
            MAX_LEASE
        );
        let above = lease_timeout_from_proto(&dur(3600, 1_000_000)).expect_err("above the ceiling");
        assert!(above.to_string().contains("out of bounds"), "{above}");
    }

    /// Sub-millisecond nanos are truncated toward zero, so a duration of
    /// 999_999_999ns is 999ms, not 1000ms.
    #[test]
    fn sub_millisecond_nanos_truncate_rather_than_round() {
        let d = lease_timeout_from_proto(&dur(1, 999_999_999)).expect("1s999999999ns is in range");
        assert_eq!(d, Duration::from_millis(1999), "1s + 999ms, not 1s + 1000ms");
    }

    #[test]
    fn seconds_and_nanos_accumulate() {
        assert_eq!(
            lease_timeout_from_proto(&dur(2, 500_000_000)).expect("in range"),
            Duration::from_millis(2500)
        );
    }

    /// A negative `nanos` field subtracts from the seconds, as protobuf's
    /// normalization allows.
    #[test]
    fn a_negative_nanos_field_is_accounted_for() {
        // 1s - 1ms = 999ms, inside the bounds.
        let d = lease_timeout_from_proto(&dur(1, -1_000_000)).expect("999ms is inside the floor");
        assert_eq!(d, Duration::from_millis(999));
    }

    /// `i64::MAX` seconds overflows the millisecond conversion and must be
    /// reported, not wrapped into a plausible-looking lease.
    #[test]
    fn a_huge_seconds_value_reports_overflow() {
        let err = lease_timeout_from_proto(&dur(i64::MAX, 0)).expect_err("overflow");
        assert!(matches!(err, PortError::InvalidArgument(_)), "{err:?}");
        assert!(err.to_string().contains("overflow"), "{err}");
    }

    /// `i64::MIN` seconds overflows the millisecond conversion before the sign
    /// check runs, so it is reported as an overflow rather than as a negative.
    #[test]
    fn a_hugely_negative_value_is_rejected() {
        let err = lease_timeout_from_proto(&dur(i64::MIN, 0)).expect_err("negative");
        let message = err.to_string();
        assert!(
            message.contains("overflow") || message.contains("must be >= 0"),
            "the rejection must name the problem: {message}"
        );
    }

    #[test]
    fn a_negative_millis_result_is_rejected_before_the_cast() {
        // -1s + 999ms = -1ms, which is negative and must not wrap to a huge u64.
        let err = lease_timeout_from_proto(&dur(-1, 999_000_000)).expect_err("negative");
        assert!(err.to_string().contains("must be >= 0"), "{err}");
    }

    /// Every rejection is an `InvalidArgument`, which the RPC layer maps to a
    /// client error rather than an internal fault.
    #[test]
    fn every_rejection_is_an_invalid_argument() {
        for bad in [dur(-1, 0), dur(0, 1), dur(0, 100_000), dur(7200, 0), dur(i64::MAX, 0)] {
            let err = lease_timeout_from_proto(&bad).expect_err("must be rejected");
            assert!(matches!(err, PortError::InvalidArgument(_)), "{bad:?} produced {err:?}");
        }
    }

    // -----------------------------------------------------------------------
    // default_lease
    // -----------------------------------------------------------------------

    /// The default is three missed heartbeats, so a strategy that heartbeats
    /// every 10s gets a 30s grace period.
    #[test]
    fn the_default_lease_is_three_heartbeats() {
        assert_eq!(default_lease(10_000), Duration::from_secs(30));
        assert_eq!(default_lease(1_000), Duration::from_secs(3));
        assert_eq!(default_lease(60_000), Duration::from_secs(180));
    }

    /// A heartbeat faster than the DoS floor still gets the floor, because a
    /// sub-500ms lease lets one lost packet kill a session.
    #[test]
    fn a_fast_heartbeat_is_clamped_up_to_the_floor() {
        assert_eq!(default_lease(0), MIN_LEASE, "a zero heartbeat still gets the floor");
        assert_eq!(default_lease(1), MIN_LEASE);
        assert_eq!(default_lease(166), MIN_LEASE);
        // 167ms * 3 == 501ms, the first value above the floor.
        assert_eq!(default_lease(167), Duration::from_millis(501));
    }

    /// A heartbeat slow enough to push past the ceiling is clamped down.
    #[test]
    fn a_slow_heartbeat_is_clamped_to_the_ceiling() {
        assert_eq!(default_lease(1_200_000), MAX_LEASE);
        assert_eq!(default_lease(1_200_001), MAX_LEASE, "the multiply saturates, then clamps");
        assert_eq!(default_lease(u64::MAX), MAX_LEASE, "the multiply must not overflow");
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Every accepted lease lies within the documented bounds.
        #[test]
        fn an_accepted_lease_is_always_in_bounds(
            secs in 0i64..7200,
            nanos in -1_000_000_000i32..1_000_000_000,
        ) {
            if let Ok(lease) = lease_timeout_from_proto(&dur(secs, nanos)) {
                prop_assert!(lease >= MIN_LEASE, "{secs}s {nanos}ns -> {lease:?} below the floor");
                prop_assert!(lease <= MAX_LEASE, "{secs}s {nanos}ns -> {lease:?} above the ceiling");
            }
        }

        /// A lease outside the bounds is always refused with an
        /// `InvalidArgument`, never accepted.
        #[test]
        fn an_out_of_bounds_lease_is_always_refused(secs in -100i64..-1) {
            prop_assert!(matches!(
                lease_timeout_from_proto(&dur(secs, 0)),
                Err(PortError::InvalidArgument(_))
            ));
        }

        /// `default_lease` never escapes the bounds, whatever the heartbeat.
        #[test]
        fn the_default_lease_never_escapes_the_bounds(heartbeat in any::<u64>()) {
            let lease = default_lease(heartbeat);
            prop_assert!(lease >= MIN_LEASE, "{heartbeat} -> {lease:?}");
            prop_assert!(lease <= MAX_LEASE, "{heartbeat} -> {lease:?}");
        }

        /// A slower heartbeat never yields a shorter lease.
        #[test]
        fn the_default_lease_is_monotonic(a in 0u64..400_000, b in 0u64..400_000) {
            prop_assume!(a <= b);
            prop_assert!(default_lease(a) <= default_lease(b), "{a}ms vs {b}ms");
        }

    }

    /// A heartbeat whose triple lands exactly on a bound keeps that bound.
    #[test]
    fn a_default_lease_lands_exactly_on_the_bounds() {
        assert_eq!(default_lease(500), Duration::from_millis(1500));
        assert_eq!(default_lease(1_200_000), MAX_LEASE);
    }
}
