//! Wall-clock helpers for the session control plane.
//!
//! Single owner for "now" in this module: the event bus, the service layer and
//! the proxies each need epoch millis, a protobuf `Timestamp` and monotonic
//! nanos, and three private copies of that arithmetic is how rounding rules and
//! overflow behaviour drift apart. Every conversion here saturates instead of
//! wrapping — a far-future timestamp must not become 1970.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall clock as Unix milliseconds, saturating at [`i64::MAX`].
#[inline]
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Current wall clock as Unix nanoseconds, saturating at [`i64::MAX`].
#[inline]
#[must_use]
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// Build a `google.protobuf.Timestamp` from Unix milliseconds.
///
/// Negative millis (pre-epoch) round toward negative infinity via
/// `div_euclid`/`rem_euclid`, so `seconds`/`nanos` always describe the same
/// instant the protobuf JSON mapping expects.
#[inline]
#[must_use]
pub fn timestamp_from_ms(ms: i64) -> buffa_types::google::protobuf::Timestamp {
    buffa_types::google::protobuf::Timestamp {
        seconds: ms.div_euclid(1000),
        // The ms remainder (< 1000) scaled to nanos always fits in i32.
        nanos: i32::try_from(ms.rem_euclid(1000) * 1_000_000).unwrap_or(0),
        ..Default::default()
    }
}

/// Build a `google.protobuf.Timestamp` for the current wall clock.
#[inline]
#[must_use]
pub fn now_ts() -> buffa_types::google::protobuf::Timestamp {
    timestamp_from_ms(now_ms())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ms_is_plausible_epoch_millis() {
        let ms = now_ms();
        // Anything after 2001-09-09 and before year 3000 in millis.
        assert!(ms > 1_000_000_000_000, "now_ms {ms} is not epoch millis");
        assert!(ms < 32_503_680_000_000, "now_ms {ms} is implausibly large");
    }

    #[test]
    fn now_ns_is_millis_times_roughly_a_million() {
        let ms = now_ms();
        let ns = now_ns();
        assert!(ns >= ms * 1_000_000, "ns {ns} must not predate ms {ms}");
    }

    #[test]
    fn timestamp_from_ms_splits_seconds_and_nanos() {
        let ts = timestamp_from_ms(1_234_567_890);
        assert_eq!(ts.seconds, 1_234_567);
        assert_eq!(ts.nanos, 890_000_000);
    }

    #[test]
    fn timestamp_from_ms_handles_negative_millis_without_wrapping() {
        // -1 ms is 1969-12-31T23:59:59.999, not second -1 with a positive remainder.
        let ts = timestamp_from_ms(-1);
        assert_eq!(ts.seconds, -1);
        assert_eq!(ts.nanos, 999_000_000);
    }

    #[test]
    fn timestamp_from_ms_saturates_instead_of_wrapping() {
        let ts = timestamp_from_ms(i64::MAX);
        assert_eq!(ts.seconds, i64::MAX / 1000);
        assert!(ts.nanos >= 0 && ts.nanos < 1_000_000_000);
    }

    #[test]
    fn now_ts_agrees_with_now_ms() {
        let ts = now_ts();
        let ms = now_ms();
        let reconstructed = ts.seconds * 1000 + i64::from(ts.nanos) / 1_000_000;
        assert!(
            (reconstructed - ms).abs() < 1_000,
            "now_ts {reconstructed} must track now_ms {ms}"
        );
    }
}
