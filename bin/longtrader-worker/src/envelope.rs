//! Connect streaming envelope: 5-byte prefix (flags/length/payload).
//!
//! Server-streaming RPCs (`StreamStrategyEvents`) use
//! `Content-Type: application/connect+proto`. Body is a sequence of
//! 5-byte-prefixed envelopes (see `docs/bare-protocol-guide.md` §5):
//! - byte 0: flags — `0x00` = message data, `0x02` = end-of-stream (JSON)
//! - bytes 1..5: u32 big-endian payload length
//! - bytes 5..: payload (serialized proto message, or JSON object when flags=`0x02`)
//!
//! `connectrpc` handles this framing internally; these helpers exist so
//! bare-protocol clients (and tests) can encode/decode without re-implementing.

/// Flag for a normal proto message envelope.
pub const FLAG_MESSAGE: u8 = 0x00;
/// Flag for the terminal end-of-stream JSON envelope.
pub const FLAG_EOS: u8 = 0x02;

/// Encode one envelope: flags + big-endian length + payload.
#[inline]
pub fn encode_envelope(flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flags);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode errors for [`decode_envelope_result`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    #[error("buffer too short for 5-byte header")]
    TooShort,
    #[error("truncated payload: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
}

/// Decode one envelope, returning a typed error instead of silent `None`.
#[inline]
pub fn decode_envelope_result(buf: &[u8]) -> Result<(u8, &[u8], usize), EnvelopeError> {
    if buf.len() < 5 {
        return Err(EnvelopeError::TooShort);
    }
    let flags = buf[0];
    let len = u32::from_be_bytes(buf[1..5].try_into().expect("5-byte header")) as usize;
    if buf.len() < 5 + len {
        return Err(EnvelopeError::Truncated { need: 5 + len, have: buf.len() });
    }
    Ok((flags, &buf[5..5 + len], 5 + len))
}

/// Decode one envelope from the front of `buf`.
///
/// Returns `(flags, payload_slice, consumed_bytes)` on success, or an error
/// if `buf` is too short or truncated. Caller advances by `consumed_bytes`.
#[inline]
pub fn decode_envelope(buf: &[u8]) -> Result<(u8, &[u8], usize), EnvelopeError> {
    decode_envelope_result(buf)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn roundtrip_message_envelope() {
        let payload = b"hello proto";
        let encoded = encode_envelope(FLAG_MESSAGE, payload);
        assert_eq!(encoded[0], FLAG_MESSAGE);
        let (flags, body, consumed) = decode_envelope(&encoded).expect("decode");
        assert_eq!(flags, FLAG_MESSAGE);
        assert_eq!(body, payload);
        assert_eq!(consumed, 5 + payload.len());
    }

    #[test]
    fn eos_envelope_carries_json() {
        let json = br#"{"error":null}"#;
        let encoded = encode_envelope(FLAG_EOS, json);
        assert_eq!(encoded[0], FLAG_EOS);
        let (flags, body, _) = decode_envelope(&encoded).expect("decode");
        assert_eq!(flags, FLAG_EOS);
        assert_eq!(body, json);
    }

    #[test]
    fn decode_truncated_returns_err() {
        assert!(decode_envelope(&[0x00, 0, 0, 0]).is_err());
        let mut truncated = encode_envelope(FLAG_MESSAGE, b"abc");
        truncated.truncate(6);
        assert!(decode_envelope(&truncated).is_err());
    }

    #[test]
    fn decode_concatenated_stream() {
        let a = encode_envelope(FLAG_MESSAGE, b"first");
        let b = encode_envelope(FLAG_MESSAGE, b"second");
        let mut stream = Vec::new();
        stream.extend_from_slice(&a);
        stream.extend_from_slice(&b);
        let (f1, p1, c1) = decode_envelope(&stream).expect("first");
        assert_eq!(f1, FLAG_MESSAGE);
        assert_eq!(p1, b"first");
        let (f2, p2, c2) = decode_envelope(&stream[c1..]).expect("second");
        assert_eq!(f2, FLAG_MESSAGE);
        assert_eq!(p2, b"second");
        assert_eq!(c1 + c2, stream.len());
    }

    // -----------------------------------------------------------------------
    // Header boundary
    // -----------------------------------------------------------------------

    /// A header is exactly five bytes; anything shorter cannot even be read for
    /// a length, which is a distinct condition from a short payload.
    #[test]
    fn a_buffer_shorter_than_the_header_reports_too_short() {
        for len in 0..5usize {
            assert_eq!(
                decode_envelope(&vec![0u8; len]),
                Err(EnvelopeError::TooShort),
                "{len} bytes must be TooShort"
            );
        }
    }

    /// A complete header with an incomplete payload names both the total needed
    /// and what it actually got, so a client can size its read.
    #[test]
    fn an_incomplete_payload_reports_need_and_have() {
        let encoded = encode_envelope(FLAG_MESSAGE, b"abcdefgh");
        // Keep the header (5 bytes) plus 3 of 8 payload bytes.
        let partial = &encoded[..8];
        assert_eq!(decode_envelope(partial), Err(EnvelopeError::Truncated { need: 13, have: 8 }));
    }

    /// Exactly the declared payload length is enough; one byte less is not.
    #[test]
    fn the_payload_boundary_is_inclusive() {
        let encoded = encode_envelope(FLAG_MESSAGE, b"abcd");
        assert!(decode_envelope(&encoded).is_ok(), "exactly 4 payload bytes decode");
        assert_eq!(
            decode_envelope(&encoded[..encoded.len() - 1]),
            Err(EnvelopeError::Truncated { need: 9, have: 8 })
        );
    }

    #[test]
    fn errors_render_distinct_messages() {
        assert_eq!(EnvelopeError::TooShort.to_string(), "buffer too short for 5-byte header");
        assert_eq!(
            EnvelopeError::Truncated { need: 10, have: 7 }.to_string(),
            "truncated payload: need 10 bytes, have 7"
        );
    }

    // -----------------------------------------------------------------------
    // Payloads
    // -----------------------------------------------------------------------

    /// A zero-length payload is legal: the end-of-stream envelope carries an
    /// empty body on some Connect implementations.
    #[test]
    fn an_empty_payload_round_trips() {
        let encoded = encode_envelope(FLAG_EOS, b"");
        assert_eq!(encoded.len(), 5, "an empty envelope is just the header");
        assert_eq!(encoded[0], FLAG_EOS);
        assert_eq!(encoded[1..5], [0, 0, 0, 0]);
        let (flags, payload, consumed) = decode_envelope(&encoded).expect("decode");
        assert_eq!(flags, FLAG_EOS);
        assert!(payload.is_empty());
        assert_eq!(consumed, 5);
    }

    /// Trailing bytes beyond the envelope must be left for the next read, not
    /// swallowed: this is what lets a caller advance by `consumed`.
    #[test]
    fn trailing_bytes_are_left_for_the_next_envelope() {
        let mut stream = encode_envelope(FLAG_MESSAGE, b"a");
        let tail = b"-TRAILER";
        stream.extend_from_slice(tail);
        let (_, payload, consumed) = decode_envelope(&stream).expect("decode");
        assert_eq!(payload, b"a");
        assert_eq!(&stream[consumed..], tail);
    }

    /// The flags byte is carried through verbatim; the decoder must not reject or
    /// rewrite a flag value it does not itself define (a compression flag, for
    /// instance, belongs to the caller).
    #[test]
    fn unknown_flags_are_passed_through_untouched() {
        for flags in [0x01u8, 0x03, 0x80, 0xFF] {
            let encoded = encode_envelope(flags, b"payload");
            let (decoded, payload, _) = decode_envelope(&encoded).expect("decode");
            assert_eq!(decoded, flags, "flag {flags:#04x} must survive");
            assert_eq!(payload, b"payload");
        }
    }

    /// The length prefix is big-endian on the wire, so pin the byte order
    /// explicitly rather than only through a round trip.
    #[test]
    fn the_length_prefix_is_big_endian() {
        let encoded = encode_envelope(FLAG_MESSAGE, &[7u8; 258]);
        assert_eq!(&encoded[1..5], &[0, 0, 1, 2], "258 == 0x00000102 big-endian");
        let (_, payload, consumed) = decode_envelope(&encoded).expect("decode");
        assert_eq!(payload.len(), 258);
        assert_eq!(consumed, 5 + 258);
    }

    /// `decode_envelope` is a documented alias of `decode_envelope_result`; the
    /// two must never drift.
    #[test]
    fn the_alias_behaves_identically_to_the_result_decoder() {
        for buf in [vec![], vec![0u8; 4], vec![0u8; 5], vec![0u8; 9]] {
            assert_eq!(
                decode_envelope(&buf),
                decode_envelope_result(&buf),
                "{buf:?} decoded differently through the two entry points"
            );
        }
    }

    /// A header claiming a huge length must report the demand rather than
    /// allocate or index out of bounds.
    #[test]
    fn a_hostile_length_is_reported_not_honoured() {
        let buf = [0x00u8, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(
            decode_envelope(&buf),
            Err(EnvelopeError::Truncated { need: 5 + u32::MAX as usize, have: 5 })
        );
    }

    // -----------------------------------------------------------------------
    // Stream decoding
    // -----------------------------------------------------------------------

    /// Decoding a whole stream by repeatedly advancing by `consumed` must
    /// reconstruct exactly the payloads that were encoded, in order.
    #[test]
    fn a_multi_envelope_stream_round_trips_in_order() {
        let payloads: [&[u8]; 5] = [b"", b"a", b"two", b"", b"five!"];
        let mut stream = Vec::new();
        for payload in payloads {
            stream.extend_from_slice(&encode_envelope(FLAG_MESSAGE, payload));
        }
        stream.extend_from_slice(&encode_envelope(FLAG_EOS, br#"{"error":null}"#));

        let mut decoded = Vec::new();
        let mut cursor = 0usize;
        while cursor < stream.len() {
            let (flags, payload, consumed) = decode_envelope(&stream[cursor..]).expect("decode");
            decoded.push((flags, payload.to_vec()));
            cursor += consumed;
            assert!(consumed > 0, "a decoded envelope must consume at least its header");
        }
        assert_eq!(cursor, stream.len(), "the cursor must land exactly at the end");

        let messages: Vec<&[u8]> = decoded
            .iter()
            .filter(|(flags, _)| *flags == FLAG_MESSAGE)
            .map(|(_, payload)| payload.as_slice())
            .collect();
        assert_eq!(messages, payloads.to_vec());
        let eos = decoded.last().expect("the eos envelope");
        assert_eq!(eos.0, FLAG_EOS);
        assert_eq!(eos.1, br#"{"error":null}"#);
    }

    /// Every prefix of a valid stream must either decode cleanly or fail with a
    /// typed error — never panic, never read past the end.
    #[test]
    fn truncating_a_valid_stream_never_panics() {
        let mut stream = Vec::new();
        for payload in [b"first".as_slice(), b"second", b""] {
            stream.extend_from_slice(&encode_envelope(FLAG_MESSAGE, payload));
        }
        for cut in 0..=stream.len() {
            let prefix = &stream[..cut];
            // Walk the prefix as far as it decodes; every step must be a clean
            // error rather than a panic.
            let mut cursor = 0usize;
            while cursor < prefix.len() {
                match decode_envelope(&prefix[cursor..]) {
                    Ok((_, _, consumed)) => cursor += consumed,
                    Err(EnvelopeError::TooShort | EnvelopeError::Truncated { .. }) => break,
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    /// Payload bytes are arbitrary: anything that fits the length prefix must
    /// survive the round trip byte for byte.
    fn arb_payload() -> impl proptest::strategy::Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..256)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn encode_then_decode_recovers_the_payload(flags in any::<u8>(), payload in arb_payload()) {
            let encoded = encode_envelope(flags, &payload);
            prop_assert_eq!(encoded.len(), 5 + payload.len());
            let (decoded_flags, decoded_payload, consumed) =
                decode_envelope(&encoded).expect("a freshly encoded envelope decodes");
            prop_assert_eq!(decoded_flags, flags);
            prop_assert_eq!(decoded_payload, &payload[..]);
            prop_assert_eq!(consumed, 5 + payload.len());
        }

        /// Any byte string either decodes or yields a typed error — the
        /// property a network-facing parser must hold.
        #[test]
        fn decoding_arbitrary_bytes_never_panics(buf in proptest::collection::vec(any::<u8>(), 0..64)) {
            if let Ok((_, payload, consumed)) = decode_envelope(&buf) {
                prop_assert!(consumed >= 5, "consumed {consumed} is below the header size");
                prop_assert!(consumed <= buf.len(), "consumed {consumed} exceeds the buffer");
                prop_assert_eq!(&buf[5..consumed], payload);
            }
        }

        /// Appending arbitrary junk after a valid envelope never changes how the
        /// envelope itself decodes.
        #[test]
        fn trailing_junk_does_not_change_the_decoded_envelope(
            flags in any::<u8>(),
            payload in arb_payload(),
            junk in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let mut buf = encode_envelope(flags, &payload);
            buf.extend_from_slice(&junk);
            let (decoded_flags, decoded_payload, _) = decode_envelope(&buf).expect("decodes");
            prop_assert_eq!(decoded_flags, flags);
            prop_assert_eq!(decoded_payload, &payload[..]);
        }

        /// A buffer one byte short of the declared payload is always
        /// `Truncated`, naming the exact shortfall.
        #[test]
        fn a_short_payload_is_always_truncated(
            payload in proptest::collection::vec(any::<u8>(), 1..64),
        ) {
            let mut buf = encode_envelope(FLAG_MESSAGE, &payload);
            buf.pop();
            match decode_envelope(&buf) {
                Err(EnvelopeError::Truncated { need, have }) => {
                    prop_assert_eq!(need, 5 + payload.len());
                    prop_assert_eq!(have, 4 + payload.len());
                }
                Err(other) => prop_assert!(false, "expected Truncated, got {other:?}"),
                Ok((_, _, consumed)) => {
                    prop_assert!(false, "expected Truncated, consumed {consumed}");
                }
            }
        }
    }
}
