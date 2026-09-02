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

/// Decode one envelope from the front of `buf`.
///
/// Returns `(flags, payload_slice, consumed_bytes)` on success, `None` if
/// `buf` is too short or truncated. Caller advances by `consumed_bytes`.
#[inline]
pub fn decode_envelope(buf: &[u8]) -> Option<(u8, &[u8], usize)> {
    if buf.len() < 5 {
        return None;
    }
    let flags = buf[0];
    let len = u32::from_be_bytes(buf[1..5].try_into().ok()?) as usize;
    if buf.len() < 5 + len {
        return None;
    }
    Some((flags, &buf[5..5 + len], 5 + len))
}

#[cfg(test)]
mod tests {
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
    fn decode_truncated_returns_none() {
        assert!(decode_envelope(&[0x00, 0, 0, 0]).is_none());
        let mut truncated = encode_envelope(FLAG_MESSAGE, b"abc");
        truncated.truncate(6);
        assert!(decode_envelope(&truncated).is_none());
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
}
