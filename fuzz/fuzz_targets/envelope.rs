#![no_main]

//! Fuzz the Connect streaming envelope decoder.
//!
//! `longtrader_worker::envelope` parses the 5-byte-prefixed framing used by
//! Connect server-streaming RPCs, so it consumes bytes from a remote host and
//! is a genuine parser of untrusted input. The invariants a decoder like this
//! must hold are:
//!
//! 1. it never panics, whatever bytes arrive;
//! 2. a successful decode is internally consistent — the reported payload is
//!    exactly the slice it claims, and the consumed length lies inside the
//!    buffer;
//! 3. the cursor it reports always makes progress, so a caller walking a stream
//!    cannot spin forever.

use libfuzzer_sys::fuzz_target;
use longtrader_worker::envelope::{decode_envelope_result, encode_envelope, FLAG_MESSAGE};

fuzz_target!(|data: &[u8]| {
    // Property 1 + 2: decode the whole buffer, then walk it as a stream.
    let mut cursor = 0usize;
    let mut messages = 0usize;
    while cursor < data.len() {
        // Bound the walk so a pathological buffer cannot make the fuzzer slow.
        if messages > 64 {
            break;
        }
        match decode_envelope_result(&data[cursor..]) {
            Ok((flags, payload, consumed)) => {
                assert!(consumed >= 5, "an envelope must consume at least its header");
                assert!(cursor + consumed <= data.len(), "decode ran past the buffer");
                assert_eq!(payload, &data[cursor + 5..cursor + consumed]);
                assert_eq!(flags, data[cursor]);
                assert!(consumed > 0, "the cursor must advance");
                cursor += consumed;
                messages += 1;
            }
            Err(_) => break,
        }
    }

    // Property 3, from the other direction: anything the encoder produces must
    // decode, so the round trip cannot be broken by a change on either side.
    for len in [0usize, 1, 5, 255] {
        let payload = &data[..len.min(data.len())];
        let encoded = encode_envelope(FLAG_MESSAGE, payload);
        let (flags, decoded, consumed) =
            decode_envelope_result(&encoded).expect("a freshly encoded envelope decodes");
        assert_eq!(flags, FLAG_MESSAGE);
        assert_eq!(decoded, payload);
        assert_eq!(consumed, encoded.len());
    }
});