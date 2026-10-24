"""Offline tests for the hand-written Connect stream decoder.

These exercise the framing rules the Python SDK implements by hand -- the
5-byte envelope, the untrusted length prefix, and the end-of-stream frame --
mirroring the cases ``sdks/go/session/envelope_test.go`` holds the Go decoder
to. No server is involved: the decoder is driven with byte strings.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from longtrader_sdk import session as sdk  # noqa: E402


def _pb():
    return sdk._pb()


def _frame(flags: int, payload: bytes) -> bytes:
    return bytes([flags]) + len(payload).to_bytes(4, "big") + payload


def _event_bytes(sequence: int) -> bytes:
    pb = _pb()
    msg = pb.StrategyEvent()
    msg.header.sequence = sequence
    return msg.SerializeToString()


def _chunks(*frames: bytes):
    """Yield each frame as its own chunk (one iteration of the pump)."""
    yield from frames


# -- frame-length bound --------------------------------------------------------


def test_max_frame_length_matches_the_go_decoder():
    assert sdk.MAX_FRAME_LENGTH == 64 << 20


def test_the_bound_accepts_the_limit_and_refuses_one_more():
    sdk._check_frame_length(sdk.MAX_FRAME_LENGTH)  # must not raise
    with pytest.raises(ValueError, match="exceeds the maximum length"):
        sdk._check_frame_length(sdk.MAX_FRAME_LENGTH + 1)


def test_decode_envelope_refuses_an_oversized_claim():
    with pytest.raises(ValueError, match="exceeds the maximum length"):
        sdk._decode_envelope(b"\x00\xff\xff\xff\xff\x01")


def test_first_message_payload_refuses_an_oversized_claim():
    with pytest.raises(ValueError, match="exceeds the maximum length"):
        sdk._first_message_payload(b"\x00\xff\xff\xff\xff\x01")


def test_the_bound_is_checked_before_truncation():
    """A 5-byte header alone already proves the claim impossible."""
    with pytest.raises(ValueError, match="exceeds the maximum length"):
        sdk._decode_envelope(b"\x00\xff\xff\xff\xff")


def test_the_stream_decoder_refuses_an_oversized_claim():
    def chunks():
        yield b"\x00\xff\xff\xff\xff"  # 4 GiB announced
        while True:
            yield b"x" * 4096

    with pytest.raises(ValueError, match="exceeds the maximum length"):
        list(sdk._iter_events(chunks(), _pb().StrategyEvent))


def test_an_in_bound_claim_still_waits_for_the_payload():
    """The bound must not reject a merely-not-yet-complete frame."""
    header = b"\x00" + (4).to_bytes(4, "big")
    with pytest.raises(ValueError, match="truncated"):
        sdk._decode_envelope(header + b"ab")  # 2 of 4 payload bytes


# -- end-of-stream error frame -------------------------------------------------


def test_a_clean_close_out_carries_no_error():
    assert sdk._end_stream_frame_error(b'{"error":null,"metadata":{}}') is None


def test_a_fieldless_frame_carries_no_error():
    assert sdk._end_stream_frame_error(b"{}") is None


def test_a_non_json_frame_carries_no_error():
    assert sdk._end_stream_frame_error(b"not json") is None


def test_a_json_array_carries_no_error():
    assert sdk._end_stream_frame_error(b"[]") is None


def test_an_error_frame_maps_to_a_connect_error():
    err = sdk._end_stream_frame_error(
        b'{"error":{"code":"unauthenticated","message":"token expired"}}'
    )
    assert isinstance(err, sdk.ConnectError)
    assert err.status == 200
    assert err.code == "unauthenticated"
    assert err.message == "token expired"


def test_a_fieldless_error_still_reports_with_the_fallbacks():
    err = sdk._end_stream_frame_error(b'{"error":{}}')
    assert isinstance(err, sdk.ConnectError)
    assert err.code == "unknown"
    assert err.message == ""


# -- stream decoder behaviour --------------------------------------------------


def test_a_clean_end_of_stream_ends_the_stream_silently():
    frames = [
        _frame(sdk.FLAG_MESSAGE, _event_bytes(1)),
        _frame(sdk.FLAG_END_OF_STREAM, b'{"error":null}'),
    ]
    events = list(sdk._iter_events(_chunks(*frames), _pb().StrategyEvent))
    assert len(events) == 1
    assert events[0].header.sequence == 1


def test_an_error_end_of_stream_frame_raises():
    frames = [
        _frame(sdk.FLAG_MESSAGE, _event_bytes(1)),
        _frame(
            sdk.FLAG_END_OF_STREAM,
            b'{"error":{"code":"permission_denied","message":"no"}}',
        ),
    ]
    iterator = sdk._iter_events(_chunks(*frames), _pb().StrategyEvent)
    assert next(iterator).header.sequence == 1  # events arrive first
    with pytest.raises(sdk.ConnectError) as excinfo:
        next(iterator)
    assert excinfo.value.code == "permission_denied"


def test_a_sequence_gap_is_reported_as_none():
    frames = [
        _frame(sdk.FLAG_MESSAGE, _event_bytes(1)),
        _frame(sdk.FLAG_MESSAGE, _event_bytes(3)),
    ]
    events = list(sdk._iter_events(_chunks(*frames), _pb().StrategyEvent))
    assert events[1] is None, "1 -> 3 missed event 2"


def test_frames_may_straddle_chunk_boundaries():
    stream = _frame(sdk.FLAG_MESSAGE, _event_bytes(1)) + _frame(
        sdk.FLAG_MESSAGE, _event_bytes(2)
    )

    def split_chunks():
        # Cut the byte stream into awkward pieces, not frame-aligned.
        for i in range(0, len(stream), 7):
            yield stream[i : i + 7]

    events = list(sdk._iter_events(split_chunks(), _pb().StrategyEvent))
    assert [e.header.sequence for e in events] == [1, 2]
