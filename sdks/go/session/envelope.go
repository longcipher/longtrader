package session

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Connect streaming frame flags (see docs/bare-protocol-guide.md and
// bin/longtrader-worker/src/envelope.rs).
const (
	// FlagMessage marks a data frame carrying a protobuf message.
	FlagMessage byte = 0x00
	// FlagEndOfStream marks the terminating frame, whose payload is the JSON
	// end-stream message rather than protobuf.
	FlagEndOfStream byte = 0x02
	// EnvelopeHeaderSize is the size of the Connect envelope header: one flags
	// byte followed by a big-endian uint32 payload length.
	EnvelopeHeaderSize = 5
)

// MaxFrameLength bounds the payload length a peer may announce in one Connect
// envelope.
//
// The length is an untrusted uint32, so a hostile or buggy peer can claim up to
// 4 GiB. Without a bound the decoder has no way to tell a real frame from a
// bogus one until it has buffered the whole claim, and a peer that keeps
// sending filler drives the buffer to that claim: measured, a 5-byte
// `00 FF FF FF FF` header plus filler reached 768 MiB of heap in 8 seconds.
// 64 MiB is far above any real strategy event, market data update, or log
// event, so anything larger is a protocol error rather than a big message.
const MaxFrameLength = 64 << 20

// Envelope errors.
var (
	// ErrTruncatedFrame reports a header or payload shorter than announced.
	ErrTruncatedFrame = errors.New("session: truncated connect envelope")
	// ErrNoMessageFrame reports a body with no data frame at all.
	ErrNoMessageFrame = errors.New("session: no message frame in connect response")
	// ErrEndOfStream reports that a stream was terminated by the peer. The
	// StreamDecoder surfaces it as io.EOF so a plain range loop can end.
	ErrEndOfStream = errors.New("session: end of stream")
	// ErrFrameTooLarge reports an announced frame length above MaxFrameLength.
	ErrFrameTooLarge = errors.New("session: connect frame exceeds the maximum length")
)

// Envelope wraps payload in the 5-byte Connect envelope.
//
// The length is big-endian; a little-endian length is read by the peer as a
// multi-gigabyte frame, which is why the order is not negotiable.
func Envelope(payload []byte, flags byte) []byte {
	out := make([]byte, EnvelopeHeaderSize+len(payload))
	out[0] = flags
	binary.BigEndian.PutUint32(out[1:EnvelopeHeaderSize], uint32(len(payload)))
	copy(out[EnvelopeHeaderSize:], payload)
	return out
}

// EncodeMessage frames payload as a data frame (flags 0x00).
func EncodeMessage(payload []byte) []byte { return Envelope(payload, FlagMessage) }

// EncodeEndOfStream frames an end-of-stream payload (JSON).
func EncodeEndOfStream(payload []byte) []byte { return Envelope(payload, FlagEndOfStream) }

// DecodeEnvelope returns the payload of a single message frame.
//
// It returns ErrEndOfStream for an end-of-stream frame (its JSON payload
// carries nothing the caller can use) and ErrTruncatedFrame when fewer than
// EnvelopeHeaderSize bytes are available.
func DecodeEnvelope(frame []byte) ([]byte, error) {
	if len(frame) < EnvelopeHeaderSize {
		return nil, ErrTruncatedFrame
	}
	if frame[0]&FlagEndOfStream != 0 {
		return nil, ErrEndOfStream
	}
	length := int64(binary.BigEndian.Uint32(frame[1:EnvelopeHeaderSize]))
	if length > MaxFrameLength {
		return nil, fmt.Errorf("%w: %d bytes announced, limit %d", ErrFrameTooLarge, length, MaxFrameLength)
	}
	if int64(len(frame)-EnvelopeHeaderSize) < length {
		return nil, ErrTruncatedFrame
	}
	return frame[EnvelopeHeaderSize : EnvelopeHeaderSize+int(length)], nil
}

// FirstMessagePayload returns the first data-frame payload in buf.
//
// A client-streaming reply that carries a single response message (ReportLog)
// is one data frame followed by an end-of-stream frame whose payload is JSON.
// Decoding the whole body as protobuf fails, so the data frame is located
// first.
func FirstMessagePayload(buf []byte) ([]byte, error) {
	for pos := 0; pos+EnvelopeHeaderSize <= len(buf); {
		// The announced length is untrusted. On 64-bit it can never be
		// negative, so the bound is the only check that means anything; the
		// overflow-safe comparison below is what keeps a bogus huge claim from
		// wrapping into an apparently small end offset.
		length := int64(binary.BigEndian.Uint32(buf[pos+1 : pos+EnvelopeHeaderSize]))
		if length > MaxFrameLength {
			return nil, fmt.Errorf("%w: %d bytes announced, limit %d", ErrFrameTooLarge, length, MaxFrameLength)
		}
		if int64(len(buf)-pos-EnvelopeHeaderSize) < length {
			return nil, ErrTruncatedFrame
		}
		end := pos + EnvelopeHeaderSize + int(length)
		if buf[pos]&FlagEndOfStream == 0 {
			return buf[pos+EnvelopeHeaderSize : end], nil
		}
		pos = end
	}
	return nil, ErrNoMessageFrame
}

// StreamDecoder turns a Connect protobuf byte stream into message payloads.
//
// Frames may straddle read boundaries, so a running buffer is kept and each
// call blocks until one complete frame is available.
type StreamDecoder struct {
	r    io.Reader
	buf  []byte
	off  int
	end  int
	done bool
	eos  []byte
}

// NewStreamDecoder wraps r, typically the body of a connect+proto response.
func NewStreamDecoder(r io.Reader) *StreamDecoder {
	return &StreamDecoder{r: r}
}

// Next returns the payload of the next data frame.
//
// It returns io.EOF once the stream is finished -- either because an
// end-of-stream frame arrived or because the reader hit end-of-file -- so
// `for { payload, err := dec.Next(); if err == io.EOF { break } }` is the
// natural loop. A truncated trailing frame is reported as a decode error
// rather than a clean end, because a partial message is data loss.
func (d *StreamDecoder) Next() ([]byte, error) {
	// An end-of-stream frame is final: anything after it is a protocol error
	// on the peer, not a message the client should act on.
	for !d.done {
		payload, err, ok := d.nextBuffered()
		if ok {
			if err != nil {
				return nil, err
			}
			return payload, nil
		}
		n, err := d.r.Read(d.grow())
		if n > 0 {
			d.end += n
			continue
		}
		if err == nil {
			err = io.EOF
		}
		if err == io.EOF && d.off == d.end {
			break
		}
		return nil, ErrTruncatedFrame
	}
	d.done = true
	return nil, io.EOF
}

// grow returns the writable tail of the buffer, extending it when needed.
func (d *StreamDecoder) grow() []byte {
	if cap(d.buf)-d.end < 4096 {
		next := make([]byte, d.end, 2*cap(d.buf)+4096)
		copy(next, d.buf[:d.end])
		d.buf = next
	}
	return d.buf[d.end:cap(d.buf)]
}

// nextBuffered extracts one frame from the buffer when a whole one is
// buffered, reporting whether the caller should try again.
func (d *StreamDecoder) nextBuffered() (payload []byte, err error, ok bool) {
	avail := d.buf[d.off:d.end]
	if len(avail) < EnvelopeHeaderSize {
		return nil, nil, false
	}
	// The length is an untrusted uint32. On 64-bit it can never be negative, so
	// the only real check is the explicit bound: growing toward a bogus 4 GiB
	// claim is how a peer turns a 5-byte header into a heap exhaustion.
	length := uint64(binary.BigEndian.Uint32(avail[1:EnvelopeHeaderSize]))
	if length > MaxFrameLength {
		return nil, fmt.Errorf("%w: %d bytes announced, limit %d", ErrFrameTooLarge, length, MaxFrameLength), true
	}
	if uint64(len(avail)) < uint64(EnvelopeHeaderSize)+length {
		return nil, nil, false
	}
	flags := avail[0]
	end := EnvelopeHeaderSize + int(length)
	// Copy: the buffer is recycled for the next read, so a slice handed back
	// to the caller would be overwritten under them.
	payload = append([]byte(nil), avail[EnvelopeHeaderSize:end]...)
	d.off += end
	if d.off == d.end {
		d.buf, d.off, d.end = d.buf[:0], 0, 0
	}
	if flags&FlagEndOfStream != 0 {
		d.done = true
		// Keep the end-of-stream payload: a Connect 200 can carry its error
		// there and nowhere else, so a reader that only checks the HTTP status
		// would discard the reason the stream ended.
		d.eos = payload
		return nil, io.EOF, true
	}
	return payload, nil, true
}

// EndOfStreamPayload returns the payload of the end-of-stream frame that ended
// the stream, or nil when the stream ended any other way (the reader hit EOF
// first, or the peer never sent one).
func (d *StreamDecoder) EndOfStreamPayload() []byte { return d.eos }
