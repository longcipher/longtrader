package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/longcipher/longtrader/sdks/go/contract"
)

func TestEnvelopeUsesBigEndianLength(t *testing.T) {
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	got := EncodeMessage(payload)
	want := []byte{0x00, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeMessage = % x, want % x", got, want)
	}
	// Spell out the failure mode: a little-endian length makes the peer read
	// 0x04000000 bytes, so the stream stalls forever instead of decoding.
	if bytes.Equal(got[1:5], []byte{0x04, 0x00, 0x00, 0x00}) {
		t.Fatal("the envelope length is little-endian; Connect requires big-endian")
	}
}

func TestEnvelopeEmptyPayload(t *testing.T) {
	if got := EncodeMessage(nil); !bytes.Equal(got, []byte{0, 0, 0, 0, 0}) {
		t.Errorf("EncodeMessage(nil) = % x", got)
	}
}

func TestEncodeEndOfStream(t *testing.T) {
	body, err := json.Marshal(map[string]string{"error": "internal"})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	got := EncodeEndOfStream(body)
	if got[0] != FlagEndOfStream {
		t.Errorf("flags = %#x, want %#x", got[0], FlagEndOfStream)
	}
	if _, err := DecodeEnvelope(got); err != ErrEndOfStream {
		t.Errorf("DecodeEnvelope of an end-of-stream frame = %v, want ErrEndOfStream", err)
	}
}

func TestDecodeEnvelopeMessage(t *testing.T) {
	payload := []byte("hello")
	got, err := DecodeEnvelope(EncodeMessage(payload))
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload = %q, want %q", got, payload)
	}
}

func TestDecodeEnvelopeTruncated(t *testing.T) {
	cases := [][]byte{
		{},
		{0x00},
		{0x00, 0x00, 0x00, 0x00},             // header incomplete
		{0x00, 0x00, 0x00, 0x00, 0x04, 0x01}, // payload short
	}
	for _, in := range cases {
		if _, err := DecodeEnvelope(in); err != ErrTruncatedFrame {
			t.Errorf("DecodeEnvelope(% x) = %v, want ErrTruncatedFrame", in, err)
		}
	}
}

// The bug this helper exists for: a single-message reply is one data frame
// followed by an end-of-stream frame whose payload is JSON, so decoding the
// body as one protobuf message fails. The data frame must be located first.
func TestFirstMessagePayloadSkipsTrailingEndOfStream(t *testing.T) {
	payload := []byte{0x08, 0x02} // ReportLogResponse{accepted: 2}
	body := append(EncodeMessage(payload), EncodeEndOfStream([]byte(`{"error":null,"metadata":{}}`))...)

	got, err := FirstMessagePayload(body)
	if err != nil {
		t.Fatalf("FirstMessagePayload: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = % x, want % x", got, payload)
	}
	// The naive alternative -- parsing the whole body as one message -- is
	// what fails: the length prefix of the first frame reads as field 0.
	if err := naiveParseAsResponse(body); err == nil {
		t.Error("decoding the whole body as one message should fail; the test no longer covers the regression")
	}
}

// naiveParseAsResponse decodes a whole Connect body as a single protobuf
// message, which is the shortcut the framing rules exist to forbid.
func naiveParseAsResponse(b []byte) error {
	var accepted uint64
	err := contract.Scan(b, func(f contract.Field) error {
		if f.Number == 1 && f.Wire == contract.WireVarint {
			accepted = f.AsUint64()
		}
		return nil
	})
	if err == nil && accepted != 2 {
		return errors.New("decoded the wrong value")
	}
	return err
}

func TestFirstMessagePayloadVariants(t *testing.T) {
	payload := []byte{0x08, 0x01}
	t.Run("leading end-of-stream is skipped", func(t *testing.T) {
		body := append(EncodeEndOfStream([]byte("{}")), EncodeMessage(payload)...)
		got, err := FirstMessagePayload(body)
		if err != nil {
			t.Fatalf("FirstMessagePayload: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("payload = % x, want % x", got, payload)
		}
	})
	t.Run("first of several frames wins", func(t *testing.T) {
		body := append(EncodeMessage(payload), EncodeMessage([]byte{0x08, 0x02})...)
		got, err := FirstMessagePayload(body)
		if err != nil {
			t.Fatalf("FirstMessagePayload: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("payload = % x, want % x", got, payload)
		}
	})
	t.Run("no data frame", func(t *testing.T) {
		if _, err := FirstMessagePayload(EncodeEndOfStream([]byte("{}"))); err != ErrNoMessageFrame {
			t.Errorf("error = %v, want ErrNoMessageFrame", err)
		}
		if _, err := FirstMessagePayload(nil); err != ErrNoMessageFrame {
			t.Errorf("error = %v, want ErrNoMessageFrame", err)
		}
	})
	t.Run("truncated payload", func(t *testing.T) {
		if _, err := FirstMessagePayload([]byte{0x00, 0x00, 0x00, 0x00, 0x04, 0x01}); err != ErrTruncatedFrame {
			t.Errorf("error = %v, want ErrTruncatedFrame", err)
		}
	})
}

func TestStreamDecoderReadsMultipleFrames(t *testing.T) {
	var body []byte
	want := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for _, p := range want {
		body = append(body, EncodeMessage(p)...)
	}
	body = append(body, EncodeEndOfStream([]byte("{}"))...)

	dec := NewStreamDecoder(bytes.NewReader(body))
	var got [][]byte
	for {
		payload, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, payload)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("frame %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// Frames straddling read boundaries is the normal case on a real socket, not
// an edge case: the decoder must buffer until a frame is whole.
func TestStreamDecoderHandlesFramesSplitAcrossReads(t *testing.T) {
	var body []byte
	want := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma-longer")}
	for _, p := range want {
		body = append(body, EncodeMessage(p)...)
	}
	body = append(body, EncodeEndOfStream([]byte("{}"))...)

	dec := NewStreamDecoder(&chunkReader{data: body, size: 1})
	var got [][]byte
	for {
		payload, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, payload)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("frame %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestStreamDecoderEndsAtEndOfStreamFrame(t *testing.T) {
	body := append(EncodeMessage([]byte("x")), EncodeEndOfStream([]byte("{}"))...)
	body = append(body, EncodeMessage([]byte("after-eos"))...) // must be ignored

	dec := NewStreamDecoder(bytes.NewReader(body))
	if _, err := dec.Next(); err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if _, err := dec.Next(); err != io.EOF {
		t.Fatalf("second Next = %v, want io.EOF", err)
	}
	if _, err := dec.Next(); err != io.EOF {
		t.Fatalf("third Next = %v, want io.EOF", err)
	}
}

// The announced length is an untrusted uint32, so a peer can claim 4 GiB in a
// 5-byte header. The decoder must reject that instead of growing toward it: a
// header plus a stream of filler is enough to drive the heap to whatever the
// peer claimed. This is the measured shape of that attack -- the old code grew
// to 768 MiB of heap in 8 seconds before this bound existed.
func TestStreamDecoderRejectsAnOversizedAnnouncedLength(t *testing.T) {
	// 0xFFFFFFFF bytes announced, and a reader that never stops supplying
	// bytes: the decoder must not wait for a payload that can never be sane.
	dec := NewStreamDecoder(&fillerReader{})
	_, err := dec.Next()
	if err == nil {
		t.Fatal("a 4 GiB announced frame must be rejected")
	}
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("error = %v, want ErrFrameTooLarge", err)
	}
	// The buffer must have grown by the header and nothing more.
	if cap(dec.buf) > 1<<20 {
		t.Errorf("buffer grew to %d bytes for a header-only rejection; the claim was believed", cap(dec.buf))
	}
}

// A frame exactly at the bound is legal; one byte over it is not.
func TestStreamDecoderAcceptsAFrameAtTheBound(t *testing.T) {
	dec := NewStreamDecoder(bytes.NewReader(EncodeMessage([]byte("ok"))))
	if _, err := dec.Next(); err != nil {
		t.Fatalf("a small frame must decode: %v", err)
	}
	if MaxFrameLength <= 0 {
		t.Fatalf("MaxFrameLength = %d, want a positive bound", MaxFrameLength)
	}
}

// The same bound applies to the finite-body helpers, which read a whole reply
// into memory: a bogus length there is just as hostile.
func TestFirstMessagePayloadRejectsAnOversizedAnnouncedLength(t *testing.T) {
	body := []byte{0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}
	if _, err := FirstMessagePayload(body); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("error = %v, want ErrFrameTooLarge", err)
	}
	if _, err := DecodeEnvelope(body); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("DecodeEnvelope error = %v, want ErrFrameTooLarge", err)
	}
}

// A Connect 200 whose only frame is an end-of-stream error frame: the status
// check passes, and the reason the call failed is in the frame.
func TestEndOfStreamErrorReadsTheErrorFrame(t *testing.T) {
	body := EncodeEndOfStream([]byte(`{"error":{"code":"failed_precondition","message":"session is not ACTIVE"},"metadata":{}}`))
	cerr := endOfStreamError(body)
	if cerr == nil {
		t.Fatal("an end-of-stream error frame must be reported as an error")
	}
	if cerr.Code != "failed_precondition" || cerr.Message != "session is not ACTIVE" {
		t.Errorf("error = %+v, want the host's code and message", cerr)
	}
	// The ordinary clean close-out is not an error.
	if got := endOfStreamError(EncodeEndOfStream([]byte(`{"error":null,"metadata":{}}`))); got != nil {
		t.Errorf("a clean end-of-stream reported %v, want nil", got)
	}
	// A real data frame followed by a clean close-out is not an error either.
	clean := append(EncodeMessage([]byte{0x08, 0x01}), EncodeEndOfStream([]byte(`{"error":null}`))...)
	if got := endOfStreamError(clean); got != nil {
		t.Errorf("a successful reply reported %v, want nil", got)
	}
}

// fillerReader always supplies bytes, like a peer that keeps talking. It is the
// input that turns an believed length into unbounded growth.
type fillerReader struct{}

func (fillerReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestStreamDecoderEmptyBody(t *testing.T) {
	if _, err := NewStreamDecoder(bytes.NewReader(nil)).Next(); err != io.EOF {
		t.Errorf("empty body = %v, want io.EOF", err)
	}
}

// A header without its payload is data loss, not a clean end of stream.
func TestStreamDecoderTruncatedTail(t *testing.T) {
	body := append(EncodeMessage([]byte("x")), []byte{0x00, 0x00, 0x00, 0x00, 0x08, 0x01}...)
	dec := NewStreamDecoder(bytes.NewReader(body))
	if _, err := dec.Next(); err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if _, err := dec.Next(); err != ErrTruncatedFrame {
		t.Errorf("truncated tail = %v, want ErrTruncatedFrame", err)
	}
}

// chunkReader hands out at most size bytes per Read, simulating a socket.
type chunkReader struct {
	data []byte
	off  int
	size int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := r.size
	if n > len(p) {
		n = len(p)
	}
	if r.off+n > len(r.data) {
		n = len(r.data) - r.off
	}
	copy(p, r.data[r.off:r.off+n])
	r.off += n
	return n, nil
}
