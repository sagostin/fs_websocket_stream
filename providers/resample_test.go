package providers

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func pcm16(samples ...int16) []byte {
	var buf bytes.Buffer
	for _, s := range samples {
		_ = binary.Write(&buf, binary.LittleEndian, s)
	}
	return buf.Bytes()
}

func samplesOf(b []byte) []int16 {
	out := make([]int16, len(b)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(b[2*i:]))
	}
	return out
}

func TestResamplePCM16SameRate(t *testing.T) {
	in := pcm16(1, -2, 300, -400)
	got := ResamplePCM16(in, 16000, 16000)
	if !bytes.Equal(got, in) {
		t.Fatalf("same-rate resample altered data: %v != %v", got, in)
	}
}

func TestResamplePCM16Upsample(t *testing.T) {
	// 16k -> 24k is 2 -> 3 samples; a ramp should stay monotonic.
	in := pcm16(0, 1000, 2000, 3000)
	got := samplesOf(ResamplePCM16(in, 16000, 24000))
	if len(got) != 6 {
		t.Fatalf("want 6 samples, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("ramp not monotonic at %d: %v", i, got)
		}
	}
	if got[0] != 0 || got[len(got)-1] > 3000 {
		t.Fatalf("endpoints off: %v", got)
	}
}

func TestResamplePCM16Downsample(t *testing.T) {
	// 24k -> 16k is 3 -> 2 samples.
	in := pcm16(0, 1500, 3000, 4500, 6000, 7500)
	got := samplesOf(ResamplePCM16(in, 24000, 16000))
	if len(got) != 4 {
		t.Fatalf("want 4 samples, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("ramp not monotonic at %d: %v", i, got)
		}
	}
}

func TestResamplePCM16EdgeCases(t *testing.T) {
	if got := ResamplePCM16(nil, 16000, 24000); got != nil {
		t.Fatalf("nil input: want nil, got %v", got)
	}
	if got := ResamplePCM16([]byte{0x01}, 16000, 24000); got != nil {
		t.Fatalf("odd single byte: want nil, got %v", got)
	}
	// Odd trailing byte is dropped at equal rates.
	in := pcm16(7, 8)
	odd := append(in, 0xff)
	if got := ResamplePCM16(odd, 16000, 16000); !bytes.Equal(got, in) {
		t.Fatalf("trailing odd byte not dropped: %v", got)
	}
	// DC offset survives resampling.
	dc := samplesOf(ResamplePCM16(pcm16(1000, 1000, 1000, 1000), 16000, 24000))
	for _, s := range dc {
		if s != 1000 {
			t.Fatalf("DC not preserved: %v", dc)
		}
	}
}
