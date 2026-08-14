package providers

// ResamplePCM16 converts mono signed 16-bit little-endian PCM between sample
// rates using linear interpolation. It is stateless; stream callers should
// resample in blocks aligned to ResampleBlockBytes and carry the remainder
// to avoid boundary artifacts. A trailing odd byte is ignored.
func ResamplePCM16(in []byte, fromRate, toRate int) []byte {
	if len(in) < 2 {
		return nil
	}
	if fromRate <= 0 || toRate <= 0 || fromRate == toRate {
		out := make([]byte, len(in)-len(in)%2)
		copy(out, in)
		return out
	}

	n := len(in) / 2
	samples := make([]int16, n)
	for i := range samples {
		samples[i] = int16(in[2*i]) | int16(in[2*i+1])<<8
	}

	outN := n * toRate / fromRate
	if outN == 0 {
		return nil
	}
	out := make([]byte, outN*2)
	step := float64(fromRate) / float64(toRate)
	for i := 0; i < outN; i++ {
		pos := float64(i) * step
		j := int(pos)
		frac := pos - float64(j)
		a := samples[j]
		b := a
		if j+1 < n {
			b = samples[j+1]
		}
		v := int16(float64(a) + frac*float64(int32(b)-int32(a)))
		out[2*i] = byte(v)
		out[2*i+1] = byte(v >> 8)
	}
	return out
}

// ResampleBlockBytes returns the input block size (in bytes) that resamples
// exactly between the two rates: a whole number of input samples producing
// a whole number of output samples. Stream callers should resample in
// multiples of this size and carry any remainder into the next block.
func ResampleBlockBytes(fromRate, toRate int) int {
	if fromRate <= 0 || toRate <= 0 {
		return 2
	}
	g := gcd(fromRate, toRate)
	return 2 * fromRate / g
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
