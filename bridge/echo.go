package bridge

// EchoHandler sends all received uplink audio straight back to the caller.
// Useful for validating full-duplex connectivity without any AI pipeline.
type EchoHandler struct {
	BaseHandler
}

// OnAudio echoes the PCM frame back down to FreeSWITCH.
func (EchoHandler) OnAudio(s *Session, pcm []byte) {
	_ = s.SendAudio(pcm)
}
