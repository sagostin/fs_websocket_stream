// Package bridge implements the server side of the mod_ws_bridge wire
// protocol: a WebSocket audio bridge between FreeSWITCH and an AI pipeline.
//
// Wire protocol (per connection, one call):
//
//	FreeSWITCH module -> bridge:
//	  - optional JSON text frame first (call metadata, any shape)
//	  - binary frames: raw L16 PCM (16-bit signed LE) at the negotiated
//	    sample rate, typically 20ms chunks
//	bridge -> FreeSWITCH module:
//	  - binary frames: raw L16 PCM to play to the caller (downlink)
//	  - JSON text frames: ControlMessage (clear/mark/error)
//
// The sample rate and mix type are conveyed as query parameters on the
// WebSocket URL (e.g. ws://host:8080/stream?rate=16000&mix=mono) since the
// module passes the URL through verbatim.
package bridge

import "encoding/json"

// Message types used in ControlMessage.Type.
const (
	// ControlClear instructs the module to immediately flush its playback
	// buffer (barge-in). Any queued downlink audio is discarded.
	ControlClear = "clear"
	// ControlMark is an application-level marker echoed back as an event.
	ControlMark = "mark"
	// ControlError reports an error to the module (surfaced as a
	// mod_ws_bridge::error event).
	ControlError = "error"
)

// ControlMessage is a JSON text frame sent from the bridge to the module.
type ControlMessage struct {
	Type    string `json:"type"`
	Name    string `json:"name,omitempty"`
	Message string `json:"message,omitempty"`
}

// Marshal serializes the control message to a JSON text frame payload.
func (m ControlMessage) Marshal() []byte {
	b, _ := json.Marshal(m)
	return b
}
