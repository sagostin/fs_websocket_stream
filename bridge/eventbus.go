package bridge

import "sync"

// Event is a bridge-domain event published on the EventBus. Name is dotted
// (e.g. "audio.start", "audio.end", "transcript", "call.answer"); UUID is
// the FreeSWITCH call UUID when known.
type Event struct {
	Name string         `json:"event"`
	UUID string         `json:"uuid,omitempty"`
	Data map[string]any `json:"data,omitempty"`
}

// EventBus is a fan-out publisher. Subscribers with full channels miss
// events (events are telemetry, not state).
type EventBus struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

// NewEventBus creates an empty bus.
func NewEventBus() *EventBus {
	return &EventBus{subs: make(map[chan Event]struct{})}
}

// Subscribe returns a channel of events and an unsubscribe function.
func (b *EventBus) Subscribe(buf int) (<-chan Event, func()) {
	ch := make(chan Event, buf)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, ch)
		close(ch)
		b.mu.Unlock()
	}
}

// Publish fan-outs an event to all subscribers without blocking.
func (b *EventBus) Publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- e:
		default:
		}
	}
}
