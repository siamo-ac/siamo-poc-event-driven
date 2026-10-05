// Package bus provides the event-bus port (interface) plus the in-process
// adapter used by this POC. A Kafka/NATS adapter would implement the same
// EventBus interface — see README for where the seam is.
package bus

import (
	"sync"
	"time"
)

// Event is the canonical envelope every participant publishes.
type Event struct {
	Seq     int64          `json:"seq"`      // global ordering of the trail
	ID      string         `json:"id"`       // per-event id
	SagaID  string         `json:"saga_id"`  // correlation id across participants
	Topic   string         `json:"topic"`
	Type    string         `json:"type"`
	Source  string         `json:"source"`   // participant that published it
	At      time.Time      `json:"at"`
	Payload map[string]any `json:"payload,omitempty"`
}

// Handler is a subscriber callback.
type Handler func(e Event)

// EventBus is the port. In-process, Kafka, or NATS — all behind this.
type EventBus interface {
	// Publish puts e on topic and appends it to the trail.
	Publish(topic string, e Event) error
	// Subscribe registers h for a topic; the returned func unsubscribes.
	Subscribe(topic string, h Handler) (unsubscribe func())
	// Trail returns the full append-only event trail (oldest first).
	Trail() []Event
}

// Topics used by the saga participants.
const (
	TopicOrder     = "order"
	TopicPayment   = "payment"
	TopicInventory = "inventory"
)

// InProcessBus is a synchronous, in-memory EventBus. POC only: no durability,
// no replay, no cross-process delivery.
type InProcessBus struct {
	mu     sync.RWMutex
	subs   map[string][]Handler
	trail  []Event
	seq    int64
	closed bool
}

func NewInProcessBus() *InProcessBus {
	return &InProcessBus{subs: map[string][]Handler{}}
}

func (b *InProcessBus) Publish(topic string, e Event) error {
	b.mu.Lock()
	b.seq++
	e.Seq = b.seq
	e.Topic = topic
	e.At = time.Now().UTC()
	b.trail = append(b.trail, e)
	handlers := append([]Handler(nil), b.subs[topic]...)
	b.mu.Unlock()

	// Deliver synchronously to this topic's subscribers (not to self-loop
	// any concern here: participants only subscribe to *other* topics).
	for _, h := range handlers {
		h(e)
	}
	return nil
}

func (b *InProcessBus) Subscribe(topic string, h Handler) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[topic] = append(b.subs[topic], h)
	idx := len(b.subs[topic]) - 1
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.subs[topic] = append(b.subs[topic][:idx], b.subs[topic][idx+1:]...)
	}
}

func (b *InProcessBus) Trail() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return append([]Event(nil), b.trail...)
}
