// Package participants holds the three saga participants. Each one only talks
// to the event bus: it publishes events about its own work and reacts to
// events from the other participants. There is no orchestrator — this is
// choreography: the saga's "flow" emerges from who reacts to what.
package participants

import (
	"fmt"
	"sync"
	"time"

	"github.com/siamosystems/siamo-poc-event-driven/internal/bus"
)

// FailMode is what the failure injector asks a participant to fail.
type FailMode string

const (
	FailNone      FailMode = ""
	FailPayment   FailMode = "payment"
	FailInventory FailMode = "inventory"
)

// Injector is the failure-injection port. Set a mode for the next saga;
// participants consume (and clear) it when their step runs.
type Injector struct {
	mu   sync.Mutex
	mode FailMode
}

func NewInjector() *Injector { return &Injector{} }

func (i *Injector) Set(mode FailMode) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.mode = mode
}

// Consume reports whether the requested mode matches and clears it.
func (i *Injector) Consume(mode FailMode) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.mode == mode {
		i.mode = FailNone
		return true
	}
	return false
}

func newEvent(sagaID, typ, source string, payload map[string]any) bus.Event {
	return bus.Event{
		ID:      fmt.Sprintf("%s-%d", sagaID, time.Now().UnixNano()),
		SagaID:  sagaID,
		Type:    typ,
		Source:  source,
		Payload: payload,
	}
}

// ---------------------------------------------------------------------------
// Order — owns the order lifecycle. Starts sagas, confirms on success,
// cancels on failure (its own compensating transaction when downstream steps
// fail after it already created the order).
// ---------------------------------------------------------------------------

type Order struct {
	b *bus.InProcessBus
}

func NewOrder(b *bus.InProcessBus) *Order {
	o := &Order{b: b}
	// React to payment failures → cancel the order (compensation).
	b.Subscribe(bus.TopicPayment, func(e bus.Event) {
		if e.Type == "payment.failed" {
			o.publish(e.SagaID, "order.cancelled", map[string]any{
				"reason":       "payment failed",
				"compensation": true,
			})
		}
	})
	// React to inventory outcomes.
	b.Subscribe(bus.TopicInventory, func(e bus.Event) {
		switch e.Type {
		case "inventory.reserved":
			o.publish(e.SagaID, "order.confirmed", map[string]any{
				"status": "all steps complete",
			})
		case "inventory.failed":
			o.publish(e.SagaID, "order.cancelled", map[string]any{
				"reason":       "inventory unavailable",
				"compensation": true,
			})
		}
	})
	return o
}

// Start kicks off a saga by creating the order.
func (o *Order) Start(sagaID string, item string, qty int) {
	o.publish(sagaID, "order.created", map[string]any{
		"item": item, "qty": qty,
	})
}

func (o *Order) publish(sagaID, typ string, payload map[string]any) {
	_ = o.b.Publish(bus.TopicOrder, newEvent(sagaID, typ, "order", payload))
}

// ---------------------------------------------------------------------------
// Payment — reacts to order.created, charges (or fails), and compensates by
// refunding when it later learns the order was cancelled.
// ---------------------------------------------------------------------------

type Payment struct {
	b   *bus.InProcessBus
	inj *Injector
	mu  sync.Mutex
	// ledger tracks which sagas we actually charged, so compensation only
	// refunds real side effects (payment.failed never charged anything).
	ledger map[string]bool
}

func NewPayment(b *bus.InProcessBus, inj *Injector) *Payment {
	p := &Payment{b: b, inj: inj, ledger: map[string]bool{}}
	b.Subscribe(bus.TopicOrder, func(e bus.Event) {
		switch e.Type {
		case "order.created":
			if inj.Consume(FailPayment) {
				p.publish(e.SagaID, "payment.failed", map[string]any{
					"reason": "card declined (injected)",
				})
				return
			}
			p.mu.Lock()
			p.ledger[e.SagaID] = true
			p.mu.Unlock()
			p.publish(e.SagaID, "payment.processed", map[string]any{
				"amount_usd": 49.99,
			})
		case "order.cancelled":
			// Compensating transaction: undo the charge we already made.
			p.mu.Lock()
			didCharge := p.ledger[e.SagaID]
			p.mu.Unlock()
			if didCharge {
				p.publish(e.SagaID, "payment.refunded", map[string]any{
					"amount_usd":   49.99,
					"compensation": true,
				})
			}
		}
	})
	return p
}

func (p *Payment) publish(sagaID, typ string, payload map[string]any) {
	_ = p.b.Publish(bus.TopicPayment, newEvent(sagaID, typ, "payment", payload))
}

// ---------------------------------------------------------------------------
// Inventory — reacts to payment.processed, reserves stock (or fails).
// ---------------------------------------------------------------------------

type Inventory struct {
	b   *bus.InProcessBus
	inj *Injector
}

func NewInventory(b *bus.InProcessBus, inj *Injector) *Inventory {
	inv := &Inventory{b: b, inj: inj}
	b.Subscribe(bus.TopicPayment, func(e bus.Event) {
		if e.Type != "payment.processed" {
			return
		}
		if inj.Consume(FailInventory) {
			inv.publish(e.SagaID, "inventory.failed", map[string]any{
				"reason": "out of stock (injected)",
			})
			return
		}
		inv.publish(e.SagaID, "inventory.reserved", map[string]any{
			"sku": "WIDGET-42", "qty": 1,
		})
	})
	return inv
}

func (inv *Inventory) publish(sagaID, typ string, payload map[string]any) {
	_ = inv.b.Publish(bus.TopicInventory, newEvent(sagaID, typ, "inventory", payload))
}
