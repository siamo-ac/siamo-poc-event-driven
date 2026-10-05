# siamo-poc-event-driven — Pub/Sub + Choreographed Saga (POC)

POC 5 of the Siamo Workflow Atlas backend-architecture series: an in-process
publish/subscribe event bus plus a multi-step saga done in **choreography**
style (no central boss), with failure injection that triggers compensating
transactions.

## The concept, in plain language

**Pub/sub.** Think of it like a company bulletin board with labeled sections
(topics): `order`, `payment`, `inventory`. Anyone can pin a note (publish an
event) to a section, and anyone who signed up (subscribed) gets a copy the
moment it lands. The sender doesn't know or care who reads it — that
decoupling is the whole point.

**Saga in choreography style.** A saga is just a long business transaction
split into steps across services — here, "take an order" means:
Order creates it → Payment charges → Inventory reserves stock → Order confirms.

There are two ways to coordinate the steps:

- **Orchestration** = a conductor with a baton. One central service tells each
  participant what to do next and watches for failures. Easy to follow, but
  the conductor becomes the bottleneck and the single thing that must
  understand everything.
- **Choreography** = dancers who each know their cue. There is no conductor.
  Payment simply reacts to `order.created`; Inventory reacts to
  `payment.processed`; Order reacts to whatever comes back. This POC uses
  choreography: the "flow" is emergent, not commanded.

**Compensating transactions.** When a later step fails, you can't just "roll
back" like a database — the credit card was already charged. Each participant
publishes an *undo* event for its own side effect: Order cancels the order,
Payment refunds the charge. The saga ends in a consistent state, but by
undoing forward instead of rolling back.

## Layout

```
cmd/eventdriven/main.go          — HTTP server entrypoint (:8095)
internal/bus/bus.go              — EventBus port (interface) + in-process adapter
internal/participants/           — Order, Payment, Inventory + failure Injector
internal/api/api.go              — HTTP handlers
```

## Run it

```bash
cd ~/workspace/services/pocs/siamo-poc-event-driven
go build ./...
go run ./cmd/eventdriven            # listens on :8095
```

## Try the three paths

Happy path:

```bash
curl -s -X POST localhost:8095/saga/start -H 'Content-Type: application/json' -d '{}' \
  | python3 -c "import json,sys; [print(e['seq'], e['type'], '<-', e['source']) for e in json.load(sys.stdin)['events']]"
```

Payment fails (inject inline):

```bash
curl -s -X POST localhost:8095/saga/start -H 'Content-Type: application/json' -d '{"fail":"payment"}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); [print(e['seq'], e['type'], '<-', e['source']) for e in d['events'] if e['saga_id']==d['saga_id']]"
```

Inventory fails (arm ahead via the inject endpoint, then start):

```bash
curl -s -X POST localhost:8095/saga/inject-failure -H 'Content-Type: application/json' -d '{"fail":"inventory"}'
curl -s -X POST localhost:8095/saga/start -H 'Content-Type: application/json' -d '{}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); [print(e['seq'], e['type'], '<-', e['source'], 'COMP' if e.get('payload',{}).get('compensation') else '') for e in d['events'] if e['saga_id']==d['saga_id']]"
```

Full trail any time:

```bash
curl -s localhost:8095/events | python3 -m json.tool
```

## What to observe

- **The compensation path in the log.** Run the inventory-failure case and read
  the trail in order: `order.created → payment.processed → inventory.failed →
  order.cancelled → payment.refunded`. Nobody told Payment to refund — it
  reacted to `order.cancelled` and undid its own charge from its own ledger.
  That's choreography doing its job, and the `compensation` payload flag marks
  the undo events.
- **The payment-failure case is shorter on purpose.** Payment failed *before*
  charging, so its ledger shows no charge and no `payment.refunded` appears —
  compensation only undoes real side effects. Order still cancels itself.
- **The inject endpoint arms the *next* saga**, then the mode is consumed. One
  armed failure = one failed saga; the run after is happy again. Watch for
  that in repeated demos.

## Honest limits (POC — not production hardening)

- **In-process bus.** Subscribers are function calls in one binary: no Kafka,
  no durability, no replay, no delivery across machines, and the event trail
  lives in RAM (restart wipes it). The `EventBus` interface in
  `internal/bus/bus.go` is the seam — a Kafka adapter would implement the same
  `Publish/Subscribe/Trail` contract and drop in without touching the
  participants. In Kafka terms: topics map 1:1, the trail becomes the log,
  and subscriptions become consumer groups.
- **Synchronous delivery.** Publish blocks until every subscriber's handler
  returns. Real buses deliver asynchronously, with retries and dead-letter
  queues; this POC has none of that.
- **Demo data only.** Amounts, SKUs, card declines are fictional constants.
- **No auth, no TLS, no rate limiting.** Localhost demo only.
