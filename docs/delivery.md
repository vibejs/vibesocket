# Delivery, RPC and replay

## What success means

| Stage             | Meaning                                                                 |
| ----------------- | ----------------------------------------------------------------------- |
| queued            | Accepted into this client's bounded, volatile memory                    |
| written           | Accepted by the local WebSocket implementation                          |
| transport receipt | A Cloudflare credit frame was accepted by the client protocol processor |
| RPC result        | The handler completed and its response passed validation                |
| persisted         | Only the application/database can establish this                        |
| replayed          | The configured history source returned a retained event                 |

Cloudflare receipts control outbound credit; they do not assert a business effect, database persistence or user visibility. Native Node sends do not wait for those receipts. An acknowledgment is never automatically a durability guarantee.

## Baseline guarantees

Ordinary events are best effort and are not retransmitted after an uncertain write. A delivered connection's frames dispatch serially after validation, in WebSocket wire order. Separate connections, concurrent publishers and multiple server processes have no global ordering guarantee. Async schema validation can affect producer ordering; await ordered application operations or use an application sequence. Consumers must compare business revisions when applicable.

RPC uses correlation IDs, bounded pending maps, cancellation and timeouts. It is separate from ordinary broadcasts. IDs correlate a response; they do not create persistence or deduplication. The server rejects an actively pending duplicate, but completed IDs are not retained as a durable deduplication log. A duplicate operation after a reconnect can execute again unless your application checks an idempotency key.

No exactly-once processing is claimed. No default at-least-once delivery is claimed. Browser reconnection, room rejoin and EventSource Last-Event-ID alone cannot recover missing business data. The SSE replay facility supplies retained history when configured, but it does not acknowledge committed client application effects or persist client cursors; end-to-end at-least-once effects still require application cursor persistence and idempotency.

## Safe recovery

1. Renew authentication through HTTP if needed.
2. Reconnect and rejoin rooms, awaiting active subscription state.
3. Fetch changes after the last applied application cursor.
4. Validate sequence/revision, ignore duplicates within your explicit retention boundary.
5. When history is unavailable, reload an authoritative snapshot.
6. Merge live events captured during recovery without overwriting newer revisions.

`recoverState({ cursor, replay, snapshot, apply })` implements the explicit replay-or-snapshot branch and returns the new cursor. It does not own storage, subscribe to events, persist the cursor or infer ordering. Keep live capture and application-specific merge semantics around it. The chat example merges history with concurrent live arrivals by persisted message IDs; Kanban compares revisions and refreshes on conflict.

## Idempotency and transactions

`request(..., { idempotencyKey })` forwards the key as `context.idempotencyKey`. Scope a durable key to the trusted principal, operation name and meaningful payload. In the same transaction, check/insert the key, apply the business effect and store its result. Return the stored result for repeated identical operations. Reject mismatched payload reuse. Keys, not connection IDs, define your application retry boundary.

For durable announcements, commit the database transaction before acknowledging. A database commit followed by an in-memory publish can still lose the publication if the process crashes between them. Use a transactional outbox and an independently retrying publisher when that gap matters. Subscribers must tolerate duplicated outbox delivery. VibeSocket supplies the transport and schema boundary, not distributed atomicity or an ORM.

The development chat commits SQLite before publish/result. It intentionally does not implement an outbox or durable idempotency ledger and must recover missed publications by loading history. Important mutation timeouts are surfaced to the user rather than automatically replayed.

## Protocol evolution

Every JSON text frame has v:1. Both sides exchange a hello with the contract's numeric version before application dispatch. Unsupported protocol/contract versions close with PROTOCOL_MISMATCH. Version negotiation is an exact supported-version check, not a guess. Unknown event names are rejected and unknown envelope fields fail closed. One consistent JSON framing strategy is used in Node and Cloudflare; no Socket.IO/Engine.IO protocol is mixed in.

Add optional schema fields only when old peers tolerate them. Keep wire event identifiers stable and locale-independent. Rename/remove events or introduce required fields under a new contract version. A rolling breaking deployment should route compatible clients to the corresponding handler version or ship parallel endpoints; this package does not translate arbitrary old contracts. Backward-compatible changes can retain the existing version and do not force an application reload.
