# Client and reactivity

## Creating clients

`createSocketClient({ contract, url })` creates an idle client. `url` may be an asynchronous factory, evaluated on each connection attempt (useful for fresh short-lived connection tickets). It must not contain reusable session secrets. `webSocket` can supply a compatible implementation for non-browser clients/tests. `rawSocket` exposes the current socket without deep proxying; manually calling close is treated as a transport close, so use `disconnect()` for intentional shutdown.

Operations: `connect`, `disconnect`, `reconnect`, `dispose`, `send`, `request`, `on`, `observe`, `subscribe`, `observeSubscriptions`, `cancelQueued`, `clearQueue`. `on` and observers return unsubscribe callbacks. Multiple listeners are isolated: a throwing callback does not stop other listeners. Async callbacks own their rejected promises; prefer catching errors in application callbacks.

## Connection state

The deterministic lifecycle is idle → connecting → connected. Retryable loss enters reconnecting; exhaustion or terminal close enters failed. Intentional disconnect enters disconnected. Dispose enters closed and cannot reopen. Manual reconnect passes through disconnected, resets attempts and starts connecting. No independent connected boolean is maintained by the transport.

`state` exposes status, attempts, last error, latency (application heartbeat round trip), transport, pending request count, queueDepth and queuedBytes. Read it directly in headless code or use `useSocketStatus` for reactive state/status/connected. Derive connecting/reconnecting booleans from status. A `connected` transport can still have pending or rejected room subscriptions.

Generation tokens invalidate in-flight URL factories, message validation and callbacks from retired sockets. Retry timers and heartbeat timers are cleared on intentional disconnect/dispose. Pending RPC rejects on connection loss and is never automatically replayed.

## Events and requests

```ts
const off = socket.on('changed', (payload, metadata) => {
  // metadata.room is present for room broadcasts.
});
const answer = await socket.request('echo', 'hello', {
  timeout: 5000,
  signal: abortController.signal,
  idempotencyKey: 'application-operation-id',
});
off();
```

Each direction is inferred from its contract map. Producer validation rejects bad local payloads; the receiver validates again. Standard Schema transformations run at the receiver; the original validated input is serialized. Prefer JSON-native wire schemas without transforms when returning payloads across different processes.

Requests have random correlation IDs, a default 5s timeout, bounded pending maps and optional AbortSignal cancellation. The server receives a cooperative cancellation signal. A timeout or connection loss leaves the business outcome uncertain. Late and duplicate responses have no pending correlation and are ignored. Cancellation cannot undo a committed database operation. Idempotency keys are passed to handlers; applications must implement durable, principal-scoped deduplication.

## Reconnection

The default policy allows 10 attempts, a 250ms floor and 30s cap. Exponential capped delays use jitter between the floor and the cap for the current attempt. Backoff resets after a connection has remained stable for 30s. `reconnect: false` disables retries; `shouldRetry(code)` can further restrict retryable codes. Authentication, permission, malformed/protocol errors, oversized frames and rate-limit closes are terminal. Browser handshake failures expose only a generic close, so after the finite retry budget the application should revalidate its HTTP session and explicitly reconnect after renewal.

```ts
createSocketClient({
  contract,
  url,
  reconnect: { minDelay: 500, maxDelay: 20000, maxAttempts: 8, stableAfter: 60000 },
  heartbeat: { interval: 30000, timeout: 10000 },
});
```

`online` is a hint and reschedules retry with jitter; it does not assert reachability. Hidden tabs remain connected by default. Optional `lifecycle: { pauseWhenHidden: true }` pauses connections on hide. pagehide suspends an active client, pageshow reconnects it, including bfcache restoration. `lifecycle: false` disables browser listeners. A disposed app never reopens. Mobile sleep/wake is detected by heartbeats and close events; background timer throttling can delay detection.

## Explicit volatile queues

Default disconnected sends reject. Use `{ disconnected: 'drop' }` for typing and cursor hints, or `{ disconnected: 'queue', ttl: 3000 }` for explicitly selected recoverable events. Queue defaults: 100 events, 256KiB total and 10s TTL. Both count and bytes are bounded. Expiration is evaluated on queue operations/flush; failure callbacks are lazy when no further queue operation occurs. Queues live only in memory and are cleared by intentional disconnect/dispose.

```ts
const accepted = await socket.send(
  'typing',
  { room: 'project:123', active: true },
  {
    disconnected: 'drop',
  },
);
// accepted.stage: written, queued or dropped. None means persisted.
```

Queued sends return a cancellation ID. Optional `coalesce` replaces earlier queued state with the same key. Flush follows queue order after rejoining rooms; it stops if the transport buffer limit prevents a write. There is no infinite background drain loop; retry a blocked flush by reconnecting or explicitly manage producer load. Never use this queue as the durable store for important business commands.

## Rooms and subscriptions

`subscribe(room)` returns `{ id, active, state, unsubscribe }`. IDs are stable for the lifetime of shared local ownership. Multiple owners of one room share one server subscription with reference counting. Final unsubscribe sends leave; reconnect rejoins desired rooms once with fresh authorization. States are pending/active/rejected/closed and include the last rejection. `observeSubscriptions` exposes changes.

`useSocketRoom(client, refOrGetter)` returns reactive active/state and owns cleanup. Old ownership is removed synchronously before new reactive room parameters become authoritative. Events contain the server's acknowledged subscription ID; stale validation completions for retired subscriptions are discarded. `useSocketSubscription` is an exact alias because rooms are the subscription unit, not a second overlapping mechanism.

## Composables

- `createSocketPlugin(client, { autoConnect })`, `useSocket(contract)`; plugin also offers its bound `useSocket()`.
- `useSocketStatus(client)`: reactive state, status and connected.
- `useSocketEvent(client, event, callback?, { room? })`: only that event's latest validated payload; optional reactive room filtering clears stale room data.
- `useSocketRoom` / `useSocketSubscription`: reactive parameters and reference-counted cleanup.
- `useSocketRequest(client, event)`: execute, cancel, pending, data, error; a new execute cancels its predecessor.
- `useEventStream(options)`: owns EventSource, state and disposal.

No global connection singleton exists. App injection is isolated. Native socket objects stay outside refs; network parsing is independent of UI rendering. Scope disposal removes listeners/subscriptions; app unmount disposes the plugin client. For composables used in `app.runWithContext` without a scope, manage cleanup yourself through the headless APIs.

An installed plugin begins connecting before app.mount unless autoConnect is false. If an app is never mounted, dispose its client explicitly: Vibe.js only runs app unmount callbacks for mounted applications.
