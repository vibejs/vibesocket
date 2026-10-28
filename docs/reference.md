# API reference

## Package exports

| Subpath           | Main public exports                                                                                                                                        | Runtime dependency boundary                 |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| @usevibejs/socket | createSocketClient, defineSocketContract, SocketError, errorCodes, retryDelay, connectionTransitions                                                       | Browser-safe, no UI dependency              |
| /contract         | defineSocketContract, validate, lookup, schema/contract/inference types                                                                                    | Standard Schema interfaces only             |
| /client           | createSocketClient, createSocketPlugin, useSocket, useSocketStatus, useSocketEvent, useSocketRoom, useSocketSubscription, useSocketRequest, useEventStream | Public @usevibejs/ui peer                   |
| /server           | createSocketServer; Principal, Connection, ServerTransport, HandlerContext, ServerOptions                                                                  | Runtime independent                         |
| /node             | attachNodeSocket, NodeAdapterOptions                                                                                                                       | node:http/stream types; optional ws runtime |
| /cloudflare       | createDurableSocket, CloudflareOptions, HibernationState, WorkerSocket                                                                                     | Worker globals accessed only on invocation  |
| /sse              | formatSSE, createSSEStream, createSSEChannel, createEventStream, replay/event types                                                                        | Fetch, streams, optional native EventSource |
| /adapters         | createSessionAuthenticator, syncSocketQuery, presenceSnapshot, createTypingTracker, createMemoryPubSub, connectPubSub, recoverState                        | Structural optional ecosystem integrations  |

Generated declarations are the exact signature reference. All packages are ESM, side-effect free and independently importable. The Node adapter owns no HTTP listener. The core owns no reactive runtime or database.

## Contracts

`defineSocketContract({ version?, client?, server?, requests?, bidirectional? })` freezes the contract/maps. Event maps contain Standard Schema validators. Bidirectional entries appear in both maps; explicit directional entries override the corresponding bidirectional entry. Requests contain input/output validators and optional readonly error-code arrays. A contract is distinct from @usevibejs/contract's HTTP route map.

Types: Schema, EventMap, SocketContract, RequestDefinition, Input, Output, ClientEvent, ServerEvent, RequestName, RequestError. Inputs infer producer payloads; outputs infer validated receiver payloads. Every request's result is validated. Unknown runtime event names fail even if JavaScript bypasses static TypeScript checking. Payloads must be JSON-compatible; binary application messages are not implemented.

## Client options and defaults

| Option             | Default / meaning                                              |
| ------------------ | -------------------------------------------------------------- |
| contract, url      | Required; URL may be an async factory                          |
| webSocket          | Native global WebSocket factory                                |
| connectTimeout     | 10000ms, includes async URL acquisition                        |
| reconnect          | 10 attempts; 250ms floor, 30000ms cap, 30000ms stable reset    |
| heartbeat          | 30000ms interval, 10000ms timeout; false disables              |
| maxMessageBytes    | 65536 UTF-8 bytes                                              |
| maxBufferedBytes   | 262144 outgoing bytes                                          |
| maxPendingRequests | 64 requests/commands                                           |
| maxInbox           | 256 frames awaiting client parsing/validation                  |
| queue              | 100 events, 262144 bytes, 10000ms TTL                          |
| lifecycle          | Browser pagehide/pageshow/online; hidden tabs remain connected |
| diagnostic         | Optional safe retry metadata callback; no payload logging      |

Numeric resource limits are checked for finite integer configuration. Zero is accepted for disabling count capacities where meaningful. Retry delay clamps to a minimum 50ms and caps exponential growth. No queue persistence is built in.

`send(event, payload, options?)` resolves to `{ stage, id? }`. Options: disconnected reject/queue/drop, ttl, coalesce, onFailure. `request(event, input, { timeout?, signal?, idempotencyKey? })` returns the typed response or rejects with SocketError. `requestResult` returns a discriminated `{ ok:true, data } | { ok:false, error }`; its error code is the declared request codes plus built-ins. Unknown remote codes become a safe INTERNAL_ERROR in the typed-result API.

`subscribe(room)` returns stable id, active, state and unsubscribe. `observeSubscriptions` emits pending/active/rejected/closed transitions. `evict`/authorization revocation turns an active client subscription into rejected; fresh reconnect can reauthorize it. State and event observers return cleanup functions.

## Server configuration

Required: contract and authorize. Optional validatePrincipal and diagnostic. Defaults: 10,000 connections, 8/principal, 32 rooms, 64 queued incoming frames, 100 frames/sec/connection, 64KiB messages, 256KiB buffered output, 30s handler deadline and 45s complete processing deadline (processingTimeout). Per-message receipt/heartbeat/cancel control frames count toward the frame rate. Limits are local to this instance.

Methods: accept, receive, drain, isReady, remove, close, evict, revoke, on, handle, send, sendToUser, publish, shutdown. `accept` is for trusted runtime adapters; its restore argument is for trusted hibernation metadata, not client input. `receive` validates/enqueues a frame; `drain` lets event runtimes await processing. `remove` handles transport disconnect cleanup. `close` removes then closes transport. `evict(connectionId, room, code?)` removes a room and notifies its subscriber. `revoke(principalId)` closes all that identity's indexed connections.

`on/handle` return registration cleanup. Handlers run serially per connection, with cross-connection concurrency. Cancellation and heartbeats bypass a blocked handler. Queued canceled request IDs are bounded and skipped before dispatch. The server cannot undo application side effects that already committed.

`publish(room,event,payload,{exclude?,overflow?})` returns attempted/written/rejected. `send` returns a local write boolean. `sendToUser` returns attempted/written. Payload validation and per-recipient authorization precede writes. Stats expose activeConnections, activeRooms, subscriptions, bufferedBytes, inboxDepth, received, sent, rejected and handlerErrors. `handlerErrors` counts rejected dispatches, including validation/permission errors. Do not treat it as a count of process crashes.

## Node configuration

Required server, core, allowedOrigins, authenticate. Optional path `/socket`, baseURL for building the authentication Request (default internal http://localhost), allowMissingOrigin false, handshakeTimeout 5000ms, maxPayload 65536, maxPendingUpgrades 128, maxConnectionsPerSource 128, attemptsPerMinute 120, unmatchedPath reject, allowUpgrade callback and idleTimeout (90000ms, or false). A shared sweep releases abandoned idle connections. Supply a trusted baseURL when your custom authentication checks request URL audience; never derive it from an unchecked Host header.

`attachNodeSocket` returns close and a stats getter with handshake/upgrade counters. Close is idempotent, detaches listeners and drains/terminates sockets; it does not close the caller's HTTP server. Compression is disabled. Maximum connection lifetime is one day, or principal expiration if earlier, followed by a renewed handshake.

## SSE configuration

`createSSEStream`: signal, maxBufferedBytes (256KiB), keepalive (15000ms or false), onClose. Return response/send/comment/close, closed and bufferedBytes. `formatSSE` supports event, id, retry and string data.

`createSSEChannel(schemas, { replay?, maxReplayEvents?, maxMessageBytes?, keepalive?, maxSubscribers?, maxSubscribersPerKey? })`: publish, response, close, size. Defaults: 1000 replay events/live capture count, 64KiB payloads, 1000 subscribers. Supply access.key from trusted server identity to enforce the default 8 subscribers/key. Access includes allowed callback and optional expiresAt. Channel close is terminal.

`createEventStream`: url, events, withCredentials, optional EventSource factory, maxMessageBytes 65536 and maxInbox 256. Methods connect, close, dispose, on, observe. close allows reconnect; dispose is final. It never accepts arbitrary header options.

## Error codes

| Code               | Meaning / recovery                                               |
| ------------------ | ---------------------------------------------------------------- |
| CONNECTION_LOST    | Connection unavailable; business outcome may be uncertain        |
| AUTH_REQUIRED      | Renew session through HTTP, then explicitly reconnect            |
| FORBIDDEN          | Server denied permission; do not blindly retry                   |
| INVALID_PAYLOAD    | Schema, frame size/shape or field validation failed              |
| UNSUPPORTED_EVENT  | Event/request not declared or no handler installed               |
| TIMEOUT            | Pending request deadline elapsed; do not assume rollback         |
| CANCELLED          | Caller canceled or operation was replaced/disposed               |
| RATE_LIMITED       | Local capacity/rate policy rejected work                         |
| PROTOCOL_MISMATCH  | Unsupported protocol/contract version; use compatible deployment |
| REPLAY_UNAVAILABLE | Retained history missing; request an authoritative snapshot      |
| QUEUE_OVERFLOW     | Volatile queue/backlog reached its bound                         |
| QUEUE_EXPIRED      | Queued event TTL elapsed before flushing                         |
| SERVER_UNAVAILABLE | Adapter/core/broker is shutting down or closed                   |
| INTERNAL_ERROR     | Safe generic handler failure; details remain server-side         |
| SLOW_CONSUMER      | Outgoing buffered bytes reached the configured limit             |

Applications can declare additional RPC codes. `SocketError` carries code and message; remote errors never contain stack traces. Browser upgrade HTTP failures often surface only as generic network failure, so use the HTTP session source to distinguish expired authentication from transport outages.
