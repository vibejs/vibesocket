# Engineering report

VibeSocket 0.1.0, verified locally on 2026-10-28. The repository contains a functioning library, real Vibe.js server integration, four runnable showcases, documentation and reproducible verification tools. No publication, push, remote repository creation, deployment or production-service access occurred.

## 1. Final architecture

Transport primitives and the headless client live in the browser-safe root export. Standard Schema contracts and strict versioned JSON envelopes form the core. The /client adapter uses public Vibe.js reactivity and app/scope lifecycle APIs. The runtime-independent /server owns connection, principal and room indexes. /node handles real Node upgrades; /sse handles Fetch streams/native EventSource; /cloudflare owns hibernating Durable Object connections. Optional structural ecosystem/persistence/pubsub helpers live in /adapters. There are no imports of private Vibe.js server internals or UI internal APIs.

The actual Vibe.js source was inspected before implementation. It exposes app.fetch, middleware, app.get and lifecycle hooks but no public Node listener/upgrade handle. No Vibe.js core source extension was necessary: an externally owned Node HTTP listener routes normal traffic through srvx to app.fetch and upgrades through ws. The original two-browser vertical slice was verified before reliability/scaling features.

## 2. Dependencies and versions

| Purpose                                   | Verified version                                                         |
| ----------------------------------------- | ------------------------------------------------------------------------ |
| Type interfaces                           | @standard-schema/spec 1.1.0                                              |
| Optional Node transport                   | ws 8.22.0                                                                |
| Vibe.js UI/server/compiler/language-tools | 0.1.0                                                                    |
| Declarations / SFC content mapper         | TypeScript 7.0.2 / 7.1.0-dev.20261015.1                                  |
| Build and HTTP bridge                     | tsdown 0.23.3, Vite 8.3.4, srvx 1.0.8                                    |
| Verification                              | Vitest 5.0.5, Playwright 1.64.1, oxlint 1.90.0, oxfmt 0.75.0             |
| Local Worker runtime                      | Miniflare 4.20260730.0, workerd 1.20260730.1                             |
| Demo schemas/auth/cache                   | Valibot 1.5.0, VibeAuth 0.1.1, Better Auth 1.7.13, VibeQuery 0.1.1       |
| Unpublished demo snapshots                | VibeDND, VibeVirtual, VibeI18n 0.1.0; MIT archives and hashes in vendor/ |

The core has no mandatory validation implementation, database, auth/cache/UI component dependency or second reactive runtime. @usevibejs/ui is an optional peer used by /client. ws is an optional dependency used by /node. All dependency versions are pinned in the manifest/lockfile.

## 3. Public APIs

Eight exports: root, /client, /contract, /server, /node, /cloudflare, /sse and /adapters. The root supplies createSocketClient, SocketError and contract/state helpers. The reactive adapter supplies createSocketPlugin, useSocket, useSocketStatus, useSocketEvent, useSocketRoom/useSocketSubscription, useSocketRequest and useEventStream. The server supplies typed on/handle, send/sendToUser/publish, connection acceptance/dispatch/drain, room eviction, revocation and shutdown. SSE supplies frame formatting, bounded streams, typed channels and EventSource clients. The full signatures/configuration are in reference.md and generated declarations.

## 4. Typed contracts

Separate client/server/bidirectional event maps, typed request input/output/error definitions and numeric contract version. Standard Schema validators run at producer/receiver boundaries. Event names and payload direction are inferred; negative compile cases are checked in both repository and packed consumers. Producers serialize validated original input and receivers expose validated output, allowing schema transformations without double-transforming producer input. requestResult exposes a discriminated result with declared plus built-in error codes.

## 5. Connection state machine

Idle, connecting, connected, reconnecting, disconnected, failed and closed. An exported transition table is enforced by the client. Intentional disconnect disables retries; dispose is final. Generation checks invalidate late URL factories and old socket callbacks. Native objects remain outside deep reactive proxies.

## 6. Reconnection and queues

Default finite retry budget: 10 attempts, exponential jitter above a 250ms floor up to 30s, reset after 30s stable connectivity. Known auth/permission/protocol/oversize/rate/slow-consumer closes are terminal. Page lifecycle and online hints avoid duplicate sockets; hidden tabs stay connected unless explicitly paused. Outbound events default to rejection when disconnected. Explicit queue/drop policies are supported; the volatile queue is bounded at 100 messages/256KiB with a 10s TTL, cancellation, replacement/coalescing and lazy expiration callbacks. RPC is never automatically replayed.

## 7. Node integration

Real ws noServer/handleUpgrade on a caller-owned listener. Exact Origin checking, session authentication, abortable handshake deadline, source/principal/total caps, bounded attempt index, initial hello timeout, ws payload limits, no compression and passive idle cleanup. The client sends application heartbeats; Node does not add a competing ping timer. Node stats expose observed/rejected upgrade/auth/origin/rate counters. Adapter close leaves ordinary HTTP working, detaches ownership and drains/terminates sockets within a bound.

## 8. SSE integration

UTF-8 Response plus byte-bounded ReadableStream, multiline data, named events, IDs, retry hints, comments, request abort and cancellation. Real HTTP streaming was verified through Vibe.js's response pipeline. EventSource owns reconnect; arbitrary header/POST support is intentionally absent. Authenticated typed channels cap total/keyed subscribers and reauthorize private delivery. The demo uses SQLite retained IDs and validates cursors, gaps and replay.

## 9. Rooms and subscriptions

Indexed authorized rooms, shared client ownership with stable IDs, reference-counted leave, reactive room replacement and scope cleanup. Reconnect reauthorizes/rejoins once. Server events carry subscription IDs; retired/rejected subscriptions cannot update room-filtered state. Eviction/permission rejection explicitly updates subscription state. Registry records principal, membership, transport, activity timestamps, abort lifecycle and close metadata without serializing transport-owned resources.

## 10. Authentication and authorization

Real VibeAuth middleware and Better Auth SQLite sessions. The public session source bypasses cookie cache/refresh for verification; identity/expiry are server bound. Trusted membership/role lookups guard sensitive operations and outbound recipients. Cookies are never treated as authorization by themselves. Session revocation blocks subsequent private delivery and stale-cookie reconnect. The loopback demo has known development accounts and explicitly relaxed local sign-in frequency; it is not an internet-ready authentication configuration.

## 11. Delivery and recovery

Ordinary events are best effort. Written, queued, protocol-received, handler-processed and persisted are distinct stages. Request completion means validated handler completion; durability depends on application commits. The chat commits before publication, then recovers by history; Kanban compares revisions and reloads conflicts. SSE can replay configured retained history and explicitly signal gaps. recoverState supplies an explicit replay/snapshot extension point. No exactly-once effects, automatic mutation replay or default end-to-end at-least-once delivery are claimed.

## 12. Backpressure and resource protection

Bounded client queue/inbox/pending RPC, server inbox/rate/room/connection caps, outgoing buffered bytes, explicit ephemeral drop versus disconnect, bounded SSE byte streams/live replay capture and Worker credit receipts. Async handlers have a 30s deadline and total validation/authorization processing has a 45s watchdog. Cleanup clears their timers on disconnect. An uncooperative external promise cannot be forcibly undone; handlers must honor signals and transactional/idempotency rules.

## 13. Cloudflare support

Actual Worker WebSocketPair handshakes and Durable Object routing tested in workerd. Uses acceptWebSocket, getWebSockets, versioned serialized attachments, projected non-secret identity and automatic constant heartbeat responses. No perpetual timers defeat hibernation. Outstanding credit is bounded to 32 frames/256KiB; receipts mean protocol processing only. Local tests reconstruct the adapter over live attachments and continue authorized broadcasts. Production eviction timing, rolling deployment behavior and actual hibernation still require a separately authorized deployment test. Stateless Worker-only room registries are not advertised.

## 14. Ecosystem results

VibeAuth real sessions/roles/revocation; VibeQuery-owned scoped cache/history reconciliation and reconnect invalidation; VibeDND drag-end operations with authoritative ordering; VibeVirtual viewport rendering over a 500-event retained live feed; VibeI18n English/Turkish connection/room/presence/typing labels and localized timestamps; Vibecn MIT source Button/Badge compositions and available primitives. No ecosystem runtime dependency enters the headless core.

## 15. Showcases and documentation

Chat: two rooms, persisted/acknowledged messages, bounded history, unread counts, multi-tab/user presence, throttled expiring typing, draft retention and reconnect recovery. Kanban: actual drag/drop and keyboard move controls, empty-column drop targets, optimistic moves, trusted tenant/role policy, transactional revision conflicts and snapshot reconciliation. Dashboard: deterministic SSE metrics, chart, live notification threshold, event IDs, pause/resume, bounded retention and virtual rows. Streaming: named simulated token/done events, inspector and cancellation. All are real compiled .vibe components sharing an explicit local backend.

The responsive Vibe.js site includes light/dark themes, searchable documentation, rendered tables/code, copy controls, transport guidance, live workbench, connection state and bounded event/retry inspectors. It was built locally and not deployed.

## 16. Unit and integration results

pnpm verify exited 0. Vitest: **55 tests passed across 6 files**. Coverage includes contracts/inference, protocol framing/limits, queues, jitter, deterministic clocks, scope disposal, callbacks, request timeout/cancel/correlation, real Node handshake/room/rejoin/error/oversize/version/idle/shutdown behavior, real HTTP SSE chunks/abort/replay/gaps/subscribers, real auth/database tests and the real local Worker runtime. No 100% coverage percentage or independent audit is claimed.

## 17. Browser verification

Playwright: **27 passed (9 Chromium, 9 Firefox, 9 WebKit)**. Independent contexts verify no-refresh chat delivery and persistence across reload, reconnect/rejoin/logout, shared Kanban changes and actual pointer drag, SSE metrics/pause/token cancellation/completion, tenant denial, localization/theme/search, multiple independent Vibe apps, scope release, pagehide/pageshow signals, offline retry and narrow-view layout. The pointer-drag case also passed three consecutive repetitions in each browser (9 additional targeted runs), awaiting observable drag/drop state instead of arbitrary sleeps. Browser runs use fresh isolated SQLite/session stores and remove their temporary directories on graceful shutdown. The page-transition signal fixture does not prove every browser's production bfcache eviction behavior or physical mobile sleep/wake behavior.

## 18. Security and package verification

Missing/expired/forged identity, hostile Origin, cross-tenant room/history access, role denial, actual session signOut revocation, conflicting board operations, flooded rooms/messages, malformed/deep/oversized envelopes, invalid contract/protocol, replay gaps, stuck auth/schema callbacks and slow consumers have executable tests. See security.md for the threat-to-test mapping.

The npm tarball was installed into a fresh temporary consumer. All eight imports and strict TypeScript declarations passed, wrong-direction events remained compile errors, and an actual native-client WebSocket RPC succeeded from the installed package. Browser bundles exclude Node/ws/Cloudflare/server SSE code; tree shaking removes unused transports. The measured unminified browser-plus-reactive import bundle was 50,063 bytes; the retry-only tree-shaking fixture was 733 bytes. These are fixture measurements, not marketing size claims.

## 19. Performance measurements

Recorded 2026-10-28T07:07:35.798Z, Node v24.22.1, Apple M4, 10 cores, 24GiB, darwin 25.6.0 arm64. Client/server/generator share one process, loopback, no TLS, no database, compression off, descriptor limit 16384. Raw data: reports/benchmark.json.

| Connections | Connected | RPC completed | Duration ms | RPC/sec | Latency p50 / p95 / p99 ms  | Peak process RSS MiB | Room broadcast client p95 ms |
| ----------- | --------- | ------------- | ----------- | ------- | --------------------------- | -------------------- | ---------------------------- |
| 100         | 100       | 1000          | 22.38       | 44675   | 18.682 / 21.343 / 21.608    | 122.67               | 1.03                         |
| 1000        | 1000      | 10000         | 1084.22     | 9223    | 23.315 / 33.078 / 35.775    | 190.05               | 15.469                       |
| 5000        | 5000      | 10000         | 148.85      | 67183   | 131.494 / 133.046 / 133.084 | 310.22               | 35.481                       |

The 1000-client case offered 10,000 requests/sec and measured 9223 completions/sec. The 100/5000 cases are bursts, not sustained-throughput guarantees. Every scenario completed 1000 join/leave churn commands and reported zero RPC errors. CPU user/system and initial RSS are recorded per scenario in the raw JSON. Buffered-byte sampling was 10ms and is not proof of zero instantaneous OS buffering.

A 100-client reconnect storm completed in 205.27ms with 100 attempts; scheduled delays ranged 101.17–199.74ms. A real paused WebSocket stream hit the 256KiB outgoing limit after 135 8KiB frames and was disconnected. Active registry entries before final shutdown: 0. There is no general server capacity/connection-count ceiling implied by this local run.

## 20. Known limitations

No Redis backend implementation, transactional outbox, durable idempotency ledger, CRDT, binary wire format, fetch SSE parser, automatic WebSocket/SSE fallback, automatic safe-mutation inference or production Cloudflare hibernation verification. PubSub's default backend is one process; broker deduplication is bounded/restart-local. Durable Object attachment metadata is capped below the platform limit and can constrain large room sets. Native EventSource cannot set custom headers. Idle/background timer policies may need deployment tuning. Queued-event expiry callbacks are lazy.

The chat history response is limited to 200 messages and 48KiB; older-history pagination is not included. The virtualized dashboard retains 500 samples, not an unbounded 100,000-item live array. Presence is approximate and typing expires. Application database operations that already committed cannot be rolled back by transport cancellation. TLS, trusted proxies, distributed limits/revocation, persistent outbox/idempotency and operational soak testing remain application/deployment responsibilities. The local auth fixture shares a loopback rate-limit bucket; configure trusted source forwarding for production.

## 21. Local commits

| Commit  | Verified milestone                                                                                      |
| ------- | ------------------------------------------------------------------------------------------------------- |
| 34dc53f | First two-browser typed WebSocket vertical slice                                                        |
| 7f7193c | Resilient core, runtime adapters, security tests, ecosystem showcases and measured load scenarios       |
| 3795105 | Worker frame-byte limit includes receipt framing; real workerd boundary regression                      |
| 5ee1fc7 | Deterministic pointer collaboration, isolated browser databases and explicitly owned graceful bootstrap |

The report-only delivery commit is identified in the final response and local git log. The benchmark JSON was recorded against the implementation represented by 7f7193c; subsequent changes cover Worker overflow and showcase/test lifecycle. All changes are local and carry no attribution trailers. No remote is configured.
