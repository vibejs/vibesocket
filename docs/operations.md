# Operations and troubleshooting

## Observability

The headless client exposes status, attempts, heartbeat round trip, pending work and bounded queue depth/bytes. Its diagnostic callback reports retry attempt, delay and close code. Server stats expose active connections/rooms/subscriptions, buffered bytes, inbox depth, received/sent frames and rejections/handler errors. Server diagnostics report connection counts and handler duration/error categories. Diagnostics are opt-in and callback failures are isolated.

Integrate those low-cardinality fields with your logger or metrics collector. Do not label metrics with raw user IDs, room IDs, event content, tokens or query strings. No payload logging or verbose production debugging is enabled by default. Node adapter stats record handshake attempts/failures, Origin/authentication/rate rejection counts and active upgrades; collect external edge rate-limit decisions separately; rejected HTTP upgrades do not create core connections and therefore are not active-connection counts.

The workbench's connection inspector displays transport state, pending requests, queue depth, retry count and latency. Its bounded diagnostics view lists retry scheduling. The live event feed and streaming frame view expose real incoming events. Producer metrics are deterministic simulated values, not production telemetry.

## Performance measurements

`pnpm bench` only targets an ephemeral loopback listener. It raises the process file-descriptor limit to 16384 and runs 100, 1000 and 5000 connections; timestamp RPC, room fanout, 1000 join/leave churn commands per scenario, a 100-client reconnect storm and an actual paused stream slow consumer. See reports/benchmark.json for raw results and the engineering report for the measured values.

Client and server share one Node process on the same machine. There is no TLS, database, remote network or UI rendering in the transport benchmark. CPU and RSS include both endpoints and the generator; they cannot be interpreted as isolated server capacity. The 1000-connection case offers 10,000 requests/sec, while completion throughput and latency are measured. Burst cases are not sustained capacity claims. maxObservedBufferedBytes is a 10ms sample, not proof of zero instantaneous buffering. A small bounded local benchmark is not a production soak test.

Use your own deployment's authentication, payload distribution, fanout shape, network/TLS, replay retention and persistence latency for capacity planning. No universal connection-count ceiling is advertised.

## Troubleshooting

- HTTP works but WebSocket fails: confirm that the externally owned Node HTTP server has attachNodeSocket. Ordinary Fetch middleware and app.listen do not expose its upgrade stream.
- Upgrade denied: verify exact Origin, supported vibesocket.v1 subprotocol, valid cookie session and capacity policy. CORS configuration does not replace the Origin allowlist.
- Browser retries then fails: native WebSocket hides the handshake's HTTP reason. Revalidate the HTTP session, inspect trusted server diagnostics and reconnect after renewal.
- Connected but no room events: await active subscription state and inspect rejection. Check both join and receive policies, tenant membership and current contract version.
- HTTP middleware endpoint bypasses guards: authenticate that middleware explicitly; only routed HTTP handlers receive their configured guards.
- Duplicate application updates: register listeners once per scope; respect revision/cursor/idempotency boundaries. A reconnect does not deduplicate business operations.
- History overwrites newer events: capture live updates while loading a snapshot and merge by authoritative IDs/revisions.
- SSE appears as one final chunk: check proxy buffering/compression settings, use a real streaming client and avoid response.text on an infinite stream.
- SSE stops after a history gap: refresh the authoritative snapshot. Last-Event-ID cannot manufacture a replay log.
- CPU stays active in a Durable Object: remove untracked timers, await event work and use hibernation acceptWebSocket/auto responses.
- Requests time out during a slow handler: dispatch is serial per connection and has a bounded 45s processing watchdog; cancellations/heartbeats bypass it, but other RPC waits. Use HTTP for long CRUD/background jobs, or separate connections where explicit isolation is required.
- A huge history response closes a socket: paginate or bound payload size. Default frames are 64KiB. The demo's history chooses at most 200 messages within a 48KiB JSON budget; add cursor pagination for older history.
- `.vibe` template reads an imported ref incorrectly: use a setup-local binding or explicit .value. Run the pinned language-tools mapper with --runExternalCode.

## Known limitations

No Redis implementation, durable outbox, durable idempotency store, general CRDT, binary wire format, fetch-based SSE client, automatic transport fallback, automatic unsafe mutation retry or exactly-once effects are provided. The default registry/broker is process-local. Presence is approximate. Typing expires and is not durable.

The Cloudflare export provides a hibernating Durable Object owner, not a stateless Worker room registry. Local workerd verifies runtime compatibility and reconstruction; actual production hibernation/rolling-deployment behavior remains unverified. Attachment limits can constrain room count before the generic core room limit. Optional ecosystem snapshots in vendor/ are test/demo dependencies, not npm core dependencies.

The demo session database, known development credentials and non-secure loopback cookie configuration are intentionally local. Do not expose it as an internet authentication service. The SQLite chat/board fixture commits before publication but has no transactional outbox. A process crash can leave a committed update unpublished; history/snapshot recovery is the demonstrated remedy.
