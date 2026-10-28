# Architecture and research

The inspected Vibe.js 0.1.0 public surface provides app plugins, app-scoped provide/inject, detached effect scopes, scope disposal, app unmount callbacks, refs, shallow refs and computed values. The adapter uses those APIs without a second reactive runtime. Event listeners update only their own consumers. Native sockets remain outside deep proxies.

The server exposes `VibeFactory.create`, `app.fetch`, Fetch middleware, `app.get` and shutdown hooks. `app.listen` internally owns an srvx listener and returns only a URL. There is **no public Node upgrade hook or server handle**. Use an externally owned `node:http` server with `srvx/node`'s `toNodeHandler(app.fetch)` and attach the Node adapter to that server. No framework modification or monkey patch is required. The CLI's normal HTTP middleware mounting does not install the socket upgrade listener; the example bootstrap owns it explicitly.

The HTTP pipeline accepts a Response and preserves its body as a stream when adding headers. It counts requests until the Response is returned, not until streaming ends: realtime stream cleanup belongs to the adapter, and must precede app shutdown. HTTP guards do not automatically guard middleware endpoints or upgrades. Authenticate each explicitly.

## Boundaries

`contract`: Standard Schema definitions and type inference, independent of HTTP contracts. `.`: browser-safe transport/core client. `client`: Vibe.js plugin and composables. `server`: runtime-independent connection registry and message routing. `node`: ws noServer adapter for an externally owned listener. `sse`: Fetch streams and native EventSource, independent of WebSocket framing. Optional scaling adapters have separate exports.

JSON text frames use protocol version 1. A server hello binds the contract version before the client dispatches data. Event frames are unacknowledged; request frames resolve only after handler completion and response validation. Room subscriptions use acknowledged commands, stable local ownership and fresh authorization on reconnect. No automatic mutation replay. Persistence/replay belongs to application storage.

## Primary references (checked 2026-10-28)

- [WebSocket specification](https://websockets.spec.whatwg.org/): browser send buffers and ping/pong limitations.
- [HTML SSE specification](https://html.spec.whatwg.org/multipage/server-sent-events.html): UTF-8 fields, IDs, retry and Last-Event-ID.
- [Streams specification](https://streams.spec.whatwg.org/): byte queue strategies, cancellation and desiredSize.
- [Node HTTP](https://nodejs.org/api/http.html#event-upgrade): upgrade stream ownership is distinct from request handling.
- [ws](https://github.com/websockets/ws#client-authentication): noServer, handleUpgrade, maxPayload and shutdown.
- [Standard Schema](https://standardschema.dev/): async validation and input/output inference without a mandatory schema implementation.
- [Cloudflare WebSockets](https://developers.cloudflare.com/workers/runtime-apis/websockets/), [DO hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/): WebSocketPair, acceptWebSocket, attachments, auto responses; in-memory state can disappear.
- [Hono](https://hono.dev/docs/helpers/websocket): runtime-specific upgrade implementations.
- [PartyKit](https://docs.partykit.io/how-partykit-works/): room ownership and coordination.
- [Socket.IO delivery](https://socket.io/docs/v4/delivery-guarantees/): ordering and persistence are separate from reconnect.
- [Page visibility](https://developer.mozilla.org/en-US/docs/Web/API/Page_Visibility_API), [online hints](https://developer.mozilla.org/en-US/docs/Web/API/Window/online_event): visibility and connectivity are hints, not proof of reachability.

## Verified ecosystem APIs

VibeAuth delegates to Better Auth's `api.getSession({ headers, query: { disableCookieCache: true, disableRefresh: true } })`; use this public source on each authorization check rather than manufacturing an HTTP RequestContext. VibeQuery exports TanStack QueryClient including invalidateQueries/setQueryData. VibeDND exports useSortable and drag-end monitor APIs. VibeVirtual exports useVirtualizer. VibeI18n exports createI18n/useI18n via its adapter. Vibecn provides source primitives; the headless package does not depend on them.
