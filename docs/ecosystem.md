# Ecosystem integrations

All integrations are optional. The headless npm core imports no auth, query, drag/drop, virtualizer, localization or UI component library. The workbench uses verified public APIs from the local ecosystem; three unreleased packages are pinned as development-only MIT snapshots in vendor/.

## VibeAuth

`examples/backend.ts` mounts the real `createAuthMiddleware(auth)` from @usevibejs/auth/server. Better Auth owns SQLite sessions, cookie verification, sign-in and sign-out. `createSessionAuthenticator` calls its public session source with cookie-cache/refresh disabled. Trusted membership and role records are separate application tables, not user-supplied organization fields.

The browser uses `createVibeAuthClient().client.signIn.email` and signOut. HTTP `/api/me` resolves server identity before connecting. Logout disconnects, unmounts private realtime components and clears VibeQuery. Reconnect uses fresh cookies. A core validatePrincipal callback checks the real session store on every sensitive operation/private delivery. The actual integration tests sign in through Better Auth, forge a payload identity, attempt cross-tenant subscriptions, deny a member's administrator-only task creation, sign out and verify stale sessions receive no private data.

## VibeQuery

`syncSocketQuery(client, event, apply, resynchronize)` returns cleanup. It observes connection **transitions**, not every state notification. It runs resynchronization on re-entry after a previous connection and catches that callback's asynchronous failure. The application should expose a resynchronization error state when recovery fails.

Chat uses the real VibeQuery QueryClient.setQueryData. Cache keys include the trusted user's ID and room. It merges history and concurrent events by persisted message IDs, retains 200 visible messages (history responses also have a 48KiB JSON budget), and clears caches on identity change. For ordinary server state, call QueryClient.invalidateQueries from the event handler instead. The socket adapter owns no second query cache. Revision/cursor rules belong to your application; do not blindly append out-of-order mutation events.

## VibeDND

The Kanban component uses createDndManager/useDnd and useSortable; accepted drag-end operations produce one task:move request. Keyboard-accessible Move buttons use the same server operation. No raw pointer frames or permanent ordering updates are sent during drag frames.

SQLite stores the ordered task array and board revision. Each operation supplies expectedRevision. The server serializes its transaction, rejects a stale revision with CONFLICT, commits and publishes the authoritative board. Optimistic local UI is reconciled by the returned/broadcast revision; conflicts reload the snapshot. This is an application conflict policy, not CRDT collaboration. VibeSync remains the place for generalized conflict-free documents.

## VibeVirtual

Dashboard uses useVirtualizer to render only visible metric rows. Incoming events retain at most 500 samples; chart history uses the latest 40. The EventSource is independent of viewport rendering. Use durable history/pagination when building larger feeds, and preserve an application anchor when prepending rows. The example never forces scroll-to-bottom; paused users skip samples rather than accumulate an unbounded backlog. The UI explicitly reports skipped samples.

## VibeI18n

The site installs createI18n and uses locale-independent wire events/codes. English/Turkish UI labels cover connection status, room labels, presence, typing and localized chat timestamps. Locale changes affect presentation only. Core error codes stay stable across languages; applications should map those codes to their catalog rather than localizing protocol identifiers.

## Vibecn

The workbench copies source primitives (Button and Badge, plus the available Card/Input/Skeleton primitives) from the inspected MIT registry, retaining notices and resolving its documented utility placeholder. It styles the source compositions for this site. Chat, connection inspector, feed, typing/presence and reconnection controls compose those primitives; the headless package imports none. Consumers do not need to install VibeSocket to use unrelated Vibecn components.

## Showcase source

The four runnable examples share one explicit backend/bootstrap and contract to avoid four divergent server implementations. Their `.vibe` components live in site/components: Chat, Kanban/Task, Dashboard and Streaming. The examples/chat, examples/kanban, examples/dashboard and examples/sse guides link to the actual executable source. All compile through the real Vibe.js compiler and language-tools mapper. Browser tests use independent contexts and the real HTTP/WebSocket backend.
