# Scaling and Cloudflare

## Multiple processes

Each `createSocketServer` owns an in-memory registry, user index and room index. One process cannot directly see another process's sockets. `createMemoryPubSub()` shares channels only among consumers of that same object in one process. It is explicitly not a Redis substitute or distributed broker.

`PubSub` has asynchronous publish, subscribe/unsubscribe and close. Messages contain an ID, room, event and payload. `connectPubSub(core, backend, namespace)` validates inbound events through the core and rechecks local recipient permissions. Call the returned publish method to fan out through the configured backend; it does not monkey patch ordinary core.publish. Close subscriptions before closing the backend. Its 1000-ID in-memory deduplication window prevents recent duplicate fanout only; restart or eviction ends that boundary. The backend is trusted server infrastructure, never a client-controlled bus endpoint.

```ts
import { createMemoryPubSub, connectPubSub } from '@usevibejs/socket/adapters';
const bus = createMemoryPubSub();
const binding = await connectPubSub(realtime, bus, 'my-application');
await binding.publish('project:123', 'changed', { text: 'updated' });
await binding.close();
await bus.close();
```

A Redis adapter is intentionally not included. Implement the PubSub interface using your chosen maintained Redis library and specify its ordering, disconnect, retry, duplicate and durability behavior. Redis Pub/Sub itself is transient. Persistence/outbox semantics do not become durable just because a distributed fanout bus exists.

## Cloudflare Workers and Durable Objects

`/cloudflare` imports neither node:* nor ws. `createDurableSocket(state, options)` composes with a Durable Object's hibernation API. Forward fetch, webSocketMessage, webSocketClose and webSocketError. Its `core` exposes the same event/handler/publish interface as Node.

```ts
import { createDurableSocket } from '@usevibejs/socket/cloudflare';
export class RealtimeObject {
  realtime;
  constructor(state, env) {
    this.realtime = createDurableSocket(state, {
      contract,
      allowedOrigins: ['https://app.example'],
      authenticate: (request) => env.sessions.authenticate(request),
      validatePrincipal: (principal) => env.sessions.isValid(principal),
      authorize: (principal, operation) => env.permissions.check(principal, operation),
      attachmentPrincipal: (principal) => ({ id: principal.id, expiresAt: principal.expiresAt }),
    });
    this.realtime.core.handle('echo', (input) => input);
  }
  fetch(request) {
    return this.realtime.fetch(request);
  }
  webSocketMessage(socket, message) {
    return this.realtime.webSocketMessage(socket, message);
  }
  webSocketClose(socket, code, reason) {
    this.realtime.webSocketClose(socket, code, reason);
  }
  webSocketError(socket) {
    this.realtime.webSocketError(socket);
  }
}
```

The snippet's contract, bindings and permission services are application supplied. `test/worker.ts` is a complete executable Worker/DO fixture. Route the Worker to a Durable Object ID chosen by trusted application tenant/room policy. Validate both routing and subscriptions. A Durable Object is an explicit coordination owner; it does not automatically make arbitrary Node registries global.

## Hibernation

The adapter accepts the server side of WebSocketPair using state.acceptWebSocket; it does not call ws.accept or add ordinary message listeners. A Worker-only 101 Response returns the client side with the supported subprotocol. This Response must be constructed inside the Worker runtime, outside Vibe.js's ordinary Response-copying HTTP wrapper.

It serializes an attachment with schema version, contract version, projected identity, connection/subscription IDs, joined rooms, timestamps and outstanding credit counters. Reconstructing the adapter enumerates state.getWebSockets and restores indexes from deserializeAttachment. Invalid/incompatible attachments close the socket. No secrets are copied automatically: attachmentPrincipal must explicitly select identity fields. Attachment size is capped at 1900 UTF-8 bytes, below the runtime's limit; verbose principals or room sets can hit that cap before the core room count. Reduce metadata/room names, or store additional metadata in application storage.

Constant JSON application ping/pong frames use state.setWebSocketAutoResponse, so heartbeats need not wake the object. No perpetual timers are installed. Temporary processing/handler deadline timers exist only while processing a message and are cleared before returning. Application handlers must await their work and persistent writes; do not start untracked timers or assume maps survive hibernation.

Workers does not document browser-style bufferedAmount for accepted sockets. The adapter instead attaches receipt IDs to server frames and tracks a bounded 32-frame/256KiB credit window. The compatible VibeSocket client receipts frames after protocol validation/dispatch. Credit and IDs survive reconstruction. A full window closes the slow consumer. These receipts are transport flow control, not durable acknowledgments.

`webSocketMessage` awaits core draining so workerd tracks the event's complete asynchronous processing. Pending RPC is not serialized; in-flight work must finish before hibernation. Application persistence, replay cursors and snapshots belong to Durable Object storage or another application store, not the attachment.

## Runtime verification boundaries

The Miniflare/workerd test verifies real WebSocketPair handshakes, Origin/session denial, multiple connections, room authorization, broadcasts, receipts, automatic pong, attachment reconstruction and a Worker bundle without Node dependencies. It deliberately reconstructs the adapter over live attachments. That verifies restoration logic but does not prove Cloudflare production eviction timing, geography, connection limits or actual billing/hibernation behavior. Production hibernation and rolling Worker deployment behavior still need a separately authorized deployment test.

Plain stateless Workers cannot retain an authoritative room registry across requests. This package offers the hibernating Durable Object owner instead of advertising unreliable stateless process-local coordination. SSE remains Fetch-compatible and can be returned from a Worker with an application-owned producer and cancellation policy.
