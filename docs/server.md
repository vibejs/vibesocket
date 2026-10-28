# Server and Node integration

## Own the upgrade listener

Vibe.js 0.1.0 does not expose its internally owned Node listener or a WebSocket upgrade hook. Its Fetch middleware cannot replace Node's native upgrade event. This bootstrap uses public APIs and keeps ordinary HTTP routes working:

```ts
import { createServer, type RequestListener } from 'node:http';
import { toNodeHandler } from 'srvx/node';
import { Module, VibeFactory } from '@usevibejs/server';
import { createSocketServer } from '@usevibejs/socket/server';
import { attachNodeSocket } from '@usevibejs/socket/node';
import { contract } from './contract.ts';

class Root {}
Module({})(Root);
const app = await VibeFactory.create(Root);
const realtime = createSocketServer({
  contract,
  authorize: (principal, operation) => !operation.room || operation.room === `user:${principal.id}`,
});
realtime.handle('echo', async (input, context) => input);
const http = createServer(toNodeHandler(app.fetch) as RequestListener);
const adapter = attachNodeSocket(http, realtime, {
  path: '/socket',
  allowedOrigins: ['https://app.example'],
  authenticate: async (request) => {
    // Replace with your application's verified session service.
    return null;
  },
});
http.listen(3000);
// On shutdown, stop producers first, then:
await adapter.close();
await new Promise<void>((resolve) => http.close(() => resolve()));
await app.close();
```

Install `srvx` explicitly when using the bridge above. Do not call `app.listen` as well: the external server already owns HTTP. The stock `vibejs dev` HTTP middleware does not attach the socket listener; use the example bootstrap or attach the adapter to a publicly owned Vite HTTP server. No private server imports or listener monkey patches are used. A normal Response with status 101 is not a Node WebSocket upgrade.

## Event handlers and services

`on(clientEvent, handler)` handles notifications. `handle(requestName, handler)` resolves RPC. Input validation precedes dispatch. Async failures become safe error codes and cannot crash another connection. Handlers receive a server-created connection context, cooperative AbortSignal, optional idempotencyKey and `fail(declaredCode)`. No identity is inferred from payload user IDs.

Use `app.get(Service)` to capture existing application-scoped services in closures. HTTP RequestContext and guards are request-specific and are not fabricated for socket events. Put reusable permissions in a service and call it from both HTTP guards and socket policies. Provider `onModuleDestroy` can stop realtime producers; explicitly close external adapters before closing the app.

## Authentication and authorization

`authenticate(Request, { address, signal })` runs during the real Node upgrade, with a 5s default timeout and bounded pending upgrades. Return a trusted principal with an ID and optional millisecond expiresAt, or null. The default requires an Origin exactly matching the allowlist. `allowMissingOrigin` is an explicit non-browser opt-in; Origin is a browser CSWSH boundary, not proof of identity for arbitrary network clients.

Core `authorize(principal, { action, room?, event? }, connection)` is required. It runs for joins, each inbound event/request and each outbound recipient. Rooms grant routing eligibility only after authorization. `validatePrincipal` can query the session store on every operation/delivery. Expiry is checked by the core; Node also closes at expiry or a bounded maximum connection lifetime. `revoke(principalId)` immediately disconnects all indexed connections. Permission rejection removes outbound room membership; restore permissions and explicitly re-subscribe/reconnect to rejoin.

## Broadcasts

`publish(room, event, payload, { exclude?, overflow? })` targets indexed members, revalidates recipients and returns attempted/written/rejected counts. Written means the local transport accepted the frame. It is not remote application delivery. `overflow: 'drop'` explicitly drops ephemeral state when a buffer is full; default disconnect protects important event consumers from silent loss. Use replay/snapshots after reconnect.

`send(connectionId, event, payload)` targets one authorized connection. `sendToUser(principalId, event, payload)` uses the principal index for multiple tabs/devices. Organization broadcast is an authorized application room such as `organization:456`; raw organization identifiers supplied by clients are never an authorization policy. There is no unrestricted global-broadcast convenience API.

## Registry and resource limits

The registry belongs to the core instance. Connections expose ID, frozen principal, joined room/subscription IDs, connectedAt, lastActivity, transport type, optional closeInfo (code/reason/time) and a disconnect signal. Transport resources are separate. `connections`, `roomCount`, `metrics` and `stats` provide inspection. Avoid serializing session metadata into untrusted logs.

Defaults: 10,000 total connections, 8 per principal, 32 rooms each, 64 queued incoming frames, 100 incoming frames/sec, 64KiB frames, 256KiB outbound buffered bytes 30s handler timeout and a 45s complete processing deadline (including async validation/authorization). These are per instance, not distributed quotas. An async handler that ignores cancellation can continue its own external work after timeout; it must check the signal around expensive work and revalidate permissions before privileged commits.

The Node adapter uses a shared passive idle sweep: application messages/heartbeats renew a 90s lease. Configure idleTimeout or set it to false for clients with another health policy. Hidden-tab timer throttling can delay application heartbeats and cause a reconnect; increase this timeout for such deployments. No duplicate native ping loop is installed.

The Node adapter additionally bounds pending upgrades (128), connections per source (128), attempts per source (120/min with a bounded 4096-source index), message bytes and initial protocol negotiation (5s). Do not trust forwarded IP headers by default. An `allowUpgrade(address)` hook supports external/distributed limits. Behind a trusted proxy, apply edge limits and an authenticated source mechanism. Unmatched upgrade paths are rejected by default; set `unmatchedPath: 'ignore'` when a separate owner routes those paths, and install an explicit fallback.

`ws` runs with noServer, compression disabled and maxPayload enforced before application parsing. Binary frames are rejected. The browser sends application ping/pong because JavaScript cannot emit native protocol control pings. Node does not install a competing heartbeat loop. Close detaches upgrade listeners, closes pending handshakes and active connections, then terminates remaining sockets after a bounded drain. The external HTTP listener remains caller-owned.
