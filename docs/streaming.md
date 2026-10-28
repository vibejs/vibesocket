# Server-Sent Events

## Fetch streams

```ts
import { createSSEStream } from '@usevibejs/socket/sse';
const stream = createSSEStream({
  signal: request.signal,
  maxBufferedBytes: 262144,
  keepalive: 15000,
  onClose: () => unsubscribeFromProducer(),
});
stream.send({ event: 'log', id: '42', retry: 2000, data: 'first line\nsecond line' });
return stream.response;
```

`createSSEStream` returns a Fetch Response over a UTF-8 ReadableStream. Fields are newline/NUL checked, each data line has its own data field, and frames end in a blank line. Headers disable response transformation and proxy buffering. Keepalive comments maintain idle streams and carry no event data. `keepalive: false` disables the timer. `send` returns false after cancellation or byte overflow. A full byte queue closes the slow subscriber; producers must respect that result.

Stream cancellation and request abort call cleanup once and stop keepalive timers. Request lifetimes and stream lifetimes differ: Vibe.js considers the request handled when returning a Response. Close your streams before application shutdown. Infrastructure proxies can still buffer or time out streams; configure them explicitly and test your deployment. The integration suite reads multiple chunks over real HTTP through Vibe.js, rather than calling response.text on an infinite stream.

## Typed channels and native EventSource

```ts
import * as v from 'valibot';
import { createSSEChannel, createEventStream } from '@usevibejs/socket/sse';
const events = { metric: v.object({ value: v.number() }) };
const channel = createSSEChannel(events);
// In authenticated Fetch middleware:
const response = await channel.response(request, {
  allowed: () => sessionStillValid(),
  expiresAt: session.expiresAt,
});
await channel.publish('metric', { value: 42 }, '42');
// Browser:
const client = createEventStream({ url: '/events', events, withCredentials: true });
const stop = client.on('metric', (payload, id) => {});
client.connect();
// On disposal:
stop();
client.dispose();
```

Those application session functions are intentionally supplied by your auth service. Channel response rejects non-GET requests, denies failed authorization, checks expiration and removes disconnected subscribers. Publication rechecks each subscriber's access. Close the channel on shutdown. It defaults to 1000 total subscribers; supply a trusted access.key to enforce the default 8 subscribers per identity. Already-aborted requests do not consume identity capacity. Each channel is a single-process fanout; use a shared backend/owner for multiple processes.

EventSource handles named events and its own automatic reconnect; VibeSocket never runs a second SSE reconnect timer. Its state exposes status, lastEventId and error. Received JSON payloads are validated with Standard Schema and dispatched in order through a bounded incoming backlog. Oversized or flooded backlogs fail closed. `close()` stops reconnecting; `connect()` after close creates a fresh EventSource.

Native EventSource cannot set Authorization or other arbitrary headers. Use same-origin cookies or your own separate fetch-stream implementation when headers/POST are required. This package does not ship a fetch-based SSE parser. Credentials and CORS policy must be configured at the HTTP boundary; the channel does not automatically allow cross-origin credentialed requests.

## IDs, replay and gaps

The browser retains IDs on the same EventSource and sends Last-Event-ID when it reconnects. A new EventSource does not automatically inherit a prior object's cursor. `lastEventId` is observable, not proof of retained history. IDs must identify an application-ordered stream, and your storage must enforce cursor/tenant scope.

```ts
const channel = createSSEChannel(events, {
  replay: {
    async readAfter(cursor, signal) {
      // Return retained authorized frames, or gap:true when unavailable.
      return { gap: true, events: [] };
    },
  },
  maxReplayEvents: 1000,
});
```

The channel captures live publications while replay storage is read, validates replay payloads, sends ordered history, and skips buffered IDs already replayed. The live replay buffer is capped at 256KiB; overflow closes the stream. Publication ordering assumes one ordered producer. A provider must apply the same stream/tenant authorization as live delivery; the channel cannot infer business meaning from an opaque cursor. Access is rechecked before replay frames. Do not share a private channel's replay provider across tenants without scoping it.

No provider, an expired cursor or provider failure emits a named `vibesocket:gap` event with REPLAY_UNAVAILABLE. The client exposes that error so the application can load an authoritative snapshot. A gap does not invent missing data. The example retains the latest 1000 metric rows in SQLite; arbitrary IDs and cursors outside retained bounds produce a gap. WebSocket chat uses explicit history recovery instead of automatic event replay.

The token-stream showcase is a deterministic simulated one-shot stream, not an AI SDK. It demonstrates named token/done events and cancellation and intentionally has no replay history.
