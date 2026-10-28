# VibeSocket

**Realtime, Native to Vibe.js**

Typed WebSockets, authorized rooms, bounded reconnecting clients and Fetch-compatible SSE for Vibe.js. MIT licensed. Package: `@usevibejs/socket`.

```sh
pnpm add @usevibejs/socket @usevibejs/ui valibot
```

Define once, validate on both sides:

```ts
import { defineSocketContract } from '@usevibejs/socket/contract';
import * as v from 'valibot';

export const contract = defineSocketContract({
  client: { typing: v.object({ room: v.string(), active: v.boolean() }) },
  server: { 'message:created': v.object({ id: v.string(), text: v.string() }) },
  requests: {
    'message:send': {
      input: v.object({ room: v.string(), text: v.string() }),
      output: v.object({ id: v.string() }),
      errors: ['FORBIDDEN'],
    },
  },
});
```

```ts
import { createSocketClient } from '@usevibejs/socket';
import { createSocketPlugin } from '@usevibejs/socket/client';
const socket = createSocketClient({ contract, url: 'wss://your-app.example/socket' });
const plugin = createSocketPlugin(socket);
app.use(plugin);
```

```vibe
<script setup lang="ts">
import { useSocket, useSocketEvent, useSocketRoom } from '@usevibejs/socket/client';
import { contract } from './chat.contract.ts';
const socket = useSocket(contract);
const room = useSocketRoom(socket, 'chat:general');
const latest = useSocketEvent(socket, 'message:created');
</script>
<template><p>{{ latest?.text }}</p><p>{{ room.active.value ? 'Subscribed' : 'Joining' }}</p></template>
```

The Node adapter uses an externally owned HTTP server and `ws`'s real upgrade handler. **Ordinary Vibe.js Fetch middleware cannot handle Node upgrades.** See [Node integration](docs/server.md). SSE does run through Fetch middleware and is tested through the actual Vibe.js HTTP pipeline.

Events are best effort. RPC resolves after handler completion and response validation; durable success depends on what the handler commits. Reconnection restores subscriptions, not missed business data. [Delivery guarantees](docs/delivery.md) explain the boundaries.

Run the local workbench:

```sh
pnpm install
pnpm dev
# Open http://127.0.0.1:5173 in two independent browsers.
```

Choose Ada and Lin for the same organization; Eve belongs to another. Chat and Kanban use SQLite in `.data/`. Sessions use VibeAuth/Better Auth. Metrics use retained SSE history; token streaming demonstrates cancellation. The headless npm package has no showcase dependencies. The three unreleased ecosystem packages are pinned local development snapshots in `vendor/` (MIT notices included).

```sh
pnpm verify        # build, strict TS + .vibe checks, lint, format, tests, site, 3 browsers, packed consumers
pnpm bench         # local load scenarios; no external target
```

Read [getting started](docs/getting-started.md), [client](docs/client.md), [security](docs/security.md), [scaling](docs/scaling.md), [API reference](docs/reference.md) and [engineering report](docs/report.md). Browser/Node/Cloudflare entry points are isolated. Cloudflare is tested locally with workerd; actual production hibernation remains a deployment verification requirement.

No npm publication, remote repository creation, push or deployment is part of this repository's verification.
