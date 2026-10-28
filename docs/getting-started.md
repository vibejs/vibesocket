# Getting started

VibeSocket is a headless realtime library for Vibe.js applications. It uses native browser WebSocket/EventSource and established runtime server adapters. It does not replace HTTP contracts, your database, VibeQuery or application authentication.

## Installation

```sh
pnpm add @usevibejs/socket @usevibejs/ui
# Choose a Standard Schema implementation for shared contracts:
pnpm add valibot
```

Node 24.15+ (24.x), or Node 26+, is supported. `ws` 8.22.0 is optional and used only by the Node export. Installing with optional dependencies disabled requires separately installing ws before importing `/node`. Cloudflare and browser entry points do not import it. Vibe.js 0.1.0 is the verified peer; use a matching compiler/runtime.

## Quick start

```ts
import * as v from 'valibot';
import { defineSocketContract } from '@usevibejs/socket/contract';
export const contract = defineSocketContract({
  client: { typing: v.object({ room: v.string(), active: v.boolean() }) },
  server: { changed: v.object({ text: v.string() }) },
  requests: { echo: { input: v.string(), output: v.string(), errors: ['FORBIDDEN'] } },
});
```

```ts
import { createSocketClient } from '@usevibejs/socket';
import { createSocketPlugin } from '@usevibejs/socket/client';
const socket = createSocketClient({ contract, url: 'wss://app.example/socket' });
app.use(createSocketPlugin(socket));
```

Inside a component use `useSocket(contract)` with the same contract object passed to the plugin. The app provides its own client. Each plugin owns its client and disposes it on app unmount. For explicit shared connection ownership, manage a headless client yourself and pass it to composables; do not install that same client into two independently owned plugins.

```vibe
<script setup lang="ts">
import { useSocket, useSocketEvent, useSocketRoom, useSocketStatus } from '@usevibejs/socket/client';
import { contract } from './contract.ts';
const socket = useSocket(contract);
const { active } = useSocketRoom(socket, 'project:123');
const { connected } = useSocketStatus(socket);
const latest = useSocketEvent(socket, 'changed');
</script>
<template><p>{{ connected ? 'Connected' : 'Connecting' }}</p><p>{{ active ? latest?.text : 'Joining room' }}</p></template>
```

Use local setup bindings for template ref unwrapping. Refs imported directly from another module require explicit `.value` in Vibe.js templates. Composables require component setup or an active effect scope; headless clients require explicit disposal.

## Choose WebSocket or SSE

| Need                                                         | Transport                                 |
| ------------------------------------------------------------ | ----------------------------------------- |
| Two-way interactive events, typing, rooms, realtime requests | WebSocket                                 |
| One-way metrics, notifications, streamed text                | SSE                                       |
| CRUD, uploads, cacheable reads                               | Ordinary HTTP                             |
| Reliable historical recovery                                 | Either, plus retained application history |

SSE cannot send client operations. Native EventSource accepts a URL and a credential flag, not arbitrary request headers or POST bodies. WebSocket clients cannot set arbitrary handshake headers either. Same-origin cookies plus an Origin allowlist are the simplest session approach. The SSE client delegates retries and Last-Event-ID to the browser.

## Local examples

`pnpm dev` starts the externally owned Vibe.js HTTP server on loopback port 3100 and the Vibe.js site on 5173. The site proxies `/api` to the backend. Use the exact address `http://127.0.0.1:5173`. The database, session secret and messages are local in `.data/` and excluded from Git. The supplied demo identities/password are exclusively for this loopback development fixture.
