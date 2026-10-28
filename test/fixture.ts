import { createServer, type RequestListener } from 'node:http';
import { once } from 'node:events';
import { Module, VibeFactory, JsonLogger, type Middleware } from '@usevibejs/server';
import { toNodeHandler } from 'srvx/node';
import WebSocket from 'ws';
import * as v from 'valibot';
import { defineSocketContract } from '../src/contract.ts';
import { createSocketServer } from '../src/server.ts';
import { attachNodeSocket, type NodeAdapterOptions } from '../src/node.ts';
import { createSocketClient, type ConnectionStatus, type SocketClient } from '../src/index.ts';
import type { SocketContract } from '../src/contract.ts';

export const contract = defineSocketContract({
  client: { notify: v.object({ text: v.string() }) },
  server: { changed: v.object({ value: v.number() }) },
  requests: {
    double: { input: v.number(), output: v.number(), errors: ['CONFLICT'] },
    wait: { input: v.null(), output: v.null() },
  },
});
export interface Identity {
  id: string;
  org: string;
  expiresAt?: number;
}
export function waitState<C extends SocketContract>(
  client: SocketClient<C>,
  status: ConnectionStatus,
): Promise<void> {
  if (client.state.status === status) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      off();
      reject(new Error(`Expected ${status}, got ${client.state.status}`));
    }, 3000);
    const off = client.observe((state) => {
      if (state.status === status) {
        clearTimeout(timeout);
        off();
        resolve();
      }
    });
  });
}
export function waitRoom<C extends SocketContract>(
  client: SocketClient<C>,
  id: string,
  status = 'active',
) {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      off();
      reject(new Error(`Room did not become ${status}`));
    }, 3000);
    const off = client.observeSubscriptions((next) => {
      if (next.id === id && next.status === status) {
        clearTimeout(timer);
        off();
        resolve();
      }
    });
  });
}
export async function fixture(
  middleware: Middleware[] = [],
  limits: {
    maxRooms?: number;
    maxBufferedBytes?: number;
    messagesPerSecond?: number;
    maxMessageBytes?: number;
  } = {},
  nodeOptions: Partial<NodeAdapterOptions<Identity>> = {},
) {
  const identities = new Map<string, Identity>([
    ['a', { id: 'alice', org: 'a' }],
    ['b', { id: 'bob', org: 'b' }],
    ['c', { id: 'carol', org: 'a' }],
  ]);
  const core = createSocketServer({
    contract,
    ...limits,
    validatePrincipal: (p: Identity) => [...identities.values()].some((i) => i.id === p.id),
    authorize: (p, op) => !op.room || op.room === `org:${p.org}`,
  });
  core.handle('double', (value, ctx) => {
    if (value < 0) ctx.fail('CONFLICT');
    return value * 2;
  });
  core.handle(
    'wait',
    (_value, ctx) =>
      new Promise<null>((resolve) =>
        ctx.signal.addEventListener('abort', () => resolve(null), { once: true }),
      ),
  );
  class Root {}
  Module({})(Root);
  const app = await VibeFactory.create(Root, {
    logger: new JsonLogger({ level: 'error' }),
    middleware: [...middleware, async () => Response.json({ ok: true })],
  });
  const http = createServer(toNodeHandler(app.fetch) as RequestListener);
  const adapter = attachNodeSocket(http, core, {
    allowedOrigins: ['http://app.test'],
    maxPayload: limits.maxMessageBytes,
    authenticate: (req) =>
      identities.get(req.headers.get('cookie')?.replace('session=', '') ?? '') ?? null,
    ...nodeOptions,
  });
  http.listen(0, '127.0.0.1');
  await once(http, 'listening');
  const address = http.address();
  if (!address || typeof address === 'string') throw new Error('No address');
  const url = `http://127.0.0.1:${address.port}`;
  const clients: ReturnType<typeof createSocketClient<typeof contract>>[] = [];
  function client(
    session = 'a',
    config: Partial<Parameters<typeof createSocketClient<typeof contract>>[0]> = {},
  ) {
    const c = createSocketClient({
      contract,
      url: url.replace('http', 'ws') + '/socket',
      heartbeat: false,
      lifecycle: false,
      webSocket: (target, protocols) =>
        new WebSocket(target, protocols, {
          origin: 'http://app.test',
          headers: { cookie: `session=${session}` },
        }) as unknown as globalThis.WebSocket,
      ...config,
    });
    clients.push(c);
    return c;
  }
  return {
    app,
    http,
    core,
    identities,
    adapter,
    url,
    client,
    async close() {
      for (const c of clients) c.dispose();
      await adapter.close();
      await new Promise<void>((resolve) => http.close(() => resolve()));
      await app.close();
    },
  };
}
