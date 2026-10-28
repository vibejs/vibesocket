import { afterEach, expect, it } from 'vitest';
import { createServer, type RequestListener } from 'node:http';
import { once } from 'node:events';
import { toNodeHandler } from 'srvx/node';
import WebSocket from 'ws';
import { createDemoBackend, demoPassword } from '../examples/backend.ts';
import { demoContract } from '../examples/contract.ts';
import { attachNodeSocket } from '../src/node.ts';
import { createSocketClient } from '../src/index.ts';
import { waitRoom, waitState } from './fixture.ts';
const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
});
async function backend() {
  const b = await createDemoBackend({
    database: ':memory:',
    secret: 'local-security-test-secret-with-more-than-32-characters',
    baseURL: 'http://app.test',
    origins: ['http://app.test'],
    simulate: false,
  });
  const http = createServer(toNodeHandler(b.app.fetch) as RequestListener);
  const adapter = attachNodeSocket(http, b.core, {
    allowedOrigins: ['http://app.test'],
    authenticate: b.authenticate,
  });
  http.listen(0, '127.0.0.1');
  await once(http, 'listening');
  const address = http.address();
  if (!address || typeof address === 'string') throw new Error('No address');
  const port = address.port;
  const clients: { dispose(): void }[] = [];
  cleanup.push(async () => {
    for (const c of clients) c.dispose();
    await adapter.close();
    await new Promise<void>((resolve) => http.close(() => resolve()));
    await b.close();
  });
  async function user(name: string) {
    const response = await b.auth.api.signInEmail({
      body: { email: `${name.toLowerCase()}@demo.test`, password: demoPassword },
      asResponse: true,
    });
    const cookie = response.headers
      .getSetCookie()
      .map((line) => line.split(';')[0])
      .join('; ');
    const c = createSocketClient({
      contract: demoContract,
      url: `ws://127.0.0.1:${port}/socket`,
      heartbeat: false,
      lifecycle: false,
      webSocket: (url, protocols) =>
        new WebSocket(url, protocols, {
          origin: 'http://app.test',
          headers: { cookie },
        }) as unknown as globalThis.WebSocket,
    });
    clients.push(c);
    c.connect();
    await waitState(c, 'connected');
    const principal = await b.authenticate(new Request('http://app.test', { headers: { cookie } }));
    if (!principal) throw new Error('Missing real session');
    return { c, cookie, principal };
  }
  return { ...b, user };
}
it('uses real VibeAuth sessions, server identity, roles and cross-tenant room authorization', async () => {
  const b = await backend(),
    ada = await b.user('Ada'),
    eve = await b.user('Eve'),
    lin = await b.user('Lin');
  const room = 'chat:orbit:general',
    as = ada.c.subscribe(room),
    es = eve.c.subscribe(room);
  await Promise.all([waitRoom(ada.c, as.id), waitRoom(eve.c, es.id, 'rejected')]);
  await expect(eve.c.request('history', { room })).rejects.toMatchObject({ code: 'FORBIDDEN' });
  await expect(
    lin.c.request('task:create', { title: 'forbidden', expectedRevision: 0 }),
  ).rejects.toMatchObject({ code: 'FORBIDDEN' });
  const payload = { room, text: 'server-bound identity', user: eve.principal.id };
  const result = await ada.c.request('message:send', payload);
  expect(result.user).toBe(ada.principal.id);
  expect(result.user).not.toBe(eve.principal.id);
  expect(await ada.c.request('history', { room })).toContainEqual(result);
  expect(es.active).toBe(false);
  expect(JSON.stringify(b.core.connections)).not.toContain('token');
});
it('rechecks real session revocation before outbound delivery and rejects stale cookies on reconnect', async () => {
  const b = await backend(),
    ada = await b.user('Ada');
  const sub = ada.c.subscribe('chat:orbit:general');
  await waitRoom(ada.c, sub.id);
  await b.auth.api.signOut({ headers: new Headers({ cookie: ada.cookie }) });
  expect(
    (
      await b.core.publish('chat:orbit:general', 'typing:updated', {
        room: 'chat:orbit:general',
        users: [],
      })
    ).written,
  ).toBe(0);
  await waitState(ada.c, 'failed');
  expect(
    await b.authenticate(new Request('http://app.test', { headers: { cookie: ada.cookie } })),
  ).toBeNull();
});
it('serializes authoritative board revisions and rejects conflicting mutations', async () => {
  const b = await backend(),
    ada = await b.user('Ada'),
    lin = await b.user('Lin');
  const original = await ada.c.request('board', null);
  const operations = await Promise.allSettled([
    ada.c.request('task:move', {
      id: original.tasks[0].id,
      column: 'done',
      index: 0,
      expectedRevision: original.revision,
    }),
    lin.c.request('task:move', {
      id: original.tasks[1].id,
      column: 'todo',
      index: 0,
      expectedRevision: original.revision,
    }),
  ]);
  expect(operations.filter((o) => o.status === 'fulfilled')).toHaveLength(1);
  expect(operations.find((o) => o.status === 'rejected')).toMatchObject({
    reason: { code: 'CONFLICT' },
  });
  expect((await ada.c.request('board', null)).revision).toBe(original.revision + 1);
});

it('replays persisted SQLite SSE history through Vibe.js and reports unavailable cursors', async () => {
  const b = await backend(),
    ada = await b.user('Ada');
  await b.sample();
  await b.sample();
  await b.sample();
  const read = async (cursor: string, marker: string) => {
    const response = await b.app.request('/api/events', {
      headers: { cookie: ada.cookie, 'last-event-id': cursor },
    });
    expect(response.headers.get('content-type')).toContain('text/event-stream');
    const reader = response.body!.getReader(),
      decoder = new TextDecoder();
    let text = '';
    while (!text.includes(marker)) {
      const item = await reader.read();
      if (item.done) throw new Error('Replay ended prematurely');
      text += decoder.decode(item.value);
    }
    await reader.cancel();
    return text;
  };
  const replay = await read('1', '"tick":3');
  expect(replay).toContain('id: 2');
  expect(replay).toContain('id: 3');
  expect(replay).not.toContain('id: 1\n');
  expect(await read('not-a-cursor', 'REPLAY_UNAVAILABLE')).toContain('vibesocket:gap');
  expect(b.channel.size).toBe(0);
});
