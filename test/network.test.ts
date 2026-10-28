import { afterEach, expect, it } from 'vitest';
import WebSocket from 'ws';
import { once } from 'node:events';
import { fixture, waitRoom, waitState } from './fixture.ts';
const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
});
async function setup(limits: Parameters<typeof fixture>[1] = {}) {
  const f = await fixture([], limits);
  cleanup.push(f.close);
  return f;
}
it('performs a real handshake and retains ordinary Vibe.js HTTP requests', async () => {
  const f = await setup(),
    c = f.client();
  c.connect();
  await waitState(c, 'connected');
  expect(c.rawSocket?.protocol).toBe('vibesocket.v1');
  expect(await (await fetch(f.url)).json()).toEqual({ ok: true });
  expect(await c.request('double', 21)).toBe(42);
});
it('routes only to authorized room members with reference counted subscriptions', async () => {
  const f = await setup(),
    a = f.client(),
    b = f.client('b'),
    c = f.client('c');
  const sa = a.subscribe('org:a'),
    sb = b.subscribe('org:a'),
    sc = c.subscribe('org:a');
  const ready = [waitRoom(a, sa.id), waitRoom(b, sb.id, 'rejected'), waitRoom(c, sc.id)];
  a.connect();
  b.connect();
  c.connect();
  await Promise.all(ready);
  const values: number[] = [];
  a.on('changed', (p) => values.push(p.value));
  b.on('changed', () => {
    throw new Error('Cross-tenant event');
  });
  expect(await f.core.publish('org:a', 'changed', { value: 1 })).toEqual({
    attempted: 2,
    written: 2,
    rejected: 0,
  });
  expect(b.state.error?.code).toBe('FORBIDDEN');
  expect(f.core.connections.find((s) => s.principal.id === 'bob')?.rooms.size).toBe(0);
  const duplicate = a.subscribe('org:a');
  expect(duplicate.id).toBe(sa.id);
  sa.unsubscribe();
  expect(duplicate.active).toBe(true);
  duplicate.unsubscribe();
  sc.unsubscribe();
  await c.request('double', 1);
  expect(f.core.roomCount).toBe(0);
});
it('reconnects with backoff and rejoins once', async () => {
  const f = await setup(),
    c = f.client('a', { reconnect: { minDelay: 50, maxDelay: 50 } });
  const sub = c.subscribe('org:a');
  const ready = waitRoom(c, sub.id);
  c.connect();
  await ready;
  const rejoin = waitRoom(c, sub.id);
  f.core.close(f.core.connections[0].id, 4000, 'network test');
  await rejoin;
  expect(c.state.attempts).toBe(1);
  expect(f.core.connections).toHaveLength(1);
  expect(f.core.connections[0].rooms.size).toBe(1);
});
it('does not reconnect after intentional disconnect or terminal revocation', async () => {
  const f = await setup(),
    c = f.client();
  c.connect();
  await waitState(c, 'connected');
  f.core.revoke('alice');
  await waitState(c, 'failed');
  expect(c.state.attempts).toBe(0);
  c.reconnect();
  await waitState(c, 'connected');
  c.disconnect();
  expect(c.state.status).toBe('disconnected');
  expect(c.rawSocket).toBeUndefined();
});
it('rejects runtime payload errors and declared errors without leaking internals', async () => {
  const f = await setup(),
    c = f.client();
  c.connect();
  await waitState(c, 'connected');
  await expect(c.request('double', -1)).rejects.toMatchObject({ code: 'CONFLICT' });
  f.core.handle('double', () => {
    throw new Error('postgres secret.host password');
  });
  await expect(c.request('double', 1)).rejects.toMatchObject({
    code: 'INTERNAL_ERROR',
    message: 'The operation failed.',
  });
});
it('times out, cancels, rejects pending on loss and ignores late responses', async () => {
  const f = await setup(),
    c = f.client();
  c.connect();
  await waitState(c, 'connected');
  await expect(c.request('wait', null, { timeout: 20 })).rejects.toMatchObject({ code: 'TIMEOUT' });
  const controller = new AbortController();
  const pending = c.request('double', 2, { signal: controller.signal });
  controller.abort();
  await expect(pending).rejects.toMatchObject({ code: 'CANCELLED' });
  const loss = c.request('double', 3);
  c.disconnect();
  await expect(loss).rejects.toMatchObject({ code: 'CONNECTION_LOST' });
  expect(c.state.pending).toBe(0);
});
it('rejects expired and missing sessions and hostile Origins at handshake', async () => {
  const f = await setup();
  f.identities.set('expired', { id: 'eve', org: 'a', expiresAt: 1 });
  for (const [session, origin, status] of [
    ['missing', 'http://app.test', 401],
    ['expired', 'http://app.test', 401],
    ['a', 'https://evil.test', 403],
  ] as const) {
    const ws = new WebSocket(f.url.replace('http', 'ws') + '/socket', ['vibesocket.v1'], {
      origin,
      headers: { cookie: `session=${session}` },
    });
    const response = await new Promise<number>((resolve) => {
      ws.on('unexpected-response', (_req, res) => {
        res.resume();
        resolve(res.statusCode!);
        ws.terminate();
      });
      ws.on('error', () => {});
    });
    expect(response).toBe(status);
  }
});
it('rechecks revoked permissions before private outbound delivery', async () => {
  const f = await setup(),
    c = f.client();
  const sub = c.subscribe('org:a'),
    ready = waitRoom(c, sub.id);
  c.connect();
  await ready;
  f.identities.delete('a');
  expect((await f.core.publish('org:a', 'changed', { value: 9 })).written).toBe(0);
  await waitState(c, 'failed');
  expect(f.core.connections).toHaveLength(0);
});
it('bounds rooms, flooding and malformed messages', async () => {
  const f = await setup({ maxRooms: 0, messagesPerSecond: 4 }),
    c = f.client();
  const sub = c.subscribe('org:a'),
    rejected = waitRoom(c, sub.id, 'rejected');
  c.connect();
  await rejected;
  expect(c.state.error?.code).toBe('RATE_LIMITED');
  for (let i = 0; i < 8; i++) c.rawSocket?.send('{"v":1,"type":"ping"}');
  await waitState(c, 'failed');
});
it('rejects oversized and protocol-mismatched actual WebSocket frames', async () => {
  const f = await setup({ maxMessageBytes: 256 });
  for (const [payload, code] of [
    ['x'.repeat(1024), 1009],
    ['{"v":9,"type":"ping"}', 4406],
    ['{"v":1,"type":"event","event":7,"payload":3}', 1008],
  ] as const) {
    const ws = new WebSocket(f.url.replace('http', 'ws') + '/socket', ['vibesocket.v1'], {
      origin: 'http://app.test',
      headers: { cookie: 'session=a' },
    });
    await once(ws, 'open');
    ws.send(payload);
    const [closed] = await once(ws, 'close');
    expect(closed).toBe(code);
  }
});
it('queues selected events, drops ephemeral events and never replays RPC', async () => {
  const f = await setup(),
    c = f.client();
  const seen = new Promise<void>((resolve) => f.core.on('notify', () => resolve()));
  await expect(c.send('notify', { text: 'reject' })).rejects.toMatchObject({
    code: 'CONNECTION_LOST',
  });
  expect((await c.send('notify', { text: 'typing' }, { disconnected: 'drop' })).stage).toBe(
    'dropped',
  );
  expect((await c.send('notify', { text: 'queue' }, { disconnected: 'queue' })).stage).toBe(
    'queued',
  );
  c.connect();
  await seen;
  expect(c.state.queueDepth).toBe(0);
});

it('cleans up a timed-out authentication callback and accepts the next upgrade', async () => {
  let calls = 0;
  const f = await fixture(
    [],
    {},
    {
      handshakeTimeout: 30,
      maxPendingUpgrades: 1,
      authenticate: async () => (++calls === 1 ? new Promise(() => {}) : { id: 'alice', org: 'a' }),
    },
  );
  cleanup.push(f.close);
  const ws = new WebSocket(f.url.replace('http', 'ws') + '/socket', ['vibesocket.v1'], {
    origin: 'http://app.test',
  });
  ws.on('error', () => {});
  await new Promise((resolve) => ws.once('close', resolve));
  const c = f.client();
  c.connect();
  await waitState(c, 'connected');
  expect(calls).toBe(2);
});
it('expires abandoned protocol negotiations and passive idle leases', async () => {
  for (const [config, hello, expected] of [
    [{ handshakeTimeout: 30 }, false, 4406],
    [{ idleTimeout: 30 }, true, 4000],
  ] as const) {
    const f = await fixture([], {}, config);
    cleanup.push(f.close);
    const ws = new WebSocket(f.url.replace('http', 'ws') + '/socket', ['vibesocket.v1'], {
      origin: 'http://app.test',
      headers: { cookie: 'session=a' },
    });
    await once(ws, 'open');
    if (hello) ws.send('{"v":1,"type":"hello","contract":1}');
    const [code] = await once(ws, 'close');
    expect(code).toBe(expected);
    expect(f.core.connections).toHaveLength(0);
  }
});
it('rejects a mismatched contract version without retrying', async () => {
  const f = await setup(),
    c = f.client('a', { contract: { ...f.core.contract, version: 2 } });
  c.connect();
  await waitState(c, 'failed');
  expect(c.state.error?.code).toBe('PROTOCOL_MISMATCH');
  expect(c.state.attempts).toBe(0);
});

it('closes live sockets idempotently while leaving the externally owned HTTP listener working', async () => {
  const f = await setup(),
    c = f.client('a', { reconnect: false });
  c.connect();
  await waitState(c, 'connected');
  await f.adapter.close();
  await waitState(c, 'failed');
  expect(f.core.connections).toHaveLength(0);
  expect((await fetch(f.url)).status).toBe(200);
  await f.adapter.close();
});
