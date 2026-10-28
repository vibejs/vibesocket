import { afterEach, expect, it, vi } from 'vitest';
import * as v from 'valibot';
import { effectScope, ref } from '@usevibejs/ui';
import { createSocketClient } from '../src/index.ts';
import { useSocketEvent, useSocketRoom } from '../src/client.ts';
import { createSocketServer } from '../src/server.ts';
import {
  createMemoryPubSub,
  connectPubSub,
  createTypingTracker,
  presenceSnapshot,
  syncSocketQuery,
  recoverState,
} from '../src/adapters.ts';
import { defineSocketContract } from '../src/contract.ts';
import { contract, fixture, waitRoom, waitState } from './fixture.ts';
const cleanup: (() => Promise<void> | void)[] = [];
afterEach(async () => {
  vi.useRealTimers();
  for (const close of cleanup.splice(0)) await close();
});
class FakeSocket extends EventTarget {
  readyState = 1;
  bufferedAmount = 0;
  sent: string[] = [];
  protocol = 'vibesocket.v1';
  send(value: string) {
    this.sent.push(value);
  }
  close(code = 1000) {
    this.readyState = 3;
    this.dispatchEvent(Object.assign(new Event('close'), { code }));
  }
  message(frame: object) {
    this.dispatchEvent(
      Object.assign(new Event('message'), { data: JSON.stringify({ v: 1, ...frame }) }),
    );
  }
}
const settle = async () => {
  for (let i = 0; i < 15; i++) await Promise.resolve();
};
it('invalidates stale async connection factories and never reopens disposed clients', async () => {
  let resolve!: (url: string) => void;
  const factory = vi.fn();
  const c = createSocketClient({
    contract,
    url: () =>
      new Promise((r) => {
        resolve = r;
      }),
    webSocket: factory,
    heartbeat: false,
    lifecycle: false,
  });
  c.connect();
  c.dispose();
  resolve('ws://test');
  await settle();
  expect(factory).not.toHaveBeenCalled();
  expect(c.state.status).toBe('closed');
  c.connect();
  expect(c.state.status).toBe('closed');
});
it('bounds retries deterministically and clears heartbeat deadlines on shutdown', async () => {
  vi.useFakeTimers();
  const sockets: FakeSocket[] = [];
  const c = createSocketClient({
    contract,
    url: 'ws://test',
    webSocket: () => {
      const ws = new FakeSocket();
      sockets.push(ws);
      return ws as unknown as WebSocket;
    },
    reconnect: { minDelay: 50, maxDelay: 50, maxAttempts: 2 },
    heartbeat: { interval: 100, timeout: 30 },
    lifecycle: false,
  });
  cleanup.push(() => c.dispose());
  c.connect();
  sockets[0].message({ type: 'hello', contract: 1 });
  await settle();
  expect(c.state.status).toBe('connected');
  await vi.advanceTimersByTimeAsync(100);
  expect(sockets[0].sent.at(-1)).toContain('ping');
  sockets[0].message({ type: 'pong' });
  await settle();
  expect(c.state.latency).toBe(0);
  sockets[0].close(1006);
  await vi.advanceTimersByTimeAsync(50);
  expect(sockets).toHaveLength(2);
  sockets[1].close(1006);
  await vi.advanceTimersByTimeAsync(50);
  sockets[2].close(1006);
  expect(c.state.status).toBe('failed');
  expect(c.state.attempts).toBe(2);
  c.dispose();
  expect(vi.getTimerCount()).toBe(0);
});
it('scope disposal releases events and reactive room ownership before stale events can apply', async () => {
  const f = await fixture();
  cleanup.push(f.close);
  const c = f.client(),
    scope = effectScope(),
    room = ref('org:a'),
    received = vi.fn();
  scope.run(() => {
    useSocketRoom(c, room);
    useSocketEvent(c, 'changed', received);
  });
  const joined = new Promise<void>((resolve) => {
    const off = c.observeSubscriptions((s) => {
      if (s.status === 'active') {
        off();
        resolve();
      }
    });
  });
  c.connect();
  await joined;
  expect(f.core.roomCount).toBe(1);
  scope.stop();
  await c.request('double', 1);
  expect(f.core.roomCount).toBe(0);
  await f.core.publish('org:a', 'changed', { value: 1 });
  expect(received).not.toHaveBeenCalled();
});
it('query resynchronization runs once per reconnect, never for pending-count updates', async () => {
  const f = await fixture();
  cleanup.push(f.close);
  const c = f.client(),
    resync = vi.fn();
  cleanup.push(syncSocketQuery(c, 'changed', () => {}, resync));
  c.connect();
  await waitState(c, 'connected');
  await c.request('double', 2);
  await c.request('double', 3);
  expect(resync).not.toHaveBeenCalled();
  c.reconnect();
  await waitState(c, 'connected');
  await c.request('double', 4);
  expect(resync).toHaveBeenCalledOnce();
});
it('aggregates multi-tab presence and expires typing on lease and disconnect', async () => {
  vi.useFakeTimers();
  const closed = new AbortController(),
    rooms = new Map([['org:a', 'sub']]);
  const connection = {
    id: 'one',
    principal: { id: 'alice' },
    rooms,
    signal: closed.signal,
    transport: 'websocket' as const,
    connectedAt: Date.now(),
    lastActivity: Date.now(),
  };
  expect(presenceSnapshot([connection, { ...connection, id: 'two' }])).toMatchObject([
    { userId: 'alice', connections: 2 },
  ]);
  expect(presenceSnapshot([connection], { now: Date.now() + 100000 })).toEqual([]);
  const change = vi.fn(),
    tracker = createTypingTracker({ ttl: 1000, onChange: change });
  cleanup.push(() => tracker.close());
  tracker.set(connection, 'org:a', true);
  tracker.set(connection, 'org:a', true);
  expect(change).toHaveBeenCalledOnce();
  await vi.advanceTimersByTimeAsync(1001);
  expect(tracker.snapshot('org:a')).toEqual([]);
  tracker.set(connection, 'org:a', true);
  closed.abort();
  expect(tracker.snapshot('org:a')).toEqual([]);
  expect(() => tracker.set(connection, 'org:b', true)).toThrow();
});
it('enforces slow-consumer drop and disconnect policies, with bounded queues', async () => {
  const core = createSocketServer({ contract, authorize: () => true, maxBufferedBytes: 2048 });
  let buffer = 0;
  const closed = vi.fn(),
    sent = vi.fn();
  const c = core.accept(
    {
      get bufferedAmount() {
        return buffer;
      },
      send: sent,
      close: closed,
    },
    { id: 'alice' },
  );
  core.receive(c.id, '{"v":1,"type":"hello","contract":1}');
  core.receive(c.id, '{"v":1,"type":"join","id":"room","room":"org:a"}');
  await core.drain(c.id);
  buffer = 2048;
  expect((await core.publish('org:a', 'changed', { value: 1 }, { overflow: 'drop' })).written).toBe(
    0,
  );
  expect(closed).not.toHaveBeenCalled();
  expect((await core.publish('org:a', 'changed', { value: 2 })).written).toBe(0);
  expect(closed).toHaveBeenCalledWith(4008, 'SLOW_CONSUMER');
  expect(core.connections).toHaveLength(0);
});
it('connects application-scoped brokers and retains recipient authorization', async () => {
  const a = await fixture(),
    b = await fixture();
  cleanup.push(a.close, b.close);
  const bus = createMemoryPubSub();
  cleanup.push(() => bus.close());
  const bridgeA = await connectPubSub(a.core, bus, 'app'),
    bridgeB = await connectPubSub(b.core, bus, 'app');
  cleanup.push(bridgeA.close, bridgeB.close);
  const c = b.client(),
    sub = c.subscribe('org:a'),
    joined = waitRoom(c, sub.id);
  c.connect();
  await joined;
  const received = new Promise<number>((resolve) => c.on('changed', (p) => resolve(p.value)));
  await bridgeA.publish('org:a', 'changed', { value: 42 });
  expect(await received).toBe(42);
});
it('falls back to a snapshot when retained replay has a gap', async () => {
  const apply = vi.fn();
  expect(
    await recoverState({
      cursor: 'old',
      replay: async () => ({ gap: true, changes: [], cursor: 'old' }),
      snapshot: async () => ({ state: 42, cursor: 'new' }),
      apply,
    }),
  ).toBe('new');
  expect(apply).toHaveBeenCalledWith(42, 'snapshot');
});
it('validates transforms at the receiving boundary while serializing original input', async () => {
  const transformed = defineSocketContract({
    client: { size: v.pipe(v.string(), v.transform(Number)) },
  });
  const ws = new FakeSocket(),
    client = createSocketClient({
      contract: transformed,
      url: 'ws://test',
      webSocket: () => ws as unknown as WebSocket,
      heartbeat: false,
      lifecycle: false,
    });
  cleanup.push(() => client.dispose());
  client.connect();
  ws.message({ type: 'hello', contract: 1 });
  await settle();
  await client.send('size', '42');
  expect(JSON.parse(ws.sent.at(-1)!).payload).toBe('42');
  const server = createSocketServer({ contract: transformed, authorize: () => true }),
    handler = vi.fn();
  server.on('size', handler);
  const connection = server.accept({ bufferedAmount: 0, send() {}, close() {} }, { id: 'one' });
  server.receive(connection.id, '{"v":1,"type":"hello","contract":1}');
  server.receive(connection.id, ws.sent.at(-1)!);
  await server.drain(connection.id);
  expect(handler.mock.calls[0][0]).toBe(42);
  server.shutdown();
});

it('notifies a revoked room and leaves the client subscription rejected', async () => {
  const f = await fixture();
  cleanup.push(f.close);
  const c = f.client(),
    subscription = c.subscribe('org:a'),
    joined = waitRoom(c, subscription.id);
  c.connect();
  await joined;
  const rejected = waitRoom(c, subscription.id, 'rejected');
  f.core.evict(f.core.connections[0].id, 'org:a');
  await rejected;
  expect(subscription.active).toBe(false);
  expect(subscription.state.error?.code).toBe('FORBIDDEN');
});
it('rejects invalid resource limits instead of allowing unbounded configuration', () => {
  expect(() => createSocketClient({ contract, url: 'ws://test', maxInbox: NaN })).toThrow(
    'finite integer',
  );
  expect(() => createSocketServer({ contract, authorize: () => true, maxRooms: Infinity })).toThrow(
    'finite integer',
  );
});
it('treats a ticket factory authentication failure as terminal', async () => {
  const { SocketError } = await import('../src/errors.ts');
  const c = createSocketClient({
    contract,
    url: async () => {
      throw new SocketError('AUTH_REQUIRED');
    },
    lifecycle: false,
  });
  cleanup.push(() => c.dispose());
  c.connect();
  await settle();
  expect(c.state.status).toBe('failed');
  expect(c.state.error?.code).toBe('AUTH_REQUIRED');
  expect(c.state.attempts).toBe(0);
});
it('skips canceled work queued behind a pending handler', async () => {
  const f = await fixture();
  cleanup.push(f.close);
  const c = f.client();
  c.connect();
  await waitState(c, 'connected');
  const first = c.request('wait', null, { timeout: 30 });
  const controller = new AbortController();
  const second = c.request('double', 1, { signal: controller.signal });
  await settle();
  controller.abort();
  await expect(second).rejects.toMatchObject({ code: 'CANCELLED' });
  await expect(first).rejects.toMatchObject({ code: 'TIMEOUT' });
  expect(await c.request('double', 2)).toBe(4);
});

it('bounds asynchronous schema processing and clears its watchdog on disconnect', async () => {
  vi.useFakeTimers();
  const schema = {
    '~standard': {
      version: 1 as const,
      vendor: 'hung',
      validate: async () => new Promise<{ value: unknown }>(() => {}),
    },
  };
  const c = defineSocketContract({ client: { hung: schema } }),
    transport = { bufferedAmount: 0, send: vi.fn(), close: vi.fn() };
  const server = createSocketServer({ contract: c, authorize: () => true, processingTimeout: 20 });
  const connection = server.accept(transport, { id: 'one' });
  server.receive(connection.id, '{"v":1,"type":"hello","contract":1}');
  await server.drain(connection.id);
  server.receive(connection.id, '{"v":1,"type":"event","event":"hung","payload":null}');
  await vi.advanceTimersByTimeAsync(21);
  expect(server.connections).toHaveLength(0);
  expect(transport.close).toHaveBeenCalledWith(1011, 'Processing deadline exceeded');
  expect(vi.getTimerCount()).toBe(0);
});
