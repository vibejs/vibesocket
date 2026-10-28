import { createServer } from 'node:http';
import { once } from 'node:events';
import { cpus, totalmem, platform, release, arch } from 'node:os';
import { mkdir, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket, { createWebSocketStream } from 'ws';
import * as v from 'valibot';
import { createSocketServer } from '../src/server.ts';
import { attachNodeSocket } from '../src/node.ts';
import { defineSocketContract } from '../src/contract.ts';
import { createSocketClient } from '../src/index.ts';

const contract = defineSocketContract({
  server: { sample: v.number(), bulk: v.string() },
  requests: { echo: { input: v.number(), output: v.number() } },
});
const core = createSocketServer({
  contract,
  authorize: (_p, op) => !op.room || op.room === 'bench',
  messagesPerSecond: 1000,
  maxConnections: 6000,
  maxInbox: 64,
});
core.handle('echo', (input) => input);
const http = createServer((_req, res) => res.end('local benchmark'));
const adapter = attachNodeSocket(http, core, {
  allowedOrigins: ['http://benchmark.test'],
  maxConnectionsPerSource: 6000,
  attemptsPerMinute: 20000,
  authenticate: (req) => ({ id: req.headers.get('x-benchmark-id') ?? crypto.randomUUID() }),
});
http.listen(0, '127.0.0.1');
await once(http, 'listening');
const address = http.address();
if (!address || typeof address === 'string') throw new Error('No listener');
const url = `ws://127.0.0.1:${address.port}/socket`;
function deadline<T>(promise: Promise<T>, ms = 30000) {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([
    promise,
    new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error('Benchmark deadline exceeded')), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}
async function connect(index: string) {
  const socket = new WebSocket(url, ['vibesocket.v1'], {
    origin: 'http://benchmark.test',
    headers: { 'x-benchmark-id': index },
  });
  let serial = 0;
  const waiting = new Map<
    string,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  >();
  const callbacks = new Set<(value: number) => void>();
  let ready!: () => void, fail!: (error: Error) => void;
  const connected = new Promise<void>((resolve, reject) => {
    ready = resolve;
    fail = reject;
  });
  socket.on('error', fail);
  socket.on('close', () => {
    for (const entry of waiting.values()) entry.reject(new Error('Connection lost'));
  });
  socket.on('message', (raw) => {
    const frame = JSON.parse(String(raw)) as {
      type: string;
      id: string;
      payload: number;
      code?: string;
    };
    if (frame.type === 'hello') {
      socket.send('{"v":1,"type":"hello","contract":1}');
      ready();
    } else if (frame.type === 'result' || frame.type === 'error') {
      const entry = waiting.get(frame.id);
      if (entry) {
        waiting.delete(frame.id);
        if (frame.type === 'result') entry.resolve(frame.payload);
        else entry.reject(new Error(frame.code));
      }
    } else if (frame.type === 'event') for (const callback of callbacks) callback(frame.payload);
  });
  await connected;
  const command = (frame: object) =>
    new Promise<unknown>((resolve, reject) => {
      const id = String(++serial);
      waiting.set(id, { resolve, reject });
      socket.send(JSON.stringify({ v: 1, ...frame, id }));
    });
  return { socket, callbacks, command };
}
const scenarios: Record<string, unknown>[] = [];
const percentile = (values: number[], p: number) =>
  Number(values[Math.min(values.length - 1, Math.floor(values.length * p))].toFixed(3));
try {
  for (const count of [100, 1000, 5000]) {
    const initialRSS = process.memoryUsage().rss,
      opened = performance.now();
    const clients: Awaited<ReturnType<typeof connect>>[] = [];
    for (let start = 0; start < count; start += 100)
      clients.push(
        ...(await deadline(
          Promise.all(
            Array.from({ length: Math.min(100, count - start) }, (_, i) =>
              connect(`${count}:${start + i}`),
            ),
          ),
        )),
      );
    const connectMs = performance.now() - opened;
    await deadline(Promise.all(clients.map((c) => c.command({ type: 'join', room: 'bench' }))));
    const latencies: number[] = [],
      cpu = process.cpuUsage(),
      start = performance.now();
    const iterations = count === 5000 ? 2 : 10,
      requests: Promise<void>[] = [];
    let maxBufferedBytes = 0,
      peakRSS = process.memoryUsage().rss;
    const sample = setInterval(() => {
      maxBufferedBytes = Math.max(maxBufferedBytes, core.stats.bufferedBytes);
      peakRSS = Math.max(peakRSS, process.memoryUsage().rss);
    }, 10);
    for (let round = 0; round < iterations; round++) {
      for (const c of clients) {
        const sent = performance.now();
        requests.push(
          c.command({ type: 'request', event: 'echo', payload: sent }).then(() => {
            latencies.push(performance.now() - sent);
          }),
        );
      }
      // The 1000-connection scenario offers 10,000 RPC requests/sec for one second.
      if (count === 1000) await delay(100);
    }
    await deadline(Promise.all(requests));
    clearInterval(sample);
    const elapsed = performance.now() - start,
      usage = process.cpuUsage(cpu);
    latencies.sort((a, b) => a - b);
    const broadcastAt = performance.now();
    const broadcasts = clients.map(
      (c) =>
        new Promise<number>((resolve) => {
          const callback = () => {
            c.callbacks.delete(callback);
            resolve(performance.now() - broadcastAt);
          };
          c.callbacks.add(callback);
        }),
    );
    const written = await core.publish('bench', 'sample', broadcastAt);
    const delivery = (await deadline(Promise.all(broadcasts))).sort((a, b) => a - b);
    // Bounded subscription churn exercises removal/rejoin with actual command acknowledgments.
    await deadline(
      Promise.all(
        clients.slice(0, 100).map(async (c) => {
          for (let i = 0; i < 5; i++) {
            await c.command({ type: 'leave', room: 'bench' });
            await c.command({ type: 'join', room: 'bench' });
          }
        }),
      ),
    );
    const result = {
      connections: count,
      successfulConnections: clients.length,
      connectMs: Number(connectMs.toFixed(2)),
      rpcRequests: latencies.length,
      offeredRPCPerSecond: count === 1000 ? 10000 : 'burst',
      elapsedMs: Number(elapsed.toFixed(2)),
      completedRPCPerSecond: Math.round((latencies.length * 1000) / elapsed),
      latencyMs: {
        p50: percentile(latencies, 0.5),
        p95: percentile(latencies, 0.95),
        p99: percentile(latencies, 0.99),
      },
      cpuUserMs: usage.user / 1000,
      cpuSystemMs: usage.system / 1000,
      peakRSSMiB: Number((peakRSS / 1048576).toFixed(2)),
      initialRSSMiB: Number((initialRSS / 1048576).toFixed(2)),
      maxObservedBufferedBytes: maxBufferedBytes,
      broadcast: { ...written, clientP95Ms: percentile(delivery, 0.95) },
      churnCommands: Math.min(count, 100) * 10,
      errors: 0,
    };
    scenarios.push(result);
    console.log(JSON.stringify(result));
    await deadline(
      Promise.all(
        clients.map((c) => {
          const closed = once(c.socket, 'close');
          c.socket.close(1000);
          return closed;
        }),
      ),
    );
  }
  const retries: number[] = [],
    storm = Array.from({ length: 100 }, () =>
      createSocketClient({
        contract,
        url,
        heartbeat: false,
        lifecycle: false,
        reconnect: { minDelay: 100, maxDelay: 500 },
        webSocket: (target, protocols) =>
          new WebSocket(target, protocols, {
            origin: 'http://benchmark.test',
          }) as unknown as globalThis.WebSocket,
        diagnostic: (event, fields) => {
          if (event === 'connection.retry') retries.push(Number(fields.delay));
        },
      }),
    );
  const connected = (client: (typeof storm)[number]) =>
    client.state.status === 'connected'
      ? Promise.resolve()
      : new Promise<void>((resolve) => {
          const off = client.observe((s) => {
            if (s.status === 'connected') {
              off();
              resolve();
            }
          });
        });
  for (const client of storm) client.connect();
  await deadline(Promise.all(storm.map(connected)));
  const reconnected = storm.map(
    (client) =>
      new Promise<void>((resolve) => {
        let lost = false;
        const off = client.observe((s) => {
          if (s.status === 'reconnecting') lost = true;
          if (lost && s.status === 'connected') {
            off();
            resolve();
          }
        });
      }),
  );
  const stormAt = performance.now();
  for (const connection of core.connections)
    core.close(connection.id, 4000, 'Local reconnect storm');
  await deadline(Promise.all(reconnected));
  const reconnectStorm = {
    connections: 100,
    attempts: retries.length,
    elapsedMs: performance.now() - stormAt,
    minScheduledDelayMs: Math.min(...retries),
    maxScheduledDelayMs: Math.max(...retries),
    activeAfter: core.connections.length,
  };
  for (const client of storm) client.dispose();
  const slow = await connect('slow');
  await slow.command({ type: 'join', room: 'bench' });
  const unread = createWebSocketStream(slow.socket, { highWaterMark: 1 });
  unread.on('error', () => {});
  let sent = 0,
    rejected = false;
  for (; sent < 2000; sent++) {
    const result = await core.publish('bench', 'bulk', 'x'.repeat(8192));
    if (!result.written) {
      rejected = true;
      break;
    }
    if (sent % 16 === 0) await new Promise<void>((resolve) => setImmediate(resolve));
  }
  unread.destroy();
  slow.socket.terminate();
  const report = {
    recordedAt: new Date().toISOString(),
    runtime: process.version,
    hardware: {
      cpu: cpus()[0].model,
      cores: cpus().length,
      memoryGiB: Math.round(totalmem() / 1073741824),
      os: `${platform()} ${release()} ${arch()}`,
    },
    configuration: {
      transport: 'ws 8.22.0',
      sameProcessClientAndServer: true,
      tls: false,
      loopbackOnly: true,
      descriptorLimit: 16384,
      compression: false,
      persistence: false,
      payload: 'timestamp number RPC; 8KiB slow-consumer event',
    },
    scenarios,
    reconnectStorm,
    slowConsumer: {
      sentFramesBeforeRejection: sent,
      closedOnBufferOverflow: rejected,
      configuredBufferLimitBytes: 262144,
    },
    cleanup: { activeBeforeShutdown: core.connections.length },
  };
  await mkdir('reports', { recursive: true });
  await writeFile('reports/benchmark.json', JSON.stringify(report, null, 2) + '\n');
  if (!rejected) throw new Error('Slow consumer buffer did not become bounded during this run');
} finally {
  await adapter.close();
  await new Promise<void>((resolve) => http.close(() => resolve()));
}
