import { afterEach, expect, it, vi } from 'vitest';
import * as v from 'valibot';
import { createSSEChannel, createSSEStream, formatSSE } from '../src/sse.ts';
import { fixture } from './fixture.ts';
const closes: (() => Promise<void>)[] = [];
afterEach(async () => {
  vi.useRealTimers();
  for (const close of closes.splice(0)) await close();
});
const decoder = new TextDecoder();
async function readUntil(reader: ReadableStreamDefaultReader<Uint8Array>, marker: string) {
  let text = '';
  while (!text.includes(marker)) {
    const next = await reader.read();
    if (next.done) throw new Error(`Stream closed before ${marker}: ${text}`);
    text += decoder.decode(next.value);
  }
  return text;
}
it('formats UTF-8, multiline data, retry hints and safe fields', () => {
  expect(formatSSE({ event: 'message', id: '2', data: 'hello\r\n世界\rthird', retry: 2000 })).toBe(
    'event: message\nid: 2\nretry: 2000\ndata: hello\ndata: 世界\ndata: third\n\n',
  );
  expect(() => formatSSE({ event: 'forged\ndata: x', data: 'ok' })).toThrow();
  expect(() => formatSSE({ retry: -1, data: '' })).toThrow();
});
it('stops keepalives on cancellation and closes on slow consumers', async () => {
  vi.useFakeTimers();
  const closed = vi.fn(),
    stream = createSSEStream({ keepalive: 100, onClose: closed, maxBufferedBytes: 32 });
  const reader = stream.response.body!.getReader();
  await vi.advanceTimersByTimeAsync(100);
  expect(decoder.decode((await reader.read()).value)).toBe(': keepalive\n\n');
  await reader.cancel();
  await vi.advanceTimersByTimeAsync(500);
  expect(closed).toHaveBeenCalledOnce();
  expect(stream.send({ data: 'late' })).toBe(false);
  const slow = createSSEStream({ keepalive: false, maxBufferedBytes: 20 });
  expect(slow.send({ data: 'x'.repeat(30) })).toBe(false);
  expect(slow.closed).toBe(true);
});
it('propagates request abort exactly once', async () => {
  const controller = new AbortController(),
    close = vi.fn();
  const stream = createSSEStream({ signal: controller.signal, onClose: close });
  controller.abort();
  expect(stream.closed).toBe(true);
  expect(close).toHaveBeenCalledOnce();
  expect((await stream.response.body!.getReader().read()).done).toBe(true);
});
it('streams multiple real HTTP chunks through the actual Vibe.js middleware pipeline', async () => {
  const channel = createSSEChannel({ metric: v.number() }, { keepalive: false });
  const f = await fixture([
    async (req, next) =>
      new URL(req.url).pathname === '/events'
        ? channel.response(req, { allowed: () => true })
        : next(),
  ]);
  closes.push(async () => {
    channel.close();
    await f.close();
  });
  const abort = new AbortController(),
    response = await fetch(f.url + '/events', { signal: abort.signal });
  expect(response.headers.get('content-type')).toContain('text/event-stream');
  expect(response.headers.has('x-request-id')).toBe(true);
  const reader = response.body!.getReader();
  expect(await readUntil(reader, 'VibeSocket')).toContain(': VibeSocket stream');
  await channel.publish('metric', 1, '1');
  expect(await readUntil(reader, 'data: 1')).toContain('event: metric\nid: 1');
  await channel.publish('metric', 2, '2');
  expect(await readUntil(reader, 'data: 2')).toContain('id: 2');
  abort.abort();
  await vi.waitFor(() => expect(channel.size).toBe(0));
});
it('replays after Last-Event-ID, buffers concurrent publications, and reports gaps', async () => {
  let resolveReplay!: (value: {
    events: { data: string; id: string; event: string }[];
    gap: boolean;
  }) => void;
  const channel = createSSEChannel(
    { metric: v.number() },
    {
      keepalive: false,
      replay: {
        readAfter: async (cursor) => {
          expect(cursor).toBe('1');
          return new Promise((resolve) => {
            resolveReplay = resolve;
          });
        },
      },
    },
  );
  const response = await channel.response(
    new Request('http://test/events', { headers: { 'last-event-id': '1' } }),
    { allowed: () => true },
  );
  await channel.publish('metric', 3, '3');
  resolveReplay({
    events: [
      { data: '2', id: '2', event: 'metric' },
      { data: '3', id: '3', event: 'metric' },
    ],
    gap: false,
  });
  const reader = response.body!.getReader(),
    text = await readUntil(reader, 'data: 3');
  expect(text).toContain('data: 2');
  expect(text.match(/id: 3/g)).toHaveLength(1);
  await reader.cancel();
  const lossy = createSSEChannel({ metric: v.number() }, { keepalive: false });
  const gap = await lossy.response(
    new Request('http://test', { headers: { 'last-event-id': 'missing' } }),
    { allowed: () => true },
  );
  const r = gap.body!.getReader();
  expect(await readUntil(r, 'REPLAY_UNAVAILABLE')).toContain('vibesocket:gap');
  await r.cancel();
});
it('isolates concurrent SSE subscribers and rechecks access before publication', async () => {
  const channel = createSSEChannel({ metric: v.number() }, { keepalive: false });
  let allowed = true;
  const a = await channel.response(new Request('http://test'), { allowed: () => allowed }),
    b = await channel.response(new Request('http://test'), { allowed: () => true });
  const ar = a.body!.getReader(),
    br = b.body!.getReader();
  await ar.read();
  await br.read();
  allowed = false;
  expect((await channel.publish('metric', 7)).written).toBe(1);
  expect((await ar.read()).done).toBe(true);
  expect(await readUntil(br, 'data: 7')).toContain('data: 7');
  await br.cancel();
  expect(channel.size).toBe(0);
});

it('bounds subscribers and releases trusted identity capacity on abort', async () => {
  const channel = createSSEChannel(
    { metric: v.number() },
    { keepalive: false, maxSubscribers: 2, maxSubscribersPerKey: 1 },
  );
  const access = { key: 'alice', allowed: () => true };
  const first = await channel.response(new Request('http://test'), access);
  expect((await channel.response(new Request('http://test'), access)).status).toBe(429);
  const alreadyAborted = new AbortController();
  alreadyAborted.abort();
  await channel.response(new Request('http://test', { signal: alreadyAborted.signal }), {
    key: 'bob',
    allowed: () => true,
  });
  expect(channel.size).toBe(1);
  await first.body!.cancel();
  expect((await channel.response(new Request('http://test'), access)).status).toBe(200);
  channel.close();
  expect((await channel.response(new Request('http://test'), access)).status).toBe(503);
});
