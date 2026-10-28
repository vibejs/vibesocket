import type { EventMap, Input, Output } from './contract.ts';
import { lookup, validate } from './contract.ts';
import { SocketError } from './errors.ts';
import { bytes } from './protocol.ts';
import { checkLimits } from './core/limits.ts';

export interface SSEEvent {
  data: string;
  event?: string;
  id?: string;
  retry?: number;
}
const field = (value: string) => {
  if (/[\r\n\0]/.test(value)) throw new TypeError('SSE fields cannot contain CR, LF or NUL.');
  return value;
};
export function formatSSE(message: SSEEvent): string {
  const lines: string[] = [];
  if (message.event !== undefined) lines.push(`event: ${field(message.event)}`);
  if (message.id !== undefined) lines.push(`id: ${field(message.id)}`);
  if (message.retry !== undefined) {
    if (!Number.isSafeInteger(message.retry) || message.retry < 0)
      throw new TypeError('SSE retry must be a nonnegative integer.');
    lines.push(`retry: ${message.retry}`);
  }
  for (const line of message.data.replaceAll('\r\n', '\n').replaceAll('\r', '\n').split('\n'))
    lines.push(`data: ${line}`);
  return `${lines.join('\n')}\n\n`;
}
export interface SSEStreamOptions {
  signal?: AbortSignal;
  maxBufferedBytes?: number;
  keepalive?: number | false;
  onClose?: () => void;
}
export function createSSEStream(options: SSEStreamOptions = {}) {
  checkLimits(options, ['maxBufferedBytes'], false);
  if (options.keepalive !== false) checkLimits(options, ['keepalive'], false);
  let controller: ReadableStreamDefaultController<Uint8Array>,
    closed = false;
  let timer: ReturnType<typeof setInterval> | undefined;
  const encoder = new TextEncoder(),
    max = options.maxBufferedBytes ?? 262144;
  const abort = () => close();
  function cleanup() {
    if (closed) return;
    closed = true;
    clearInterval(timer);
    options.signal?.removeEventListener('abort', abort);
    try {
      options.onClose?.();
    } catch {
      /* Cleanup isolation. */
    }
  }
  function close() {
    if (closed) return;
    cleanup();
    controller.close();
  }
  function write(text: string): boolean {
    if (closed) return false;
    const data = encoder.encode(text);
    if (data.byteLength > (controller.desiredSize ?? 0)) {
      close();
      return false;
    }
    controller.enqueue(data);
    return true;
  }
  const stream = new ReadableStream<Uint8Array>(
    {
      start(c) {
        controller = c;
      },
      cancel() {
        cleanup();
      },
    },
    new ByteLengthQueuingStrategy({ highWaterMark: max }),
  );
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) close();
  if (!closed && options.keepalive !== false) {
    timer = setInterval(() => write(': keepalive\n\n'), options.keepalive ?? 15000);
    timer.unref?.();
  }
  return {
    response: new Response(stream, {
      headers: {
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-cache, no-transform',
        'x-accel-buffering': 'no',
      },
    }),
    get closed() {
      return closed;
    },
    get bufferedBytes() {
      return max - (controller.desiredSize ?? 0);
    },
    send(event: SSEEvent) {
      return write(formatSSE(event));
    },
    comment(text: string) {
      return write(`: ${field(text)}\n\n`);
    },
    close,
  };
}
export interface ReplayResult {
  events: readonly SSEEvent[];
  gap: boolean;
}
export interface ReplayProvider {
  readAfter(cursor: string, signal: AbortSignal): Promise<ReplayResult>;
}
/** Single-process fanout. Configure a replay provider backed by your own history. */
export function createSSEChannel<E extends EventMap>(
  schemas: E,
  options: {
    replay?: ReplayProvider;
    maxReplayEvents?: number;
    maxMessageBytes?: number;
    keepalive?: number | false;
    maxSubscribers?: number;
    maxSubscribersPerKey?: number;
  } = {},
) {
  checkLimits(options, ['maxMessageBytes', 'maxReplayEvents'], false);
  checkLimits(options, ['maxSubscribers', 'maxSubscribersPerKey']);
  const keys = new Map<string, number>();
  let closed = false;
  const subscribers = new Set<{
    send: (event: SSEEvent) => boolean;
    close: () => void;
    allowed: () => boolean | Promise<boolean>;
  }>();
  return {
    get size() {
      return subscribers.size;
    },
    async publish<K extends keyof E & string>(event: K, payload: Input<E[K]>, id?: string) {
      if (closed) throw new SocketError('SERVER_UNAVAILABLE');
      await validate(lookup(schemas, event), payload);
      if (id !== undefined) field(id);
      const message = { event, data: JSON.stringify(payload), id };
      if (bytes(message.data) > (options.maxMessageBytes ?? 65536))
        throw new SocketError('INVALID_PAYLOAD');
      const attempted = subscribers.size;
      let written = 0;
      for (const subscriber of [...subscribers]) {
        try {
          if (await subscriber.allowed()) {
            if (subscriber.send(message)) written++;
          } else subscriber.close();
        } catch {
          subscriber.close();
        }
      }
      return { attempted, written };
    },
    async response(
      request: Request,
      access: { allowed: () => boolean | Promise<boolean>; expiresAt?: number; key?: string },
    ) {
      if (closed) return new Response(null, { status: 503 });
      if (request.method !== 'GET')
        return new Response(null, { status: 405, headers: { allow: 'GET' } });
      if (
        !(await access.allowed()) ||
        (access.expiresAt !== undefined && access.expiresAt <= Date.now())
      )
        return new Response(null, { status: 401 });
      if (
        subscribers.size >= (options.maxSubscribers ?? 1000) ||
        (access.key && (keys.get(access.key) ?? 0) >= (options.maxSubscribersPerKey ?? 8))
      )
        return new Response(null, { status: 429 });
      const cursor = request.headers.get('last-event-id');
      if (cursor && (cursor.length > 256 || /[\r\n\0]/.test(cursor)))
        return new Response(null, { status: 400 });
      let replaying = !!cursor,
        bufferBytes = 0;
      const buffer: SSEEvent[] = [];
      const abort = new AbortController();
      let expiry: ReturnType<typeof setTimeout> | undefined;
      const entry = {
        allowed: access.allowed,
        close: () => stream.close(),
        send(event: SSEEvent) {
          if (replaying) {
            bufferBytes += bytes(formatSSE(event));
            if (bufferBytes > 262144 || buffer.length >= (options.maxReplayEvents ?? 1000)) {
              stream.close();
              return false;
            }
            buffer.push(event);
            return true;
          }
          return stream.send(event);
        },
      };
      const stream = createSSEStream({
        signal: request.signal,
        keepalive: options.keepalive,
        onClose() {
          abort.abort();
          clearTimeout(expiry);
          const removed = subscribers.delete(entry);
          buffer.length = 0;
          if (access.key && removed) {
            const count = (keys.get(access.key) ?? 1) - 1;
            if (count) keys.set(access.key, count);
            else keys.delete(access.key);
          }
        },
      });
      if (stream.closed) return stream.response;
      subscribers.add(entry);
      if (access.key) keys.set(access.key, (keys.get(access.key) ?? 0) + 1);
      if (access.expiresAt !== undefined) {
        const schedule = () => {
          expiry = setTimeout(
            () => {
              if (access.expiresAt! <= Date.now()) stream.close();
              else schedule();
            },
            Math.min(2147483647, Math.max(0, access.expiresAt! - Date.now())),
          );
          expiry.unref?.();
        };
        schedule();
      }
      stream.comment('VibeSocket stream');
      if (cursor) {
        void (async () => {
          const replay = options.replay
            ? await options.replay.readAfter(cursor, abort.signal)
            : { events: [], gap: true };
          if (replay.events.length > (options.maxReplayEvents ?? 1000))
            throw new SocketError('REPLAY_UNAVAILABLE');
          const ids = new Set<string>();
          if (replay.gap)
            stream.send({
              event: 'vibesocket:gap',
              data: JSON.stringify({ code: 'REPLAY_UNAVAILABLE' }),
            });
          for (const event of replay.events) {
            if (!(await access.allowed())) {
              stream.close();
              return;
            }
            if (!event.event || bytes(event.data) > (options.maxMessageBytes ?? 65536))
              throw new SocketError('INVALID_PAYLOAD');
            await validate(lookup(schemas, event.event), JSON.parse(event.data));
            if (event.id) ids.add(event.id);
            if (!stream.send(event)) break;
          }
          replaying = false;
          for (const event of buffer) if (!event.id || !ids.has(event.id)) stream.send(event);
          buffer.length = 0;
        })().catch(() => {
          stream.send({ event: 'vibesocket:gap', data: '{"code":"REPLAY_UNAVAILABLE"}' });
          stream.close();
        });
      }
      return stream.response;
    },
    close() {
      closed = true;
      for (const entry of [...subscribers]) entry.close();
    },
  };
}

export function createEventStream<E extends EventMap>(options: {
  url: string;
  events: E;
  withCredentials?: boolean;
  eventSource?: (url: string, init: EventSourceInit) => EventSource;
  maxMessageBytes?: number;
  maxInbox?: number;
}) {
  let source: EventSource | undefined,
    generation = 0;
  let disposed = false;
  let state: {
    status: 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'closed' | 'failed';
    lastEventId: string;
    error: SocketError | null;
  } = { status: 'idle', lastEventId: '', error: null };
  const observers = new Set<(value: typeof state) => void>();
  const listeners = new Map<string, Set<(payload: never, id: string) => void>>();
  const update = (patch: Partial<typeof state>) => {
    state = { ...state, ...patch };
    for (const fn of observers) {
      try {
        fn(state);
      } catch {
        /* Consumer isolation. */
      }
    }
  };
  const api = {
    get state() {
      return state;
    },
    get rawEventSource() {
      return source;
    },
    observe(fn: (value: typeof state) => void) {
      observers.add(fn);
      fn(state);
      return () => {
        observers.delete(fn);
      };
    },
    on<K extends keyof E & string>(event: K, fn: (payload: Output<E[K]>, id: string) => void) {
      let set = listeners.get(event);
      if (!set) listeners.set(event, (set = new Set()));
      set.add(fn as never);
      return () => {
        set.delete(fn as never);
      };
    },
    connect() {
      if (source || disposed) return;
      const epoch = ++generation;
      update({ status: 'connecting' });
      const es = (options.eventSource ?? ((url, init) => new EventSource(url, init)))(options.url, {
        withCredentials: options.withCredentials ?? false,
      });
      source = es;
      es.onopen = () => {
        if (epoch === generation) update({ status: 'connected', error: null });
      };
      es.onerror = () => {
        if (epoch === generation)
          update({
            status: es.readyState === 2 ? 'failed' : 'reconnecting',
            error: new SocketError('CONNECTION_LOST'),
          });
      };
      let tail = Promise.resolve(),
        inbox = 0;
      for (const event of Object.keys(options.events))
        es.addEventListener(event, (raw) => {
          if (epoch !== generation) return;
          const message = raw as MessageEvent<string>;
          if (
            ++inbox > (options.maxInbox ?? 256) ||
            bytes(message.data) > (options.maxMessageBytes ?? 65536)
          ) {
            api.close();
            update({ status: 'failed', error: new SocketError('QUEUE_OVERFLOW') });
            return;
          }
          tail = tail
            .then(async () => {
              if (epoch !== generation) return;
              const payload = await validate(options.events[event], JSON.parse(message.data));
              if (epoch !== generation) return;
              update({ lastEventId: message.lastEventId });
              for (const fn of [...(listeners.get(event) ?? [])]) {
                try {
                  fn(payload as never, message.lastEventId);
                } catch {
                  /* Consumer isolation. */
                }
              }
            })
            .catch(() => {
              if (epoch === generation) update({ error: new SocketError('INVALID_PAYLOAD') });
            })
            .finally(() => {
              inbox--;
            });
        });
      es.addEventListener('vibesocket:gap', () => {
        if (epoch === generation) update({ error: new SocketError('REPLAY_UNAVAILABLE') });
      });
    },
    close() {
      ++generation;
      source?.close();
      source = undefined;
      update({ status: 'closed' });
    },
    dispose() {
      disposed = true;
      api.close();
      observers.clear();
      listeners.clear();
    },
  };
  return api;
}
