import {
  lookup,
  validate,
  type SocketContract,
  type ClientEvent,
  type ServerEvent,
  type RequestName,
  type RequestError,
  type Input,
  type Output,
} from './contract.ts';
import { checkLimits } from './core/limits.ts';
import { assertTransition, type ConnectionStatus } from './core/state.ts';
import { SocketError, publicError, errorCodes, type ErrorCode } from './errors.ts';
import { SUBPROTOCOL, bytes, encodeFrame, parseFrame, type Frame } from './protocol.ts';
import { createOutboundQueue, type QueueOptions } from './core/queue.ts';
import { retryDelay, terminalClose, type RetryPolicy } from './core/retry.ts';
export { SocketError, errorCodes } from './errors.ts';
export type { ErrorCode } from './errors.ts';
export { defineSocketContract } from './contract.ts';
export type { SocketContract } from './contract.ts';
export { retryDelay } from './core/retry.ts';

export type { ConnectionStatus } from './core/state.ts';
export { connectionTransitions } from './core/state.ts';
export interface ClientState {
  status: ConnectionStatus;
  attempts: number;
  error: SocketError<string> | null;
  latency: number | null;
  transport: 'websocket';
  pending: number;
  queueDepth: number;
  queuedBytes: number;
}
export interface ClientOptions<C extends SocketContract> {
  contract: C;
  url: string | (() => string | Promise<string>);
  webSocket?: (url: string, protocols: string[]) => WebSocket;
  reconnect?: false | RetryPolicy;
  connectTimeout?: number;
  heartbeat?: false | { interval?: number; timeout?: number };
  maxMessageBytes?: number;
  maxBufferedBytes?: number;
  maxPendingRequests?: number;
  maxInbox?: number;
  queue?: QueueOptions;
  lifecycle?: false | { pauseWhenHidden?: boolean };
  diagnostic?: (event: string, fields: Readonly<Record<string, number | string>>) => void;
}
export interface SendOptions {
  disconnected?: 'reject' | 'queue' | 'drop';
  ttl?: number;
  coalesce?: string;
  onFailure?: (error: SocketError) => void;
}
export interface SubscriptionState {
  id: string;
  room: string;
  status: 'pending' | 'active' | 'rejected' | 'closed';
  error: SocketError<string> | null;
}
type Listener = (payload: never, metadata: { room?: string }) => void;
export function createSocketClient<C extends SocketContract>(options: ClientOptions<C>) {
  checkLimits(
    options,
    ['connectTimeout', 'maxMessageBytes', 'maxBufferedBytes', 'maxInbox'],
    false,
  );
  checkLimits(options, ['maxPendingRequests']);
  if (options.heartbeat) checkLimits(options.heartbeat, ['interval', 'timeout'], false);
  if (options.reconnect) {
    checkLimits(options.reconnect, ['minDelay', 'maxDelay', 'stableAfter'], false);
    checkLimits(options.reconnect, ['maxAttempts']);
  }
  let socket: WebSocket | undefined,
    generation = 0,
    intentional = false,
    disposed = false;
  let retryTimer: ReturnType<typeof setTimeout> | undefined,
    connectTimer: ReturnType<typeof setTimeout> | undefined;
  let connectedAt = 0,
    suspended = false;
  let opening = false;
  let heartbeatTimer: ReturnType<typeof setTimeout> | undefined,
    heartbeatDeadline: ReturnType<typeof setTimeout> | undefined;
  let heartbeatAt = 0,
    inbox = 0;
  const queue = createOutboundQueue(options.queue);
  const subscriptionObservers = new Set<(state: SubscriptionState) => void>();
  const listeners = new Map<string, Set<Listener>>(),
    observers = new Set<(state: Readonly<ClientState>) => void>();
  const pending = new Map<
    string,
    {
      resolve: (payload: unknown) => void;
      reject: (error: unknown) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  const subscriptions = new Map<
    string,
    { id: string; count: number; active: boolean; state: SubscriptionState }
  >();
  let state: ClientState = {
    status: 'idle',
    attempts: 0,
    error: null,
    latency: null,
    transport: 'websocket',
    pending: 0,
    queueDepth: 0,
    queuedBytes: 0,
  };
  const update = (patch: Partial<ClientState>) => {
    if (patch.status) assertTransition(state.status, patch.status);
    state = Object.freeze({ ...state, ...patch });
    for (const fn of [...observers]) {
      try {
        fn(state);
      } catch {
        /* Isolate consumers. */
      }
    }
  };
  function report(event: string, fields: Record<string, number | string> = {}) {
    try {
      options.diagnostic?.(event, fields);
    } catch {
      /* Diagnostics are isolated. */
    }
  }
  const syncQueue = () => update({ queueDepth: queue.size, queuedBytes: queue.bytes });
  function writeData(data: string) {
    if (!socket || socket.readyState !== 1) throw new SocketError('CONNECTION_LOST');
    if (bytes(data) > (options.maxMessageBytes ?? 65536)) throw new SocketError('INVALID_PAYLOAD');
    if (socket.bufferedAmount + bytes(data) > (options.maxBufferedBytes ?? 262144))
      throw new SocketError('SLOW_CONSUMER');
    socket.send(data);
  }
  const write = (frame: Frame) => writeData(encodeFrame(frame));
  function stopHeartbeat() {
    clearTimeout(heartbeatTimer);
    clearTimeout(heartbeatDeadline);
    heartbeatTimer = heartbeatDeadline = undefined;
  }
  const clear = () => {
    clearTimeout(retryTimer);
    clearTimeout(connectTimer);
    retryTimer = connectTimer = undefined;
    stopHeartbeat();
  };
  function scheduleHeartbeat() {
    if (options.heartbeat === false || state.status !== 'connected') return;
    heartbeatTimer = setTimeout(
      () => {
        if (state.status !== 'connected') return;
        heartbeatAt = Date.now();
        try {
          write({ v: 1, type: 'ping' });
        } catch {
          socket?.close(4000, 'Heartbeat write failed');
          return;
        }
        heartbeatDeadline = setTimeout(
          () => {
            const stale = socket;
            lost(4000);
            stale?.close(4000, 'Heartbeat timeout');
          },
          (options.heartbeat && options.heartbeat.timeout) || 10000,
        );
      },
      (options.heartbeat && options.heartbeat.interval) || 30000,
    );
  }
  function notifySubscription(
    room: string,
    status: SubscriptionState['status'],
    error: SocketError<string> | null = null,
  ) {
    const entry = subscriptions.get(room);
    if (!entry) return;
    entry.active = status === 'active';
    entry.state = { id: entry.id, room, status, error };
    for (const fn of [...subscriptionObservers]) {
      try {
        fn(entry.state);
      } catch {
        /* Isolate consumers. */
      }
    }
  }
  function rejectPending() {
    for (const p of pending.values()) {
      clearTimeout(p.timer);
      p.reject(new SocketError('CONNECTION_LOST'));
    }
    pending.clear();
    update({ pending: 0 });
  }
  function command(frame: Frame & { id: string }, timeout = 5000): Promise<unknown> {
    if (pending.size >= (options.maxPendingRequests ?? 64))
      return Promise.reject(new SocketError('RATE_LIMITED'));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(frame.id);
        update({ pending: pending.size });
        reject(new SocketError('TIMEOUT'));
        try {
          if (frame.type === 'request') write({ v: 1, type: 'cancel', id: frame.id });
          else if (frame.type === 'join')
            write({ v: 1, type: 'leave', id: crypto.randomUUID(), room: frame.room });
        } catch {
          /* Transport already lost. */
        }
      }, timeout);
      pending.set(frame.id, { resolve, reject, timer });
      update({ pending: pending.size });
      try {
        write(frame);
      } catch (error) {
        clearTimeout(timer);
        pending.delete(frame.id);
        update({ pending: pending.size });
        reject(error);
      }
    });
  }
  async function join(room: string, entry: { id: string; active: boolean }) {
    notifySubscription(room, 'pending');
    try {
      await command({ v: 1, type: 'join', id: entry.id, room });
      if (subscriptions.get(room) === entry) notifySubscription(room, 'active');
    } catch (error) {
      if (subscriptions.get(room) === entry) {
        const safe = publicError(error);
        notifySubscription(room, 'rejected', safe);
        update({ error: safe });
      }
    }
  }
  function lost(code: number) {
    ++generation;
    opening = false;
    clear();
    socket = undefined;
    rejectPending();
    for (const room of subscriptions.keys()) notifySubscription(room, 'pending');
    if (disposed || intentional) return;
    const policy = options.reconnect === false ? undefined : (options.reconnect ?? {});
    if (connectedAt && Date.now() - connectedAt >= (policy?.stableAfter ?? 30000))
      update({ attempts: 0 });
    connectedAt = 0;
    const retryable = !terminalClose(code);
    if (
      !policy ||
      !retryable ||
      policy.shouldRetry?.(code) === false ||
      state.attempts >= (policy.maxAttempts ?? 10)
    ) {
      update({
        status: 'failed',
        error: new SocketError(
          code === 4401
            ? 'AUTH_REQUIRED'
            : code === 4403
              ? 'FORBIDDEN'
              : code === 4406
                ? 'PROTOCOL_MISMATCH'
                : code === 4008
                  ? 'SLOW_CONSUMER'
                  : code === 1009 || code === 1008
                    ? 'INVALID_PAYLOAD'
                    : 'CONNECTION_LOST',
        ),
      });
      return;
    }
    const attempt = state.attempts + 1,
      delay = retryDelay(attempt, policy);
    update({ status: 'reconnecting', attempts: attempt });
    report('connection.retry', { attempt, delay, code });
    retryTimer = setTimeout(() => {
      void open(true);
    }, delay);
  }
  async function receive(frame: Frame, epoch: number) {
    if (epoch !== generation) return;
    if (frame.type === 'hello') {
      if (frame.contract !== options.contract.version || state.status === 'connected') {
        socket?.close(4406, 'PROTOCOL_MISMATCH');
        return;
      }
      clearTimeout(connectTimer);
      write({ v: 1, type: 'hello', contract: options.contract.version });
      connectedAt = Date.now();
      update({ status: 'connected', error: null });
      scheduleHeartbeat();
      void Promise.all([...subscriptions].map(([room, entry]) => join(room, entry))).then(() => {
        if (epoch !== generation) return;
        queue.flush((data) => {
          try {
            writeData(data);
            return true;
          } catch {
            return false;
          }
        });
        syncQueue();
      });
      return;
    }
    if (state.status !== 'connected') throw new SocketError('PROTOCOL_MISMATCH');
    if (frame.type === 'revoked') {
      if (subscriptions.get(frame.room)?.id === frame.subscription)
        notifySubscription(frame.room, 'rejected', new SocketError(frame.code));
      return;
    }
    if (frame.type === 'event') {
      if (
        frame.room &&
        (!subscriptions.get(frame.room)?.active ||
          subscriptions.get(frame.room)?.id !== frame.subscription)
      )
        return;
      const value = await validate(lookup(options.contract.server, frame.event), frame.payload);
      if (
        epoch !== generation ||
        (frame.room &&
          (!subscriptions.get(frame.room)?.active ||
            subscriptions.get(frame.room)?.id !== frame.subscription))
      )
        return;
      for (const fn of [...(listeners.get(frame.event) ?? [])]) {
        try {
          fn(value as never, { room: frame.room });
        } catch {
          /* One listener cannot disrupt others. */
        }
      }
      return;
    }
    if (frame.type === 'result' || frame.type === 'error') {
      const p = frame.id ? pending.get(frame.id) : undefined;
      if (p && frame.id) {
        pending.delete(frame.id);
        clearTimeout(p.timer);
        update({ pending: pending.size });
        if (frame.type === 'result') p.resolve(frame.payload);
        else p.reject(new SocketError(frame.code, frame.message));
      } else if (frame.type === 'error' && !frame.id)
        update({ error: new SocketError(frame.code, frame.message) });
      return;
    }
    if (frame.type === 'pong') {
      if (heartbeatDeadline) {
        clearTimeout(heartbeatDeadline);
        heartbeatDeadline = undefined;
        update({ latency: Date.now() - heartbeatAt });
        scheduleHeartbeat();
      }
      return;
    }
    throw new SocketError('PROTOCOL_MISMATCH');
  }
  async function open(retry = false) {
    if (disposed || intentional || socket || opening || (!retry && state.status === 'reconnecting'))
      return;
    clear();
    opening = true;
    const epoch = ++generation;
    update({ status: retry ? 'reconnecting' : 'connecting' });
    connectTimer = setTimeout(() => {
      if (epoch === generation) {
        socket?.close();
        lost(1006);
      }
    }, options.connectTimeout ?? 10000);
    try {
      const url = typeof options.url === 'function' ? await options.url() : options.url;
      if (epoch !== generation || intentional || disposed) return;
      const ws = (options.webSocket ?? ((target, protocols) => new WebSocket(target, protocols)))(
        url,
        [SUBPROTOCOL],
      );
      socket = ws;
      opening = false;
      let tail = Promise.resolve();
      ws.addEventListener('message', (e) => {
        if (epoch !== generation) return;
        if (++inbox > (options.maxInbox ?? 256)) {
          ws.close(1008, 'Incoming backlog exceeded');
          return;
        }
        tail = tail
          .then(async () => {
            if (epoch !== generation) return;
            if (typeof e.data !== 'string') throw new SocketError('INVALID_PAYLOAD');
            const frame = parseFrame(e.data, options.maxMessageBytes);
            await receive(frame, epoch);
            if (frame.receipt && epoch === generation)
              write({ v: 1, type: 'receipt', id: frame.receipt });
          })
          .catch((error) => {
            if (epoch === generation) update({ error: publicError(error) });
            ws.close(1008, 'INVALID_PAYLOAD');
          })
          .finally(() => {
            inbox--;
          });
      });
      ws.addEventListener('close', (e) => {
        if (epoch === generation) lost(e.code);
      });
      ws.addEventListener('error', () => {
        if (epoch === generation) update({ error: new SocketError('CONNECTION_LOST') });
      });
    } catch (error) {
      if (epoch === generation) {
        update({ error: publicError(error) });
        lost(
          error instanceof SocketError && error.code === 'AUTH_REQUIRED'
            ? 4401
            : error instanceof SocketError && error.code === 'FORBIDDEN'
              ? 4403
              : 1006,
        );
      }
    }
  }
  const api = {
    contract: options.contract,
    get state(): Readonly<ClientState> {
      return state;
    },
    get rawSocket() {
      return socket;
    },
    observe(fn: (state: Readonly<ClientState>) => void) {
      observers.add(fn);
      fn(state);
      return () => {
        observers.delete(fn);
      };
    },
    connect() {
      if (disposed) return;
      intentional = false;
      if (state.status !== 'connecting' && state.status !== 'connected') void open();
    },
    disconnect() {
      if (disposed) return;
      suspended = false;
      intentional = true;
      ++generation;
      opening = false;
      clear();
      socket?.close(1000, 'Client disconnect');
      socket = undefined;
      rejectPending();
      queue.clear();
      syncQueue();
      for (const room of subscriptions.keys()) notifySubscription(room, 'pending');
      update({ status: 'disconnected' });
    },
    reconnect() {
      api.disconnect();
      update({ attempts: 0 });
      api.connect();
    },
    dispose() {
      if (disposed) return;
      api.disconnect();
      disposed = true;
      lifecycleCleanup();
      listeners.clear();
      subscriptions.clear();
      subscriptionObservers.clear();
      update({ status: 'closed' });
      observers.clear();
    },
    on<K extends ServerEvent<C>>(
      event: K,
      listener: (payload: Output<C['server'][K]>, metadata: { room?: string }) => void,
    ) {
      let set = listeners.get(event);
      if (!set) listeners.set(event, (set = new Set()));
      set.add(listener as Listener);
      return () => {
        set.delete(listener as Listener);
        if (!set.size) listeners.delete(event);
      };
    },
    async send<K extends ClientEvent<C>>(
      event: K,
      payload: Input<C['client'][K]>,
      config: SendOptions = {},
    ): Promise<{ stage: 'written' | 'queued' | 'dropped'; id?: string }> {
      await validate(lookup(options.contract.client, event), payload);
      if (disposed) throw new SocketError('CANCELLED');
      const data = encodeFrame({ v: 1, type: 'event', event, payload });
      if (bytes(data) > (options.maxMessageBytes ?? 65536))
        throw new SocketError('INVALID_PAYLOAD');
      if (state.status === 'connected') {
        writeData(data);
        return { stage: 'written' };
      }
      if (config.disconnected === 'drop') return { stage: 'dropped' };
      if (config.disconnected !== 'queue') throw new SocketError('CONNECTION_LOST');
      const id = queue.push(data, config);
      syncQueue();
      return { stage: 'queued', id };
    },
    cancelQueued(id: string) {
      queue.cancel(id);
      syncQueue();
    },
    clearQueue() {
      queue.clear();
      syncQueue();
    },
    async request<K extends RequestName<C>>(
      event: K,
      payload: Input<C['requests'][K]['input']>,
      config: { timeout?: number; signal?: AbortSignal; idempotencyKey?: string } = {},
    ): Promise<Output<C['requests'][K]['output']>> {
      checkLimits(config, ['timeout'], false);
      if (
        config.idempotencyKey !== undefined &&
        (!config.idempotencyKey || config.idempotencyKey.length > 256)
      )
        throw new SocketError(
          'INVALID_PAYLOAD',
          'Idempotency keys must contain 1 to 256 characters.',
        );
      await validate(lookup(options.contract.requests, event)?.input, payload);
      if (config.signal?.aborted) throw new SocketError('CANCELLED');
      if (state.status !== 'connected') throw new SocketError('CONNECTION_LOST');
      const id = crypto.randomUUID();
      const abort = () => {
        const p = pending.get(id);
        if (p) {
          pending.delete(id);
          clearTimeout(p.timer);
          p.reject(new SocketError('CANCELLED'));
          update({ pending: pending.size });
          try {
            write({ v: 1, type: 'cancel', id });
          } catch {
            /* Lost connection. */
          }
        }
      };
      config.signal?.addEventListener('abort', abort, { once: true });
      try {
        const value = await command(
          {
            v: 1,
            type: 'request',
            id,
            event,
            payload,
            ...(config.idempotencyKey ? { key: config.idempotencyKey } : {}),
          },
          config.timeout,
        );
        return (await validate(options.contract.requests[event].output, value)) as Output<
          C['requests'][K]['output']
        >;
      } finally {
        config.signal?.removeEventListener('abort', abort);
      }
    },
    async requestResult<K extends RequestName<C>>(
      event: K,
      payload: Input<C['requests'][K]['input']>,
      config: { timeout?: number; signal?: AbortSignal; idempotencyKey?: string } = {},
    ): Promise<
      | { ok: true; data: Output<C['requests'][K]['output']> }
      | { ok: false; error: SocketError<ErrorCode | RequestError<C, K>> }
    > {
      try {
        return { ok: true, data: await api.request(event, payload, config) };
      } catch (failure) {
        let safe = publicError(failure);
        if (
          !errorCodes.includes(safe.code as ErrorCode) &&
          !options.contract.requests[event]?.errors?.includes(safe.code)
        )
          safe = new SocketError('INTERNAL_ERROR', 'The operation failed.');
        return { ok: false, error: safe as SocketError<ErrorCode | RequestError<C, K>> };
      }
    },
    subscribe(room: string) {
      if (disposed || !room || room.length > 256) throw new SocketError('INVALID_PAYLOAD');
      let entry = subscriptions.get(room);
      if (!entry) {
        const id = crypto.randomUUID();
        subscriptions.set(
          room,
          (entry = {
            id,
            count: 0,
            active: false,
            state: { id, room, status: 'pending', error: null },
          }),
        );
        if (state.status === 'connected') void join(room, entry);
      }
      entry.count++;
      let removed = false;
      return {
        id: entry.id,
        get active() {
          return entry.active;
        },
        get state() {
          return entry.state;
        },
        unsubscribe() {
          if (removed) return;
          removed = true;
          if (--entry.count) return;
          notifySubscription(room, 'closed');
          subscriptions.delete(room);
          entry.active = false;
          if (state.status === 'connected')
            void command({ v: 1, type: 'leave', id: crypto.randomUUID(), room }).catch(() => {});
        },
      };
    },
    observeSubscriptions(fn: (state: SubscriptionState) => void) {
      subscriptionObservers.add(fn);
      return () => {
        subscriptionObservers.delete(fn);
      };
    },
  };
  let lifecycleCleanup = () => {};
  if (options.lifecycle !== false && typeof window !== 'undefined') {
    const pagehide = () => {
      if (!intentional && !disposed) {
        api.disconnect();
        suspended = true;
      }
    };
    const pageshow = () => {
      if (suspended && !disposed) {
        suspended = false;
        api.connect();
      }
    };
    const online = () => {
      if (!intentional && !disposed && state.status === 'reconnecting') {
        clearTimeout(retryTimer);
        retryTimer = setTimeout(
          () => {
            void open(true);
          },
          retryDelay(state.attempts, options.reconnect || {}),
        );
      }
    };
    const visibility = () => {
      if (options.lifecycle && options.lifecycle.pauseWhenHidden) {
        if (document.hidden) pagehide();
        else pageshow();
      }
    };
    window.addEventListener('pagehide', pagehide);
    window.addEventListener('pageshow', pageshow);
    window.addEventListener('online', online);
    document.addEventListener('visibilitychange', visibility);
    lifecycleCleanup = () => {
      window.removeEventListener('pagehide', pagehide);
      window.removeEventListener('pageshow', pageshow);
      window.removeEventListener('online', online);
      document.removeEventListener('visibilitychange', visibility);
    };
  }
  return api;
}
export type SocketClient<C extends SocketContract> = ReturnType<typeof createSocketClient<C>>;
