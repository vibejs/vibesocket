import { lookup, validate, type SocketContract, type ClientEvent, type ServerEvent, type RequestName, type Input, type Output } from './contract.ts';
import { SocketError, publicError } from './errors.ts';
import { SUBPROTOCOL, encodeFrame, parseFrame, type Frame } from './protocol.ts';
export { SocketError, errorCodes } from './errors.ts';
export type { ErrorCode } from './errors.ts';
export { defineSocketContract } from './contract.ts';
export type { SocketContract } from './contract.ts';

export type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'disconnected' | 'closed' | 'failed';
export interface ClientState { status: ConnectionStatus; attempts: number; error: SocketError<string> | null; latency: number | null; transport: 'websocket'; pending: number; queueDepth: number; queuedBytes: number }
export interface ClientOptions<C extends SocketContract> {
  contract: C; url: string | (() => string | Promise<string>);
  webSocket?: (url: string, protocols: string[]) => WebSocket;
  reconnect?: false | { minDelay?: number; maxDelay?: number; maxAttempts?: number; stableAfter?: number; shouldRetry?: (code: number) => boolean };
  connectTimeout?: number;
}
type Listener = (payload: never, metadata: { room?: string }) => void;
export function createSocketClient<C extends SocketContract>(options: ClientOptions<C>) {
  let socket: WebSocket | undefined, generation = 0, intentional = false, disposed = false;
  let retryTimer: ReturnType<typeof setTimeout> | undefined, connectTimer: ReturnType<typeof setTimeout> | undefined;
  let connectedAt = 0;
  const listeners = new Map<string, Set<Listener>>(), observers = new Set<(state: Readonly<ClientState>) => void>();
  const pending = new Map<string, { resolve: (payload: unknown) => void; reject: (error: unknown) => void; timer: ReturnType<typeof setTimeout> }>();
  const subscriptions = new Map<string, { id: string; count: number; active: boolean }>();
  let state: ClientState = { status: 'idle', attempts: 0, error: null, latency: null, transport: 'websocket', pending: 0, queueDepth: 0, queuedBytes: 0 };
  const update = (patch: Partial<ClientState>) => { state = { ...state, ...patch }; for (const fn of [...observers]) { try { fn(state); } catch { /* Isolate consumers. */ } } };
  const write = (frame: Frame) => { if (!socket || socket.readyState !== 1) throw new SocketError('CONNECTION_LOST'); socket.send(encodeFrame(frame)); };
  const clear = () => { clearTimeout(retryTimer); clearTimeout(connectTimer); retryTimer = connectTimer = undefined; };
  function rejectPending() { for (const p of pending.values()) { clearTimeout(p.timer); p.reject(new SocketError('CONNECTION_LOST')); } pending.clear(); update({ pending: 0 }); }
  function command(frame: Frame & { id: string }, timeout = 5000): Promise<unknown> {
    return new Promise((resolve, reject) => { const timer = setTimeout(() => { pending.delete(frame.id); update({ pending: pending.size }); reject(new SocketError('TIMEOUT')); }, timeout);
      pending.set(frame.id, { resolve, reject, timer }); update({ pending: pending.size });
      try { write(frame); } catch (error) { clearTimeout(timer); pending.delete(frame.id); update({ pending: pending.size }); reject(error); }
    });
  }
  async function join(room: string, entry: { id: string; active: boolean }) {
    try { await command({ v: 1, type: 'join', id: entry.id, room }); if (subscriptions.get(room) === entry) entry.active = true; }
    catch (error) { if (subscriptions.get(room) === entry) update({ error: publicError(error) }); }
  }
  function lost(code: number) {
    clear(); socket = undefined; rejectPending(); for (const entry of subscriptions.values()) entry.active = false;
    if (disposed || intentional) return;
    const policy = options.reconnect === false ? undefined : options.reconnect ?? {};
    if (connectedAt && Date.now() - connectedAt >= (policy?.stableAfter ?? 30000)) update({ attempts: 0 });
    connectedAt = 0;
    const retryable = ![1000, 1002, 1003, 1008, 4401, 4403, 4406, 4429].includes(code);
    if (!policy || !retryable || policy.shouldRetry?.(code) === false || state.attempts >= (policy.maxAttempts ?? 10)) { update({ status: 'failed', error: new SocketError('CONNECTION_LOST') }); return; }
    const attempt = state.attempts + 1, min = policy.minDelay ?? 250, cap = Math.min(policy.maxDelay ?? 30000, min * 2 ** Math.min(attempt, 20));
    update({ status: 'reconnecting', attempts: attempt });
    retryTimer = setTimeout(() => { void open(true); }, min + Math.random() * (cap - min));
  }
  async function receive(frame: Frame, epoch: number) {
    if (epoch !== generation) return;
    if (frame.type === 'hello') {
      if (frame.contract !== options.contract.version || state.status === 'connected') { socket?.close(4406, 'PROTOCOL_MISMATCH'); return; }
      clearTimeout(connectTimer); write({ v: 1, type: 'hello', contract: options.contract.version }); connectedAt = Date.now(); update({ status: 'connected', error: null });
      for (const [room, entry] of subscriptions) void join(room, entry); return;
    }
    if (state.status !== 'connected') throw new SocketError('PROTOCOL_MISMATCH');
    if (frame.type === 'event') {
      if (frame.room && subscriptions.get(frame.room)?.id !== frame.subscription) return;
      const value = await validate(lookup(options.contract.server, frame.event), frame.payload);
      if (epoch !== generation) return;
      for (const fn of [...listeners.get(frame.event) ?? []]) { try { fn(value as never, { room: frame.room }); } catch { /* One listener cannot disrupt others. */ } } return;
    }
    if (frame.type === 'result' || frame.type === 'error') {
      const p = frame.id ? pending.get(frame.id) : undefined;
      if (p && frame.id) { pending.delete(frame.id); clearTimeout(p.timer); update({ pending: pending.size }); if (frame.type === 'result') p.resolve(frame.payload); else p.reject(new SocketError(frame.code, frame.message)); }
      else if (frame.type === 'error') update({ error: new SocketError(frame.code, frame.message) }); return;
    }
    if (frame.type === 'pong') return;
    throw new SocketError('PROTOCOL_MISMATCH');
  }
  async function open(retry = false) {
    if (disposed || intentional || socket || (!retry && state.status === 'reconnecting')) return;
    clear(); const epoch = ++generation; update({ status: retry ? 'reconnecting' : 'connecting' });
    try {
      const url = typeof options.url === 'function' ? await options.url() : options.url;
      if (epoch !== generation || intentional || disposed) return;
      const ws = (options.webSocket ?? ((target, protocols) => new WebSocket(target, protocols)))(url, [SUBPROTOCOL]); socket = ws;
      let tail = Promise.resolve();
      ws.addEventListener('message', e => { if (epoch !== generation) return; tail = tail.then(async () => { if (typeof e.data !== 'string') throw new SocketError('INVALID_PAYLOAD'); await receive(parseFrame(e.data), epoch); }).catch(error => { update({ error: publicError(error) }); ws.close(1008, 'INVALID_PAYLOAD'); }); });
      ws.addEventListener('close', e => { if (epoch === generation) lost(e.code); });
      ws.addEventListener('error', () => { if (epoch === generation) update({ error: new SocketError('CONNECTION_LOST') }); });
      connectTimer = setTimeout(() => { if (epoch === generation) { ++generation; ws.close(); socket = undefined; lost(1006); } }, options.connectTimeout ?? 10000);
    } catch (error) { if (epoch === generation) { update({ error: publicError(error) }); lost(1006); } }
  }
  const api = {
    contract: options.contract,
    get state(): Readonly<ClientState> { return state; },
    get rawSocket() { return socket; },
    observe(fn: (state: Readonly<ClientState>) => void) { observers.add(fn); fn(state); return () => { observers.delete(fn); }; },
    connect() { if (disposed) return; intentional = false; if (state.status !== 'connecting' && state.status !== 'connected') void open(); },
    disconnect() { intentional = true; ++generation; clear(); socket?.close(1000, 'Client disconnect'); socket = undefined; rejectPending(); update({ status: 'disconnected' }); },
    reconnect() { api.disconnect(); update({ attempts: 0 }); api.connect(); },
    dispose() { api.disconnect(); disposed = true; listeners.clear(); observers.clear(); subscriptions.clear(); update({ status: 'closed' }); },
    on<K extends ServerEvent<C>>(event: K, listener: (payload: Output<C['server'][K]>, metadata: { room?: string }) => void) { let set = listeners.get(event); if (!set) listeners.set(event, set = new Set()); set.add(listener as Listener); return () => { set.delete(listener as Listener); if (!set.size) listeners.delete(event); }; },
    async send<K extends ClientEvent<C>>(event: K, payload: Input<C['client'][K]>) { await validate(lookup(options.contract.client, event), payload); if (state.status !== 'connected') throw new SocketError('CONNECTION_LOST'); write({ v: 1, type: 'event', event, payload }); },
    async request<K extends RequestName<C>>(event: K, payload: Input<C['requests'][K]['input']>, config: { timeout?: number; signal?: AbortSignal; idempotencyKey?: string } = {}): Promise<Output<C['requests'][K]['output']>> {
      await validate(lookup(options.contract.requests, event)?.input, payload);
      if (config.signal?.aborted) throw new SocketError('CANCELLED');
      if (state.status !== 'connected') throw new SocketError('CONNECTION_LOST');
      const id = crypto.randomUUID(); const abort = () => { const p = pending.get(id); if (p) { pending.delete(id); clearTimeout(p.timer); p.reject(new SocketError('CANCELLED')); update({ pending: pending.size }); try { write({ v: 1, type: 'cancel', id }); } catch { /* Lost connection. */ } } };
      config.signal?.addEventListener('abort', abort, { once: true });
      try { const value = await command({ v: 1, type: 'request', id, event, payload, ...(config.idempotencyKey ? { key: config.idempotencyKey } : {}) }, config.timeout); return await validate(options.contract.requests[event].output, value) as Output<C['requests'][K]['output']>; }
      finally { config.signal?.removeEventListener('abort', abort); }
    },
    subscribe(room: string) {
      let entry = subscriptions.get(room); if (!entry) { subscriptions.set(room, entry = { id: crypto.randomUUID(), count: 0, active: false }); if (state.status === 'connected') void join(room, entry); } entry.count++;
      let removed = false;
      return { id: entry.id, get active() { return entry.active; }, unsubscribe() { if (removed) return; removed = true; if (--entry.count) return; subscriptions.delete(room); entry.active = false; if (state.status === 'connected') void command({ v: 1, type: 'leave', id: crypto.randomUUID(), room }).catch(() => {}); } };
    },
  };
  return api;
}
export type SocketClient<C extends SocketContract> = ReturnType<typeof createSocketClient<C>>;
