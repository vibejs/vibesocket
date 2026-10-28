import { lookup, validate, type SocketContract, type ClientEvent, type ServerEvent, type RequestName, type Input, type Output, type RequestError } from './contract.ts';
import { SocketError, publicError } from './errors.ts';
import { bytes, encodeFrame, parseFrame, type Frame } from './protocol.ts';

export interface Principal { readonly id: string; readonly expiresAt?: number }
export interface ServerTransport {
  readonly bufferedAmount: number;
  send(data: string): void;
  close(code: number, reason: string): void;
}
export interface Connection<P extends Principal> {
  readonly id: string;
  readonly principal: P;
  readonly rooms: ReadonlyMap<string, string>;
  readonly connectedAt: number;
  readonly lastActivity: number;
  readonly signal: AbortSignal;
}
export interface HandlerContext<P extends Principal, E extends string = string> {
  readonly connection: Connection<P>;
  readonly signal: AbortSignal;
  readonly idempotencyKey?: string;
  fail(code: E, message?: string): never;
}
export interface ServerOptions<C extends SocketContract, P extends Principal> {
  contract: C;
  authorize: (principal: P, operation: { action: 'join' | 'event' | 'request' | 'receive'; room?: string; event?: string }, connection: Connection<P>) => boolean | Promise<boolean>;
  validatePrincipal?: (principal: P) => boolean | Promise<boolean>;
  maxMessageBytes?: number;
  maxBufferedBytes?: number;
  maxConnections?: number;
  maxConnectionsPerPrincipal?: number;
  maxRooms?: number;
  maxInbox?: number;
  messagesPerSecond?: number;
  diagnostic?: (event: string, fields: Readonly<Record<string, number | string>>) => void;
}
type Session<P extends Principal> = {
  connection: Connection<P>; transport: ServerTransport; rooms: Map<string, string>;
  abort: AbortController; ready: boolean; tail: Promise<void>; inbox: number; window: number; count: number;
  pending: Map<string, AbortController>;
};
export function createSocketServer<C extends SocketContract, P extends Principal>(options: ServerOptions<C, P>) {
  const sessions = new Map<string, Session<P>>();
  const rooms = new Map<string, Set<string>>();
  const users = new Map<string, Set<string>>();
  const events = new Map<string, (payload: never, ctx: HandlerContext<P>) => unknown>();
  const requests = new Map<string, (payload: never, ctx: HandlerContext<P>) => unknown>();
  let closing = false;
  const metrics = { received: 0, sent: 0, rejected: 0, handlerErrors: 0 };
  function report(event: string, fields: Record<string, number | string> = {}) { try { options.diagnostic?.(event, fields); } catch { /* Diagnostics cannot affect delivery. */ } }
  function remove(id: string) {
    const s = sessions.get(id); if (!s) return;
    sessions.delete(id); s.abort.abort();
    for (const controller of s.pending.values()) controller.abort();
    for (const room of s.rooms.keys()) leave(s, room);
    const group = users.get(s.connection.principal.id); group?.delete(id); if (!group?.size) users.delete(s.connection.principal.id);
    report('connection.closed', { active: sessions.size });
  }
  function close(id: string, code = 1000, reason = 'Closed') { const s = sessions.get(id); if (!s) return; remove(id); s.transport.close(code, reason); }
  function write(s: Session<P>, frame: Frame): boolean {
    if (!sessions.has(s.connection.id)) return false;
    const data = encodeFrame(frame);
    if (s.transport.bufferedAmount + bytes(data) > (options.maxBufferedBytes ?? 262144)) { close(s.connection.id, 4008, 'SLOW_CONSUMER'); return false; }
    try { s.transport.send(data); metrics.sent++; return true; } catch { close(s.connection.id, 1011, 'Write failed'); return false; }
  }
  function leave(s: Session<P>, room: string) { s.rooms.delete(room); const group = rooms.get(room); group?.delete(s.connection.id); if (!group?.size) rooms.delete(room); }
  async function authorized(s: Session<P>, op: Parameters<typeof options.authorize>[1]) {
    const p = s.connection.principal;
    if ((p.expiresAt !== undefined && p.expiresAt <= Date.now()) || (options.validatePrincipal && !await options.validatePrincipal(p))) { close(s.connection.id, 4401, 'AUTH_REQUIRED'); throw new SocketError('AUTH_REQUIRED'); }
    if (!await options.authorize(p, op, s.connection)) throw new SocketError('FORBIDDEN');
    if (s.abort.signal.aborted) throw new SocketError('CONNECTION_LOST');
  }
  async function dispatch(s: Session<P>, frame: Frame) {
    if (frame.type === 'hello') { if (s.ready || frame.contract !== options.contract.version) throw new SocketError('PROTOCOL_MISMATCH'); s.ready = true; return; }
    if (!s.ready) throw new SocketError('PROTOCOL_MISMATCH');
    if (frame.type === 'ping') { write(s, { v: 1, type: 'pong' }); return; }
    if (frame.type === 'join') {
      await authorized(s, { action: 'join', room: frame.room });
      if (!s.rooms.has(frame.room) && s.rooms.size >= (options.maxRooms ?? 32)) throw new SocketError('RATE_LIMITED');
      s.rooms.set(frame.room, frame.id);
      let group = rooms.get(frame.room); if (!group) rooms.set(frame.room, group = new Set()); group.add(s.connection.id);
      write(s, { v: 1, type: 'result', id: frame.id, payload: null }); return;
    }
    if (frame.type === 'leave') { leave(s, frame.room); write(s, { v: 1, type: 'result', id: frame.id, payload: null }); return; }
    if (frame.type === 'cancel') { s.pending.get(frame.id)?.abort(); return; }
    if (frame.type !== 'event' && frame.type !== 'request') throw new SocketError('INVALID_PAYLOAD');
    if (frame.type === 'event' && (frame.room || frame.subscription)) throw new SocketError('INVALID_PAYLOAD');
    await authorized(s, { action: frame.type, event: frame.event });
    const definition = frame.type === 'request' ? lookup(options.contract.requests, frame.event) : undefined;
    const payload = await validate(frame.type === 'request' ? definition?.input : lookup(options.contract.client, frame.event), frame.payload);
    const handler = (frame.type === 'request' ? requests : events).get(frame.event);
    if (!handler) throw new SocketError('UNSUPPORTED_EVENT');
    const controller = new AbortController();
    const abort = () => controller.abort(); s.abort.signal.addEventListener('abort', abort, { once: true });
    if (frame.type === 'request') { if (s.pending.has(frame.id)) throw new SocketError('INVALID_PAYLOAD'); s.pending.set(frame.id, controller); }
    try {
      const started = performance.now();
      const result = await handler(payload as never, { connection: s.connection, signal: controller.signal, idempotencyKey: frame.type === 'request' ? frame.key : undefined,
        fail(code, message) { if (!definition?.errors?.includes(code)) throw new Error('Undeclared error'); throw new SocketError(code, message); } });
      report('handler.completed', { ms: performance.now() - started });
      if (frame.type === 'request' && !controller.signal.aborted) { await validate(definition!.output, result); write(s, { v: 1, type: 'result', id: frame.id, payload: result }); }
    } finally { s.abort.signal.removeEventListener('abort', abort); if (frame.type === 'request') s.pending.delete(frame.id); }
  }
  return {
    contract: options.contract,
    metrics,
    get connections(): readonly Connection<P>[] { return [...sessions.values()].map(s => s.connection); },
    get roomCount() { return rooms.size; },
    on<K extends ClientEvent<C>>(event: K, handler: (payload: Output<C['client'][K]>, ctx: HandlerContext<P>) => void | Promise<void>) { events.set(event, handler as never); return () => { events.delete(event); }; },
    handle<K extends RequestName<C>>(event: K, handler: (payload: Output<C['requests'][K]['input']>, ctx: HandlerContext<P, RequestError<C, K>>) => Input<C['requests'][K]['output']> | Promise<Input<C['requests'][K]['output']>>) { requests.set(event, handler as never); return () => { requests.delete(event); }; },
    accept(transport: ServerTransport, principal: P): Connection<P> {
      if (closing) throw new SocketError('SERVER_UNAVAILABLE');
      if (sessions.size >= (options.maxConnections ?? 10000) || (users.get(principal.id)?.size ?? 0) >= (options.maxConnectionsPerPrincipal ?? 8)) throw new SocketError('RATE_LIMITED');
      const id = crypto.randomUUID(), abort = new AbortController(), membership = new Map<string, string>();
      const connection: Connection<P> = { id, principal: Object.freeze({ ...principal }), rooms: membership, connectedAt: Date.now(), lastActivity: Date.now(), signal: abort.signal };
      const s: Session<P> = { connection, transport, rooms: membership, abort, ready: false, tail: Promise.resolve(), inbox: 0, window: Date.now(), count: 0, pending: new Map() };
      sessions.set(id, s); let group = users.get(principal.id); if (!group) users.set(principal.id, group = new Set()); group.add(id);
      write(s, { v: 1, type: 'hello', contract: options.contract.version }); report('connection.opened', { active: sessions.size }); return connection;
    },
    receive(id: string, raw: string) {
      const s = sessions.get(id); if (!s) return;
      metrics.received++;
      if (Date.now() - s.window >= 1000) { s.window = Date.now(); s.count = 0; }
      if (++s.count > (options.messagesPerSecond ?? 100) || s.inbox >= (options.maxInbox ?? 64)) { metrics.rejected++; close(id, 4429, 'RATE_LIMITED'); return; }
      let frame: Frame;
      try { frame = parseFrame(raw, options.maxMessageBytes); } catch (error) { const safe = publicError(error); write(s, { v: 1, type: 'error', code: safe.code, message: safe.code }); close(id, safe.code === 'PROTOCOL_MISMATCH' ? 4406 : 1008, safe.code); return; }
      // Cancellation must bypass a blocked asynchronous handler.
      if (frame.type === 'cancel') { s.pending.get(frame.id)?.abort(); return; }
      (s.connection as { lastActivity: number }).lastActivity = Date.now(); s.inbox++;
      s.tail = s.tail.then(async () => { if (s.abort.signal.aborted) return; await dispatch(s, frame); }).catch(error => {
        metrics.handlerErrors++; const safe = publicError(error);
        write(s, { v: 1, type: 'error', ...('id' in frame ? { id: frame.id } : {}), code: safe.code, message: safe.code === 'INTERNAL_ERROR' ? safe.message : safe.code });
        report('handler.error', { code: safe.code }); if (safe.code === 'PROTOCOL_MISMATCH') close(id, 4406, safe.code);
      }).finally(() => { s.inbox--; });
    },
    remove, close,
    revoke(principalId: string) { for (const id of [...users.get(principalId) ?? []]) close(id, 4401, 'AUTH_REQUIRED'); },
    async publish<K extends ServerEvent<C>>(room: string, event: K, payload: Input<C['server'][K]>, config: { exclude?: string } = {}) {
      await validate(lookup(options.contract.server, event), payload);
      const result = { attempted: 0, written: 0, rejected: 0 };
      for (const id of [...rooms.get(room) ?? []]) {
        if (id === config.exclude) continue; const s = sessions.get(id); if (!s) continue; result.attempted++;
        try { await authorized(s, { action: 'receive', room, event }); if (s.rooms.has(room) && write(s, { v: 1, type: 'event', room, subscription: s.rooms.get(room), event, payload })) result.written++; else result.rejected++; } catch { leave(s, room); result.rejected++; }
      }
      return result;
    },
    async send<K extends ServerEvent<C>>(id: string, event: K, payload: Input<C['server'][K]>) {
      await validate(lookup(options.contract.server, event), payload); const s = sessions.get(id); if (!s) return false;
      try { await authorized(s, { action: 'receive', event }); return write(s, { v: 1, type: 'event', event, payload }); } catch { return false; }
    },
    shutdown() { closing = true; for (const id of [...sessions.keys()]) close(id, 1001, 'Server shutdown'); },
  };
}
export type SocketServer<C extends SocketContract, P extends Principal> = ReturnType<typeof createSocketServer<C, P>>;
