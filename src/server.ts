import { checkLimits } from './core/limits.ts';
import {
  lookup,
  validate,
  type SocketContract,
  type ClientEvent,
  type ServerEvent,
  type RequestName,
  type Input,
  type Output,
  type RequestError,
} from './contract.ts';
import { SocketError, publicError, errorCodes, type ErrorCode } from './errors.ts';
import { bytes, encodeFrame, parseFrame, type Frame } from './protocol.ts';

export interface Principal {
  readonly id: string;
  readonly expiresAt?: number;
}
export interface ServerTransport {
  readonly bufferedAmount: number;
  send(data: string): void;
  close(code: number, reason: string): void;
  acknowledge?(id: string): void;
  checkpoint?(connection: Connection<Principal>, ready: boolean): void;
}
export interface Connection<P extends Principal> {
  readonly id: string;
  readonly principal: P;
  readonly rooms: ReadonlyMap<string, string>;
  readonly connectedAt: number;
  readonly lastActivity: number;
  readonly signal: AbortSignal;
  readonly transport: 'websocket';
  readonly closeInfo?: { readonly code: number; readonly reason: string; readonly at: number };
}
export interface HandlerContext<P extends Principal, E extends string = string> {
  readonly connection: Connection<P>;
  readonly signal: AbortSignal;
  readonly idempotencyKey?: string;
  fail(code: E | ErrorCode, message?: string): never;
}
export interface ServerOptions<C extends SocketContract, P extends Principal> {
  contract: C;
  authorize: (
    principal: P,
    operation: { action: 'join' | 'event' | 'request' | 'receive'; room?: string; event?: string },
    connection: Connection<P>,
  ) => boolean | Promise<boolean>;
  validatePrincipal?: (principal: P) => boolean | Promise<boolean>;
  maxMessageBytes?: number;
  maxBufferedBytes?: number;
  maxConnections?: number;
  maxConnectionsPerPrincipal?: number;
  maxRooms?: number;
  maxInbox?: number;
  messagesPerSecond?: number;
  handlerTimeout?: number;
  processingTimeout?: number;
  diagnostic?: (event: string, fields: Readonly<Record<string, number | string>>) => void;
}
type Session<P extends Principal> = {
  connection: Connection<P>;
  transport: ServerTransport;
  rooms: Map<string, string>;
  abort: AbortController;
  ready: boolean;
  tail: Promise<void>;
  inbox: number;
  window: number;
  count: number;
  pending: Map<string, AbortController>;
  cancelled: Set<string>;
};
export function createSocketServer<C extends SocketContract, P extends Principal>(
  options: ServerOptions<C, P>,
) {
  checkLimits(
    options,
    ['maxMessageBytes', 'maxBufferedBytes', 'maxInbox', 'handlerTimeout', 'processingTimeout'],
    false,
  );
  checkLimits(options, [
    'maxConnections',
    'maxConnectionsPerPrincipal',
    'maxRooms',
    'messagesPerSecond',
  ]);
  if (typeof options.authorize !== 'function')
    throw new TypeError('An authorization policy is required.');
  const sessions = new Map<string, Session<P>>();
  const rooms = new Map<string, Set<string>>();
  const users = new Map<string, Set<string>>();
  const events = new Map<string, (payload: never, ctx: HandlerContext<P>) => unknown>();
  const requests = new Map<string, (payload: never, ctx: HandlerContext<P>) => unknown>();
  let closing = false;
  const metrics = { received: 0, sent: 0, rejected: 0, handlerErrors: 0 };
  function report(event: string, fields: Record<string, number | string> = {}) {
    try {
      options.diagnostic?.(event, fields);
    } catch {
      /* Diagnostics cannot affect delivery. */
    }
  }
  function remove(id: string, code = 1006, reason = 'Transport disconnected') {
    const s = sessions.get(id);
    if (!s) return;
    (s.connection as { closeInfo: Connection<P>['closeInfo'] }).closeInfo = Object.freeze({
      code,
      reason,
      at: Date.now(),
    });
    sessions.delete(id);
    s.abort.abort();
    for (const controller of s.pending.values()) controller.abort();
    for (const room of s.rooms.keys()) leave(s, room);
    const group = users.get(s.connection.principal.id);
    group?.delete(id);
    if (!group?.size) users.delete(s.connection.principal.id);
    report('connection.closed', { active: sessions.size, code });
  }
  function close(id: string, code = 1000, reason = 'Closed') {
    const s = sessions.get(id);
    if (!s) return;
    remove(id, code, reason);
    try {
      s.transport.close(code, reason);
    } catch {
      report('transport.close_failed', { code });
    }
  }
  function write(
    s: Session<P>,
    frame: Frame,
    overflow: 'disconnect' | 'drop' = 'disconnect',
  ): boolean {
    if (!sessions.has(s.connection.id)) return false;
    const data = encodeFrame(frame);
    if (bytes(data) > (options.maxMessageBytes ?? 65536)) {
      close(s.connection.id, 1009, 'INVALID_PAYLOAD');
      return false;
    }
    if (s.transport.bufferedAmount + bytes(data) > (options.maxBufferedBytes ?? 262144)) {
      if (overflow === 'disconnect') close(s.connection.id, 4008, 'SLOW_CONSUMER');
      return false;
    }
    try {
      s.transport.send(data);
      metrics.sent++;
      return true;
    } catch (error) {
      close(
        s.connection.id,
        error instanceof SocketError && error.code === 'SLOW_CONSUMER'
          ? 4008
          : error instanceof SocketError && error.code === 'INVALID_PAYLOAD'
            ? 1009
            : 1011,
        'Write failed',
      );
      return false;
    }
  }
  function leave(s: Session<P>, room: string) {
    s.rooms.delete(room);
    const group = rooms.get(room);
    group?.delete(s.connection.id);
    if (!group?.size) rooms.delete(room);
  }
  function evict(id: string, room: string, code = 'FORBIDDEN') {
    const s = sessions.get(id),
      subscription = s?.rooms.get(room);
    if (!s || !subscription) return;
    leave(s, room);
    write(s, { v: 1, type: 'revoked', room, subscription, code });
    try {
      s.transport.checkpoint?.(s.connection, s.ready);
    } catch {
      close(id, 1011, 'Checkpoint failed');
    }
  }
  async function authorized(s: Session<P>, op: Parameters<typeof options.authorize>[1]) {
    if (s.abort.signal.aborted) throw new SocketError('CONNECTION_LOST');
    const p = s.connection.principal;
    if (
      (p.expiresAt !== undefined && p.expiresAt <= Date.now()) ||
      (options.validatePrincipal && !(await options.validatePrincipal(p)))
    ) {
      close(s.connection.id, 4401, 'AUTH_REQUIRED');
      throw new SocketError('AUTH_REQUIRED');
    }
    if (s.abort.signal.aborted) throw new SocketError('CONNECTION_LOST');
    if (!(await options.authorize(p, op, s.connection))) throw new SocketError('FORBIDDEN');
    if (s.abort.signal.aborted) throw new SocketError('CONNECTION_LOST');
  }
  async function dispatch(s: Session<P>, frame: Frame) {
    if (frame.receipt) throw new SocketError('INVALID_PAYLOAD');
    if (frame.type === 'hello') {
      if (s.ready || frame.contract !== options.contract.version)
        throw new SocketError('PROTOCOL_MISMATCH');
      s.ready = true;
      s.transport.checkpoint?.(s.connection, s.ready);
      return;
    }
    if (!s.ready) throw new SocketError('PROTOCOL_MISMATCH');
    if (frame.type === 'ping') {
      write(s, { v: 1, type: 'pong' });
      return;
    }
    if (frame.type === 'join') {
      await authorized(s, { action: 'join', room: frame.room });
      if (!s.rooms.has(frame.room) && s.rooms.size >= (options.maxRooms ?? 32))
        throw new SocketError('RATE_LIMITED');
      s.rooms.set(frame.room, frame.id);
      let group = rooms.get(frame.room);
      if (!group) rooms.set(frame.room, (group = new Set()));
      group.add(s.connection.id);
      s.transport.checkpoint?.(s.connection, s.ready);
      write(s, { v: 1, type: 'result', id: frame.id, payload: null });
      return;
    }
    if (frame.type === 'leave') {
      leave(s, frame.room);
      s.transport.checkpoint?.(s.connection, s.ready);
      write(s, { v: 1, type: 'result', id: frame.id, payload: null });
      return;
    }
    if (frame.type === 'cancel') {
      s.pending.get(frame.id)?.abort();
      return;
    }
    if (frame.type !== 'event' && frame.type !== 'request')
      throw new SocketError('INVALID_PAYLOAD');
    if (frame.type === 'request' && s.cancelled.delete(frame.id)) return;
    if (frame.type === 'event' && (frame.room || frame.subscription))
      throw new SocketError('INVALID_PAYLOAD');
    await authorized(s, { action: frame.type, event: frame.event });
    const definition =
      frame.type === 'request' ? lookup(options.contract.requests, frame.event) : undefined;
    const payload = await validate(
      frame.type === 'request' ? definition?.input : lookup(options.contract.client, frame.event),
      frame.payload,
    );
    if (s.abort.signal.aborted) throw new SocketError('CONNECTION_LOST');
    if (frame.type === 'request' && s.cancelled.delete(frame.id)) return;
    const handler = (frame.type === 'request' ? requests : events).get(frame.event);
    if (!handler) throw new SocketError('UNSUPPORTED_EVENT');
    const controller = new AbortController();
    const abort = () => controller.abort();
    s.abort.signal.addEventListener('abort', abort, { once: true });
    if (frame.type === 'request') {
      if (s.pending.has(frame.id)) throw new SocketError('INVALID_PAYLOAD');
      s.pending.set(frame.id, controller);
    }
    try {
      const started = performance.now();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const work = Promise.resolve().then(() =>
        handler(payload as never, {
          connection: s.connection,
          signal: controller.signal,
          idempotencyKey: frame.type === 'request' ? frame.key : undefined,
          fail(code, message) {
            if (!definition?.errors?.includes(code) && !errorCodes.includes(code as ErrorCode))
              throw new Error('Undeclared error');
            throw new SocketError(code, message);
          },
        }),
      );
      let result: unknown;
      try {
        result = await Promise.race([
          work,
          new Promise<never>((_resolve, reject) => {
            const abort = () => reject(new SocketError('CANCELLED'));
            controller.signal.addEventListener('abort', abort, { once: true });
            timer = setTimeout(() => {
              reject(new SocketError('TIMEOUT'));
              controller.abort();
            }, options.handlerTimeout ?? 30000);
            void work
              .finally(() => controller.signal.removeEventListener('abort', abort))
              .catch(() => {});
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
      report('handler.completed', { ms: performance.now() - started });
      if (frame.type === 'request' && !controller.signal.aborted) {
        await validate(definition!.output, result);
        write(s, { v: 1, type: 'result', id: frame.id, payload: result });
      }
    } finally {
      s.abort.signal.removeEventListener('abort', abort);
      if (frame.type === 'request') s.pending.delete(frame.id);
    }
  }
  return {
    contract: options.contract,
    metrics,
    get stats() {
      return {
        activeConnections: sessions.size,
        activeRooms: rooms.size,
        subscriptions: [...sessions.values()].reduce((n, s) => n + s.rooms.size, 0),
        bufferedBytes: [...sessions.values()].reduce((n, s) => n + s.transport.bufferedAmount, 0),
        inboxDepth: [...sessions.values()].reduce((n, s) => n + s.inbox, 0),
        ...metrics,
      };
    },
    get connections(): readonly Connection<P>[] {
      return [...sessions.values()].map((s) => s.connection);
    },
    get roomCount() {
      return rooms.size;
    },
    on<K extends ClientEvent<C>>(
      event: K,
      handler: (payload: Output<C['client'][K]>, ctx: HandlerContext<P>) => void | Promise<void>,
    ) {
      events.set(event, handler as never);
      return () => {
        if (events.get(event) === handler) events.delete(event);
      };
    },
    handle<K extends RequestName<C>>(
      event: K,
      handler: (
        payload: Output<C['requests'][K]['input']>,
        ctx: HandlerContext<P, RequestError<C, K>>,
      ) => Input<C['requests'][K]['output']> | Promise<Input<C['requests'][K]['output']>>,
    ) {
      requests.set(event, handler as never);
      return () => {
        if (requests.get(event) === handler) requests.delete(event);
      };
    },
    accept(
      transport: ServerTransport,
      principal: P,
      restore?: {
        id: string;
        rooms: readonly [string, string][];
        ready: boolean;
        connectedAt: number;
        lastActivity: number;
      },
    ): Connection<P> {
      if (closing) throw new SocketError('SERVER_UNAVAILABLE');
      if (restore && (sessions.has(restore.id) || restore.rooms.length > (options.maxRooms ?? 32)))
        throw new SocketError('RATE_LIMITED');
      if (
        typeof principal.id !== 'string' ||
        !principal.id ||
        principal.id.length > 256 ||
        (principal.expiresAt !== undefined && !Number.isFinite(principal.expiresAt))
      )
        throw new SocketError('AUTH_REQUIRED');
      if (
        sessions.size >= (options.maxConnections ?? 10000) ||
        (users.get(principal.id)?.size ?? 0) >= (options.maxConnectionsPerPrincipal ?? 8)
      )
        throw new SocketError('RATE_LIMITED');
      const id = restore?.id ?? crypto.randomUUID(),
        abort = new AbortController(),
        membership = new Map<string, string>(restore?.rooms);
      const connection: Connection<P> = {
        id,
        principal: Object.freeze({ ...principal }),
        rooms: membership,
        connectedAt: restore?.connectedAt ?? Date.now(),
        lastActivity: restore?.lastActivity ?? Date.now(),
        signal: abort.signal,
        transport: 'websocket',
      };
      const s: Session<P> = {
        connection,
        transport,
        rooms: membership,
        abort,
        ready: restore?.ready ?? false,
        tail: Promise.resolve(),
        inbox: 0,
        window: Date.now(),
        count: 0,
        pending: new Map(),
        cancelled: new Set(),
      };
      sessions.set(id, s);
      let group = users.get(principal.id);
      if (!group) users.set(principal.id, (group = new Set()));
      group.add(id);
      for (const room of membership.keys()) {
        let members = rooms.get(room);
        if (!members) rooms.set(room, (members = new Set()));
        members.add(id);
      }
      try {
        transport.checkpoint?.(connection, s.ready);
      } catch (error) {
        remove(id);
        throw error;
      }
      if (!restore) write(s, { v: 1, type: 'hello', contract: options.contract.version });
      report('connection.opened', { active: sessions.size });
      return connection;
    },
    receive(id: string, raw: string) {
      const s = sessions.get(id);
      if (!s) return;
      metrics.received++;
      if (Date.now() - s.window >= 1000) {
        s.window = Date.now();
        s.count = 0;
      }
      if (++s.count > (options.messagesPerSecond ?? 100) || s.inbox >= (options.maxInbox ?? 64)) {
        metrics.rejected++;
        close(id, 4429, 'RATE_LIMITED');
        return;
      }
      let frame: Frame;
      try {
        frame = parseFrame(raw, options.maxMessageBytes);
      } catch (error) {
        const safe = publicError(error);
        write(s, { v: 1, type: 'error', code: safe.code, message: safe.code });
        close(id, safe.code === 'PROTOCOL_MISMATCH' ? 4406 : 1008, safe.code);
        return;
      }
      if (frame.receipt) {
        close(id, 1008, 'INVALID_PAYLOAD');
        return;
      }
      // Cancellation must bypass a blocked asynchronous handler.
      (s.connection as { lastActivity: number }).lastActivity = Date.now();
      if (frame.type === 'receipt') {
        s.transport.acknowledge?.(frame.id);
        return;
      }
      if (frame.type === 'cancel') {
        const controller = s.pending.get(frame.id);
        if (controller) controller.abort();
        else {
          s.cancelled.add(frame.id);
          if (s.cancelled.size > (options.maxInbox ?? 64))
            s.cancelled.delete(s.cancelled.values().next().value!);
        }
        return;
      }
      if (frame.type === 'ping' && s.ready) {
        write(s, { v: 1, type: 'pong' });
        return;
      }
      (s.connection as { lastActivity: number }).lastActivity = Date.now();
      s.inbox++;
      s.tail = s.tail
        .then(async () => {
          if (s.abort.signal.aborted) return;
          let deadline: ReturnType<typeof setTimeout> | undefined;
          let detach = () => {};
          const aborted = new Promise<void>((resolve) => {
            const closed = () => resolve();
            s.abort.signal.addEventListener('abort', closed, { once: true });
            detach = () => s.abort.signal.removeEventListener('abort', closed);
            deadline = setTimeout(() => {
              close(id, 1011, 'Processing deadline exceeded');
              resolve();
            }, options.processingTimeout ?? 45000);
          });
          try {
            await Promise.race([dispatch(s, frame), aborted]);
          } finally {
            clearTimeout(deadline);
            detach();
          }
        })
        .catch((error) => {
          metrics.handlerErrors++;
          const safe = publicError(error);
          write(s, {
            v: 1,
            type: 'error',
            ...('id' in frame ? { id: frame.id } : {}),
            code: safe.code,
            message: safe.code === 'INTERNAL_ERROR' ? safe.message : safe.code,
          });
          report('handler.error', { code: safe.code });
          if (safe.code === 'PROTOCOL_MISMATCH') close(id, 4406, safe.code);
        })
        .finally(() => {
          s.inbox--;
          try {
            s.transport.checkpoint?.(s.connection, s.ready);
          } catch {
            close(id, 1011, 'Checkpoint failed');
          }
        });
    },
    remove,
    evict,
    close,
    isReady(id: string) {
      return sessions.get(id)?.ready ?? false;
    },
    async drain(id: string) {
      await sessions.get(id)?.tail;
    },
    revoke(principalId: string) {
      for (const id of [...(users.get(principalId) ?? [])]) close(id, 4401, 'AUTH_REQUIRED');
    },
    async publish<K extends ServerEvent<C>>(
      room: string,
      event: K,
      payload: Input<C['server'][K]>,
      config: { exclude?: string; overflow?: 'disconnect' | 'drop' } = {},
    ) {
      await validate(lookup(options.contract.server, event), payload);
      if (
        bytes(
          encodeFrame({ v: 1, type: 'event', room, subscription: 'x'.repeat(256), event, payload }),
        ) > (options.maxMessageBytes ?? 65536)
      )
        throw new SocketError('INVALID_PAYLOAD');
      const result = { attempted: 0, written: 0, rejected: 0 };
      for (const id of [...(rooms.get(room) ?? [])]) {
        if (id === config.exclude) continue;
        const s = sessions.get(id);
        if (!s) continue;
        result.attempted++;
        try {
          await authorized(s, { action: 'receive', room, event });
          if (
            s.rooms.has(room) &&
            write(
              s,
              { v: 1, type: 'event', room, subscription: s.rooms.get(room), event, payload },
              config.overflow,
            )
          )
            result.written++;
          else result.rejected++;
        } catch {
          evict(id, room);
          result.rejected++;
        }
      }
      return result;
    },
    async send<K extends ServerEvent<C>>(id: string, event: K, payload: Input<C['server'][K]>) {
      await validate(lookup(options.contract.server, event), payload);
      const s = sessions.get(id);
      if (!s) return false;
      try {
        await authorized(s, { action: 'receive', event });
        return write(s, { v: 1, type: 'event', event, payload });
      } catch {
        return false;
      }
    },
    async sendToUser<K extends ServerEvent<C>>(
      principalId: string,
      event: K,
      payload: Input<C['server'][K]>,
    ) {
      await validate(lookup(options.contract.server, event), payload);
      let written = 0;
      const recipients = [...(users.get(principalId) ?? [])];
      for (const id of recipients) {
        const s = sessions.get(id);
        if (!s) continue;
        try {
          await authorized(s, { action: 'receive', event });
          if (write(s, { v: 1, type: 'event', event, payload })) written++;
        } catch {
          /* A rejected recipient cannot affect others. */
        }
      }
      return { attempted: recipients.length, written };
    },
    shutdown() {
      closing = true;
      for (const id of [...sessions.keys()]) close(id, 1001, 'Server shutdown');
    },
  };
}
export type SocketServer<C extends SocketContract, P extends Principal> = ReturnType<
  typeof createSocketServer<C, P>
>;
