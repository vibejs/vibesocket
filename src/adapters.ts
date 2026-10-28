import type { SocketClient } from './index.ts';
import type { SocketContract, ServerEvent, Output } from './contract.ts';
import type { Connection, Principal, SocketServer } from './server.ts';
import { SocketError } from './errors.ts';

/** The public Better Auth session source used by VibeAuth. No session tokens are retained. */
export interface SessionSource<S> {
  api: {
    getSession(options: {
      headers: Headers;
      query: { disableCookieCache: true; disableRefresh: true };
    }): Promise<S | null>;
  };
}
export function createSessionAuthenticator<
  S extends { user: { id: string }; session: { id: string; expiresAt: Date } },
  P extends Principal,
>(source: SessionSource<S>, principal: (session: S) => P) {
  return async (request: Request): Promise<(P & { expiresAt: number }) | null> => {
    const session = await source.api.getSession({
      headers: request.headers,
      query: { disableCookieCache: true, disableRefresh: true },
    });
    if (!session || session.session.expiresAt.getTime() <= Date.now()) return null;
    return {
      ...principal(session),
      id: session.user.id,
      expiresAt: session.session.expiresAt.getTime(),
    };
  };
}
/** QueryClient remains the cache owner. Call cleanup on logout/identity change. */
export function syncSocketQuery<C extends SocketContract, K extends ServerEvent<C>>(
  client: SocketClient<C>,
  event: K,
  apply: (payload: Output<C['server'][K]>, metadata: { room?: string }) => void,
  resynchronize: () => void | Promise<void>,
) {
  let wasConnected = client.state.status === 'connected';
  let previous = client.state.status;
  const off = client.on(event, apply);
  const state = client.observe((value) => {
    const entered = value.status === 'connected' && previous !== 'connected';
    previous = value.status;
    if (entered) {
      if (wasConnected)
        void Promise.resolve()
          .then(resynchronize)
          .catch(() => {});
      wasConnected = true;
    }
  });
  return () => {
    off();
    state();
  };
}
/** Approximate presence: one identity, potentially many active tabs/devices. */
export function presenceSnapshot<P extends Principal>(
  connections: readonly Connection<P>[],
  options: { room?: string; lease?: number; now?: number } = {},
) {
  const users = new Map<string, { userId: string; connections: number; lastActivity: number }>();
  for (const connection of connections) {
    if (
      connection.signal.aborted ||
      (options.room && !connection.rooms.has(options.room)) ||
      (options.now ?? Date.now()) - connection.lastActivity > (options.lease ?? 90000)
    )
      continue;
    const user = users.get(connection.principal.id) ?? {
      userId: connection.principal.id,
      connections: 0,
      lastActivity: 0,
    };
    user.connections++;
    user.lastActivity = Math.max(user.lastActivity, connection.lastActivity);
    users.set(user.userId, user);
  }
  return [...users.values()];
}
export function createTypingTracker(options: {
  ttl?: number;
  maxEntries?: number;
  onChange: (room: string, users: readonly string[]) => void;
}) {
  const entries = new Map<
    string,
    { user: string; room: string; expires: number; off: () => void }
  >();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const snapshot = (room: string) => [
    ...new Set(
      [...entries.values()]
        .filter((e) => e.room === room && e.expires > Date.now())
        .map((e) => e.user),
    ),
  ];
  function emit(room: string) {
    try {
      options.onChange(room, snapshot(room));
    } catch {
      /* Consumer isolation. */
    }
  }
  function remove(key: string) {
    const entry = entries.get(key);
    if (!entry) return;
    entries.delete(key);
    entry.off();
    emit(entry.room);
  }
  function schedule() {
    clearTimeout(timer);
    if (!entries.size) return;
    timer = setTimeout(
      () => {
        for (const [key, entry] of entries) if (entry.expires <= Date.now()) remove(key);
        schedule();
      },
      Math.max(1, Math.min(...[...entries.values()].map((e) => e.expires)) - Date.now()),
    );
    timer.unref?.();
  }
  return {
    snapshot,
    set<P extends Principal>(connection: Connection<P>, room: string, typing: boolean) {
      if (!connection.rooms.has(room) || connection.signal.aborted)
        throw new SocketError('FORBIDDEN');
      const key = JSON.stringify([connection.id, room]),
        previous = entries.get(key);
      if (!typing) {
        remove(key);
        schedule();
        return;
      }
      if (previous) {
        previous.expires = Date.now() + (options.ttl ?? 4000);
        schedule();
        return;
      }
      if (entries.size >= (options.maxEntries ?? 1000)) throw new SocketError('RATE_LIMITED');
      const abort = () => {
        remove(key);
        schedule();
      };
      connection.signal.addEventListener('abort', abort, { once: true });
      entries.set(key, {
        user: connection.principal.id,
        room,
        expires: Date.now() + (options.ttl ?? 4000),
        off: () => connection.signal.removeEventListener('abort', abort),
      });
      emit(room);
      schedule();
    },
    close() {
      clearTimeout(timer);
      for (const entry of entries.values()) entry.off();
      entries.clear();
    },
  };
}
export interface PubSubMessage {
  readonly id: string;
  readonly room: string;
  readonly event: string;
  readonly payload: unknown;
}
export interface PubSub {
  publish(channel: string, message: PubSubMessage): Promise<void>;
  subscribe(
    channel: string,
    listener: (message: PubSubMessage) => Promise<void>,
  ): Promise<() => void | Promise<void>>;
  close(): Promise<void>;
}
/** Share this instance within one process. It is not a multi-process backend. */
export function createMemoryPubSub(): PubSub {
  const channels = new Map<string, Set<(message: PubSubMessage) => Promise<void>>>();
  let closed = false;
  return {
    async publish(channel, message) {
      if (closed) throw new SocketError('SERVER_UNAVAILABLE');
      await Promise.all(
        [...(channels.get(channel) ?? [])].map((fn) => fn(structuredClone(message))),
      );
    },
    async subscribe(channel, listener) {
      if (closed) throw new SocketError('SERVER_UNAVAILABLE');
      let group = channels.get(channel);
      if (!group) channels.set(channel, (group = new Set()));
      group.add(listener);
      return () => {
        group.delete(listener);
        if (!group.size) channels.delete(channel);
      };
    },
    async close() {
      closed = true;
      channels.clear();
    },
  };
}
export async function connectPubSub<C extends SocketContract, P extends Principal>(
  server: SocketServer<C, P>,
  bus: PubSub,
  namespace: string,
) {
  const seen = new Set<string>();
  const close = await bus.subscribe(namespace, async (message) => {
    if (seen.has(message.id)) return;
    seen.add(message.id);
    if (seen.size > 1000) seen.delete(seen.values().next().value!);
    await server.publish(message.room, message.event, message.payload as never);
  });
  return {
    close,
    publish: (room: string, event: ServerEvent<C>, payload: unknown) =>
      bus.publish(namespace, { id: crypto.randomUUID(), room, event, payload }),
  };
}
/** Recovery is explicit: replay retained changes or load an authoritative snapshot. */
export async function recoverState<T>(options: {
  cursor: string;
  replay: (cursor: string) => Promise<{ gap: boolean; changes: readonly T[]; cursor: string }>;
  snapshot: () => Promise<{ state: T; cursor: string }>;
  apply: (value: T, mode: 'change' | 'snapshot') => void;
}) {
  const replay = await options.replay(options.cursor);
  if (replay.gap) {
    const snapshot = await options.snapshot();
    options.apply(snapshot.state, 'snapshot');
    return snapshot.cursor;
  }
  for (const change of replay.changes) options.apply(change, 'change');
  return replay.cursor;
}
