import { DatabaseSync } from 'node:sqlite';
import { betterAuth } from 'better-auth';
import { getMigrations } from 'better-auth/db/migration';
import { createAuthMiddleware } from '@usevibejs/auth/server';
import { Module, VibeFactory, JsonLogger } from '@usevibejs/server';
import { createSocketServer } from '../src/server.ts';
import { createSSEChannel, createSSEStream } from '../src/sse.ts';
import {
  createSessionAuthenticator,
  createTypingTracker,
  presenceSnapshot,
} from '../src/adapters.ts';
import { SocketError } from '../src/errors.ts';
import { demoContract, streamEvents, type taskSchema } from './contract.ts';
import type { InferOutput } from 'valibot';

type Task = InferOutput<typeof taskSchema>;
type Board = { revision: number; tasks: Task[] };
export interface DemoPrincipal {
  id: string;
  org: string;
  role: string;
  sessionId: string;
  expiresAt: number;
}
export const demoPassword = 'local-realtime-example-2026';
export async function createDemoBackend(options: {
  database: string;
  secret: string;
  baseURL: string;
  origins: string[];
  simulate?: boolean;
}) {
  const db = new DatabaseSync(options.database);
  const auth = betterAuth({
    database: db,
    baseURL: options.baseURL,
    secret: options.secret,
    trustedOrigins: options.origins,
    emailAndPassword: { enabled: true, minPasswordLength: 12 },
    session: { cookieCache: { enabled: false } },
    advanced: { useSecureCookies: false },
    rateLimit: { enabled: true, customRules: { '/sign-in/email': { window: 60, max: 100 } } },
  });
  await (await getMigrations(auth.options)).runMigrations();
  db.exec(
    'CREATE TABLE IF NOT EXISTS members (user_id TEXT PRIMARY KEY, org TEXT NOT NULL, role TEXT NOT NULL); CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, room TEXT NOT NULL, text TEXT NOT NULL, user TEXT NOT NULL, at TEXT NOT NULL); CREATE TABLE IF NOT EXISTS boards (org TEXT PRIMARY KEY, revision INTEGER NOT NULL, tasks TEXT NOT NULL); CREATE TABLE IF NOT EXISTS stream_events (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL);',
  );
  for (const [name, org, role] of [
    ['Ada', 'orbit', 'admin'],
    ['Lin', 'orbit', 'member'],
    ['Eve', 'lattice', 'member'],
  ]) {
    const email = `${name.toLowerCase()}@demo.test`;
    let user = db.prepare('SELECT id FROM user WHERE email=?').get(email) as
      | { id: string }
      | undefined;
    if (!user)
      user = (await auth.api.signUpEmail({ body: { email, password: demoPassword, name } })).user;
    db.prepare('INSERT OR IGNORE INTO members VALUES (?,?,?)').run(user.id, org, role);
  }
  for (const org of ['orbit', 'lattice'])
    db.prepare('INSERT OR IGNORE INTO boards VALUES (?,0,?)').run(
      org,
      JSON.stringify([
        { id: 'transport', title: 'Review transport boundaries', column: 'todo' },
        { id: 'auth', title: 'Verify room permissions', column: 'progress' },
        { id: 'stream', title: 'Stream the first event', column: 'done' },
      ]),
    );
  const authenticate = createSessionAuthenticator(auth, (session) => {
    const member = db
      .prepare('SELECT org,role FROM members WHERE user_id=?')
      .get(session.user.id) as { org: string; role: string } | undefined;
    return {
      id: session.user.id,
      org: member?.org ?? '',
      role: member?.role ?? '',
      sessionId: session.session.id,
    };
  });
  const valid = (p: DemoPrincipal) => {
    const row = db
      .prepare('SELECT expiresAt FROM session WHERE id=? AND userId=?')
      .get(p.sessionId, p.id) as { expiresAt: number | string } | undefined;
    return !!row && new Date(row.expiresAt).getTime() > Date.now();
  };
  const memberOf = (p: DemoPrincipal) =>
    db.prepare('SELECT org,role FROM members WHERE user_id=?').get(p.id) as
      | { org: string; role: string }
      | undefined;
  const core = createSocketServer({
    contract: demoContract,
    validatePrincipal: valid,
    authorize: (p: DemoPrincipal, op) => {
      const member = memberOf(p);
      if (!member || member.org !== p.org) return false;
      if (op.action === 'request' && op.event === 'task:create' && member.role !== 'admin')
        return false;
      if (
        op.room &&
        ![
          `chat:${member.org}:general`,
          `chat:${member.org}:engineering`,
          `board:${member.org}`,
        ].includes(op.room)
      )
        return false;
      return true;
    },
  });
  const typing = createTypingTracker({
    onChange: (room, users) => {
      void core.publish(room, 'typing:updated', { room, users: [...users] });
    },
  });
  core.on('typing', (value, ctx) => {
    typing.set(ctx.connection, value.room, value.active);
  });
  function checkRoom(p: DemoPrincipal, room: string) {
    if (![`chat:${p.org}:general`, `chat:${p.org}:engineering`].includes(room))
      throw new SocketError('FORBIDDEN');
  }
  core.handle('message:send', async (payload, ctx) => {
    checkRoom(ctx.connection.principal, payload.room);
    if (!ctx.connection.rooms.has(payload.room)) ctx.fail('FORBIDDEN');
    const message = {
      id: crypto.randomUUID(),
      ...payload,
      user: ctx.connection.principal.id,
      at: new Date().toISOString(),
    };
    db.prepare('INSERT INTO messages VALUES (?,?,?,?,?)').run(
      message.id,
      message.room,
      message.text,
      message.user,
      message.at,
    );
    await core.publish(payload.room, 'message:created', message);
    return message;
  });
  core.on('message:send', async (payload, ctx) => {
    checkRoom(ctx.connection.principal, payload.room);
    if (!ctx.connection.rooms.has(payload.room)) ctx.fail('FORBIDDEN');
    const message = {
      id: crypto.randomUUID(),
      ...payload,
      user: ctx.connection.principal.id,
      at: new Date().toISOString(),
    };
    db.prepare('INSERT INTO messages VALUES (?,?,?,?,?)').run(
      message.id,
      message.room,
      message.text,
      message.user,
      message.at,
    );
    await core.publish(payload.room, 'message:created', message);
  });
  core.handle('history', ({ room }, ctx) => {
    checkRoom(ctx.connection.principal, room);
    const rows = db
      .prepare('SELECT * FROM messages WHERE room=? ORDER BY rowid DESC LIMIT 200')
      .all(room) as unknown as InferOutput<(typeof demoContract.server)['message:created']>[];
    const selected: typeof rows = [];
    let total = 2;
    for (const message of rows) {
      const size = Buffer.byteLength(JSON.stringify(message)) + 1;
      if (total + size > 49152) break;
      selected.push(message);
      total += size;
    }
    return selected.reverse();
  });
  const readBoard = (org: string): Board => {
    const row = db.prepare('SELECT revision,tasks FROM boards WHERE org=?').get(org) as {
      revision: number;
      tasks: string;
    };
    return { revision: row.revision, tasks: JSON.parse(row.tasks) as Task[] };
  };
  core.handle('board', (_payload, ctx) => readBoard(ctx.connection.principal.org));
  async function changeBoard(
    p: DemoPrincipal,
    revision: number,
    mutate: (tasks: Task[]) => Task[],
  ) {
    db.exec('BEGIN IMMEDIATE');
    let board: Board;
    try {
      board = readBoard(p.org);
      if (board.revision !== revision) throw new SocketError('CONFLICT');
      board = { revision: board.revision + 1, tasks: mutate(board.tasks) };
      db.prepare('UPDATE boards SET revision=?,tasks=? WHERE org=?').run(
        board.revision,
        JSON.stringify(board.tasks),
        p.org,
      );
      db.exec('COMMIT');
    } catch (error) {
      db.exec('ROLLBACK');
      throw error;
    }
    await core.publish(`board:${p.org}`, 'board:updated', board);
    return board;
  }
  core.handle('task:move', (payload, ctx) =>
    changeBoard(ctx.connection.principal, payload.expectedRevision, (tasks) => {
      const task = tasks.find((t) => t.id === payload.id);
      if (!task) throw new SocketError('NOT_FOUND');
      const remaining = tasks.filter((t) => t.id !== task.id),
        sameColumn = remaining.filter((t) => t.column === payload.column);
      const before = sameColumn[payload.index],
        position = before ? remaining.indexOf(before) : remaining.length;
      remaining.splice(position, 0, { ...task, column: payload.column });
      return remaining;
    }),
  );
  core.handle('task:create', (payload, ctx) =>
    changeBoard(ctx.connection.principal, payload.expectedRevision, (tasks) => {
      if (tasks.length >= 100) ctx.fail('RATE_LIMITED');
      return [...tasks, { id: crypto.randomUUID(), title: payload.title, column: 'todo' }];
    }),
  );
  const channel = createSSEChannel(
    { metric: streamEvents.metric },
    {
      replay: {
        async readAfter(cursor) {
          if (!/^[1-9][0-9]{0,12}$/.test(cursor)) return { gap: true, events: [] };
          const bounds = db
            .prepare('SELECT min(id) AS first,max(id) AS last FROM stream_events')
            .get() as { first: number | null; last: number | null };
          const gap =
            !bounds.first || Number(cursor) < bounds.first - 1 || Number(cursor) > bounds.last!;
          return {
            gap,
            events: gap
              ? []
              : (
                  db
                    .prepare('SELECT id,data FROM stream_events WHERE id>? ORDER BY id LIMIT 1000')
                    .all(Number(cursor)) as unknown as { id: number; data: string }[]
                ).map((row) => ({ id: String(row.id), event: 'metric', data: row.data })),
          };
        },
      },
    },
  );
  const aiStreams = new Set<ReturnType<typeof createSSEStream>>();
  class Root {}
  Module({})(Root);
  const app = await VibeFactory.create(Root, {
    logger: new JsonLogger({ level: 'error' }),
    middleware: [
      createAuthMiddleware(auth),
      async (req, next) => {
        const path = new URL(req.url).pathname;
        if (path === '/health') return Response.json({ ok: true });
        if (!path.startsWith('/api/')) return next();
        const p = await authenticate(req);
        if (!p || !p.org) return new Response(null, { status: 401 });
        if (path === '/api/me') return Response.json(p);
        if (path === '/api/events')
          return channel.response(req, {
            allowed: () => valid(p),
            expiresAt: p.expiresAt,
            key: p.id,
          });
        if (path === '/api/ai') {
          const words =
            'A small protocol. A real connection. Your application owns its data.'.split(' ');
          let index = 0;
          let timer: ReturnType<typeof setInterval>;
          const stream = createSSEStream({
            signal: req.signal,
            onClose: () => {
              clearInterval(timer);
              aiStreams.delete(stream);
            },
          });
          if (stream.closed) return stream.response;
          aiStreams.add(stream);
          timer = setInterval(() => {
            if (!valid(p)) {
              stream.close();
              return;
            }
            if (index < words.length)
              stream.send({
                event: 'token',
                id: String(index),
                data: JSON.stringify(words[index++] + ' '),
              });
            else {
              stream.send({ event: 'done', data: 'true' });
              stream.close();
            }
          }, 120);
          return stream.response;
        }
        if (path === '/api/admin/revoke' && req.method === 'POST') {
          if (
            !options.origins.includes(req.headers.get('origin') ?? '') ||
            memberOf(p)?.role !== 'admin'
          )
            return new Response(null, { status: 403 });
          db.prepare('DELETE FROM session WHERE userId=?').run(p.id);
          core.revoke(p.id);
          return Response.json({ revoked: true });
        }
        return next();
      },
    ],
  });
  let tick = Number(
    (db.prepare('SELECT max(id) as id FROM stream_events').get() as { id: number }).id ?? 0,
  );
  async function sample() {
    tick++;
    const data = {
      tick,
      requests: 1200 + ((tick * 137) % 900),
      latency: 12 + ((tick * 7) % 31),
      cpu: 18 + ((tick * 3) % 50),
      at: new Date().toISOString(),
    };
    const result = db
      .prepare('INSERT INTO stream_events(data) VALUES (?)')
      .run(JSON.stringify(data));
    db.prepare('DELETE FROM stream_events WHERE id <= ?').run(
      Number(result.lastInsertRowid) - 1000,
    );
    await channel.publish('metric', data, String(result.lastInsertRowid));
    for (const org of ['orbit', 'lattice']) {
      await core.publish(`board:${org}`, 'presence:updated', {
        room: `board:${org}`,
        users: presenceSnapshot(core.connections, { room: `board:${org}` }),
      });
      for (const suffix of ['general', 'engineering']) {
        const room = `chat:${org}:${suffix}`;
        await core.publish(room, 'presence:updated', {
          room,
          users: presenceSnapshot(core.connections, { room }),
        });
      }
    }
  }
  const timer =
    options.simulate === false
      ? undefined
      : setInterval(() => {
          void sample();
        }, 1000);
  return {
    app,
    auth,
    authenticate,
    core,
    db,
    sample,
    channel,
    async close() {
      clearInterval(timer);
      typing.close();
      channel.close();
      for (const stream of aiStreams) stream.close();
      core.shutdown();
      await app.close();
      db.close();
    },
  };
}
