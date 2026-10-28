import { createServer } from 'node:http';
import type { RequestListener } from 'node:http';
import { Module, VibeFactory, JsonLogger } from '@usevibejs/server';
import { toNodeHandler } from 'srvx/node';
import { createServer as createViteServer } from 'vite';
import { createSocketServer } from '../src/server.ts';
import { attachNodeSocket } from '../src/node.ts';
import { demoContract } from './contract.ts';

class AppModule {}
Module({})(AppModule);
const core = createSocketServer({ contract: demoContract, authorize: (_p, op) => !op.room || ['chat:general', 'chat:engineering'].includes(op.room) });
core.on('message:send', async (payload, ctx) => {
  if (!ctx.connection.rooms.has(payload.room)) ctx.fail('FORBIDDEN');
  await core.publish(payload.room, 'message:created', { id: crypto.randomUUID(), room: payload.room, text: payload.text, user: ctx.connection.principal.id, at: new Date().toISOString() });
});
const app = await VibeFactory.create(AppModule, { logger: new JsonLogger({ level: 'error' }), middleware: [async (req, next) => new URL(req.url).pathname === '/health' ? Response.json({ ok: true }) : next()] });
const http = createServer(toNodeHandler(app.fetch) as RequestListener);
const adapter = attachNodeSocket(http, core, { allowedOrigins: ['http://localhost:5173', 'http://127.0.0.1:5173'], authenticate: () => ({ id: crypto.randomUUID() }) });
http.listen(3100, '127.0.0.1');
const vite = await createViteServer({ configFile: 'site/vite.config.ts' }); await vite.listen();
async function close() { await vite.close(); await adapter.close(); await new Promise<void>(resolve => http.close(() => resolve())); await app.close(); }
process.once('SIGINT', () => { void close(); }); process.once('SIGTERM', () => { void close(); });
