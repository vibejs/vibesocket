import { createServer, type RequestListener } from 'node:http';
import { mkdirSync, existsSync, readFileSync, writeFileSync, chmodSync } from 'node:fs';
import { toNodeHandler } from 'srvx/node';
import { createServer as createViteServer } from 'vite';
import { attachNodeSocket } from '../src/node.ts';
import { createDemoBackend } from './backend.ts';

mkdirSync('.data', { recursive: true, mode: 0o700 });
if (!existsSync('.data/secret'))
  writeFileSync('.data/secret', crypto.randomUUID() + crypto.randomUUID(), { mode: 0o600 });
const backend = await createDemoBackend({
  database: '.data/demo.sqlite',
  secret: readFileSync('.data/secret', 'utf8'),
  baseURL: 'http://127.0.0.1:5173',
  origins: ['http://localhost:5173', 'http://127.0.0.1:5173'],
});
chmodSync('.data/demo.sqlite', 0o600);
const http = createServer(toNodeHandler(backend.app.fetch) as RequestListener);
const adapter = attachNodeSocket(http, backend.core, {
  allowedOrigins: ['http://localhost:5173', 'http://127.0.0.1:5173'],
  authenticate: backend.authenticate,
});
http.listen(3100, '127.0.0.1');
const vite = await createViteServer({ configFile: 'site/vite.config.ts' });
await vite.listen();
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  await vite.close();
  await adapter.close();
  await new Promise<void>((resolve) => http.close(() => resolve()));
  await backend.close();
}
process.once('SIGINT', () => {
  void close();
});
process.once('SIGTERM', () => {
  void close();
});
