import { createServer, type RequestListener } from 'node:http';
import {
  mkdirSync,
  existsSync,
  readFileSync,
  writeFileSync,
  chmodSync,
  mkdtempSync,
  rmSync,
} from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { once } from 'node:events';
import { toNodeHandler } from 'srvx/node';
import { createServer as createViteServer } from 'vite';
import { attachNodeSocket } from '../src/node.ts';
import { createDemoBackend } from './backend.ts';

const temporary = process.env.VIBESOCKET_TEST_MODE === '1';
const dataDirectory = temporary ? mkdtempSync(join(tmpdir(), 'vibesocket-e2e-')) : '.data';
const secretPath = join(dataDirectory, 'secret'),
  databasePath = join(dataDirectory, 'demo.sqlite');
mkdirSync(dataDirectory, { recursive: true, mode: 0o700 });
if (!existsSync(secretPath))
  writeFileSync(secretPath, crypto.randomUUID() + crypto.randomUUID(), { mode: 0o600 });
const backend = await createDemoBackend({
  database: databasePath,
  secret: readFileSync(secretPath, 'utf8'),
  baseURL: 'http://127.0.0.1:5173',
  origins: ['http://localhost:5173', 'http://127.0.0.1:5173'],
});
chmodSync(databasePath, 0o600);
const handleHTTP = toNodeHandler(backend.app.fetch) as RequestListener;
const http = createServer((request, response) => {
  // Override the private development source header; never trust client-supplied forwarding.
  request.headers['x-vibesocket-source'] = request.socket.remoteAddress ?? '127.0.0.1';
  handleHTTP(request, response);
});
const adapter = attachNodeSocket(http, backend.core, {
  allowedOrigins: ['http://localhost:5173', 'http://127.0.0.1:5173'],
  authenticate: backend.authenticate,
});
http.listen(3100, '127.0.0.1');
// Middleware mode leaves process signals and both HTTP listeners with this bootstrap.
const uiHttp = createServer();
const vite = await createViteServer({
  configFile: 'site/vite.config.ts',
  server: { middlewareMode: true, ws: { server: uiHttp } },
});
uiHttp.on('request', vite.middlewares);
uiHttp.listen(5173, '127.0.0.1');
await once(uiHttp, 'listening');
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  await adapter.close();
  await backend.close();
  await vite.close();
  await new Promise<void>((resolve) => uiHttp.close(() => resolve()));
  await new Promise<void>((resolve) => http.close(() => resolve()));
  if (temporary) rmSync(dataDirectory, { recursive: true, force: true });
}
process.once('SIGINT', () => {
  void close();
});
process.once('SIGTERM', () => {
  void close();
});
