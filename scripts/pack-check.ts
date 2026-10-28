import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { build } from 'vite';
const root = process.cwd(),
  directory = await mkdtemp(join(tmpdir(), 'vibesocket-consumer-'));
try {
  const tarball = join(directory, 'socket.tgz');
  execFileSync('pnpm', ['pack', '--out', tarball], { cwd: root, stdio: 'pipe' });
  const listing = execFileSync('tar', ['-tzf', tarball], { encoding: 'utf8' });
  if (/node_modules|\.data\/|vendor\/|site\//.test(listing))
    throw new Error('Development files leaked into package');
  await writeFile(
    join(directory, 'package.json'),
    JSON.stringify({
      name: 'consumer',
      private: true,
      type: 'module',
      dependencies: {
        '@usevibejs/socket': `file:${tarball}`,
        '@usevibejs/ui': '0.1.0',
        '@types/node': '24.19.2',
        valibot: '1.5.0',
      },
    }),
  );
  execFileSync('pnpm', ['install', '--ignore-scripts'], { cwd: directory, stdio: 'pipe' });
  const manifest = JSON.parse(
    await readFile(join(directory, 'node_modules/@usevibejs/socket/package.json'), 'utf8'),
  ) as { exports: Record<string, unknown> };
  if (JSON.stringify(manifest).includes(root))
    throw new Error('Absolute workspace path leaked into package manifest');
  await writeFile(
    join(directory, 'consumer.ts'),
    `import * as v from 'valibot';
import { defineSocketContract } from '@usevibejs/socket/contract';
import { createSocketClient } from '@usevibejs/socket';
import { createSocketPlugin } from '@usevibejs/socket/client';
import { createSocketServer } from '@usevibejs/socket/server';
import { attachNodeSocket } from '@usevibejs/socket/node';
import { createDurableSocket } from '@usevibejs/socket/cloudflare';
import { createSSEStream } from '@usevibejs/socket/sse';
import { createMemoryPubSub } from '@usevibejs/socket/adapters';
const contract = defineSocketContract({ client: { send: v.string() }, server: { receive: v.number() } });
const client = createSocketClient({ contract, url: 'ws://localhost' });
void client.send('send', 'hello');
// @ts-expect-error wrong direction
void client.send('receive', 42);
void [createSocketPlugin, createSocketServer, attachNodeSocket, createDurableSocket, createSSEStream, createMemoryPubSub];`,
  );
  await writeFile(
    join(directory, 'tsconfig.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2024',
        module: 'NodeNext',
        moduleResolution: 'NodeNext',
        strict: true,
        noEmit: true,
        types: ['node'],
        lib: ['ES2024', 'DOM', 'DOM.Iterable'],
      },
      include: ['consumer.ts'],
    }),
  );
  execFileSync(
    resolve('node_modules/typescript/bin/tsc'),
    ['-p', join(directory, 'tsconfig.json')],
    { cwd: directory, stdio: 'pipe' },
  );
  for (const path of Object.keys(manifest.exports))
    execFileSync(
      process.execPath,
      [
        '--input-type=module',
        '-e',
        `await import(${JSON.stringify('@usevibejs/socket' + (path === '.' ? '' : path.slice(1)))})`,
      ],
      { cwd: directory, stdio: 'pipe' },
    );
  await writeFile(
    join(directory, 'network.mjs'),
    `
import { createServer } from 'node:http';
import { once } from 'node:events';
import * as v from 'valibot';
import { defineSocketContract, createSocketClient } from '@usevibejs/socket';
import { createSocketServer } from '@usevibejs/socket/server';
import { attachNodeSocket } from '@usevibejs/socket/node';
const contract = defineSocketContract({ requests: { echo: { input: v.number(), output: v.number() } } });
const core = createSocketServer({ contract, authorize: () => true }); core.handle('echo', value => value);
const server = createServer((_req, res) => res.end('ok'));
const adapter = attachNodeSocket(server, core, { allowedOrigins: [], allowMissingOrigin: true, authenticate: () => ({ id: 'packed-consumer' }) });
server.listen(0, '127.0.0.1'); await once(server, 'listening');
const client = createSocketClient({ contract, url: 'ws://127.0.0.1:' + server.address().port + '/socket', heartbeat: false, lifecycle: false });
try { const connected = new Promise(resolve => { const off = client.observe(s => { if (s.status === 'connected') { off(); resolve(); } }); }); client.connect(); await connected; if (await client.request('echo', 42) !== 42) throw Error('Packed RPC failed'); }
finally { client.dispose(); await adapter.close(); await new Promise(resolve => server.close(resolve)); }
`,
  );
  execFileSync(process.execPath, [join(directory, 'network.mjs')], {
    cwd: directory,
    stdio: 'pipe',
    timeout: 15000,
  });
  await writeFile(
    join(directory, 'browser.ts'),
    `export { createSocketClient } from '@usevibejs/socket'; export { createSocketPlugin } from '@usevibejs/socket/client';`,
  );
  await build({
    configFile: false,
    root: directory,
    logLevel: 'error',
    build: {
      lib: { entry: join(directory, 'browser.ts'), formats: ['es'], fileName: () => 'browser.js' },
      outDir: join(directory, 'bundle'),
      minify: false,
    },
  });
  const browser = await readFile(join(directory, 'bundle/browser.js'), 'utf8');
  if (/node:http|node:net|WebSocketServer|acceptWebSocket|createSSEChannel/.test(browser))
    throw new Error('Server runtime entered the browser bundle');
  await writeFile(join(directory, 'tree.ts'), `export { retryDelay } from '@usevibejs/socket';`);
  await build({
    configFile: false,
    root: directory,
    logLevel: 'error',
    build: {
      lib: { entry: join(directory, 'tree.ts'), formats: ['es'], fileName: () => 'tree.js' },
      outDir: join(directory, 'tree'),
      minify: false,
    },
  });
  const tree = await readFile(join(directory, 'tree/tree.js'), 'utf8');
  if (/WebSocket|EventSource|createSocketClient/.test(tree))
    throw new Error('Tree shaking retained unused transport code');
  console.log(
    JSON.stringify(
      {
        exports: Object.keys(manifest.exports),
        strictConsumer: 'passed',
        runtimeImports: 'passed',
        packedWebSocketRPC: 'passed',
        browserIsolation: 'passed',
        treeShaking: 'passed',
        browserBytes: Buffer.byteLength(browser),
        treeBytes: Buffer.byteLength(tree),
      },
      null,
      2,
    ),
  );
} catch (error) {
  if (error && typeof error === 'object' && 'stdout' in error)
    process.stderr.write(String(error.stdout));
  throw error;
} finally {
  await rm(directory, { recursive: true, force: true });
}
