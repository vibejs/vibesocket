import { expect, it } from 'vitest';
import { build } from 'vite';
import { Miniflare } from 'miniflare';
import { resolve } from 'node:path';
it('runs real Worker upgrades, authorization, broadcast, receipts and attachment reconstruction', async () => {
  await build({
    configFile: false,
    logLevel: 'error',
    build: {
      target: 'es2022',
      lib: { entry: resolve('test/worker.ts'), formats: ['es'], fileName: () => 'worker.js' },
      outDir: '.wrangler/test',
      emptyOutDir: true,
      minify: false,
    },
  });
  const mf = new Miniflare({
    modules: true,
    scriptPath: '.wrangler/test/worker.js',
    compatibilityDate: '2026-07-01',
    durableObjects: { ROOM: 'Room' },
  });
  try {
    const headers = {
      upgrade: 'websocket',
      origin: 'https://app.test',
      cookie: 'session=alice',
      'sec-websocket-protocol': 'vibesocket.v1',
    };
    expect(
      (
        await mf.dispatchFetch('http://test/', {
          headers: { ...headers, origin: 'https://evil.test' },
        })
      ).status,
    ).toBe(403);
    expect(
      (await mf.dispatchFetch('http://test/', { headers: { ...headers, cookie: '' } })).status,
    ).toBe(401);
    const first = await mf.dispatchFetch('http://test/', { headers }),
      second = await mf.dispatchFetch('http://test/', { headers });
    expect(first.status).toBe(101);
    const a = first.webSocket!,
      b = second.webSocket!;
    const received: Record<string, unknown>[] = [];
    const waiters: {
      match: (frame: Record<string, unknown>) => boolean;
      resolve: (value: Record<string, unknown>) => void;
    }[] = [];
    for (const ws of [a, b]) {
      ws.addEventListener('message', (raw) => {
        const frame = JSON.parse(raw.data as string) as Record<string, unknown>;
        received.push(frame);
        if (frame.receipt) ws.send(JSON.stringify({ v: 1, type: 'receipt', id: frame.receipt }));
        for (const waiter of [...waiters])
          if (waiter.match(frame)) {
            waiters.splice(waiters.indexOf(waiter), 1);
            waiter.resolve(frame);
          }
      });
      ws.accept();
      ws.send('{"v":1,"type":"hello","contract":1}');
    }
    const wait = (match: (frame: Record<string, unknown>) => boolean) =>
      new Promise<Record<string, unknown>>((resolve) => {
        waiters.push({ match, resolve });
      });
    for (const [ws, id] of [
      [a, 'a'],
      [b, 'b'],
    ] as const) {
      const joined = wait((f) => f.type === 'result' && f.id === id);
      ws.send(JSON.stringify({ v: 1, type: 'join', id, room: 'org:a' }));
      await joined;
    }
    const denied = wait((f) => f.id === 'denied');
    a.send('{"v":1,"type":"join","id":"denied","room":"org:b"}');
    expect((await denied).code).toBe('FORBIDDEN');
    const restored = await mf.dispatchFetch('http://test/restore');
    expect(await restored.json()).toEqual({ connections: 2, rooms: 1 });
    const messages = Promise.all(
      ['a', 'b'].map((id) =>
        wait(
          (f) =>
            f.type === 'event' && f.subscription === id && f.payload === 'after reconstruction',
        ),
      ),
    );
    a.send('{"v":1,"type":"event","event":"say","payload":"after reconstruction"}');
    await messages;
    expect(received.some((f) => f.type === 'event' && f.subscription === 'a')).toBe(true);
    const pong = wait((f) => f.type === 'pong');
    a.send('{"v":1,"type":"ping"}');
    await pong;
    const overflow = new Promise<number>((resolve) =>
      a.addEventListener('close', (event) => resolve(event.code)),
    );
    a.send('{"v":1,"type":"request","id":"large","event":"large","payload":"boundary"}');
    expect(await overflow).toBe(1009);
    b.close(1000);
  } finally {
    await mf.dispose();
  }
}, 30000);
