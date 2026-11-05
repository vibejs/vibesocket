import * as v from 'valibot';
import { expect, it, vi } from 'vitest';

vi.mock('@usevibejs/query', () => {
  throw new Error('@usevibejs/query must not load with the socket runtime');
});

import { defineSocketContract } from '../src/contract.ts';
import { createSocketClient } from '../src/index.ts';
import { bindQuery } from '../src/query.ts';

it('keeps the socket and bindQuery loadable when the query peer is absent', () => {
  const contract = defineSocketContract({
    server: { refreshed: v.object({ id: v.string() }) },
  });
  const socket = createSocketClient({ contract, url: 'ws://127.0.0.1:9', lifecycle: false });
  const invalidateQueries = vi.fn();
  const stop = bindQuery(socket, { invalidateQueries }, { refreshed: ['items'] });
  expect(invalidateQueries).not.toHaveBeenCalled();
  stop();
  socket.dispose();
});
