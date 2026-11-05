import { QueryClient } from '@usevibejs/query';
import { describe, expect, it, vi } from 'vitest';
import { bindQuery, type QueryInvalidator, type QuerySocket } from '../src/query.ts';

function socket() {
  const listeners = new Map<string, Set<(payload: unknown, metadata: { room?: string }) => void>>();
  const api = {
    on(event: string, listener: (payload: unknown, metadata: { room?: string }) => void) {
      let set = listeners.get(event);
      if (!set) listeners.set(event, (set = new Set()));
      set.add(listener);
      return () => {
        set.delete(listener);
      };
    },
    emit(event: string) {
      for (const listener of [...(listeners.get(event) ?? [])]) listener({ ok: true }, {});
    },
  };
  return api;
}

describe('bindQuery', () => {
  it('invalidates one key, several keys, and ignores blank keys', async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const invalidateQueries = vi.spyOn(client, 'invalidateQueries');
    const bus = socket();
    const stop = bindQuery(bus, client, {
      'task:updated': ['tasks', 'board'],
      'task:deleted': [['tasks'], ['board'], []],
      'task:noop': [],
      'task:missing': undefined,
      'task:blank': [[]],
    });
    bus.emit('task:updated');
    bus.emit('task:deleted');
    bus.emit('task:noop');
    bus.emit('task:missing');
    bus.emit('task:blank');
    bus.emit('other');
    expect(invalidateQueries).toHaveBeenCalledTimes(3);
    expect(invalidateQueries).toHaveBeenNthCalledWith(1, { queryKey: ['tasks', 'board'] });
    expect(invalidateQueries).toHaveBeenNthCalledWith(2, { queryKey: ['tasks'] });
    expect(invalidateQueries).toHaveBeenNthCalledWith(3, { queryKey: ['board'] });
    stop();
    bus.emit('task:updated');
    expect(invalidateQueries).toHaveBeenCalledTimes(3);
    stop();
    await client.cancelQueries();
  });

  it('isolates thrown and rejected invalidations and accepts non-promises', async () => {
    const bus = socket();
    const seen: unknown[] = [];
    const client: QueryInvalidator = {
      invalidateQueries(filters) {
        seen.push(filters.queryKey[0]);
        if (filters.queryKey[0] === 'throw') throw new Error('cache');
        if (filters.queryKey[0] === 'reject') return Promise.reject(new Error('refetch'));
        if (filters.queryKey[0] === 'null') return null;
        if (filters.queryKey[0] === 'object') return { settled: true };
        return undefined;
      },
    };
    bindQuery(bus, client, {
      changed: [['throw'], ['reject'], ['null'], ['object'], ['plain']],
    });
    bus.emit('changed');
    await Promise.resolve();
    expect(seen).toEqual(['throw', 'reject', 'null', 'object', 'plain']);
  });

  it('accepts the socket client shape', () => {
    const bus: QuerySocket = socket();
    const stop = bindQuery(bus, { invalidateQueries: () => undefined }, { ping: ['presence'] });
    stop();
  });
});
