import { checkLimits } from './limits.ts';
import { SocketError } from '../errors.ts';
import { bytes } from '../protocol.ts';

export interface QueueOptions {
  maxMessages?: number;
  maxBytes?: number;
  ttl?: number;
}
export interface QueuedMessage {
  id: string;
  data: string;
  expires: number;
  size: number;
  onFailure?: (error: SocketError) => void;
  coalesce?: string;
}
/** Explicit, volatile event queue. Request frames are never replayed. */
export function createOutboundQueue(options: QueueOptions = {}, now: () => number = Date.now) {
  checkLimits(options, ['maxMessages', 'maxBytes', 'ttl']);
  const entries: QueuedMessage[] = [];
  let total = 0;
  function fail(entry: QueuedMessage, error: SocketError) {
    try {
      entry.onFailure?.(error);
    } catch {
      /* Isolate application callbacks. */
    }
  }
  function remove(index: number, error?: SocketError) {
    const [entry] = entries.splice(index, 1);
    total -= entry.size;
    if (error) fail(entry, error);
    return entry;
  }
  function expire() {
    for (let i = entries.length - 1; i >= 0; i--)
      if (entries[i].expires <= now()) remove(i, new SocketError('QUEUE_EXPIRED'));
  }
  return {
    get size() {
      expire();
      return entries.length;
    },
    get bytes() {
      expire();
      return total;
    },
    push(
      data: string,
      config: { ttl?: number; onFailure?: QueuedMessage['onFailure']; coalesce?: string } = {},
    ) {
      checkLimits(config, ['ttl']);
      expire();
      const size = bytes(data),
        replaced = config.coalesce ? entries.findIndex((e) => e.coalesce === config.coalesce) : -1;
      const previousSize = replaced >= 0 ? entries[replaced].size : 0;
      if (
        entries.length - (replaced >= 0 ? 1 : 0) >= (options.maxMessages ?? 100) ||
        total - previousSize + size > (options.maxBytes ?? 262144)
      )
        throw new SocketError('QUEUE_OVERFLOW');
      if (replaced >= 0) remove(replaced, new SocketError('CANCELLED', 'Replaced by newer state.'));
      const entry: QueuedMessage = {
        id: crypto.randomUUID(),
        data,
        expires: now() + (config.ttl ?? options.ttl ?? 10000),
        size,
        ...config,
      };
      entries.push(entry);
      total += size;
      return entry.id;
    },
    flush(send: (data: string) => boolean) {
      expire();
      while (entries.length) {
        if (!send(entries[0].data)) break;
        remove(0);
      }
    },
    cancel(id: string) {
      const index = entries.findIndex((e) => e.id === id);
      if (index >= 0) remove(index, new SocketError('CANCELLED'));
    },
    clear(error = new SocketError('CANCELLED')) {
      while (entries.length) remove(0, error);
    },
    expire,
  };
}
