import { describe, expect, it, vi } from 'vitest';
import * as v from 'valibot';
import { defineSocketContract, validate } from '../src/contract.ts';
import { parseFrame } from '../src/protocol.ts';
import { createOutboundQueue } from '../src/core/queue.ts';
import { retryDelay } from '../src/core/retry.ts';

describe('contract and protocol boundary', () => {
  it('validates Standard Schema including asynchronous schemas', async () => {
    expect(await validate(v.number(), 4)).toBe(4);
    await expect(validate(v.number(), '4')).rejects.toMatchObject({ code: 'INVALID_PAYLOAD' });
    await expect(validate(undefined, 4)).rejects.toMatchObject({ code: 'UNSUPPORTED_EVENT' });
    expect(
      await validate(
        { '~standard': { version: 1, vendor: 'test', validate: async (value) => ({ value }) } },
        5,
      ),
    ).toBe(5);
  });
  it('freezes maps and merges bidirectional schemas', () => {
    const c = defineSocketContract({ bidirectional: { echo: v.string() } });
    expect(Object.isFrozen(c.client)).toBe(true);
    expect(c.client.echo).toBe(c.server.echo);
    expect(() => defineSocketContract({ version: -1 })).toThrow();
  });
  it.each([
    'null',
    '[]',
    '{}',
    '{',
    '{"v":2,"type":"ping"}',
    '{"v":1,"type":"ping","principal":"admin"}',
    '{"v":1,"type":"request","event":"x","id":1,"payload":{}}',
  ])('rejects malformed envelopes: %s', (frame) => {
    expect(() => parseFrame(frame)).toThrow();
  });
  it('rejects oversized UTF-8 and deeply nested frames', () => {
    expect(() => parseFrame('{"v":1,"type":"event","event":"x","payload":"😀"}', 20)).toThrow();
    expect(() =>
      parseFrame(JSON.stringify({ v: 1, type: 'event', event: 'x', payload: [[[1]]] }), 1024, 2),
    ).toThrow();
  });
  it('accepts every defined command', () => {
    for (const frame of [
      { type: 'hello', contract: 1 },
      { type: 'event', event: 'x', payload: null },
      { type: 'request', id: '1', event: 'x', payload: {} },
      { type: 'result', id: '1', payload: null },
      { type: 'error', code: 'FORBIDDEN', message: 'No' },
      { type: 'join', room: 'a', id: '1' },
      { type: 'leave', room: 'a', id: '1' },
      { type: 'cancel', id: '1' },
      { type: 'ping' },
      { type: 'pong' },
    ])
      expect(parseFrame(JSON.stringify({ v: 1, ...frame })).type).toBe(frame.type);
  });
});
describe('bounded queue', () => {
  it('flushes in order, stops when backpressured, and expires messages', () => {
    let now = 0;
    const queue = createOutboundQueue({ ttl: 100, maxMessages: 2 }, () => now),
      failed = vi.fn();
    queue.push('one');
    queue.push('two', { onFailure: failed });
    expect(() => queue.push('three')).toThrow();
    const sent: string[] = [];
    queue.flush((data) => {
      if (data === 'two') return false;
      sent.push(data);
      return true;
    });
    expect(sent).toEqual(['one']);
    now = 101;
    expect(queue.size).toBe(0);
    expect(failed).toHaveBeenCalledWith(expect.objectContaining({ code: 'QUEUE_EXPIRED' }));
    expect(queue.bytes).toBe(0);
  });
  it('coalesces, cancels and limits bytes', () => {
    const queue = createOutboundQueue({ maxBytes: 6 });
    const fail = vi.fn();
    queue.push('one', { coalesce: 'cursor', onFailure: fail });
    const id = queue.push('two', { coalesce: 'cursor' });
    expect(queue.size).toBe(1);
    expect(fail).toHaveBeenCalledOnce();
    expect(() => queue.push('😀😀')).toThrow();
    queue.cancel(id);
    expect(queue.bytes).toBe(0);
    queue.push('ok');
    queue.clear();
    expect(queue.size).toBe(0);
  });
  it('backoff has a minimum, exponential cap and jitter', () => {
    expect(retryDelay(1, {}, () => 0)).toBe(250);
    expect(retryDelay(3, {}, () => 1)).toBe(2000);
    expect(retryDelay(30, {}, () => 1)).toBe(30000);
    expect(retryDelay(1, { minDelay: 0 }, () => 0)).toBe(50);
  });
});
