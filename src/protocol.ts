import { SocketError } from './errors.ts';

export const PROTOCOL_VERSION = 1;
export const SUBPROTOCOL = 'vibesocket.v1';
export type Frame = { v: 1 } & (
  | { type: 'hello'; contract: number }
  | { type: 'event'; event: string; payload: unknown; room?: string; subscription?: string }
  | { type: 'request'; id: string; event: string; payload: unknown; key?: string }
  | { type: 'result'; id: string; payload: unknown }
  | { type: 'error'; id?: string; code: string; message: string }
  | { type: 'join' | 'leave'; id: string; room: string }
  | { type: 'cancel'; id: string }
  | { type: 'ping' | 'pong' }
);
export const bytes = (value: string): number => new TextEncoder().encode(value).byteLength;
const name = (value: unknown): value is string => typeof value === 'string' && value.length > 0 && value.length <= 256;
export function parseFrame(raw: string, limit = 65536, maxDepth = 24): Frame {
  if (bytes(raw) > limit) throw new SocketError('INVALID_PAYLOAD', 'Message exceeds the byte limit.');
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw new SocketError('INVALID_PAYLOAD', 'Expected a JSON text frame.'); }
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new SocketError('INVALID_PAYLOAD');
  const f = value as Record<string, unknown>;
  if (f.v !== 1) throw new SocketError('PROTOCOL_MISMATCH');
  let keys: string[];
  switch (f.type) {
    case 'hello': if (!Number.isSafeInteger(f.contract) || Number(f.contract) < 1) throw new SocketError('INVALID_PAYLOAD'); keys = ['contract']; break;
    case 'event': if (!name(f.event) || !Object.hasOwn(f, 'payload') || (f.room !== undefined && !name(f.room)) || (f.subscription !== undefined && !name(f.subscription))) throw new SocketError('INVALID_PAYLOAD'); keys = ['event', 'payload', 'room', 'subscription']; break;
    case 'request': if (!name(f.id) || !name(f.event) || !Object.hasOwn(f, 'payload') || (f.key !== undefined && !name(f.key))) throw new SocketError('INVALID_PAYLOAD'); keys = ['id', 'event', 'payload', 'key']; break;
    case 'result': if (!name(f.id) || !Object.hasOwn(f, 'payload')) throw new SocketError('INVALID_PAYLOAD'); keys = ['id', 'payload']; break;
    case 'error': if (!name(f.code) || typeof f.message !== 'string' || f.message.length > 512 || (f.id !== undefined && !name(f.id))) throw new SocketError('INVALID_PAYLOAD'); keys = ['id', 'code', 'message']; break;
    case 'join': case 'leave': if (!name(f.id) || !name(f.room)) throw new SocketError('INVALID_PAYLOAD'); keys = ['id', 'room']; break;
    case 'cancel': if (!name(f.id)) throw new SocketError('INVALID_PAYLOAD'); keys = ['id']; break;
    case 'ping': case 'pong': keys = []; break;
    default: throw new SocketError('INVALID_PAYLOAD', 'Unknown frame type.');
  }
  if (Object.keys(f).some(k => !['v', 'type', ...keys].includes(k))) throw new SocketError('INVALID_PAYLOAD', 'Unexpected frame field.');
  const stack: [unknown, number][] = [[value, 0]];
  while (stack.length) {
    const [item, depth] = stack.pop()!;
    if (depth > maxDepth) throw new SocketError('INVALID_PAYLOAD', 'Message nesting exceeds the limit.');
    if (item && typeof item === 'object') for (const child of Object.values(item)) stack.push([child, depth + 1]);
  }
  return value as Frame;
}
export const encodeFrame = (frame: Frame): string => JSON.stringify(frame);
