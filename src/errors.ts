export const errorCodes = [
  'CONNECTION_LOST',
  'AUTH_REQUIRED',
  'FORBIDDEN',
  'INVALID_PAYLOAD',
  'UNSUPPORTED_EVENT',
  'TIMEOUT',
  'CANCELLED',
  'RATE_LIMITED',
  'PROTOCOL_MISMATCH',
  'REPLAY_UNAVAILABLE',
  'QUEUE_OVERFLOW',
  'QUEUE_EXPIRED',
  'SERVER_UNAVAILABLE',
  'INTERNAL_ERROR',
  'SLOW_CONSUMER',
] as const;
export type ErrorCode = (typeof errorCodes)[number];
export class SocketError<C extends string = ErrorCode> extends Error {
  override readonly name = 'SocketError';
  readonly code: C;
  constructor(code: C, message: string = code, options?: ErrorOptions) {
    super(message, options);
    this.code = code;
  }
}
export function publicError(error: unknown): SocketError<string> {
  return error instanceof SocketError && error.code !== 'INTERNAL_ERROR'
    ? error
    : new SocketError('INTERNAL_ERROR', 'The operation failed.');
}
