export type ConnectionStatus =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'disconnected'
  | 'closed'
  | 'failed';
export const connectionTransitions: Readonly<
  Record<ConnectionStatus, readonly ConnectionStatus[]>
> = Object.freeze({
  idle: ['connecting', 'disconnected', 'closed'],
  connecting: ['connected', 'reconnecting', 'failed', 'disconnected', 'closed'],
  connected: ['reconnecting', 'failed', 'disconnected', 'closed'],
  reconnecting: ['connected', 'failed', 'disconnected', 'closed'],
  disconnected: ['connecting', 'closed'],
  failed: ['connecting', 'disconnected', 'closed'],
  closed: [],
});
export function assertTransition(from: ConnectionStatus, to: ConnectionStatus) {
  if (from !== to && !connectionTransitions[from].includes(to))
    throw new Error(`Invalid socket transition: ${from} -> ${to}`);
}
