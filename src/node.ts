import type { IncomingMessage, Server } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocketServer } from 'ws';
import type { SocketContract } from './contract.ts';
import type { Principal, SocketServer } from './server.ts';
import { SUBPROTOCOL } from './protocol.ts';
import { checkLimits } from './core/limits.ts';

export interface NodeAdapterOptions<P extends Principal> {
  path?: string;
  baseURL?: string;
  allowedOrigins: readonly string[];
  allowMissingOrigin?: boolean;
  authenticate: (
    request: Request,
    source: { address: string | undefined; signal: AbortSignal },
  ) => P | null | Promise<P | null>;
  handshakeTimeout?: number;
  maxPayload?: number;
  maxPendingUpgrades?: number;
  maxConnectionsPerSource?: number;
  attemptsPerMinute?: number;
  /** Passive lease renewed by application messages/heartbeats; false disables idle cleanup. */
  idleTimeout?: number | false;
  diagnostic?: (event: string, fields: Readonly<Record<string, number>>) => void;
  unmatchedPath?: 'reject' | 'ignore';
  /** External limit hook. Do not trust forwarded headers unless a trusted proxy validates them. */
  allowUpgrade?: (source: string | undefined) => boolean | Promise<boolean>;
}
export function attachNodeSocket<C extends SocketContract, P extends Principal>(
  server: Server,
  core: SocketServer<C, P>,
  options: NodeAdapterOptions<P>,
) {
  checkLimits(options, ['handshakeTimeout', 'maxPayload'], false);
  if (options.idleTimeout !== false) checkLimits(options, ['idleTimeout'], false);
  checkLimits(options, ['maxPendingUpgrades', 'maxConnectionsPerSource', 'attemptsPerMinute']);
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: options.maxPayload ?? 65536,
    perMessageDeflate: false,
    handleProtocols: (protocols) => (protocols.has(SUBPROTOCOL) ? SUBPROTOCOL : false),
  });
  let disposed = false;
  const pending = new Set<Duplex>();
  const attempts = new Map<string, { since: number; count: number }>();
  const sourceConnections = new Map<string, number>();
  const metrics = {
    connectionAttempts: 0,
    connectionFailures: 0,
    authenticationFailures: 0,
    originRejections: 0,
    rateLimitRejections: 0,
    upgraded: 0,
  };
  const lease =
    options.idleTimeout === false
      ? undefined
      : setInterval(
          () => {
            for (const connection of core.connections)
              if (Date.now() - connection.lastActivity > (options.idleTimeout || 90000))
                core.close(connection.id, 4000, 'Idle connection expired');
          },
          Math.min(30000, Math.max(1, (options.idleTimeout || 90000) / 3)),
        );
  lease?.unref();
  function reject(socket: Duplex, status: number) {
    metrics.connectionFailures++;
    if (status === 401) metrics.authenticationFailures++;
    if (status === 403) metrics.originRejections++;
    if (status === 429) metrics.rateLimitRejections++;
    try {
      options.diagnostic?.('upgrade.rejected', { status });
    } catch {
      /* Diagnostics are isolated. */
    }
    socket.end(`HTTP/1.1 ${status} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  }
  async function upgrade(req: IncomingMessage, socket: Duplex, head: Buffer) {
    const onError = () => {
      socket.destroy();
    };
    socket.on('error', onError);
    let url: URL;
    try {
      url = new URL(req.url ?? '/', options.baseURL ?? 'http://localhost');
    } catch {
      reject(socket, 400);
      return;
    }
    if (url.pathname !== (options.path ?? '/socket')) {
      if (options.unmatchedPath !== 'ignore') reject(socket, 404);
      else socket.removeListener('error', onError);
      return;
    }
    metrics.connectionAttempts++;
    if (disposed || pending.size >= (options.maxPendingUpgrades ?? 128)) {
      reject(socket, 503);
      return;
    }
    const origin = req.headers.origin;
    if (
      (!origin && !options.allowMissingOrigin) ||
      (origin && !options.allowedOrigins.includes(origin))
    ) {
      reject(socket, 403);
      return;
    }
    if (
      !req.headers['sec-websocket-protocol']
        ?.split(',')
        .map((s) => s.trim())
        .includes(SUBPROTOCOL)
    ) {
      reject(socket, 400);
      return;
    }
    const source = req.socket.remoteAddress ?? 'unknown';
    const bucket = attempts.get(source) ?? { since: Date.now(), count: 0 };
    if (Date.now() - bucket.since > 60000) {
      bucket.since = Date.now();
      bucket.count = 0;
    }
    if (attempts.size >= 4096 && !attempts.has(source))
      attempts.delete(attempts.keys().next().value!);
    attempts.set(source, bucket);
    if (
      ++bucket.count > (options.attemptsPerMinute ?? 120) ||
      (sourceConnections.get(source) ?? 0) >= (options.maxConnectionsPerSource ?? 128)
    ) {
      reject(socket, 429);
      return;
    }
    pending.add(socket);
    const controller = new AbortController();
    const timer = setTimeout(() => {
      controller.abort();
      socket.destroy();
    }, options.handshakeTimeout ?? 5000);
    const abort = () => controller.abort();
    socket.once('close', abort);
    const aborted = new Promise<never>((_resolve, reject) =>
      controller.signal.addEventListener('abort', () => reject(new Error('Handshake cancelled')), {
        once: true,
      }),
    );
    try {
      if (
        options.allowUpgrade &&
        !(await Promise.race([options.allowUpgrade(req.socket.remoteAddress), aborted]))
      ) {
        reject(socket, 429);
        return;
      }
      const headers = new Headers();
      for (const [key, value] of Object.entries(req.headers))
        if (value !== undefined) headers.set(key, Array.isArray(value) ? value.join(', ') : value);
      const principal = await Promise.race([
        options.authenticate(new Request(url, { headers }), {
          address: req.socket.remoteAddress,
          signal: controller.signal,
        }),
        aborted,
      ]);
      if (controller.signal.aborted || disposed || socket.destroyed) return;
      if (!principal || (principal.expiresAt !== undefined && principal.expiresAt <= Date.now())) {
        reject(socket, 401);
        return;
      }
      wss.handleUpgrade(req, socket, head, (ws) => {
        ws.on('error', () => {});
        try {
          const connection = core.accept(
            {
              get bufferedAmount() {
                return ws.bufferedAmount;
              },
              send: (data) => ws.send(data),
              close: (code, reason) => ws.close(code, reason),
            },
            principal,
          );
          sourceConnections.set(source, (sourceConnections.get(source) ?? 0) + 1);
          metrics.upgraded++;
          const helloTimeout = setTimeout(() => {
            if (!core.isReady(connection.id))
              core.close(connection.id, 4406, 'Negotiation timeout');
          }, options.handshakeTimeout ?? 5000);
          helloTimeout.unref();
          ws.on('message', (data, binary) => {
            if (binary) ws.close(1003, 'Text frames required');
            else {
              core.receive(connection.id, data.toString());
              void core.drain(connection.id).then(() => {
                if (core.isReady(connection.id)) clearTimeout(helloTimeout);
              });
            }
          });
          ws.once('close', (code, reason) => {
            core.remove(connection.id, code, reason.toString());
            clearTimeout(helloTimeout);
            const count = (sourceConnections.get(source) ?? 1) - 1;
            if (count) sourceConnections.set(source, count);
            else sourceConnections.delete(source);
          });
          // A bounded maximum lifetime forces a new handshake and fresh session credentials.
          const timeout = setTimeout(
            () =>
              ws.close(
                principal.expiresAt && principal.expiresAt <= Date.now() ? 4401 : 4000,
                'Renew connection',
              ),
            Math.min(
              86400000,
              Math.max(1, (principal.expiresAt ?? Date.now() + 86400000) - Date.now()),
            ),
          );
          timeout.unref();
          ws.once('close', () => clearTimeout(timeout));
        } catch {
          ws.close(4429, 'Connection limit');
        }
      });
    } catch {
      if (!socket.destroyed) reject(socket, 401);
    } finally {
      pending.delete(socket);
      clearTimeout(timer);
      socket.removeListener('close', abort);
      socket.removeListener('error', onError);
    }
  }
  const listener = (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    void upgrade(req, socket, head);
  };
  server.on('upgrade', listener);
  return {
    get stats() {
      return {
        ...metrics,
        pendingUpgrades: pending.size,
        activeConnections: [...sourceConnections.values()].reduce((a, b) => a + b, 0),
      };
    },
    async close() {
      if (disposed) return;
      disposed = true;
      clearInterval(lease);
      server.off('upgrade', listener);
      for (const socket of pending) socket.destroy();
      pending.clear();
      core.shutdown();
      const timer = setTimeout(() => {
        for (const ws of wss.clients) ws.terminate();
      }, 1000);
      timer.unref();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
      clearTimeout(timer);
    },
  };
}
