import type { IncomingMessage, Server } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocketServer } from 'ws';
import type { SocketContract } from './contract.ts';
import type { Principal, SocketServer } from './server.ts';
import { SUBPROTOCOL } from './protocol.ts';

export interface NodeAdapterOptions<P extends Principal> {
  path?: string;
  allowedOrigins: readonly string[];
  allowMissingOrigin?: boolean;
  authenticate: (request: Request, source: { address: string | undefined; signal: AbortSignal }) => P | null | Promise<P | null>;
  handshakeTimeout?: number;
  maxPayload?: number;
  maxPendingUpgrades?: number;
  /** External limit hook. Do not trust forwarded headers unless a trusted proxy validates them. */
  allowUpgrade?: (source: string | undefined) => boolean | Promise<boolean>;
}
export function attachNodeSocket<C extends SocketContract, P extends Principal>(server: Server, core: SocketServer<C, P>, options: NodeAdapterOptions<P>) {
  const wss = new WebSocketServer({ noServer: true, maxPayload: options.maxPayload ?? 65536, perMessageDeflate: false,
    handleProtocols: protocols => protocols.has(SUBPROTOCOL) ? SUBPROTOCOL : false });
  let disposed = false;
  const pending = new Set<Duplex>();
  function reject(socket: Duplex, status: number) { socket.end(`HTTP/1.1 ${status} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`); }
  async function upgrade(req: IncomingMessage, socket: Duplex, head: Buffer) {
    const onError = () => { socket.destroy(); }; socket.on('error', onError);
    let url: URL;
    try { url = new URL(req.url ?? '/', 'http://localhost'); } catch { reject(socket, 400); return; }
    if (url.pathname !== (options.path ?? '/socket')) { socket.removeListener('error', onError); return; }
    if (disposed || pending.size >= (options.maxPendingUpgrades ?? 128)) { reject(socket, 503); return; }
    const origin = req.headers.origin;
    if ((!origin && !options.allowMissingOrigin) || (origin && !options.allowedOrigins.includes(origin))) { reject(socket, 403); return; }
    if (!req.headers['sec-websocket-protocol']?.split(',').map(s => s.trim()).includes(SUBPROTOCOL)) { reject(socket, 400); return; }
    pending.add(socket);
    const controller = new AbortController();
    const timer = setTimeout(() => { controller.abort(); socket.destroy(); }, options.handshakeTimeout ?? 5000);
    const abort = () => controller.abort(); socket.once('close', abort);
    try {
      if (options.allowUpgrade && !await options.allowUpgrade(req.socket.remoteAddress)) { reject(socket, 429); return; }
      const headers = new Headers(); for (const [key, value] of Object.entries(req.headers)) if (value !== undefined) headers.set(key, Array.isArray(value) ? value.join(', ') : value);
      const principal = await options.authenticate(new Request(url, { headers }), { address: req.socket.remoteAddress, signal: controller.signal });
      if (controller.signal.aborted || disposed || socket.destroyed) return;
      if (!principal || (principal.expiresAt !== undefined && principal.expiresAt <= Date.now())) { reject(socket, 401); return; }
      wss.handleUpgrade(req, socket, head, ws => {
        ws.on('error', () => {});
        try {
          const connection = core.accept({ get bufferedAmount() { return ws.bufferedAmount; }, send: data => ws.send(data), close: (code, reason) => ws.close(code, reason) }, principal);
          ws.on('message', (data, binary) => { if (binary) ws.close(1003, 'Text frames required'); else core.receive(connection.id, data.toString()); });
          ws.once('close', () => core.remove(connection.id));
          const timeout = setTimeout(() => { if (ws.readyState !== ws.CLOSED) ws.terminate(); }, 86400000); timeout.unref();
          ws.once('close', () => clearTimeout(timeout));
        } catch { ws.close(4429, 'Connection limit'); }
      });
    } catch { if (!socket.destroyed) reject(socket, 401); }
    finally { pending.delete(socket); clearTimeout(timer); socket.removeListener('close', abort); socket.removeListener('error', onError); }
  }
  const listener = (req: IncomingMessage, socket: Duplex, head: Buffer) => { void upgrade(req, socket, head); };
  server.on('upgrade', listener);
  return {
    wss,
    async close() {
      if (disposed) return; disposed = true; server.off('upgrade', listener);
      for (const socket of pending) socket.destroy(); pending.clear(); core.shutdown();
      const timer = setTimeout(() => { for (const ws of wss.clients) ws.terminate(); }, 1000); timer.unref();
      await new Promise<void>(resolve => wss.close(() => resolve())); clearTimeout(timer);
    },
  };
}
