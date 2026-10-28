import {
  createSocketServer,
  type ServerOptions,
  type Principal,
  type Connection,
} from './server.ts';
import type { SocketContract } from './contract.ts';
import { bytes, SUBPROTOCOL } from './protocol.ts';
import { SocketError } from './errors.ts';

/** Structural runtime interfaces keep Cloudflare globals out of browser/Node type consumers. */
export interface WorkerSocket {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  serializeAttachment(value: unknown): void;
  deserializeAttachment(): unknown;
}
export interface HibernationState {
  acceptWebSocket(socket: WorkerSocket): void;
  getWebSockets(): WorkerSocket[];
  setWebSocketAutoResponse(pair: unknown): void;
}
interface Attachment<P extends Principal> {
  schema: 1;
  contract: number;
  principal: P;
  id: string;
  rooms: [string, string][];
  ready: boolean;
  connectedAt: number;
  lastActivity: number;
  credit: [string, number][];
  counter: number;
}
export interface CloudflareOptions<
  C extends SocketContract,
  P extends Principal,
> extends ServerOptions<C, P> {
  allowedOrigins: readonly string[];
  authenticate: (request: Request) => P | null | Promise<P | null>;
  /** Must explicitly select non-secret identity fields persisted in connection attachments. */
  attachmentPrincipal: (principal: P) => P;
}
/** Compose inside a Durable Object; forward fetch/webSocketMessage/webSocketClose/webSocketError. */
export function createDurableSocket<C extends SocketContract, P extends Principal>(
  state: HibernationState,
  options: CloudflareOptions<C, P>,
) {
  const core = createSocketServer(options),
    ids = new WeakMap<WorkerSocket, string>();
  const runtime = globalThis as unknown as {
    WebSocketPair: new () => { 0: WorkerSocket; 1: WorkerSocket };
    WebSocketRequestResponsePair: new (request: string, response: string) => unknown;
  };
  // Constant frames can be answered without waking the object. No perpetual timers.
  state.setWebSocketAutoResponse(
    new runtime.WebSocketRequestResponsePair('{"v":1,"type":"ping"}', '{"v":1,"type":"pong"}'),
  );
  function attach(socket: WorkerSocket, principal: P, restored?: Attachment<P>) {
    let attachment = restored;
    const credit = new Map<string, number>(restored?.credit),
      max = options.maxBufferedBytes ?? 262144;
    let counter = restored?.counter ?? 0;
    const save = () => {
      if (!attachment) return;
      attachment.credit = [...credit];
      attachment.counter = counter;
      if (bytes(JSON.stringify(attachment)) > 1900)
        throw new Error('Attachment exceeds safe limit');
      socket.serializeAttachment(attachment);
    };
    const connection = core.accept(
      {
        get bufferedAmount() {
          return [...credit.values()].reduce((a, b) => a + b, 0);
        },
        send(data) {
          if (
            credit.size >= 32 ||
            [...credit.values()].reduce((a, b) => a + b, 0) + bytes(data) > max
          )
            throw new SocketError('SLOW_CONSUMER');
          const id = (++counter).toString(36),
            frame = JSON.parse(data) as Record<string, unknown>;
          frame.receipt = id;
          const encoded = JSON.stringify(frame);
          if (bytes(encoded) > (options.maxMessageBytes ?? 65536))
            throw new SocketError('INVALID_PAYLOAD');
          if ([...credit.values()].reduce((a, b) => a + b, 0) + bytes(encoded) > max)
            throw new SocketError('SLOW_CONSUMER');
          credit.set(id, bytes(encoded));
          save();
          socket.send(encoded);
        },
        acknowledge(id) {
          credit.delete(id);
          save();
        },
        close: (code, reason) => socket.close(code, reason),
        checkpoint(connection: Connection<Principal>, ready) {
          attachment = {
            schema: 1,
            contract: options.contract.version,
            principal: options.attachmentPrincipal(principal),
            id: connection.id,
            rooms: [...connection.rooms],
            ready,
            connectedAt: connection.connectedAt,
            lastActivity: connection.lastActivity,
            credit: [...credit],
            counter,
          };
          save();
        },
      },
      principal,
      restored,
    );
    ids.set(socket, connection.id);
    return connection;
  }
  for (const socket of state.getWebSockets()) {
    try {
      const saved = socket.deserializeAttachment() as Attachment<P> | undefined;
      if (
        !saved ||
        saved.schema !== 1 ||
        saved.contract !== options.contract.version ||
        !saved.principal?.id ||
        !Array.isArray(saved.rooms)
      )
        throw new Error('Invalid attachment');
      attach(socket, saved.principal, saved);
    } catch {
      socket.close(4406, 'Invalid connection attachment');
    }
  }
  return {
    core,
    async fetch(request: Request): Promise<Response> {
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket')
        return new Response('WebSocket required', { status: 426 });
      if (!options.allowedOrigins.includes(request.headers.get('origin') ?? ''))
        return new Response(null, { status: 403 });
      if (
        !request.headers
          .get('sec-websocket-protocol')
          ?.split(',')
          .map((s) => s.trim())
          .includes(SUBPROTOCOL)
      )
        return new Response(null, { status: 400 });
      const principal = await options.authenticate(request);
      if (!principal || (principal.expiresAt !== undefined && principal.expiresAt <= Date.now()))
        return new Response(null, { status: 401 });
      const pair = new runtime.WebSocketPair();
      state.acceptWebSocket(pair[1]);
      try {
        attach(pair[1], principal);
      } catch {
        pair[1].close(4429, 'Connection rejected');
      }
      // Only a Worker runtime can construct this upgrade response.
      return new Response(null, {
        status: 101,
        webSocket: pair[0],
        headers: { 'sec-websocket-protocol': SUBPROTOCOL },
      } as ResponseInit);
    },
    async webSocketMessage(socket: WorkerSocket, message: string | ArrayBuffer) {
      const id = ids.get(socket);
      if (!id) return;
      if (typeof message !== 'string') {
        core.close(id, 1003, 'Text frames required');
        return;
      }
      core.receive(id, message);
      await core.drain(id);
    },
    webSocketClose(socket: WorkerSocket, code: number, reason: string) {
      const id = ids.get(socket);
      if (id) core.remove(id, code, reason);
      socket.close(code === 1006 ? 1000 : code, reason);
    },
    webSocketError(socket: WorkerSocket) {
      const id = ids.get(socket);
      if (id) core.close(id, 1011, 'Transport failure');
    },
  };
}
