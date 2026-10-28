import {
  createDurableSocket,
  type HibernationState,
  type WorkerSocket,
} from '../src/cloudflare.ts';
import { defineSocketContract } from '../src/contract.ts';

const stringSchema = {
  '~standard': {
    version: 1 as const,
    vendor: 'fixture',
    validate: (value: unknown) =>
      typeof value === 'string' ? { value } : { issues: [{ message: 'Expected string' }] },
  },
};
const contract = defineSocketContract({
  client: { say: stringSchema },
  server: { said: stringSchema },
  requests: { large: { input: stringSchema, output: stringSchema } },
});
export class Room {
  adapter: ReturnType<typeof createDurableSocket<typeof contract, { id: string; org: string }>>;
  state: HibernationState;
  constructor(state: HibernationState) {
    this.state = state;
    this.adapter = this.create();
  }
  create(): ReturnType<typeof createDurableSocket<typeof contract, { id: string; org: string }>> {
    const adapter = createDurableSocket(this.state, {
      contract,
      allowedOrigins: ['https://app.test'],
      authenticate: async (req) =>
        req.headers.get('cookie') === 'session=alice' ? { id: 'alice', org: 'a' } : null,
      attachmentPrincipal: (p) => ({ id: p.id, org: p.org }),
      authorize: (p, op) => !op.room || op.room === `org:${p.org}`,
    });
    adapter.core.on('say', async (payload) => {
      await adapter.core.publish('org:a', 'said', payload);
    });
    adapter.core.handle('large', () =>
      'x'.repeat(65536 - JSON.stringify({ v: 1, type: 'result', id: 'large', payload: '' }).length),
    );
    return adapter;
  }
  fetch(req: Request) {
    if (new URL(req.url).pathname === '/restore') {
      this.adapter = this.create();
      return Response.json({
        connections: this.adapter.core.connections.length,
        rooms: this.adapter.core.roomCount,
      });
    }
    return this.adapter.fetch(req);
  }
  webSocketMessage(ws: WorkerSocket, data: string | ArrayBuffer) {
    return this.adapter.webSocketMessage(ws, data);
  }
  webSocketClose(ws: WorkerSocket, code: number, reason: string) {
    this.adapter.webSocketClose(ws, code, reason);
  }
  webSocketError(ws: WorkerSocket) {
    this.adapter.webSocketError(ws);
  }
}
export default {
  fetch(
    req: Request,
    env: {
      ROOM: {
        idFromName(name: string): unknown;
        get(id: unknown): { fetch(req: Request): Promise<Response> };
      };
    },
  ) {
    return env.ROOM.get(env.ROOM.idFromName('test')).fetch(req);
  },
};
