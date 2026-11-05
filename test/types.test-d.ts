import * as v from 'valibot';
import { QueryClient } from '@usevibejs/query';
import { defineSocketContract } from '../src/contract.ts';
import { bindQuery, createSocketClient } from '../src/index.ts';
import { createSocketServer } from '../src/server.ts';
const contract = defineSocketContract({
  client: { send: v.object({ text: v.string() }) },
  server: { received: v.number() },
  requests: { sum: { input: v.array(v.number()), output: v.number(), errors: ['CONFLICT'] } },
});
const client = createSocketClient({ contract, url: 'ws://test' });
void client.send('send', { text: 'ok' });
// @ts-expect-error wrong direction
void client.send('received', 1);
// @ts-expect-error unknown event
client.on('send', () => {});
// @ts-expect-error wrong payload
void client.send('send', { text: 42 });
client.on('received', (value) => {
  const number: number = value;
  return number;
});
const response: Promise<number> = client.request('sum', [1, 2]);
void response;
// @ts-expect-error unknown RPC
void client.request('missing', 1);
const server = createSocketServer({ contract, authorize: () => true });
server.handle('sum', (values, context) => {
  if (!values.length) context.fail('CONFLICT');
  return values.reduce((a, b) => a + b, 0);
});
// @ts-expect-error wrong return type
server.handle('sum', () => 'wrong');
// @ts-expect-error undeclared application error
server.handle('sum', (_values, context) => context.fail('INVENTED'));
// @ts-expect-error wrong direction
void server.publish('room', 'send', { text: 'wrong' });
bindQuery(client, new QueryClient(), { received: ['inbox'], sum: [['inbox'], ['unread']] });
void client.requestResult('sum', [1]).then((result) => {
  if (!result.ok) {
    const code: import('../src/errors.ts').ErrorCode | 'CONFLICT' = result.error.code;
    return code;
  }
  return result.data;
});
