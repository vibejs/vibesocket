import { createApp } from '@usevibejs/ui';
import { createSocketClient } from '../src/index.ts';
import { createSocketPlugin } from '../src/client.ts';
import { demoContract } from '../examples/contract.ts';
import Isolated from './Isolated.vibe';
export function mountIsolated(element: Element) {
  const client = createSocketClient({
    contract: demoContract,
    url: 'ws://127.0.0.1:3100/socket',
    heartbeat: false,
    lifecycle: { pauseWhenHidden: true },
  });
  const app = createApp(Isolated).use(createSocketPlugin(client));
  app.mount(element);
  return { client, app };
}
