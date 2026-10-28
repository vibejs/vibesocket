import { createApp } from '@usevibejs/ui';
import { createSocketClient } from '../src/index.ts';
import { createSocketPlugin } from '../src/client.ts';
import { demoContract } from '../examples/contract.ts';
import App from './App.vibe';
export const socket = createSocketClient({ contract: demoContract, url: 'ws://127.0.0.1:3100/socket' });
export const plugin = createSocketPlugin(socket);
export const app = createApp(App).use(plugin); app.mount('#app');
// Explicit development controls used by browser integration tests.
Object.assign(window, { demo: { socket, app } });
