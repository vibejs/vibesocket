import { createApp } from '@usevibejs/ui';
import { socket, plugin, i18n, restoreSession, login, logout } from './state.ts';
import App from './App.vibe';
import './style.css';
export const app = createApp(App).use(plugin).use(i18n);
app.mount('#app');
void restoreSession();
Object.assign(window, { demo: { socket, app, login, logout } });
