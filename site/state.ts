import { shallowRef } from '@usevibejs/ui';
import { createVibeAuthClient } from '@usevibejs/auth';
import { QueryClient } from '@usevibejs/query';
import { createI18n } from '@usevibejs/i18n';
import { createSocketClient } from '../src/index.ts';
import { createSocketPlugin } from '../src/client.ts';
import { demoContract } from '../examples/contract.ts';
export const socket = createSocketClient({
  contract: demoContract,
  url: 'ws://127.0.0.1:3100/socket',
  diagnostic: (event, fields) => {
    diagnostics.value = [{ event, ...fields }, ...diagnostics.value].slice(0, 30);
  },
});
export const plugin = createSocketPlugin(socket, { autoConnect: false });
export const auth = createVibeAuthClient({ baseURL: window.location.origin });
export const query = new QueryClient();
export const identity = shallowRef<{ id: string; org: string; role: string } | null>(null);
export const authError = shallowRef('');
export const diagnostics = shallowRef<Record<string, string | number>[]>([]);
export const i18n = createI18n({
  locale: 'en',
  fallbackLocale: 'en',
  messages: {
    en: {
      connected: 'Connected',
      connecting: 'Connecting',
      reconnecting: 'Reconnecting',
      disconnected: 'Disconnected',
      idle: 'Ready to connect',
      failed: 'Connection failed',
      closed: 'Closed',
      online: 'online',
      typing: 'Someone is typing…',
      general: 'General',
      engineering: 'Engineering',
    },
    tr: {
      connected: 'Bağlandı',
      connecting: 'Bağlanıyor',
      reconnecting: 'Yeniden bağlanıyor',
      disconnected: 'Bağlantı kesildi',
      idle: 'Bağlanmaya hazır',
      failed: 'Bağlantı hatası',
      closed: 'Kapalı',
      online: 'çevrimiçi',
      typing: 'Birisi yazıyor…',
      general: 'Genel',
      engineering: 'Mühendislik',
    },
  },
});
export async function login(name: string) {
  authError.value = '';
  socket.disconnect();
  query.clear();
  const result = await auth.client.signIn.email({
    email: `${name.toLowerCase()}@demo.test`,
    password: 'local-realtime-example-2026',
  });
  if (result.error) {
    authError.value = result.error.message ?? 'Sign in failed';
    return;
  }
  const response = await fetch('/api/me');
  if (!response.ok) {
    authError.value = 'Session validation failed';
    return;
  }
  identity.value = await response.json();
  socket.connect();
}
export async function restoreSession() {
  const response = await fetch('/api/me');
  if (response.ok) {
    identity.value = await response.json();
    socket.connect();
  }
}
export async function logout() {
  socket.disconnect();
  identity.value = null;
  query.clear();
  await auth.client.signOut();
}
