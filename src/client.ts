import { computed, inject, onScopeDispose, shallowRef, watch, toValue, type App, type MaybeRefOrGetter } from '@usevibejs/ui';
import type { SocketClient } from './index.ts';
import type { SocketContract, ServerEvent, Output } from './contract.ts';
export { createSocketClient } from './index.ts';
export type { ClientOptions, ClientState, ConnectionStatus, SocketClient } from './index.ts';

export function createSocketPlugin<C extends SocketContract>(client: SocketClient<C>) {
  // A fresh key per plugin supports multiple independent apps and multiple contracts.
  const key = Symbol('VibeSocket');
  return {
    install(app: App) { app.provide(key, client); app.onUnmount(() => client.dispose()); client.connect(); },
    useSocket(): SocketClient<C> { const value = inject<SocketClient<C> | null>(key, null); if (!value) throw new Error('Install the matching VibeSocket plugin first.'); return value; },
  };
}
export function useSocketStatus<C extends SocketContract>(client: SocketClient<C>) {
  const state = shallowRef(client.state);
  const stop = client.observe(value => { state.value = value; }); onScopeDispose(stop);
  return { state, status: computed(() => state.value.status), connected: computed(() => state.value.status === 'connected') };
}
export function useSocketEvent<C extends SocketContract, K extends ServerEvent<C>>(client: SocketClient<C>, event: K, callback?: (payload: Output<C['server'][K]>, metadata: { room?: string }) => void) {
  const data = shallowRef<Output<C['server'][K]>>();
  const stop = client.on(event, (payload, metadata) => { data.value = payload; callback?.(payload, metadata); }); onScopeDispose(stop); return data;
}
export function useSocketRoom<C extends SocketContract>(client: SocketClient<C>, room: MaybeRefOrGetter<string | null>) {
  const active = shallowRef(false);
  const stop = watch(() => toValue(room), (value, _old, cleanup) => {
    active.value = false; if (!value) return;
    const subscription = client.subscribe(value);
    cleanup(() => subscription.unsubscribe());
    active.value = subscription.active;
  }, { immediate: true, flush: 'sync' }); onScopeDispose(stop); return { active };
}
