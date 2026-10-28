import {
  computed,
  inject,
  onScopeDispose,
  shallowRef,
  watch,
  toValue,
  type App,
  type MaybeRefOrGetter,
} from '@usevibejs/ui';
import type { SocketClient, SubscriptionState } from './index.ts';
import type {
  SocketContract,
  ServerEvent,
  RequestName,
  Input,
  Output,
  EventMap,
} from './contract.ts';
import { createEventStream } from './sse.ts';
export { createSocketClient } from './index.ts';
export type { ClientOptions, ClientState, ConnectionStatus, SocketClient } from './index.ts';

const keys = new WeakMap<SocketContract, symbol>();
function keyFor(contract: SocketContract) {
  let key = keys.get(contract);
  if (!key) keys.set(contract, (key = Symbol('VibeSocket')));
  return key;
}
export function useSocket<C extends SocketContract>(contract: C): SocketClient<C> {
  const client = inject<SocketClient<C> | null>(keyFor(contract), null);
  if (!client) throw new Error('Install a VibeSocket plugin for this contract first.');
  return client;
}
export function createSocketPlugin<C extends SocketContract>(
  client: SocketClient<C>,
  options: { autoConnect?: boolean } = {},
) {
  const key = keyFor(client.contract);
  return {
    install(app: App) {
      app.provide(key, client);
      app.onUnmount(() => client.dispose());
      if (options.autoConnect !== false) client.connect();
    },
    useSocket(): SocketClient<C> {
      const value = inject<SocketClient<C> | null>(key, null);
      if (!value) throw new Error('Install the matching VibeSocket plugin first.');
      return value;
    },
  };
}
export function useSocketStatus<C extends SocketContract>(client: SocketClient<C>) {
  const state = shallowRef(client.state);
  const stop = client.observe((value) => {
    state.value = value;
  });
  onScopeDispose(stop);
  return {
    state,
    status: computed(() => state.value.status),
    connected: computed(() => state.value.status === 'connected'),
  };
}
export function useSocketEvent<C extends SocketContract, K extends ServerEvent<C>>(
  client: SocketClient<C>,
  event: K,
  callback?: (payload: Output<C['server'][K]>, metadata: { room?: string }) => void,
  options: { room?: MaybeRefOrGetter<string | null> } = {},
) {
  const data = shallowRef<Output<C['server'][K]>>();
  const stop = client.on(event, (payload, metadata) => {
    if (options.room !== undefined && metadata.room !== toValue(options.room)) return;
    data.value = payload;
    callback?.(payload, metadata);
  });
  onScopeDispose(stop);
  if (options.room !== undefined)
    watch(
      () => toValue(options.room),
      () => {
        data.value = undefined;
      },
      { flush: 'sync' },
    );
  return data;
}
export function useSocketRoom<C extends SocketContract>(
  client: SocketClient<C>,
  room: MaybeRefOrGetter<string | null>,
) {
  const active = shallowRef(false);
  const state = shallowRef<SubscriptionState>();
  const stop = watch(
    () => toValue(room),
    (value, _old, cleanup) => {
      active.value = false;
      if (!value) return;
      const subscription = client.subscribe(value);
      state.value = subscription.state;
      const off = client.observeSubscriptions((next) => {
        if (next.id === subscription.id) {
          state.value = next;
          active.value = next.status === 'active';
        }
      });
      cleanup(() => {
        off();
        subscription.unsubscribe();
        state.value = undefined;
        active.value = false;
      });
      active.value = subscription.active;
    },
    { immediate: true, flush: 'sync' },
  );
  onScopeDispose(stop);
  return { active, state };
}
/** Alias: rooms are the protocol's subscription unit. */
export const useSocketSubscription = useSocketRoom;
export function useSocketRequest<C extends SocketContract, K extends RequestName<C>>(
  client: SocketClient<C>,
  event: K,
) {
  const pending = shallowRef(false),
    data = shallowRef<Output<C['requests'][K]['output']>>(),
    error = shallowRef<unknown>(null);
  let controller: AbortController | undefined,
    revision = 0;
  const cancel = () => {
    revision++;
    controller?.abort();
    pending.value = false;
  };
  onScopeDispose(cancel);
  return {
    pending,
    data,
    error,
    cancel,
    async execute(
      payload: Input<C['requests'][K]['input']>,
      options: { timeout?: number; idempotencyKey?: string } = {},
    ) {
      cancel();
      const token = revision;
      controller = new AbortController();
      pending.value = true;
      error.value = null;
      try {
        const result = await client.request(event, payload, {
          ...options,
          signal: controller.signal,
        });
        if (token === revision) data.value = result;
        return result;
      } catch (failure) {
        if (token === revision) error.value = failure;
        throw failure;
      } finally {
        if (token === revision) pending.value = false;
      }
    },
  };
}
export function useEventStream<E extends EventMap>(
  options: Parameters<typeof createEventStream<E>>[0],
) {
  const stream = createEventStream(options),
    state = shallowRef(stream.state);
  const off = stream.observe((next) => {
    state.value = next;
  });
  onScopeDispose(() => {
    off();
    stream.dispose();
  });
  stream.connect();
  return { stream, state };
}
