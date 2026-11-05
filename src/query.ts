/** The cache operation this bridge calls. `@usevibejs/query` is not imported. */
export interface QueryInvalidator {
  invalidateQueries(filters: { readonly queryKey: readonly unknown[] }, options?: unknown): unknown;
}

/** Any client with `on`. The socket package does not import a query implementation. */
export interface QuerySocket {
  on(event: string, listener: (payload: unknown, metadata: { room?: string }) => void): () => void;
}

/**
 * One query key, or several keys when every entry is itself an array.
 * An empty key is ignored so a blank entry cannot invalidate the whole cache.
 */
function queryKeys(value: readonly unknown[]): readonly (readonly unknown[])[] {
  if (value.length === 0) return [];
  if (value.every((entry) => Array.isArray(entry)))
    return (value as readonly (readonly unknown[])[]).filter((key) => key.length > 0);
  return [value];
}

function invalidate(client: QueryInvalidator, queryKey: readonly unknown[]): void {
  try {
    const result = client.invalidateQueries({ queryKey });
    if (
      result !== null &&
      result !== undefined &&
      typeof (result as { then?: unknown }).then === 'function'
    )
      void (result as Promise<unknown>).catch(() => {
        /* A failed refetch stays inside the query cache. */
      });
  } catch {
    /* One cache update cannot break the socket listener. */
  }
}

/**
 * Invalidates query keys when the named socket events arrive.
 * Pass the application's `QueryClient`. This module does not load `@usevibejs/query`.
 * The returned function removes every listener.
 */
export function bindQuery(
  socket: QuerySocket,
  queryClient: QueryInvalidator,
  events: Readonly<Record<string, readonly unknown[] | undefined>>,
): () => void {
  const stops: Array<() => void> = [];
  for (const event of Object.keys(events)) {
    const queryKey = events[event];
    if (!queryKey) continue;
    const keys = queryKeys(queryKey);
    if (keys.length === 0) continue;
    stops.push(
      socket.on(event, () => {
        for (const key of keys) invalidate(queryClient, key);
      }),
    );
  }
  return () => {
    for (const stop of stops.splice(0)) stop();
  };
}
