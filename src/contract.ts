import type { StandardSchemaV1 } from '@standard-schema/spec';
import { SocketError } from './errors.ts';

export type Schema = StandardSchemaV1;
export type EventMap = Readonly<Record<string, Schema>>;
export interface RequestDefinition {
  readonly input: Schema;
  readonly output: Schema;
  readonly errors?: readonly string[];
}
export interface SocketContract {
  readonly version: number;
  readonly client: EventMap;
  readonly server: EventMap;
  readonly requests: Readonly<Record<string, RequestDefinition>>;
}
export type Input<S extends Schema> = StandardSchemaV1.InferInput<S>;
export type Output<S extends Schema> = StandardSchemaV1.InferOutput<S>;
export type ClientEvent<C extends SocketContract> = keyof C['client'] & string;
export type ServerEvent<C extends SocketContract> = keyof C['server'] & string;
export type RequestName<C extends SocketContract> = keyof C['requests'] & string;
export type RequestError<C extends SocketContract, K extends RequestName<C>> = NonNullable<C['requests'][K]['errors']>[number];

export function defineSocketContract<
  const C extends EventMap = Record<never, never>,
  const S extends EventMap = Record<never, never>,
  const R extends Readonly<Record<string, RequestDefinition>> = Record<never, never>,
>(definition: { version?: number; client?: C; server?: S; requests?: R; bidirectional?: EventMap }) {
  const version = definition.version ?? 1;
  if (!Number.isSafeInteger(version) || version < 1) throw new TypeError('Contract version must be a positive integer.');
  const freeze = <T extends object>(value: T) => Object.freeze(value);
  return freeze({
    version,
    client: freeze({ ...definition.bidirectional, ...definition.client }) as C,
    server: freeze({ ...definition.bidirectional, ...definition.server }) as S,
    requests: freeze({ ...definition.requests }) as R,
  });
}

export async function validate<S extends Schema>(schema: S | undefined, value: unknown): Promise<Output<S>> {
  if (!schema) throw new SocketError('UNSUPPORTED_EVENT', 'This event is not declared in the contract.');
  const result = await schema['~standard'].validate(value);
  if (result.issues) {
    // Schema diagnostics are available locally; do not echo potentially sensitive input remotely.
    throw new SocketError('INVALID_PAYLOAD', result.issues.map(i => i.message).join('; ').slice(0, 512));
  }
  return result.value as Output<S>;
}
export function lookup<T>(map: Readonly<Record<string, T>>, key: string): T | undefined {
  return Object.hasOwn(map, key) ? map[key] : undefined;
}
