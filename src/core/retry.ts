export interface RetryPolicy {
  minDelay?: number;
  maxDelay?: number;
  maxAttempts?: number;
  stableAfter?: number;
  shouldRetry?: (code: number) => boolean;
}
export function retryDelay(
  attempt: number,
  policy: RetryPolicy = {},
  random = Math.random,
): number {
  const min = Math.max(50, policy.minDelay ?? 250),
    max = Math.max(min, policy.maxDelay ?? 30000);
  return (
    min +
    Math.max(0, Math.min(1, random())) * (Math.min(max, min * 2 ** Math.min(attempt, 20)) - min)
  );
}
export const terminalClose = (code: number) =>
  [1000, 1002, 1003, 1008, 1009, 4008, 4401, 4403, 4406, 4429].includes(code);
