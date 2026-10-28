export function checkLimits(options: object, keys: readonly string[], allowZero = true) {
  for (const key of keys) {
    const value: unknown = Reflect.get(options, key);
    if (
      value !== undefined &&
      (typeof value !== 'number' || !Number.isSafeInteger(value) || value < (allowZero ? 0 : 1))
    )
      throw new TypeError(
        `${key} must be a ${allowZero ? 'nonnegative' : 'positive'} finite integer.`,
      );
  }
}
