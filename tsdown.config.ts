import { defineConfig } from 'tsdown';
export default defineConfig({
  entry: ['src/index.ts', 'src/client.ts', 'src/contract.ts', 'src/server.ts', 'src/node.ts', 'src/sse.ts'],
  format: 'esm', dts: true, sourcemap: true, clean: true,
  deps: { neverBundle: ['@usevibejs/ui', 'ws', '@standard-schema/spec'] },
});
