import { defineConfig } from 'tsdown';
export default defineConfig({
  entry: [
    'src/index.ts',
    'src/client.ts',
    'src/contract.ts',
    'src/server.ts',
    'src/node.ts',
    'src/sse.ts',
    'src/adapters.ts',
    'src/cloudflare.ts',
  ],
  tsconfig: 'tsconfig.build.json',
  platform: 'neutral',
  target: 'es2022',
  format: 'esm',
  dts: { generator: 'tsgo' },
  sourcemap: true,
  clean: true,
  outExtensions: () => ({ js: '.js', dts: '.d.ts' }),
  deps: { neverBundle: ['@usevibejs/ui', 'ws', '@standard-schema/spec'] },
});
