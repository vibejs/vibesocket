import { defineConfig } from 'vite';
import { vibe } from '@usevibejs/compiler/vite';
export default defineConfig({ root: 'site', plugins: [vibe()], server: { host: '127.0.0.1', port: 5173, strictPort: true, fs: { allow: ['..'] } }, build: { outDir: '../dist-site', emptyOutDir: true } });
