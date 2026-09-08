import { defineConfig } from 'vitest/config';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import path from 'path';

export default defineConfig({
  plugins: [svelte({ compilerOptions: { dev: true } })],
  resolve: {
    alias: {
      '@hadoop-explorer/common': path.resolve(__dirname, './common/index.ts')
    },
    conditions: ['browser']
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./vitest-setup.ts'],
    include: ['common/tests/**/*.test.ts', 'apps/*/tests/**/*.test.ts'],
    server: {
      deps: {
        inline: [/lucide-svelte/]
      }
    }
  }
});
