import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [svelte(), tailwindcss()],
  server: {
    port: 3005,
    proxy: {
      '/api': {
        target: 'http://localhost:8005',
        changeOrigin: true,
      },
      '/jobs': {
        target: 'http://localhost:8005',
        changeOrigin: true,
      },
      '/tokens': {
        target: 'http://localhost:8005',
        changeOrigin: true,
      },
    },
  },
  // Бандл собирается прямо в ресурсы Spring Boot и в git не хранится (см. .gitignore);
  // каталог очищается, чтобы старые бандлы не копились
  build: {
    outDir: '../../../backend/replicator/orchestrator/src/main/resources/static',
    emptyOutDir: true,
  },
});
