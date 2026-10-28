import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [svelte(), tailwindcss()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
  // Бандл собирается прямо в ресурсы Spring Boot и в git не хранится (см. .gitignore);
  // каталог очищается, чтобы старые бандлы не копились
  build: {
    outDir: '../../../backend/hdfs/src/main/resources/static',
    emptyOutDir: true,
  },
});
