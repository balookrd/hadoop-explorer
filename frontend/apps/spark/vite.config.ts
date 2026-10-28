import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [
    svelte(),
    tailwindcss()
  ],
  server: {
    port: 5174,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true
      }
    }
  },
  // Бандл собирается прямо в ресурсы Spring Boot и в git не хранится (см. .gitignore);
  // каталог очищается, чтобы старые бандлы не копились
  build: {
    outDir: '../../../backend/spark/src/main/resources/static',
    emptyOutDir: true,
  }
})
