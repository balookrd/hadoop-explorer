/// <reference types="vite/client" />
/// <reference types="svelte" />

declare namespace svelteHTML {
  interface HTMLAttributes<T> {
    directory?: string | boolean;
    webkitdirectory?: string | boolean;
  }
}
