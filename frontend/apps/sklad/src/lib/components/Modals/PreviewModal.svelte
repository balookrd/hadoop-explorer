<script lang="ts">
  import { api } from '../../api/client';
  import { explorerStore } from '../../stores/explorer.svelte';
  import type { HdfsFileStatus, FilePreviewResponse } from '../../types';
  import { formatBytes } from '../../utils/format';
  import { X, Download, Copy, Check, FileText, Table, AlertCircle, Info } from 'lucide-svelte';

  interface Props {
    file: HdfsFileStatus | null;
    onClose: () => void;
  }

  let { file, onClose }: Props = $props();

  let previewData = $state<FilePreviewResponse | null>(null);
  let loading = $state<boolean>(false);
  let error = $state<string | null>(null);
  let copied = $state<boolean>(false);

  $effect(() => {
    if (file && explorerStore.currentCluster) {
      loadPreview(file);
    }
  });

  async function loadPreview(f: HdfsFileStatus) {
    if (!explorerStore.currentCluster) return;

    loading = true;
    error = null;
    previewData = null;
    try {
      const fullPath = explorerStore.currentPath === '/'
        ? `/${f.pathSuffix}`
        : `${explorerStore.currentPath}/${f.pathSuffix}`;

      previewData = await api.previewFile(explorerStore.currentCluster.id, fullPath);
    } catch (err: any) {
      error = err.message || 'Не удалось получить предпросмотр файла';
    } finally {
      loading = false;
    }
  }

  function getDownloadUrl() {
    if (!file || !explorerStore.currentCluster) return '#';
    const p = explorerStore.currentPath === '/'
      ? `/${file.pathSuffix}`
      : `${explorerStore.currentPath}/${file.pathSuffix}`;
    return api.getDownloadUrl(explorerStore.currentCluster.id, p);
  }

  function copyContent() {
    if (previewData?.content) {
      navigator.clipboard.writeText(previewData.content);
      copied = true;
      setTimeout(() => { copied = false; }, 2000);
    }
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && file) onClose(); }} />

{#if file}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 overflow-y-auto bg-slate-900/60 dark:bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4 sm:p-6 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) onClose(); }}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-2xl max-w-5xl w-full h-[85vh] flex flex-col border border-slate-200 dark:border-slate-800 overflow-hidden"
      onclick={(e) => e.stopPropagation()}
    >
      <!-- Header -->
      <div class="px-5 py-3.5 border-b border-slate-200 dark:border-slate-800 flex items-center justify-between bg-slate-50 dark:bg-slate-950">
        <div class="flex items-center gap-3 overflow-hidden">
          <div class="p-2 rounded-xl bg-sky-100 dark:bg-sky-950/60 text-sky-700 dark:text-sky-400 shrink-0">
            {#if previewData?.file_type === 'csv' || previewData?.file_type === 'parquet' || previewData?.file_type === 'orc'}
              <Table class="w-4 h-4" />
            {:else}
              <FileText class="w-4 h-4" />
            {/if}
          </div>
          <div class="overflow-hidden">
            <h3 class="text-sm font-bold text-slate-900 dark:text-slate-100 truncate" title={file.pathSuffix}>
              {file.pathSuffix}
            </h3>
            <div class="text-[11px] text-slate-500 dark:text-slate-400 flex items-center gap-2 font-mono mt-0.5">
              <span>{formatBytes(file.length)}</span>
              <span class="text-slate-300 dark:text-slate-600">•</span>
              <span>Владелец: {file.owner}:{file.group}</span>
              {#if previewData?.file_type}
                <span class="text-slate-300 dark:text-slate-600">•</span>
                <span class="uppercase text-sky-700 dark:text-sky-400 font-semibold px-1.5 py-0.2 bg-sky-50 dark:bg-sky-950 rounded border border-sky-200 dark:border-sky-800">{previewData.file_type}</span>
              {/if}
            </div>
          </div>
        </div>

        <div class="flex items-center gap-2">
          {#if previewData?.content}
            <button
              onclick={copyContent}
              class="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-200 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 transition cursor-pointer shadow-2xs"
              title="Копировать содержимое"
            >
              {#if copied}
                <Check class="w-3.5 h-3.5 text-emerald-600 dark:text-emerald-400" />
                <span class="text-emerald-600 dark:text-emerald-400 font-semibold">Скопировано!</span>
              {:else}
                <Copy class="w-3.5 h-3.5 text-slate-500 dark:text-slate-400" />
                <span>Копировать</span>
              {/if}
            </button>
          {/if}

          <a
            href={getDownloadUrl()}
            download
            class="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold text-white bg-sky-600 hover:bg-sky-500 rounded-lg shadow-md shadow-sky-500/20 transition cursor-pointer"
            title="Скачать файл полностью"
          >
            <Download class="w-3.5 h-3.5" />
            <span>Скачать</span>
          </a>

          <button onclick={onClose} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1.5 rounded-lg ml-1 transition cursor-pointer">
            <X class="w-4 h-4" />
          </button>
        </div>
      </div>

      <!-- Warning if truncated -->
      {#if previewData?.truncated}
        <div class="px-5 py-2 bg-amber-50 dark:bg-amber-950/40 border-b border-amber-200 dark:border-amber-900 text-amber-800 dark:text-amber-300 text-xs flex items-center gap-2 shrink-0">
          <Info class="w-4 h-4 text-amber-600 dark:text-amber-400 shrink-0" />
          <span>Показаны первые {formatBytes(previewData.size)} файла. Чтобы просмотреть весь файл, скачайте его.</span>
        </div>
      {/if}

      <!-- Body: View Matching ResultsGrid -->
      <div class="flex-1 overflow-auto bg-white dark:bg-slate-900 p-4 font-mono text-xs select-text">
        {#if loading}
          <div class="h-full flex flex-col items-center justify-center gap-2.5 text-slate-400 dark:text-slate-500 font-sans">
            <div class="w-7 h-7 border-2 border-sky-600 border-t-transparent rounded-full animate-spin"></div>
            <span class="text-xs">Чтение содержимого из HDFS...</span>
          </div>
        {:else if error}
          <div class="p-4 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 rounded-xl text-red-800 dark:text-red-300 flex items-start gap-3 shadow-2xs">
            <AlertCircle class="w-4 h-4 text-red-600 dark:text-red-400 shrink-0 mt-0.5" />
            <div>
              <h4 class="font-bold text-sm text-red-900 dark:text-red-200">Ошибка чтения файла</h4>
              <p class="mt-1 text-xs">{error}</p>
            </div>
          </div>
        {:else if previewData}
          <!-- CSV, Parquet or ORC Table View -->
          {#if (previewData.file_type === 'csv' || previewData.file_type === 'parquet' || previewData.file_type === 'orc') && previewData.columns && previewData.rows}
            <div class="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 overflow-hidden shadow-2xs">
              <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse font-mono text-xs">
                  <thead class="sticky top-0 bg-slate-50 dark:bg-slate-950 border-b border-slate-200 dark:border-slate-800 z-10 shadow-2xs select-none">
                    <tr>
                      <th class="py-2 px-3 border-r border-slate-200 dark:border-slate-800 text-slate-400 dark:text-slate-500 text-center w-12 font-normal">#</th>
                      {#each previewData.columns as col}
                        <th class="py-2 px-3 font-semibold text-slate-700 dark:text-slate-300 whitespace-nowrap border-r border-slate-200 dark:border-slate-800">{col}</th>
                      {/each}
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-slate-100 dark:divide-slate-800 text-slate-800 dark:text-slate-200">
                    {#each previewData.rows as row, idx}
                      <tr class="hover:bg-sky-50/50 dark:hover:bg-sky-950/30 transition">
                        <td class="py-1.5 px-3 text-slate-400 dark:text-slate-500 border-r border-slate-100 dark:border-slate-800 text-center bg-slate-50/40 dark:bg-slate-950/40 font-mono">{idx + 1}</td>
                        {#each row as cell}
                          <td class="py-1.5 px-3 border-r border-slate-100 dark:border-slate-800 whitespace-nowrap max-w-xs truncate" title={String(cell)}>
                            {cell}
                          </td>
                        {/each}
                      </tr>
                    {/each}
                  </tbody>
                </table>
              </div>
            </div>
            <div class="mt-2.5 text-slate-500 dark:text-slate-400 text-right font-sans text-xs">
              Отображено строк: <strong>{previewData.row_count}</strong>
            </div>
          {:else if previewData.content}
            <!-- Plain Text or JSON in pre block -->
            <div class="h-full bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl p-4 overflow-auto shadow-2xs">
              <pre class="whitespace-pre font-mono text-xs text-slate-800 dark:text-slate-200 leading-relaxed">{previewData.content}</pre>
            </div>
          {:else}
            <div class="h-full flex flex-col items-center justify-center gap-2 text-slate-400 dark:text-slate-500 font-sans text-xs">
              <span>Файл пуст или нет данных для отображения.</span>
            </div>
          {/if}
        {/if}
      </div>
    </div>
  </div>
{/if}
