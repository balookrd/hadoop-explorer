<script lang="ts">
  import { api } from '../../api/client';
  import { explorerStore } from '../../stores/explorer.svelte';
  import { formatBytes } from '../../utils/format';
  import {
    X,
    UploadCloud,
    FolderUp,
    FileArchive,
    CheckCircle2,
    AlertCircle,
    Loader2,
    File as FileIcon,
    Folder
  } from 'lucide-svelte';

  interface Props {
    isOpen: boolean;
    onClose: () => void;
  }

  let { isOpen, onClose }: Props = $props();

  interface QueuedItem {
    file: File;
    relativePath: string;
  }

  type UploadMode = 'files' | 'folder' | 'archive';

  let mode = $state<UploadMode>('files');
  let queuedItems = $state<QueuedItem[]>([]);
  let archiveFile = $state<File | null>(null);

  let overwrite = $state<boolean>(true);
  let uploading = $state<boolean>(false);
  let uploadProgress = $state<{ current: number; total: number; bytesUploaded: number; totalBytes: number }>({
    current: 0,
    total: 0,
    bytesUploaded: 0,
    totalBytes: 0,
  });
  let error = $state<string | null>(null);
  let successMessage = $state<string | null>(null);
  let isDragOver = $state<boolean>(false);

  // Рекурсивный обход перетащенных директорий HTML5 FileSystem API
  function readEntryAsync(entry: any, path: string = ''): Promise<QueuedItem[]> {
    return new Promise((resolve) => {
      if (!entry) return resolve([]);
      if (entry.isFile) {
        entry.file((file: File) => {
          resolve([{ file, relativePath: path ? `${path}/${file.name}` : file.name }]);
        });
      } else if (entry.isDirectory) {
        const reader = entry.createReader();
        const results: QueuedItem[] = [];
        const currentDirPath = path ? `${path}/${entry.name}` : entry.name;

        function readNext() {
          reader.readEntries(async (entries: any[]) => {
            if (!entries || entries.length === 0) {
              resolve(results);
            } else {
              for (const child of entries) {
                const nested = await readEntryAsync(child, currentDirPath);
                results.push(...nested);
              }
              readNext();
            }
          });
        }
        readNext();
      } else {
        resolve([]);
      }
    });
  }

  async function handleDrop(e: DragEvent) {
    e.preventDefault();
    isDragOver = false;
    error = null;
    successMessage = null;

    if (!e.dataTransfer) return;

    // Если есть items с поддержкой webkitGetAsEntry (папки)
    if (e.dataTransfer.items && e.dataTransfer.items.length > 0) {
      const items: QueuedItem[] = [];
      for (let i = 0; i < e.dataTransfer.items.length; i++) {
        const item = e.dataTransfer.items[i];
        const entry = item.webkitGetAsEntry ? item.webkitGetAsEntry() : null;
        if (entry) {
          const res = await readEntryAsync(entry, '');
          items.push(...res);
        } else {
          const file = item.getAsFile();
          if (file) items.push({ file, relativePath: file.name });
        }
      }
      if (items.length > 0) {
        queuedItems = items;
        // Если один ZIP файл, предлагаем распаковку
        if (items.length === 1 && items[0].file.name.toLowerCase().endsWith('.zip')) {
          archiveFile = items[0].file;
        }
      }
    } else if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      const files = Array.from(e.dataTransfer.files);
      queuedItems = files.map(f => ({ file: f, relativePath: f.name }));
    }
  }

  function handleFileSelect(e: Event) {
    const input = e.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      queuedItems = Array.from(input.files).map(f => ({ file: f, relativePath: f.name }));
      error = null;
    }
  }

  function handleFolderSelect(e: Event) {
    const input = e.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      queuedItems = Array.from(input.files).map(f => ({
        file: f,
        relativePath: f.webkitRelativePath || f.name
      }));
      error = null;
    }
  }

  function handleArchiveSelect(e: Event) {
    const input = e.target as HTMLInputElement;
    if (input.files && input.files[0]) {
      archiveFile = input.files[0];
      error = null;
    }
  }

  function resetState() {
    queuedItems = [];
    archiveFile = null;
    error = null;
    successMessage = null;
    uploading = false;
    uploadProgress = { current: 0, total: 0, bytesUploaded: 0, totalBytes: 0 };
  }

  let totalSelectedBytes = $derived(
    mode === 'archive'
      ? (archiveFile?.size || 0)
      : queuedItems.reduce((acc, it) => acc + it.file.size, 0)
  );

  async function uploadSingleFileChunked(
    clusterId: string,
    targetDir: string,
    file: File,
    isOverwrite: boolean,
    onChunkProgress?: (uploadedInFile: number) => void
  ) {
    const CHUNK_SIZE = 2 * 1024 * 1024; // 2 МБ на чанк
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    const uploadId = (typeof crypto !== 'undefined' && crypto.randomUUID)
      ? crypto.randomUUID()
      : `up_${Math.random().toString(36).slice(2)}_${Date.now().toString(36)}`;
    let fileBytesUploaded = 0;

    for (let idx = 0; idx < totalChunks; idx++) {
      const start = idx * CHUNK_SIZE;
      const end = Math.min(file.size, start + CHUNK_SIZE);
      const chunkBlob = file.slice(start, end);
      const chunkFile = new File([chunkBlob], `${idx}.part`, { type: 'application/octet-stream' });

      const formData = new FormData();
      formData.append('upload_id', uploadId);
      formData.append('path', targetDir);
      formData.append('filename', file.name);
      formData.append('chunk_index', String(idx));
      formData.append('total_chunks', String(totalChunks));
      formData.append('file', chunkFile);
      formData.append('overwrite', String(isOverwrite));

      // До 3 попыток на случай кратковременного сбоя сети
      let success = false;
      let lastErr: any = null;
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          await api.uploadChunk(clusterId, formData);
          success = true;
          break;
        } catch (err: any) {
          lastErr = err;
          await new Promise((r) => setTimeout(r, 400 * (attempt + 1)));
        }
      }
      if (!success) {
        throw lastErr || new Error(`Ошибка загрузки чанка ${idx + 1}/${totalChunks}`);
      }

      fileBytesUploaded += (end - start);
      if (onChunkProgress) onChunkProgress(fileBytesUploaded);
    }
  }

  async function startUpload() {
    if (!explorerStore.currentCluster) return;

    uploading = true;
    error = null;
    successMessage = null;

    try {
      if (mode === 'archive' && archiveFile) {
        // Режим распаковки ZIP-архива
        const res = await api.uploadArchive(
          explorerStore.currentCluster.id,
          explorerStore.currentPath,
          archiveFile,
          overwrite
        );
        successMessage = res.message;
        setTimeout(async () => {
          await explorerStore.refresh();
          onClose();
          resetState();
        }, 1200);

      } else if (queuedItems.length > 0) {
        // Режим загрузки файлов или дерева каталога
        const total = queuedItems.length;
        const totalBytes = totalSelectedBytes;
        let bytesDone = 0;

        uploadProgress = { current: 0, total, bytesUploaded: 0, totalBytes };

        for (let i = 0; i < queuedItems.length; i++) {
          const item = queuedItems[i];
          uploadProgress = {
            current: i + 1,
            total,
            bytesUploaded: bytesDone,
            totalBytes
          };

          // Если файл больше 5 МБ и это отдельный файл -> загружаем чанками с докачкой
          if (item.file.size > 5 * 1024 * 1024 && item.relativePath === item.file.name) {
            await uploadSingleFileChunked(
              explorerStore.currentCluster.id,
              explorerStore.currentPath,
              item.file,
              overwrite,
              (fileUploaded) => {
                uploadProgress.bytesUploaded = bytesDone + fileUploaded;
              }
            );
          } else {
            await api.uploadFile(
              explorerStore.currentCluster.id,
              explorerStore.currentPath,
              item.file,
              overwrite,
              item.relativePath !== item.file.name ? item.relativePath : undefined
            );
          }

          bytesDone += item.file.size;
          uploadProgress.bytesUploaded = bytesDone;
        }

        successMessage = `Успешно загружено объектов: ${total} (${formatBytes(totalBytes)})`;
        setTimeout(async () => {
          await explorerStore.refresh();
          onClose();
          resetState();
        }, 1200);
      }
    } catch (err: any) {
      error = err.message || 'Ошибка загрузки файлов в HDFS';
    } finally {
      uploading = false;
    }
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && isOpen) onClose(); }} />

{#if isOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 overflow-y-auto bg-slate-900/50 dark:bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) onClose(); }}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-xl max-w-lg w-full p-5 sm:p-6 border border-slate-200 dark:border-slate-800"
      onclick={(e) => e.stopPropagation()}
    >
      <!-- Header -->
      <div class="flex items-center justify-between pb-3.5 border-b border-slate-100 dark:border-slate-800">
        <div class="flex items-center gap-2 text-slate-900 dark:text-slate-100 font-bold text-sm">
          <UploadCloud class="w-4 h-4 text-sky-600 dark:text-sky-400" />
          <span>Загрузка в HDFS</span>
        </div>
        <button onclick={onClose} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded-lg cursor-pointer">
          <X class="w-4 h-4" />
        </button>
      </div>

      <!-- Mode switcher tabs -->
      <div class="flex items-center gap-1 mt-3.5 p-1 bg-slate-100 dark:bg-slate-800 rounded-xl text-xs font-medium text-slate-600 dark:text-slate-300">
        <button
          type="button"
          class={`flex-1 py-1.5 px-2.5 rounded-lg transition flex items-center justify-center gap-1.5 cursor-pointer ${
            mode === 'files' ? 'bg-white dark:bg-slate-900 text-sky-700 dark:text-sky-400 shadow-2xs font-semibold' : 'hover:text-slate-900 dark:hover:text-slate-100'
          }`}
          onclick={() => { mode = 'files'; }}
          disabled={uploading}
        >
          <UploadCloud class="w-3.5 h-3.5" />
          <span>Файлы</span>
        </button>

        <button
          type="button"
          class={`flex-1 py-1.5 px-2.5 rounded-lg transition flex items-center justify-center gap-1.5 cursor-pointer ${
            mode === 'folder' ? 'bg-white dark:bg-slate-900 text-amber-700 dark:text-amber-400 shadow-2xs font-semibold' : 'hover:text-slate-900 dark:hover:text-slate-100'
          }`}
          onclick={() => { mode = 'folder'; }}
          disabled={uploading}
        >
          <FolderUp class="w-3.5 h-3.5" />
          <span>Папка целиком</span>
        </button>

        <button
          type="button"
          class={`flex-1 py-1.5 px-2.5 rounded-lg transition flex items-center justify-center gap-1.5 cursor-pointer ${
            mode === 'archive' ? 'bg-white dark:bg-slate-900 text-indigo-700 dark:text-indigo-400 shadow-2xs font-semibold' : 'hover:text-slate-900 dark:hover:text-slate-100'
          }`}
          onclick={() => { mode = 'archive'; }}
          disabled={uploading}
        >
          <FileArchive class="w-3.5 h-3.5" />
          <span>Распаковка ZIP</span>
        </button>
      </div>

      <div class="mt-3.5">
        <div class="text-xs text-slate-500 dark:text-slate-400 mb-2 font-mono">
          Каталог назначения: <span class="text-slate-800 dark:text-slate-200 font-semibold">{explorerStore.currentPath}</span>
        </div>

        {#if error}
          <div class="mb-3 p-3 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 text-red-700 dark:text-red-300 rounded-xl text-xs flex items-start gap-2.5 shadow-2xs">
            <AlertCircle class="w-4 h-4 shrink-0 text-red-500 mt-0.5" />
            <span class="break-words">{error}</span>
          </div>
        {/if}

        {#if successMessage}
          <div class="mb-3 p-3 bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-900 text-emerald-700 dark:text-emerald-300 rounded-xl text-xs flex items-center gap-2.5 shadow-2xs">
            <CheckCircle2 class="w-4 h-4 shrink-0 text-emerald-500" />
            <span>{successMessage}</span>
          </div>
        {/if}

        <!-- Drop zone -->
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          ondragover={(e) => { e.preventDefault(); isDragOver = true; }}
          ondragleave={() => { isDragOver = false; }}
          ondrop={handleDrop}
          class={`border-2 border-dashed rounded-xl p-5 text-center transition cursor-pointer ${
            isDragOver
              ? 'border-sky-500 bg-sky-50/50 dark:bg-sky-950/30'
              : 'border-slate-300 dark:border-slate-700 hover:border-slate-400 dark:hover:border-slate-600 bg-slate-50/50 dark:bg-slate-950/50'
          }`}
        >
          {#if mode === 'files'}
            <input
              type="file"
              id="file-upload"
              multiple
              onchange={handleFileSelect}
              class="hidden"
            />
            <label for="file-upload" class="cursor-pointer flex flex-col items-center">
              <UploadCloud class={`w-8 h-8 mb-2 ${isDragOver ? 'text-sky-600 dark:text-sky-400' : 'text-slate-400 dark:text-slate-500'}`} />
              <span class="text-xs font-semibold text-slate-700 dark:text-slate-200">Перетащите файлы сюда или нажмите для выбора</span>
              <span class="text-[11px] text-slate-400 dark:text-slate-500 mt-0.5">Поддерживается выбор нескольких файлов</span>
            </label>

          {:else if mode === 'folder'}
            <input
              type="file"
              id="folder-upload"
              webkitdirectory
              directory
              multiple
              onchange={handleFolderSelect}
              class="hidden"
            />
            <label for="folder-upload" class="cursor-pointer flex flex-col items-center">
              <FolderUp class={`w-8 h-8 mb-2 ${isDragOver ? 'text-amber-600 dark:text-amber-400' : 'text-amber-500'}`} />
              <span class="text-xs font-semibold text-slate-700 dark:text-slate-200">Перетащите папку сюда или нажмите для выбора</span>
              <span class="text-[11px] text-slate-400 dark:text-slate-500 mt-0.5">Все вложенные подкаталоги будут созданы в HDFS рекурсивно</span>
            </label>

          {:else if mode === 'archive'}
            <input
              type="file"
              id="archive-upload"
              accept=".zip"
              onchange={handleArchiveSelect}
              class="hidden"
            />
            <label for="archive-upload" class="cursor-pointer flex flex-col items-center">
              <FileArchive class={`w-8 h-8 mb-2 ${isDragOver ? 'text-indigo-600 dark:text-indigo-400' : 'text-indigo-500'}`} />
              <span class="text-xs font-semibold text-slate-700 dark:text-slate-200">Выберите или перетащите ZIP-архив</span>
              <span class="text-[11px] text-slate-400 dark:text-slate-500 mt-0.5">Сервер автоматически распакует все файлы и папки в каталог HDFS</span>
            </label>
          {/if}
        </div>

        <!-- Selected preview items summary -->
        {#if mode === 'archive' && archiveFile}
          <div class="mt-2.5 p-2.5 bg-indigo-50/60 dark:bg-indigo-950/40 border border-indigo-200 dark:border-indigo-800 rounded-xl flex items-center justify-between text-xs font-mono text-indigo-900 dark:text-indigo-200 shadow-2xs">
            <div class="flex items-center gap-2 truncate">
              <FileArchive class="w-3.5 h-3.5 text-indigo-600 dark:text-indigo-400 shrink-0" />
              <span class="truncate font-semibold">{archiveFile.name}</span>
            </div>
            <span class="shrink-0 text-slate-500 dark:text-slate-400 font-sans ml-2 text-[11px]">{formatBytes(archiveFile.size)}</span>
          </div>

        {:else if mode !== 'archive' && queuedItems.length > 0}
          <div class="mt-2.5 p-2.5 bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 rounded-xl text-xs space-y-1.5 shadow-2xs">
            <div class="flex items-center justify-between text-slate-600 dark:text-slate-400 font-medium text-[11px]">
              <span>Выбрано объектов: <strong>{queuedItems.length}</strong></span>
              <span class="font-mono text-slate-800 dark:text-slate-200 font-semibold">{formatBytes(totalSelectedBytes)}</span>
            </div>

            <!-- Preview first few items -->
            <div class="max-h-20 overflow-y-auto space-y-1 pt-1 font-mono text-[11px] text-slate-500 dark:text-slate-400">
              {#each queuedItems.slice(0, 5) as it}
                <div class="flex items-center gap-1.5 truncate">
                  {#if it.relativePath.includes('/')}
                    <Folder class="w-3.5 h-3.5 text-amber-500 shrink-0" />
                  {:else}
                    <FileIcon class="w-3.5 h-3.5 text-sky-500 shrink-0" />
                  {/if}
                  <span class="truncate">{it.relativePath}</span>
                </div>
              {/each}
              {#if queuedItems.length > 5}
                <div class="text-slate-400 dark:text-slate-500 italic text-[10px]">... и еще {queuedItems.length - 5} файлов</div>
              {/if}
            </div>
          </div>
        {/if}

        <!-- Upload progress indicator -->
        {#if uploading}
          <div class="mt-3 p-3 bg-sky-50 dark:bg-sky-950/40 border border-sky-200 dark:border-sky-900 rounded-xl space-y-2 shadow-2xs">
            <div class="flex items-center justify-between text-xs text-sky-800 dark:text-sky-200 font-medium">
              <span class="flex items-center gap-2">
                <Loader2 class="w-3.5 h-3.5 animate-spin text-sky-600 dark:text-sky-400" />
                {#if mode === 'archive'}
                  <span>Распаковка архива на сервере...</span>
                {:else}
                  <span>Загрузка: {uploadProgress.current} из {uploadProgress.total}</span>
                {/if}
              </span>
              {#if mode !== 'archive' && uploadProgress.totalBytes > 0}
                <span class="font-mono font-semibold">{Math.round((uploadProgress.bytesUploaded / uploadProgress.totalBytes) * 100)}%</span>
              {/if}
            </div>

            {#if mode !== 'archive' && uploadProgress.totalBytes > 0}
              <div class="w-full bg-sky-200 dark:bg-sky-900 rounded-full h-1.5 overflow-hidden">
                <div
                  class="bg-sky-600 dark:bg-sky-400 h-1.5 rounded-full transition-all duration-200"
                  style={`width: ${Math.round((uploadProgress.bytesUploaded / uploadProgress.totalBytes) * 100)}%`}
                ></div>
              </div>
            {/if}
          </div>
        {/if}

        <div class="mt-3 flex items-center gap-2">
          <input
            type="checkbox"
            id="overwrite"
            bind:checked={overwrite}
            disabled={uploading}
            class="rounded border-slate-300 dark:border-slate-700 text-sky-600 focus:ring-sky-500 dark:bg-slate-950"
          />
          <label for="overwrite" class="text-xs text-slate-600 dark:text-slate-300 cursor-pointer select-none">
            Перезаписывать существующие файлы при совпадении имен
          </label>
        </div>
      </div>

      <div class="mt-5 flex items-center justify-end gap-2.5 pt-3.5 border-t border-slate-100 dark:border-slate-800">
        <button
          onclick={() => { resetState(); onClose(); }}
          disabled={uploading}
          class="px-3.5 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 transition cursor-pointer shadow-2xs disabled:opacity-50"
        >
          Отмена
        </button>
        <button
          onclick={startUpload}
          disabled={uploading || (mode === 'archive' ? !archiveFile : queuedItems.length === 0)}
          class="px-4 py-1.5 text-xs font-bold text-white bg-sky-600 hover:bg-sky-500 rounded-lg shadow-md shadow-sky-500/20 transition cursor-pointer disabled:opacity-50 flex items-center gap-1.5 disabled:cursor-not-allowed"
        >
          {#if uploading}
            <Loader2 class="w-3.5 h-3.5 animate-spin" />
            <span>Загрузка...</span>
          {:else}
            <span>Загрузить</span>
          {/if}
        </button>
      </div>
    </div>
  </div>
{/if}
