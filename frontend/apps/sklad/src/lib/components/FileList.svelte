<script lang="ts">
  import { explorerStore } from '../stores/explorer.svelte';
  import { api } from '../api/client';
  import type { HdfsFileStatus } from '../types';
  import { formatBytes, formatDate, formatPermissions, getFileCategory } from '../utils/format';
  import {
    Folder,
    FileText,
    FileCode,
    FileArchive,
    Database,
    File as FileGeneric,
    Download,
    Eye,
    Edit2,
    Trash2,
    ArrowUpDown,
    CornerLeftUp,
    AlertCircle,
    ArrowRightLeft,
    FolderDown
  } from 'lucide-svelte';

  interface Props {
    onPreview: (file: HdfsFileStatus) => void;
    onRename: (file: HdfsFileStatus) => void;
    onDelete: (file: HdfsFileStatus) => void;
    onCopyCrossCluster: (file: HdfsFileStatus) => void;
  }

  let { onPreview, onRename, onDelete, onCopyCrossCluster }: Props = $props();

  const ROW_HEIGHT = 37;
  const OVERSCAN = 12;

  let scrollContainer = $state<HTMLDivElement | null>(null);
  let scrollTop = $state<number>(0);
  let viewportHeight = $state<number>(600);

  let totalCount = $derived(explorerStore.filteredFiles.length);
  let startIndex = $derived(
    Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN)
  );
  let endIndex = $derived(
    Math.min(totalCount, Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + OVERSCAN)
  );
  let visibleFiles = $derived(
    explorerStore.filteredFiles.slice(startIndex, endIndex)
  );
  let topSpacerHeight = $derived(startIndex * ROW_HEIGHT);
  let bottomSpacerHeight = $derived(Math.max(0, (totalCount - endIndex) * ROW_HEIGHT));

  function handleScroll(e: Event) {
    const target = e.target as HTMLDivElement;
    scrollTop = target.scrollTop;
  }

  $effect(() => {
    // Сброс скролла при смене пути, поиска или сортировки
    const _path = explorerStore.currentPath;
    const _q = explorerStore.searchQuery;
    const _sortBy = explorerStore.sortBy;
    const _sortAsc = explorerStore.sortAsc;

    if (scrollContainer) {
      scrollContainer.scrollTop = 0;
      scrollTop = 0;
    }
  });

  function getFileIcon(file: HdfsFileStatus) {
    if (file.type === 'DIRECTORY') return Folder;
    const cat = getFileCategory(file.pathSuffix);
    if (cat === 'text') return FileText;
    if (cat === 'code') return FileCode;
    if (cat === 'archive') return FileArchive;
    if (cat === 'data') return Database;
    return FileGeneric;
  }

  function handleItemClick(file: HdfsFileStatus) {
    if (file.type === 'DIRECTORY') {
      const p = explorerStore.currentPath === '/'
        ? `/${file.pathSuffix}`
        : `${explorerStore.currentPath}/${file.pathSuffix}`;
      explorerStore.navigateTo(p);
    } else {
      onPreview(file);
    }
  }

  function getDownloadUrl(file: HdfsFileStatus) {
    if (!explorerStore.currentCluster) return '#';
    const p = explorerStore.currentPath === '/'
      ? `/${file.pathSuffix}`
      : `${explorerStore.currentPath}/${file.pathSuffix}`;
    return api.getDownloadUrl(explorerStore.currentCluster.id, p);
  }
</script>

<svelte:window
  onkeydown={(e) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'a') {
      const activeTag = document.activeElement?.tagName?.toLowerCase();
      if (activeTag !== 'input' && activeTag !== 'textarea') {
        e.preventDefault();
        explorerStore.selectAll();
      }
    }
    if (e.key === 'Escape' && explorerStore.selectedFileNames.length > 0) {
      explorerStore.clearSelection();
    }
  }}
/>

<div class="w-full px-4 sm:px-6 lg:px-8 py-5 select-none">
  {#if explorerStore.error}
    <div class="p-3.5 mb-4 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 rounded-xl flex items-start gap-2.5 text-red-800 dark:text-red-300 text-xs shadow-2xs">
      <AlertCircle class="w-4 h-4 text-red-500 shrink-0 mt-0.5" />
      <div>
        <h4 class="font-bold text-red-900 dark:text-red-200">Ошибка доступа или выполнения операции</h4>
        <p class="text-red-700 dark:text-red-400 mt-0.5 font-mono">{explorerStore.error}</p>
      </div>
    </div>
  {/if}

  <div class="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-2xs overflow-hidden flex flex-col">
    <div
      bind:this={scrollContainer}
      bind:clientHeight={viewportHeight}
      onscroll={handleScroll}
      class="overflow-auto max-h-[calc(100vh-215px)] relative"
    >
      <table class="w-full table-fixed text-left text-xs text-slate-600 dark:text-slate-300">
        <thead class="sticky top-0 z-10 bg-slate-50 dark:bg-slate-950 border-b border-slate-200 dark:border-slate-800 text-xs font-semibold text-slate-700 dark:text-slate-300 select-none shadow-xs">
          <tr>
            <th scope="col" class="w-10 pl-4 sm:pl-6 py-2.5">
              <input
                type="checkbox"
                aria-label="Выбрать все"
                checked={explorerStore.isAllSelected}
                indeterminate={explorerStore.isSomeSelected}
                onchange={() => explorerStore.toggleSelectAll()}
                class="w-3.5 h-3.5 rounded border-slate-300 dark:border-slate-700 text-sky-600 focus:ring-sky-500 cursor-pointer bg-white dark:bg-slate-900"
              />
            </th>
            <th scope="col" class="py-2.5 px-3 cursor-pointer hover:text-sky-600 dark:hover:text-sky-400 transition" onclick={() => explorerStore.toggleSort('name')}>
              <div class="flex items-center gap-1.5">
                <span>Имя</span>
                <ArrowUpDown class="w-3.5 h-3.5 text-slate-400 dark:text-slate-500" />
              </div>
            </th>
            <th scope="col" class="w-28 px-3 py-2.5 cursor-pointer hover:text-sky-600 dark:hover:text-sky-400 transition" onclick={() => explorerStore.toggleSort('size')}>
              <div class="flex items-center gap-1.5">
                <span>Размер</span>
                <ArrowUpDown class="w-3.5 h-3.5 text-slate-400 dark:text-slate-500" />
              </div>
            </th>
            <th scope="col" class="w-44 px-3 py-2.5">Владелец : Группа</th>
            <th scope="col" class="w-24 px-3 py-2.5">Права</th>
            <th scope="col" class="w-44 px-3 py-2.5 cursor-pointer hover:text-sky-600 dark:hover:text-sky-400 transition" onclick={() => explorerStore.toggleSort('modified')}>
              <div class="flex items-center gap-1.5">
                <span>Дата изменения</span>
                <ArrowUpDown class="w-3.5 h-3.5 text-slate-400 dark:text-slate-500" />
              </div>
            </th>
            <th scope="col" class="w-36 py-2.5 pl-3 pr-4 sm:pr-6 text-right">Действия</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800/60 font-mono text-xs">
          <!-- Кнопка перехода на уровень выше внутри списка -->
          {#if explorerStore.parentPath}
            <tr
              onclick={() => explorerStore.navigateUp()}
              class="hover:bg-slate-50 dark:hover:bg-slate-800/50 transition cursor-pointer group"
            >
              <td colspan="7" class="py-2 pl-4 pr-3 sm:pl-6 text-slate-500 dark:text-slate-400 group-hover:text-sky-600 dark:group-hover:text-sky-400 font-medium">
                <div class="flex items-center gap-2">
                  <CornerLeftUp class="w-3.5 h-3.5 text-slate-400 group-hover:text-sky-600 dark:group-hover:text-sky-400" />
                  <span>.. (вверх)</span>
                </div>
              </td>
            </tr>
          {/if}

          {#if totalCount === 0}
            <tr>
              <td colspan="7" class="py-12 text-center text-slate-400 dark:text-slate-500 font-sans">
                {#if explorerStore.loading}
                  <div class="flex flex-col items-center justify-center gap-2">
                    <div class="w-5 h-5 border-2 border-sky-600 border-t-transparent rounded-full animate-spin"></div>
                    <span class="text-xs">Загрузка каталога...</span>
                  </div>
                {:else if explorerStore.searchQuery}
                  <p class="text-xs">Ничего не найдено по запросу "{explorerStore.searchQuery}"</p>
                {:else}
                  <p class="text-xs">Папка пуста</p>
                {/if}
              </td>
            </tr>
          {:else}
            <!-- Верхний спейсер для виртуализации -->
            {#if topSpacerHeight > 0}
              <tr style="height: {topSpacerHeight}px;" aria-hidden="true">
                <td colspan="7" class="p-0 border-0 m-0"></td>
              </tr>
            {/if}

            {#each visibleFiles as file (file.pathSuffix)}
              {@const Icon = getFileIcon(file)}
              <tr class="{explorerStore.isSelected(file.pathSuffix) ? 'bg-sky-50 dark:bg-sky-950/40' : 'hover:bg-sky-50/50 dark:hover:bg-sky-950/20'} transition group h-[37px]">
                <!-- Checkbox -->
                <td class="w-10 pl-4 sm:pl-6 py-2 select-none">
                  <input
                    type="checkbox"
                    aria-label={`Выбрать ${file.pathSuffix}`}
                    checked={explorerStore.isSelected(file.pathSuffix)}
                    onclick={(e) => {
                      e.stopPropagation();
                      explorerStore.toggleSelect(file.pathSuffix, e.shiftKey);
                    }}
                    class="w-3.5 h-3.5 rounded border-slate-300 dark:border-slate-700 text-sky-600 focus:ring-sky-500 cursor-pointer bg-white dark:bg-slate-900"
                  />
                </td>

                <!-- Name -->
                <td class="py-2 px-3 min-w-0 font-sans">
                  <button
                    onclick={() => handleItemClick(file)}
                    class="flex items-center gap-2.5 text-left font-medium text-slate-800 dark:text-slate-200 hover:text-sky-600 dark:hover:text-sky-400 transition min-w-0 max-w-full w-full cursor-pointer"
                    title={file.pathSuffix}
                  >
                    <span class={`p-1 rounded-md shrink-0 ${file.type === 'DIRECTORY' ? 'bg-amber-100/80 dark:bg-amber-950/60 text-amber-700 dark:text-amber-400' : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400'}`}>
                      <Icon class="w-3.5 h-3.5" />
                    </span>
                    <span class="truncate min-w-0 flex-1 text-xs" title={file.pathSuffix}>
                      {file.pathSuffix}
                    </span>
                  </button>
                </td>

                <!-- Size -->
                <td class="px-3 py-2 text-xs font-mono text-slate-600 dark:text-slate-400 whitespace-nowrap">
                  {file.type === 'DIRECTORY' ? '-' : formatBytes(file.length)}
                </td>

                <!-- Owner / Group -->
                <td class="px-3 py-2 text-xs font-mono whitespace-nowrap truncate" title={`${file.owner}:${file.group}`}>
                  <span class="text-slate-800 dark:text-slate-200 font-medium">{file.owner}</span>
                  <span class="text-slate-400 dark:text-slate-600">:</span>
                  <span class="text-slate-500 dark:text-slate-400">{file.group}</span>
                </td>

                <!-- Permissions -->
                <td class="px-3 py-2 text-xs font-mono text-slate-500 dark:text-slate-400 whitespace-nowrap">
                  <span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-600 dark:text-slate-300 text-[10px]">
                    {formatPermissions(file.permission)}
                  </span>
                </td>

                <!-- Modified -->
                <td class="px-3 py-2 text-xs text-slate-500 dark:text-slate-400 whitespace-nowrap font-sans">
                  {formatDate(file.modificationTime)}
                </td>

                <!-- Actions -->
                <td class="py-2 pl-3 pr-4 sm:pr-6 text-right whitespace-nowrap font-sans">
                  <div class="flex items-center justify-end gap-1 opacity-75 group-hover:opacity-100 transition">
                    {#if file.type === 'FILE'}
                      <button
                        onclick={() => onPreview(file)}
                        class="p-1 text-slate-400 dark:text-slate-500 hover:text-sky-600 dark:hover:text-sky-400 hover:bg-sky-50 dark:hover:bg-sky-950/40 rounded-md transition cursor-pointer"
                        title="Предпросмотр"
                      >
                        <Eye class="w-3.5 h-3.5" />
                      </button>

                      <a
                        href={getDownloadUrl(file)}
                        download
                        class="p-1 text-slate-400 dark:text-slate-500 hover:text-emerald-600 dark:hover:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-950/40 rounded-md transition cursor-pointer"
                        title="Скачать файл"
                      >
                        <Download class="w-3.5 h-3.5" />
                      </a>
                    {:else}
                      <a
                        href={getDownloadUrl(file)}
                        download
                        class="p-1 text-slate-400 dark:text-slate-500 hover:text-emerald-600 dark:hover:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-950/40 rounded-md transition cursor-pointer"
                        title="Скачать папку (ZIP-архив)"
                      >
                        <FolderDown class="w-3.5 h-3.5" />
                      </a>
                    {/if}

                    <button
                      onclick={() => onCopyCrossCluster(file)}
                      class="p-1 text-slate-400 dark:text-slate-500 hover:text-purple-600 dark:hover:text-purple-400 hover:bg-purple-50 dark:hover:bg-purple-950/40 rounded-md transition cursor-pointer"
                      title="Скопировать в другой кластер"
                    >
                      <ArrowRightLeft class="w-3.5 h-3.5" />
                    </button>

                    {#if explorerStore.canWrite}
                      <button
                        onclick={() => onRename(file)}
                        class="p-1 text-slate-400 dark:text-slate-500 hover:text-indigo-600 dark:hover:text-indigo-400 hover:bg-indigo-50 dark:hover:bg-indigo-950/40 rounded-md transition cursor-pointer"
                        title="Переименовать"
                      >
                        <Edit2 class="w-3.5 h-3.5" />
                      </button>

                      <button
                        onclick={() => onDelete(file)}
                        class="p-1 text-slate-400 dark:text-slate-500 hover:text-red-600 dark:hover:text-red-400 hover:bg-red-50 dark:hover:bg-red-950/40 rounded-md transition cursor-pointer"
                        title="Удалить"
                      >
                        <Trash2 class="w-3.5 h-3.5" />
                      </button>
                    {/if}
                  </div>
                </td>
              </tr>
            {/each}

            <!-- Нижний спейсер для виртуализации -->
            {#if bottomSpacerHeight > 0}
              <tr style="height: {bottomSpacerHeight}px;" aria-hidden="true">
                <td colspan="6" class="p-0 border-0 m-0"></td>
              </tr>
            {/if}
          {/if}
        </tbody>
      </table>
    </div>
  </div>
</div>
