<script lang="ts">
  import { onMount } from 'svelte';
  import { authStore } from './lib/stores/auth.svelte';
  import { explorerStore } from './lib/stores/explorer.svelte';
  import type { HdfsFileStatus } from './lib/types';

  import Header from './lib/components/Header.svelte';
  import Breadcrumbs from './lib/components/Breadcrumbs.svelte';
  import ActionToolbar from './lib/components/ActionToolbar.svelte';
  import FileList from './lib/components/FileList.svelte';
  import LoginView from './lib/components/LoginView.svelte';

  import UploadModal from './lib/components/Modals/UploadModal.svelte';
  import MkdirModal from './lib/components/Modals/MkdirModal.svelte';
  import RenameModal from './lib/components/Modals/RenameModal.svelte';
  import DeleteModal from './lib/components/Modals/DeleteModal.svelte';
  import PreviewModal from './lib/components/Modals/PreviewModal.svelte';
  import CrossClusterCopyModal from './lib/components/Modals/CrossClusterCopyModal.svelte';

  let isUploadOpen = $state(false);
  let isMkdirOpen = $state(false);
  let activeRenameFile = $state<HdfsFileStatus | null>(null);
  let activeDeleteFile = $state<HdfsFileStatus | null>(null);
  let activePreviewFile = $state<HdfsFileStatus | null>(null);
  let activeCopyFile = $state<HdfsFileStatus | null>(null);

  onMount(async () => {
    await authStore.init();
    if (authStore.isAuthenticated) {
      await explorerStore.init();
    }
  });
</script>

{#if authStore.loading}
  <div class="min-h-screen bg-slate-50 flex flex-col items-center justify-center text-slate-800 gap-3">
    <div class="w-8 h-8 border-3 border-sky-600 border-t-transparent rounded-full animate-spin"></div>
    <span class="text-xs font-medium text-slate-500">Проверка сессии...</span>
  </div>
{:else if !authStore.isAuthenticated}
  <LoginView />
{:else}
  <div class="min-h-screen flex flex-col bg-slate-50">
    <Header />
    <Breadcrumbs />
    <ActionToolbar
      onOpenUpload={() => { isUploadOpen = true; }}
      onOpenMkdir={() => { isMkdirOpen = true; }}
    />

    <main class="flex-1">
      <FileList
        onPreview={(file) => { activePreviewFile = file; }}
        onRename={(file) => { activeRenameFile = file; }}
        onDelete={(file) => { activeDeleteFile = file; }}
        onCopyCrossCluster={(file) => { activeCopyFile = file; }}
      />
    </main>

    <!-- Modals -->
    <UploadModal
      isOpen={isUploadOpen}
      onClose={() => { isUploadOpen = false; }}
    />

    <MkdirModal
      isOpen={isMkdirOpen}
      onClose={() => { isMkdirOpen = false; }}
    />

    <RenameModal
      file={activeRenameFile}
      onClose={() => { activeRenameFile = null; }}
    />

    <DeleteModal
      file={activeDeleteFile}
      onClose={() => { activeDeleteFile = null; }}
    />

    <PreviewModal
      file={activePreviewFile}
      onClose={() => { activePreviewFile = null; }}
    />

    <CrossClusterCopyModal
      file={activeCopyFile}
      onClose={() => { activeCopyFile = null; }}
    />
  </div>
{/if}
