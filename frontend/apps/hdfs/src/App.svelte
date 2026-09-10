<script lang="ts">
  import { onMount } from 'svelte';
  import { authStore } from './lib/stores/auth.svelte';
  import { explorerStore } from './lib/stores/explorer.svelte';
  import type { HdfsFileStatus } from './lib/types';
  import { Header, LoginModal } from '@hadoop-explorer/common';
  import { Server } from 'lucide-svelte';

  import Breadcrumbs from './lib/components/Breadcrumbs.svelte';
  import ActionToolbar from './lib/components/ActionToolbar.svelte';
  import FileList from './lib/components/FileList.svelte';

  import UploadModal from './lib/components/Modals/UploadModal.svelte';
  import MkdirModal from './lib/components/Modals/MkdirModal.svelte';
  import RenameModal from './lib/components/Modals/RenameModal.svelte';
  import DeleteModal from './lib/components/Modals/DeleteModal.svelte';
  import BatchDeleteModal from './lib/components/Modals/BatchDeleteModal.svelte';
  import PreviewModal from './lib/components/Modals/PreviewModal.svelte';
  import CrossClusterCopyModal from './lib/components/Modals/CrossClusterCopyModal.svelte';
  import BatchActionBar from './lib/components/BatchActionBar.svelte';

  let isUploadOpen = $state(false);
  let isMkdirOpen = $state(false);
  let isBatchDeleteOpen = $state(false);
  let activeRenameFile = $state<HdfsFileStatus | null>(null);
  let activeDeleteFile = $state<HdfsFileStatus | null>(null);
  let activePreviewFile = $state<HdfsFileStatus | null>(null);
  let activeCopyFile = $state<HdfsFileStatus | null>(null);

  async function handleLogin(u: string, p: string) {
    const success = await authStore.login(u, p);
    if (success) {
      await explorerStore.init();
    }
  }

  async function handleKerberosSso() {
    try {
      const resp = await fetch('/api/v1/auth/sso', {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include'
      });
      if (resp.ok) {
        const data = await resp.json();
        if (data.success || data.user) {
          authStore.user = data.user;
          await explorerStore.init();
          return;
        }
      }
      authStore.error = 'Kerberos SPNEGO SSO билет не предоставлен браузером.';
    } catch {
      authStore.error = 'Сетевая ошибка при проверке Kerberos SSO.';
    }
  }

  function handleClusterSelect(clusterId: string) {
    const cluster = explorerStore.clusters.find(c => c.id === clusterId);
    if (cluster) {
      explorerStore.selectCluster(cluster);
    }
  }

  onMount(async () => {
    await authStore.init();
    if (authStore.isAuthenticated) {
      await explorerStore.init();
    }
  });
</script>

{#if authStore.loading}
  <div class="min-h-screen bg-slate-50 dark:bg-slate-950 flex flex-col items-center justify-center text-slate-800 dark:text-slate-100 gap-3">
    <div class="w-8 h-8 border-3 border-sky-600 border-t-transparent rounded-full animate-spin"></div>
    <span class="text-xs font-medium text-slate-500 dark:text-slate-400">Проверка сессии...</span>
  </div>
{:else if !authStore.isAuthenticated}
  <LoginModal
    title="HDFS Explorer"
    subtitle="Аутентификация LDAP & Kerberos SSO"
    icon={Server}
    isModal={false}
    initialError={authStore.error}
    onLogin={handleLogin}
    onKerberosSso={handleKerberosSso}
  />
{:else}
  <div class="min-h-screen flex flex-col bg-slate-50 dark:bg-slate-950 text-slate-800 dark:text-slate-100">
    <Header
      title="HDFS Explorer"
      subtitle="Multi-Cluster"
      icon={Server}
      user={authStore.user}
      clusters={explorerStore.clusters}
      selectedClusterId={explorerStore.currentCluster?.id || ''}
      onClusterSelect={handleClusterSelect}
      onLogout={() => authStore.logout()}
    />
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
    {#if isUploadOpen}
      <UploadModal
        isOpen={isUploadOpen}
        onClose={() => { isUploadOpen = false; }}
      />
    {/if}

    {#if isMkdirOpen}
      <MkdirModal
        isOpen={isMkdirOpen}
        onClose={() => { isMkdirOpen = false; }}
      />
    {/if}

    {#if activeRenameFile}
      <RenameModal
        file={activeRenameFile}
        onClose={() => { activeRenameFile = null; }}
      />
    {/if}

    {#if activeDeleteFile}
      <DeleteModal
        file={activeDeleteFile}
        onClose={() => { activeDeleteFile = null; }}
      />
    {/if}

    {#if activePreviewFile}
      <PreviewModal
        file={activePreviewFile}
        onClose={() => { activePreviewFile = null; }}
      />
    {/if}

    {#if activeCopyFile}
      <CrossClusterCopyModal
        file={activeCopyFile}
        onClose={() => { activeCopyFile = null; }}
      />
    {/if}

    <!-- Пакетные действия и модальное окно -->
    <BatchActionBar
      onOpenBatchDelete={() => { isBatchDeleteOpen = true; }}
    />

    {#if isBatchDeleteOpen}
      <BatchDeleteModal
        isOpen={isBatchDeleteOpen}
        onClose={() => { isBatchDeleteOpen = false; }}
      />
    {/if}
  </div>
{/if}

