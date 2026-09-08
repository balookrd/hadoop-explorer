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

  let isUploadOpen = $state(false);
  let isMkdirOpen = $state(false);
  let activeRenameFile = $state<HdfsFileStatus | null>(null);
  let activeDeleteFile = $state<HdfsFileStatus | null>(null);
  let activePreviewFile = $state<HdfsFileStatus | null>(null);
  let activeCopyFile = $state<HdfsFileStatus | null>(null);

  const mockUsers = [
    {
      username: 'admin',
      password: 'password123',
      displayName: 'admin (Global Admin)',
      description: 'Группы: admins, engineers • R/W все кластеры',
      badgeColor: 'text-purple-600'
    },
    {
      username: 'engineer',
      password: 'password123',
      displayName: 'engineer (Data Engineer)',
      description: 'Группы: engineers • R/W в Main Cluster',
      badgeColor: 'text-sky-600'
    },
    {
      username: 'analyst',
      password: 'password123',
      displayName: 'analyst (BI Analyst)',
      description: 'Группы: analysts • Read-Only доступ',
      badgeColor: 'text-emerald-600'
    }
  ];

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
  <div class="min-h-screen bg-slate-50 flex flex-col items-center justify-center text-slate-800 gap-3">
    <div class="w-8 h-8 border-3 border-sky-600 border-t-transparent rounded-full animate-spin"></div>
    <span class="text-xs font-medium text-slate-500">Проверка сессии...</span>
  </div>
{:else if !authStore.isAuthenticated}
  <LoginModal
    title="HDFS Explorer"
    subtitle="Корпоративный файловый менеджер для кластеров Hadoop HDFS"
    icon={Server}
    isModal={false}
    {mockUsers}
    initialError={authStore.error}
    onLogin={handleLogin}
    onKerberosSso={handleKerberosSso}
  />
{:else}
  <div class="min-h-screen flex flex-col bg-slate-50">
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

    <!-- Modals (Lazy Loaded) -->
    {#if isUploadOpen}
      {#await import('./lib/components/Modals/UploadModal.svelte') then { default: UploadModal }}
        <UploadModal
          isOpen={isUploadOpen}
          onClose={() => { isUploadOpen = false; }}
        />
      {/await}
    {/if}

    {#if isMkdirOpen}
      {#await import('./lib/components/Modals/MkdirModal.svelte') then { default: MkdirModal }}
        <MkdirModal
          isOpen={isMkdirOpen}
          onClose={() => { isMkdirOpen = false; }}
        />
      {/await}
    {/if}

    {#if activeRenameFile}
      {#await import('./lib/components/Modals/RenameModal.svelte') then { default: RenameModal }}
        <RenameModal
          file={activeRenameFile}
          onClose={() => { activeRenameFile = null; }}
        />
      {/await}
    {/if}

    {#if activeDeleteFile}
      {#await import('./lib/components/Modals/DeleteModal.svelte') then { default: DeleteModal }}
        <DeleteModal
          file={activeDeleteFile}
          onClose={() => { activeDeleteFile = null; }}
        />
      {/await}
    {/if}

    {#if activePreviewFile}
      {#await import('./lib/components/Modals/PreviewModal.svelte') then { default: PreviewModal }}
        <PreviewModal
          file={activePreviewFile}
          onClose={() => { activePreviewFile = null; }}
        />
      {/await}
    {/if}

    {#if activeCopyFile}
      {#await import('./lib/components/Modals/CrossClusterCopyModal.svelte') then { default: CrossClusterCopyModal }}
        <CrossClusterCopyModal
          file={activeCopyFile}
          onClose={() => { activeCopyFile = null; }}
        />
      {/await}
    {/if}
  </div>
{/if}
