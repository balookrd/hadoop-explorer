import { api } from '../api/client';
import type { ClusterPublicInfo, HdfsFileStatus, DirectoryListingResponse } from '../types';

class ExplorerState {
  clusters = $state<ClusterPublicInfo[]>([]);
  currentCluster = $state<ClusterPublicInfo | null>(null);
  currentPath = $state<string>('/');
  parentPath = $state<string | null>(null);
  files = $state<HdfsFileStatus[]>([]);
  totalFiles = $state<number>(0);
  totalDirs = $state<number>(0);
  totalSize = $state<number>(0);
  canWrite = $state<boolean>(true);

  loading = $state<boolean>(false);
  error = $state<string | null>(null);

  searchQuery = $state<string>('');
  sortBy = $state<'name' | 'size' | 'modified' | 'type'>('type');
  sortAsc = $state<boolean>(true);

  // Мультивыбор файлов
  selectedFileNames = $state<string[]>([]);
  lastSelectedName = $state<string | null>(null);

  // Вычисляемый отфильтрованный и отсортированный список файлов
  filteredFiles = $derived.by(() => {
    let result = this.files;
    if (this.searchQuery.trim()) {
      const q = this.searchQuery.toLowerCase();
      result = result.filter(f => f.pathSuffix.toLowerCase().includes(q));
    }

    return [...result].sort((a, b) => {
      // Директории всегда первыми
      if (a.type !== b.type) {
        return a.type === 'DIRECTORY' ? -1 : 1;
      }

      let cmp = 0;
      if (this.sortBy === 'name') {
        cmp = a.pathSuffix.localeCompare(b.pathSuffix);
      } else if (this.sortBy === 'size') {
        cmp = a.length - b.length;
      } else if (this.sortBy === 'modified') {
        cmp = a.modificationTime - b.modificationTime;
      } else {
        cmp = a.pathSuffix.localeCompare(b.pathSuffix);
      }

      return this.sortAsc ? cmp : -cmp;
    });
  });

  // Выбранные объекты файлов
  selectedItems = $derived.by(() => {
    const set = new Set(this.selectedFileNames);
    return this.filteredFiles.filter(f => set.has(f.pathSuffix));
  });

  // Полные пути выбранных объектов
  selectedFullPaths = $derived.by(() => {
    const base = this.currentPath === '/' ? '' : this.currentPath;
    return this.selectedItems.map(f => `${base}/${f.pathSuffix}`);
  });

  // Суммарный размер выбранных файлов
  selectedTotalSize = $derived.by(() => {
    return this.selectedItems.reduce((acc, f) => acc + (f.length || 0), 0);
  });

  // Флаги выбора всех или части элементов
  isAllSelected = $derived.by(() => {
    return this.filteredFiles.length > 0 && this.selectedFileNames.length === this.filteredFiles.length;
  });

  isSomeSelected = $derived.by(() => {
    return this.selectedFileNames.length > 0 && this.selectedFileNames.length < this.filteredFiles.length;
  });

  private parseUrlState(): { clusterId?: string; path?: string } {
    if (typeof window === 'undefined') return {};
    const hash = window.location.hash.startsWith('#') ? window.location.hash.slice(1) : window.location.hash;
    const search = window.location.search.startsWith('?') ? window.location.search.slice(1) : window.location.search;
    const params = new URLSearchParams(hash || search);
    return {
      clusterId: params.get('cluster') || undefined,
      path: params.get('path') || undefined
    };
  }

  private updateUrl(push: boolean = false) {
    if (typeof window === 'undefined' || !this.currentCluster) return;
    const params = new URLSearchParams();
    params.set('cluster', this.currentCluster.id);
    params.set('path', this.currentPath || '/');
    const newHash = `#${params.toString()}`;
    if (window.location.hash !== newHash) {
      if (push) {
        window.history.pushState(null, '', newHash);
      } else {
        window.history.replaceState(null, '', newHash);
      }
    }
  }

  async init() {
    this.loading = true;
    this.error = null;

    if (typeof window !== 'undefined') {
      window.addEventListener('hashchange', async () => {
        const { clusterId, path } = this.parseUrlState();
        if (clusterId && (!this.currentCluster || this.currentCluster.id !== clusterId)) {
          const target = this.clusters.find(c => c.id === clusterId);
          if (target) {
            await this.selectCluster(target, path || target.default_path || '/', false);
            return;
          }
        }
        if (path && path !== this.currentPath) {
          await this.loadDirectory(path, false);
        }
      });
    }

    try {
      this.clusters = await api.getClusters();
      if (this.clusters.length > 0) {
        const { clusterId, path } = this.parseUrlState();
        const targetCluster = this.clusters.find(c => c.id === clusterId) || this.clusters[0];
        const targetPath = path || targetCluster.default_path || '/';
        await this.selectCluster(targetCluster, targetPath, false);
      }
    } catch (err: any) {
      this.error = err.message || 'Не удалось загрузить список кластеров';
    } finally {
      this.loading = false;
    }
  }

  async selectCluster(cluster: ClusterPublicInfo, targetPath?: string, updateUrl: boolean = true) {
    this.currentCluster = cluster;
    this.currentPath = targetPath || cluster.default_path || '/';
    await this.loadDirectory(this.currentPath, updateUrl);
  }

  async loadDirectory(path: string, updateUrl: boolean = true) {
    if (!this.currentCluster) return;

    this.loading = true;
    this.error = null;
    this.clearSelection();
    try {
      const resp: DirectoryListingResponse = await api.listFiles(this.currentCluster.id, path);
      this.currentPath = resp.path;
      this.parentPath = resp.parent_path || null;
      this.files = resp.files;
      this.totalFiles = resp.total_files;
      this.totalDirs = resp.total_directories;
      this.totalSize = resp.total_size;
      this.canWrite = resp.can_write;
      if (updateUrl) {
        this.updateUrl(true);
      }
    } catch (err: any) {
      this.error = err.message || 'Ошибка загрузки каталога HDFS';
    } finally {
      this.loading = false;
    }
  }

  async navigateTo(path: string) {
    await this.loadDirectory(path, true);
  }

  async navigateUp() {
    if (this.parentPath) {
      await this.loadDirectory(this.parentPath, true);
    }
  }

  async refresh() {
    await this.loadDirectory(this.currentPath, false);
  }

  toggleSort(field: 'name' | 'size' | 'modified') {
    if (this.sortBy === field) {
      this.sortAsc = !this.sortAsc;
    } else {
      this.sortBy = field;
      this.sortAsc = true;
    }
  }

  // Методы мультивыбора
  isSelected(fileName: string): boolean {
    return this.selectedFileNames.includes(fileName);
  }

  toggleSelect(fileName: string, shiftKey: boolean = false) {
    const files = this.filteredFiles;
    const currentIndex = files.findIndex(f => f.pathSuffix === fileName);
    if (currentIndex === -1) return;

    if (shiftKey && this.lastSelectedName) {
      const lastIndex = files.findIndex(f => f.pathSuffix === this.lastSelectedName);
      if (lastIndex !== -1) {
        const start = Math.min(currentIndex, lastIndex);
        const end = Math.max(currentIndex, lastIndex);
        const rangeNames = files.slice(start, end + 1).map(f => f.pathSuffix);
        const merged = new Set([...this.selectedFileNames, ...rangeNames]);
        this.selectedFileNames = Array.from(merged);
        this.lastSelectedName = fileName;
        return;
      }
    }

    if (this.selectedFileNames.includes(fileName)) {
      this.selectedFileNames = this.selectedFileNames.filter(name => name !== fileName);
    } else {
      this.selectedFileNames = [...this.selectedFileNames, fileName];
    }
    this.lastSelectedName = fileName;
  }

  toggleSelectAll() {
    if (this.isAllSelected) {
      this.clearSelection();
    } else {
      this.selectAll();
    }
  }

  selectAll() {
    this.selectedFileNames = this.filteredFiles.map(f => f.pathSuffix);
  }

  clearSelection() {
    this.selectedFileNames = [];
    this.lastSelectedName = null;
  }
}

export const explorerStore = new ExplorerState();
