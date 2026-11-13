<script lang="ts">
  import { X, Settings, Cpu, Layers, Box, Package, Database, Check, FolderArchive } from 'lucide-svelte';
  import type { ClusterDetailResponse, CreateSessionPayload } from '../types';

  let {
    isOpen = false,
    clusterDetails,
    initialValues,
    targetKind = 'pyspark',
    targetLanguage = 'pyspark',
    onClose,
    onSave
  }: {
    isOpen: boolean;
    clusterDetails: ClusterDetailResponse | null;
    initialValues: Partial<CreateSessionPayload>;
    targetKind?: 'pyspark' | 'spark';
    targetLanguage?: 'pyspark' | 'scalaspark' | 'sql';
    onClose: () => void;
    onSave: (payload: CreateSessionPayload) => void;
  } = $props();

  let activeTab = $state<'general' | 'dependencies' | 'conf'>('general');

  let selectedSparkVersionId = $state('');
  let selectedPythonEnvId = $state('');
  let customPythonArchive = $state('');
  let customPythonPath = $state('./environment/bin/python');
  let selectedMetastoreId = $state('');
  let selectedYarnQueue = $state('');
  let selectedProfile = $state('');
  let selectedKind = $state('pyspark');

  let packagesText = $state('');
  let jarsText = $state('');
  let pyFilesText = $state('');
  let customConfText = $state('');

  $effect(() => {
    if (isOpen && clusterDetails) {
      selectedSparkVersionId = initialValues.spark_version_id || clusterDetails.spark_versions.find((v) => v.is_default)?.id || clusterDetails.spark_versions[0]?.id || '';
      
      const v = clusterDetails.spark_versions.find((v) => v.id === selectedSparkVersionId);
      if (initialValues.python_env_id === 'custom' || initialValues.custom_python_archive) {
        selectedPythonEnvId = '__custom__';
        customPythonArchive = initialValues.custom_python_archive || '';
        customPythonPath = initialValues.custom_python_path || './environment/bin/python';
      } else {
        selectedPythonEnvId = initialValues.python_env_id || v?.python_versions.find((p) => p.is_default)?.id || v?.python_versions[0]?.id || '';
        customPythonArchive = '';
        customPythonPath = './environment/bin/python';
      }
      
      selectedMetastoreId = initialValues.metastore_id || clusterDetails.metastores.find((m) => m.is_default)?.id || clusterDetails.metastores[0]?.id || '';
      selectedYarnQueue = initialValues.yarn_queue || clusterDetails.default_queue || clusterDetails.yarn_queues[0] || 'default';
      selectedProfile = initialValues.resource_profile || clusterDetails.resource_profiles[0]?.id || 'small';
      selectedKind = targetKind || initialValues.kind || 'pyspark';

      packagesText = (initialValues.packages || []).join(', ');
      jarsText = (initialValues.jars || []).join('\n');
      pyFilesText = (initialValues.py_files || []).join('\n');
      
      if (initialValues.spark_conf) {
        customConfText = Object.entries(initialValues.spark_conf)
          .map(([k, val]) => `${k}=${val}`)
          .join('\n');
      } else {
        customConfText = '';
      }
    }
  });

  const currentSparkVersion = $derived(
    clusterDetails?.spark_versions.find((v) => v.id === selectedSparkVersionId)
  );

  function handleSparkVersionChange(id: string) {
    selectedSparkVersionId = id;
    const v = clusterDetails?.spark_versions.find((item) => item.id === id);
    if (v && v.python_versions.length > 0) {
      selectedPythonEnvId = v.python_versions.find((p) => p.is_default)?.id || v.python_versions[0].id;
    } else {
      selectedPythonEnvId = '';
    }
  }

  function handleSave() {
    if (!clusterDetails) return;

    const packages = packagesText
      .split(',')
      .map((p) => p.trim())
      .filter(Boolean);

    const jars = jarsText
      .split('\n')
      .map((j) => j.trim())
      .filter(Boolean);

    const pyFiles = pyFilesText
      .split('\n')
      .map((p) => p.trim())
      .filter(Boolean);

    const sparkConf: Record<string, string> = {};
    customConfText.split('\n').forEach((line) => {
      const idx = line.indexOf('=');
      if (idx > 0) {
        const key = line.slice(0, idx).trim();
        const val = line.slice(idx + 1).trim();
        if (key && val) sparkConf[key] = val;
      }
    });

    const isPythonRequired = targetLanguage === 'pyspark';
    const isCustom = selectedPythonEnvId === '__custom__';

    onSave({
      cluster_id: clusterDetails.id,
      spark_version_id: selectedSparkVersionId,
      python_env_id: isPythonRequired ? (isCustom ? 'custom' : (selectedPythonEnvId || undefined)) : undefined,
      custom_python_archive: isPythonRequired && isCustom ? (customPythonArchive.trim() || undefined) : undefined,
      custom_python_path: isPythonRequired && isCustom ? (customPythonPath.trim() || undefined) : undefined,
      metastore_id: selectedMetastoreId,
      yarn_queue: selectedYarnQueue,
      resource_profile: selectedProfile,
      kind: selectedKind,
      packages,
      jars,
      py_files: isPythonRequired ? pyFiles : [],
      spark_conf: sparkConf
    });
  }
</script>

<svelte:window onkeydown={(e) => { if (e.key === 'Escape' && isOpen) onClose(); }} />

{#if isOpen && clusterDetails}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-[100] flex items-center justify-center bg-slate-900/60 backdrop-blur-xs p-4 select-none"
    onclick={(e) => { if (e.target === e.currentTarget) onClose(); }}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div
      class="bg-white dark:bg-slate-900 rounded-2xl shadow-2xl border border-slate-200 dark:border-slate-800 w-full max-w-2xl overflow-hidden flex flex-col max-h-[90vh] select-auto"
      onclick={(e) => e.stopPropagation()}
    >
      <!-- Заголовок -->
      <div class="px-6 py-4 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between bg-slate-50/50 dark:bg-slate-950/50">
        <div class="flex items-center gap-2.5">
          <div class="w-8 h-8 rounded-lg bg-amber-500/10 dark:bg-amber-500/20 text-amber-600 dark:text-amber-400 flex items-center justify-center">
            <Settings class="w-4 h-4" />
          </div>
          <div>
            <h2 class="text-sm font-bold text-slate-900 dark:text-slate-100">Параметры сессии Spark</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400">Кластер: <span class="font-semibold text-slate-700 dark:text-slate-200">{clusterDetails.name}</span></p>
          </div>
        </div>
        <button
          onclick={onClose}
          class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 transition cursor-pointer"
        >
          <X class="w-4 h-4" />
        </button>
      </div>

      <!-- Вкладки -->
      <div class="flex border-b border-slate-200 dark:border-slate-800 px-6 gap-6 text-xs font-semibold">
        <button
          class="py-3 border-b-2 transition cursor-pointer {activeTab === 'general' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200'}"
          onclick={() => (activeTab = 'general')}
        >
          Основные параметры
        </button>
        <button
          class="py-3 border-b-2 transition cursor-pointer {activeTab === 'dependencies' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200'}"
          onclick={() => (activeTab = 'dependencies')}
        >
          Кастомные зависимости (JARs, Packages)
        </button>
        <button
          class="py-3 border-b-2 transition cursor-pointer {activeTab === 'conf' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200'}"
          onclick={() => (activeTab = 'conf')}
        >
          Spark Conf (Свойства)
        </button>
      </div>

      <!-- Контент модального окна -->
      <div class="p-6 overflow-y-auto space-y-4 text-xs flex-1 text-slate-800 dark:text-slate-200">
        {#if activeTab === 'general'}
          <div class="grid grid-cols-2 gap-4">
            <!-- Версия Spark -->
            <div>
              <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1.5 flex items-center gap-1.5">
                <Box class="w-3.5 h-3.5 text-amber-500" />
                Версия ядра Spark
              </label>
              <select
                class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-200 focus:outline-none focus:border-amber-500 font-medium"
                value={selectedSparkVersionId}
                onchange={(e) => handleSparkVersionChange(e.currentTarget.value)}
              >
                {#each clusterDetails.spark_versions as ver}
                  <option value={ver.id}>{ver.name} {ver.is_default ? '(по умолчанию)' : ''}</option>
                {/each}
              </select>
            </div>

            <!-- Язык сессии (унаследован из активной вкладки) -->
            <div>
              <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1.5 flex items-center justify-between">
                <span>Движок / Язык</span>
                <span class="text-[10px] text-slate-400 dark:text-slate-500 font-normal">из активной вкладки</span>
              </label>
              <div class="flex items-center gap-2.5 p-2 rounded-lg border border-slate-200 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-950/70 h-[38px]">
                {#if targetLanguage === 'scalaspark' || selectedKind === 'spark'}
                  <span class="px-1.5 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-rose-100 dark:bg-rose-950 text-rose-700 dark:text-rose-300 font-mono">
                    Scala
                  </span>
                  <div class="flex flex-col">
                    <span class="font-semibold text-slate-800 dark:text-slate-200 text-xs leading-none">Scala Spark</span>
                    <span class="text-[10px] text-slate-400 dark:text-slate-500 leading-tight">SparkSession (JVM)</span>
                  </div>
                {:else if targetLanguage === 'sql'}
                  <span class="px-1.5 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-amber-100 dark:bg-amber-950 text-amber-700 dark:text-amber-300 font-mono">
                    SQL
                  </span>
                  <div class="flex flex-col">
                    <span class="font-semibold text-slate-800 dark:text-slate-200 text-xs leading-none">Spark SQL</span>
                    <span class="text-[10px] text-slate-400 dark:text-slate-500 leading-tight">Catalyst Optimizer (JVM)</span>
                  </div>
                {:else}
                  <span class="px-1.5 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-sky-100 dark:bg-sky-950 text-sky-700 dark:text-sky-300 font-mono">
                    Python
                  </span>
                  <div class="flex flex-col">
                    <span class="font-semibold text-slate-800 dark:text-slate-200 text-xs leading-none">PySpark</span>
                    <span class="text-[10px] text-slate-400 dark:text-slate-500 leading-tight">Python 3 + PySpark API</span>
                  </div>
                {/if}
              </div>
            </div>
          </div>

          <!-- Окружение Python (требуется ТОЛЬКО для PySpark, не требуется для Scala и Spark SQL) -->
          {#if targetLanguage === 'pyspark' && currentSparkVersion}
            <div class="space-y-2">
              <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1.5 flex items-center justify-between">
                <span>Окружение Python (Runtime)</span>
                {#if selectedPythonEnvId === '__custom__'}
                  <span class="text-[10px] text-amber-600 dark:text-amber-400 font-semibold px-2 py-0.5 rounded bg-amber-100/60 dark:bg-amber-950/40">Custom Virtualenv</span>
                {/if}
              </label>
              <select
                class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-200 focus:outline-none focus:border-amber-500 font-medium"
                bind:value={selectedPythonEnvId}
              >
                {#each currentSparkVersion.python_versions as py}
                  <option value={py.id}>{py.name} {py.is_default ? '(Default)' : ''}</option>
                {/each}
                <option value="__custom__">📦 Кастомный venv (HDFS archive)...</option>
              </select>

              {#if selectedPythonEnvId === '__custom__'}
                <div class="p-3.5 bg-amber-50/60 dark:bg-amber-950/20 border border-amber-200/90 dark:border-amber-800/50 rounded-xl space-y-3 mt-2">
                  <div class="flex items-center gap-2 text-xs font-semibold text-amber-900 dark:text-amber-300">
                    <FolderArchive class="w-4 h-4 text-amber-600 dark:text-amber-400 shrink-0" />
                    <span>Параметры изолированного окружения Python (venv / conda-pack)</span>
                  </div>
                  <div>
                    <label class="block text-xs font-medium text-slate-700 dark:text-slate-300 mb-1">
                      HDFS путь к архиву окружения (*.tar.gz)
                    </label>
                    <input
                      type="text"
                      placeholder="hdfs:///apps/python/envs/custom_env.tar.gz#environment"
                      bind:value={customPythonArchive}
                      class="w-full border border-slate-200 dark:border-slate-700 rounded-lg px-2.5 py-1.5 text-xs bg-white dark:bg-slate-900 text-slate-800 dark:text-slate-200 font-mono focus:outline-none focus:border-amber-500"
                    />
                    <p class="text-[11px] text-slate-500 dark:text-slate-400 mt-1">
                      Архив venv, подготовленный с помощью <code class="px-1 py-0.5 rounded bg-slate-200/70 dark:bg-slate-800 font-mono text-[10px]">venv-pack</code> или <code class="px-1 py-0.5 rounded bg-slate-200/70 dark:bg-slate-800 font-mono text-[10px]">conda-pack</code> и загруженный в HDFS.
                    </p>
                  </div>
                  <div>
                    <label class="block text-xs font-medium text-slate-700 dark:text-slate-300 mb-1">
                      Путь к интерпретатору внутри архива
                    </label>
                    <input
                      type="text"
                      placeholder="./environment/bin/python"
                      bind:value={customPythonPath}
                      class="w-full border border-slate-200 dark:border-slate-700 rounded-lg px-2.5 py-1.5 text-xs bg-white dark:bg-slate-900 text-slate-800 dark:text-slate-200 font-mono focus:outline-none focus:border-amber-500"
                    />
                    <p class="text-[11px] text-slate-500 dark:text-slate-400 mt-1">
                      Относительный путь к бинарнику python внутри распакованной папки контейнера (по умолчанию: <code class="px-1 py-0.5 rounded bg-slate-200/70 dark:bg-slate-800 font-mono text-[10px]">./environment/bin/python</code>).
                    </p>
                  </div>
                </div>
              {/if}
            </div>
          {/if}

          <!-- Hive Metastore -->
          <div>
            <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1.5 flex items-center gap-1.5">
              <Database class="w-3.5 h-3.5 text-sky-500" />
              Hive Metastore (Каталог данных)
            </label>
            <select
              class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-200 focus:outline-none focus:border-amber-500 font-medium"
              bind:value={selectedMetastoreId}
            >
              {#each clusterDetails.metastores as meta}
                <option value={meta.id}>{meta.name} {meta.is_default ? '(Основной)' : ''}</option>
              {/each}
            </select>
          </div>

          <div class="grid grid-cols-2 gap-4">
            <!-- Очередь YARN -->
            <div>
              <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1.5 flex items-center gap-1.5">
                <Layers class="w-3.5 h-3.5 text-indigo-500" />
                Очередь YARN (ACL)
              </label>
              <select
                class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-200 focus:outline-none focus:border-amber-500 font-medium font-mono"
                bind:value={selectedYarnQueue}
              >
                {#each clusterDetails.yarn_queues as q}
                  <option value={q}>{q}</option>
                {/each}
              </select>
            </div>

            <!-- Профиль ресурсов -->
            <div>
              <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1.5 flex items-center gap-1.5">
                <Cpu class="w-3.5 h-3.5 text-emerald-500" />
                Профиль ресурсов
              </label>
              <select
                class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-200 focus:outline-none focus:border-amber-500 font-medium"
                bind:value={selectedProfile}
              >
                {#each clusterDetails.resource_profiles as prof}
                  <option value={prof.id}>
                    {prof.name} (Driver: {prof.driver_memory}, {prof.num_executors}x{prof.executor_memory})
                  </option>
                {/each}
              </select>
            </div>
          </div>

        {:else if activeTab === 'dependencies'}
          <div>
            <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1 flex items-center gap-1.5">
              <Package class="w-3.5 h-3.5 text-amber-500" />
              Maven Packages (координаты через запятую)
            </label>
            <p class="text-[11px] text-slate-500 dark:text-slate-400 mb-1.5">Пример: <code>org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.7.2</code></p>
            <input
              type="text"
              class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 focus:outline-none focus:border-amber-500 font-mono text-xs"
              placeholder="groupId:artifactId:version"
              bind:value={packagesText}
            />
          </div>

          <div>
            <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">Кастомные JAR-файлы в HDFS (по одному в строке)</label>
            <p class="text-[11px] text-slate-500 dark:text-slate-400 mb-1.5">Пример: <code>hdfs:///shared/jars/clickhouse-jdbc-0.4.6.jar</code></p>
            <textarea
              rows="3"
              class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 focus:outline-none focus:border-amber-500 font-mono text-xs"
              placeholder="hdfs:///path/to/library.jar"
              bind:value={jarsText}
            ></textarea>
          </div>

          {#if selectedKind === 'pyspark'}
            <div>
              <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">Python Packages / Архивы (.zip, .whl в HDFS)</label>
              <p class="text-[11px] text-slate-500 dark:text-slate-400 mb-1.5">Пример: <code>hdfs:///user/my_user/libs/etl_utils.zip</code></p>
              <textarea
                rows="2"
                class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 focus:outline-none focus:border-amber-500 font-mono text-xs"
                placeholder="hdfs:///path/to/archive.zip"
                bind:value={pyFilesText}
              ></textarea>
            </div>
          {/if}

        {:else if activeTab === 'conf'}
          <div>
            <label class="block font-semibold text-slate-700 dark:text-slate-300 mb-1">Специфичные свойства Spark (key=value, по строкам)</label>
            <p class="text-[11px] text-slate-500 dark:text-slate-400 mb-2">Переопределяет параметры для данной сессии</p>
            <textarea
              rows="6"
              class="w-full border border-slate-200 dark:border-slate-800 rounded-lg p-2 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-100 placeholder-slate-400 dark:placeholder-slate-500 focus:outline-none focus:border-amber-500 font-mono text-xs"
              placeholder="spark.sql.shuffle.partitions=100&#10;spark.speculation=true"
              bind:value={customConfText}
            ></textarea>
          </div>
        {/if}
      </div>

      <!-- Кнопки управления -->
      <div class="px-6 py-3.5 border-t border-slate-100 dark:border-slate-800 flex items-center justify-end gap-2 bg-slate-50/50 dark:bg-slate-950/50">
        <button
          onclick={onClose}
          class="px-4 py-2 rounded-xl text-slate-600 dark:text-slate-400 hover:bg-slate-200/60 dark:hover:bg-slate-800 font-medium transition cursor-pointer"
        >
          Отмена
        </button>
        <button
          onclick={handleSave}
          class="px-5 py-2 rounded-xl bg-amber-500 hover:bg-amber-600 text-white font-semibold transition cursor-pointer flex items-center gap-1.5 shadow-sm"
        >
          <Check class="w-4 h-4" />
          Применить и подключить
        </button>
      </div>
    </div>
  </div>
{/if}
