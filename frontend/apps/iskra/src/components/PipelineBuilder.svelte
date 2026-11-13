<script lang="ts">
  import { Play, Plus, Trash2, ArrowRight, CheckCircle2, Clock, AlertCircle } from 'lucide-svelte';

  interface Node {
    id: string;
    name: string;
    type: 'pyspark' | 'spark_sql' | 'spark_submit' | 'wait';
    code: string;
    status?: 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED';
  }

  interface Edge {
    from: string;
    to: string;
  }

  let nodes: Node[] = $state([
    { id: '1', name: 'Extract Sales Data', type: 'spark_sql', code: 'SELECT * FROM raw.sales WHERE date = current_date()', status: 'SUCCESS' },
    { id: '2', name: 'Transform & Aggregate', type: 'pyspark', code: 'df.groupBy("region").agg(sum("amount"))', status: 'RUNNING' },
    { id: '3', name: 'Write to Metastore', type: 'pyspark', code: 'df.write.saveAsTable("analytics.daily_sales")', status: 'PENDING' },
  ]);

  let edges: Edge[] = $state([
    { from: '1', to: '2' },
    { from: '2', to: '3' },
  ]);

  let selectedNodeId: string | null = $state('2');
  let isRunning = $state(false);

  const selectedNode = $derived(nodes.find(n => n.id === selectedNodeId) || null);

  function addNode(type: 'pyspark' | 'spark_sql') {
    const newId = String(Date.now()).slice(-4);
    const newNode: Node = {
      id: newId,
      name: `New ${type === 'pyspark' ? 'PySpark' : 'SQL'} Step`,
      type,
      code: '',
      status: 'PENDING',
    };
    nodes = [...nodes, newNode];
    selectedNodeId = newId;
  }

  function removeNode(id: string) {
    nodes = nodes.filter(n => n.id !== id);
    edges = edges.filter(e => e.from !== id && e.to !== id);
    if (selectedNodeId === id) selectedNodeId = null;
  }

  async function runPipeline() {
    isRunning = true;
    for (const node of nodes) {
      node.status = 'RUNNING';
      await new Promise(r => setTimeout(r, 600));
      node.status = 'SUCCESS';
    }
    isRunning = false;
  }
</script>

<div class="flex flex-col h-full bg-slate-50 dark:bg-slate-900 border-t border-slate-200 dark:border-slate-800">
  <!-- Top Bar -->
  <div class="flex items-center justify-between px-4 py-2.5 bg-white dark:bg-slate-950 border-b border-slate-200 dark:border-slate-800">
    <div class="flex items-center gap-2">
      <h2 class="text-sm font-bold text-slate-800 dark:text-slate-100">Визуальный DAG Пайплайн</h2>
      <span class="text-xs px-2 py-0.5 rounded-full bg-indigo-50 dark:bg-indigo-950 text-indigo-600 dark:text-indigo-400 font-medium border border-indigo-200 dark:border-indigo-800">
        {nodes.length} узлов / {edges.length} связей
      </span>
    </div>

    <div class="flex items-center gap-2">
      <button
        onclick={() => addNode('spark_sql')}
        class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-xs font-semibold text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-700 cursor-pointer transition"
      >
        <Plus class="w-3.5 h-3.5 text-sky-500" />
        <span>+ SQL Узел</span>
      </button>

      <button
        onclick={() => addNode('pyspark')}
        class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-xs font-semibold text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-700 cursor-pointer transition"
      >
        <Plus class="w-3.5 h-3.5 text-amber-500" />
        <span>+ PySpark Узел</span>
      </button>

      <button
        onclick={runPipeline}
        disabled={isRunning || nodes.length === 0}
        class="flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-700 disabled:opacity-50 text-white text-xs font-bold shadow-xs cursor-pointer transition"
      >
        <Play class="w-3.5 h-3.5 fill-current" />
        <span>{isRunning ? 'Выполнение...' : 'Запустить пайплайн'}</span>
      </button>
    </div>
  </div>

  <!-- Content Workspace -->
  <div class="flex-1 flex overflow-hidden">
    <!-- DAG Flow Canvas -->
    <div class="flex-1 p-6 overflow-y-auto">
      <div class="flex flex-col items-center gap-4 max-w-xl mx-auto">
        {#each nodes as node, idx}
          <!-- Node Card -->
          <div
            role="button"
            tabindex="0"
            onclick={() => selectedNodeId = node.id}
            onkeydown={(e) => { if (e.key === 'Enter') selectedNodeId = node.id; }}
            class="w-full p-4 rounded-xl border-2 transition cursor-pointer text-left {
              selectedNodeId === node.id
                ? 'border-indigo-500 bg-white dark:bg-slate-800 shadow-md ring-2 ring-indigo-500/20'
                : 'border-slate-200 dark:border-slate-700 bg-white/80 dark:bg-slate-800/80 hover:border-slate-300 dark:hover:border-slate-600'
            }"
          >
            <div class="flex items-center justify-between mb-2">
              <div class="flex items-center gap-2">
                <span class="text-[10px] font-bold px-2 py-0.5 rounded {
                  node.type === 'spark_sql' ? 'bg-sky-100 dark:bg-sky-950 text-sky-700 dark:text-sky-400' : 'bg-amber-100 dark:bg-amber-950 text-amber-700 dark:text-amber-400'
                }">
                  {node.type.toUpperCase()}
                </span>
                <span class="text-xs font-bold text-slate-800 dark:text-slate-100">{node.name}</span>
              </div>

              <div class="flex items-center gap-2">
                {#if node.status === 'SUCCESS'}
                  <CheckCircle2 class="w-4 h-4 text-emerald-500" />
                {:else if node.status === 'RUNNING'}
                  <Clock class="w-4 h-4 text-amber-500 animate-spin" />
                {:else if node.status === 'FAILED'}
                  <AlertCircle class="w-4 h-4 text-red-500" />
                {:else}
                  <Clock class="w-4 h-4 text-slate-400" />
                {/if}

                <button
                  onclick={(e) => { e.stopPropagation(); removeNode(node.id); }}
                  class="p-1 text-slate-400 hover:text-red-600 transition cursor-pointer"
                  title="Удалить узел"
                >
                  <Trash2 class="w-3.5 h-3.5" />
                </button>
              </div>
            </div>

            <p class="text-[11px] font-mono text-slate-500 dark:text-slate-400 truncate bg-slate-50 dark:bg-slate-900 p-1.5 rounded border border-slate-100 dark:border-slate-800">
              {node.code || '// Код не задан'}
            </p>
          </div>

          {#if idx < nodes.length - 1}
            <div class="flex items-center justify-center text-slate-400 dark:text-slate-500">
              <ArrowRight class="w-4 h-4 rotate-90" />
            </div>
          {/if}
        {/each}
      </div>
    </div>

    <!-- Node Inspector Sidebar -->
    {#if selectedNode}
      <div class="w-80 border-l border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 p-4 flex flex-col gap-3">
        <h3 class="text-xs font-bold text-slate-700 dark:text-slate-200 uppercase tracking-wider">Параметры узла</h3>

        <div>
          <label for="p-name" class="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1">Название шага</label>
          <input
            id="p-name"
            type="text"
            bind:value={selectedNode.name}
            class="w-full px-2.5 py-1.5 text-xs rounded-lg border border-slate-300 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 text-slate-800 dark:text-slate-100 outline-none focus:border-indigo-500"
          />
        </div>

        <div>
          <label for="p-code" class="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1">Код исполнения ({selectedNode.type})</label>
          <textarea
            id="p-code"
            rows="6"
            bind:value={selectedNode.code}
            class="w-full p-2 text-xs font-mono rounded-lg border border-slate-300 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 text-slate-800 dark:text-slate-100 outline-none focus:border-indigo-500 resize-none"
          ></textarea>
        </div>
      </div>
    {/if}
  </div>
</div>
