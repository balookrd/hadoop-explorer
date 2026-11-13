/**
 * Раскладка партиций Kafka-задачи по агентам для карточки задачи.
 *
 * Оркестратор отдает по последним heartbeat список «агент, топик, партиции». Отчеты агентов
 * приходят в разное время, поэтому во время ребаланса партиция может числиться за двумя
 * агентами или ни за одним: такие ячейки выделяются, а не прячутся.
 */

export interface AgentPartitions {
  agentId: string;
  topic: string;
  partitions: number[];
}

export interface PartitionCell {
  partition: number;
  /** Агенты, которые читают партицию; пусто — партиция не назначена никому */
  agents: string[];
}

export interface TopicLayout {
  topic: string;
  cells: PartitionCell[];
  unassigned: number;
  duplicated: number;
}

export interface PartitionLayout {
  topics: TopicLayout[];
  /** Агенты по алфавиту с числом партиций всех топиков задачи */
  agents: { agentId: string; partitions: number }[];
}

/** Разбор поля partition_assignment из ответа оркестратора. */
export function parseAssignment(raw: unknown): AgentPartitions[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((item: any) => ({
      agentId: String(item?.agent_id ?? item?.agentId ?? ''),
      topic: String(item?.topic ?? ''),
      partitions: Array.isArray(item?.partitions)
        ? item.partitions.map(Number).filter((p: number) => Number.isInteger(p) && p >= 0)
        : []
    }))
    .filter((a) => a.agentId !== '' && a.topic !== '');
}

/**
 * Сетка партиций по топикам. Число партиций топика оркестратору неизвестно, поэтому сетка
 * идет до наибольшего назначенного номера: пропуски внутри нее — незанятые партиции.
 */
export function buildPartitionLayout(assignment: AgentPartitions[]): PartitionLayout {
  const byTopic = new Map<string, Map<number, string[]>>();
  const agentTotals = new Map<string, number>();
  for (const { agentId, topic, partitions } of assignment) {
    const cells = byTopic.get(topic) ?? new Map<number, string[]>();
    byTopic.set(topic, cells);
    for (const p of partitions) {
      const agents = cells.get(p) ?? [];
      if (!agents.includes(agentId)) agents.push(agentId);
      cells.set(p, agents);
    }
    agentTotals.set(agentId, (agentTotals.get(agentId) ?? 0) + partitions.length);
  }

  const topics: TopicLayout[] = [...byTopic.keys()].sort().map((topic) => {
    const assigned = byTopic.get(topic)!;
    const size = assigned.size > 0 ? Math.max(...assigned.keys()) + 1 : 0;
    const cells: PartitionCell[] = [];
    for (let p = 0; p < size; p++) {
      cells.push({ partition: p, agents: [...(assigned.get(p) ?? [])].sort() });
    }
    return {
      topic,
      cells,
      unassigned: cells.filter((c) => c.agents.length === 0).length,
      duplicated: cells.filter((c) => c.agents.length > 1).length
    };
  });

  const agents = [...agentTotals.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([agentId, partitions]) => ({ agentId, partitions }));
  return { topics, agents };
}

/** Цвета агентов: классы перечислены целиком, иначе Tailwind их не соберет. */
export const AGENT_COLORS = [
  'bg-sky-500',
  'bg-emerald-500',
  'bg-violet-500',
  'bg-amber-500',
  'bg-rose-500',
  'bg-cyan-500',
  'bg-lime-500',
  'bg-fuchsia-500',
  'bg-orange-500',
  'bg-teal-500',
  'bg-indigo-500',
  'bg-pink-500'
];

/** Цвет агента по его месту в отсортированном списке агентов задачи. */
export function agentColor(agentIds: string[], agentId: string): string {
  const index = agentIds.indexOf(agentId);
  return index < 0 ? 'bg-slate-300' : AGENT_COLORS[index % AGENT_COLORS.length];
}
