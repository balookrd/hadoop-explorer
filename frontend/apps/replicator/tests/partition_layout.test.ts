import { describe, it, expect } from 'vitest';
import { agentColor, AGENT_COLORS, buildPartitionLayout, parseAssignment } from '../src/lib/partitionLayout';

describe('Replicator: раскладка партиций Kafka-задачи по агентам', () => {
  it('разбирает ответ оркестратора и отбрасывает неполные записи', () => {
    expect(parseAssignment([
      { agent_id: 'agent-1', topic: 'orders', partitions: [0, 1, 'x', -1] },
      { agent_id: '', topic: 'orders', partitions: [2] },
      { agent_id: 'agent-2', topic: '', partitions: [3] }
    ])).toEqual([{ agentId: 'agent-1', topic: 'orders', partitions: [0, 1] }]);
    expect(parseAssignment(null)).toEqual([]);
  });

  it('строит сетку по топикам и считает партиции агентов', () => {
    const layout = buildPartitionLayout([
      { agentId: 'agent-b', topic: 'orders', partitions: [2, 3] },
      { agentId: 'agent-a', topic: 'orders', partitions: [0, 1] },
      { agentId: 'agent-a', topic: 'payments', partitions: [0] }
    ]);
    expect(layout.topics.map((t) => t.topic)).toEqual(['orders', 'payments']);
    expect(layout.topics[0].cells.map((c) => c.agents)).toEqual([['agent-a'], ['agent-a'], ['agent-b'], ['agent-b']]);
    expect(layout.topics[0].unassigned).toBe(0);
    expect(layout.agents).toEqual([
      { agentId: 'agent-a', partitions: 3 },
      { agentId: 'agent-b', partitions: 2 }
    ]);
  });

  it('показывает незанятые партиции и партиции у двух агентов во время ребаланса', () => {
    const layout = buildPartitionLayout([
      { agentId: 'agent-a', topic: 'orders', partitions: [0, 3] },
      { agentId: 'agent-b', topic: 'orders', partitions: [3] }
    ]);
    const orders = layout.topics[0];
    expect(orders.cells.map((c) => c.agents)).toEqual([['agent-a'], [], [], ['agent-a', 'agent-b']]);
    expect(orders.unassigned).toBe(2);
    expect(orders.duplicated).toBe(1);
  });

  it('цвет агента устойчив и повторяется по кругу', () => {
    const ids = Array.from({ length: AGENT_COLORS.length + 1 }, (_, i) => `agent-${i}`);
    expect(agentColor(ids, 'agent-0')).toBe(AGENT_COLORS[0]);
    expect(agentColor(ids, `agent-${AGENT_COLORS.length}`)).toBe(AGENT_COLORS[0]);
    expect(agentColor(ids, 'missing')).toBe('bg-slate-300');
  });
});
