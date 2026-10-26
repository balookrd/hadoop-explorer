import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import TopologyView from '../src/components/TopologyView.svelte';
import type { AgentInfo } from '../src/types';

// Реестр в сотни агентов: показ порциями, фильтр по кластеру и поиск
function agents(): AgentInfo[] {
  const list: AgentInfo[] = [];
  for (let i = 0; i < 130; i++) {
    const cluster = i < 100 ? 'prod-dc1' : i < 125 ? 'prod-dc2' : 'prod-dc3';
    list.push({
      agent_id: `agent-${String(i).padStart(3, '0')}`,
      cluster_id: cluster,
      mode: 'all',
      status: i === 129 ? 'offline' : 'online',
      grpc_address: `10.0.0.${i}:50051`,
    } as AgentInfo);
  }
  return list;
}

function renderView() {
  return render(TopologyView, {
    props: {
      topology: null,
      topologyLoading: false,
      registeredAgents: agents(),
      agentsLoading: false,
      streamingLease: null,
      streamingLeases: {},
      isAdmin: false,
      onRefreshTopology: () => {},
      onRefreshAgents: () => {},
      onSaveGlobalLimit: async () => {},
      onSaveDcLimit: async () => {},
      onSaveHdfsLimit: async () => {},
    },
  });
}

describe('Реестр агентов в масштабе', () => {
  it('показывает агентов порциями и сводку по кластерам', async () => {
    renderView();
    expect(screen.getByText('Показано 60 из 130')).toBeTruthy();
    expect(screen.queryByText('agent-100')).toBeNull();

    await fireEvent.click(screen.getByText('Показать еще 60'));
    expect(screen.getByText('Показано 120 из 130')).toBeTruthy();
  });

  it('фильтрует по кластеру и ищет по ID', async () => {
    renderView();
    await fireEvent.click(screen.getByTitle('Онлайн 4 из 5'));
    expect(screen.getByText('agent-125')).toBeTruthy();
    expect(screen.queryByText('agent-000')).toBeNull();

    await fireEvent.click(screen.getByText(/Все кластеры/));
    await fireEvent.input(screen.getByPlaceholderText('Поиск по ID или адресу агента'), { target: { value: 'agent-042' } });
    expect(screen.getByText('agent-042')).toBeTruthy();
    expect(screen.queryByText('agent-041')).toBeNull();
  });
});
