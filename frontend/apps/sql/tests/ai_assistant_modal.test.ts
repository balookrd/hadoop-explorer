import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import AIAssistantModal from '../src/components/AIAssistantModal.svelte';
import { api } from '../src/api/client';

describe('SQL AIAssistantModal Component', () => {
  it('does not render when isOpen is false', () => {
    render(AIAssistantModal, {
      props: {
        isOpen: false,
        sqlQuery: 'SELECT 1'
      }
    });

    expect(screen.queryByText(/ИИ Ассистент|Text-to-SQL/i)).not.toBeInTheDocument();
  });

  it('renders modal with generate tab and templates when open', async () => {
    vi.spyOn(api, 'getAiStatus').mockResolvedValue({
      enabled: true,
      provider: 'ollama',
      model: 'qwen2.5-coder',
      supports_streaming: false
    });

    const { container } = render(AIAssistantModal, {
      props: {
        isOpen: true,
        initialTab: 'generate',
        sqlQuery: '',
        clusterId: 'trino-1'
      }
    });

    expect(screen.getByText('ИИ SQL Ассистент')).toBeInTheDocument();
    expect(screen.getByText(/Топ-10 клиентов по заказам/i)).toBeInTheDocument();
    expect(screen.getByText(/Заказы по статусам/i)).toBeInTheDocument();

    vi.spyOn(api, 'generateSql').mockResolvedValue({
      generated_sql: 'SELECT * FROM customer WHERE balance > 0 LIMIT 20;',
      explanation: 'Запрос выбирает топ клиентов',
      confidence: 0.95
    } as any);

    const templateBtn = screen.getByText(/Топ клиентов по балансу счета/i);
    await fireEvent.click(templateBtn);

    const textarea = container.querySelector('#generate-prompt-input') as HTMLTextAreaElement;
    expect(textarea).toBeInTheDocument();
    expect(textarea.value).toContain('топ 20 клиентов');
  });
});
