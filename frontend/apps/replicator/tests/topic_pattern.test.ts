import { describe, it, expect } from 'vitest';
import { matchesPattern, patternError } from '../src/lib/topicPattern';

describe('Replicator: подбор топиков Kafka по шаблону', () => {
  it('шаблон сравнивается с именем целиком', () => {
    expect(matchesPattern('orders', 'orders', '')).toBe(true);
    expect(matchesPattern('orders-dlq', 'orders', '')).toBe(false);
    expect(matchesPattern('old-orders', 'orders', '')).toBe(false);
    expect(matchesPattern('orders-eu', 'orders-.*', '')).toBe(true);
    expect(matchesPattern('payments', 'orders|payments', '')).toBe(true);
  });

  it('исключающий шаблон тоже целиком', () => {
    expect(matchesPattern('orders-dlq', 'orders-.*', '.*-dlq')).toBe(false);
    expect(matchesPattern('orders-dlq-archive', 'orders-.*', '.*-dlq')).toBe(true);
    expect(matchesPattern('audit', '*', 'audit')).toBe(false);
    expect(matchesPattern('audit-raw', '*', 'audit')).toBe(true);
  });

  it('пустой шаблон ничего не выбирает, «*» выбирает все', () => {
    expect(matchesPattern('orders', '', '')).toBe(false);
    expect(matchesPattern('orders', '  ', '')).toBe(false);
    expect(matchesPattern('orders', '*', '')).toBe(true);
  });

  it('некорректный шаблон ничего не выбирает и сообщает ошибку', () => {
    expect(matchesPattern('orders(', 'orders(', '')).toBe(false);
    expect(matchesPattern('orders', '*', '[')).toBe(false);
    expect(patternError('orders(')).not.toBeNull();
    expect(patternError('orders-.*')).toBeNull();
    expect(patternError('*')).toBeNull();
    expect(patternError('')).toBeNull();
  });
});
