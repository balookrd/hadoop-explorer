/**
 * Подбор топиков и групп по шаблону в форме Kafka-задачи.
 *
 * Шаблон сравнивается с именем целиком, как Pattern.matches в Java и подписка Kafka по шаблону:
 * раньше `orders` совпадал и с `orders-dlq`, и с `old-orders`, и в задачу попадали чужие топики.
 * Пустой шаблон не выбирает ничего, «*» — всё.
 */

/** Компиляция шаблона для сравнения целиком; ошибка синтаксиса возвращается текстом. */
export function compileFullMatch(pattern: string): RegExp | string {
  try {
    return new RegExp(`^(?:${pattern})$`);
  } catch (e: any) {
    return e?.message || 'некорректное регулярное выражение';
  }
}

/** Ошибка синтаксиса шаблона или null; пустой шаблон и «*» корректны. */
export function patternError(pattern: string): string | null {
  const p = (pattern || '').trim();
  if (p === '' || p === '*') return null;
  const compiled = compileFullMatch(p);
  return typeof compiled === 'string' ? compiled : null;
}

/**
 * Совпадение имени с включающим шаблоном и отсутствие совпадения с исключающим.
 * Некорректный шаблон ничего не выбирает: раньше он молча превращался в поиск подстроки.
 */
export function matchesPattern(name: string, includePattern: string, excludePattern: string): boolean {
  const include = (includePattern || '').trim();
  const exclude = (excludePattern || '').trim();
  if (include === '') return false;
  if (include !== '*') {
    const inRe = compileFullMatch(include);
    if (typeof inRe === 'string' || !inRe.test(name)) return false;
  }
  if (exclude !== '') {
    const exRe = compileFullMatch(exclude);
    if (typeof exRe === 'string') return false;
    if (exRe.test(name)) return false;
  }
  return true;
}
