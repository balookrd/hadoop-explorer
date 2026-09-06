export interface SqlStatement {
  text: string;
  startOffset: number;
  endOffset: number;
}

/**
 * Разбивает SQL скрипт на отдельные запросы (statements),
 * корректно обрабатывая строковые литералы ('...', "..."), бэктики (`...`),
 * однострочные комментарии (-- ...) и многострочные комментарии (/* ... * /).
 */
export function splitSqlStatements(sql: string): SqlStatement[] {
  const statements: SqlStatement[] = [];
  let stmtStart = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inLineComment = false;
  let inBlockComment = false;

  const len = sql.length;

  for (let i = 0; i < len; i++) {
    const char = sql[i];
    const nextChar = i + 1 < len ? sql[i + 1] : '';

    // Внутри однострочного комментария
    if (inLineComment) {
      if (char === '\n') {
        inLineComment = false;
      }
      continue;
    }

    // Внутри многострочного комментария
    if (inBlockComment) {
      if (char === '*' && nextChar === '/') {
        inBlockComment = false;
        i++; // пропускаем '/'
      }
      continue;
    }

    // Внутри одинарных кавычек '...'
    if (inSingleQuote) {
      if (char === "'" && nextChar === "'") {
        i++; // экранированная одинарная кавычка ''
      } else if (char === '\\') {
        i++; // экранирование следующего символа
      } else if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }

    // Внутри двойных кавычек "..."
    if (inDoubleQuote) {
      if (char === '"' && nextChar === '"') {
        i++; // экранированная двойная кавычка ""
      } else if (char === '\\') {
        i++;
      } else if (char === '"') {
        inDoubleQuote = false;
      }
      continue;
    }

    // Внутри бэктиков `...`
    if (inBacktick) {
      if (char === '`' && nextChar === '`') {
        i++;
      } else if (char === '`') {
        inBacktick = false;
      }
      continue;
    }

    // Проверка начала комментариев
    if (char === '-' && nextChar === '-') {
      inLineComment = true;
      i++;
      continue;
    }

    if (char === '/' && nextChar === '*') {
      inBlockComment = true;
      i++;
      continue;
    }

    // Проверка начала кавычек
    if (char === "'") {
      inSingleQuote = true;
      continue;
    }

    if (char === '"') {
      inDoubleQuote = true;
      continue;
    }

    if (char === '`') {
      inBacktick = true;
      continue;
    }

    // Точка с запятой вне строк и комментариев — граница выражения
    if (char === ';') {
      const stmtText = sql.slice(stmtStart, i + 1);
      if (stmtText.trim().replace(/^;+|;+$/g, '').trim().length > 0) {
        statements.push({
          text: stmtText,
          startOffset: stmtStart,
          endOffset: i + 1
        });
      }
      stmtStart = i + 1;
    }
  }

  // Завершающий блок без финальной точки с запятой
  if (stmtStart < len) {
    const trailingText = sql.slice(stmtStart);
    if (trailingText.trim().length > 0) {
      statements.push({
        text: trailingText,
        startOffset: stmtStart,
        endOffset: len
      });
    }
  }

  return statements;
}

/**
 * Находит выражение, на котором находится курсор каретки,
 * или ближайшее к нему выражение.
 */
export function getStatementAtCursor(sql: string, cursorOffset: number): SqlStatement | null {
  const statements = splitSqlStatements(sql);
  if (statements.length === 0) return null;

  // 1. Проверяем попадание курсора внутрь диапазона
  for (const s of statements) {
    if (cursorOffset >= s.startOffset && cursorOffset <= s.endOffset) {
      return s;
    }
  }

  // 2. Если курсор между запросами или за пределами, ищем ближайший
  let closest: SqlStatement = statements[0];
  let minDiff = Infinity;

  for (const s of statements) {
    let diff = 0;
    if (cursorOffset < s.startOffset) {
      diff = s.startOffset - cursorOffset;
    } else if (cursorOffset > s.endOffset) {
      diff = cursorOffset - s.endOffset;
    }

    if (diff < minDiff) {
      minDiff = diff;
      closest = s;
    }
  }

  return closest;
}

/**
 * Очищает SQL от концевых точек с запятой и крайних пробелов
 */
export function sanitizeSql(sql: string): string {
  return sql.trim().replace(/;+$/, '').trim();
}
