<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import * as monaco from 'monaco-editor';
  import { getStatementAtCursor, sanitizeSql } from '../utils/sqlSplitter';

  let {
    value = $bindable(),
    onExecute,
    registerTrigger
  }: {
    value: string;
    onExecute: (queryToRun: string) => void;
    registerTrigger?: (fn: () => void) => void;
  } = $props();

  let editorContainer: HTMLDivElement;
  let editorInstance: monaco.editor.IStandaloneCodeEditor | null = null;
  let currentDecorations: string[] = [];

  onMount(() => {
    // Регистрация кастомных ключевых слов Trino и Hive в синтаксисе SQL
    monaco.languages.registerCompletionItemProvider('sql', {
      provideCompletionItems: (model, position) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn
        };

        const trinoHiveKeywords = [
          'APPROX_DISTINCT', 'ARBITRARY', 'ARRAY_AGG', 'CARDINALITY', 'CAST',
          'COALESCE', 'CONCAT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
          'DATE_ADD', 'DATE_DIFF', 'DATE_FORMAT', 'DATE_PARSE', 'DATE_TRUNC',
          'FLATTEN', 'FROM_ISO8601_TIMESTAMP', 'FROM_UNIXTIME', 'JSON_ARRAY_CONTAINS',
          'JSON_EXTRACT', 'JSON_EXTRACT_SCALAR', 'JSON_PARSE', 'JSON_SIZE',
          'LEAD', 'LAG', 'NOW', 'PARTITION', 'REGEXP_EXTRACT', 'REGEXP_LIKE',
          'ROW_NUMBER', 'SPLIT', 'TRY', 'TRY_CAST', 'UNNEST', 'ZIP'
        ];

        const suggestions: monaco.languages.CompletionItem[] = trinoHiveKeywords.map((kw) => ({
          label: kw,
          kind: monaco.languages.CompletionItemKind.Function,
          insertText: kw,
          range: range,
          detail: 'Trino/Hive Function'
        }));

        return { suggestions };
      }
    });

    editorInstance = monaco.editor.create(editorContainer, {
      value: value,
      language: 'sql',
      theme: 'vs',
      automaticLayout: true,
      fontSize: 13,
      fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, 'Courier New', monospace",
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      lineNumbers: 'on',
      renderLineHighlight: 'all',
      padding: { top: 12, bottom: 12 },
      quickSuggestions: true,
      tabSize: 2
    });

    // Слушатель изменений содержимого
    editorInstance.onDidChangeModelContent(() => {
      if (editorInstance) {
        value = editorInstance.getValue();
        // Очищаем старые маркеры при редактировании
        const model = editorInstance.getModel();
        if (model) {
          monaco.editor.setModelMarkers(model, 'ai-sql-linter', []);
        }
      }
    });

    // Шорткат Cmd+Enter / Ctrl+Enter для выполнения
    editorInstance.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      triggerExecution();
    });

    if (registerTrigger) {
      registerTrigger(triggerExecution);
    }
  });

  export function setAiMarkers(issues: any[]) {
    if (!editorInstance) return;
    const model = editorInstance.getModel();
    if (!model) return;

    const markers: monaco.editor.IMarkerData[] = issues.map((issue) => {
      let severity = monaco.MarkerSeverity.Warning;
      if (issue.severity === 'error') severity = monaco.MarkerSeverity.Error;
      else if (issue.severity === 'info') severity = monaco.MarkerSeverity.Info;

      const lineContent = model.getLineContent(Math.min(issue.line, model.getLineCount())) || '';
      const maxCol = Math.max(lineContent.length + 1, 2);

      return {
        severity,
        startLineNumber: Math.min(issue.line, model.getLineCount()),
        startColumn: Math.min(issue.column || 1, maxCol),
        endLineNumber: Math.min(issue.end_line || issue.line, model.getLineCount()),
        endColumn: Math.min(issue.end_column || maxCol, maxCol),
        message: `[AI ${issue.category ? issue.category.toUpperCase() : 'LINTER'}] ${issue.message}${issue.suggestion ? '\n💡 Рекомендация: ' + issue.suggestion : ''}`,
        source: 'AI SQL Assistant'
      };
    });

    monaco.editor.setModelMarkers(model, 'ai-sql-linter', markers);
  }

  export function clearAiMarkers() {
    if (!editorInstance) return;
    const model = editorInstance.getModel();
    if (model) {
      monaco.editor.setModelMarkers(model, 'ai-sql-linter', []);
    }
  }

  export function revealLine(lineNumber: number) {
    if (!editorInstance) return;
    editorInstance.revealLineInCenter(lineNumber);
    editorInstance.setPosition({ lineNumber, column: 1 });
    editorInstance.focus();
  }

  export function triggerExecution() {

    if (!editorInstance) return;
    const model = editorInstance.getModel();
    if (!model) return;

    // 1. Приоритет: явное выделение пользователем
    const selection = editorInstance.getSelection();
    if (selection && !selection.isEmpty()) {
      const selectedText = model.getValueInRange(selection);
      const clean = sanitizeSql(selectedText);
      if (clean) {
        onExecute(clean);
        return;
      }
    }

    // 2. Определение запроса под курсором (Statement at Cursor)
    const position = editorInstance.getPosition();
    const fullText = model.getValue();
    const cursorOffset = position ? model.getOffsetAt(position) : 0;

    const targetStatement = getStatementAtCursor(fullText, cursorOffset);
    if (targetStatement) {
      const clean = sanitizeSql(targetStatement.text);
      if (clean) {
        // Визуальная подсветка выполняемого запроса в Monaco
        const startPos = model.getPositionAt(targetStatement.startOffset);
        const endPos = model.getPositionAt(targetStatement.endOffset);
        const highlightRange = new monaco.Range(
          startPos.lineNumber,
          startPos.column,
          endPos.lineNumber,
          endPos.column
        );

        currentDecorations = editorInstance.deltaDecorations(currentDecorations, [
          {
            range: highlightRange,
            options: {
              className: 'executing-query-highlight',
              isWholeLine: false
            }
          }
        ]);

        setTimeout(() => {
          if (editorInstance) {
            currentDecorations = editorInstance.deltaDecorations(currentDecorations, []);
          }
        }, 700);

        onExecute(clean);
        return;
      }
    }

    // 3. Fallback: весь текст редактора
    const fallback = sanitizeSql(fullText);
    if (fallback) {
      onExecute(fallback);
    }
  }

  // Обновление значения снаружи (например при клике на историю или сниппет)
  $effect(() => {
    if (editorInstance && value !== editorInstance.getValue()) {
      editorInstance.setValue(value);
    }
  });

  onDestroy(() => {
    if (editorInstance) {
      editorInstance.dispose();
    }
  });
</script>

<div class="h-full w-full relative overflow-hidden flex flex-col bg-white">
  <div bind:this={editorContainer} class="flex-1 w-full h-full"></div>
</div>

<style>
  :global(.executing-query-highlight) {
    background-color: rgba(2, 132, 199, 0.12) !important;
    border-left: 3px solid #0284c7 !important;
    border-radius: 2px;
  }
</style>

