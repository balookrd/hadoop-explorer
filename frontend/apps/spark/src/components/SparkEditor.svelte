<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import * as monaco from 'monaco-editor';
  import { getStatementAtCursor, sanitizeSql } from '../utils/sqlSplitter';
  import { themeStore } from '../../../../common/stores/theme.svelte';

  let {
    code = $bindable(),
    language = 'pyspark',
    onCodeChange,
    onRun,
    registerTrigger
  }: {
    code: string;
    language: 'pyspark' | 'scalaspark' | 'sql';
    onCodeChange?: (val: string) => void;
    onRun: (codeToRun?: string) => void;
    registerTrigger?: (fn: () => void) => void;
  } = $props();

  let editorContainer: HTMLDivElement;
  let editorInstance: monaco.editor.IStandaloneCodeEditor | null = null;
  let currentDecorations: string[] = [];

  function getMonacoLanguage(lang: string): string {
    if (lang === 'pyspark') return 'python';
    if (lang === 'scalaspark') return 'scala';
    return 'sql';
  }

  onMount(() => {
    // Регистрация сниппетов для PySpark
    monaco.languages.registerCompletionItemProvider('python', {
      provideCompletionItems: (model, position) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn
        };

        const snippets: monaco.languages.CompletionItem[] = [
          {
            label: 'spark.read.table',
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: 'df = spark.read.table("${1:database.table_name}")\ndisplay(df)',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            range,
            detail: 'PySpark: прочитать таблицу и отобразить'
          },
          {
            label: 'display',
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: 'display(${1:df}, limit=1000)',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            range,
            detail: 'Spark Explorer: отобразить DataFrame как интерактивную таблицу'
          },
          {
            label: 'spark.sql',
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: 'df = spark.sql("""\n  SELECT * FROM ${1:table_name}\n  LIMIT 100\n""")\ndisplay(df)',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            range,
            detail: 'PySpark: выполнить Spark SQL запрос'
          }
        ];
        return { suggestions: snippets };
      }
    });

    // Регистрация сниппетов для Scala
    monaco.languages.registerCompletionItemProvider('scala', {
      provideCompletionItems: (model, position) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn
        };

        const snippets: monaco.languages.CompletionItem[] = [
          {
            label: 'spark.read.table',
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: 'val df = spark.read.table("${1:database.table_name}")\ndf.show(50, false)',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            range,
            detail: 'Scala Spark: прочитать таблицу из Metastore'
          },
          {
            label: 'spark.sql',
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: 'val df = spark.sql("""SELECT * FROM ${1:table} LIMIT 100""")\ndf.show()',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            range,
            detail: 'Scala Spark: запустить SQL'
          }
        ];
        return { suggestions: snippets };
      }
    });

    editorInstance = monaco.editor.create(editorContainer, {
      value: code,
      language: getMonacoLanguage(language),
      theme: themeStore.isDark ? 'vs-dark' : 'vs',
      automaticLayout: true,
      fontSize: 13,
      fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, 'Courier New', monospace",
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      lineNumbers: 'on',
      padding: { top: 12, bottom: 12 },
      quickSuggestions: true,
      tabSize: 2
    });

    editorInstance.onDidChangeModelContent(() => {
      if (editorInstance) {
        code = editorInstance.getValue();
        onCodeChange?.(code);
      }
    });

    if (registerTrigger) {
      registerTrigger(triggerExecution);
    }

    // Cmd+Enter для запуска
    editorInstance.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      triggerExecution();
    });
  });

  $effect(() => {
    const isDark = themeStore.isDark;
    if (monaco?.editor) {
      monaco.editor.setTheme(isDark ? 'vs-dark' : 'vs');
    }
  });

  export function triggerExecution() {
    if (!editorInstance) return;
    const model = editorInstance.getModel();
    if (!model) return;

    // 1. Приоритет: явное выделение пользователем
    const selection = editorInstance.getSelection();
    if (selection && !selection.isEmpty()) {
      const selectedText = model.getValueInRange(selection);
      const clean = language === 'sql' ? sanitizeSql(selectedText) : selectedText.trim();
      if (clean) {
        // Визуальная подсветка выполняемого выделенного фрагмента
        currentDecorations = editorInstance.deltaDecorations(currentDecorations, [
          {
            range: selection,
            options: {
              className: 'executing-code-highlight',
              isWholeLine: false
            }
          }
        ]);
        setTimeout(() => {
          if (editorInstance) {
            currentDecorations = editorInstance.deltaDecorations(currentDecorations, []);
          }
        }, 800);

        onRun(clean);
        return;
      }
    }

    // 2. Если ничего не выделено: для SQL определяем запрос под курсором
    if (language === 'sql') {
      const position = editorInstance.getPosition();
      const fullText = model.getValue();
      const cursorOffset = position ? model.getOffsetAt(position) : 0;
      const targetStatement = getStatementAtCursor(fullText, cursorOffset);
      if (targetStatement) {
        const clean = sanitizeSql(targetStatement.text);
        if (clean) {
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
                className: 'executing-code-highlight',
                isWholeLine: false
              }
            }
          ]);
          setTimeout(() => {
            if (editorInstance) {
              currentDecorations = editorInstance.deltaDecorations(currentDecorations, []);
            }
          }, 800);

          onRun(clean);
          return;
        }
      }
    }

    // 3. Fallback: весь текст редактора
    const fullText = model.getValue().trim();
    if (fullText) {
      onRun(language === 'sql' ? sanitizeSql(fullText) : fullText);
    }
  }

  $effect(() => {
    if (editorInstance) {
      const currentModel = editorInstance.getModel();
      if (currentModel) {
        const targetLang = getMonacoLanguage(language);
        monaco.editor.setModelLanguage(currentModel, targetLang);
      }
    }
  });

  $effect(() => {
    if (editorInstance && editorInstance.getValue() !== code) {
      editorInstance.setValue(code);
    }
  });

  onDestroy(() => {
    if (editorInstance) {
      editorInstance.dispose();
    }
  });
</script>

<div class="h-full w-full relative bg-white dark:bg-slate-950" bind:this={editorContainer}></div>

<style>
  :global(.executing-code-highlight) {
    background-color: rgba(245, 158, 11, 0.25) !important;
    border-radius: 3px;
  }
</style>
