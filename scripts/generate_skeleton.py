#!/usr/bin/env python3
"""
AST-based Code Skeletonizer for Hadoop Explorer Platform.

Извлекает компактный синтаксический каркас (скелет) кодовой базы:
- Классы, их базовые классы и поля моделей (Pydantic / Dataclasses).
- Сигнатуры функций и методов (параметры, аннотации типов, возвращаемые значения).
- Docstrings (опционально).
- Заменяет тела функций на '...' (Ellipsis / stub).
- Для TypeScript: извлекает интерфейсы, типы, enum и сигнатуры экспортируемых функций/классов.

Позволяет сократить потребление токенов LLM на 80-90% при передаче контекста архитектуры.
"""

from __future__ import annotations

import argparse
import ast
import os
import re
import sys
from pathlib import Path
from typing import Iterator, Optional, Tuple


class PythonSkeletonizer(ast.NodeTransformer):
    def __init__(self, keep_docstrings: bool = True):
        self.keep_docstrings = keep_docstrings

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        return self._transform_func(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AsyncFunctionDef:
        return self._transform_func(node)

    def _transform_func(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.FunctionDef | ast.AsyncFunctionDef:
        new_body: list[ast.stmt] = []
        if self.keep_docstrings:
            doc = ast.get_docstring(node)
            if doc:
                # Сохраняем краткий docstring (первый параграф или до 3 строк)
                short_doc = doc.strip().split("\n\n")[0].strip()
                if len(short_doc.split("\n")) > 4:
                    short_doc = "\n".join(short_doc.split("\n")[:3]) + "..."
                new_body.append(ast.Expr(value=ast.Constant(value=short_doc)))

        new_body.append(ast.Expr(value=ast.Constant(value=Ellipsis)))
        node.body = new_body
        return node

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.ClassDef:
        # Рекурсивно трансформируем методы и атрибуты внутри класса
        self.generic_visit(node)
        if self.keep_docstrings:
            doc = ast.get_docstring(node)
            if doc:
                short_doc = doc.strip().split("\n\n")[0].strip()
                if not any(
                    isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str)
                    for stmt in node.body
                ):
                    node.body.insert(0, ast.Expr(value=ast.Constant(value=short_doc)))
        if not node.body:
            node.body = [ast.Pass()]
        return node


def skeletonize_python(source_code: str, keep_docstrings: bool = True) -> str:
    """Генерирует скелет Python-файла с сохранением сигнатур и типов."""
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        return "# [SyntaxError: Не удалось распарсить Python файл]"

    transformer = PythonSkeletonizer(keep_docstrings=keep_docstrings)
    transformed_tree = transformer.visit(tree)
    ast.fix_missing_locations(transformed_tree)
    return ast.unparse(transformed_tree)


def skeletonize_typescript(source_code: str) -> str:
    """
    Извлекает интерфейсы, типы, enums и сигнатуры экспортируемых функций/классов из TypeScript.
    """
    lines = source_code.splitlines()
    result_lines: list[str] = []
    in_block = 0
    capture_block = False
    current_block: list[str] = []

    for line in lines:
        stripped = line.strip()

        # Пропускаем однострочные комментарии и пустые строки вне блоков
        if not in_block and (stripped.startswith("//") or not stripped):
            continue

        # Начало определений типов, интерфейсов или enums
        if not in_block and re.match(r"^(export\s+)?(interface|type|enum)\s+", stripped):
            capture_block = True
            in_block += stripped.count("{") - stripped.count("}")
            current_block.append(line)
            if in_block <= 0:
                result_lines.append(line)
                capture_block = False
                in_block = 0
                current_block = []
            continue

        if capture_block:
            in_block += stripped.count("{") - stripped.count("}")
            current_block.append(line)
            if in_block <= 0:
                result_lines.extend(current_block)
                capture_block = False
                in_block = 0
                current_block = []
            continue

        # Экспортируемые функции и константы с типами
        if re.match(r"^export\s+(async\s+)?function\s+", stripped):
            # Извлекаем сигнатуру до открывающей фигурной скобки
            sig = line.split("{")[0].strip()
            result_lines.append(f"{sig};")
            continue

        if re.match(r"^export\s+const\s+[a-zA-Z0-9_]+\s*:\s*", stripped):
            sig = line.split("=")[0].strip()
            result_lines.append(f"{sig} = ...;")
            continue

        # Экспортируемые классы (декларация)
        if re.match(r"^export\s+(abstract\s+)?class\s+", stripped):
            sig = line.split("{")[0].strip()
            result_lines.append(f"{sig} {{ /* class definition */ }}")
            continue

    return "\n".join(result_lines)


def should_ignore_file(path: Path) -> bool:
    """Проверяет, нужно ли игнорировать файл при генерации скелета."""
    parts = set(path.parts)
    ignored_dirs = {
        ".git",
        ".venv",
        "node_modules",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        "dist",
        "build",
        "data",
        "coverage",
        ".svelte-kit",
    }
    if parts & ignored_dirs:
        return True

    # Игнорируем тестовые файлы и бинарники для общего скелета
    name = path.name
    if name.startswith("test_") or name.endswith("_test.py") or name.endswith(".spec.ts"):
        return True
    if name.endswith((".db", ".db-wal", ".db-shm", ".lock", ".log", ".png", ".svg", ".jpg")):
        return True

    return False


def collect_files(target_path: Path) -> Iterator[Path]:
    """Рекурсивно находит исходные файлы Python и TypeScript."""
    if target_path.is_file():
        if not should_ignore_file(target_path):
            yield target_path
        return

    for root, dirs, files in os.walk(target_path):
        # Оптимизация обхода: исключаем ненужные директории на месте
        dirs[:] = [d for d in dirs if d not in {".git", ".venv", "node_modules", "data", "dist", "build", "__pycache__"}]
        for f in files:
            p = Path(root) / f
            if not should_ignore_file(p):
                if p.suffix in {".py", ".ts"}:
                    yield p


def process_path(
    target_path: Path,
    keep_docstrings: bool = True,
    format_markdown: bool = True,
) -> Tuple[str, int, int]:
    """
    Обрабатывает файл или директорию, возвращает:
    (текст скелета, исходный размер символов, размер символов скелета).
    """
    output_parts: list[str] = []
    total_orig_chars = 0
    total_skel_chars = 0
    files_processed = 0

    base_dir = Path.cwd()

    for file_path in sorted(collect_files(target_path)):
        try:
            content = file_path.read_text(encoding="utf-8")
        except Exception:
            continue

        orig_len = len(content)
        total_orig_chars += orig_len

        rel_path = file_path.relative_to(base_dir) if file_path.is_relative_to(base_dir) else file_path

        if file_path.suffix == ".py":
            skel = skeletonize_python(content, keep_docstrings=keep_docstrings)
            lang = "python"
        elif file_path.suffix == ".ts":
            skel = skeletonize_typescript(content)
            lang = "typescript"
        else:
            continue

        skel = skel.strip()
        if not skel:
            continue

        files_processed += 1
        total_skel_chars += len(skel)

        if format_markdown:
            output_parts.append(f"### `{rel_path}`\n```{lang}\n{skel}\n```\n")
        else:
            output_parts.append(f"# --- File: {rel_path} ---\n{skel}\n")

    header = ""
    if format_markdown:
        approx_orig_tokens = total_orig_chars // 4
        approx_skel_tokens = total_skel_chars // 4
        savings = 0 if total_orig_chars == 0 else int((1 - (total_skel_chars / total_orig_chars)) * 100)
        header = (
            f"<!-- Skeleton generated for: {target_path} | "
            f"Files: {files_processed} | "
            f"Tokens: ~{approx_orig_tokens:,} -> ~{approx_skel_tokens:,} (-{savings}%) -->\n\n"
        )

    return header + "\n".join(output_parts), total_orig_chars, total_skel_chars


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Генератор компактных синтаксических скелетов (AST) для экономии токенов."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=["backend/yarn"],
        help="Пути к файлам или директориям для анализа (по умолчанию: backend/yarn)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Путь к файлу для сохранения результата (по умолчанию: вывод в stdout)",
    )
    parser.add_argument(
        "--no-docstrings",
        action="store_true",
        help="Удалить все docstrings для максимального сжатия",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Выводить чистый код вместо блоков Markdown",
    )

    args = parser.parse_args()

    combined_output = []
    total_orig = 0
    total_skel = 0

    for p_str in args.paths:
        p = Path(p_str).resolve()
        if not p.exists():
            print(f"Предупреждение: путь {p} не найден, пропускаем.", file=sys.stderr)
            continue
        text, orig, skel = process_path(
            p,
            keep_docstrings=not args.no_docstrings,
            format_markdown=not args.raw,
        )
        combined_output.append(text)
        total_orig += orig
        total_skel += skel

    result_text = "\n\n".join(combined_output)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(result_text, encoding="utf-8")
        approx_orig_tokens = total_orig // 4
        approx_skel_tokens = total_skel // 4
        savings = 0 if total_orig == 0 else int((1 - (total_skel / total_orig)) * 100)
        print(
            f"Скелет успешно записан в: {args.output}\n"
            f"Символов: {total_orig:,} -> {total_skel:,}\n"
            f"Ориентировочные токены: ~{approx_orig_tokens:,} -> ~{approx_skel_tokens:,} (-{savings}% экономии)"
        )
    else:
        print(result_text)


if __name__ == "__main__":
    main()
