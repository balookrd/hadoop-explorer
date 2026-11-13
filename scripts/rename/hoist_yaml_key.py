#!/usr/bin/env python3
"""Move a nested YAML key (parent.child) to the top level under a new name.

Line-based on purpose: keeps comments, ordering and quoting intact.
Handles block style only; inline maps are left untouched and reported.
"""
import re
import sys


def _indent(line):
    return len(line) - len(line.lstrip(" "))


def _hoist_at(lines, start, child, new):
    """Hoist the child of the parent block starting at `start`.

    Returns (lines, changed, resume_index).
    """
    end = start + 1
    while end < len(lines) and (not lines[end].strip() or lines[end].startswith(" ")):
        end += 1
    while end > start + 1 and not lines[end - 1].strip():
        end -= 1
    # Direct children share the indent of the first content line under the parent.
    first = next((i for i in range(start + 1, end)
                  if lines[i].strip() and not lines[i].lstrip().startswith("#")), None)
    if first is None:
        return lines, False, end
    d_indent = _indent(lines[first])
    child_re = re.compile(rf"^ {{{d_indent}}}{re.escape(child)}:\s*(#.*)?$")
    c_start = next((i for i in range(start + 1, end)
                    if child_re.match(lines[i].rstrip("\n"))), None)
    if c_start is None:
        return lines, False, end
    c_indent = d_indent
    c_end = c_start + 1
    while c_end < end and (not lines[c_end].strip() or _indent(lines[c_end]) > c_indent):
        c_end += 1
    while c_end > c_start + 1 and not lines[c_end - 1].strip():
        c_end -= 1
    block = [f"{new}:\n"] + [
        l[c_indent:] if l.strip() else l for l in lines[c_start + 1:c_end]
    ]
    rest = lines[start + 1:c_start] + lines[c_end:end]
    has_other = any(l.strip() and not l.strip().startswith("#") for l in rest)
    kept_parent = [lines[start]] + rest if has_other else []
    out = lines[:start] + kept_parent + block + lines[end:]
    resume = start + len(kept_parent) + len(block)
    return out, True, resume


def hoist(lines, parent, child, new):
    parent_re = re.compile(rf"^{re.escape(parent)}:\s*(#.*)?$")
    changed = False
    i = 0
    while i < len(lines):
        if parent_re.match(lines[i].rstrip("\n")):
            lines, did, i = _hoist_at(lines, i, child, new)
            changed = changed or did
        else:
            i += 1
    return lines, changed


def main(argv):
    if len(argv) < 5:
        print("usage: hoist_yaml_key.py <parent> <child> <new> <file>...", file=sys.stderr)
        return 2
    parent, child, new, files = argv[1], argv[2], argv[3], argv[4:]
    for path in files:
        with open(path, encoding="utf-8") as fh:
            lines = fh.readlines()
        out, changed = hoist(lines, parent, child, new)
        if changed:
            with open(path, "w", encoding="utf-8", newline="") as fh:
                fh.writelines(out)
        print(f"{'hoisted' if changed else 'no-op  '}\t{path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
