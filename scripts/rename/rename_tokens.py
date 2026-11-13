#!/usr/bin/env python3
"""Apply a token-replacement manifest to tracked text files."""
import fnmatch
import re
import os
import subprocess
import sys

EXCLUDED_PREFIXES = ("docs/superpowers/", "data/", "scripts/rename/", "CHANGELOG")
EXCLUDED_SUFFIXES = ("package-lock.json", "pnpm-lock.yaml", "V1__baseline_schema.sql")


def parse_manifest(lines):
    rules = []
    for raw in lines:
        line = raw.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = line.split("\t")
        kind = parts[0]
        if kind == "keep" and len(parts) == 2:
            rules.append(("keep", parts[1], None, None))
        elif kind in ("sub", "re") and len(parts) in (3, 4):
            globs = parts[3].split(",") if len(parts) == 4 else None
            rules.append((kind, parts[1], parts[2], globs))
        else:
            raise ValueError(f"bad manifest line: {line!r}")
    return rules


def load_denylist_keeps(lines):
    """Turn quoted literals (one per line) into implicit keep rules."""
    keeps = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if len(line) >= 2 and line[0] == '"' and line[-1] == '"':
            line = line[1:-1]
        keeps.append(("keep", line, None, None))
    return keeps


def apply_rules(text, path, rules):
    keeps = [r[1] for r in rules if r[0] == "keep"]
    # Mask longer literals first so a shorter substring cannot split a longer one.
    for i in sorted(range(len(keeps)), key=lambda k: len(keeps[k]), reverse=True):
        text = text.replace(keeps[i], f"\x00K{i}\x00")
    counts = []
    for kind, old, new, globs in rules:
        if kind == "keep":
            continue
        if globs and not any(fnmatch.fnmatch(path, g) for g in globs):
            counts.append(0)
            continue
        if kind == "sub":
            counts.append(text.count(old))
            text = text.replace(old, new)
        else:
            text, n = re.subn(old, new, text, flags=re.MULTILINE)
            counts.append(n)
    for i, literal in enumerate(keeps):
        text = text.replace(f"\x00K{i}\x00", literal)
    return text, counts


def tracked_files():
    out = subprocess.run(["git", "ls-files", "-z"], check=True, capture_output=True).stdout
    for name in out.decode(errors="surrogateescape").split("\0"):
        if not name or name.startswith(EXCLUDED_PREFIXES) or name.endswith(EXCLUDED_SUFFIXES):
            continue
        yield name


def main(argv):
    dry = "--dry-run" in argv
    args = [a for a in argv[1:] if a != "--dry-run"]
    if not args:
        print("usage: rename_tokens.py [--dry-run] <manifest>", file=sys.stderr)
        return 2
    with open(args[0], encoding="utf-8") as fh:
        rules = parse_manifest(fh)
    deny_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "denylist.txt")
    if os.path.exists(deny_path):
        with open(deny_path, encoding="utf-8") as fh:
            # Denylist keeps go before the manifest's own rules.
            rules = load_denylist_keeps(fh) + rules
    active = [r for r in rules if r[0] != "keep"]
    totals = [0] * len(active)
    changed = 0
    for path in tracked_files():
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except (FileNotFoundError, IsADirectoryError):
            continue
        if b"\0" in data:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        new, counts = apply_rules(text, path, rules)
        totals = [a + b for a, b in zip(totals, counts)]
        if new != text:
            changed += 1
            if not dry:
                with open(path, "w", encoding="utf-8", newline="") as fh:
                    fh.write(new)
    for (kind, old, new, _), n in zip(active, totals):
        flag = "WARNING no matches" if n == 0 else f"{n:6d}"
        print(f"{flag}\t{kind}\t{old}\t->\t{new}")
    print(f"files changed: {changed}{' (dry run)' if dry else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
