#!/usr/bin/env python3
"""Apply YARN Explorer edits to capacity-scheduler.xml.

The explorer cannot read the deployed file (the ResourceManager does not expose it over REST),
so a change request is shipped as a list of edits and applied here, to the file that is actually
on the ResourceManager. Properties that are not mentioned in the patch are left untouched.

Patch format (JSON list), see ConfigPatchOp in the YARN backend:
    {"op": "set",           "name": "<property>", "value": "<value>"}
    {"op": "remove",        "name": "<property>"}
    {"op": "remove_prefix", "name": "<property name prefix>"}
    {"op": "list_add",      "name": "<property>", "value": "<item>"}   # comma-separated list
    {"op": "list_remove",   "name": "<property>", "value": "<item>"}

Usage:
    apply_capacity_patch.py --input current.xml --patch patch.json --output new.xml
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET

ALLOWED_PREFIX = "yarn.scheduler.capacity."


class PatchError(Exception):
    pass


def load(path):
    # The input is the ResourceManager's own config file, not user-supplied data; expat in the
    # standard library does not resolve external entities (Python >= 3.7.1)
    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
    tree = ET.parse(path, parser=parser)
    root = tree.getroot()
    if root.tag != "configuration":
        raise PatchError("root element is <%s>, expected <configuration>" % root.tag)
    return tree


def properties(root):
    """Ordered list of (name, property element)."""
    out = []
    for prop in root.findall("property"):
        name = (prop.findtext("name") or "").strip()
        out.append((name, prop))
    return out


def find(root, name):
    for n, prop in properties(root):
        if n == name:
            return prop
    return None


def set_value(root, name, value):
    prop = find(root, name)
    if prop is None:
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = name
        ET.SubElement(prop, "value").text = value
        return "added"
    value_el = prop.find("value")
    if value_el is None:
        value_el = ET.SubElement(prop, "value")
    old = value_el.text or ""
    if old == value:
        return None
    value_el.text = value
    return "changed (was %r)" % old


def list_items(value):
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def apply(tree, ops):
    root = tree.getroot()
    log = []
    for i, op in enumerate(ops):
        kind, name, value = op.get("op"), op.get("name"), op.get("value")
        if not name or not name.startswith(ALLOWED_PREFIX):
            raise PatchError("operation #%d: property %r is outside %s*" % (i, name, ALLOWED_PREFIX))

        if kind == "set":
            if value is None:
                raise PatchError("operation #%d: set without value" % i)
            result = set_value(root, name, value)
            if result:
                log.append("SET    %s = %s  [%s]" % (name, value, result))
        elif kind == "remove":
            prop = find(root, name)
            if prop is not None:
                root.remove(prop)
                log.append("REMOVE %s" % name)
        elif kind == "remove_prefix":
            # at least yarn.scheduler.capacity.root.<queue>. — never the whole root
            if not name.endswith(".") or name.count(".") < 5:
                raise PatchError("operation #%d: refusing too broad prefix %r" % (i, name))
            for n, prop in properties(root):
                if n.startswith(name):
                    root.remove(prop)
                    log.append("REMOVE %s" % n)
        elif kind in ("list_add", "list_remove"):
            if not value:
                raise PatchError("operation #%d: %s without value" % (i, kind))
            prop = find(root, name)
            items = list_items(prop.findtext("value") if prop is not None else "")
            if kind == "list_add" and value not in items:
                items.append(value)
            elif kind == "list_remove":
                if value not in items:
                    log.append("SKIP   %s: %r is already absent" % (name, value))
                    continue
                items.remove(value)
                if not items:
                    raise PatchError("operation #%d: %s would become empty" % (i, name))
            else:
                continue
            result = set_value(root, name, ",".join(items))
            if result:
                log.append("SET    %s = %s  [%s]" % (name, ",".join(items), result))
        else:
            raise PatchError("operation #%d: unknown op %r" % (i, kind))
    return log


def validate(root):
    names = [n for n, _ in properties(root)]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise PatchError("duplicate properties: %s" % ", ".join(duplicates))
    if "yarn.scheduler.capacity.root.queues" not in names:
        raise PatchError("yarn.scheduler.capacity.root.queues is missing")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--input", required=True)
    ap.add_argument("--patch", required=True, help="JSON file with the list of operations")
    ap.add_argument("--output", required=True)
    args = ap.parse_args(argv)

    try:
        with open(args.patch, encoding="utf-8") as f:
            ops = json.load(f)
        if not isinstance(ops, list) or not ops:
            raise PatchError("patch must be a non-empty JSON list")
        tree = load(args.input)
        log = apply(tree, ops)
        validate(tree.getroot())
        ET.indent(tree, space="  ")
        tree.write(args.output, encoding="UTF-8", xml_declaration=True)
    except (PatchError, ET.ParseError, ValueError, OSError) as e:
        print("PATCH FAILED: %s" % e, file=sys.stderr)
        return 1

    print("\n".join(log) if log else "No changes: the file already matches the patch")
    return 0


if __name__ == "__main__":
    sys.exit(main())
