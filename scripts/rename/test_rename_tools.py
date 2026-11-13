import unittest

from hoist_yaml_key import hoist
from rename_tokens import apply_rules, load_denylist_keeps, parse_manifest


class ApplyRulesTest(unittest.TestCase):
    def test_sub_replaces_literal(self):
        rules = parse_manifest(["sub\tyarn-explorer\tartel"])
        self.assertEqual(apply_rules("image: yarn-explorer:1", "a.yml", rules)[0],
                         "image: artel:1")

    def test_keep_protects_literal(self):
        rules = parse_manifest([
            "keep\tyarn-explorer-test-secret-key",
            "sub\tyarn-explorer\tartel",
        ])
        text = 'deny("yarn-explorer-test-secret-key"); host = "yarn-explorer";'
        self.assertEqual(
            apply_rules(text, "A.java", rules)[0],
            'deny("yarn-explorer-test-secret-key"); host = "artel";')

    def test_glob_limits_scope(self):
        rules = parse_manifest(["re\t^yarn:\tartel:\tbackend/artel/*"])
        self.assertEqual(apply_rules("yarn:\n  a: 1\n", "backend/artel/x.yml", rules)[0],
                         "artel:\n  a: 1\n")
        self.assertEqual(apply_rules("yarn:\n  a: 1\n", "demo/x.yml", rules)[0],
                         "yarn:\n  a: 1\n")

    def test_regex_lookbehind_keeps_hadoop_classes(self):
        rules = parse_manifest([
            "re\t(?<![.\\w])hadoop\\.security\\.(jwt|auth(?![a-z_]))\tzastava.\\1",
        ])
        text = ("org.apache.hadoop.security.UserGroupInformation "
                "hadoop.security.authentication hadoop.security.jwt.x "
                "hadoop.security.auth.mode")
        self.assertEqual(
            apply_rules(text, "A.java", rules)[0],
            "org.apache.hadoop.security.UserGroupInformation "
            "hadoop.security.authentication zastava.jwt.x zastava.auth.mode")

    def test_counts_are_reported_per_rule(self):
        rules = parse_manifest(["sub\ta\tb", "sub\tzzz\ty"])
        _, counts = apply_rules("aa", "f", rules)
        self.assertEqual(counts, [2, 0])

    def test_overlapping_keeps_mask_longer_first(self):
        # With the shorter literal masked first, "abc-def" would be left as K0-def
        # and the sub would rewrite its "def" part.
        rules = parse_manifest(["keep\tabc", "keep\tabc-def", "sub\tdef\tX"])
        self.assertEqual(apply_rules("abc-def def", "f", rules)[0], "abc-def X")


class DenylistTest(unittest.TestCase):
    def test_load_denylist_keeps_strips_quotes(self):
        keeps = load_denylist_keeps(['"some-secret"\n', "\n", '"other"'])
        self.assertEqual(keeps, [("keep", "some-secret", None, None),
                                 ("keep", "other", None, None)])

    def test_denylisted_literal_untouched_elsewhere_replaced(self):
        rules = load_denylist_keeps(['"yarn-explorer-secret"']) + parse_manifest(
            ["sub\tyarn-explorer\tartel"])
        self.assertEqual(
            apply_rules('"yarn-explorer-secret" yarn-explorer', "f", rules)[0],
            '"yarn-explorer-secret" artel')


class HoistTest(unittest.TestCase):
    def test_moves_child_to_top_level(self):
        src = ("server:\n  port: 1\n"
               "hadoop:\n  security:\n    jwt:\n      secret-key: x\n  hdfs:\n    a: 1\n"
               "management:\n  x: 1\n").splitlines(keepends=True)
        out, changed = hoist(src, "hadoop", "security", "zastava")
        self.assertTrue(changed)
        self.assertEqual("".join(out),
                         "server:\n  port: 1\n"
                         "hadoop:\n  hdfs:\n    a: 1\n"
                         "zastava:\n  jwt:\n    secret-key: x\n"
                         "management:\n  x: 1\n")

    def test_removes_empty_parent(self):
        src = "hadoop:\n  security:\n    debug: true\n".splitlines(keepends=True)
        out, changed = hoist(src, "hadoop", "security", "zastava")
        self.assertTrue(changed)
        self.assertEqual("".join(out), "zastava:\n  debug: true\n")

    def test_no_child_is_noop(self):
        src = "hadoop:\n  hdfs:\n    a: 1\n".splitlines(keepends=True)
        out, changed = hoist(src, "hadoop", "security", "zastava")
        self.assertFalse(changed)
        self.assertEqual(out, src)

    def test_keeps_comments_inside_block(self):
        src = "hadoop:\n  security:\n    # note\n    debug: true\n".splitlines(keepends=True)
        out, _ = hoist(src, "hadoop", "security", "zastava")
        self.assertEqual("".join(out), "zastava:\n  # note\n  debug: true\n")

    def test_grandchild_is_not_hoisted(self):
        src = "hadoop:\n  hdfs:\n    security:\n      a: 1\n".splitlines(keepends=True)
        out, changed = hoist(src, "hadoop", "security", "zastava")
        self.assertFalse(changed)
        self.assertEqual(out, src)

    def test_multi_document(self):
        src = ("hadoop:\n  security:\n    a: 1\n---\n"
               "hadoop:\n  security:\n    b: 2\n  hdfs:\n    c: 3\n").splitlines(keepends=True)
        out, changed = hoist(src, "hadoop", "security", "zastava")
        self.assertTrue(changed)
        self.assertEqual("".join(out),
                         "zastava:\n  a: 1\n---\n"
                         "hadoop:\n  hdfs:\n    c: 3\nzastava:\n  b: 2\n")


if __name__ == "__main__":
    unittest.main()
