import unittest

from hoist_yaml_key import hoist
from rename_tokens import apply_rules, parse_manifest


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


if __name__ == "__main__":
    unittest.main()
