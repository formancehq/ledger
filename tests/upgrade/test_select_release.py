import unittest

from select_release import candidates


class ReleaseSelectionTest(unittest.TestCase):
    def test_orders_stable_after_rc_and_keeps_only_selected_line(self):
        tags = [
            "v3.0.0-rc.9", "v3.0.0-rc.10", "v3.0.0", "v3.0.1-rc.1",
            "v3.1.0", "v3.0.0-beta.10", "v2.4.16",
        ]
        self.assertEqual(
            candidates("release/v3.0", tags),
            ["v3.0.1-rc.1", "v3.0.0", "v3.0.0-rc.10", "v3.0.0-rc.9"],
        )

    def test_rejects_unscoped_branch(self):
        with self.assertRaises(ValueError):
            candidates("main", ["v3.0.0"])


if __name__ == "__main__":
    unittest.main()
