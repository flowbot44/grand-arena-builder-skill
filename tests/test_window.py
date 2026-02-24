from datetime import date
import unittest

from app.ingest import compute_window_dates


class WindowTests(unittest.TestCase):
    def test_window_for_2026_02_21(self) -> None:
        got = compute_window_dates(date(2026, 2, 21), lookbehind_days=2, lookahead_days=2)
        self.assertEqual(
            [d.isoformat() for d in got],
            [
                "2026-02-19",
                "2026-02-20",
                "2026-02-21",
                "2026-02-22",
                "2026-02-23",
            ],
        )


if __name__ == "__main__":
    unittest.main()
