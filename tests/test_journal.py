import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


TEST_TEMP_ROOT = os.path.join(os.path.dirname(__file__), ".tmp")
os.makedirs(TEST_TEMP_ROOT, exist_ok=True)
TEST_DIRECTORY = tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT)
os.environ["SCAN_DATA_DIR"] = TEST_DIRECTORY.name

import app as scanner  # noqa: E402


class JournalTests(unittest.TestCase):
    def setUp(self):
        for filename in os.listdir(TEST_DIRECTORY.name):
            path = os.path.join(TEST_DIRECTORY.name, filename)
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
        scanner._state["access_token"] = "test-token"

    def test_scan_and_signal_are_stored_and_exposed(self):
        result = {
            "timeframe": "1h",
            "symbol": "TEST",
            "signal": "bullish",
            "ltp": 100.0,
            "pkama": 98.0,
            "vol_ratio": 2.0,
            "dist_pct": 2.04,
            "sig_time": "2026-08-01T09:15:00+05:30",
        }
        params = {"length": 50, "vol_mult": 1.5}
        scanner.append_timeframe_journal("1h", [result], "test", params, 500)
        scanner.append_timeframe_journal("1d", [], "test", params, 500)

        payload = scanner.get_journal_payload("all")
        self.assertEqual(payload["summary"]["signals"], 1)
        self.assertEqual(payload["summary"]["scan_runs"], 2)
        self.assertEqual(payload["signals"][0]["symbol"], "TEST")

        client = scanner.app.test_client()
        self.assertEqual(client.get("/journal").status_code, 200)
        self.assertEqual(client.get("/api/journal?timeframe=1h").status_code, 200)
        download = client.get("/api/journal/download/1h.csv")
        self.assertEqual(download.status_code, 200)
        self.assertIn(b"TEST", download.data)

    def test_twenty_one_session_performance_is_calculated(self):
        result = {
            "timeframe": "1h",
            "symbol": "TEST",
            "signal": "bullish",
            "ltp": 100.0,
            "pkama": 98.0,
            "vol_ratio": 2.0,
            "dist_pct": 2.04,
            "sig_time": "2026-08-01T09:15:00+05:30",
        }
        scanner.append_timeframe_journal(
            "1h", [result], "test", {"length": 50, "vol_mult": 1.5}, 500
        )
        start = datetime(2026, 8, 2, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))
        candles = []
        for index in range(21):
            close = 101.0 + index
            candles.append(
                {
                    "date": start + timedelta(days=index),
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 2.0,
                    "close": close,
                    "volume": 1000,
                }
            )
        original_fetch = scanner.fetch_nifty500_instruments
        original_candles = scanner.get_upstox_candles
        scanner.fetch_nifty500_instruments = lambda: {"TEST": "NSE_EQ|TEST"}
        scanner.get_upstox_candles = lambda *_args, **_kwargs: candles
        try:
            update = scanner.update_signal_journal_tracking()
        finally:
            scanner.fetch_nifty500_instruments = original_fetch
            scanner.get_upstox_candles = original_candles

        self.assertEqual(update["completed"], 1)
        row = scanner.get_journal_payload("1h")["signals"][0]
        self.assertEqual(row["status"], "COMPLETED")
        self.assertEqual(row["return_1d"], 1.0)
        self.assertEqual(row["return_1m"], 21.0)
        self.assertEqual(row["best_move_pct"], 22.0)
        self.assertEqual(row["worst_move_pct"], -1.0)


if __name__ == "__main__":
    unittest.main()

