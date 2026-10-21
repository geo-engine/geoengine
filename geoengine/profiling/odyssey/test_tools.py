"""Exercise metadata-derived coordinates and the executable runner against a local HTTP server."""
from contextlib import redirect_stdout
import http.server
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).parent
spec = importlib.util.spec_from_file_location("tiles", ROOT / "tiles.py")
tiles = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tiles)
MATRIX = {"id": "7", "pointOfOrigin": [-10000, 10000], "cellSize": 10,
          "tileWidth": 100, "tileHeight": 100, "matrixWidth": 20, "matrixHeight": 20}


class ToolsTest(unittest.TestCase):
    def test_viewport_uses_custom_origin_and_tilesize(self):
        result = tiles.viewport(MATRIX, 0, 0, 3)
        self.assertEqual(result[0], (10, 10))
        self.assertEqual(set(result), {(r, c) for r in (9, 10, 11) for c in (9, 10, 11)})
        with self.assertRaises(ValueError):
            tiles.viewport(MATRIX, 50, 0, 3)

    def test_executable_records_repeats_duplicates_and_failures(self):
        seen = []
        fail = [False]

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.connection.settimeout(.1)

            def do_GET(self):
                if "tileMatrixSets" in self.path:
                    body = json.dumps({"tileMatrices": [MATRIX]}).encode()
                    content_type = "application/json"
                else:
                    seen.append((self.path, self.headers["X-GeoEngine-Profile-Run"]))
                    body = tiles.PNG + b"test"
                    content_type = "image/png"
                self.send_response(503 if fail[0] and "tileMatrixSets" not in self.path else 200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "run"
                command = [sys.executable, str(ROOT / "tiles.py"), "--api-url", f"http://127.0.0.1:{server.server_port}/api",
                           "--layer-id", "layer", "--center", "0", "0", "--zoom", "7", "--size", "3", "--concurrency", "4",
                           "--repeats", "2", "--duplicates", "2", "--pause", ".2", "--out", str(out)]
                import os
                result = subprocess.run(command, env=dict(os.environ, GEOENGINE_TOKEN="test-token"), capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                rows = [json.loads(r) for r in (out / "requests.jsonl").read_text().splitlines()]
                self.assertEqual(len(rows), 36)
                self.assertTrue(all(row["valid_png"] for row in rows))
                self.assertEqual(len(seen), 36)
                self.assertTrue(all("/7/" in path for path, _run in seen))
                self.assertNotIn("test-token", (out / "run.json").read_text())
                # Failed tiles remain in the artifacts and cause a nonzero exit, without retries.
                fail[0] = True
                failed_out = Path(tmp) / "failed"
                result = subprocess.run(command[:-1] + [str(failed_out)], env=dict(os.environ, GEOENGINE_TOKEN="test-token"), capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                failed_rows = [json.loads(r) for r in (failed_out / "requests.jsonl").read_text().splitlines()]
                self.assertEqual(len(failed_rows), 36)
                self.assertTrue(all(row["status"] == 503 and not row["valid_png"] for row in failed_rows))
                self.assertEqual(len(seen), 72)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_trace_summary_counts_tile_amplification(self):
        spec = importlib.util.spec_from_file_location("traces", ROOT / "traces.py")
        traces = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(traces)
        span = {"operationName": "raster_subquery_tile", "duration": 5000,
                "tags": [{"key": "input_tiles", "value": 4}]}
        output = io.StringIO()
        with redirect_stdout(output):
            traces.summarize([{"spans": [span]}])
        self.assertIn("total=4 outputs=1 mean=4.00", output.getvalue())

    def test_process_summary_uses_counter_deltas_and_pid_identity(self):
        spec = importlib.util.spec_from_file_location("traces", ROOT / "traces.py")
        traces = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(traces)
        samples = [
            {"unix_us": 1_000_000, "processes": [{"pid": 2, "start_ticks": 1, "comm": "gdalsource-proc", "cpu_seconds": 10}],
             "cgroup": {"cpu.stat": "nr_throttled 100\nnr_periods 1000"}},
            {"unix_us": 3_000_000, "processes": [{"pid": 2, "start_ticks": 1, "comm": "gdalsource-proc", "cpu_seconds": 11}],
             "cgroup": {"cpu.stat": "nr_throttled 102\nnr_periods 1020"}},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "processes.jsonl"
            path.write_text("\n".join(json.dumps(s) for s in samples))
            output = io.StringIO()
            with redirect_stdout(output):
                traces.summarize_processes(path, None)
            self.assertIn("average occupied cores=0.50", output.getvalue())
            self.assertIn("delta nr_throttled: 2", output.getvalue())


if __name__ == "__main__":
    unittest.main()
