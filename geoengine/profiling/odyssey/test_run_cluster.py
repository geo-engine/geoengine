"""Validate cluster Job binding and credential handling without touching a cluster."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("run_cluster", Path(__file__).with_name("run-cluster.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ClusterJobTest(unittest.TestCase):
    def job(self):
        return {"metadata": {}, "spec": {"template": {"spec": {
            "containers": [{"env": [{"name": "LAYER_ID", "value": "placeholder"},
                                     {"name": "SCENE_TIME", "value": "placeholder"},
                                     {"name": "GEOENGINE_TOKEN", "valueFrom": {"secretKeyRef": {"name": "placeholder", "key": "token"}}}],
                            "args": ["python /tools/tiles.py --api-url http://geoengine-edv-staging-backend:8080/api --concurrency 6 --repeats 3 --resolution 38.218514 --out /results/run"]}],
            "volumes": [{"configMap": {"name": "placeholder"}}]}}}}

    def args(self):
        return argparse.Namespace(layer_id="actual-layer", time="2026-04-01T00:00:00Z", concurrency=12, repeats=2, resolution=9.55, deployment="backend")

    def test_anonymous_job_omits_secret_and_binds_workload(self):
        job = runner.configure_job(self.job(), self.args(), "run-1", False)
        spec = job["spec"]["template"]["spec"]
        container = spec["containers"][0]
        self.assertEqual(job["metadata"]["name"], "run-1")
        self.assertEqual(spec["volumes"][0]["configMap"]["name"], "run-1")
        self.assertNotIn("GEOENGINE_TOKEN", json.dumps(container["env"]))
        self.assertIn("--anonymous", container["args"][0])
        self.assertIn("--run-id run-1", container["args"][0])
        self.assertIn("--concurrency 12 --repeats 2", container["args"][0])
        self.assertIn("--resolution 9.55", container["args"][0])
        self.assertIn("http://backend:8080/api", container["args"][0])
        self.assertEqual(container["env"][0]["value"], "actual-layer")

    def test_authenticated_job_references_its_own_secret(self):
        job = runner.configure_job(self.job(), self.args(), "run-2", True)
        container = job["spec"]["template"]["spec"]["containers"][0]
        self.assertNotIn("--anonymous", container["args"][0])
        self.assertEqual(container["env"][2]["valueFrom"]["secretKeyRef"]["name"], "run-2-session")
        self.assertNotIn('"token":', json.dumps(job))

    def test_connection_error_reports_stderr_and_honors_explicit_kubeconfig(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            kubectl = root / "kubectl"
            kubectl.write_text("#!/usr/bin/env python3\nimport json, pathlib, sys\n"
                               "pathlib.Path(__file__).with_suffix('.args').write_text(json.dumps(sys.argv[1:]))\n"
                               "print('cluster connection failed for test', file=sys.stderr)\nsys.exit(1)\n")
            kubectl.chmod(0o700)
            out = root / "existing-empty-results"
            out.mkdir()
            result = subprocess.run([sys.executable, str(Path(runner.__file__)),
                                     "--layer-id", "actual-layer", "--time", "2026-04-23T00:00:00Z",
                                     "--kubeconfig", str(root / "cluster.yaml"), "--out", str(out)],
                                    env=dict(os.environ, KUBECTL=str(kubectl)), capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("cluster connection failed for test", result.stderr)
            self.assertNotIn("Traceback", result.stderr)
            command = json.loads(kubectl.with_suffix('.args').read_text())
            self.assertEqual(command[:2], ["--kubeconfig", str(root / "cluster.yaml")])
            self.assertEqual(list(out.iterdir()), [])

    def test_documentation_placeholders_fail_before_creating_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "results"
            result = subprocess.run([sys.executable, str(Path(runner.__file__)),
                                     "--layer-id", "YOUR_SENTINEL2_TCI_LAYER_ID",
                                     "--time", "YOUR_CONFIRMED_ACQUISITION_DAY", "--out", str(out)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("Replace the layer-id and time placeholders", result.stderr)
            self.assertFalse(out.exists())


if __name__ == "__main__":
    unittest.main()
