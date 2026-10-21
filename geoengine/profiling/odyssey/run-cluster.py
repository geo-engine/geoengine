#!/usr/bin/env python3
"""Run the artificial viewport inside Kubernetes and collect its profiling artifacts locally."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

TOOLS = Path(__file__).resolve().parent


def configure_job(job, args, name, authenticated):
    """Bind the existing Job template without embedding credentials in the artifact."""
    job["metadata"]["name"] = name
    spec = job["spec"]["template"]["spec"]
    container = spec["containers"][0]
    for env in container["env"]:
        if env["name"] == "LAYER_ID":
            env["value"] = args.layer_id
        elif env["name"] == "SCENE_TIME":
            env["value"] = args.time
        elif env["name"] == "GEOENGINE_TOKEN":
            env["valueFrom"]["secretKeyRef"]["name"] = name + "-session"
    container["args"][0] = container["args"][0].replace("--concurrency 6 --repeats 3", f"--concurrency {args.concurrency} --repeats {args.repeats}").replace("--resolution 38.218514", f"--resolution {args.resolution}").replace("--out /results/run", f"--run-id {name} --out /results/run")
    spec["volumes"][0]["configMap"]["name"] = name
    container["args"][0] = container["args"][0].replace("http://geoengine-edv-staging-backend:8080", "http://" + args.deployment + ":8080")
    if not authenticated:
        container["env"] = [e for e in container["env"] if e["name"] != "GEOENGINE_TOKEN"]
        container["args"][0] = container["args"][0].replace("python /tools/tiles.py", "python /tools/tiles.py --anonymous")
    return job


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer-id", required=True)
    parser.add_argument("--time", required=True, help="Confirmed Sentinel-2 acquisition/day")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path, help="Kubernetes config file; otherwise kubectl uses KUBECONFIG/defaults")
    parser.add_argument("--namespace", default="staging-geoengine-edv")
    parser.add_argument("--deployment", default="geoengine-edv-staging-backend")
    parser.add_argument("--jaeger-url", default="http://127.0.0.1:16686")
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--resolution", type=float, default=38.218514)
    parser.add_argument("--timeout", type=int, default=900, help="Overall workload deadline in seconds")
    args = parser.parse_args()
    if args.layer_id.startswith("YOUR_") or args.time.startswith("YOUR_"):
        parser.error("Replace the layer-id and time placeholders with an imported Sentinel-2 layer ID and acquisition day")
    if min(args.concurrency, args.repeats, args.timeout) <= 0 or args.resolution <= 0:
        parser.error("concurrency, repeats, timeout and resolution must be positive")
    if args.out.exists() and (not args.out.is_dir() or any(args.out.iterdir())):
        parser.error("output directory must be empty or new")
    kubectl = os.environ.get("KUBECTL") or ("/snap/kubectl/current/kubectl" if Path("/snap/kubectl/current/kubectl").exists() else shutil.which("kubectl"))
    if not kubectl:
        parser.error("kubectl is required")
    command = [kubectl]
    if args.kubeconfig:
        command += ["--kubeconfig", str(args.kubeconfig)]
    command += ["-n", args.namespace]

    def k(*parts, data=None):
        return subprocess.run(command + list(parts), input=data, text=True, capture_output=True, check=True).stdout

    def save(name, *parts, data=None):
        (args.out / name).write_text(k(*parts, data=data))

    deployment = json.loads(k("get", "deployment", args.deployment, "-o", "json"))
    image = next(c["image"] for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == args.deployment)
    pods = json.loads(k("get", "pods", "-l", "app.kubernetes.io/name=" + args.deployment, "-o", "json"))["items"]
    ready = [p for p in pods if not p["metadata"].get("deletionTimestamp") and any(
        c["name"] == args.deployment and c.get("ready") and c["image"] == image for c in p.get("status", {}).get("containerStatuses", []))]
    if len(ready) != 1:
        raise RuntimeError("Expected exactly one ready backend pod on the deployment's current image; wait for rollout completion")
    pod = ready[0]["metadata"]["name"]
    exec_args = ("exec", "-i", pod, "-c", args.deployment, "--", "python3", "-")
    # Fail before creating workload resources if this is not a running profiling build.
    k("exec", pod, "-c", args.deployment, "--", "curl", "--fail", "--silent", "--max-time", "5", "http://127.0.0.1:6770/functions_timing")
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "backend-pod.json").write_text(json.dumps(ready[0], indent=2))
    name = "odyssey-profile-" + time.strftime("%Y%m%d%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
    print(f"Run: {name}; backend: {pod}; image: {image}", flush=True)
    job = json.loads(k("create", "--dry-run=client", "-f", str(TOOLS / "job.yaml"), "-o", "json"))
    token = os.environ.get("GEOENGINE_TOKEN")
    job = configure_job(job, args, name, bool(token))
    sampler = None
    created = []
    try:
        config = k("create", "configmap", name, "--from-file=" + str(TOOLS / "tiles.py"), "--dry-run=client", "-o", "json")
        k("create", "-f", "-", data=config)
        created.append(("configmap", name))
        if token:
            secret = k("create", "secret", "generic", name + "-session", "--from-file=token=/dev/stdin", "--dry-run=client", "-o", "json", data=token)
            k("create", "-f", "-", data=secret)
            created.append(("secret", name + "-session"))
        source = (TOOLS / "pod_snapshot.py").read_text()
        save("hotpath-before.jsonl", *exec_args, "--hotpath", data=source)
        with (args.out / "processes.jsonl").open("w") as samples, (args.out / "sampler.log").open("w") as errors:
            sampler = subprocess.Popen(command + list(exec_args) + ["--seconds", str(args.timeout + 60)], stdin=subprocess.PIPE, stdout=samples, stderr=errors, text=True)
            sampler.stdin.write(source)
            sampler.stdin.close()
            k("create", "-f", "-", data=json.dumps(job))
            created.append(("job", name))
            (args.out / "job.json").write_text(json.dumps(job, indent=2))
            k("wait", "--for=jsonpath={.status.active}=1", "job/" + name, "--timeout=120s")
            worker = json.loads(k("get", "pods", "-l", "job-name=" + name, "-o", "json"))["items"][0]["metadata"]["name"]
            k("wait", "--for=condition=Ready", "pod/" + worker, "--timeout=120s")
            deadline = time.monotonic() + args.timeout
            while True:
                logs = k("logs", worker)
                if "workload_exit=" in logs:
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError("Workload deadline reached; inspect workload.log and the saved Job")
                (args.out / "workload.log").write_text(logs)
                time.sleep(2)
            (args.out / "workload.log").write_text(logs)
            print(logs, flush=True)
            k("cp", worker + ":/results/run", str(args.out / "run"))
            sampler.terminate()
            sampler.wait(timeout=15)
            sampler = None
            # Closing the exec stream can truncate its final JSONL row.
            sample_path = args.out / "processes.jsonl"
            raw_samples = sample_path.read_bytes()
            if raw_samples and not raw_samples.endswith(b"\n"):
                sample_path.write_bytes(raw_samples[:raw_samples.rfind(b"\n") + 1])
        save("hotpath-after.jsonl", *exec_args, "--hotpath", data=source)
        save("backend.log", "logs", pod, "-c", args.deployment, "--since=30m", "--timestamps")
        run_file = args.out / "run/run.json"
        if not run_file.is_file():
            raise RuntimeError(f"Workload failed before producing measurements; inspect {args.out / 'workload.log'}")
        time.sleep(10)  # Allow batched OTLP export to reach Jaeger.
        subprocess.run([sys.executable, str(TOOLS / "traces.py"), "--run", str(run_file), "--processes", str(args.out / "processes.jsonl"), "--jaeger-url", args.jaeger_url, "--out", str(args.out / "traces.json")], stdout=(args.out / "span-summary.txt").open("w"), check=True)
        if "workload_exit=0" not in logs:
            raise RuntimeError("Some workload requests failed; see workload.log and run/requests.jsonl")
        print(f"Artifacts: {args.out}; Jaeger filter: profile_run={name}")
    finally:
        if sampler:
            sampler.terminate()
            sampler.wait(timeout=15)
        for kind, resource in reversed(created):
            k("delete", kind, resource, "--ignore-not-found", "--wait=false")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(f"Command failed (exit {error.returncode}): {' '.join(error.cmd)}", file=sys.stderr)
        if error.stderr:
            print(error.stderr.strip(), file=sys.stderr)
        raise SystemExit(1) from None
    except RuntimeError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from None
