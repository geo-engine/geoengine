_default:
    @just --list

# Clear the terminal before executing a command. Does not fail in a CI.
_clear:
    @-clear

# Check if there are uncommitted changes in the git repository. If there are, print an error message and exit with a non-zero status code. Otherwise, print a success message.
[group('ci')]
check-no-changes-in-git-repo:
    #!/usr/bin/env bash
    set -euo pipefail
    # fail if `git status` itself fails (e.g. due to the ownership check), instead of treating its empty output as clean
    changes="$(git status --porcelain)"
    if [ -n "$changes" ]; then
      echo "Error: Uncommitted changes found in git repository."
      echo "$changes"
      exit 1
    else
      echo "No uncommitted changes found in git repository."
    fi

# Wait for a port to be open. If the port is not open, wait for a specified amount of time and check again. Repeat until the port is open.
[arg("port", help="Port to wait for.")]
[arg("sleep", long="sleep", help="Time in seconds to sleep between checks.")]
[group('ci')]
[script("python3")]
wait-for-port port sleep="1.0":
    import socket
    import time
    import sys

    port = int("{{ port }}")
    sleep_time = float("{{ sleep }}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      s.settimeout(1)
      while True:
        try:
          s.connect(("localhost", port))
          print(f"Port {port} is open.")
          break
        except (socket.timeout, ConnectionRefusedError, ConnectionAbortedError):
          print(f"Port {port} is not open. Retrying in {sleep_time} seconds...")
          time.sleep(sleep_time)

# Run a command in the current directory and retry it after a linearly growing pause if it fails. Use it for downloads that have no retries of their own.
[arg("backoff", help="Pause in seconds after the first failed attempt, growing linearly with each further one.")]
[arg("command", help="Command and its arguments.")]
[arg("attempts", help="Maximum number of attempts.")]
[group('ci')]
[no-cd]
[positional-arguments]
retry attempts backoff +command:
    #!/usr/bin/env bash
    set -uo pipefail
    attempts="$1"
    backoff="$2"
    shift 2
    for attempt in $(seq 1 "$attempts"); do
      "$@" && exit 0
      if [ "$attempt" -lt "$attempts" ]; then
        pause=$((attempt * backoff))
        echo "Attempt $attempt/$attempts of \`$*\` failed. Retrying in ${pause}s..." >&2
        sleep "$pause"
      fi
    done
    echo "All $attempts attempts of \`$*\` failed." >&2
    exit 1
