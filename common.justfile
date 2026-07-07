_default:
    @just --list

# Clear the terminal before executing a command. Does not fail in a CI.
_clear:
    @-clear

# Check if there are uncommitted changes in the git repository. If there are, print an error message and exit with a non-zero status code. Otherwise, print a success message.
[group('ci')]
check-no-changes-in-git-repo:
    #!/usr/bin/env bash
    if [ -n "$(git status --porcelain)" ]; then
      echo "Error: Uncommitted changes found in git repository."
      git status --porcelain
      exit 1
    else
      echo "No uncommitted changes found in git repository."
    fi

# Wait for a port to be open. If the port is not open, wait for a specified amount of time and check again. Repeat until the port is open.
[arg("port", help="Port to wait for.")]
[arg("sleep", long="sleep", help="Time in seconds to sleep between checks.")]
[group('ci')]
[script("python3")]
wait-for-port port sleep="0.5":
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
