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

[arg("port", help="Port to wait for.")]
[arg("sleep", long="sleep", help="Time in seconds to sleep between checks.")]
[group('ci')]
[script("bash")]
wait-for-port port sleep="0.5":
    while ! nc -z localhost {{ port }}; do
      sleep {{ sleep }}
    done
