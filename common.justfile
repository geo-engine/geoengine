_default:
    @just --list

# Clear the terminal before executing a command. Does not fail in a CI.
_clear:
    @-clear
