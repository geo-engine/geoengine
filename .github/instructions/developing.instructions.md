---
name: General Development Instructions
applyTo: "**"
---

# General Developer Instructions

These instructions are for contributors working on the codebase.

## Development workflow

- If on the main branch, create a feature branch named `feat/<short-desc>` or `fix/<short-desc>` per conventional commits.
- Keep changes focused and small; open separate PRs for unrelated changes.

## CI / PR expectations

- PRs must include a descriptive title and body following `type(scope): description` (conventional commits).
  - Valid scopes are `backend`, `ui`, `python`, `api-client`, `www`
- Ensure all CI checks pass (formatting, lints, tests, and any repository-specific checks).
- Include a short testing checklist in the PR description: how to run, expected behavior, and any required migrations or configuration changes.

## Useful links

- Project README: [README.md](../../README.md)
- Justfile (tasks): [justfile](../../justfile)

## Notes for Copilot / Assistant

- Use this document as the primary source when answering developer questions about codebase workflows and conventions.
- If asked to change coding conventions, validate against this file and [`CODESTYLE.md`](../../CODESTYLE.md).
