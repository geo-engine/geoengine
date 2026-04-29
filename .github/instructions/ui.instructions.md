---
name: UI Coding Instructions
applyTo: "**/*.ts, ui/**/*.json"
---

# UI Development Instructions

## Quick Setup

- **Install & Deps:** Node.js LTS and your package manager (`just ui install`).
- **Start:** Use `just ui run`.

## Coding Guidelines

- **Style:** Follow existing project conventions and lints.
- **Types:** Use TypeScript types for all function parameters and return values.
- **Small PRs:** Keep changes focused and well-scoped.

## Testing & Linting

- **Run tests:** `just ui test` (unit/e2e where applicable).
- **Lint:** Run `just ui lint` and fix reported issues.
- Test all functions that are testable with unit tests.

## Accessibility & UX

- **A11y:** Follow basic accessibility checks (ARIA, keyboard).
- **Consistency:** Reuse existing components and tokens.
