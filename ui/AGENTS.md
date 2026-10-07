# AGENTS.md: UI (Angular)

UI-specific guidance.
Repo-wide context (generated-code chain, PR rules) is in the root `AGENTS.md`.
Inside this directory, drop the module prefix from the `just` recipes below (`just build gis` instead of `just ui build gis`).

## Commands

- Install: `just ui install`.
  It also npm-links `../api-clients/typescript` as `@geoengine/api-client`, so rerun it after regenerating the API clients.
- Run: `just ui run [project] [--type=local|nightly]`.
  The default project is `gis`.
    - `local` proxies `/api` to a backend on `localhost:3030` (start it with `just backend run`).
    - `nightly` proxies to the hosted nightly backend, so no local backend is needed.
- Build: `just ui build <project>` (e.g. `gis`, `edv`, `core`, `common`).
  Building `core` or `common` writes to `dist/`; delete that output afterwards (see below).
- Lint: `just ui lint [--project=gis] [--fix]` (prettier + eslint)
- Test: `just ui test [--project=gis] [--include=<path>] [--ci] [filter]` (vitest).
  By default tests run in jsdom.
  `--ci` runs them in headless Chromium via Playwright, as CI does.
  Use it when a test passes locally but fails in CI.

## Structure

- `projects/common` and `projects/core` are libraries consumed by the apps as `@geoengine/common` / `@geoengine/core`.
  The apps are `gis`, `enhanced-data-viewer`, `manager` and `dashboards/*`.
- `tsconfig.json` resolves the libraries from `./dist/<lib>` **before** `./projects/<lib>/src`.
  **Don't leave built `dist/common` or `dist/core` behind**: the apps would compile against stale builds instead of the sources.
  No recipe cleans them up, so delete them yourself.
  Don't build `common` into `dist/` to make another build pass.
- Before writing a new component, service or style, check whether `common` or `core` already provides it.
  Put code shared between apps there rather than in an app.

## Conventions

- TypeScript is strict.
  Give every function parameter and return value an explicit type; ESLint makes missing return types an error.
  Avoid `any`.
- ESLint also requires the `geoengine` prefix on component and directive selectors, warns when signals could replace other state, and makes floating promises an error.
- Add `*.spec.ts` unit tests next to new or changed logic (services, pipes, utilities).
- Interactive elements must work with the keyboard and have accessible names (`aria-label`, `<label>`, or visible text).
