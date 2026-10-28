---
name: update-dependencies
description: Update the dependencies of all Geo Engine projects (Rust toolchain and crates, OpenAPI generator, Python, Angular/npm, Astro) as a stack of PRs, one per project, in dependency order. Use when asked to update dependencies, toolchains or Angular/Rust/Astro versions, for one project or all.
---

# Update dependencies

The projects depend on each other through the generated-code chain (root `AGENTS.md`): backend → `openapi.json` → api-clients → python and ui → www docs.
CI (`just repo lint-generated-code`) regenerates the spec, the clients and www and fails on any diff.
So update the projects in the order below, one PR per project, each stacked on the previous one.

If the user asks for a single project, do only its step, but still branch from the previous step's branch if that is not merged yet.

## 1. Rules for the stack

- One branch and PR per project, named `build/<project>-dependencies-<YYYY-MM-DD>` (e.g. `build/ui-dependencies-2026-10-28`).
  The first branch starts from `main`, every later one from the previous branch.
  Each PR targets the previous branch, so its diff shows only its own changes.
- PR titles: `build(<scope>): update dependencies`, optionally naming the headline updates (`build(ui): update dependencies, Angular 22.2 and vitest 5`).
  Scopes: `backend`, `api-client` (not `api-clients`), `python`, `ui`, `www`.
  The description says how to test, the expected behavior and any configuration changes (root `AGENTS.md`).
  List the major updates and the majors that were left out, with the reason.
- Ask the user before every commit, push and PR.
- Skip a project if nothing changes; the next branch then starts from the last one that exists.
- Leave a major update out if a peer does not allow it yet (e.g. TypeScript newer than `@angular/build` accepts).
- PRs are squash-merged.
  After a lower PR is merged, rebase the next branch onto `main` with `git rebase --onto main <old-base-branch> <branch>` and change its PR base to `main`.
- Before each commit, run `just ci <module>` and `just repo lint`.

## 2. backend (`build(backend)`)

1. `just backend update-step1-rust`: sets the latest stable Rust in `rust-toolchain.toml` and runs `clippy --fix`.
2. `just backend update-step2-minor-dependencies`: `cargo upgrade` (including incompatible versions) and `cargo update`.
3. `just backend update-step3-aftermath`: syncs `expression/deps-workspace` and the sqlfluff version.
   Check it with `cargo test --package geoengine-expression --test check-expression-deps`.
4. Fix new clippy lints and compile errors.
5. If the updates change the OpenAPI spec (e.g. a new utoipa version), run `just backend generate-openapi-spec` and `just api-clients build` and commit the result in this PR.

## 3. api-clients (`build(api-client)`)

There is no recipe.

1. Set the latest version of the OpenAPI generator in `api-clients/openapitools.json` under `generator-cli.version`.
   `npx @openapitools/openapi-generator-cli version-manager list` shows the available versions.
2. `just api-clients build`.
3. If the generated clients change their interface, adapt `python/` and `ui/` in this PR, so that their CI passes.

## 4. python (`build(python)`)

1. `just python update-step1-dependencies --dry-run` lists the upper bounds in `pyproject.toml` that would be raised.
   Entries marked `[major]` admit a new major version; check their changelogs.
2. `just python update-step1-dependencies` writes the bounds.
   Lower bounds stay, since CI also tests the lowest versions (`--resolution=lowest-direct`).
3. `just python update-step2-aftermath` recreates `.venv` and runs `lint --fix`.
4. `just python test` (builds and starts the backend, needs Postgres) and `just python run-examples`.
5. CI does not run the notebooks in `examples/` subdirectories, e.g. the ML app in `examples/interactive_ml/app/`.
   Test them by hand when their libraries (ipywidgets, ipyvuetify, voila) get a major update, or say in the PR that they are untested.

## 5. ui (`build(ui)`)

1. `just ui update-step1-angular`: installs, runs `ng update` for Angular, Material and angular-eslint, then `lint --fix`.
2. `just ui update-step2-npm`: minor updates of all other packages.
3. `npx npm-check-updates --target latest` lists the remaining major updates.
   Check each one's peers with `npm view <pkg>@<version> peerDependencies`, especially against `@angular/build` (it limits `typescript` and `vitest`).
   Update the compatible ones in groups, test tooling first, then libraries, and test after each group.
4. Verify with `just ui lint`, `just ui test --ci` and `just ui build <project>` for `gis`, `manager`, `eodyssey` and the dashboards that use changed packages.
   Delete the build output in `dist/` afterwards (`ui/AGENTS.md`).

## 6. www (`build(www)`)

www comes last because its generated docs come from `openapi.json` and the Python docstrings.

1. `just www update-step1-astro`, then `just www update-step2-npm`.
2. `just www build` and `just www lint`.
   Commit regenerated docs in this PR.

## Pitfalls

- `ng update` fails with `ELSPROBLEMS` and lists every package as missing when `ui/node_modules` does not exist.
  `update-step1-angular` installs first for this reason.
- npm `ERESOLVE` within a package family whose members pin each other exactly as peers (e.g. `vitest` and `@vitest/*`): a transitive member such as `@vitest/browser` keeps the whole family on the old version, even when the direct members are named in `npm install`.
  Remove the family from `node_modules` and its `packages["node_modules/<name>"]` entries from `package-lock.json` (e.g. with `jq --indent 4`), then run `npm install` again.
- Minor updates can tighten types, e.g. OpenLayers 10.11 made `Feature.getStyle()` return `null` and typed the tile caches as `LRUCache<Tile | null>`.
  Adapt the code rather than casting.
- Tests that assert on the wording of third-party error messages break when it changes.
  Assert only the part that is the same in the lowest and latest supported versions.
- New formatter versions can format more, e.g. ruff formats Python code blocks in Markdown.
  Keep the result, since the format check in CI uses the new version.
