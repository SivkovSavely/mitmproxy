- This project uses uv. Always use `uv run pytest` and don't run pytest directly.
- To run all tests: `uv run tox`.
- When adding new source files, additionally run: `uv run tox -e individual_coverage -- FILENAME`.

## mitmweb frontend (web/)

- npm project. Run jest from `web/` (`npx jest <spec>`); from the repo root npx resolves a wrong global jest and fails confusingly. Full gate: `npm test` in `web/` (eslint + `tsc --noEmit` + jest --coverage).
- Jest/babel quirk: spec files must not use imports solely in type annotations (fails with "Cannot transform the imported binding ..."). Use local structural types in mocks instead.
- `web/src/js/backends/consts.ts` is generated (`web/gen/backend_consts.py`); do not hand-edit it.
- Building the frontend: `npm run ci-build-release` in `web/`. Vite 7 requires Node 20.19+; system node is 18 — use `~/.nvm/versions/node/v20.19.4/bin`. The script does `rm -rf ../mitmproxy/tools/web/static` first, so a failed build leaves the assets deleted; verify output exists afterwards.
- Built assets ARE tracked in git (`mitmproxy/tools/web/static/`, `mitmproxy/tools/web/index.html`). After any frontend source change: rebuild and commit the bundle too, otherwise the served UI is stale (source/bundle drift is not caught by tests). Sanity check a rebuild picked up your change by counting a known string in `static/index-*.js` (e.g. `grep -c 'Show more'`).
- This checkout is deployed: a systemd user unit `mitmproxy.service` runs `uv --directory <repo> run mitmweb` from it (drop-in `~/.config/systemd/user/mitmproxy.service.d/override.conf`). Restart the unit after changes; hard-refresh the browser after bundle rebuilds. Runtime config lives in `~/.mitmproxy/config.yaml` (upstream proxy mode, ports 20809/8083, web_password).
