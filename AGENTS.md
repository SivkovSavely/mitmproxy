* This project uses uv. Always use `uv run pytest` and don't run pytest directly.
* To run all tests: `uv run tox`.
* When adding new source files, additionally run: `uv run tox -e individual_coverage -- FILENAME`.

## Completion and commits

* A feature/increment is complete only after its required implementation, tests, and validation have succeeded.
* After a feature/increment is complete and validated, commit it using the `commit-work` skill.
* Use one focused commit per feature/increment by default. Do not bundle unrelated changes into the same commit.
* Do not commit work that is known to be broken or has failed required validation.
* If validation cannot be completed, report that clearly and leave the work uncommitted unless the user explicitly instructs otherwise.

## Processes and services

* Never restart, stop, start, reload, kill, or otherwise manipulate running processes, services, systemd units, containers, or deployed applications unless the user explicitly instructs you to do so in the current task.
* This includes `mitmproxy.service` and any mitmweb/mitmproxy process.
* If changes require a restart/reload to take effect, do not perform it. Tell the user that a restart is required and provide the exact command(s) they should run.
* When a browser refresh is also required, tell the user explicitly.

## mitmweb frontend (web/)

* npm project. Run jest from `web/` (`npx jest <spec>`); from the repo root npx resolves a wrong global jest and fails confusingly. Full gate: `npm test` in `web/` (eslint + `tsc --noEmit` + jest --coverage).
* Jest/babel quirk: spec files must not use imports solely in type annotations (fails with "Cannot transform the imported binding ..."). Use local structural types in mocks instead.
* `web/src/js/backends/consts.ts` is generated (`web/gen/backend_consts.py`); do not hand-edit it.
* Building the frontend: `npm run ci-build-release` in `web/`. Vite 7 requires Node 20.19+; system node is 18 — use `~/.nvm/versions/node/v20.19.4/bin`. The script does `rm -rf ../mitmproxy/tools/web/static` first, so a failed build leaves the assets deleted; verify output exists afterwards.
* Built assets ARE tracked in git (`mitmproxy/tools/web/static/`, `mitmproxy/tools/web/index.html`). After any frontend source change: rebuild and include the resulting bundle changes in the feature commit, otherwise the served UI is stale (source/bundle drift is not caught by tests). Sanity check a rebuild picked up your change by counting a known string in `static/index-*.js` (e.g. `grep -c 'Show more'`).
* This checkout is deployed: a systemd user unit `mitmproxy.service` runs `uv --directory <repo> run mitmweb` from it (drop-in `~/.config/systemd/user/mitmproxy.service.d/override.conf`). **Do not restart the service yourself.** If changes require the running instance to reload, tell the user to run:
  `systemctl --user restart mitmproxy.service`
  Then tell the user to hard-refresh the browser if frontend assets changed.
* Runtime config lives in `~/.mitmproxy/config.yaml` (upstream proxy mode, ports 20809/8083, web_password). The drop-in sets `PYTHONUNBUFFERED=1`: mitmproxy logs via `print()` to stdout (`TermLogHandler`), and Python block-buffers stdout when it is a journald pipe, which otherwise holds all log lines until process exit.
* Content-view "Show more" loads +10000 lines per click (user preference; reduce if it lags). JSON bodies (`view_name === "JSON"`) render read-only via CodeMirror (`@codemirror/lang-json`) in `HttpMessage.tsx`/`CodeEditor.tsx`; do not add "json" to the generated backend `SyntaxHighlight` enum.
* Flow persistence: `~/.local/bin/mitmweb-fork` streams flows to `flows-%Y-%m-%d_%H.mitm` (strftime pattern passed unexpanded; mitmproxy rotates hourly on its own; `+` prefix required to append, not truncate). Never add `-r`: it loads the whole stream into RAM at startup (a 36G file = 36G RAM peak and a hung UI). `~/.local/bin/mitm-flows-zstd` compresses closed files (every 5 min via `mitm-flows-zstd.timer` and as `ExecStartPost` after each restart). It skips the current hour's file (name matched with `date +%Y-%m-%d_%H`, because `ExecStartPost` can race mitmproxy's first open) and files still open (`fuser`); on `.zst` collision it writes suffixed `flows-...-N.zst` (`zstd` refuses to overwrite); it must always exit 0 so the service can't fail on it. Most recorded traffic is LLM API SSE (openrouter.ai etc.), stored decompressed — ~38G/day raw, ~2G compressed. To browse recorded traffic use `~/.local/bin/mitm-flows-view [file.mitm|.zst]` (default: newest raw file): a read-only `mitmweb --no-server -r` on port 8084 with a throwaway confdir (CLI `--set web_port` would otherwise be overridden by `~/.mitmproxy/config.yaml`, which mitmproxy applies after CLI `--set`); `.zst` input is decompressed to a temp file removed on exit (plus a stale-sweep for SIGKILLed runs).
