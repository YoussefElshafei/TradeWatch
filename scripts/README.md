# Scripts

Thin wrappers over the tools, in two flavours so both shells work. They add nothing the
underlying commands do not do — the point is that one command runs exactly what CI runs.

| Script | Purpose |
| --- | --- |
| `setup.sh` / `setup.ps1` | First run: check prerequisites, create `.env`, install backend and frontend dependencies. |
| `check.sh` / `check.ps1` | Every check CI runs, in CI's order. `--fix` / `-Fix` applies formatting and auto-fixable lint first. |
| `dev.sh` / `dev.ps1` | Start infrastructure, migrations, the API, every worker and the frontend; restart anything that exits; Ctrl+C stops it all. `--no-frontend` / `-NoFrontend` leaves the frontend to you. |

There is no Makefile: `make` is not present on a default Windows install, and a task runner
that only works for half the contributors is worse than typing the command.
