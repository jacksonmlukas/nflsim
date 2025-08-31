# nflsim — NFL Game Simulation & Forecasting Skeleton

This repo is a production-grade skeleton for a **state-of-the-art NFL game simulator** using only free/public data.

**What’s here**
- Clean `src/` package layout
- Partitioned Parquet data lake + DuckDB views
- Configs via YAML overlays
- Pipelines for data → features → models → sim → eval (stubs)
- Testing, linting, docs scaffolding

## Quick start
```bash
# (1) create venv and install (uv or pip)
pip install -e .
# (2) run CLI help
nflsim --help
```

## Structure
- `configs/` configuration overlays (env/season/model/sim)
- `data/` local data lake (raw → interim → processed → features)
- `src/nflsim/` package code (ETL, features, models, simulator)
- `tests/` unit + e2e scaffolding
- `docs/` mkdocs site scaffold

> All implementation files are intentionally **stubs**. Fill them in as you build.
