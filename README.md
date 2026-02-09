# Game Analytics (dbt)

dbt project that builds a game analytics warehouse on top of raw data in Snowflake. Raw tables (`RAW_PLAYERS`, `RAW_SESSIONS`, `RAW_GAME_EVENTS`) are produced by the [game-data-platform](../game-data-platform) pipeline; this project stages and models them into core and analytics marts.

**Profile:** `game_analytics`

## Project layout

```
models/
├── raw/           # Source definitions (src_players, src_sessions, src_game_events)
├── staging/       # Views: stg_players, stg_sessions, stg_game_events
└── marts/
    ├── core/      # dim_players, fct_sessions, fct_game_events
    └── analytics/ # daily_active_players, funnel_sessions, retention
```

- **Staging:** views over raw sources (cleaning, renaming, typing).
- **Marts:** tables — core for dimensions/facts, analytics for reporting (DAU, funnel, retention).

## Prerequisites

- dbt Core (or dbt Cloud) with a Snowflake adapter
- Raw data loaded in Snowflake via game-data-platform (schema: `GAME_ANALYTICS.RAW` or as configured in your profile)

## Setup

1. **Configure the dbt profile** (e.g. `~/.dbt/profiles.yml`):

```yaml
game_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<your_account>"
      user: "<your_user>"
      password: "<your_password>"
      role: "<your_role>"
      database: GAME_ANALYTICS
      warehouse: "<your_warehouse>"
      schema: dev
      threads: 4
```

2. **Install dependencies** (if using packages):

```bash
dbt deps
```

## Commands

| Command | Description |
|--------|-------------|
| `dbt build` | Run all models (staging + marts); recommended for a full refresh. |
| `dbt run` | Run all models (no tests). |
| `dbt test` | Run tests on sources and models. |
| `dbt run --select staging` | Run only staging models. |
| `dbt run --select marts` | Run only marts. |

## Materialization

- **Staging:** views (`+materialized: view` in `dbt_project.yml`).
- **Marts:** tables (`+materialized: table`).
- Seeds (if used) build into the `raw` schema.

## CI/CD (GitHub Actions)

On every **push** and **pull_request** to `main`, the workflow runs:

- Checkout → Setup Python 3.11 → Install `dbt-snowflake` → `dbt deps` → `dbt compile --target ci`

**Required secret:** In the repo **Settings → Secrets and variables → Actions**, add:

- **`SNOWFLAKE_CI_PROFILE`** — full contents of `profiles.yml` for CI. Must include profile `game_analytics` with a target **`ci`**, for example:

```yaml
game_analytics:
  target: ci
  outputs:
    ci:
      type: snowflake
      account: "<ci_account>"
      user: "<ci_user>"
      password: "<ci_password>"
      role: "<ci_role>"
      database: GAME_ANALYTICS
      warehouse: "<ci_warehouse>"
      schema: ci
      threads: 2
```

If `dbt compile --target ci` fails, the workflow fails and the PR cannot merge (when branch protection requires this check).

## Related repo

Data generation and Snowflake load live in **game-data-platform**. Generate data and load to Snowflake there, then run `dbt build` in this project to refresh the warehouse.
