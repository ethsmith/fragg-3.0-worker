# parser-worker

A Go worker that:

1. Asks the CSC core GraphQL API for every match in a configured season.
2. Skips matches whose either roster belongs to a test franchise (TFR / Test
   Franchise / TestHomeTeam / TestAwayTeam etc.).
3. Skips matches that are already ingested in the stats DB.
4. Downloads each remaining match's demo archive (`.zip` or `.7z` — sniffed
   by magic bytes, not by extension).
5. Parses every `.dem` inside with [`github.com/ethsmith/eco-rating`].
6. Posts the resulting player stats to the [fragg-3.0 stats API].

It is idempotent: re-runs are safe — already-ingested matches are skipped via
`GET /player-stats/match/:id`, and the POST uses `?upsert=true` as a
backstop. Restarting after a crash mid-pass loses no work.

[`github.com/ethsmith/eco-rating`]: https://github.com/ethsmith/eco-rating
[fragg-3.0 stats API]: ../fragg-3.0-api

---

## Repository layout

```
parser-worker/
├── cmd/
│   ├── worker/         # production binary (`go build ./cmd/worker`)
│   ├── probe/          # debug: hit CSC GraphQL and dump match info
│   └── parsetest/      # debug: download + parse a single match URL
├── internal/
│   ├── config/         # env-var loader
│   ├── csc/            # CSC core GraphQL client
│   ├── stats/          # fragg-3.0 stats API client
│   └── worker/         # orchestration (download → parse → upsert)
├── deploy/
│   └── egg-parser-worker.json  # Pterodactyl egg (panel import)
├── .env.example
└── go.mod
```

## Run modes

The binary inspects `CHECK_INTERVAL_MINUTES` to pick its mode:

| `CHECK_INTERVAL_MINUTES` | Behavior                                           | When to use                       |
| ------------------------ | -------------------------------------------------- | --------------------------------- |
| `0` or unset             | One pass, print JSON summary to stdout, exit       | local ad-hoc run, external cron   |
| `>0`                     | Daemon: pass → sleep N min → pass, until SIGTERM   | Pterodactyl, anything long-lived  |

In daemon mode `SIGINT` / `SIGTERM` cancels the in-flight pass cleanly, then
exits — Pterodactyl's stop signal (`^C` / SIGINT) shuts it down gracefully.

## Environment variables

See [`.env.example`](.env.example). Required:

| Var                  | Purpose                                                           |
| -------------------- | ----------------------------------------------------------------- |
| `SEASON`             | CSC season number to poll                                         |
| `STATS_API_URL`      | Base URL of the fragg-3.0 stats API                               |
| `STATS_API_KEY`      | Bearer token matching the stats API's `API_KEY`                   |

Optional knobs: `MATCH_TYPE`, `CSC_GRAPHQL_URL`, `MAX_MATCHES_PER_RUN`,
`CHECK_INTERVAL_MINUTES`.

## How `match_id` is derived

The fragg stats DB is keyed on `(match_id, steam_id)`. A CSC match is a
series (BO3 etc.) with multiple `.dem` files in the archive — one per map.
To keep the key deterministic and unique-per-map, the worker builds:

```
match_id = "<csc_match_id>-m<N>"
```

…where `N` is the 1-indexed alphabetical position of the `.dem` in the
archive. Sorting alphabetically before indexing means the same archive
always yields the same `match_id`, so re-runs upsert in place instead of
duplicating.

## Local quick start

```bash
cp .env.example .env
# fill in real values, then:
go run ./cmd/worker            # one pass (CHECK_INTERVAL_MINUTES=0 by default)
```

## Build a release binary

```bash
CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o parser-worker ./cmd/worker
```

The result is a single statically-linked binary with no system deps. Drop it
on any Linux host and run.

## Deploy on Pterodactyl

The egg at [`deploy/egg-parser-worker.json`](deploy/egg-parser-worker.json)
is a complete recipe — every setting (Git repo, Git branch, Go version,
season number, API URL/key, intervals, etc.) is exposed as a panel
variable, so a fresh server gets fully configured at create time.

### 1. Import the egg

In the Pterodactyl admin panel: **Nests → (your nest, or create one) → Import
Egg** → upload `deploy/egg-parser-worker.json`. The egg appears as **Parser
Worker** in that nest.

### 2. Push this repo to a Git host

The install script clones from a Git URL — that's how parser-worker gets
onto the server. Push this repo to GitHub (or any public git host) and
note the clone URL.

### 3. Create a server using the egg

**Servers → Create New** with:

- **Egg:** Parser Worker
- **Resources:** ~1 GB RAM, ~2 GB disk is plenty (demos are streamed to
  `/tmp` and deleted after each parse).
- **Variables:** the panel will prompt you for everything:

  | Variable                  | Example                                                |
  | ------------------------- | ------------------------------------------------------ |
  | `GIT_ADDRESS`             | `https://github.com/your-user/parser-worker.git`       |
  | `GIT_BRANCH`              | `main`                                                 |
  | `GO_VERSION`              | `1.25.0`                                               |
  | `SEASON`                  | `19`                                                   |
  | `STATS_API_URL`           | `https://fragg-3-0-api.example.com`                    |
  | `STATS_API_KEY`           | (paste the API key)                                    |
  | `MATCH_TYPE`              | `Regulation`                                           |
  | `CSC_GRAPHQL_URL`         | `https://core.playcsc.com/graphql`                     |
  | `MAX_MATCHES_PER_RUN`     | `50`                                                   |
  | `CHECK_INTERVAL_MINUTES`  | `30`                                                   |

### 4. Install + start

The panel's install step will fetch Go, clone the repo, build a static
binary into `/mnt/server/parser-worker`, then drop you into the runtime
container. Press **Start** — you should see a JSON summary every
`CHECK_INTERVAL_MINUTES`. Use **Stop** to shut down (sends SIGINT, which
the worker handles cleanly).

### 5. Updating

When you push new code, hit **Settings → Reinstall** in the panel. The
install script re-clones at the configured branch and rebuilds. Variables
are preserved across reinstalls.

## Debug / development tools

```bash
# Hit CSC GraphQL and dump a match including franchise info
go run ./cmd/probe

# Download + parse a single demo URL (no DB writes)
go run ./cmd/parsetest "https://cscdemos.../mid7704.7z"
```

## Output

Every pass prints a JSON summary to stdout. Example:

```json
{
  "ok": true,
  "result": {
    "season": 19,
    "matches_fetched": 928,
    "matches_eligible": 649,
    "matches_test_filtered": 1,
    "matches_skipped": 644,
    "matches_processed": 5,
    "demos_parsed": 7,
    "demos_failed": 0,
    "stats_docs_upserted": 70,
    "duration_seconds": 92.4
  }
}
```

Per-demo failures (if any) appear under `result.failed_demos` with the
specific match-id, demo filename, and error string.
