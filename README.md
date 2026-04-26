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
│   └── parser-worker.service   # example systemd unit
├── Dockerfile          # multi-stage build → distroless/static (~25 MB)
├── .env.example
└── go.mod
```

## Run modes

The binary inspects `CHECK_INTERVAL_MINUTES` to pick its mode:

| `CHECK_INTERVAL_MINUTES` | Behavior                                           | When to use                          |
| ------------------------ | -------------------------------------------------- | ------------------------------------ |
| `0` or unset             | One pass, print JSON summary to stdout, exit       | system cron, k8s CronJob, ad-hoc run |
| `>0`                     | Daemon: pass → sleep N min → pass, until SIGTERM   | systemd, docker, anything long-lived |

In daemon mode `SIGINT` / `SIGTERM` cancels the in-flight pass cleanly, then
exits.

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

## Deploy

### Option A — systemd (daemon mode)

`deploy/parser-worker.service` is a hardened example unit. Quick path:

```bash
go build -trimpath -ldflags='-s -w' -o parser-worker ./cmd/worker
sudo install -m 755 ./parser-worker /usr/local/bin/parser-worker
sudo install -m 644 deploy/parser-worker.service /etc/systemd/system/
sudo install -m 600 .env /etc/parser-worker.env

sudo systemctl daemon-reload
sudo systemctl enable --now parser-worker.service
sudo journalctl -u parser-worker -f
```

Set `CHECK_INTERVAL_MINUTES` to a positive number in `/etc/parser-worker.env`
so the worker stays alive and polls on its own.

### Option B — Docker (daemon mode)

```bash
docker build -t parser-worker .
docker run -d --name parser-worker --restart=unless-stopped \
  --env-file .env parser-worker
docker logs -f parser-worker
```

Image is built `FROM gcr.io/distroless/static-debian12:nonroot`, runs as a
non-root user, ~25 MB total.

### Option C — system cron (one-shot mode)

Leave `CHECK_INTERVAL_MINUTES` at `0`. The binary runs one pass and exits.
Wire it into cron:

```cron
*/30 * * * *  /usr/local/bin/parser-worker >> /var/log/parser-worker.log 2>&1
```

`/etc/parser-worker.env` can be sourced via a wrapper script, or the env
vars can be set inline in the crontab.

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
