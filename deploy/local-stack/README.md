# Local test stack

The whole stats pipeline in Docker, built from your local checkouts: MongoDB, the stats API
(fragg-3.0-api), the worker (this repo) and the stats site (fragg-stats). Use it to run a
parser, API or site change end to end before deploying anything.

| Service | URL | Built from |
|---|---|---|
| site | http://localhost:5175 | `SITE_DIR` (default `../../../../WebstormProjects/fragg-stats`) |
| api | http://localhost:3999 | `API_DIR` (default `../../../../WebstormProjects/fragg-3.0-api`) |
| mongo | `mongodb://localhost:27018`, database `fragg_local` | `mongo:7` |
| worker | runs once, then exits | this repo |

If your checkouts live elsewhere, put `API_DIR=` / `SITE_DIR=` in a `.env` file next to
`docker-compose.yml`.

## Run

```bash
cd deploy/local-stack
docker compose up -d --build mongo api site   # infrastructure and site
docker compose run --rm worker                # one pass; prints the JSON summary at the end
```

The worker defaults to **season 21, Recruit tier, regulation only**. Change it per run:

```bash
SEASON=20 TIER=Contender TYPES=regulation,playoff docker compose run --rm worker
TIER= TYPES=combine docker compose run --rm worker   # TIER= (empty) means every tier
```

| Variable | Default | Meaning |
|---|---|---|
| `SEASON` | `21` | CSC season |
| `TIER` | `Recruit` | CSC tier (Core matches the home team's tier); empty for every tier |
| `TYPES` | `regulation` | comma list of `regulation`, `playoff`, `combine`, `preseason`, `scrim` |
| `MAX_MATCHES_PER_RUN` | `10000` | cap per pass |
| `STATS_API_KEY` | `local-test-key` | shared by the API and the worker |

A second run skips maps already stored. To re-parse everything, set `reparse: true` in the
worker's `reparse.yml`, which lives in the `worker-data` volume:

```bash
docker compose run --rm --entrypoint sh worker -c "printf 'reparse: true\n' > /data/reparse.yml"
docker compose run --rm worker
```

## Look at the data

- Site: http://localhost:5175. Pick the season and mode in the header.
- API: `curl "localhost:3999/meta"`, `curl "localhost:3999/players?season=21&type=regulation"`,
  `curl "localhost:3999/matches?season=21&type=regulation"`.
- Mongo: `mongosh mongodb://localhost:27018/fragg_local` (collection `matches`).

## Rebuild after a code change

```bash
docker compose build api          # or site / worker
docker compose up -d api site
```

## Reset

```bash
docker compose down        # stop, keep the data
docker compose down -v     # stop and delete the Mongo and worker volumes
```
