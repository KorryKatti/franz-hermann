# franz-hermann

A Fluxer bot that shows F1 race info from the [OpenF1 API](https://api.openf1.org/):
countdown/race-day status in its presence, driver standings, last-race results,
and driver lookups by car number.

Built with Go + [gofluxer](https://github.com/go-fluxer/gofluxer).

## Invite

[Invite Franz Hermann to your server](https://fluxer.com/oauth2/authorize?client_id=1566837767571247107&scope=bot&permissions=274878171200)

## Commands

All commands use the `>` prefix (e.g. `>who 1`).

| Command | What it does |
| --- | --- |
| `>ping` | Live latency to Fluxer + OpenF1, plus CPU/memory usage. OpenF1 is probed at most once per 2 hours; pings in between report the cached value |
| `>ds [title]` | Top 10 driver standings from the latest race session |
| `>who <driver number>` | Driver name, team and country by car number (e.g. `>who 1`). Drivers without a headshot (e.g. reserve driver `#50`) just get no photo |
| `>last [title]` | Top 10 results from the last race |
| `>help [title]` | Shows the help message |

The bot also sets its presence to the next race countdown, or `🏁 RACE DAY at …`
from 1 hour before lights out until 12 hours after the start, refreshed every
5 minutes.

## Configuration

The only setting is the bot token, via `.env` in the project root:

```env
BOT_TOKEN=your-token-here
```

Never commit `.env` — it's gitignored and excluded from the Docker build context
(see `.dockerignore`). `.env.example` is a safe template to copy from.

## Running

### Docker (recommended)

```bash
docker compose up -d --build   # build + start detached
docker compose logs -f         # follow logs
docker compose down            # stop
```

The compose service:

- injects `BOT_TOKEN` from `.env` at runtime (`env_file`) — it's never baked into the image,
- persists the JSON cache in `./data` (mounted to `/app/data`),
- restarts automatically (`unless-stopped`),
- exposes no ports — the bot only talks outbound (Gateway websocket + REST).

The image is a multi-stage build (`golang:1.27-alpine` → `alpine:3.22`, ~28 MB)
and runs as a non-root user.

> If `./data` ends up owned by root (e.g. after running the bare binary as root),
> the container can't write its cache. Fix with:
> `sudo chown -R "$(id -u):$(id -g)" data/`

### Locally

Requires Go 1.27 (see `go.mod`):

```bash
cp .env.example .env   # then fill in BOT_TOKEN
go run .
```

## Data & caching

`data/` holds JSON caches written at runtime (gitignored, refetched on demand):

| File | Contents | Cache lifetime |
| --- | --- | --- |
| `data/race.json` | Race sessions for the season | 72 hours |
| `data/drivers.json` | Drivers for the last race session | 480 hours (20 days) |
| `data/standings.json` | Championship driver standings | 72 hours |
| `data/last-standings.json` | Last race session results | Until the next race (keyed by session) |

All OpenF1 requests are bounded by a 10-second timeout, and command handlers run
off the gateway loop with panic recovery, so a slow API can't freeze the bot.

## Project layout

| File | Purpose |
| --- | --- |
| `main.go` | Bot, commands, OpenF1 fetching/caching, ping/resource probes |
| `embeds.go` | Typed embed helpers (strips empty image/thumbnail URLs so embeds aren't rejected) |
| `Dockerfile` / `docker-compose.yml` | Container build + deployment |
| `data/` | Runtime JSON cache (see above) |
