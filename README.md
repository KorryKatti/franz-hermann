# franz-hermann

A Fluxer bot that shows F1 race info primarily from the [Jolpica API](https://api.jolpi.ca/ergast/f1/) (OpenF1 kept only as a cold-start fallback):
countdown/race-day status in its presence, driver and constructor standings, last-race results,
and driver lookups by car number.

Built with Go + [gofluxer](https://github.com/go-fluxer/gofluxer).

## Invite

[Invite Franz Hermann to your server](https://fluxer.com/oauth2/authorize?client_id=1566837767571247107&scope=bot&permissions=274878171200)

## Commands

All commands use the `>` prefix (e.g. `>who 1`).

| Command | What it does |
| --- | --- |
| `>ping` | Live latency to Fluxer + Jolpica, plus CPU/memory usage. Jolpica is probed at most once per 30 minutes; pings in between report the cached value |
| `>ds [title]` | Top 10 driver standings from the latest race session |
| `>ts [title]` | Top 10 constructor standings from the latest race session |
| `>who <driver number>` | Driver name, team, country, date of birth and Wikipedia link by car number (e.g. `>who 1`) |
| `>last [title]` | Top 10 results from the last race |
| `>next [title]` | Next session with location, circuit, country and countdown |
| `>track [round\|next]` | Circuit/location info for a round (defaults to next upcoming) |
| `>follow` | Subscribe this channel to race/sprint reminders 30 minutes before start |
| `>unfollow` | Unsubscribe this server from race/sprint reminders |
| `>substatus` | Tell whether this server is subscribed to race/sprint reminders, and in which channel (alias: `>subscription`) |
| `>help [title]` | Shows the help message |

The bot also sets its presence to the next race countdown, or `🏁 RACE DAY at …`
from 1 hour before lights out until 1 hour after the session ends, refreshed every
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

The image is built by GitHub Actions (`.github/workflows/docker.yml`) on every
push to `main` and pushed to GHCR as a public image:
`ghcr.io/korrykatti/franz-hermann:latest` (plus an immutable `:<commit-sha>`
tag for rollback). The VM never builds — its RAM is too small for that.

Deploy on the VM:

```bash
cd ~/folk_bots/franz-hermann
git pull                       # latest compose file
# optional backup first: docker compose cp franz-hermann:/app/data ./data-backup-$(date +%F-%H%M)
docker compose pull            # fetch the new prebuilt image
docker compose up -d           # recreate the container
docker compose logs -f         # follow logs
docker compose down            # stop (does NOT delete data — never use `down -v`)
```

The compose service:

- injects `BOT_TOKEN` from `.env` at runtime (`env_file`) — it's never baked into the image,
- persists runtime data (JSON caches + `f1bot.db` subscriptions) in the named
  volume `franz-data`, mounted at `/app/data`, so it survives image updates,
- restarts automatically (`unless-stopped`),
- exposes no ports — the bot only talks outbound (Gateway websocket + REST).

A failed Actions build changes nothing on the VM — the old image keeps running.
To roll back, set `image:` in `docker-compose.yml` to a previous
`ghcr.io/korrykatti/franz-hermann:<commit-sha>` tag and `docker compose up -d`.

The image is a multi-stage build (`golang:1.27-alpine` → `alpine:3.22`, ~28 MB)
and runs as a non-root user. To build it manually (e.g. to test a change before
pushing): `docker build -t franz-hermann:latest .`

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
| `data/race.json` | Race + Sprint sessions for the season | 12 hours (30 min while a race is underway) |
| `data/sessions.json` | All sessions for the season | 12 hours (30 min while a race is underway) |
| `data/drivers.json` | Drivers for the last race session | 72 hours |
| `data/standings.json` | Championship driver standings | 6 hours (30 min right after a race), keyed by round |
| `data/team_standings.json` | Championship constructor standings | 6 hours (30 min right after a race), keyed by round |
| `data/last-standings.json` | Last race session results | Until the next race (keyed by session) |

All API requests are bounded by a 10-second timeout, and command handlers run
off the gateway loop with panic recovery, so a slow API can't freeze the bot.

## Project layout

| File | Purpose |
| --- | --- |
| `main.go` | Bot, commands, Jolpica fetching/caching (OpenF1 cold-start fallback), ping/resource probes |
| `embeds.go` | Typed embed helpers (strips empty image/thumbnail URLs so embeds aren't rejected) |
| `Dockerfile` / `docker-compose.yml` | Container build + deployment |
| `data/` | Runtime JSON cache (see above) |
