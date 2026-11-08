# Franz Hermann — Updated Plan (v2, verified)

**Status:** Draft, verified against live APIs (Brazil GP, 2026-11-08) and current code. **Phases 1–3 implemented and locally tested 2026-11-08** (schedule, standings, results, drivers, ping — verified against Brazil results: NOR/VER/HAD, ANT 390 pts). **Phase 4 (live presence) cancelled:** operator will not buy an OpenF1 key for at least a year. OpenF1 remains only as a free cold-start fallback off-live. **Do not deploy production until follow-dedup test + volume backup complete (see §8).**

---

## 1. Goals / Non-goals

> **#1 TOP GOAL: Never get API rate-limited — ever.**
> The bot must run autonomously with zero manual monitoring. The operator cannot be awake 24 hours to watch for 429s, throttling, or a dead API. Every design choice in this plan (cache-first, TTLs, failure cooldowns, polling budgets) serves this goal first, freshness second. A bot that is slightly stale but *never* rate-limited beats a fresh bot that gets throttled and silently stops updating.
>
> This covers all upstreams: Jolpica (4/s burst, 500/hr sustained, *will decrease*), OpenF1 (429 on hammering / 401 live), and Fluxer gateway/REST (presence 5 per rolling 20s, websocket 600/min, REST 429 exits the process in gofluxer). No polling loop in the system may be able to burn budget during an outage or a live race.

**Goals**
- No command may fail because of OpenF1's live-session 401.
- Core commands work fully during and immediately after a race.
- Keep `>follow` reminder behavior identical for existing subscriptions.
- Preserve existing caches/subscriptions in the `franz-data` volume across migration.
- **Verified 2026-11-08 (Brazil GP):** the `>follow` reminder worked fine in production today — it fires 30 min *before* lights-out, before OpenF1's live gate begins, so the pre-race schedule fetch was unaffected. This confirms the follow timing path is not currently exposed to the 401; migration must preserve that property.

**Non-goals**
- Self-hosting Jolpica (hosted + local cache + stashed dump as disaster recovery only).
- Fastest-lap anything.
- Fixing gofluxer 429-exit / presence websocket race (separate `fluxer/plan.md` effort).
- Buying an OpenF1 key in this pass.

## 2. Verified facts this plan rests on

- OpenF1 401s globally during live sessions **and at least 27 min after the flag** (tested today). Unlock time unknown.
- Jolpica results for Brazil landed **< ~27 min after the flag** today; podium NOR/VER/HAD confirmed, standings math confirmed (ANT 380→390).
- Jolpica `/current/results` returns **round 1** — only `/current/last/results` is the last completed race.
- Jolpica `/current/driverstandings` labels the **upcoming round before its results land** (round 20, round-19 points, pre-race). Round-label alone is not a safe cache key.
- Jolpica schedule has **no `DateEnd`, no cancelled flag**. Race-day grace needs estimated durations.
- Jolpica drivers endpoint has **no constructor, no headshot**; totals are season/round entrants (36/32/28), not the 20 starters — results must be used to filter race starters.
- Jolpica rate limit: 4/s burst, 500/hr, *"will decrease."* CDN `max-age=600`. Docs ask for a custom User-Agent (not hard-enforced today — tested 200 with empty UA).
- Fluxer presence: **5 updates / rolling 20s, over-limit dropped without close.** Heartbeat 41.25s. Live presence at 2m10s is ~13% of the sustained limit — Fluxer is not a constraint.
- This checkout is **not production**: `.env` = test bot, no DB in `data/`, prod state is the `franz-data` volume elsewhere. Subscription count unverified from here.

## 3. Architecture

**Normalizer pattern:** commands keep consuming today's internal shapes (`Session`, `driverStandings`, driver info). A Jolpica adapter fills them; the existing OpenF1 code remains only as last rung. Commands never know the source.

**Fallback ladder (per helper):**
1. Fresh cache → return, no network.
2. Jolpica fetch → validate → write cache → return.
3. Jolpica fail/empty → stale cache.
4. OpenF1 → **only if no cache exists AND no session is live** (cold-start parachute).
5. Error to user.

**Anti-hammer:** every helper gets a failure cooldown (30 min normal, 15 min race window). Today only `getRaces` has one, conflated with its 72h TTL — the others would re-fetch every 5-min presence tick during an outage. This must land *with or before* any TTL shortening.

**Fetch hygiene:** set `User-Agent: FranzHermann/<version>` on all Jolpica requests; respect `limit` clamp (≤100); never cache empty/partial Jolpica responses.

### Rate-limit budget (enforcement of goal #1)

Every loop in the system must stay within budget *even during an outage*, with no human watching:

| Upstream | Hard limit (verified) | This bot's worst-case use | Guardrail |
|---|---|---|---|
| Jolpica | 4/s burst, 500/hr sustained (will decrease) | ~10 fetches/day normal, ~30 on race Sunday with §4 TTLs | Cache-first; no Jolpica poll <10 min (CDN max-age is 600s, faster polling cannot be fresher); per-helper failure cooldown 30m/15m so an outage cannot turn the 5-min presence ticker into a fetch loop |
| OpenF1 | 401 during live + 429 on hammering | Cold-start only (Phase 1–3); 2m10s poll in race window only if Phase 4 key is bought | Skip entirely while `raceUnderway`; in-memory last-good, never a file-cache loop |
| Fluxer presence | 5 / rolling 20s, dropped (no close) | 1 per 5 min normal; 1 per 2m10s live (Phase 4) | Well under limit by construction; over-limit degrades to stale status, never a crash |
| Fluxer REST | 429 → gofluxer `os.Exit(1)` | Command replies only, unchanged | Out of scope for this plan; separate wrapper effort (`fluxer/plan.md`) |

Rule of thumb: if a fetch fails, the *next* attempt waits for the cooldown — never for a human. Stale data served silently is always preferable to a retry storm.

## 4. Cache & TTL (revised)

| Data | Now | New target | Key |
|---|---|---|---|
| Schedule (merge `race.json`+`sessions.json` → one) | 72h / 120h | 12h normal, 30m race window | Season |
| Standings | 72h time-only | 6h normal, 30m for ~2h post-race | **Last completed round** (from `/last/results`), *not* current-round label |
| Last results | Infinite, session-keyed | Infinite, round-keyed; don't cache empty | Completed round |
| Drivers | 480h | 72h, 24h race week | Last completed round; filter starters via results |
| Ping probe | 2h | 30m, probe Jolpica | — |
| Live presence poll (Phase 4 only) | — | 2m10s, in-memory | Session |

Old-format cache files in the prod volume must be **ignored-and-rebuilt**, never crash the bot.

## 5. Per-command plan

| Command | Change | Phase |
|---|---|---|
| `>next` | Jolpica schedule; Sprint expanded from nested `Sprint` field | 1 |
| `>ping` | Probe Jolpica, relabel | 1 |
| Presence (non-live) | Unchanged UI/rotation; data from migrated helpers; grace window uses estimated session durations | 1 |
| `>ds` | Jolpica standings, completed-round key; **no** OpenF1 rung (401-prone, confusing points) | 2 |
| `>last` | Jolpica `/last/results`; no OpenF1 race (disproven today — Jolpica had results, OpenF1 401'd) | 2 |
| `>who` | Jolpica identity (number/code/name/nationality + constructor joined from results); **headshot dropped** (default) | 3 |
| `>help` | Text only if ping label changes | 1 |
| `>follow`/`>unfollow` | Logic + DB untouched (Interpretation B). Schedule source changes underneath — see §6 | 1, with extra testing |

## 6. `>follow` migration risk (load-bearing)

- Reminder checker shares `getRaces()` — it moves with Phase 1 by construction.
- Dedup compares stored `SessionKey` (OpenF1 ints) vs new round-based IDs. **New IDs won't match stored ones → each server may get one duplicate reminder post-migration.** Mitigation options: (a) accept one duplicate, (b) one-time ID mapping/backfill in the subscriptions table at deploy. Decide before Phase 1; test with a fake session on the test bot either way.
- Sprint expansion and estimated `DateEnd` directly affect reminder timing (30 min before start). Shadow-test a sprint weekend before prod.

## 7. Live presence — CANCELLED

Operator decision 2026-11-08: no OpenF1 key for at least a year, so no live timing source exists. Presence stays as-is (countdown / RACE DAY / rotating Next·Leader·Last P1), now Jolpica-backed. Revisit only if a free live source is found.

## 8. Rollout

- **Phase 0 — Shadow (one full race cycle, test bot):** fetch Jolpica alongside OpenF1, log per-command diffs (schedule times incl. Sprint, standings points incl. the pre-race round trap, drivers filtering, results landing time vs flag). Success = zero unexplained diffs + measured Jolpica lag.
- **Phase 1–3:** migrate schedule → standings/results → who, one phase per deploy, test bot first, **off-weekend only**.
- **Phase 4:** live presence, only with key + verified endpoints.
- **Before any prod deploy:** back up `franz-data` volume; keep previous image for rollback; deploy off-weekend; watch logs (presence ticks, reminder checker, command errors) for the first hour.

## 9. Open decisions (defaults in bold)

1. Headshot: **drop it** vs. keep tiny OpenF1 call for the photo.
2. Follow: **Interpretation B** (logic frozen, schedule migrates, accept/mitigate one duplicate) vs. A (freeze separate OpenF1 path).
3. Live presence: **defer until after Phases 1–3** vs. buy key now.

## 10. Unknowns Phase 0 must resolve

- Exact Jolpica results/standings landing lag across a few races (today: <27 min).
- When OpenF1 actually unlocks post-race (still 401 at +27 min today).
- Session-duration estimates vs. real `DateEnd` for the grace window (validate against OpenF1 off-live data).
- Stable round-based session IDs + follow-dedup behavior on the test bot.
