<img src="logo.png" alt="" width="96">

# Dispatcharr M3U Failover

A small proxy that gives a [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr) M3U account several provider URLs.

Dispatcharr stores one server URL per account. This proxy gives each account a local port and forwards every request to the first working URL on that account's list. It is one Python file with no dependencies.

Docker image: [`brxdy22/m3u-failover`](https://hub.docker.com/r/brxdy22/m3u-failover) (amd64 and arm64).

## What it does

- **Fails over before playback starts.** Connection errors, timeouts, 403 and 5xx answers move on to the next URL.
- **Leaves account errors alone.** 401, 458 and 460 (connection limit) are passed straight through, because another URL would give the same answer.
- **Double-checks 404s.** A 404 or 511 on a video is checked on other URLs, because a mirror can fail for every title while the others still work.
- **Avoids hammering the provider.** Attempts per request are capped. If everything fails, the account pauses for a while, and the pause doubles if failures continue.
- **Tells a refused VPN exit from a missing title.** When every URL answers 404 for a movie or episode, it asks a title that played before for one byte. If that is refused too, [Gluetun AutoSwitch](https://github.com/Brxdy22/Dispatcharr-Gluetun-AutoSwitch-Plugin) 1.7 or later moves the exit, and the same request plays on the new exit.
- **Sorts URLs every night.** The most reliable and fastest URLs move to the top of `upstreams.conf`. URLs are never removed.
- **Edits from Dispatcharr.** With the [VOD Failover](https://github.com/Brxdy22/Dispatcharr-VOD-Failover-Plugin) plugin 1.5, you can see and change each account's URLs on the plugin page.

Tested on Dispatcharr 0.33.0.

Works with [VOD Failover](https://github.com/Brxdy22/Dispatcharr-VOD-Failover-Plugin) and [Gluetun AutoSwitch](https://github.com/Brxdy22/Dispatcharr-Gluetun-AutoSwitch-Plugin). All three together cover provider URL failover, account failover for VOD, and VPN exit switching.

See [Dispatcharr Failover Stack](https://github.com/Brxdy22/Dispatcharr-Failover-Stack) to install all three side by side.

## Install

1. Make a folder for the config, and copy `upstreams.conf.example` into it as `upstreams.conf`. Add your URLs.
2. Add the service from [`docker-compose.example.yml`](docker-compose.example.yml) to your Compose file. Set `network_mode` to match your setup, and the volume to your config folder.
3. Start the container: `docker compose up -d m3u-failover`.
4. In Dispatcharr, set each account's Server URL to `http://127.0.0.1:<port>`, using the port from `upstreams.conf`.

The image is pinned by version, for example `brxdy22/m3u-failover:1.3`. To update, change the tag to the [latest release](https://github.com/Brxdy22/Dispatcharr-M3U-Failover/releases/latest) and start the container again.

To run it without the image, mount `failover.py` into `python:3.12-alpine` at `/app/failover.py` and use `command: ["python", "-u", "/app/failover.py"]`.

Usernames and passwords stay in Dispatcharr. The proxy holds them in memory for the nightly sort, and never logs or saves them.

## Check it

```sh
# Table of accounts, URLs and current streams
docker exec m3u-failover python /app/failover.py --status

# Run the sort now
docker exec m3u-failover python /app/failover.py --sort-now
```

An account is skipped by the sort until Dispatcharr has sent a request through the proxy since it started.

## Settings

Set these as environment variables. The full list is at the top of `failover.py`.

| Variable | Default | Meaning |
|---|---|---|
| `CONNECT_TIMEOUT` | `8` | Seconds to connect before trying the next URL |
| `HEADER_TIMEOUT` | `60` | Seconds to wait for headers before trying the next URL |
| `COOLDOWN_SECONDS` | `300` | How long a failed URL is tried last |
| `FAILOVER_STATUSES` | `403,500,502,503,504,520-530` | Answers that move on to the next URL |
| `MAX_ATTEMPTS` | `4` | Most URLs tried per request |
| `BREAKER_SECONDS` | `60` | First pause after everything fails |
| `API_RETRY_STATUSES` | `404` | `player_api.php` answers that are tried again on the next URL |
| `API_RETRIES` | `2` | Most extra URLs tried for one API call |
| `API_RETRY_DELAY` | `2` | Seconds to wait before that retry |
| `SORT_AT` | `03:00` | Time of the nightly sort, or `off` |
| `BIND` | `127.0.0.1` | Address the proxy listens on |
| `EXIT_HOOK_URL` | `http://127.0.0.1:9733/vod-blocked` | Where to ask Gluetun AutoSwitch to move a refused exit, or `off` |
| `REQUEST_DEADLINE_SECONDS` | `9` | Most time a VOD request may take, including an exit move. Keep it below Dispatcharr's 10 s read timeout |
| `CANARIES` | `2` | Titles that played before, asked to tell a refused exit from a missing title |
| `CANARY_FILE` | `vod_canaries.json` next to `upstreams.conf` | Where those titles are kept: type and file name only, never the login |
| `SORT_USER_AGENT` | `m3u-failover` | User agent for the nightly sort. Change it if a provider refuses these logins |

`GET /__drop_live` on an account's port closes its live streams at once, so clients reconnect right after a VPN exit change instead of waiting for a read timeout. Gluetun AutoSwitch calls it after every switch.

`GET /__before_move?hold=N` on an account's port closes all its streams, VOD included, and holds new requests for up to 6 seconds while the exit moves. `/__drop_live` ends the hold early. Gluetun AutoSwitch 1.8.2 calls it just before every switch.

`GET /__status` shows `stuck_slot_seconds` for an account whose provider keeps answering with a connection limit (458 or 460) while nothing is streaming. During that time the proxy answers repeat requests itself, so the provider sees about one try every 15 seconds.

`POST /__urls` with `{"urls": [...]}` on an account's port replaces that account's URLs in `upstreams.conf` and uses them at once. Notes on URLs that stay are kept. It is accepted only from loopback, and only base URLs (`scheme://host[:port]`) without logins. The [VOD Failover](https://github.com/Brxdy22/Dispatcharr-VOD-Failover-Plugin) plugin 1.5 uses it to show and edit the lists from Dispatcharr. `GET /__status` lists the full URLs under `urls`.

`GET /__vod_check` asks the remembered titles for one byte and answers whether the exit is refused. Gluetun AutoSwitch calls it in the background while nobody is watching. Add `?forget=1` to also forget titles that were refused.

## Security

The proxy has no login of its own, so keep `BIND` on `127.0.0.1` unless you trust everything that can reach it.

Keep `upstreams.conf` out of Git. It is already in `.gitignore`.

## More by Brxdy22

- [VOD Failover](https://github.com/Brxdy22/Dispatcharr-VOD-Failover-Plugin): Retries a failed VOD stream on the next provider account that carries the title.
- [Gluetun AutoSwitch](https://github.com/Brxdy22/Dispatcharr-Gluetun-AutoSwitch-Plugin): Moves Gluetun to another VPN exit when streams slow down, VOD is refused or speed tests fail.
- [Clean Titles](https://github.com/Brxdy22/Dispatcharr-Clean-Titles-Plugin): Renames VOD movies and series to their official TMDB titles and tidies provider categories.
- [Teamarr Assistant](https://github.com/Brxdy22/Dispatcharr-Teamarr-Assistant-Plugin): Tags regional sports networks with their teams and starts Teamarr generations when new streams appear.

## Release notes

See [Releases](https://github.com/Brxdy22/Dispatcharr-M3U-Failover/releases/latest).

## Licence

MIT
