<img src="logo.png" alt="" width="96">

# Dispatcharr M3U Failover

A small proxy that gives a [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr) M3U account several provider URLs.

Dispatcharr stores one server URL per account. This proxy gives each account a local port and forwards every request to the first working URL on that account's list. It is one Python file with no dependencies.

## What it does

- **Fails over before playback starts.** Connection errors, timeouts, 403 and 5xx answers move on to the next URL.
- **Leaves account errors alone.** 401, 458 and 460 (connection limit) are passed straight through, because another URL would give the same answer.
- **Double-checks 404s.** A 404 or 511 on a video is checked on other URLs, because a mirror can fail for every title while the others still work.
- **Avoids hammering the provider.** Attempts per request are capped. If everything fails, the account pauses for a while, and the pause doubles if failures continue.
- **Sorts URLs every night.** The most reliable and fastest URLs move to the top of `upstreams.conf`. URLs are never removed.

Tested on Dispatcharr 0.33.0.

## Install

1. Copy `failover.py`, `upstreams.conf.example` and `docker-compose.example.yml` into a folder.
2. Rename `upstreams.conf.example` to `upstreams.conf` and add your URLs.
3. Add the service from `docker-compose.example.yml` to your Compose file, and set `network_mode` to match your setup.
4. Start the container.
5. In Dispatcharr, set each account's Server URL to `http://127.0.0.1:<port>`, using the port from `upstreams.conf`.

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
| `SORT_USER_AGENT` | `m3u-failover` | User agent for the nightly sort. Change it if a provider refuses these logins |

`GET /__drop_live` on an account's port closes its live streams at once, so clients reconnect right after a VPN exit change instead of waiting for a read timeout. Gluetun AutoSwitch calls it after every switch.

## Security

The proxy has no login of its own, so keep `BIND` on `127.0.0.1` unless you trust everything that can reach it.

Keep `upstreams.conf` out of Git. It is already in `.gitignore`.

## Release notes

See [Releases](https://github.com/Brxdy22/Dispatcharr-M3U-Failover/releases/latest).

## Licence

MIT
