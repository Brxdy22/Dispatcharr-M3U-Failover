#!/usr/bin/env python3
"""
M3U / Xtream Codes upstream failover proxy for Dispatcharr.

Dispatcharr stores a single server_url per M3U account. This proxy gives each
account a local port (e.g. http://127.0.0.1:8091) and forwards every request
(player_api.php, get.php, xmltv.php, /live, /movie, /series, ...) to the first
working URL from that account's list in upstreams.conf.

Failover happens only BEFORE any bytes reach the client:
  * connection refused / DNS failure / TLS error / timeout waiting for headers
  * HTTP status listed in FAILOVER_STATUSES (default 403, 5xx, Cloudflare 52x;
    403 because a CDN edge can block while other edges / mirror URLs work)
Answers such as 401/458/460 (connection limit) are passed straight through: they
come from the account, not the URL, so retrying elsewhere would only burn the
single connection slot. If every target fails, the last real answer is relayed.

404/511 on a playback path (RECHECK_STATUSES) are double-checked on up to
RECHECK_URLS other URLs first: a mirror can start answering 404 for every title
while the others still serve them. If another URL plays it, the 404 one is rested
for COOLDOWN_SECONDS. If they all agree, the 404 is passed through, never trips
the breaker, and is repeated locally for NEG_CACHE_SECONDS so a player's retry
storm doesn't reach the provider.

Up to MAX_ATTEMPTS URLs are tried per request. Within a URL, the next IP its
hostname resolves to (IPv4 first, at most IPS_PER_URL) is tried only when the
connection itself fails; an HTTP answer from one CDN edge counts for the whole
URL. The URL (and IP) that last worked is tried first next time; a failed URL
is tried last for COOLDOWN_SECONDS. Duplicate URLs in the config are ignored.

Protection against hammering the provider (and getting the VPN IP blocked):
  * 403 from MAX_403_URLS different hostnames stops the attempt loop: that looks
    like the provider blocking us, not a bad edge, so the 403 is passed through.
  * When a request exhausts its options (every target failed, or the 403 rule
    fired), the account is paused for BREAKER_SECONDS: requests get an
    immediate 503 + Retry-After without contacting the provider. Each further
    failure right after a pause doubles it, up to BREAKER_MAX_SECONDS. Any
    success, or editing the account's URLs in upstreams.conf, resets it.
  * A 403 on one live feed is that feed's problem, so it does not pause the account:
    the 403 is passed through and repeated locally for FEED_403_SECONDS. Only
    FEED_403_LIMIT different live feeds refused within FEED_403_SECONDS, with no live
    feed starting in between, count as a block and pause the account. 403s on API
    calls and VOD pause it straight away.

upstreams.conf is reloaded automatically when it changes (URL lists only;
adding or changing ports needs a restart).

Redirects on playback paths (provider -> CDN/video server) are followed here
instead of being handed to Dispatcharr, so every stream, seek and reconnect
passes through and shows up in the logs/status, and a dead video server is
retried via the next URL (which usually hands out a different server). HLS
playlists (.m3u8) are still handed over unchanged. FOLLOW_REDIRECTS=0 disables.

Nightly sort (SORT_AT, default 03:00 in the container's TZ; "off" disables): every
account's URLs get SORT_ROUNDS spaced XC logins straight to each URL, then
upstreams.conf is rewritten: most reliable first, then fastest, with the first
MAX_ATTEMPTS slots drawn from different domains. URLs are never removed; ones that
fail every login go to the bottom with a note, and an account whose URLs all fail
is left as is. Credentials are learned in memory from the requests Dispatcharr
sends through the proxy (never logged or written); an account not seen since the
proxy started is skipped. Run now: docker exec m3u-failover python /app/failover.py --sort-now

A 404 from player_api.php (API_RETRY_STATUSES) is tried on the next URL after
API_RETRY_DELAY seconds, up to API_RETRIES times: a provider can answer 404 to calls
that come too close together, which would fail a whole account refresh.

GET /__drop_live closes this account's live streams at once, so a client reconnects
straight after a VPN exit change instead of waiting out its read timeout.

Status: GET http://127.0.0.1:<port>/__status  (JSON, no credentials), including
the streams playing right now and which URL/IP each one uses. Readable table:
    docker exec m3u-failover python /app/failover.py --status
"""
import http.client
import http.server
import json
import logging
import os
import re
import socket
import ssl
import sys
import threading
import time
import datetime
import ipaddress
import random
import statistics
import tempfile
from urllib.parse import parse_qs, unquote, urljoin, urlsplit

CONFIG = os.environ.get("CONFIG", "/config/upstreams.conf")
BIND = os.environ.get("BIND", "127.0.0.1")
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "8"))
HEADER_TIMEOUT = float(os.environ.get("HEADER_TIMEOUT", "60"))
READ_TIMEOUT = float(os.environ.get("READ_TIMEOUT", "60"))
COOLDOWN = float(os.environ.get("COOLDOWN_SECONDS", "300"))
VERIFY_TLS = os.environ.get("VERIFY_TLS", "1") not in ("0", "false", "no")
CHUNK = 64 * 1024


def _parse_statuses(spec):
    out = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return out


FAILOVER_STATUSES = _parse_statuses(os.environ.get("FAILOVER_STATUSES", "403,500,502,503,504,520-530"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "4"))  # URLs per request
IPS_PER_URL = int(os.environ.get("IPS_PER_URL", "2"))
RECHECK_STATUSES = _parse_statuses(os.environ.get("RECHECK_STATUSES", "404,511"))
API_RETRY_STATUSES = _parse_statuses(os.environ.get("API_RETRY_STATUSES", "404"))
API_RETRIES = int(os.environ.get("API_RETRIES", "2"))
API_RETRY_DELAY = float(os.environ.get("API_RETRY_DELAY", "2"))
RECHECK_URLS = int(os.environ.get("RECHECK_URLS", "2"))  # extra URLs asked before trusting a 404
NEG_CACHE = float(os.environ.get("NEG_CACHE_SECONDS", "30"))
MAX_403_URLS = int(os.environ.get("MAX_403_URLS", "2"))
BREAKER = float(os.environ.get("BREAKER_SECONDS", "60"))
BREAKER_MAX = float(os.environ.get("BREAKER_MAX_SECONDS", "600"))
FEED_403_SECONDS = float(os.environ.get("FEED_403_SECONDS", "300"))
FEED_403_LIMIT = int(os.environ.get("FEED_403_LIMIT", "3"))
FOLLOW_REDIRECTS = os.environ.get("FOLLOW_REDIRECTS", "1") not in ("0", "false", "no")
MAX_REDIRECTS = 5
REDIRECT_STATUSES = {301, 302, 303, 307, 308}


SORT_AT = os.environ.get("SORT_AT", "03:00").strip()
SORT_ROUNDS = int(os.environ.get("SORT_ROUNDS", "5"))
SORT_GAP = float(os.environ.get("SORT_GAP_SECONDS", "4"))
SORT_MARK = "# [nightly-sort]"
CLOUDFLARE_NETS = [ipaddress.ip_network(n) for n in (
    "173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22", "141.101.64.0/18",
    "108.162.192.0/18", "190.93.240.0/20", "188.114.96.0/20", "197.234.240.0/22", "198.41.128.0/17",
    "162.158.0.0/15", "104.16.0.0/13", "104.24.0.0/14", "172.64.0.0/13", "131.0.72.0/22")]


class RedirectFailed(Exception):
    def __init__(self, msg, blocked=False):
        super().__init__(msg)
        self.blocked = blocked  # the mirror itself is unusable (e.g. Cloudflare block page)

# Redirect targets that mean "this mirror is blocked", never real video. Cloudflare sends
# blocked zones to its terms-of-service abuse page, which answers 200 with an ~11 KB stub.
BLOCKED_REDIRECT_HOSTS = [h.strip().lower() for h in os.environ.get(
    "BLOCKED_REDIRECT_HOSTS", "cloudflare-terms-of-service-abuse.com").split(",") if h.strip()]


def is_blocked_host(host):
    host = (host or "").lower().rstrip(".")
    return any(host == b or host.endswith("." + b) for b in BLOCKED_REDIRECT_HOSTS)

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te",
              "trailer", "trailers", "transfer-encoding", "upgrade", "proxy-connection", "host"}

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("m3u-failover")

_PATH_CREDS = re.compile(r"^/(live|movie|series|timeshift|hls|play)/([^/]+)/([^/]+)/", re.I)
_BARE_CREDS = re.compile(r"^/(?!\*\*\*/)(?!(?:live|movie|series|timeshift|hls|play)/)[^/]+/[^/]+/(?=\d+(?:\.\w+)?$)")
_QUERY_CREDS = re.compile(r"((?:username|password|token)=)[^&]*", re.I)


def redact(path):
    """Strip XC credentials from a request path before it is logged."""
    path = _PATH_CREDS.sub(lambda m: f"/{m.group(1)}/***/***/", path)
    path = _BARE_CREDS.sub("/***/***/", path)
    return _QUERY_CREDS.sub(r"\1***", path)


def host_of(url):
    u = urlsplit(url)
    return u.netloc or url


def resolve(url):
    """IPs for a URL's host, IPv4 first."""
    u = urlsplit(url)
    try:
        infos = socket.getaddrinfo(u.hostname, u.port or (443 if u.scheme == "https" else 80),
                                   type=socket.SOCK_STREAM)
    except OSError:
        return [None]  # connect will raise the DNS error and be logged
    v4 = [i[4][0] for i in infos if i[0] == socket.AF_INET]
    v6 = [i[4][0] for i in infos if i[0] == socket.AF_INET6]
    return list(dict.fromkeys(v4 + v6)) or [None]


class Account:
    """Upstream URLs for one M3U account; health is tracked per URL."""

    def __init__(self, name, port, urls):
        self.name = name
        self.port = port
        self.lock = threading.Lock()
        self.urls = []
        self.preferred = None  # (url, ip) that last worked
        self.bad_until = {}    # url -> timestamp
        self.last_error = {}   # url -> text
        self.paused_until = 0.0
        self.backoff = 0.0
        self.pause_reason = None
        self.creds = None      # (username, password) seen in Dispatcharr's requests; memory only
        self.last_sort = None  # summary of the last nightly sort
        self.missing = {}      # playback path -> (expires, status, reason): recent agreed 404s, refused feeds
        self.refused_feeds = {}  # live feed -> time it was last refused with a 403
        self.set_urls(urls)

    def learn_creds(self, path):
        """Remember the XC login from a request path (query or /movie/u/p/...). Never logged."""
        found = None
        if "?" in path:
            q = parse_qs(path.split("?", 1)[1])
            if q.get("username") and q.get("password"):
                found = (q["username"][0], q["password"][0])
        if not found:
            m = _PATH_CREDS.match(path)
            if m:
                found = (unquote(m.group(2)), unquote(m.group(3)))
        if found and found != self.creds:
            self.creds = found

    def set_urls(self, urls):
        urls = list(dict.fromkeys(u.rstrip("/") for u in urls))
        with self.lock:
            if urls != self.urls:
                log.info("[%s] upstreams (%d): %s", self.name, len(urls),
                         ", ".join(host_of(u) for u in urls) or "(none)")
                if self.paused_until:
                    log.info("[%s] URL list changed; clearing pause", self.name)
                self.paused_until, self.backoff = 0.0, 0.0
                # A reordered list is a new preference: start again from the top URL
                self.preferred = None
            self.urls = urls

    def order(self):
        """[(url, [ips...]), ...] for one request: preferred first, cooling URLs last."""
        with self.lock:
            urls = list(self.urls)
            preferred = self.preferred
            now = time.time()
            if preferred and preferred[0] in urls:
                urls.remove(preferred[0])
                urls.insert(0, preferred[0])
            healthy = [u for u in urls if self.bad_until.get(u, 0) <= now]
            cooling = sorted((u for u in urls if self.bad_until.get(u, 0) > now),
                             key=lambda u: self.bad_until[u])
        out = []
        for url in (healthy + cooling)[:MAX_ATTEMPTS]:  # cooling URLs are still a last resort
            ips = resolve(url)
            if preferred and preferred[0] == url and preferred[1] in ips:
                ips.remove(preferred[1])
                ips.insert(0, preferred[1])
            out.append((url, ips[:IPS_PER_URL]))
        return out

    def mark_bad(self, url, reason):
        with self.lock:
            self.bad_until[url] = time.time() + COOLDOWN
            self.last_error[url] = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {reason}"

    def mark_good(self, target):
        with self.lock:
            if self.preferred != target:
                log.info("[%s] now using %s", self.name, label(target))
            if self.backoff:
                log.info("[%s] provider reachable again; pause cleared", self.name)
            self.preferred = target
            self.bad_until.pop(target[0], None)
            self.paused_until, self.backoff = 0.0, 0.0

    def paused(self):
        """Seconds left in the current pause (0 = not paused)."""
        with self.lock:
            return max(0.0, self.paused_until - time.time())

    def trip(self, reason):
        """Every option failed: stop contacting the provider for a while."""
        with self.lock:
            now = time.time()
            if now < self.paused_until:
                return self.paused_until - now  # a concurrent request already tripped it
            self.backoff = min(BREAKER_MAX, self.backoff * 2) if self.backoff else BREAKER
            self.paused_until = now + self.backoff
            self.pause_reason = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {reason}"
            log.error("[%s] pausing all upstream requests for %ds: %s", self.name, self.backoff, reason)
            return self.backoff

    def refuse_feed(self, path, feed, reason):
        """One live feed got a 403: answer it locally for a while. True if this looks like a block."""
        with self.lock:
            now = time.time()
            self.missing[path] = (now + FEED_403_SECONDS, 403, reason or "")
            self.refused_feeds = {f: t for f, t in self.refused_feeds.items() if now - t < FEED_403_SECONDS}
            self.refused_feeds[feed] = now
            count = len(self.refused_feeds)
        log.warning("[%s] live %s refused; answering it locally for %ds (%d of %d feeds before pausing the account)",
                    self.name, feed, int(FEED_403_SECONDS), count, FEED_403_LIMIT)
        return count >= FEED_403_LIMIT

    def feed_played(self):
        """A live feed started, so the provider isn't refusing us as a whole."""
        with self.lock:
            self.refused_feeds.clear()

    def status(self):
        with self.lock:
            now = time.time()
            known = [u for u in self.urls if u in self.last_error]
            return {
                "account": self.name,
                "port": self.port,
                "preferred": label(self.preferred) if self.preferred else None,
                "paused_seconds": max(0, int(self.paused_until - now)),
                "next_pause_seconds": int(min(BREAKER_MAX, self.backoff * 2) if self.backoff else BREAKER),
                "refused_feeds": sorted(f for f, t in self.refused_feeds.items() if now - t < FEED_403_SECONDS),
                "pause_reason": self.pause_reason if self.paused_until > now else None,
                "upstreams": [host_of(u) for u in self.urls],
                "active_streams": active_for(self.name),
                "last_sort": self.last_sort,
                "login_known": self.creds is not None,
                "failures": [{
                    "target": host_of(u),
                    "cooling_down_seconds": max(0, int(self.bad_until.get(u, 0) - now)),
                    "last_error": self.last_error[u],
                } for u in known],
            }


_STREAM_PATH = re.compile(r"^/(movie|series|live|timeshift)/[^/]+/[^/]+/(.+?)(?:\?.*)?$", re.I)
_BARE_STREAM = re.compile(r"^/[^/]+/[^/]+/(\d+(?:\.\w+)?)$")
ACTIVE = {}
LIVE_SOCKS = {}  # entry id -> upstream socket of a live stream, for /__drop_live
ACTIVE_LOCK = threading.Lock()
_ids = iter(range(1, 1 << 62))


def stream_of(path):
    """('movie', '12345.mkv') for playback paths, None for API calls."""
    m = _STREAM_PATH.match(path)
    if m:
        kind, leaf = m.group(1).lower(), m.group(2)
        return kind, leaf.rsplit("/", 1)[-1] if kind != "timeshift" else leaf
    m = _BARE_STREAM.match(path.split("?", 1)[0])
    return ("live", m.group(1)) if m else None


def human_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def human_secs(sec):
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s_ = divmod(rem, 60)
    return f"{h}h{m:02d}m{s_:02d}s" if h else f"{m}m{s_:02d}s" if m else f"{s_}s"


def active_for(account_name):
    now = time.time()
    with ACTIVE_LOCK:
        rows = [dict(v) for v in ACTIVE.values() if v["account"] == account_name]
    for r in rows:
        r["running"] = human_secs(now - r.pop("started"))
        r["transferred"] = human_bytes(r.pop("bytes"))
        r.pop("account")
    return rows


def drop_live(account_name):
    """Close the upstream side of an account's live streams; each relay then ends normally."""
    with ACTIVE_LOCK:
        socks = [s for entry_id, s in LIVE_SOCKS.items()
                 if ACTIVE.get(entry_id, {}).get("account") == account_name]
    for sock in socks:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
    return len(socks)


def label(target):
    url, ip = target
    return f"{host_of(url)} ({ip})" if ip and ip != urlsplit(url).hostname else host_of(url)


def load_config(path):
    """Parse upstreams.conf -> {name: (port, [urls])}."""
    accounts, name, port, urls = {}, None, None, []

    def flush():
        if name is not None:
            if port is None:
                raise ValueError(f"[{name}] has no port = line")
            accounts[name] = (port, urls)

    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.split("#", 1)[0].strip() if not raw.lstrip().startswith(("http://", "https://")) \
                else raw.strip()
            if not line:
                continue
            if line.startswith("[") and line.endswith("]"):
                flush()
                name, port, urls = line[1:-1].strip(), None, []
            elif line.lower().startswith("port"):
                port = int(line.split("=", 1)[1])
            elif line.startswith(("http://", "https://")):
                if name is None:
                    raise ValueError(f"line {lineno}: URL before any [account] section")
                urls.append(line.split()[0])
            else:
                raise ValueError(f"line {lineno}: can't parse {line!r}")
    flush()
    return accounts


def make_handler(account):
    ssl_ctx = ssl.create_default_context()
    if not VERIFY_TLS:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "m3u-failover"
        sys_version = ""

        def log_message(self, fmt, *args):
            pass

        def do_GET(self):
            self._proxy()

        do_HEAD = do_POST = do_PUT = do_DELETE = do_OPTIONS = do_GET

        def _send_simple(self, code, payload, ctype="application/json"):
            data = payload.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)

        def _proxy(self):
            if self.path == "/__status":
                return self._send_simple(200, json.dumps(account.status(), indent=2))
            if self.path == "/__sort":
                started = start_sort("manual")
                return self._send_simple(202 if started else 409, json.dumps(
                    {"sort": "started" if started else "already running"}))
            if self.path == "/__drop_live":
                dropped = drop_live(account.name)
                log.info("[%s] dropped %d live stream(s) on request", account.name, dropped)
                return self._send_simple(200, json.dumps({"dropped": dropped}))
            account.learn_creds(self.path)

            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP}

            stream_req = stream_of(self.path) if self.command in ("GET", "HEAD") else None
            if stream_req and NEG_CACHE:
                hit = account.missing.get(self.path)
                if hit and hit[0] > time.time():
                    self.close_connection = True
                    return self._send_simple(hit[1], json.dumps({"error": f"HTTP {hit[1]} {hit[2]}",
                                                                 "cached": True}))
                if hit:
                    account.missing.pop(self.path, None)

            wait = account.paused()
            if wait:
                self.close_connection = True
                data = json.dumps({"error": "provider paused after repeated failures",
                                   "retry_after_seconds": int(wait) + 1}).encode()
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", str(int(wait) + 1))
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)
                return

            errors = []
            forbidden_hosts = set()
            api_retries = 0
            soft_failed = []  # URLs that said 404/511 for this title; rested only if another URL plays it
            targets = account.order()
            for n, target in enumerate(targets, 1):
                last = n == len(targets)
                base, ips = target
                u = urlsplit(base)
                conn_cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
                kwargs = {"context": ssl_ctx} if u.scheme == "https" else {}
                resp, conn_errors = None, []
                for ip in ips:  # another edge IP only when the connection itself fails
                    conn = conn_cls(u.hostname, u.port, timeout=CONNECT_TIMEOUT, **kwargs)
                    if ip:  # pin the edge IP; Host header and TLS SNI still use the hostname
                        conn._create_connection = lambda addr, timeout, src, ip=ip: \
                            socket.create_connection((ip, addr[1]), timeout, src)
                    try:
                        conn.connect()
                        sock = conn.sock  # http.client drops conn.sock once the response will close
                        sock.settimeout(HEADER_TIMEOUT)
                        conn.request(self.command, u.path.rstrip("/") + self.path, body=body,
                                     headers={**headers, "Host": u.netloc})
                        resp = conn.getresponse()
                        break
                    except (OSError, http.client.HTTPException, ssl.SSLError) as e:
                        conn.close()
                        conn_errors.append(f"{label((base, ip))}: {type(e).__name__}: {e}")
                if resp is None:
                    reason = "; ".join(conn_errors)
                    account.mark_bad(base, reason)
                    errors.extend(conn_errors)
                    log.warning("[%s] %s failed for %s %s", account.name, reason, self.command,
                                redact(self.path))
                    continue
                target = (base, ip)

                via = label(target)
                if FOLLOW_REDIRECTS and resp.status in REDIRECT_STATUSES and self.command in ("GET", "HEAD") \
                        and stream_of(self.path):
                    origin = f"{u.scheme}://{u.netloc}{u.path.rstrip('/')}{self.path}"
                    try:
                        conn, sock, resp, hops = self._follow(conn, sock, resp, origin, headers)
                    except RedirectFailed as e:
                        # The provider URL answered; its video server didn't. Ask the next target,
                        # which normally hands out a fresh (often different) server.
                        if e.blocked:  # this mirror can't serve video at all: rest it
                            account.mark_bad(base, f"redirects to {e}")
                        errors.append(f"{via} -> {e}")
                        log.warning("[%s] video server failed for %s %s (%s -> %s)", account.name,
                                    self.command, redact(self.path), via, e)
                        continue
                    if hops:
                        via = f"{via} -> {hops}"

                if resp.status in FAILOVER_STATUSES:
                    account.mark_bad(base, f"HTTP {resp.status}")
                    if resp.status == 403:
                        forbidden_hosts.add(u.hostname)
                        if len(forbidden_hosts) >= MAX_403_URLS:
                            last = True  # several URLs refuse us: a block, not a bad edge
                    if not last:
                        conn.close()
                        errors.append(f"{label(target)}: HTTP {resp.status}")
                        log.warning("[%s] %s returned %s for %s %s", account.name, label(target), resp.status,
                                    self.command, redact(self.path))
                        continue
                    # Nothing left to try: hand the client the provider's real answer.
                    log.error("[%s] giving up on %s %s; passing through HTTP %s (%s)",
                              account.name, self.command, redact(self.path), resp.status,
                              "; ".join(errors) or "no other targets")
                    # Every URL asked refused this one live feed: only a block if other feeds are refused too
                    one_feed = (resp.status == 403 and stream_req and stream_req[0] == "live"
                                and len(errors) == len(forbidden_hosts) - 1)
                    if not one_feed or account.refuse_feed(self.path, stream_req[1], resp.reason):
                        account.trip(f"HTTP {resp.status} from {label(target)}"
                                     + (f" after {len(errors)} other failure(s)" if errors else ""))
                elif (resp.status in API_RETRY_STATUSES and not last and api_retries < API_RETRIES
                      and self.path.startswith("/player_api.php")):
                    conn.close()
                    api_retries += 1
                    errors.append(f"{label(target)}: HTTP {resp.status}")
                    log.warning("[%s] %s returned %s for the API; trying the next URL in %gs",
                                account.name, label(target), resp.status, API_RETRY_DELAY)
                    time.sleep(API_RETRY_DELAY)
                    continue
                elif stream_req and resp.status in RECHECK_STATUSES:
                    if not last and len(soft_failed) < RECHECK_URLS:
                        conn.close()
                        soft_failed.append(base)
                        errors.append(f"{label(target)}: HTTP {resp.status}")
                        log.warning("[%s] %s returned %s for %s %s; checking another URL", account.name,
                                    label(target), resp.status, stream_req[0], stream_req[1])
                        continue
                    # Every URL asked agrees: the title really isn't there (or the account is refused).
                    if NEG_CACHE:
                        now = time.time()
                        for k in [k for k, v in account.missing.items() if v[0] <= now]:
                            account.missing.pop(k, None)
                        account.missing[self.path] = (now + NEG_CACHE, resp.status, resp.reason or "")
                    if soft_failed:
                        log.warning("[%s] %s %s: HTTP %s from %d URL(s); answering it locally for %ds",
                                    account.name, stream_req[0], stream_req[1], resp.status,
                                    len(soft_failed) + 1, int(NEG_CACHE))
                else:
                    for b in soft_failed:  # this URL plays a title those called missing: rest them
                        account.mark_bad(b, "HTTP 404/511 for a title another URL serves")
                    if errors:
                        log.info("[%s] failed over to %s after: %s", account.name, label(target),
                                 "; ".join(errors))
                    account.mark_good(target)
                    if stream_req and stream_req[0] == "live" and resp.status < 400:
                        account.feed_played()
                log.debug("[%s] %s %s -> %s %s", account.name, self.command, redact(self.path),
                          via, resp.status)
                self._relay(resp, conn, sock, via)
                return

            log.error("[%s] all upstreams failed for %s %s", account.name, self.command, redact(self.path))
            account.trip(f"all {len(errors)} target(s) unreachable")
            self._send_simple(502, json.dumps({"error": "all upstreams failed", "attempts": errors}))

        def _follow(self, conn, sock, resp, current, headers):
            """Follow provider redirects ourselves. Returns (conn, sock, resp, 'host -> host')."""
            hops = []
            for _ in range(MAX_REDIRECTS):
                loc = resp.getheader("Location") if resp.status in REDIRECT_STATUSES else None
                if not loc:
                    break
                nxt = urljoin(current, loc)
                nu = urlsplit(nxt)
                if is_blocked_host(nu.hostname):
                    resp.close()
                    conn.close()
                    raise RedirectFailed(f"{nu.netloc}: blocked by Cloudflare (abuse page, not video)",
                                         blocked=True)
                if nu.scheme not in ("http", "https") or nu.path.lower().endswith((".m3u8", ".m3u")):
                    break  # playlists resolve relative segment URLs: hand those to the client as-is
                method = "GET" if resp.status == 303 and self.command != "HEAD" else self.command
                resp.close()
                conn.close()
                conn_cls = http.client.HTTPSConnection if nu.scheme == "https" else http.client.HTTPConnection
                kwargs = {"context": ssl_ctx} if nu.scheme == "https" else {}
                conn = conn_cls(nu.hostname, nu.port, timeout=CONNECT_TIMEOUT, **kwargs)
                try:
                    conn.connect()
                    sock = conn.sock
                    sock.settimeout(HEADER_TIMEOUT)
                    conn.request(method, (nu.path or "/") + (f"?{nu.query}" if nu.query else ""),
                                 headers={**headers, "Host": nu.netloc})
                    resp = conn.getresponse()
                except (OSError, http.client.HTTPException, ssl.SSLError) as e:
                    conn.close()
                    raise RedirectFailed(f"{nu.netloc}: {type(e).__name__}: {e}")
                hops.append(nu.netloc)
                if resp.status in FAILOVER_STATUSES:
                    conn.close()
                    raise RedirectFailed(f"{nu.netloc}: HTTP {resp.status}")
                current = nxt
            return conn, sock, resp, " -> ".join(hops)

        def _relay(self, resp, conn, sock, via):
            stream = stream_of(self.path) if self.command == "GET" else None
            entry_id, outcome = None, "finished"
            if stream and 300 <= resp.status < 400:
                # Provider hands playback to another host; bytes won't pass through this proxy.
                loc = urlsplit(resp.getheader("Location") or "")
                log.info("[%s] > %s %s via %s -> handed to client: %s", account.name, stream[0], stream[1],
                         via, loc.netloc or "(same host)")
                stream = None
            elif stream and resp.status >= 400:
                # Account-level refusals (401/404/458/460...) pass straight through; log them so
                # they're visible here and not only in Dispatcharr's log.
                log.warning("[%s] x %s %s via %s -> HTTP %s %s", account.name, stream[0], stream[1], via,
                            resp.status, resp.reason or "")
                stream = None
            if stream:
                entry_id = next(_ids)
                rng = self.headers.get("Range")
                with ACTIVE_LOCK:
                    ACTIVE[entry_id] = {"account": account.name, "type": stream[0], "stream": stream[1],
                                        "via": via, "range": rng, "client": self.client_address[0],
                                        "started": time.time(), "bytes": 0}
                    if stream[0] == "live":
                        LIVE_SOCKS[entry_id] = sock
                log.info("[%s] > %s %s via %s%s", account.name, stream[0], stream[1], via,
                         f" ({rng})" if rng else "")
            try:
                sock.settimeout(READ_TIMEOUT)
                self.send_response_only(resp.status, resp.reason)
                has_length = False
                for k, v in resp.getheaders():
                    lk = k.lower()
                    if lk in HOP_BY_HOP:
                        continue
                    if lk == "content-length":
                        has_length = True
                    self.send_header(k, v)
                no_body = self.command == "HEAD" or resp.status in (204, 304) or 100 <= resp.status < 200
                if not has_length and not no_body:
                    # Unknown length (live TS, chunked JSON): stream until upstream EOF, then close.
                    self.send_header("Connection", "close")
                    self.close_connection = True
                self.end_headers()
                if no_body:
                    return
                while True:
                    chunk = resp.read1(CHUNK)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    if entry_id:
                        ACTIVE[entry_id]["bytes"] += len(chunk)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                outcome = "client stopped"
                self.close_connection = True  # client went away; dropping upstream frees the provider slot
            except (socket.timeout, OSError, http.client.HTTPException) as e:
                outcome = f"interrupted: {e}"
                log.warning("[%s] stream interrupted for %s: %s", account.name, redact(self.path), e)
                self.close_connection = True
            finally:
                resp.close()
                conn.close()
                if entry_id:
                    with ACTIVE_LOCK:
                        e = ACTIVE.pop(entry_id)
                        LIVE_SOCKS.pop(entry_id, None)
                    log.info("[%s] # %s %s via %s ended after %s, %s (%s)", account.name, e["type"],
                             e["stream"], e["via"], human_secs(time.time() - e["started"]),
                             human_bytes(e["bytes"]), outcome)

    return Handler


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def watch_config(accounts):
    last = None
    while True:
        try:
            mtime = os.stat(CONFIG).st_mtime
            if last is not None and mtime != last:
                cfg = load_config(CONFIG)
                for name, (port, urls) in cfg.items():
                    acct = accounts.get(name)
                    if acct is None or acct.port != port:
                        log.warning("[%s] new account or port change in config; restart the container to apply",
                                    name)
                        continue
                    acct.set_urls(urls)
            last = mtime
        except Exception as e:
            log.error("config reload failed (keeping previous config): %s", e)
        time.sleep(5)


# ---------------------------------------------------------------- nightly sort
ACCOUNTS = {}
_SORT_LOCK = threading.Lock()


def _domain(url):
    host = urlsplit(url).hostname or url
    return ".".join(host.split(".")[-2:])


def _kind(ips):
    try:
        if any(any(ipaddress.ip_address(ip) in n for n in CLOUDFLARE_NETS) for ip in ips):
            return "Cloudflare"
    except ValueError:
        pass
    return "direct" if ips else "?"


def _bench_once(url, creds):
    u = urlsplit(url)
    from urllib.parse import urlencode
    query = "/player_api.php?" + urlencode({"username": creds[0], "password": creds[1]})
    t0 = time.perf_counter()
    try:
        ip = socket.getaddrinfo(u.hostname, u.port or 80, socket.AF_INET, socket.SOCK_STREAM)[0][4][0]
        cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
        conn = cls(u.hostname, u.port, timeout=10)
        conn._create_connection = lambda addr, timeout, src, ip=ip: socket.create_connection((ip, addr[1]), timeout, src)
        conn.request("GET", u.path.rstrip("/") + query, headers={"Host": u.netloc, "User-Agent": SORT_UA})
        resp = conn.getresponse()
        body = resp.read()
        conn.close()
        try:
            ok = resp.status == 200 and json.loads(body).get("user_info", {}).get("auth") == 1
        except ValueError:
            ok = False
        return {"ok": ok, "status": resp.status, "total": time.perf_counter() - t0, "ip": ip}
    except Exception as e:
        return {"ok": False, "err": f"{type(e).__name__}: {str(e)[:70]}", "total": time.perf_counter() - t0}


SORT_UA = os.environ.get("SORT_USER_AGENT", "m3u-failover")


def _rank(urls, results):
    stats = []
    for url in urls:
        rs = results[url]
        ok = [r for r in rs if r["ok"]]
        stats.append({
            "url": url, "ok": len(ok), "n": len(rs),
            "median": statistics.median(r["total"] for r in ok) if ok else float("inf"),
            "kind": _kind({r["ip"] for r in rs if "ip" in r}),
            "errs": sorted({r.get("err") or f"HTTP {r.get('status')}" for r in rs if not r["ok"]}),
        })
    by_score = sorted(stats, key=lambda x: (-x["ok"] / max(1, x["n"]), x["median"]))
    order, used, pool = [], set(), [x for x in by_score if x["ok"]]
    while pool and len(order) < MAX_ATTEMPTS:  # first slots from different domains
        pick = next((x for x in pool if _domain(x["url"]) not in used), pool[0])
        order.append(pick)
        used.add(_domain(pick["url"]))
        pool.remove(pick)
    return order + [x for x in by_score if x not in order]


def _note(x):
    if not x["ok"]:
        return f"FAILED {x['n']}/{x['n']} logins: {'; '.join(x['errs'])[:90]}"
    rel = "" if x["ok"] == x["n"] else f", {x['ok']}/{x['n']} OK"
    return f"{x['kind']}, {x['median'] * 1000:.0f} ms median{rel}"


def _write_sorted(ranked_by_section, when):
    """Rewrite upstreams.conf with the new orders. Keeps the header, port lines, comments and
    commented-out URLs; URLs added while the sort ran are kept after the sorted ones."""
    out, section, urls, trailing, banner_done = [], None, [], [], False

    def flush():
        ranked = ranked_by_section.get(section)
        present = [u for u, _ in urls]
        if ranked:
            keep = [x for x in ranked if x["url"] in present]
            out.extend(f"{x['url']:<32} # {_note(x)}" for x in keep)
            done = {x["url"] for x in keep}
            out.extend(raw for u, raw in urls if u not in done)
        else:
            out.extend(raw for _, raw in urls)
        out.extend(trailing)

    for raw in open(CONFIG, encoding="utf-8").read().split("\n"):
        st = raw.strip()
        if st.startswith((SORT_MARK, "# Sorted ", "# the first four")):
            continue  # old banner; a fresh one is written below
        if st.startswith("[") and st.endswith("]"):
            if section is None and not banner_done:
                while out and not out[-1].strip():
                    out.pop()
                out += ["", f"{SORT_MARK} Auto-sorted at {SORT_AT} by the m3u-failover container (last run {when}):",
                        f"{SORT_MARK} most reliable, then fastest login; the first {MAX_ATTEMPTS} come from "
                        "different domains.",
                        f"{SORT_MARK} Add/remove URLs freely; the order is redone every night.", ""]
                banner_done = True
            if section is not None:
                flush()
            section, urls, trailing = st[1:-1].strip(), [], []
            out.append(raw)
        elif section is None:
            out.append(raw)
        elif st.startswith(("http://", "https://")):
            u = st.split()[0].rstrip("/")
            if u not in [x for x, _ in urls]:
                urls.append((u, raw))
        elif st.lower().startswith("port"):
            out.append(raw)
        else:
            trailing.append(raw)  # comments, commented-out URLs, blank lines
    if section is not None:
        flush()
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(out).rstrip("\n") + "\n")
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(CONFIG), prefix=".upstreams.")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.chmod(tmp, os.stat(CONFIG).st_mode & 0o777)
    os.replace(tmp, CONFIG)  # atomic; the config watcher reloads it within ~5s


def sort_all(reason):
    try:
        when = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        jobs, skipped = [], []
        for acct in ACCOUNTS.values():
            if acct.creds is None:
                skipped.append(acct.name)
                log.warning("[%s] nightly sort: login not seen since the proxy started; skipped", acct.name)
                continue
            jobs += [(acct, u) for u in acct.urls]
        if not jobs:
            log.warning("nightly sort (%s): nothing to test", reason)
            return
        log.info("nightly sort (%s): %d URL(s), %d round(s), %.0fs apart", reason, len(jobs), SORT_ROUNDS, SORT_GAP)
        results = {(a.name, u): [] for a, u in jobs}
        for _ in range(SORT_ROUNDS):
            random.shuffle(jobs)
            for acct, url in jobs:
                results[(acct.name, url)].append(_bench_once(url, acct.creds))
                time.sleep(SORT_GAP)
        ranked_by_section = {}
        for acct in ACCOUNTS.values():
            urls = [u for (n, u) in results if n == acct.name]
            if not urls:
                continue
            ranked = _rank(urls, {u: results[(acct.name, u)] for u in urls})
            if not any(x["ok"] for x in ranked):
                log.error("[%s] nightly sort: every URL failed (VPN/provider down?); order kept", acct.name)
                acct.last_sort = {"at": when, "result": "every URL failed; order kept"}
                continue
            ranked_by_section[acct.name] = ranked
            acct.last_sort = {"at": when, "result": "sorted", "order": [
                f"{host_of(x['url'])}: {_note(x)}" for x in ranked]}
            log.info("[%s] nightly sort: %s", acct.name,
                     " > ".join(f"{host_of(x['url'])} ({_note(x)})" for x in ranked))
        if ranked_by_section:
            _write_sorted(ranked_by_section, when)
            log.info("nightly sort: upstreams.conf rewritten")
    except Exception as e:
        log.error("nightly sort failed (upstreams.conf unchanged): %s: %s", type(e).__name__, e)
    finally:
        _SORT_LOCK.release()


def start_sort(reason):
    if not _SORT_LOCK.acquire(blocking=False):
        return False
    threading.Thread(target=sort_all, args=(reason,), daemon=True, name="sort").start()
    return True


def sort_scheduler():
    if SORT_AT.lower() in ("", "off", "0", "no"):
        log.info("nightly sort disabled")
        return
    hh, mm = (int(x) for x in SORT_AT.split(":"))
    while True:
        now = datetime.datetime.now()
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target += datetime.timedelta(days=1)  # naive local time: stays at HH:MM across DST
        log.info("next nightly sort: %s", target.strftime("%Y-%m-%d %H:%M %Z").strip())
        while datetime.datetime.now() < target:
            time.sleep(min(60, max(1, (target - datetime.datetime.now()).total_seconds())))
        if not start_sort("scheduled"):
            log.warning("nightly sort already running; skipped this slot")
        time.sleep(61)


def print_status():
    """Human-readable view of every account's /__status (for docker exec)."""
    import urllib.request
    for name, (port, _urls) in load_config(CONFIG).items():
        try:
            st = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/__status", timeout=5))
        except Exception as e:
            print(f"[{name}] port {port}: not reachable ({e})\n")
            continue
        print(f"[{name}]  port {port}  preferred: {st['preferred'] or '-'}"
              + (f"  PAUSED {st['paused_seconds']}s ({st['pause_reason']})" if st["paused_seconds"] else ""))
        streams = st.get("active_streams") or []
        if not streams:
            print("  no active streams")
        for a in streams:
            print(f"  > {a['type']:<9} {a['stream']:<22} via {a['via']:<38} {a['running']:>9}  "
                  f"{a['transferred']:>9}" + (f"  {a['range']}" if a.get("range") else ""))
        for f in st.get("failures") or []:
            if f["cooling_down_seconds"]:
                print(f"  ! {f['target']} skipped for {f['cooling_down_seconds']}s: {f['last_error']}")
        ls = st.get("last_sort")
        print(f"  last sort: {ls['at']} - {ls['result']}" if ls else
              f"  last sort: none yet{'' if st.get('login_known') else ' (login not seen yet; would be skipped)'}")
        print()


def trigger_sort():
    import urllib.request
    port = next(iter(load_config(CONFIG).values()))[0]
    try:
        r = urllib.request.urlopen(f"http://127.0.0.1:{port}/__sort", timeout=5)
        print("sort started; follow it with: docker logs -f m3u-failover")
    except urllib.error.HTTPError as e:
        print("a sort is already running" if e.code == 409 else f"failed: {e}")


def main():
    if "--status" in sys.argv:
        return print_status()
    if "--sort-now" in sys.argv:
        return trigger_sort()
    cfg = load_config(CONFIG)
    if not cfg:
        log.error("no accounts in %s", CONFIG)
        sys.exit(1)
    accounts = {}
    for name, (port, urls) in cfg.items():
        acct = Account(name, port, urls)
        accounts[name] = acct
        ACCOUNTS[name] = acct
        srv = Server((BIND, port), make_handler(acct))
        threading.Thread(target=srv.serve_forever, daemon=True, name=f"srv-{name}").start()
        log.info("[%s] listening on %s:%d", name, BIND, port)
    threading.Thread(target=sort_scheduler, daemon=True, name="sort-scheduler").start()
    watch_config(accounts)


if __name__ == "__main__":
    main()
