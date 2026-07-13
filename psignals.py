#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "yfinance==1.5.1",
#     "pandas==3.0.3",
#     "numpy==2.4.6",   # explicit + pinned; 2.4.6 installs on both 3.11 (Pi) and 3.13 (Mac)
#     "pyyaml==6.0.2",
# ]
# ///

import argparse
import contextlib
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    import fcntl  # Unix (Pi + macOS); locking degrades to a no-op if absent
except ImportError:
    fcntl = None

import pandas as pd
import yaml

CRYPTO_MAP = {"BTC": "BTC-USD", "ETH": "ETH-USD"}
BENCHMARK = "SPY"
LOOKBACK = "2y"
# Cache location precedence (portable across the Pi under cron and the Mac dev box):
#   PSIGNALS_CACHE (explicit override) -> XDG_CACHE_HOME/psignals -> ~/.cache/psignals
# Persistent on purpose: deterministic under cron (no XDG_RUNTIME_DIR dependency)
# and survives reboots. Each host keeps its own cache.
CACHE_DIR = Path(
    os.environ.get("PSIGNALS_CACHE")
    or Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "psignals"
)
HISTORY_TTL = 3600  # 60 min; closed daily bars are immutable
MIN_HISTORY_ROWS = 60  # a series shorter than this is invalid: never cache/serve it
DEBUG = False  # set by --debug; surfaces yfinance errors and fetch attempts


@contextlib.contextmanager
def run_lock(enabled=True, timeout=120):
    """Serialize invocations that share this host's cache and yfinance tz DB.

    An advisory fcntl lock tied to the open fd: the OS releases it automatically
    if the process dies, so there is no stale-lock cleanup (unlike a PID file).
    A blocked run waits up to `timeout` seconds, then exits rather than racing.
    No-op when locking is disabled or fcntl is unavailable (non-Unix).
    """
    if not enabled or fcntl is None:
        yield
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = CACHE_DIR / "psignals.lock"
    fh = open(lock_path, "w")
    try:
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise SystemExit(
                        f"psignals: another run holds {lock_path} "
                        f"(waited {timeout}s) — exiting to avoid a data race."
                    )
                time.sleep(0.5)
        yield
    finally:
        try:
            fcntl.flock(fh, fcntl.LOCK_UN)
        finally:
            fh.close()
PCTILE_ADD_STRONG = 5
PCTILE_ADD_WATCH = 10
PCTILE_TRIM = 90
RSI_ADD = 40
RSI_TRIM = 70
MIN_200D_BUFFER_PCT = 5.0
RS21_FLOOR_PCT = -15.0
MARKET_STRESS_Z20 = -2.0


def load_config(path):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    positions = cfg.get("positions", {})
    constraints = cfg.get("constraints", {})
    watchlist = cfg.get("watchlist", {}) or {}
    return positions, constraints, watchlist


def _cache_path(ticker, period):
    safe = ticker.replace("/", "_").replace("\\", "_")
    return CACHE_DIR / f"{safe}__{period}.pkl"


def _yf_prepare(yf):
    """Quiet yfinance's logging and pin its tz cache to a stable path.

    The tz/cookie cache is a SQLite DB; on the default path it intermittently
    throws 'database is locked' (e.g. on the Pi), which silently drops tickers
    from a batch. Pinning it under CACHE_DIR + serializing runs (run_lock) avoids
    that. Idempotent, so both fetch paths can call it.
    """
    import logging

    logging.getLogger("yfinance").setLevel(logging.DEBUG if DEBUG else logging.CRITICAL)
    try:
        yf.set_tz_cache_location(str(CACHE_DIR / "yf-tz"))
    except Exception:
        pass


def _dbg(msg):
    if DEBUG:
        print(f"[debug] {msg}", file=sys.stderr)


def _download_daily(yf, tickers, period, retries=2):
    """yf.download on the daily endpoint with retry/backoff for transient locks.

    threads=False serializes the download: yfinance's default multi-threaded
    fetch has all worker threads writing the same tz-cache SQLite DB at once,
    which collides on slower I/O (the Pi's SD card) as 'database is locked' and
    silently drops a ticker. Single-threaded fetch trades a little speed for
    correctness; on a daily job the cost is negligible.
    """
    data = None
    for attempt in range(retries + 1):
        try:
            data = yf.download(
                tickers, period=period, interval="1d",
                auto_adjust=True, progress=False, group_by="ticker",
                threads=False,
            )
            if data is not None and not data.empty:
                return data
            _dbg(f"download {tickers} attempt {attempt}: empty frame")
        except Exception as e:
            _dbg(f"download {tickers} attempt {attempt}: {type(e).__name__}: {e}")
        time.sleep(0.5 * (attempt + 1))
    return data


def _ticker_history_close(yf, ticker, period):
    """Last-resort single-ticker fetch via a different yfinance code path."""
    try:
        df = yf.Ticker(ticker).history(period=period, auto_adjust=True)
        s = df["Close"].dropna()
        return s if len(s) else None
    except Exception as e:
        _dbg(f"Ticker.history({ticker}): {type(e).__name__}: {e}")
        return None


def _extract_close(data, ticker, multi):
    """Pull a clean (NaN-dropped) close Series for one ticker, or None."""
    if data is None:
        return None
    try:
        s = (data[ticker]["Close"] if multi else data["Close"]).dropna()
    except (KeyError, TypeError, AttributeError):
        return None
    return s if len(s) else None


def fetch_history(tickers, period=LOOKBACK, ttl=HISTORY_TTL, use_cache=True):
    """Daily close history per ticker.

    Closed daily bars never change, so each ticker's 2y series is cached to disk
    with a 60-min TTL. Only stale/missing tickers hit the network. The batch is
    retried, and any ticker that drops from an otherwise-successful batch (the
    SPY 'database is locked' symptom) is retried individually so the benchmark
    isn't silently lost. Pass use_cache=False to force a full refresh.
    """
    import yfinance as yf

    _yf_prepare(yf)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    now = time.time()
    out, stale = {}, []

    def _valid(s):
        return s is not None and len(s) >= MIN_HISTORY_ROWS

    for t in tickers:
        cp = _cache_path(t, period)
        if use_cache and cp.exists() and now - cp.stat().st_mtime < ttl:
            try:
                s = pd.read_pickle(cp)
                if _valid(s):
                    out[t] = s
                    continue
                _dbg(f"{t}: cached series too short ({len(s)} rows), ignoring + refetching")
            except Exception:
                pass  # corrupt cache -> refetch
        stale.append(t)

    def _store(t, close):
        """Cache + keep only valid-length series; never persist a short one."""
        if not _valid(close):
            _dbg(f"{t}: fetched series too short ({0 if close is None else len(close)} rows), skipping")
            return False
        try:
            close.to_pickle(_cache_path(t, period))
        except Exception:
            pass  # cache write is best-effort
        out[t] = close
        return True

    if stale:
        data = _download_daily(yf, stale, period)
        for t in stale:
            _store(t, _extract_close(data, t, len(stale) > 1))
        # per-ticker retry for stragglers, then a different-code-path fallback
        for t in [t for t in stale if t not in out]:
            if _store(t, _extract_close(_download_daily(yf, [t], period), t, False)):
                continue
            if _store(t, _ticker_history_close(yf, t, period)):
                continue
            _dbg(f"{t}: unrecoverable, dropped from history")

    return dict(out)


def _naive_day(ts):
    """Calendar-day Timestamp with tz stripped, for tz-safe comparison.

    yfinance index tz-awareness varies by version/platform; the fast_info
    fallback also yields a tz-naive today(). Comparing naive vs aware raises,
    so reduce both sides to a bare normalized day before comparing.
    """
    ts = pd.Timestamp(ts)
    if ts.tz is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def _splice(close, last, last_dt):
    """Overwrite the current bar or append a newer one; tz-robust, ignore stale."""
    anchor = _naive_day(close.index[-1])
    day = _naive_day(last_dt)
    if day == anchor:
        close.iloc[-1] = last
    elif day > anchor:
        idx_tz = getattr(close.index, "tz", None)
        label = pd.Timestamp(day)
        if idx_tz is not None:  # match the series' tz so the index stays uniform
            label = label.tz_localize(idx_tz)
        close.loc[label] = last


def apply_live_prices(history, quote_period="5d", retries=2):
    """Refresh each series' latest bar with a fresh (uncached) quote.

    Uses Yahoo's DAILY endpoint -- the same one fetch_history uses and which is
    reliable for crypto -- re-fetched uncached so intraday reruns pick up the
    updated forming-bar close. The 1-minute intraday endpoint was dropped: it is
    separately rate-limited and returns empty for crypto in large batches.
    Freshness = Yahoo quote delay (~15 min equities, near-real-time crypto).

    Hardening: retry the batch, pin yfinance's tz cache to a stable path (avoids
    the 'database is locked' SQLite error), and fall back to a per-ticker
    fast_info quote for any straggler. Returns tickers left on cached last close.
    """
    import yfinance as yf

    _yf_prepare(yf)
    if not history:
        return []
    multi = len(history) > 1
    data = _download_daily(yf, list(history), quote_period, retries=retries)

    missed = []
    for t, close in history.items():
        last = last_dt = None
        s = _extract_close(data, t, multi)
        if s is not None:
            v = float(s.iloc[-1])
            if v == v:  # not NaN
                last, last_dt = v, s.index[-1].normalize()
        if last is None:  # per-ticker fallback: lightweight quote endpoint
            try:
                v = float(yf.Ticker(t).fast_info.last_price)
                if v == v:
                    last, last_dt = v, pd.Timestamp(date.today())
            except Exception:
                pass
        if last is None:
            missed.append(t)
            continue
        _splice(close, last, last_dt)
    return missed


def rsi(close, n=14):
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    rs = gain / loss
    return 100 - 100 / (1 + rs)


def compute_indicators(close):
    ind = {"price": float(close.iloc[-1])}
    for n in (20, 50, 200):
        if len(close) < n + 5:
            ind[f"dist{n}"] = None
            ind[f"pctile{n}"] = None
            continue
        sma = close.rolling(n).mean()
        dist = close / sma - 1
        ind[f"sma{n}"] = float(sma.iloc[-1])
        ind[f"dist{n}"] = float(dist.iloc[-1]) * 100
        ind[f"pctile{n}"] = float(dist.rank(pct=True).iloc[-1]) * 100
    if len(close) >= 220:
        sma200 = close.rolling(200).mean()
        ind["slope200"] = float(sma200.iloc[-1] / sma200.iloc[-21] - 1) * 100
    else:
        ind["slope200"] = None
    ind["rsi14"] = float(rsi(close).iloc[-1])
    ind["vol20"] = float(close.pct_change().rolling(20).std().iloc[-1]) * 100
    if len(close) >= 22:
        ind["ret21"] = float(close.iloc[-1] / close.iloc[-22] - 1) * 100
    else:
        ind["ret21"] = None
    if len(close) >= 25:
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        ind["z20"] = float((close.iloc[-1] - sma20.iloc[-1]) / std20.iloc[-1])
    else:
        ind["z20"] = None
    return ind


def parse_earnings_date(value):
    if not isinstance(value, str):
        return value
    for fmt in ("%Y-%m-%d", "%b-%d-%Y", "%d-%b-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unparseable earnings date: {value!r}")


def in_blackout(earnings, blackout_days):
    if not earnings:
        return False, None
    if not isinstance(earnings, (list, tuple)):
        earnings = [earnings]
    today = date.today()
    upcoming = sorted(d for d in map(parse_earnings_date, earnings) if d >= today)
    if not upcoming:
        return False, None
    days = (upcoming[0] - today).days
    return days <= blackout_days, days


def evaluate(name, pos, ind, constraints, watch=False):
    flags = []
    blackout_days = constraints.get("earnings_blackout_days", 5)
    blackout, days_to = in_blackout(pos.get("earnings"), blackout_days)

    if blackout:
        flags.append(
            {"ticker": name, "signal": "NOTE", "reason": f"earnings in {days_to}d, flags suppressed"}
        )
        return flags

    d20, p20 = ind.get("dist20"), ind.get("pctile20")
    d200 = ind.get("dist200")
    slope = ind.get("slope200")
    r = ind.get("rsi14")
    sma20, sma50 = ind.get("sma20"), ind.get("sma50")
    vol20 = ind.get("vol20")
    rs21 = ind.get("rs21")

    trend_ok = d200 is not None and d200 > 0 and (slope is None or slope > 0)
    sma200 = ind.get("sma200")

    pullback_eligible = pos.get("entry_interest") if watch else pos.get("add_zone")
    if pullback_eligible and p20 is not None and trend_ok:
        if p20 <= PCTILE_ADD_STRONG and r < RSI_ADD:
            tier = "ENTRY*" if watch else "ADD*"
        elif p20 <= PCTILE_ADD_WATCH and r < RSI_ADD + 5:
            tier = "ENTRY" if watch else "ADD"
        else:
            tier = None
        if tier:
            caveats = []
            buffer_pct = MIN_200D_BUFFER_PCT
            if vol20 is not None:
                buffer_pct = max(MIN_200D_BUFFER_PCT, vol20 * 2.2)
            if d200 < buffer_pct:
                caveats.append(f"200d buffer thin ({d200:+.1f}% < {buffer_pct:.0f}%)")
            if sma20 is not None and sma50 is not None and sma20 < sma50:
                caveats.append("20d<50d stack broken")
            if rs21 is not None and rs21 < RS21_FLOOR_PCT:
                caveats.append("idiosyncratic weakness vs SPY")
            if watch and sma50 is not None and sma200 is not None and sma50 < sma200:
                caveats.append("50d<200d, trend not established")
            if caveats:
                tier = "ENTRY?" if watch else "ADD?"
            rs_txt = f", RS21 {rs21:+.0f}%" if rs21 is not None else ""
            reason = f"{d20:+.1f}% v20d (p{p20:.0f}) RSI {r:.0f}{rs_txt}"
            if caveats:
                reason += " | " + "; ".join(caveats)
            flags.append({"ticker": name, "signal": tier, "reason": reason, "ind": ind})

    if pos.get("trim_zone") and p20 is not None:
        if p20 >= PCTILE_TRIM and r > RSI_TRIM:
            if constraints.get("_market_stress"):
                pass
            else:
                flags.append(
                    {
                        "ticker": name,
                        "signal": "TRIM",
                        "reason": f"{d20:+.1f}% v20d (p{p20:.0f}) RSI {r:.0f}",
                        "ind": ind,
                    }
                )

    if (
        d200 is not None
        and d200 < 0
        and slope is not None
        and slope < 0
        and (pos.get("role") == "tactical" or pos.get("regime_watch"))
    ):
        rs_txt = f", RS21 {rs21:+.0f}%" if rs21 is not None else ""
        flags.append(
            {
                "ticker": name,
                "signal": "REGIME",
                "reason": f"{d200:+.1f}% v200d, 200d slope {slope:+.1f}%/mo{rs_txt}",
                "ind": ind,
            }
        )
    return flags


def group_weights(positions, constraints):
    lines = []
    groups = {}
    for name, pos in positions.items():
        g = pos.get("group")
        if g:
            groups.setdefault(g, 0.0)
            groups[g] += pos.get("target_weight", 0.0)
    cap = constraints.get("btc_complex_max_pct")
    for g, w in groups.items():
        note = ""
        if g == "btc_complex" and cap and w > cap:
            note = f" EXCEEDS CAP {cap}%"
        lines.append(f"{g}: {w:.1f}% (target sum){note}")
    return lines


def format_briefing(all_flags, n_neutral, group_lines, n_watch_neutral=0):
    today = date.today().isoformat()
    lines = [f"-- Portfolio Signals {today} --"]
    order = {"ADD*": 0, "ADD": 1, "ADD?": 2, "ENTRY*": 3, "ENTRY": 4, "ENTRY?": 5,
             "TRIM": 6, "REGIME": 7, "NOTE": 8}
    for f in sorted(all_flags, key=lambda x: order.get(x["signal"], 9)):
        lines.append(f"{f['signal']:<7}{f['ticker']:<7}{f['reason']}")
    if not all_flags:
        lines.append("no active flags")
    watch_txt = f", {n_watch_neutral} watchlist quiet" if n_watch_neutral else ""
    lines.append(f"-- {n_neutral} names neutral{watch_txt} --")
    lines.extend(group_lines)
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description="Portfolio SMA-distance signal engine v0.1.1")
    ap.add_argument("config", nargs="?", default="portfolio.yaml")
    ap.add_argument("--json", action="store_true", help="emit NDJSON instead of text")
    ap.add_argument("--no-cache", action="store_true",
                    help="ignore the history cache and force a full refetch")
    ap.add_argument("--no-live", action="store_true",
                    help="skip the live-price overlay (use cached last close)")
    ap.add_argument("--no-lock", action="store_true",
                    help="do not serialize with other runs (skip the flock)")
    ap.add_argument("--lock-timeout", type=int, default=120, metavar="SEC",
                    help="max seconds to wait for a concurrent run before exiting (default 120)")
    ap.add_argument("--debug", action="store_true",
                    help="surface yfinance errors and per-ticker fetch attempts on stderr")
    args = ap.parse_args()

    global DEBUG
    DEBUG = args.debug

    positions, constraints, watchlist = load_config(args.config)

    # Serialize the network/cache-touching work so a scheduled run (OpenClaw,
    # cron, ...) and a manual run don't race the shared cache or yfinance tz DB.
    with run_lock(enabled=not args.no_lock, timeout=args.lock_timeout):
        monitored = {
            n: p for n, p in positions.items() if p.get("mode", "signals") == "signals"
        }
        ticker_map = {n: CRYPTO_MAP.get(n, n) for n in monitored}
        watch_map = {n: CRYPTO_MAP.get(n, n) for n in watchlist}
        fetch_set = set(ticker_map.values()) | set(watch_map.values()) | {BENCHMARK}
        history = fetch_history(list(fetch_set), use_cache=not args.no_cache)

        live_missed = []
        if not args.no_live:
            live_missed = apply_live_prices(history)

        bench_ret21 = None
        market_stress = False
        if BENCHMARK in history:
            bench_ind = compute_indicators(history[BENCHMARK])
            bench_ret21 = bench_ind.get("ret21")
            z = bench_ind.get("z20")
            market_stress = z is not None and z < MARKET_STRESS_Z20
        constraints["_market_stress"] = market_stress

        all_flags = []
        if market_stress:
            all_flags.append(
                {"ticker": BENCHMARK, "signal": "NOTE",
                 "reason": f"market stress (SPY z20 < {MARKET_STRESS_Z20}), trim flags suppressed"}
            )
        if bench_ret21 is None:
            all_flags.append(
                {"ticker": BENCHMARK, "signal": "NOTE",
                 "reason": "benchmark unavailable, RS21 disabled for all names"}
            )
        if live_missed:
            reverse = {v: k for k, v in {**ticker_map, **watch_map}.items()}
            for yft in live_missed:
                name = reverse.get(yft)
                if name is not None:
                    all_flags.append(
                        {"ticker": name, "signal": "NOTE",
                         "reason": "live price unavailable, using cached last close"}
                    )
        n_neutral = 0
        n_watch_neutral = 0
        for group, mapping, is_watch in (
            (monitored, ticker_map, False),
            (watchlist, watch_map, True),
        ):
            for name, pos in group.items():
                yft = mapping[name]
                if yft not in history:
                    all_flags.append(
                        {"ticker": name, "signal": "NOTE", "reason": "no price data"}
                    )
                    continue
                ind = compute_indicators(history[yft])
                if bench_ret21 is not None and ind.get("ret21") is not None:
                    ind["rs21"] = ind["ret21"] - bench_ret21
                else:
                    ind["rs21"] = None
                flags = evaluate(name, pos, ind, constraints, watch=is_watch)
                if flags:
                    all_flags.extend(flags)
                elif is_watch:
                    n_watch_neutral += 1
                else:
                    n_neutral += 1

    if args.json:
        for f in all_flags:
            f.pop("ind", None)
            print(json.dumps(f))
    else:
        print(format_briefing(all_flags, n_neutral, group_weights(positions, constraints), n_watch_neutral))


if __name__ == "__main__":
    main()
