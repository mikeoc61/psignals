#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "yfinance>=0.2.50",
#     "pandas>=2.0",
#     "pyyaml>=6.0",
# ]
# ///

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

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


def fetch_history(tickers, period=LOOKBACK, ttl=HISTORY_TTL, use_cache=True):
    """Daily close history per ticker.

    Closed daily bars never change, so each ticker's 2y series is cached to disk
    with a 60-min TTL. Only stale/missing tickers hit the network. Pass
    use_cache=False to force a full refresh.
    """
    import yfinance as yf

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    now = time.time()
    out, stale = {}, []
    for t in tickers:
        cp = _cache_path(t, period)
        if use_cache and cp.exists() and now - cp.stat().st_mtime < ttl:
            try:
                out[t] = pd.read_pickle(cp)
                continue
            except Exception:
                pass  # corrupt cache -> refetch
        stale.append(t)

    if stale:
        data = yf.download(
            stale, period=period, auto_adjust=True, progress=False, group_by="ticker"
        )
        for t in stale:
            try:
                close = data[t]["Close"].dropna() if len(stale) > 1 else data["Close"].dropna()
            except KeyError:
                continue
            try:
                close.to_pickle(_cache_path(t, period))
            except Exception:
                pass  # cache write is best-effort
            out[t] = close

    return {t: c for t, c in out.items() if len(c) >= 60}


def _splice(close, last, last_dt):
    """Overwrite the current bar or append a newer one; ignore stale quotes."""
    anchor = close.index[-1].normalize()
    if last_dt == anchor:
        close.iloc[-1] = last
    elif last_dt > anchor:
        close.loc[last_dt] = last


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
    import logging
    import yfinance as yf

    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
    try:
        yf.set_tz_cache_location(str(CACHE_DIR / "yf-tz"))
    except Exception:
        pass

    if not history:
        return []
    tickers = list(history)
    multi = len(tickers) > 1

    data = None
    for attempt in range(retries + 1):
        try:
            data = yf.download(
                tickers, period=quote_period, interval="1d",
                auto_adjust=True, progress=False, group_by="ticker",
            )
            if data is not None and not data.empty:
                break
        except Exception:
            pass
        time.sleep(0.5 * (attempt + 1))

    missed = []
    for t, close in history.items():
        last = last_dt = None
        try:
            q = (data[t]["Close"] if multi else data["Close"]).dropna()
            v = float(q.iloc[-1])
            if v == v:  # not NaN
                last, last_dt = v, q.index[-1].normalize()
        except (KeyError, IndexError, ValueError, TypeError, AttributeError):
            pass
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
    args = ap.parse_args()

    positions, constraints, watchlist = load_config(args.config)

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
