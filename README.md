# psignals

SMA-distance signal engine for a multi-account portfolio. Reads a YAML config of
holdings, pulls price history via `yfinance`, and emits add/trim/rebalance signals
based on moving-average distance, RSI, and per-position guardrails.

## Privacy

**`portfolio.yaml` contains real holdings, weights, account labels, and total
value. It is gitignored and must never be pushed to a public repo.** Work from
the sanitized `portfolio.example.yaml` template instead.

```bash
cp portfolio.example.yaml portfolio.yaml   # then edit with your real positions
```

Before the first public push, verify the private file is not tracked:

```bash
git check-ignore portfolio.yaml   # should print: portfolio.yaml
git ls-files | grep portfolio.yaml   # should return nothing
```

## Setup

Scripts are [PEP 723](https://peps.python.org/pep-0723/) self-contained and run
with [uv](https://docs.astral.sh/uv/) — dependencies are declared inline, no
manual install needed:

```bash
uv run psignals.py                 # reads ./portfolio.yaml by default
uv run psignals.py path/to/cfg.yaml
uv run psignals.py --json          # NDJSON output instead of text
```

Ad-hoc SMA distances for arbitrary tickers:

```bash
uv run sma_dist.py FBTC NVDA GLD
```

## Data caching

Price fetching is split by mutability:

- **Daily history (2y)** is cached per ticker with a **60-minute TTL**. Closed
  daily bars are immutable, so rolling anchors (SMA20/50/200, the percentile
  distribution) are computed from cache and only the added/stale tickers hit the
  network. This cuts yfinance call volume and rate-limit risk.
- **Live price** is fetched fresh on every run (one batched 1-minute download)
  and spliced onto each series as the current bar, so the price-driven signals
  (`dist20`, `pctile20`, `rsi14`, `z20`, `ret21`) stay near-realtime (≤~2 min lag)
  while the anchors ride on cached bars.

If the live fetch fails for a ticker, it falls back to the cached last close and
emits a `NOTE` so the degradation is visible.

Cache location precedence: `PSIGNALS_CACHE` (explicit) → `XDG_CACHE_HOME/psignals`
→ `~/.cache/psignals`. It's a persistent disk cache by design — deterministic
under cron (no `XDG_RUNTIME_DIR` dependency) and survives reboots.

```bash
uv run psignals.py --no-cache   # force full history refetch
uv run psignals.py --no-live    # skip overlay, use cached last close
PSIGNALS_CACHE=./.cache uv run psignals.py   # repo-local cache
```

### Deployment notes

- **Pi (primary, daily scheduled run):** at daily cadence the 60-min TTL is always
  expired, so each run refetches full history anyway — correct, since you want the
  new daily bar. The cache mainly helps intra-hour reruns; the live overlay keeps
  price fresh regardless. Persistent `~/.cache` works cleanly under cron. If you
  schedule via cron, ensure `HOME` is set (it normally is) so `~/.cache` resolves;
  a systemd user timer with `loginctl enable-linger` is a tidier alternative.
- **Mac (interactive dev):** same default path (`~/.cache/psignals`); the cache
  pays off across rapid dev iterations within the hour.
- Do **not** point the cache at `/run/user/$UID` (tmpfs) for the scheduled job —
  it may be absent under cron and offers no benefit here.

## Config model

| Field | Applies to | Meaning |
|-------|-----------|---------|
| `role` | all | `core` \| `tactical` \| `income` |
| `mode` | all | `allocation` (drift-band rebalance) \| `signals` (SMA/RSI zones) |
| `target_weight` | all | % of `total_value_ref` |
| `max_weight` | signals | optional hard cap % |
| `drift_band` | allocation | +/- band before a rebalance flag |
| `add_zone` / `trim_zone` | signals | enable buy / sell signal generation |
| `group` | optional | aggregation key (e.g. `btc_complex`, `hard_assets`) |
| `source: crypto` | optional | routes ticker through `CRYPTO_MAP` (`BTC-USD`, `ETH-USD`) |
| `earnings` | optional | `[Mon-DD-YYYY, ...]` dates for blackout logic |

Other blocks: `unmonitored` (held for % math, no price fetch), `watchlist`
(not held, tracked for entry), and `constraints` (portfolio-wide guardrails).

## Signal thresholds

Tunable constants at the top of `psignals.py`:

- `PCTILE_ADD_STRONG` / `PCTILE_ADD_WATCH` / `PCTILE_TRIM` — SMA-distance percentile gates
- `RSI_ADD` / `RSI_TRIM` — RSI bounds
- `MIN_200D_BUFFER_PCT`, `RS21_FLOOR_PCT`, `MARKET_STRESS_Z20` — regime filters

## Reading the report

Each line answers one question per signal class, ordered by descending
actionability. Every line is a **condition detection plus its mechanism, never an
instruction** — the engine surfaces and ranks; you decide. The config encodes
which decisions you've pre-authorized it to prompt (`add_zone` / `trim_zone` /
roles). The suppressors (earnings, market stress, trend gate) encode the known
failure modes of mean-reversion logic, so what survives to a signal message is
pre-filtered for the regimes where the statistics actually mean what they claim.

### `ADD*` / `ADD` — is a pre-approved name temporarily cheap within an intact uptrend?

Fires only when **all** of:

- price above a **rising 200d** (structural trend intact)
- 20d distance at/below its own **5th/10th percentile** (statistically unusual pullback for that name)
- **RSI confirming** (< 40/45)
- quality gates: 200d buffer wider than a vol-scaled cushion (not a 200d test in disguise), 20d > 50d (intermediate trend unbroken), RS21 above −15% (not idiosyncratic collapse)

Mechanism: mean reversion is only tradable when the anchor is rising; the gates
strip out the cases where "cheap vs. the 20d" is actually early-stage breakdown.

### `ADD?` — pullback conditions met, but a quality gate failed

Same trigger, one or more gates failed, **failures named inline**. Exists so
downgrades are visible rather than silent: you see why the engine didn't fully
endorse it (the RKLB case — thin buffer, broken stack, −28% RS). Human review
required by construction.

### `TRIM` — is a name approved for trimming statistically stretched?

20d distance ≥ **90th** own-percentile **AND** RSI > 70. The dual gate prevents
flagging slow grinds. Rationale: trimming into extension sells to momentum buyers
instead of into weakness. Suppressed entirely during **market stress** (SPY z20
< −2), because in a broad squeeze/washout percentiles are distorted and relative
strength shouldn't be sold.

### `REGIME` — has a name left the environment where the above logic is valid?

Below a **declining 200d**, for tacticals and `regime_watch` opt-ins. Not a sell
signal — a **mode switch**: mean-reversion adds are auto-blocked, and the line is
a standing prompt to re-underwrite the thesis. RS21 distinguishes stale
downtrends already in the price (ETH complex near 0%) from active idiosyncratic
selling (CRCL −21%).

### `NOTE` — why is the engine deliberately quiet or degraded?

Earnings blackout (gap risk dominates mean-reversion logic within 5 sessions of a
print), missing price data, or market-stress suppression. Makes silences
auditable.

### `N names neutral` — reconciliation

Flags + neutrals must equal the monitored count. A drift means a data or config
problem.

### Group lines — is a thesis sleeve breaching its risk budget?

Aggregate target weights per `group` vs. caps (`btc_complex` vs. 15%). Exists
because per-name monitoring structurally hides correlated exposure — nine names
can each look small while the sleeve is 14%.

### What it deliberately cannot tell you

Whether a thesis is broken, whether a flag coincides with news, or anything about
sizing. Those stay human by design.

## Disclaimer

Personal tooling for informational use only. Not investment advice.
