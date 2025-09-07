Smart Scanner / Trading Bot

Overview
- Purpose: scans crypto markets for high‑quality signals and can auto‑trade them on BloFin (paper/live). Also accepts external signals via a simple webhook.
- Core pieces: WebSocket universe + optional WS candles/orderflow; strategy engine + calibrator + regime; dedupe/cooldowns; AutoTrader with paper/live routing; JSONL metrics.

Quickstart
- Python: 3.10+
- Install deps (package lives in the `smart_scanner` folder):
  - `pip install -e smart_scanner`
  - Optional for .env loading: `pip install python-dotenv`
- Run a one‑off scan:
  - `python -m smart_scanner.scanner_runner`
- Run the polling loop (REST/WS mix):
  - `python -m smart_scanner.scanner_runner --loop`
- Run the event‑driven loop (signals on WS bar close):
  - `python -m smart_scanner.scanner_runner --event`
- Start the external signal webhook (to trade your own signals):
  - `python -m smart_scanner.signal_gateway --host 0.0.0.0 --port 8080`
‑ Run a simple built‑in bot (EMA cross):
  - `python -m smart_scanner.simple_bot --loop`

Configuration
- Env file: `smart_scanner/.env` (or export vars). If `python-dotenv` is installed, it’s auto‑loaded.
- Key toggles:
  - `ENABLE_AUTOTRADE`: set `1` to let signals route to the trader
  - `PAPER_TRADING`: leave `1` while testing; set `0` for live
  - `TRADE_NOTIONAL_USD`, `TRADE_MIN_SCORE`, `TRADE_MIN_PROB`, `TRADE_COOLDOWN_SEC`, exposure caps
- Smart TP/SL (position-level):
    - `ENABLE_TPSL=1`, `ENABLE_SMART_TPSL=1`, `TPSL_MODE=atr|bps|level_atr`
    - ATR mode: `ATR_SL_MULT` (default 1.2), `ATR_TP1_MULT` (1.6), `ATR_TP2_MULT` (3.0)
    - Scale-out: `SCALE_OUT_PCT1` (0.5), `SCALE_OUT_PCT2` (0.5)
    - Use signal levels: `USE_SIGNAL_LEVEL=1`, buffer `LEVEL_BUFFER_BPS` (12)
    - Rounding: `PRICE_ROUND_DP` (4)
  - WS/REST: `UNIVERSE_FROM_WS`, `USE_WS_CANDLES`, orderflow gates
- Smart risk (dynamic leverage/margin/notional):
    - `ENABLE_SMART_RISK=1`
    - Leverage bounds: `LEV_MIN`, `LEV_MAX`, base `LEV_BASE`
    - Sensitivity: `ATR_REF_PCT`, `LEV_K_CONF`, `LEV_K_VOL`, `REGIME_SCALE_RISK`
    - Margin decision: `CROSS_IF_ATR_GT`, `ISOLATED_IF_CONF_GT`
    - Dynamic notional: `ENABLE_DYNAMIC_NOTIONAL`, `DYN_NOTIONAL_MIN_MULT`, `DYN_NOTIONAL_MAX_MULT`
  - Metrics/output: `PRINT_JSON_LINES`, `METRICS_PATH`
- Secrets: `BLOFIN_API_KEY`, `BLOFIN_API_SECRET`, `BLOFIN_API_PASSPHRASE` (only needed when `PAPER_TRADING=0`). Never commit real keys.

How It Flows
- Universe: `ws_universe` streams tickers, ranks by 24h quote volume and price/liquidity gates.
- Data: candles via WS (`ws_klines`) when enabled, otherwise REST fallback. Optional orderflow (`ws_orderflow`) adds spread/trade‑rate/depth features.
- Signals: `signal_engine` computes strategies (`strategies` + `indicators`), blends via bandit/regime/calibration into composite signals.
- Online learning: matured labels automatically update bandit weights (positive returns reinforce contributing strategies).
- Selection: best per timeframe → best per symbol → capped per loop; dedupe + cooldown enforced.
- Trading: `GLOBAL_TRADER` evaluates gates and places paper fills or live orders (with optional TP/SL).

Files (What Each Does)
- `notes.txt`: lightweight internal notes and webhook example.
- `bandit_state.json`: persisted EXP3 weights (written periodically).
- `labels_state.json`: labeler state across restarts.
- `scanner_metrics.jsonl`: append‑only JSONL metrics (signals, orders, paper fills, labels, etc.).
- `signals/`: placeholder for external signal artifacts (optional).
- `artifacts/`: optional outputs (backtests, calibration, etc.).

Package: `smart_scanner/`
- `smart_scanner/scanner_runner.py`: main entrypoints. Builds WS universe, fetches candles, evaluates signals, prints/emits metrics, and calls the trader. Supports:
  - `--loop` polling loop and `--event` WS bar‑close loop.
- `smart_scanner/signal_engine.py`: turns kline rows into `Signal` objects. Strategy evaluation, bandit weights, regime multiplier, calibration, probability/EV shaping.
- `smart_scanner/strategies.py`: small, fast, composable strategies (breakout, EMA pullback, momentum burst, supertrend flip). Returns pass/score/side/tags/level.
- `smart_scanner/indicators.py`: NumPy TA helpers (EMA, RSI, ATR, Supertrend).
- `smart_scanner/features.py`: basic feature set (returns, EMA slope, ATR pct, volume burst, etc.).
- `smart_scanner/regime.py`: BTC‑anchored market regime (bull/bear/chop) with a smooth composite score and suggested risk multiplier.
- `smart_scanner/calibration.py`: optional logistic calibration of probabilities from features + score if `calibration.json` exists.
- `smart_scanner/bandit.py`: tiny EXP3 contextual scaffold; persists weights to `bandit_state.json`.
- `smart_scanner/lifecycle.py`: in‑memory dedupe and per‑key cooldown tracking.
- `smart_scanner/metrics.py`: JSONL metrics emitter to `METRICS_PATH` when `METRICS_ENABLED=1`.
- `smart_scanner/signal_types.py`: `Signal` dataclass and schema; deterministic `id` for dedupe; `to_json()`.
- `smart_scanner/trader.py`: AutoTrader and PaperBroker.
  - Eligibility gates (score/prob/qVol/cooldown/positions/exposure) and notional sizing.
  - Paper fills with slippage; live orders via `BlofinClient` + optional TP/SL.
- `smart_scanner/blofin_client.py`: async REST client for BloFin. Public market data (instruments, tickers, candles) and private trading endpoints (orders, TP/SL, leverage, balances, positions). Handles pacing and 429 cool‑offs.
- `smart_scanner/ws_universe.py`: public WS client for tickers; builds and maintains a liquid universe with include/exclude/pattern/blue‑chip gates.
- `smart_scanner/ws_klines.py`: public WS candles store; keeps rolling bars per (symbol,timeframe); emits events on bar confirmation.
- `smart_scanner/ws_orderflow.py`: public WS orderflow store; top‑of‑book stats, spread, depth, imbalance, rolling trade rates and notional.
- `smart_scanner/ws_private.py`: resilient private WS (login, heartbeat, reconnect, resubscribe). Channel routing for orders/positions/orders‑algo with simple callbacks.
- `smart_scanner/signal_gateway.py`: HTTP webhook to accept external signals (e.g., TradingView) and forward to AutoTrader. Optional `WEBHOOK_SECRET` auth.
- `smart_scanner/risk_manager.py`: monitors private positions via WS and adjusts stop-loss for break-even and trailing (uses REST to update TPSL). Enabled by `ENABLE_TRAILING`.
- `smart_scanner/simple_bot.py`: minimal EMA(21)/EMA(50) cross bot that generates `Signal`s on fresh crosses and routes them to the trader. Controlled via env: `BOT_TIMEFRAME`, `BOT_EMA_FAST`, `BOT_EMA_SLOW`, `BOT_SCORE`, `BOT_PROB`.
- `smart_scanner/config.py`: centralized config with environment overrides. Loads `.env`/`.env.local` if `python-dotenv` is installed. Controls scanner, WS, orderflow, bandit, autotrade, TPSL, etc.
- `smart_scanner/.env`: example env with BloFin endpoints and default scanner/trader settings. Do not keep real keys here in production.
- `smart_scanner/pyproject.toml`: package metadata and core dependencies (aiohttp, websockets, numpy, pandas).
- `smart_scanner/setup.cfg`: setuptools config (package discovery).
- `smart_scanner/__init__.py`: package marker.
- `smart_scanner/smart_scanner.code-workspace`: VS Code workspace convenience.

External Signals (Webhook)
- Start: `python -m smart_scanner.signal_gateway --host 0.0.0.0 --port 8080`
- Secure (recommended): set `WEBHOOK_SECRET` and send header `X-Webhook-Token`.
- POST JSON to `/signal` (alias `/webhook`):
  - Example: `{"symbol":"BINANCE:BTCUSDT","side":"long","timeframe":"15m","price":50000,"score":5.1,"prob":0.7,"components":["tv"],"tags":["ext"]}`
- Gateway normalizes symbol/side, fills missing price from market if needed, builds a `Signal`, and calls the AutoTrader.

Metrics & Outputs
- JSONL file `scanner_metrics.jsonl` accumulates rows with `kind` (signal, order, paper_fill, label, order_api, order_api_tpsl).
- Trailing emits `risk_trail_sl` and the initial risk plan emits `risk_plan`.
- Bandit weights persist to `bandit_state.json`; labeler state in `labels_state.json`.

Risk & Live Trading
- Validate with `PAPER_TRADING=1` first.
- Set `ENABLE_AUTOTRADE=1` to route signals to the trader.
- For live (`PAPER_TRADING=0`), ensure BloFin keys are set and consider TP/SL via `ENABLE_TPSL`, `TP_BPS`, `SL_BPS`.
- Hard caps: `TRADE_MAX_POSITIONS`, `TRADE_MAX_EXPOSURE_USD`, symbol cooldowns and qVol gates.

Emergency Controls
- Kill switch (blocks new orders):
  - `KILL_SWITCH=1` or create a file named `.panic` (override path via `EMERGENCY_FILE`).
  - When active, the trader returns `trade_skip` with `reason=kill_switch` and no new orders are placed.
- Auto‑panic flatten (optional one‑shot):
  - Set `AUTO_PANIC_ON_FILE=1` and create `.panic` to automatically cancel open orders and reduce‑only close all positions. Metrics emitted: `panic_trigger`, `panic_cancel_order`, `panic_cancel_tpsl`, `panic_close`.
  - Behavior can be tuned with `PANIC_CANCEL_OPEN`, `PANIC_CLOSE_POSITIONS`, `PANIC_REDUCE_ONLY`.
- Manual CLI:
  - Cancel + close all: `python -m smart_scanner.emergency --all`
  - Cancel only: `python -m smart_scanner.emergency --cancel`
  - Close only:  `python -m smart_scanner.emergency --close`
  - Limit to symbols: `python -m smart_scanner.emergency --all --symbols BTC-USDT,SOL-USDT`

Troubleshooting
- No signals: lower `SCORE_MIN` or enable diagnostics (`PRINT_DIAGNOSTICS=1`).
- WS slow to populate: increase `WS_UNIVERSE_READY_WAIT_SEC` or temporarily rely on REST universe.
- Trailing doesn’t move: ensure `ENABLE_TRAILING=1`, dependencies installed, and that positions WS can authenticate.
- Rate limits: adjust `BLOFIN_MIN_REQ_INTERVAL_MS` and `BLOFIN_429_COOLDOWN_SEC`.
- .env not loading: install `python-dotenv` or export vars in your shell.

  - Trailing & Break-even (private WS):
    - `ENABLE_TRAILING=1`, `TRAIL_MODE=atr|bps`, `TRAIL_ATR_MULT`, `TRAIL_BPS`
    - Break-even: `BREAK_EVEN_AFTER_R` (e.g., 1.0R), `BREAK_EVEN_BUFFER_BPS`
    - Noise filter: `TRAIL_MIN_STEP_BPS` (skip tiny SL nudges)
    - Requires `websockets` installed (already in deps) and valid API keys

Security
- Never commit real API keys. Prefer exporting in your shell or using `.env.local` (git‑ignored in your own workflows).
- Protect the webhook with `WEBHOOK_SECRET` and network controls.

Pretty Terminal Output
- The scanner can render organized tables and colors with Rich.
- Enable via env: `PRETTY=1` (default on). Falls back to plain prints if Rich is unavailable.
- Where used:
  - Config summary (on startup)
  - Universe summary (when `PRINT_LIQUIDITY_DEBUG=1`)
  - Signals table (batch view in `--loop` mode when not printing JSON lines)

Headless Deployment
- To run 24/7 without your laptop, use Docker + Compose on a small VPS or systemd on a server.
- See `deploy/README_DEPLOY.md` for step-by-step instructions and the provided `Dockerfile` and `compose.yaml`.
# smart_scanner
# smart_scanner
# smart_scanner
# smart_scanner
# smart_scanner
