"""
Centralized config with ENV overrides.
All numbers are sane defaults; override via environment variables as needed.
"""

from __future__ import annotations
import os
from dataclasses import dataclass
from typing import List


# --- Load .env early so os.getenv sees the values ---------------------------------
def _load_env():
    """
    Load environment variables from:
      1) ENV_FILE if provided, else nearest .env (found via python-dotenv)
      2) .env.local in the same directory (overrides .env), if present
    Process environment variables already set take precedence over .env values.
    """
    try:
        from dotenv import load_dotenv, find_dotenv
    except Exception:
        return  # python-dotenv not installed; skip silently

    # 1) main .env (no override so existing exports win)
    explicit = os.getenv("ENV_FILE")
    path = explicit or find_dotenv(usecwd=True)
    if path:
        try:
            load_dotenv(dotenv_path=path, override=False)
        except Exception:
            pass

        # 2) optional .env.local (override=True so local can tweak)
        try:
            base_dir = os.path.dirname(path)
            local_path = os.path.join(base_dir, ".env.local")
            if os.path.exists(local_path):
                load_dotenv(dotenv_path=local_path, override=True)
        except Exception:
            pass
    else:
        # fallback A: current working directory
        if os.path.exists(".env"):
            try:
                load_dotenv(".env", override=False)
            except Exception:
                pass
        if os.path.exists(".env.local"):
            try:
                load_dotenv(".env.local", override=True)
            except Exception:
                pass

        # fallback B: package directory (e.g., smart_scanner/.env)
        try:
            from pathlib import Path

            pkg_dir = Path(__file__).resolve().parent
            pkg_env = pkg_dir / ".env"
            pkg_env_local = pkg_dir / ".env.local"
            if pkg_env.exists():
                load_dotenv(str(pkg_env), override=False)
            if pkg_env_local.exists():
                load_dotenv(str(pkg_env_local), override=True)
        except Exception:
            pass


_load_env()
# -----------------------------------------------------------------------------------


def _get_list(env: str, default: List[str]) -> List[str]:
    raw = os.getenv(env, "")
    if not raw.strip():
        return default
    return [s.strip() for s in raw.split(",") if s.strip()]


@dataclass(frozen=True)
class Config:
    # API
    base_url: str = os.getenv("BLOFIN_BASE_URL", "https://openapi.blofin.com")
    api_key: str = os.getenv("BLOFIN_API_KEY", "")
    # Support legacy names SECRET_KEY/PASSPHRASE to avoid misconfig
    api_secret: str = os.getenv("BLOFIN_API_SECRET", os.getenv("SECRET_KEY", ""))
    api_passphrase: str = os.getenv("BLOFIN_API_PASSPHRASE", os.getenv("PASSPHRASE", ""))

    # Scanner
    timeframes: List[str] = tuple(
        _get_list("SCAN_TIMEFRAMES", ["5m", "15m", "1H", "4H"])
    )
    candles_limit: int = int(os.getenv("CANDLES_LIMIT", "200"))
    top_symbols_by_quote_vol: int = int(os.getenv("TOP_SYMBOLS", "80"))
    include_symbols: List[str] = tuple(_get_list("INCLUDE_SYMBOLS", []))
    exclude_symbols: List[str] = tuple(_get_list("EXCLUDE_SYMBOLS", []))

    # Liquidity/quality gates (used for both REST and WS universes)
    min_quote_vol_usdt: float = float(os.getenv("MIN_QUOTE_VOL_USDT", "150000"))
    # Optional, separate signal-time volume gate (independent of universe gate)
    signal_min_qvol_usdt: float = float(
        os.getenv("SIGNAL_MIN_QVOL_USDT", os.getenv("MIN_QUOTE_VOL_USDT", "0"))
    )
    min_last_price: float = float(os.getenv("MIN_LAST_PRICE", "0.005"))

    # Concurrency / rate limiting
    rest_concurrency: int = int(os.getenv("REST_CONCURRENCY", "8"))
    candles_concurrency: int = int(os.getenv("CANDLES_CONCURRENCY", "6"))
    candles_chunk: int = int(os.getenv("CANDLES_CHUNK", "24"))
    candles_inter_chunk_sleep: float = float(
        os.getenv("CANDLES_INTER_CHUNK_SLEEP", "0.6")
    )
    max_retries: int = int(os.getenv("HTTP_MAX_RETRIES", "5"))
    base_backoff: float = float(os.getenv("HTTP_BASE_BACKOFF", "0.8"))
    max_backoff: float = float(os.getenv("HTTP_MAX_BACKOFF", "8"))

    # Strategy gating
    min_score: float = float(os.getenv("SCORE_MIN", "3.0"))
    score_max: float = float(os.getenv("SCORE_MAX", "7.0"))
    require_confirmation: int = int(os.getenv("REQUIRE_CONFIRMATION", "0"))  # 0/1

    # Regime + bandit
    regime_window: int = int(
        os.getenv("REGIME_WINDOW", "300")
    )  # bars for realized vol / EMA slope
    bandit_tau: float = float(os.getenv("BANDIT_TAU", "0.07"))  # EXP3 temperature

    # Cooldowns / dedupe
    cooldown_sec: int = int(os.getenv("SIGNAL_COOLDOWN_SEC", "90"))
    dedupe_ttl_sec: int = int(os.getenv("DEDUPE_TTL_SEC", "600"))

    # Caching / loop pacing
    tickers_ttl_sec: int = int(
        os.getenv("TICKERS_TTL_SEC", "120")
    )  # for REST fallback cache
    loop_sleep_sec: float = float(os.getenv("LOOP_SLEEP_SEC", "6.0"))  # main loop sleep

    # HTTP pacing / CF cooldown
    min_req_interval_ms: int = int(
        os.getenv("BLOFIN_MIN_REQ_INTERVAL_MS", "300")
    )  # min gap between HTTP requests
    cf_429_cooldown_sec: int = int(
        os.getenv("BLOFIN_429_COOLDOWN_SEC", "120")
    )  # cool-off when CF 1015/429 triggers
    tf_sleep_sec: float = float(
        os.getenv("TF_SLEEP_SEC", "0.5")
    )  # pause between timeframe batches

    # WebSocket settings (universe via public tickers)
    ws_public_url: str = os.getenv(
        "BLOFIN_WS_PUBLIC", "wss://openapi.blofin.com/ws/public"
    )
    ws_private_url: str = os.getenv(
        "BLOFIN_WS_PRIVATE", "wss://openapi.blofin.com/ws/private"
    )
    ws_inst_type: str = os.getenv("WS_TICKERS_INSTTYPE", "SWAP")  # SWAP, FUTURES, SPOT
    ws_universe_ready_wait_sec: float = float(
        os.getenv("WS_UNIVERSE_READY_WAIT_SEC", "3.0")
    )
    ws_ping_interval_sec: float = float(os.getenv("WS_PING_INTERVAL_SEC", "20.0"))
    universe_from_ws: int = int(
        os.getenv("UNIVERSE_FROM_WS", "1")
    )  # 1 = use WS for universe

    # WebSocket klines (optional candles via WS)
    use_ws_candles: int = int(os.getenv("USE_WS_CANDLES", "0"))  # 0/1
    ws_max_symbols: int = int(os.getenv("WS_MAX_SYMBOLS", "120"))
    ws_backfil_bars: int = int(os.getenv("WS_BACKFIL_BARS", "240"))

    # Universe persistence & fallback
    universe_cache_path: str = os.getenv("UNIVERSE_CACHE_PATH", ".universe_cache.json")
    universe_ttl_sec: int = int(os.getenv("UNIVERSE_TTL_SEC", "900"))  # 15 minutes
    fallback_universe: List[str] = tuple(
        _get_list(
            "FALLBACK_UNIVERSE",
            [
                "BTC-USDT",
                "ETH-USDT",
                "SOL-USDT",
                "BNB-USDT",
                "XRP-USDT",
                "DOGE-USDT",
                "ADA-USDT",
                "ARB-USDT",
                "OP-USDT",
                "LINK-USDT",
            ],
        )
    )

    # Output shaping
    max_candidates_per_tf: int = int(os.getenv("MAX_CANDIDATES_PER_TF", "8"))
    max_candidates_per_loop: int = int(os.getenv("MAX_CANDIDATES_PER_LOOP", "32"))
    print_json_lines: int = int(os.getenv("PRINT_JSON_LINES", "0"))
    # Pretty printing (tables/colors)
    pretty: int = int(os.getenv("PRETTY", "1"))

    # Logs
    print_liquidity_debug: int = int(os.getenv("PRINT_LIQUIDITY_DEBUG", "0"))
    print_diagnostics: int = int(os.getenv("PRINT_DIAGNOSTICS", "0"))
    log_trade_skips: int = int(os.getenv("LOG_TRADE_SKIPS", "1"))
    # Startup behavior (account sanity)
    run_account_sanity: int = int(os.getenv("ACCOUNT_SANITY", "1"))  # 0/1
    account_sanity_timeout_sec: float = float(
        os.getenv("ACCOUNT_SANITY_TIMEOUT_SEC", "20")
    )
    # Emergency controls
    kill_switch: int = int(os.getenv("KILL_SWITCH", "0"))  # 1 = block new orders
    emergency_file: str = os.getenv("EMERGENCY_FILE", ".panic")  # presence triggers kill
    auto_panic_on_file: int = int(os.getenv("AUTO_PANIC_ON_FILE", "0"))  # 1 = auto-flatten when file present
    panic_cancel_open: int = int(os.getenv("PANIC_CANCEL_OPEN", "1"))
    panic_close_positions: int = int(os.getenv("PANIC_CLOSE_POSITIONS", "1"))
    panic_reduce_only: int = int(os.getenv("PANIC_REDUCE_ONLY", "1"))
    # Trading policy: allow only one open position per symbol
    single_position_per_symbol: int = int(os.getenv("SINGLE_POSITION_PER_SYMBOL", "1"))
    # Balance check before orders (live trading)
    check_balance_before_order: int = int(os.getenv("CHECK_BALANCE_BEFORE_ORDER", "1"))
    margin_safety_bps: float = float(os.getenv("MARGIN_SAFETY_BPS", "200"))  # 2% headroom
    # Minimum initial margin (USD) required to place a trade (after sizing)
    min_initial_margin_usd: float = float(os.getenv("MIN_INITIAL_MARGIN_USD", "5"))
    # If enabled, upsize quantity just enough to meet MIN_INITIAL_MARGIN_USD
    # when the computed post-rounding margin is too small (subject to balance/exposure caps)
    upsize_to_min_margin: int = int(os.getenv("UPSIZE_TO_MIN_MARGIN", "0"))
    # Contract size awareness for per-size multipliers (ctVal/contractSize). 1=on, 0=revert to coin*price
    contract_size_aware: int = int(os.getenv("CONTRACT_SIZE_AWARE", "1"))

    # Universe shaping
    swap_only: int = int(os.getenv("SWAP_ONLY", "0"))  # 1=prefer SWAP-only in WS universe
    exclude_patterns: List[str] = tuple(
        _get_list(
            "EXCLUDE_PATTERNS",
            [
                "*-INDEX*",
                "BTCDOM-*",
                "XAUT-*",
                "USDT-USDT*",
            ],
        )
    )

    # Strategy gates
    require_multi_components: int = int(os.getenv("REQUIRE_MULTI_COMPONENTS", "0"))
    min_components: int = int(os.getenv("MIN_COMPONENTS", "2"))

    # Contextual bandit persistence
    bandit_state_path: str = os.getenv("BANDIT_STATE_PATH", "bandit_state.json")

    # Metrics
    metrics_enabled: int = int(os.getenv("METRICS_ENABLED", "1"))
    metrics_path: str = os.getenv("METRICS_PATH", "scanner_metrics.jsonl")
    print_of_debug: int = int(os.getenv("PRINT_OF_DEBUG", "0"))

    # Orderflow (WS books + trades)
    use_ws_orderflow: int = int(os.getenv("USE_WS_ORDERFLOW", "0"))
    ws_book_depth: int = int(os.getenv("WS_BOOK_DEPTH", "5"))
    orderflow_window_sec: int = int(os.getenv("ORDERFLOW_WINDOW_SEC", "60"))
    max_spread_bps: float = float(os.getenv("MAX_SPREAD_BPS", "8.0"))
    min_trades_per_min: float = float(os.getenv("MIN_TRADES_PER_MIN", "0.0"))
    # Strictness and freshness for orderflow gating
    orderflow_strict: int = int(os.getenv("ORDERFLOW_STRICT", "0"))  # 1=require OF readiness
    of_ready_min_trades: int = int(os.getenv("OF_READY_MIN_TRADES", "1"))
    of_ready_max_age_sec: int = int(os.getenv("OF_READY_MAX_AGE_SEC", "20"))

    # Probability calibration
    calibration_path: str = os.getenv("CALIBRATION_PATH", "calibration.json")

    # Blue-chip only mode
    bluechip_only: int = int(os.getenv("BLUECHIP_ONLY", "0"))
    bluechip_bases: List[str] = tuple(
        _get_list(
            "BLUECHIP_BASES",
            [
                "BTC",
                "ETH",
                "SOL",
                "BNB",
                "XRP",
                "DOGE",
                "ADA",
                "LINK",
                "DOT",
                "LTC",
                "TRX",
                "AVAX",
                "BCH",
                "XMR",
                "ATOM",
                "FIL",
                "AAVE",
                "UNI",
                "ETC",
                "NEAR",
            ],
        )
    )

    # Orderflow notional activity gate (USD/min)
    min_notional_per_min_usd: float = float(
        os.getenv("MIN_NOTIONAL_PER_MIN_USD", "0")
    )
    min_depth_notional_usd: float = float(
        os.getenv("MIN_DEPTH_NOTIONAL_USD", "0")
    )  # sum of top-N levels on each side in USD

    # --- Auto-trader settings ---
    enable_autotrade: int = int(os.getenv("ENABLE_AUTOTRADE", "0"))
    paper_trading: int = int(os.getenv("PAPER_TRADING", "1"))
    trade_notional_usd: float = float(os.getenv("TRADE_NOTIONAL_USD", "50"))
    trade_max_positions: int = int(os.getenv("TRADE_MAX_POSITIONS", "3"))
    trade_max_exposure_usd: float = float(os.getenv("TRADE_MAX_EXPOSURE_USD", "500"))
    trade_cooldown_sec: int = int(os.getenv("TRADE_COOLDOWN_SEC", os.getenv("SIGNAL_COOLDOWN_SEC", "120")))
    trade_min_score: float = float(os.getenv("TRADE_MIN_SCORE", "3.6"))
    trade_min_prob: float = float(os.getenv("TRADE_MIN_PROB", "0.62"))
    order_slippage_bps: float = float(os.getenv("ORDER_SLIPPAGE_BPS", "8.0"))
    min_qvol_trade_usdt: float = float(
        os.getenv("MIN_QVOL_TRADE_USDT", os.getenv("SIGNAL_MIN_QVOL_USDT", os.getenv("MIN_QUOTE_VOL_USDT", "150000")))
    )
    trading_margin_mode: str = os.getenv("TRADING_MARGIN_MODE", "cross")  # cross/isolated
    trading_position_mode: str = os.getenv("TRADING_POSITION_MODE", "net")  # net/long/short
    trading_leverage: str = os.getenv("TRADING_LEVERAGE", "3")
    enable_private_ws: int = int(os.getenv("ENABLE_PRIVATE_WS", "1"))
    enable_tpsl: int = int(os.getenv("ENABLE_TPSL", "1"))
    tp_bps: float = float(os.getenv("TP_BPS", "80"))  # 0.8% default (legacy)
    sl_bps: float = float(os.getenv("SL_BPS", "50"))  # 0.5% default (legacy)
    # Optional: second TP distance for bps mode (enables two TPs in bps)
    tp2_bps: float = float(os.getenv("TP2_BPS", "0"))  # 0 = disabled

    # Smart TP/SL (ATR/level based)
    enable_smart_tpsl: int = int(os.getenv("ENABLE_SMART_TPSL", "1"))
    tpsl_mode: str = os.getenv("TPSL_MODE", "atr")  # atr | bps | level_atr
    atr_sl_mult: float = float(os.getenv("ATR_SL_MULT", "1.2"))
    atr_tp1_mult: float = float(os.getenv("ATR_TP1_MULT", "1.6"))
    atr_tp2_mult: float = float(os.getenv("ATR_TP2_MULT", "3.0"))
    # Optional bounds on ATR percent used for TPSL to avoid tiny TP or huge SL on spikes
    atr_tpsl_min_pct: float = float(os.getenv("ATR_TPSL_MIN_PCT", "0"))  # 0 = disabled
    atr_tpsl_max_pct: float = float(os.getenv("ATR_TPSL_MAX_PCT", "0"))  # 0 = disabled
    scale_out_pct1: float = float(os.getenv("SCALE_OUT_PCT1", "0.5"))  # 0..1
    scale_out_pct2: float = float(os.getenv("SCALE_OUT_PCT2", "0.5"))  # 0..1; remainder after TP1
    use_signal_level: int = int(os.getenv("USE_SIGNAL_LEVEL", "1"))  # if Signal.level present, anchor SL
    level_buffer_bps: float = float(os.getenv("LEVEL_BUFFER_BPS", "12"))  # small buffer beyond level
    regime_scale_risk: int = int(os.getenv("REGIME_SCALE_RISK", "1"))  # multiply by signal.meta.regime_mult
    price_round_dp: int = int(os.getenv("PRICE_ROUND_DP", "4"))

    # --- Smart risk (dynamic leverage/margin/notional) ---
    enable_smart_risk: int = int(os.getenv("ENABLE_SMART_RISK", "1"))
    lev_min: float = float(os.getenv("LEV_MIN", "1"))
    lev_max: float = float(os.getenv("LEV_MAX", "5"))
    lev_base: float = float(os.getenv("LEV_BASE", os.getenv("TRADING_LEVERAGE", "3")))
    atr_ref_pct: float = float(os.getenv("ATR_REF_PCT", "0.015"))  # 1.5% ref vol
    k_conf: float = float(os.getenv("LEV_K_CONF", "0.8"))
    k_vol: float = float(os.getenv("LEV_K_VOL", "1.2"))
    cross_if_atr_gt: float = float(os.getenv("CROSS_IF_ATR_GT", "0.03"))  # switch to cross if ATR% > 3%
    iso_if_conf_gt: float = float(os.getenv("ISOLATED_IF_CONF_GT", "0.80"))  # use isolated if high confidence
    enable_dynamic_notional: int = int(os.getenv("ENABLE_DYNAMIC_NOTIONAL", "1"))
    dyn_notional_min_mult: float = float(os.getenv("DYN_NOTIONAL_MIN_MULT", "0.6"))
    dyn_notional_max_mult: float = float(os.getenv("DYN_NOTIONAL_MAX_MULT", "1.8"))

    # --- Trailing & Break-even management (via private WS positions) ---
    enable_trailing: int = int(os.getenv("ENABLE_TRAILING", "1"))
    trail_mode: str = os.getenv("TRAIL_MODE", "atr")  # atr | bps
    trail_atr_mult: float = float(os.getenv("TRAIL_ATR_MULT", "1.0"))
    trail_bps: float = float(os.getenv("TRAIL_BPS", "35"))  # 0.35%
    break_even_after_R: float = float(os.getenv("BREAK_EVEN_AFTER_R", "1.0"))
    break_even_buffer_bps: float = float(os.getenv("BREAK_EVEN_BUFFER_BPS", "6"))
    trail_min_step_bps: float = float(os.getenv("TRAIL_MIN_STEP_BPS", "5"))  # avoid micro-updates
    # Behavior: move SL to break-even immediately after first TP triggers
    move_sl_be_on_tp1: int = int(os.getenv("MOVE_SL_BE_ON_TP1", "1"))

    # Venue-side enforcement for too-small positions (live only)
    enforce_min_margin: int = int(os.getenv("ENFORCE_MIN_MARGIN", "0"))  # 0/1
    enforce_min_margin_action: str = os.getenv("ENFORCE_MIN_MARGIN_ACTION", "close")  # close | topup

    # Trigger buffers: ensure TP/SL trigger prices satisfy venue's strict inequality vs last price
    trigger_eps_bps: float = float(os.getenv("TRIGGER_EPS_BPS", "6"))  # 6 bps default
    # Retry behavior for TP/SL attachment failures
    tpsl_retry_on_fail: int = int(os.getenv("TPSL_RETRY_ON_FAIL", "1"))
    tpsl_retry_mult: float = float(os.getenv("TPSL_RETRY_MULT", "3.0"))  # multiply epsilon for retry
    tpsl_retry_trigger_type: str = os.getenv("TPSL_RETRY_TRIGGER_TYPE", "mark")  # last|mark|index

    # Ensure venue leverage matches plan on each entry (per instrument)
    set_leverage_on_entry: int = int(os.getenv("SET_LEVERAGE_ON_ENTRY", "0"))  # 0/1
    # Reserve at least venue min size for TP2 when feasible (two-TP modes only)
    reserve_min_for_tp2: int = int(os.getenv("RESERVE_MIN_FOR_TP2", "0"))  # 0/1


CONFIG = Config()
