import os
import json
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Any, Optional, Tuple

def _to_bool(val: Any, default: bool = False) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("true", "1", "yes", "on"):
            return True
        if s in ("false", "0", "no", "off", ""):
            return False
    return default

def _to_float(val: Any, default: float) -> float:
    try:
        if isinstance(val, bool) or val is None or (isinstance(val, str) and not val.strip()):
            return default
        n = float(val)
        return default if (n != n or n in (float("inf"), float("-inf"))) else n
    except (ValueError, TypeError):
        return default

def _to_int(val: Any, default: int) -> int:
    try:
        if isinstance(val, bool) or val is None or (isinstance(val, str) and not val.strip()):
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default

# TOGGLE: entry quote log — config.json "entry_quote". A log line per regular paper bet; it changes no order and no bet.
ENTRY_QUOTE_DEFAULTS: Dict[str, Any] = {
    "log": False,     # True: the quote part of the ENTRY QUOTE line: the paper price against the live book at that instant and after
    "timing": False,  # True: the timing part: how old the Binance price was, when the live order left, how long Polymarket took
    "watch_s": 5.0,   # how many seconds after the entry the live book's ask is watched for that line
}


def entry_quote(block: Any) -> Tuple[Dict[str, Any], List[str]]:
    """TOGGLE: entry quote log — an "entry_quote" block, read: (the two values, what could not be read)."""
    out, bad = dict(ENTRY_QUOTE_DEFAULTS), []
    if block is None:
        return out, bad
    if not isinstance(block, dict):
        return out, ["entry_quote must be an object"]
    for key in ("log", "timing"):
        v = block.get(key)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        if isinstance(v, str):
            v = {"true": True, "on": True, "yes": True, "1": True, "false": False, "off": False, "no": False, "0": False}.get(v.strip().lower())
        if isinstance(v, (bool, int, float)) and v in (0, 1):
            out[key] = bool(v)
        else:
            bad.append(f"entry_quote.{key} must be true or false")
    v = block.get("watch_s")
    if not (v is None or (isinstance(v, str) and not v.strip())):
        try:
            n = float(v)
            if isinstance(v, bool) or n != n or n in (float("inf"), float("-inf")):
                raise ValueError(v)
            out["watch_s"] = n
        except (TypeError, ValueError):
            bad.append("entry_quote.watch_s must be a number")
    return out, bad

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config.json"))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    MODE: str = "hybrid"  # Unified hybrid copy-trading engine
    PAPER_BALANCE_USD: float = 1000.0
    # TOGGLE: entry quote log — config.json "entry_quote" (ENTRY_QUOTE_DEFAULTS above has the two names)
    ENTRY_QUOTE: Dict = {}
    PRIVATE_KEY: str = ""

    # ── Copy-trader continuous GTD execution ───────────────────────────────────
    COPY_RETRY_INTERVAL_MS: int = 300
    COPY_MAX_RETRIES: int = 5
    COPY_RESTING_TIMEOUT_S: float = 2.0
    # PATCH: fill-and-kill copy — config.json "copy_fak"; bot/copy_fak.py has the defaults and the checks
    COPY_FAK: Dict = {}
    # PATCH: price stream — config.json "price_stream"; bot/polybolt.py has the defaults and the checks
    PRICE_STREAM: Dict = {}
    # TOGGLE: underdog reversal strategy (from 5M, for 5m & 15m) — config.json "underdog"
    UNDERDOG: Dict = {}

    # ── Live trading (Polymarket CLOB V2) ───────────────────────────────────────
    # The wallet is DERIVED from PRIVATE_KEY (hex key or 12/24-word seed phrase) and
    # auto-detected: deposit-wallet (V2, signature_type 3) first, then legacy proxy /
    # safe — whichever actually holds pUSD. Nothing to pick by hand.
    CLOB_MAX_SLIPPAGE: float = 0.05  # marketable-limit buffer above the quote (probability units, 5¢)
    EXIT_MAX_RETRIES: int = 5
    FUNDER_ADDRESS: str = ""         # optional explicit deposit/funder wallet override (bypasses guessing)
    ORDER_SIGNER: str = "auto"       # order signer mode: auto, wallet, or eoa
    CLOB_ALLOW_PARTIAL_FILL: bool = True  # Allow partial fill for thin books
    RELAYER_API_KEY: str = ""        # Polymarket relayer API key (sponsors gasless on-chain setup)
    ALCHEMY_API_KEY: str = ""        # optional: dedicated Polygon RPC for chain reads

    SYMBOL: str = "BTCUSDT"
    BINANCE_BASE_URL: str = "https://api.binance.com"
    GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"
    CLOB_BASE_URL: str = "https://clob.polymarket.com"

    POLL_INTERVAL_MS: int = 1000
    CANDLE_WINDOW_MINUTES: int = 15

    # Risk per trade: "percent" = RISK_VALUE% of balance; "fixed" = RISK_VALUE dollars.
    RISK_TYPE: str = "percent"
    RISK_VALUE: float = 10.0

    # ── Latency-arb entry engine ────────────────────────────────────────────────
    # Fair probability (fast, from Binance spot) vs Polymarket's implied price.
    # Enter when EV = fair - ask_price clears EV_THRESHOLD (the book looks stale).
    EV_THRESHOLD: float = 0.04          # require >= this expected value per $1 share (after price)
    MIN_PROB_EV: float = 0.55           # don't bet near-coinflips even if EV looks positive
    MIN_BOOK_LIQUIDITY_USD: float = 20.0  # skip if the ask side can't absorb the stake
    MAX_BOOK_AGE_S: float = 3.0
    MAX_ODDS_ENABLED: bool = False      # only enter if odds are <= MAX_ODDS_CENTS (cheap underdog entries)
    MAX_ODDS_CENTS: float = 25.0        # max odds threshold in cents (default 25c)
    USE_CLOB_WS_BOOK: bool = True       # True = live CLOB WS book (incremental subscribe); False = 1s REST polls only

    # After a window expires, wait this long for Polymarket to publish its OFFICIAL
    # outcome before falling back to our own close-vs-open comparison. Without this the
    # local fallback always won the race and the authoritative result was never used.
    AUTHORITATIVE_SETTLE_WAIT_S: float = 90.0
    # PATCH: official results — both off: the bot books and redeems exactly as before
    REDEEM_AFTER_RESOLUTION: bool = False   # redeem a win once Polymarket has resolved the market, and retry (main.py _or_pass)
    CONFIRM_OFFICIAL: bool = False          # check every booked result against Polymarket's own and correct the list

    # Close-and-flip the open position on a strong opposite signal
    FLIP_ENABLED: bool = False
    FLIP_MIN_CONVICTION: float = 0.80   # opposite side's adjusted prob must be >= this
    FLIP_MIN_MINUTES_LEFT: float = 9.0  # and at least this much time left in the window

    RSI_PERIOD: int = 14
    # TOGGLE: inputs — config.json "inputs" (INPUTS_DEFAULTS, near the end of this file, has the eight names)
    INPUTS: Dict = {}

    # ── Capital Extractor (Auto-Withdrawal) ─────────────────────────────────────
    AUTO_WITHDRAW_ENABLED: bool = False
    WITHDRAW_TRIGGER_BALANCE: float = 200.0
    WITHDRAW_AMOUNT: float = 100.0
    WITHDRAW_ADDRESS: str = ""
    WITHDRAW_AUTO_RESUME: bool = True
    WITHDRAW_RESUME_AFTER: str = "flat"  # "flat" or "next_window"

    # ── Telegram Notifications ─────────────────────────────────────────────────
    TELEGRAM_ENABLED: bool = False
    TELEGRAM_BOT_TOKEN: str = ""

    # Polymarket
    POLYMARKET_SLUG: str = os.getenv("POLYMARKET_SLUG", "")
    POLYMARKET_SERIES_ID: str = os.getenv("POLYMARKET_SERIES_ID", "10192")
    POLYMARKET_SERIES_SLUG: str = os.getenv("POLYMARKET_SERIES_SLUG", "btc-up-or-down-15m")
    POLYMARKET_AUTO_SELECT_LATEST: bool = os.getenv("POLYMARKET_AUTO_SELECT_LATEST", "true").lower() == "true"
    POLYMARKET_LIVE_DATA_WS_URL: str = os.getenv("POLYMARKET_LIVE_WS_URL", "wss://ws-live-data.polymarket.com")
    POLYMARKET_UP_LABEL: str = os.getenv("POLYMARKET_UP_LABEL", "Up")
    POLYMARKET_DOWN_LABEL: str = os.getenv("POLYMARKET_DOWN_LABEL", "Down")

    # Chainlink
    POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon.drpc.org")
    POLYGON_RPC_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_RPC_URLS", "").split(",") if url.strip()]
    POLYGON_WSS_URL: str = os.getenv("POLYGON_WSS_URL", "wss://polygon-bor-rpc.publicnode.com")
    POLYGON_WSS_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_WSS_URLS", "").split(",") if url.strip()]
    CHAINLINK_BTC_USD_AGGREGATOR: str = os.getenv("CHAINLINK_BTC_USD_AGGREGATOR", "0xc907E116054Ad103354f2D350FD2514433D57F6f")

    CHAINLINK_AGGREGATORS: Dict[str, str] = {
        "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "SOL": "0x39771505D18301D239916F4C88367A6010F7D2e3",
        "XRP": "0x3454796324D6469C3110996E2E10972688045F19",
        "DOGE": "0xbAf93Ba318f77363f82E8896a2E830206121D506",
        "BNB": "0x82a6C67606bdc0409f959f60608226064223A57c"
    }

    def alchemy_rpc_url(self) -> str:
        """Dedicated Polygon RPC for chain reads (pUSD balance, wallet derivation).
        Empty string => the library's default public RPC."""
        return f"https://polygon-mainnet.g.alchemy.com/v2/{self.ALCHEMY_API_KEY}" if self.ALCHEMY_API_KEY else ""

    def get_aggregator(self, symbol: str) -> str:
        s = symbol.upper()
        if s.endswith("USDT"): s = s[:-4]
        return self.CHAINLINK_AGGREGATORS.get(s, self.CHAINLINK_BTC_USD_AGGREGATOR)

    # Proxy
    # ── Long shots ───────────────────────────────────────────────────────────────
    LONGSHOT: Dict[str, Any] = {}

    HTTP_PROXY: str = os.getenv("HTTP_PROXY", os.getenv("http_proxy", ""))
    HTTPS_PROXY: str = os.getenv("HTTPS_PROXY", os.getenv("https_proxy", ""))
    ALL_PROXY: str = os.getenv("ALL_PROXY", os.getenv("all_proxy", ""))


def normalize_funder_address(addr: str) -> str:
    """Validate and checksum user-specified funder/deposit wallet address."""
    addr = (addr or "").strip()
    if not addr or not addr.startswith("0x") or len(addr) != 42:
        return ""
    try:
        from eth_utils import to_checksum_address
        return to_checksum_address(addr)
    except Exception:
        return addr

def normalize_private_key(secret: str) -> str:
    """Accept either a raw hex private key or a 12/24-word seed phrase and return a
    hex private key. EOA only — the trading wallet is derived from this secret and
    nothing else. Returns "" for empty input. Raises if a seed phrase can't be parsed."""
    secret = (secret or "").strip()
    if not secret:
        return ""
    # A mnemonic is several space-separated words; a private key is a single token.
    if len(secret.split()) >= 12:
        from eth_account import Account
        Account.enable_unaudited_hdwallet_features()
        key = Account.from_mnemonic(secret).key.hex()
        return key if key.startswith("0x") else "0x" + key
    return secret


def normalize_order_signer(val: Any) -> Optional[str]:
    """Validate order signer setting: auto, wallet, or eoa."""
    if not val:
        return "auto"
    s = str(val).strip().lower()
    if s in ("auto", "wallet", "eoa"):
        return s
    return None


def load_settings():
    base_settings = Settings()
    try:
        from bot import longshot as _longshot
        base_settings.LONGSHOT = _longshot.merged({})
    except Exception:
        base_settings.LONGSHOT = {}
    try:                                   # PATCH: price stream — its defaults (everything off)
        from bot import polybolt as _polybolt
        base_settings.PRICE_STREAM = _polybolt.merged({})
    except Exception:
        base_settings.PRICE_STREAM = {}
    config_path = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)

            # PATCH: fill-and-kill copy — config.json "copy_fak", checked (defaults to winning FAK if omitted)
            if "copy_fak" in config_data:
                try:
                    from bot import copy_fak as _copy_fak
                    base_settings.COPY_FAK = _copy_fak.normalize(config_data["copy_fak"])
                    if "retry_interval_ms" in base_settings.COPY_FAK:
                        base_settings.COPY_RETRY_INTERVAL_MS = int(base_settings.COPY_FAK["retry_interval_ms"])
                    if "max_retries" in base_settings.COPY_FAK:
                        base_settings.COPY_MAX_RETRIES = int(base_settings.COPY_FAK["max_retries"])
                except Exception as e:
                    print(f"Warning: Failed to load copy_fak config: {e}")
            else:
                try:
                    from bot import copy_fak as _copy_fak
                    base_settings.COPY_FAK = dict(_copy_fak.DEFAULTS)
                except Exception:
                    pass

            # PATCH: price stream — config.json "price_stream", checked (everything stays off if it doesn't pass)
            if "price_stream" in config_data:
                try:
                    from bot import polybolt as _polybolt
                    base_settings.PRICE_STREAM = _polybolt.normalize(config_data["price_stream"])
                except Exception as e:
                    print(f"Warning: Failed to load price_stream config: {e}")

            # TOGGLE: underdog reversal strategy (from 5M, for 5m & 15m) — config.json "underdog"
            if "underdog" in config_data:
                try:
                    from bot import underdog as _underdog
                    base_settings.UNDERDOG = _underdog.normalize(config_data["underdog"])
                except Exception as e:
                    print(f"Warning: Failed to load underdog config: {e}")
            else:
                try:
                    from bot import underdog as _underdog
                    base_settings.UNDERDOG = dict(_underdog.UNDERDOG_DEFAULTS)
                except Exception:
                    pass

            if "mode" in config_data:
                base_settings.MODE = str(config_data["mode"]).strip().lower()
            if "paper_balance_usd" in config_data:
                try:
                    base_settings.PAPER_BALANCE_USD = float(config_data["paper_balance_usd"])
                except (ValueError, TypeError):
                    pass
            if "private_key" in config_data:
                base_settings.PRIVATE_KEY = normalize_private_key(config_data["private_key"])

            if "live" in config_data:
                live = config_data["live"]
                if "max_slippage" in live: base_settings.CLOB_MAX_SLIPPAGE = _to_float(live["max_slippage"], base_settings.CLOB_MAX_SLIPPAGE)
                if "exit_max_retries" in live: base_settings.EXIT_MAX_RETRIES = _to_int(live["exit_max_retries"], base_settings.EXIT_MAX_RETRIES)
                if "allow_partial_fill" in live: base_settings.CLOB_ALLOW_PARTIAL_FILL = _to_bool(live["allow_partial_fill"], base_settings.CLOB_ALLOW_PARTIAL_FILL)
                if "order_signer" in live: base_settings.ORDER_SIGNER = normalize_order_signer(live["order_signer"]) or "auto"
                if "funder_address" in live and live["funder_address"]:
                    base_settings.FUNDER_ADDRESS = normalize_funder_address(live["funder_address"])
                if "relayer_api_key" in live and live["relayer_api_key"]:
                    base_settings.RELAYER_API_KEY = str(live["relayer_api_key"]).strip()
                if "alchemy_api_key" in live and live["alchemy_api_key"]:
                    base_settings.ALCHEMY_API_KEY = str(live["alchemy_api_key"]).strip()

            # Fallback for legacy standalone or duplicate keys
            if "funder_address" in config_data and not base_settings.FUNDER_ADDRESS:
                base_settings.FUNDER_ADDRESS = normalize_funder_address(config_data["funder_address"])
            if "relayer" in config_data and not base_settings.RELAYER_API_KEY:
                rl = config_data["relayer"]
                if "api_key" in rl: base_settings.RELAYER_API_KEY = str(rl["api_key"]).strip()
            if "copy_trader" in config_data and not config_data.get("copy_fak"):
                ct = config_data["copy_trader"]
                if "retry_interval_ms" in ct: base_settings.COPY_RETRY_INTERVAL_MS = int(ct["retry_interval_ms"])
                if "max_retries" in ct: base_settings.COPY_MAX_RETRIES = int(ct["max_retries"])
                if "resting_timeout_s" in ct: base_settings.COPY_RESTING_TIMEOUT_S = float(ct["resting_timeout_s"])

            if "polymarket" in config_data:
                poly = config_data["polymarket"]
                if "gamma_base_url" in poly: base_settings.GAMMA_BASE_URL = poly["gamma_base_url"]
                if "clob_base_url" in poly: base_settings.CLOB_BASE_URL = poly["clob_base_url"]
                if "live_ws_url" in poly: base_settings.POLYMARKET_LIVE_DATA_WS_URL = poly["live_ws_url"]
                if "series_id" in poly: base_settings.POLYMARKET_SERIES_ID = poly["series_id"]
                if "series_slug" in poly: base_settings.POLYMARKET_SERIES_SLUG = poly["series_slug"]
                if "auto_select_latest" in poly: base_settings.POLYMARKET_AUTO_SELECT_LATEST = poly["auto_select_latest"]
                if "up_label" in poly: base_settings.POLYMARKET_UP_LABEL = poly["up_label"]
                if "down_label" in poly: base_settings.POLYMARKET_DOWN_LABEL = poly["down_label"]

            # TOGGLE: inputs — config.json "inputs". A value that can't be read stays as it comes, with a warning.
            try:
                _in, _in_bad = inputs(config_data.get("inputs") if isinstance(config_data, dict) else None)
                base_settings.INPUTS = _in
                for _w in _in_bad:
                    print(f"Warning: config.json: {_w} (left as it comes)")
            except Exception as e:
                print(f"Warning: Failed to load inputs config: {e}")

            if "trading" in config_data:
                trading = config_data["trading"]
                raw_sym = str(trading.get("symbol") or "").strip().upper()
                base_settings.SYMBOL = raw_sym if raw_sym else "BTCUSDT"
                if "binance_base_url" in trading: base_settings.BINANCE_BASE_URL = trading["binance_base_url"]
                if "candle_window_minutes" in trading: base_settings.CANDLE_WINDOW_MINUTES = trading["candle_window_minutes"]
                if "poll_interval_ms" in trading: base_settings.POLL_INTERVAL_MS = trading["poll_interval_ms"]
                if "risk_type" in trading: base_settings.RISK_TYPE = trading["risk_type"]
                if "risk_value" in trading: base_settings.RISK_VALUE = trading["risk_value"]

            if "ev" in config_data:
                ev = config_data["ev"]
                if "ev_threshold" in ev: base_settings.EV_THRESHOLD = _to_float(ev["ev_threshold"], base_settings.EV_THRESHOLD)
                if "min_prob" in ev: base_settings.MIN_PROB_EV = _to_float(ev["min_prob"], base_settings.MIN_PROB_EV)
                if "min_book_liquidity_usd" in ev: base_settings.MIN_BOOK_LIQUIDITY_USD = _to_float(ev["min_book_liquidity_usd"], base_settings.MIN_BOOK_LIQUIDITY_USD)
                if "max_book_age_s" in ev: base_settings.MAX_BOOK_AGE_S = _to_float(ev["max_book_age_s"], base_settings.MAX_BOOK_AGE_S)
                if "max_odds_enabled" in ev:
                    base_settings.MAX_ODDS_ENABLED = _to_bool(ev["max_odds_enabled"], base_settings.MAX_ODDS_ENABLED)
                if "max_odds_cents" in ev:
                    base_settings.MAX_ODDS_CENTS = _to_float(ev["max_odds_cents"], base_settings.MAX_ODDS_CENTS)
                if "use_clob_ws_book" in ev:
                    base_settings.USE_CLOB_WS_BOOK = _to_bool(ev["use_clob_ws_book"], base_settings.USE_CLOB_WS_BOOK)

            if "settlement" in config_data:
                st = config_data["settlement"]
                if "authoritative_settle_wait_s" in st:
                    base_settings.AUTHORITATIVE_SETTLE_WAIT_S = _to_float(st["authoritative_settle_wait_s"], base_settings.AUTHORITATIVE_SETTLE_WAIT_S)
                # PATCH: official results — true/false, also when written as text ("false" is off)
                if "redeem_after_resolution" in st:
                    base_settings.REDEEM_AFTER_RESOLUTION = _to_bool(st["redeem_after_resolution"], base_settings.REDEEM_AFTER_RESOLUTION)
                if "confirm_official" in st:
                    base_settings.CONFIRM_OFFICIAL = _to_bool(st["confirm_official"], base_settings.CONFIRM_OFFICIAL)

            if "flip" in config_data:
                flip = config_data["flip"]
                if "enabled" in flip: base_settings.FLIP_ENABLED = _to_bool(flip["enabled"], base_settings.FLIP_ENABLED)
                if "min_conviction" in flip: base_settings.FLIP_MIN_CONVICTION = _to_float(flip["min_conviction"], base_settings.FLIP_MIN_CONVICTION)
                if "min_minutes_left" in flip: base_settings.FLIP_MIN_MINUTES_LEFT = _to_float(flip["min_minutes_left"], base_settings.FLIP_MIN_MINUTES_LEFT)

            if "capital_extractor" in config_data:
                ce = config_data["capital_extractor"]
                if "enabled" in ce: base_settings.AUTO_WITHDRAW_ENABLED = _to_bool(ce["enabled"], base_settings.AUTO_WITHDRAW_ENABLED)
                if "trigger_balance" in ce: base_settings.WITHDRAW_TRIGGER_BALANCE = _to_float(ce["trigger_balance"], base_settings.WITHDRAW_TRIGGER_BALANCE)
                if "withdraw_amount" in ce: base_settings.WITHDRAW_AMOUNT = _to_float(ce["withdraw_amount"], base_settings.WITHDRAW_AMOUNT)
                addr = ce.get("withdraw_address") if "withdraw_address" in ce else ce.get("recipient_address")
                if addr is not None: base_settings.WITHDRAW_ADDRESS = str(addr).strip()
                ar = ce.get("auto_resume_after_withdrawal") if "auto_resume_after_withdrawal" in ce else ce.get("auto_resume")
                if ar is not None: base_settings.WITHDRAW_AUTO_RESUME = _to_bool(ar, base_settings.WITHDRAW_AUTO_RESUME)
                if "resume_after" in ce: base_settings.WITHDRAW_RESUME_AFTER = str(ce["resume_after"]).strip()

            if "telegram" in config_data:
                tg = config_data["telegram"]
                if "enabled" in tg: base_settings.TELEGRAM_ENABLED = _to_bool(tg["enabled"], base_settings.TELEGRAM_ENABLED)
                if "bot_token" in tg: base_settings.TELEGRAM_BOT_TOKEN = str(tg["bot_token"]).strip()

            # TOGGLE: entry quote log — config.json "entry_quote". A value that can't be read stays as it comes, with a warning.
            try:
                _eq, _eq_bad = entry_quote(config_data.get("entry_quote") if isinstance(config_data, dict) else None)
                base_settings.ENTRY_QUOTE = _eq
                for _w in _eq_bad:
                    print(f"Warning: config.json: {_w} (left as it comes)")
            except Exception as e:
                print(f"Warning: Failed to load entry_quote config: {e}")

            if "chainlink" in config_data:
                cl = config_data["chainlink"]
                if "polygon_rpc_url" in cl: base_settings.POLYGON_RPC_URL = cl["polygon_rpc_url"]
                if "polygon_wss_url" in cl: base_settings.POLYGON_WSS_URL = cl["polygon_wss_url"]
                if "btc_usd_aggregator" in cl: base_settings.CHAINLINK_BTC_USD_AGGREGATOR = cl["btc_usd_aggregator"]
                if "alchemy_api_key" in cl and not base_settings.ALCHEMY_API_KEY:
                    base_settings.ALCHEMY_API_KEY = str(cl["alchemy_api_key"]).strip()

            if "longshot" in config_data:
                try:
                    from bot import longshot as _longshot
                    base_settings.LONGSHOT = _longshot.normalize(config_data["longshot"])
                except Exception as e:
                    print(f"Warning: Failed to load longshot config: {e}")

            # Legacy flat keys fallback
            if "auto_withdraw_enabled" in config_data and "capital_extractor" not in config_data:
                base_settings.AUTO_WITHDRAW_ENABLED = _to_bool(config_data["auto_withdraw_enabled"], base_settings.AUTO_WITHDRAW_ENABLED)
            if "withdraw_trigger_balance" in config_data and "capital_extractor" not in config_data:
                base_settings.WITHDRAW_TRIGGER_BALANCE = _to_float(config_data["withdraw_trigger_balance"], base_settings.WITHDRAW_TRIGGER_BALANCE)
            if "withdraw_amount" in config_data and "capital_extractor" not in config_data:
                base_settings.WITHDRAW_AMOUNT = _to_float(config_data["withdraw_amount"], base_settings.WITHDRAW_AMOUNT)
            if "withdraw_address" in config_data and "capital_extractor" not in config_data:
                base_settings.WITHDRAW_ADDRESS = str(config_data["withdraw_address"]).strip()
            if "telegram_enabled" in config_data and "telegram" not in config_data:
                base_settings.TELEGRAM_ENABLED = _to_bool(config_data["telegram_enabled"], base_settings.TELEGRAM_ENABLED)
            if "telegram_bot_token" in config_data and "telegram" not in config_data:
                base_settings.TELEGRAM_BOT_TOKEN = str(config_data["telegram_bot_token"]).strip()

        except Exception as e:
            print(f"Warning: Failed to load config.json: {e}")

    return base_settings

# TOGGLE: inputs — config.json "inputs". How the bot takes in its feeds: a log of how late their messages arrive, and
# six ways to read them. Everything is off as it comes, and the feeds are then read exactly as before.
INPUTS_DEFAULTS: Dict[str, Any] = {
    "delay_log": False,     # True: FEED DELAY lines: how late each feed's messages arrive, for every paper bet and as a summary
    "summary_min": 10.0,    # minutes between the FEED DELAY summary lines while the log is on; 0 = no summary
    "binance_both": False,  # True: both of Binance's addresses at once for each stream; the first copy of a message is used
    "binance_first": "",    # while one address is used, the one tried first: "stream.binance.com" or "data-stream.binance.vision"; "" = as it was
    "book_both": False,     # True: a second connection to Polymarket's order-book feed; the book that is further on is the one read
    "book_light": False,    # True: a price change is put into the sorted book where it belongs, instead of the whole book being rebuilt
    "book_thread": False,   # True: Polymarket's order-book feed in a thread of its own
    "top_of_book": False,   # True: Polymarket also sends a best_bid_ask message when the top of a book changes
}
INPUTS_FIRST = ("", "stream.binance.com", "data-stream.binance.vision")


def inputs(block: Any) -> Tuple[Dict[str, Any], List[str]]:
    """TOGGLE: inputs — an "inputs" block, read: (the eight values, what could not be read)."""
    out, bad = dict(INPUTS_DEFAULTS), []
    if block is None:
        return out, bad
    if not isinstance(block, dict):
        return out, ["inputs must be an object"]
    for key in ("delay_log", "binance_both", "book_both", "book_light", "book_thread", "top_of_book"):
        v = block.get(key)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        if isinstance(v, str):
            v = {"true": True, "on": True, "yes": True, "1": True, "false": False, "off": False, "no": False, "0": False}.get(v.strip().lower())
        if isinstance(v, (bool, int, float)) and v in (0, 1):
            out[key] = bool(v)
        else:
            bad.append(f"inputs.{key} must be true or false")
    v = block.get("summary_min")
    if not (v is None or (isinstance(v, str) and not v.strip())):
        try:
            n = float(v)
            if isinstance(v, bool) or n != n or n in (float("inf"), float("-inf")):
                raise ValueError(v)
            out["summary_min"] = n
        except (TypeError, ValueError):
            bad.append("inputs.summary_min must be a number")
    v = block.get("binance_first")
    if v is not None:
        w = v.strip().lower() if isinstance(v, str) else None
        if w in INPUTS_FIRST:
            out["binance_first"] = w
        else:
            bad.append('inputs.binance_first must be "stream.binance.com", "data-stream.binance.vision" or "" (as it was)')
    return out, bad


settings = load_settings()
