# bybit_option_hub.py
"""
Единая профессиональная библиотека для работы с опционами Bybit
(main- и sub-аккаунты, REST-торговля, WebSocket-маркет-фид, option-chain,
оценка реальной цены исполнения объёма и др.).

Основные слои
-------------
1. _SessionPool      – потокобезопасный пул HTTP-сессий (main + sub).
2. data-models       – обёртки над snapshot’ами (тикер, стакан).
3. MarketDataFeed    – singleton-сервис для WS-подписок с кэшем.
4. TradingAPI        – REST-вызовы (ордера, позиции, баланс…).
5. ChainUtils        – compact/efficient option-chain snapshot’ы.
6. PriceEstimator    – расчёт VWAP/worst по REST- или локальному стакану.
7. BybitOptionHub    – единый фасад «инициализировать → пользоваться».

© 2025 Andrew (<andrew@example.com>)
"""

from __future__ import annotations

# ----------------------------------------------------------------------
#                          IMPORTS & CONSTANTS
# ----------------------------------------------------------------------
import os
import threading
import time
import json
import logging
import queue
from datetime import datetime, timedelta, timezone, date
from decimal   import Decimal, ROUND_DOWN, InvalidOperation
from typing    import Any, Dict, List, Iterable, Optional, Set, Tuple, Union

from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket

__all__ = [
    "BybitOptionHub",
    "TradingAPI",
    "MarketDataFeed",
    "ChainUtils",
    "PriceEstimator",
]

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT      = 10          # сек.
_DEFAULT_RECV_WINDOW  = 5_000       # мс.
_DEFAULT_ACCOUNT_TYPE = "UNIFIED"

# ----------------------------------------------------------------------
#                            1.  _SessionPool
# ----------------------------------------------------------------------
class _SessionPool:
    """
    Потокобезопасный пул HTTP-сессий Bybit.

    Индексация:
        1  – main-аккаунт  (BUYBIT_API / BUYBIT_SECRET)
        2+ – sub-аккаунты (BUYBIT_SUB_N / BUYBIT_SUB_SECRET_N)
    """
    _sessions: Dict[int, HTTP] = {}
    _lock = threading.RLock()

    @classmethod
    def _load_env_pairs(cls) -> Dict[int, Dict[str, str]]:
        load_dotenv()
        mapping: Dict[int, Dict[str, str]] = {}

        # main
        if os.getenv("BUYBIT_API") and os.getenv("BUYBIT_SECRET"):
            mapping[1] = {
                "api_key":    os.getenv("BUYBIT_API"),
                "api_secret": os.getenv("BUYBIT_SECRET"),
            }

        # subs
        i = 1
        while True:
            k = os.getenv(f"BUYBIT_SUB_{i}")
            s = os.getenv(f"BUYBIT_SUB_SECRET_{i}")
            if not k or not s:
                break
            mapping[i + 1] = {"api_key": k, "api_secret": s}
            i += 1
        return mapping

    # ------------------------------------------------------------------
    @classmethod
    def build_all(cls, *, testnet: bool = False, **http_kwargs: Any) -> None:
        creds_map = cls._load_env_pairs()
        if not creds_map:
            raise RuntimeError(
                "Не найдены API-ключи в .env (BUYBIT_API / BUYBIT_SECRET и др.)."
            )

        with cls._lock:
            for idx, creds in creds_map.items():
                if idx not in cls._sessions:
                    cls._sessions[idx] = HTTP(
                        **creds,
                        testnet=testnet,
                        timeout=_DEFAULT_TIMEOUT,
                        recv_window=_DEFAULT_RECV_WINDOW,
                        **http_kwargs,
                    )
                    log.debug("HTTP-сессия Bybit создана idx=%d testnet=%s", idx, testnet)

    @classmethod
    def get(cls, idx: int = 1) -> HTTP:
        try:
            return cls._sessions[idx]
        except KeyError as e:
            raise RuntimeError(
                f"Сессия для account_idx={idx} не инициализирована. "
                "Сначала вызовите BybitOptionHub.initialise()."
            ) from e

# ----------------------------------------------------------------------
#                  2.  DATA MODELS  (тикер, стакан и др.)
# ----------------------------------------------------------------------
def _to_decimal(v: Any) -> Optional[Decimal]:
    """Преобразовать строку/число в Decimal; вернуть None при пустом значении."""
    if v in (None, "", "0" * 0):
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return None


class OptionTickerSnapshot(dict):
    """
    Снимок тикера опциона Bybit.  
    Дополнительно содержит `updated_ts` и `hours_to_exp`.
    """
    __slots__ = ("updated_ts",)

    def __init__(self, data: Dict[str, Any], updated_ts: int) -> None:
        super().__init__(data)
        self.updated_ts = updated_ts

    # --- базовые поля (bid/ask/mark/last) -------------------------
    @property
    def bid(self) -> Optional[Decimal]:
        return _to_decimal(self.get("bidPrice") or self.get("bid1Price"))

    @property
    def bid_size(self) -> Optional[Decimal]:
        return _to_decimal(self.get("bidSize") or self.get("bid1Size"))

    @property
    def ask(self) -> Optional[Decimal]:
        return _to_decimal(self.get("askPrice") or self.get("ask1Price"))

    @property
    def ask_size(self) -> Optional[Decimal]:
        return _to_decimal(self.get("askSize") or self.get("ask1Size"))

    @property
    def mark(self) -> Optional[Decimal]:
        return _to_decimal(self.get("markPrice"))

    @property
    def last(self) -> Optional[Decimal]:
        return _to_decimal(self.get("lastPrice"))

    @property
    def deliveryTime(self) -> Optional[float]:
        return float(self.get("deliveryTime"))

    # --- greeks ----------------------------------------------------
    delta  = property(lambda self: _to_decimal(self.get("delta")))
    gamma  = property(lambda self: _to_decimal(self.get("gamma")))
    vega   = property(lambda self: _to_decimal(self.get("vega")))
    theta  = property(lambda self: _to_decimal(self.get("theta")))

    # --- время до экспирации --------------------------------------
    @property
    def hours_to_exp(self) -> Optional[float]:
        val = self.get("hours_to_exp")
        try:
            return float(val) if val is not None else None
        except (TypeError, ValueError):
            return None


class OptionOrderbookSnapshot:
    """
    Локальный снимок стакана (bids/asks).  
    Умеет оценивать VWAP / worst price для любого объёма.
    """
    __slots__ = ("bids", "asks", "updated_ts")

    def __init__(
        self,
        bids: List[Tuple[Decimal, Decimal]],
        asks: List[Tuple[Decimal, Decimal]],
        updated_ts: int,
    ) -> None:
        self.bids = sorted(bids, key=lambda x: x[0], reverse=True)  # bid DESC
        self.asks = sorted(asks, key=lambda x: x[0])                # ask ASC
        self.updated_ts = updated_ts

    # -- лучшие уровни ---------------------------------------------
    def best_bid(self) -> Optional[Tuple[Decimal, Decimal]]:
        return self.bids[0] if self.bids else None

    def best_ask(self) -> Optional[Tuple[Decimal, Decimal]]:
        return self.asks[0] if self.asks else None

    # -- внутренний расчёт -----------------------------------------
    @staticmethod
    def _fill_price(
        levels: List[Tuple[Decimal, Decimal]],
        qty: Decimal,
    ) -> Optional[Tuple[Decimal, Decimal]]:
        if qty <= 0 or not levels:
            return None
        left   = qty
        cost   = Decimal("0")
        worst  = Decimal("0")
        worst_level_size = Decimal("0")

        for px, sz in levels:
            if left <= 0:
                break
            take  = sz if sz <= left else left
            cost += take * px
            worst = px
            worst_level_size = sz
            left -= take

        return None if left > 0 else (cost, worst, worst_level_size)

    # -- публичные методы ------------------------------------------
    @staticmethod
    def _to_dec(v: Union[int, float, str, Decimal]) -> Decimal:
        try:
            return v if isinstance(v, Decimal) else Decimal(str(v))
        except Exception:
            raise ValueError("qty must be numeric") from None

    def bid_price_for_qty(
        self,
        qty: Union[int, float, str, Decimal],
        *,
        vwap: bool = True,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        qty_d = self._to_dec(qty)
        res = self._fill_price(self.bids, qty_d)
        if res is None:
            return 0, 0
        cost, worst, worst_lvl_sz = res
        price = cost / qty_d if vwap else worst
        return price, worst_lvl_sz

    def ask_price_for_qty(
        self,
        qty: Union[int, float, str, Decimal],
        *,
        vwap: bool = True,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        qty_d = self._to_dec(qty)
        res = self._fill_price(self.asks, qty_d)
        if res is None:
            return None, None
        cost, worst, worst_lvl_sz = res
        price = cost / qty_d if vwap else worst
        return price, worst_lvl_sz

    # -- алиасы -----------------------------------------------------
    vwap_buy        = lambda self, q: self.ask_price_for_qty(q, vwap=True)
    vwap_sell       = lambda self, q: self.bid_price_for_qty(q, vwap=True)
    worst_buy_price = lambda self, q: self.ask_price_for_qty(q, vwap=False)[0]
    worst_sell_price= lambda self, q: self.bid_price_for_qty(q, vwap=False)[0]

    # -- агрегаты ----------------------------------------------------
    @property
    def total_bid_size(self) -> Decimal:
        return sum((q for _, q in self.bids), Decimal("0"))

    @property
    def total_ask_size(self) -> Decimal:
        return sum((q for _, q in self.asks), Decimal("0"))


# ----------------------------------------------------------------------
#                    3.  MARKET DATA FEED (WebSocket)
# ----------------------------------------------------------------------
class _OptionDataCache:
    """
    Потокобезопасный кэш последних тикеров, стаканов и instrument-info.
    """
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tickers: Dict[str, OptionTickerSnapshot] = {}
        self._orderbooks: Dict[str, OptionOrderbookSnapshot] = {}
        self._instrument_info: Dict[str, Dict[str, Any]] = {}
        self._portfolio_greeks: Dict[str, Dict[str, Any]] = {}

    # -- instrument-info -------------------------------------------
    def update_instrument(self, symbol: str, info: Dict[str, Any]) -> None:
        with self._lock:
            self._instrument_info[symbol] = info

    def get_instrument(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._instrument_info.get(symbol)

    # -- ticker ----------------------------------------------------
    def update_ticker(self, symbol: str, data: Dict[str, Any], ts: int) -> None:
        with self._lock:
            self._tickers[symbol] = OptionTickerSnapshot(data, ts)

    def get_ticker(self, symbol: str) -> Optional[OptionTickerSnapshot]:
        with self._lock:
            return self._tickers.get(symbol)

    # -- orderbook -------------------------------------------------
    def update_orderbook(
        self,
        symbol: str,
        bids_raw: Iterable[Iterable[str]],
        asks_raw: Iterable[Iterable[str]],
        ts: int,
    ) -> None:
        bids, asks = [], []
        for p, q in bids_raw:
            dp, dq = _to_decimal(p), _to_decimal(q)
            if dp is not None and dq is not None:
                bids.append((dp, dq))
        for p, q in asks_raw:
            dp, dq = _to_decimal(p), _to_decimal(q)
            if dp is not None and dq is not None:
                asks.append((dp, dq))
        with self._lock:
            self._orderbooks[symbol] = OptionOrderbookSnapshot(bids, asks, ts)

    def get_orderbook(self, symbol: str) -> Optional[OptionOrderbookSnapshot]:
        with self._lock:
            return self._orderbooks.get(symbol)

    # -- portfolio greeks -----------------------------------------
    def update_portfolio_greeks(self, base_coin: str, data: Dict[str, Any]) -> None:
        with self._lock:
            self._portfolio_greeks[base_coin] = data

    def get_portfolio_greeks(self, base_coin: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._portfolio_greeks.get(base_coin)


class MarketDataFeed:
    """
    Singleton-сервис WebSocket V5 для опционов Bybit.  
    Позволяет динамически подписывать символы и хранит кэш последних snapshot’ов.
    """
    _INSTANCE: "MarketDataFeed | None" = None
    _LOCK = threading.Lock()

    # ---------------- Constructor ---------------------------------
    def __init__(
        self,
        *,
        testnet: bool = False,
        orderbook_depth: int = 25,
        subscribe_orderbook: bool = True,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        ping_interval: int = 20,
        ping_timeout: int = 10,
        retries: int = 10,
    ) -> None:
        self.testnet            = testnet
        self.orderbook_depth    = orderbook_depth
        self.subscribe_orderbook= subscribe_orderbook
        self.api_key            = api_key
        self.api_secret         = api_secret
        self.ping_interval      = ping_interval
        self.ping_timeout       = ping_timeout
        self.retries            = retries

        self.cache = _OptionDataCache()

        self._ws_pub: Optional[WebSocket] = None
        self._ws_priv: Optional[WebSocket] = None
        self._subscribed: set[str] = set()
        self._pending: "queue.Queue[str]" = queue.Queue()
        self._stop_evt = threading.Event()
        self._thr = threading.Thread(
            target=self._run,
            name="BybitOptionFeed",
            daemon=True,
        )

        self._pub_init_lock   = threading.Lock()
        self._priv_init_lock  = threading.Lock()

    # ---------------- Singleton access ----------------------------
    @classmethod
    def instance(cls, **kwargs: Any) -> "MarketDataFeed":
        if cls._INSTANCE:
            return cls._INSTANCE
        with cls._LOCK:
            if cls._INSTANCE:
                return cls._INSTANCE
            cls._INSTANCE = cls(**kwargs)
            cls._INSTANCE.start()
            return cls._INSTANCE

    # ---------------- Lifecycle -----------------------------------
    def start(self) -> None:
        if not self._thr.is_alive():
            self._thr.start()

    def stop(self, timeout: Optional[float] = None) -> None:
        self._stop_evt.set()
        self._thr.join(timeout=timeout)
        for ws in (self._ws_pub, self._ws_priv):
            if ws:
                try:
                    ws.exit()
                except Exception:  # noqa
                    pass
        MarketDataFeed._INSTANCE = None

    # ---------------- Public API ----------------------------------
    def add_symbol(self, symbol: str) -> None:
        self._pending.put(symbol.upper())

    def add_symbols(self, symbols: Iterable[str]) -> None:
        for s in symbols:
            self.add_symbol(s)

    def get_ticker(self, symbol: str) -> Optional[OptionTickerSnapshot]:
        return self.cache.get_ticker(symbol.upper())

    def get_orderbook(self, symbol: str) -> Optional[OptionOrderbookSnapshot]:
        return self.cache.get_orderbook(symbol.upper())

    def get_portfolio_greeks(self, base_coin: str) -> Optional[Dict[str, Any]]:
        return self.cache.get_portfolio_greeks(base_coin.upper())

    # ---------------- Internal helpers ----------------------------
    def _lazy_init_pub(self) -> None:
        if self._ws_pub:
            return
        with self._pub_init_lock:
            if self._ws_pub:
                return
            self._ws_pub = WebSocket(
                testnet=self.testnet,
                channel_type="option",
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                retries=self.retries,
            )
            log.info("Public WS создан (testnet=%s).", self.testnet)

    def _lazy_init_priv(self) -> None:
        if not (self.api_key and self.api_secret):
            return
        if self._ws_priv:
            return
        with self._priv_init_lock:
            if self._ws_priv:
                return
            self._ws_priv = WebSocket(
                testnet=self.testnet,
                channel_type="private",
                api_key=self.api_key,
                api_secret=self.api_secret,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                retries=self.retries,
            )
            self._ws_priv.greek_stream(callback=self._on_portfolio_greeks)
            log.info("Private WS создан (portfolio greeks).")

    # ---------------- Thread main loop ----------------------------
    def _run(self) -> None:
        import queue
        self._lazy_init_pub()
        self._lazy_init_priv()

        while not self._stop_evt.is_set():
            try:
                sym = self._pending.get(timeout=0.5)
            except queue.Empty:
                sym = None

            if sym:
                self._subscribe_symbol(sym)

            time.sleep(0.01)

        log.info("MarketDataFeed thread stopped.")

    # ---------------- Subscription -------------------------------
    def _subscribe_symbol(self, symbol: str) -> None:
        if symbol in self._subscribed:
            return
        if not self._ws_pub:
            return

        # 1. REST – instrument-info
        try:
            http = HTTP(testnet=self.testnet)
            resp = http.get_instruments_info(category="option", symbol=symbol)
            info = (resp.get("result") or {}).get("list") or []
            if info:
                self.cache.update_instrument(symbol, info[0])
        except Exception as exc:  # noqa
            log.exception("instrument-info fetch failed for %s: %s", symbol, exc)

        # 2. WS – ticker
        try:
            self._ws_pub.ticker_stream(symbol=symbol, callback=self._on_ticker)
        except Exception as exc:  # noqa
            log.exception("ticker_stream subscribe error %s: %s", symbol, exc)
        else:
            log.info("Subscribed ticker %s", symbol)

        # 3. WS – orderbook
        if self.subscribe_orderbook:
            try:
                self._ws_pub.orderbook_stream(
                    depth=self.orderbook_depth,
                    symbol=symbol,
                    callback=self._on_orderbook,
                )
            except Exception as exc:  # noqa
                log.exception("orderbook_stream subscribe error %s: %s", symbol, exc)
            else:
                log.info("Subscribed orderbook.%d %s", self.orderbook_depth, symbol)

        self._subscribed.add(symbol)

    # ---------------- WS callbacks --------------------------------
    def _on_ticker(self, msg: Dict[str, Any]) -> None:
        try:
            topic  = msg["topic"]                      # "tickers.SYM"
            symbol = topic.split(".", 1)[1]
            data   = dict(msg["data"])                # copy
            ts     = int(msg.get("ts") or data.get("ts") or time.time() * 1000)
        except Exception:
            log.exception("Malformed ticker message: %s", msg)
            return

        # deliveryTime and hours_to_exp
        if "deliveryTime" not in data:
            info = self.cache.get_instrument(symbol)
            if info and info.get("deliveryTime"):
                data["deliveryTime"] = info["deliveryTime"]

        if "deliveryTime" in data:
            try:
                now_ms       = int(time.time() * 1000)
                diff_ms      = int(data["deliveryTime"]) - now_ms
                data["hours_to_exp"] = round(diff_ms / 3_600_000.0, 4)
            except Exception:  # noqa
                pass

        self.cache.update_ticker(symbol, data, ts)

    def _on_orderbook(self, msg: Dict[str, Any]) -> None:
        try:
            topic  = msg["topic"]                     # "orderbook.<d>.SYM"
            symbol = topic.split(".", 2)[2]
            data   = msg["data"]
            ts     = int(msg.get("ts") or time.time() * 1000)
            bids   = data.get("b", [])
            asks   = data.get("a", [])
        except Exception:
            log.exception("Malformed orderbook message: %s", msg)
            return

        self.cache.update_orderbook(symbol, bids, asks, ts)

    def _on_portfolio_greeks(self, msg: Dict[str, Any]) -> None:
        try:
            for row in msg.get("data", []):
                if base := row.get("baseCoin"):
                    self.cache.update_portfolio_greeks(base, row)
        except Exception:
            log.exception("Malformed portfolio greeks: %s", msg)

# ----------------------------------------------------------------------
#                       4.  TRADING API (REST)
# ----------------------------------------------------------------------

_SPEC_CACHE: dict[str, dict[str, Any]] = {}

def _get_spec(symbol: str, *, client: Optional[HTTP] = None) -> Dict[str, Any]:
    """Вернуть спецификацию опциона (priceFilter и др.) с кэшированием."""
    if symbol in _SPEC_CACHE:
        return _SPEC_CACHE[symbol]
    cli = client or _SessionPool.get(1)
    info = cli.get_instruments_info(category="option", symbol=symbol)
    lst = info.get("result", {}).get("list", [])
    if not lst:
        raise RuntimeError(f"Инструмент {symbol!r} не найден в Bybit Options.")
    _SPEC_CACHE[symbol] = lst[0]
    return lst[0]

def _format_price(symbol: str, raw_price: float | Decimal) -> str:
    spec      = _get_spec(symbol)
    tick      = Decimal(spec["priceFilter"]["tickSize"])
    precision = abs(tick.as_tuple().exponent)

    price_dec  = Decimal(str(raw_price))
    steps      = (price_dec / tick).quantize(Decimal(0), ROUND_DOWN)
    final      = steps * tick
    return f"{final:.{precision}f}"

def _to_number(v: Any) -> Optional[Union[int, float]]:
    """
    Конвертировать строковое числовое значение → int или float.
    Пустые строки и None → None.  Нечисловое оставляем как есть.
    """
    if v in (None, "", "null", "None"):
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        # int ?
        if v.isdigit() or (v.startswith('-') and v[1:].isdigit()):
            try:
                return int(v)
            except Exception:  # noqa: BLE001
                pass
        # float
        try:
            return float(v)
        except Exception:  # noqa: BLE001
            return v
    return v


class TradingAPI:
    """
    REST-обёртка над основными торговыми методами категории «option».
    Использует _SessionPool.
    """

    # ------------------------------------------------------------------
    #                       POSITIONS / BALANCE
    # ------------------------------------------------------------------
    @staticmethod
    def get_open_positions(*,
                           account_idx: int = 1,
                           underlying: Optional[str] = None
                           ) -> List[Dict[str, Any]]:
        """
        Возвращает список открытых позиций.  
        Все строковые числовые поля автоматически преобразованы к int/float.
        """
        res = _SessionPool.get(account_idx).get_positions(category="option")
        raw_list = res.get("result", {}).get("list", [])

        filtered = [
            p for p in raw_list
            if not underlying or p["symbol"].startswith(underlying)
        ]

        # --- числовое преобразование ---------------------------------
        out: List[Dict[str, Any]] = []
        for pos in filtered:
            conv = {k: _to_number(v) for k, v in pos.items()}
            out.append(conv)
        return out

    @staticmethod
    def get_wallet_balance(*, account_idx: int = 1) -> Dict[str, Any]:
        return _SessionPool.get(account_idx).get_wallet_balance(
            accountType=_DEFAULT_ACCOUNT_TYPE)

    @staticmethod
    def get_total_equity(*, account_idx: int = 1) -> float:
        resp = TradingAPI.get_wallet_balance(account_idx=account_idx)
        try:
            return float(resp["result"]["list"][0]["totalEquity"])
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"Не удалось извлечь totalEquity: {e}") from e

    # ------------------------------------------------------------------
    #                       MARKET DATA (REST)
    # ------------------------------------------------------------------
    @staticmethod
    def get_option_chain_snapshot(underlying: str,
                                  *,
                                  account_idx: int = 1,
                                  with_greeks: bool = True
                                  ) -> List[Dict[str, Any]]:
        cli       = _SessionPool.get(account_idx)
        base_coin = underlying.upper().replace("USDT", "").replace("USDC", "")

        instruments = cli.get_instruments_info(
            category="option",
            baseCoin=base_coin,
        ).get("result", {}).get("list", [])

        tickers = cli.get_tickers(
            category="option",
            baseCoin=base_coin,
        ).get("result", {}).get("list", [])

        tmap = {t["symbol"]: t for t in tickers}
        out  = []
        for inst in instruments:
            merged = {**inst, **tmap.get(inst["symbol"], {})}
            if not with_greeks:
                for g in ("delta", "gamma", "vega", "theta", "rho", "iv"):
                    merged.pop(g, None)
            out.append(merged)
        return out

    # ------------------------------------------------------------------
    #                              KLINE
    # ------------------------------------------------------------------
    @staticmethod
    def get_kline(symbol: str,
                  *,
                  account_idx: int = 1,
                  interval: str = "1",
                  bars: int = 500) -> List[List[Any]]:
        cli      = _SessionPool.get(account_idx)
        end_ms   = int(time.time() * 1000)
        start_ms = end_ms - bars * int(interval) * 60_000
        res = cli.get_kline(category="option",
                            symbol=symbol,
                            interval=interval,
                            start=start_ms,
                            end=end_ms)
        return res.get("result", {}).get("list", [])

    # ------------------------------------------------------------------
    #                              ORDERS
    # ------------------------------------------------------------------
    @staticmethod
    def place_market_order(*,
                           account_idx: int = 1,
                           symbol: str,
                           side: str,
                           qty: float,
                           order_link_id: Optional[str] = None,
                           by_iv: bool = False,
                           iv: Optional[float] = None
                           ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "category": "option",
            "symbol":   symbol,
            "side":     side,
            "orderType": "Market",
            "qty":       str(qty),
            "timeInForce": "IOC",
        }
        if by_iv:
            payload["orderIv"] = "" if iv is None else str(iv)
        else:
            payload["price"] = "0"
        if order_link_id:
            payload["orderLinkId"] = order_link_id
        return _SessionPool.get(account_idx).place_order(**payload)

    @staticmethod
    def place_limit_order(*,
                          account_idx: int = 1,
                          symbol: str,
                          side: str,
                          price: float,
                          qty: float,
                          time_in_force: str = "GTC",
                          order_link_id: Optional[str] = None
                          ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "category": "option",
            "symbol":   symbol,
            "side":     side,
            "orderType": "Limit",
            "price":    _format_price(symbol, price),
            "qty":      str(qty),
            "timeInForce": time_in_force,
        }
        if order_link_id:
            payload["orderLinkId"] = order_link_id
        return _SessionPool.get(account_idx).place_order(**payload)

    @staticmethod
    def place_take_profit_limit_order(*,
                                      account_idx: int = 1,
                                      symbol: str,
                                      qty: float,
                                      trigger_price: float,
                                      side: str = "Sell",
                                      order_link_id: Optional[str] = None
                                      ) -> Dict[str, Any]:
        trigger_direction = 1 if side == "Sell" else 2
        payload: Dict[str, Any] = {
            "category":         "option",
            "symbol":           symbol,
            "side":             side,
            "orderType":        "Limit",
            "price":            _format_price(symbol, trigger_price),
            "qty":              str(qty),
            "triggerPrice":     _format_price(symbol, trigger_price),
            "triggerDirection": trigger_direction,
            "reduceOnly":       True,
        }
        if order_link_id:
            payload["orderLinkId"] = order_link_id
        return _SessionPool.get(account_idx).place_order(**payload)

    @staticmethod
    def cancel_all_orders(*,
                          account_idx: int = 1,
                          underlying: Optional[str] = None
                          ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"category": "option"}
        if underlying:
            payload["baseCoin"] = underlying.replace("USDT", "")
        return _SessionPool.get(account_idx).cancel_all_orders(**payload)

    @staticmethod
    def cancel_order(*,
                     account_idx: int = 1,
                     symbol: str,
                     order_id: Optional[str] = None,
                     order_link_id: Optional[str] = None
                     ) -> Dict[str, Any]:
        if not (order_id or order_link_id):
            raise ValueError("Нужно указать order_id или order_link_id.")
        return _SessionPool.get(account_idx).cancel_order(
            category="option",
            symbol=symbol,
            orderId=order_id,
            orderLinkId=order_link_id,
        )

    # ------------------------------------------------------------------
    #                              MISC
    # ------------------------------------------------------------------
    @staticmethod
    def get_server_time(*, account_idx: int = 1) -> int:
        return _SessionPool.get(account_idx).get_server_time()["time"]


# ----------------------------------------------------------------------
#                 5.  CHAIN UTILITIES  (numeric output)
# ----------------------------------------------------------------------
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

def _parse_expiry_dt(symbol: str) -> Optional[datetime]:
    try:
        token = symbol.split("-")[1].upper()  # 17JUL25
        day   = int(token[:-5])
        mon   = _MONTHS[token[-5:-2]]
        year  = 2000 + int(token[-2:])
        return datetime(year, mon, day, tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None

# ---------- helpers для числового преобразования -----------------------

def _to_float(v: Any) -> Optional[float]:
    """
    Безопасно конвертировать в float.
    Строки '', 'null', 'None' => None.
    """
    if v in (None, "", "null", "None"):
        return None
    try:
        return float(v)
    except Exception:  # noqa: BLE001
        return None

def _floatify_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Идём по словарю и пытаемся превратить строковые числа в float.
    """
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, str):
            fv = _to_float(v)
            out[k] = fv if fv is not None else v
        else:
            out[k] = v
    return out

class ChainUtils:
    """
    Снимки option-chain и SNAP по symbol.

    Все числовые значения сразу приводятся к float для удобства downstream-кода.
    """

    # ----------------------- нормализация тикера -------------------
    @staticmethod
    def _norm_fields(t: Dict[str, Any]) -> Dict[str, Any]:
        def _pick(src: Dict[str, Any], *keys: str) -> Optional[Any]:
            for k in keys:
                val = src.get(k)
                if val not in (None, "", "0"*0):
                    return val
            return None

        raw = {
            "bidPrice":  _pick(t, "bidPrice",  "bid1Price",  "bestBidPrice", "b"),
            "bidSize":   _pick(t, "bidSize",   "bid1Size",   "bestBidSize"),
            "askPrice":  _pick(t, "askPrice",  "ask1Price",  "bestAskPrice", "a"),
            "askSize":   _pick(t, "askSize",   "ask1Size",   "bestAskSize"),
            "lastPrice": _pick(t, "lastPrice", "lp"),
            "markPrice": _pick(t, "markPrice"),
            "underlyingPrice": _pick(t, "underlyingPrice", "indexPrice"),
            "delta": _pick(t, "delta"),
            "gamma": _pick(t, "gamma"),
            "vega":  _pick(t, "vega"),
            "theta": _pick(t, "theta"),
            "iv":    _pick(t, "markPriceIv", "iv"),
        }
        return {k: _to_float(v) for k, v in raw.items()}

    # ----------------------- публичные методы ---------------------
    @staticmethod
    def get_chain_full(underlying: str,
                       days: int = 2,
                       *,
                       testnet: bool = False,
                       with_greeks: bool = True,
                       http_session: Optional[HTTP] = None
                       ) -> List[Dict[str, Any]]:
        """
        Снимок ближайших `days` дней.  Все числа → float / None.
        """
        if days <= 0:
            raise ValueError("days must be positive")

        sess = http_session or HTTP(testnet=testnet)
        base = underlying.upper().replace("USDT", "").replace("USDC", "")
        now  = datetime.now(timezone.utc)

        # -- expDate токены (ддМММГГ) ---------------------------------
        tokens = [
            f"{(now + timedelta(days=i)).day}"
            f"{(now + timedelta(days=i)).strftime('%b').upper()}"
            f"{(now.year % 100):02d}"
            for i in range(days)
        ]

        # -- tickers --------------------------------------------------
        tickers: List[Dict[str, Any]] = []
        for tok in tokens:
            r = sess.get_tickers(category="option", baseCoin=base, expDate=tok)
            tickers.extend(r.get("result", {}).get("list", []) or [])

        if not tickers:
            return []

        tmap = {t["symbol"]: t for t in tickers}
        need_syms = set(tmap)

        # -- instruments-info (минимально) ----------------------------
        inst_map: Dict[str, Dict[str, Any]] = {}
        cursor: Optional[str] = None
        while len(inst_map) < len(need_syms):
            params: Dict[str, Any] = {
                "category": "option",
                "baseCoin": base,
                "limit": 1000,
            }
            if cursor:
                params["cursor"] = cursor
            res = sess.get_instruments_info(**params).get("result", {}) or {}
            for inst in res.get("list", []) or []:
                sym = inst.get("symbol")
                if sym in need_syms:
                    inst_map[sym] = inst
            cursor = res.get("nextPageCursor")
            if not cursor:
                break
        # точечный догруз
        for sym in need_syms - inst_map.keys():
            try:
                r = sess.get_instruments_info(category="option", symbol=sym)
                inst_map[sym] = (r.get("result", {}).get("list") or [{}])[0]
            except Exception:
                inst_map[sym] = {}

        # -- формируем SNAP ------------------------------------------
        out: List[Dict[str, Any]] = []
        for sym, tick in tmap.items():
            inst   = inst_map.get(sym, {})
            norm   = ChainUtils._norm_fields(tick)
            strike = _to_float(sym.split("-")[-3]) or 0.0
            is_put = (inst.get("optionsType") or sym.split("-")[-2]).upper().startswith("P")

            ul_px  = norm["underlyingPrice"]
            intrinsic = (
                max(strike - ul_px, 0.0) if is_put else max(ul_px - strike, 0.0)
            ) if ul_px is not None else 0.0

            ask_px = norm["askPrice"] or 0.0
            extrinsic = max(ask_px - intrinsic, 0.0)
            extr_frac = extrinsic / ask_px if ask_px else None

            snap: Dict[str, Any] = {
                "symbol": sym,
                "optionsType": "Put" if is_put else "Call",
                "strike": strike,
                **norm,
                "deliveryTime": _to_float(inst.get("deliveryTime")),
                "launchTime":   _to_float(inst.get("launchTime")),
                "intrinsicNow": intrinsic,
                "extrinsicNow": extrinsic,
                "extrinsicFrac": extr_frac,
            }
            if not with_greeks:
                for g in ("delta", "gamma", "vega", "theta", "iv"):
                    snap.pop(g, None)

            out.append(snap)

        out.sort(key=lambda d: (d["deliveryTime"] or 0, d["strike"]))
        return out

    @staticmethod
    def get_snapshot(symbol: str,
                     *,
                     testnet: bool = False,
                     with_greeks: bool = True,
                     http_session: Optional[HTTP] = None
                     ) -> Optional[Dict[str, Any]]:
        """
        SNAP по одному symbol.  Числа → float / None.
        """
        sess = http_session or HTTP(testnet=testnet)
        try:
            inst = (sess.get_instruments_info(category="option", symbol=symbol)
                        .get("result", {}).get("list", []) or [None])[0]
            if not inst:
                return None
        except Exception:
            return None

        try:
            tick = (sess.get_tickers(category="option", symbol=symbol)
                        .get("result", {}).get("list", []) or [{}])[0]
        except Exception:
            tick = {}

        norm = ChainUtils._norm_fields(tick)
        if not with_greeks:
            for g in ("delta", "gamma", "vega", "theta", "iv"):
                norm.pop(g, None)

        strike = _to_float(symbol.split("-")[-3]) or 0.0
        out = {
            "symbol": symbol,
            "strike": strike,
            **_floatify_dict(inst),
            **norm,
        }
        return out


# ----------------------------------------------------------------------
#                 6.  PRICE ESTIMATOR  (VWAP / worst)
# ----------------------------------------------------------------------
class PriceEstimator:
    """
    Оценка реальной цены исполнения объёма по локальному (WS) или
    REST-стакану.

    Возвращаемые значения
    ---------------------
    Tuple[
        price : Optional[Decimal],          # VWAP- или worst-цена,
        max_qty_at_price : Optional[Decimal]# объём, доступный по этой цене
    ]
    Все цены и объёмы – Decimal для точности.
    """

    # -------------------- внутренний помощник --------------------
    @staticmethod
    def _zero_pair() -> Tuple[Optional[Decimal], Optional[Decimal]]:
        return None, None

    # -------------------- локальный стакан -----------------------
    @staticmethod
    def from_local(
        symbol: str,
        qty: Union[int, float, str, Decimal],
        *,
        buy: bool = True,
        vwap: bool = True,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Использует локальный WS-стакан.  
        Если доступного объёма недостаточно – возвращает (None, None).
        """
        ob = MarketDataFeed.instance().get_orderbook(symbol)
        if not ob:
            return PriceEstimator._zero_pair()

        fn = ob.ask_price_for_qty if buy else ob.bid_price_for_qty
        price, worst_lvl_sz = fn(qty, vwap=vwap)  # второй элемент уже то, что нужно
        if price in (0, None):
            return PriceEstimator._zero_pair()

        # worst_lvl_sz может быть None, если qty не заполнился целиком
        return price, (worst_lvl_sz or Decimal("0"))

    # -------------------- REST-фоллбэк ---------------------------
    @staticmethod
    def from_rest(
        symbol: str,
        qty: Union[int, float, str, Decimal],
        *,
        buy: bool = True,
        depth: int = 200,
        vwap: bool = True,
        testnet: bool = False,
        http_session: Optional[HTTP] = None,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        REST-фоллбэк (до 200 уровней).  
        Если объёма не хватает, возвращает (None, None).
        """
        qty_d = Decimal(str(qty))
        sess  = http_session or HTTP(testnet=testnet)
        side  = "a" if buy else "b"
        resp  = sess.get_orderbook(category="option", symbol=symbol, limit=depth)
        levels_raw = resp.get("result", {}).get(side, [])
        if not levels_raw:
            return PriceEstimator._zero_pair()

        levels = [(Decimal(str(p)), Decimal(str(q))) for p, q in levels_raw]

        left                = qty_d
        cost                = Decimal("0")
        worst_px            = Decimal("0")
        worst_level_size    = Decimal("0")

        for px, sz in levels:
            if left <= 0:
                break
            take  = sz if sz <= left else left
            cost += take * px
            worst_px         = px
            worst_level_size = sz
            left -= take

        if left > 0:  # объёма недостаточно
            return PriceEstimator._zero_pair()

        price = (cost / qty_d) if vwap else worst_px
        return price, worst_level_size

    # -------------------- смарт-оценка ---------------------------
    @staticmethod
    def smart(
        symbol: str,
        qty: Union[int, float, str, Decimal],
        *,
        buy: bool = True,
        depth_rest: int = 200,
        vwap: bool = True,
        testnet: bool = False,
        http_session: Optional[HTTP] = None,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        1) Пробует локальный стакан;  
        2) Если объёма не хватает – обращается к REST-стакану.

        Возвращает (price, max_qty_at_price).
        """
        price, max_qty = PriceEstimator.from_local(
            symbol, qty, buy=buy, vwap=vwap
        )
        if price is not None:
            return price, max_qty

        return PriceEstimator.from_rest(
            symbol,
            qty,
            buy=buy,
            depth=depth_rest,
            vwap=vwap,
            testnet=testnet,
            http_session=http_session,
        )


# ----------------------------------------------------------------------
#                       7.  PUBLIC FACADE
# ----------------------------------------------------------------------
class BybitOptionHub:
    """
    Высокоуровневый фасад: один `initialise()` → далее все возможности
    через статические методы или вложенные пространства имён.
    """

    _initialised = False
    _testnet: bool = False

    # -- 1. initialisation ----------------------------------------
    @staticmethod
    def initialise(*,
                   testnet: bool = False,
                   ws_orderbook_depth: int = 25,
                   ws_subscribe_orderbook: bool = True,
                   ws_ping_interval: int = 20,
                   ws_ping_timeout: int = 10,
                   ws_retries: int = 10,
                   **http_kwargs: Any
                   ) -> None:
        """
        Инициализировать HTTP-пул и запустить WebSocket-фид (singleton).
        Достаточно вызвать один раз при старте приложения.
        """
        if BybitOptionHub._initialised:
            return

        # HTTP / REST
        _SessionPool.build_all(testnet=testnet, **http_kwargs)

        # WS / feed
        MarketDataFeed.instance(
            testnet=testnet,
            orderbook_depth=ws_orderbook_depth,
            subscribe_orderbook=ws_subscribe_orderbook,
            ping_interval=ws_ping_interval,
            ping_timeout=ws_ping_timeout,
            retries=ws_retries,
        )

        BybitOptionHub._testnet = testnet
        BybitOptionHub._initialised = True
        log.info("BybitOptionHub initialised (testnet=%s).", testnet)

    # -- 2. shortcuts ---------------------------------------------
    Trading   = TradingAPI
    Feed      = MarketDataFeed
    Chain     = ChainUtils
    Estimator = PriceEstimator

    # -- 3. helpers -----------------------------------------------
    @staticmethod
    def subscribe(symbol: str | Iterable[str]) -> None:
        """
        Подписаться на 1 или несколько символов (ticker + orderbook).
        """
        feed = MarketDataFeed.instance()
        if isinstance(symbol, str):
            feed.add_symbol(symbol)
        else:
            feed.add_symbols(symbol)

    @staticmethod
    def get_ticker(symbol: str) -> Optional[OptionTickerSnapshot]:
        return MarketDataFeed.instance().get_ticker(symbol)

    @staticmethod
    def get_orderbook(symbol: str) -> Optional[OptionOrderbookSnapshot]:
        return MarketDataFeed.instance().get_orderbook(symbol)

    @staticmethod
    def stop() -> None:
        """
        Корректно завершить работу WS-сервиса (при выключении приложения).
        """
        if MarketDataFeed._INSTANCE:
            MarketDataFeed._INSTANCE.stop()

def update_leg_subscriptions(stages: Dict[str, dict]) -> None:
    """
    Обновить подписки на потоки опционов в зависимости от переданных стадий.
    Подписаться на leg.name для stages['first'] и stages['second'],
    удалить из внутреннего списка подписок те символы, которые больше не нужны.
    """
    # 1) Собираем нужные символы
    desired: Set[str] = set()
    for stage_key in ("first", "second"):
        stage = stages.get(stage_key, {})
        if stage.get("exist"):
            name = stage.get("position", {}).get("leg", {}).get("name")
            if name:
                desired.add(name.upper())

    # 2) Подписываемся на недостающие символы
    for sym in desired:
        BybitOptionHub.subscribe(sym)
        log.info("Subscribed to %s", sym)

    # 3) Удаляем из внутреннего списка подписок те, которых больше не нужно
    feed = BybitOptionHub.Feed.instance()
    for sym in list(feed._subscribed):
        if sym not in desired:
            feed._subscribed.remove(sym)
            log.info("Removed %s from subscription list", sym)


# ----------------------------------------------------------------------
#        WATCHDOG: ensure_option_feed_alive (перезапуск WS-фида)
# ----------------------------------------------------------------------
def ensure_option_feed_alive(*,
                             staleness_sec: int = 25,
                             log_level: int = logging.WARNING) -> None:
    """
    «Сторожок» для WebSocket-фида опционов Bybit.

    Вызывать из главного цикла примерно раз в 10-20 с:

        while True:
            ...
            ensure_option_feed_alive()          # <─ здесь
            time.sleep(1)

    Параметры
    ---------
    staleness_sec : int
        Максимально допустимое «молчание» (сек) по ЛЮБОМУ подписанному
        символу.  Если данных нет дольше, чем staleness_sec, WebSocket
        перезапускается *без* потери списка подписок.
    log_level : int
        Уровень, на котором будет выведено уведомление о перезапуске.
    """
    feed = MarketDataFeed._INSTANCE           # type: Optional[MarketDataFeed]
    if feed is None:                          # ещё не инициализировали
        return

    # --------------------------------------------------------------
    # 1. Ищем самый «свежий» snapshot среди всех подписанных символов
    # --------------------------------------------------------------
    now_ms    = int(time.time() * 1000)
    newest_ts: Optional[int] = None

    for sym in list(feed._subscribed):
        snap = feed.get_ticker(sym) or feed.get_orderbook(sym)
        ts   = getattr(snap, "updated_ts", None) if snap else None
        if ts is None:
            continue
        if newest_ts is None or ts > newest_ts:
            newest_ts = ts

    # Если нет ни одного снапшота — только запустились; это нормально.
    if newest_ts is None:
        return

    if now_ms - newest_ts <= staleness_sec * 1000:
        return                                # всё живо

    # --------------------------------------------------------------
    # 2. Считаем поток «мертвым» → перезапускаем.
    # --------------------------------------------------------------
    logging.log(
        log_level,
        "Bybit Option feed: последняя активность %.1f с назад "
        "(порог %d с) — перезапускаю WebSocket.",
        (now_ms - newest_ts) / 1000.0,
        staleness_sec,
    )

    # сохраняем параметры текущего фида
    params = dict(
        testnet            = feed.testnet,
        orderbook_depth    = feed.orderbook_depth,
        subscribe_orderbook= feed.subscribe_orderbook,
        api_key            = feed.api_key,
        api_secret         = feed.api_secret,
        ping_interval      = feed.ping_interval,
        ping_timeout       = feed.ping_timeout,
        retries            = feed.retries,
    )
    subscribed = list(feed._subscribed)

    # аккуратно останавливаем старый фид
    feed.stop()                               # освобождает ресурсы и обнуляет singleton

    # создаём новый с теми же параметрами
    new_feed = MarketDataFeed.instance(**params)

    # возвращаем все прежние подписки
    if subscribed:
        new_feed.add_symbols(subscribed)

    logging.info(
        "Bybit Option feed: перезапуск завершён, повторно подписано символов: %d.",
        len(subscribed),
    )
