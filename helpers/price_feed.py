# price_feed.py
"""
Realtime price stream for Binance symbols with live candle cache.

Key features
------------
1) Subscribe to a **single** symbol (legacy-compatible)::

       from price_feed import start_price_stream, PriceCache, CandleCache
       start_price_stream("BTCUSDT", klines={60: 200})
       price  = PriceCache.get("BTCUSDT")
       klines = CandleCache.get("BTCUSDT", 60)

2) Subscribe to **several** symbols using **one** combined WebSocket
   (preferred for 2+ tickers)::

       from price_feed import start_price_streams
       stream = start_price_streams(["BTCUSDT", "ETHUSDT", "SOLUSDT"],
                                    klines={60: 200})
       # then as usual: PriceCache.get(...), CandleCache.get(...)

3) Health monitoring / auto-restart::

       from price_feed import ensure_stream_alive, ensure_streams_alive_for_symbols
       ensure_stream_alive("BTCUSDT")
       ensure_streams_alive_for_symbols(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import requests
import websockets  # pip install websockets
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
_logger = logging.getLogger("price_feed")
_logger.setLevel(logging.INFO)
if not _logger.hasHandlers():  # avoid duplicate handlers when reloaded
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-8s [%(name)s] %(message)s")
    )
    _logger.addHandler(_handler)

# --------------------------------------------------------------------------- #
# Price cache (singleton) – last price + last trade time
# --------------------------------------------------------------------------- #
class _PriceCache:
    """Thread-safe cache of last prices and their update timestamps."""

    def __init__(self) -> None:
        self._data: Dict[str, float] = {}
        self._ts_ms: Dict[str, int] = {}
        self._lock = threading.RLock()

    # public API --------------------------------------------------------------
    def set(self, symbol: str, price: float, ts_ms: Optional[int] = None) -> None:
        with self._lock:
            self._data[symbol] = price
            self._ts_ms[symbol] = ts_ms or int(time.time() * 1000)

    def get(
        self,
        symbol: str,
        *,
        wait: bool = False,
        timeout: Optional[float] = None,
    ) -> Optional[float]:
        start = time.time()
        while True:
            with self._lock:
                val = self._data.get(symbol)
            if val is not None or not wait:
                return val
            if timeout and (time.time() - start) >= timeout:
                return None
            time.sleep(0.05)

    def last_update_ms(self, symbol: str) -> Optional[int]:
        with self._lock:
            return self._ts_ms.get(symbol)

    def age_seconds(self, symbol: str) -> Optional[float]:
        ts = self.last_update_ms(symbol)
        return None if ts is None else max(0.0, time.time() - ts / 1000.0)

    def get_with_age(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        with self._lock:
            price = self._data.get(symbol)
            ts = self._ts_ms.get(symbol)
        age = None if ts is None else max(0.0, time.time() - ts / 1000.0)
        return price, age

    # convenient shorthand: PriceCache("BTCUSDT")
    def __call__(self, symbol: str, *a, **kw) -> Optional[float]:
        return self.get(symbol, *a, **kw)


PriceCache = _PriceCache()

# --------------------------------------------------------------------------- #
# Candle cache – OHLC history per timeframe
# --------------------------------------------------------------------------- #
class _CandleCache:
    """
    Global thread-safe candle cache.
    Structure: symbol → interval_minutes → np.ndarray([[t,o,h,l,c,v], …])
    """

    def __init__(self) -> None:
        self._data: Dict[str, Dict[int, np.ndarray]] = {}
        self._limits: Dict[Tuple[str, int], int] = {}
        self._lock = threading.RLock()

    # public API --------------------------------------------------------------
    def register(self, symbol: str, interval: int, limit: int) -> None:
        symbol = symbol.upper()
        kl = get_kline(symbol, limit, interval)
        if isinstance(kl, list):  # fallback
            kl = np.array(kl, dtype=float)
        with self._lock:
            self._data.setdefault(symbol, {})[interval] = kl
            self._limits[(symbol, interval)] = limit
        _logger.info("Cached %s klines for %s @ %dmin", len(kl), symbol, interval)

    def get(self, symbol: str, interval: int) -> Optional[np.ndarray]:
        symbol = symbol.upper()
        with self._lock:
            arr = self._data.get(symbol, {}).get(interval)
            return None if arr is None else arr.copy()

    def __call__(self, symbol: str, interval: int) -> Optional[np.ndarray]:
        return self.get(symbol, interval)

    # internal helpers (called from WebSocket threads) ------------------------
    def update_all(self, symbol: str, price: float, ts_ms: int) -> None:
        symbol = symbol.upper()
        with self._lock:
            frames = self._data.get(symbol)
            if not frames:
                return
            for interval, arr in frames.items():
                frames[interval] = self._update_single(
                    arr,
                    price,
                    ts_ms,
                    interval_ms=interval * 60_000,
                    limit=self._limits[(symbol, interval)],
                )

    @staticmethod
    def _update_single(
        arr: np.ndarray,
        price: float,
        ts_ms: int,
        *,
        interval_ms: int,
        limit: int,
    ) -> np.ndarray:
        candle_start = ts_ms - (ts_ms % interval_ms)

        if arr[-1][0] == candle_start:
            # update current candle
            arr[-1][2] = max(arr[-1][2], price)  # high
            arr[-1][3] = min(arr[-1][3], price)  # low
            arr[-1][4] = price                   # close
            return arr

        if candle_start < arr[-1][0]:
            return arr  # tick from the past

        # create new candle
        new_candle = np.array([candle_start, price, price, price, price, 0.0], dtype=float)
        arr = np.vstack((arr, new_candle))
        if len(arr) > limit:
            arr = arr[-limit:]
        return arr


CandleCache = _CandleCache()

# --------------------------------------------------------------------------- #
# Binance REST – historical klines (with retry)
# --------------------------------------------------------------------------- #
_RETRY = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
_SESS = requests.Session()
_SESS.mount("https://", HTTPAdapter(max_retries=_RETRY))


def get_kline(coin: str, number_candles: int, interv: int):
    """Download klines via Binance Futures REST API with automatic retries."""
    try:
        unit = "m"
        interval_param = interv
        if interv == 60:
            interval_param, unit = 1, "h"
        elif interv == 240:
            interval_param, unit = 4, "h"

        url = (
            f"https://fapi.binance.com/fapi/v1/klines?"
            f"symbol={coin}&interval={interval_param}{unit}&limit={number_candles}"
        )
        resp = _SESS.get(url, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
        out = [
            [x[0], float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])]
            for x in raw
        ]
        return np.array(out, dtype=float)
    except Exception as e:
        _logger.error(
            "Error [get_kline %s %s]: %s\n%s",
            coin, datetime.now(), e, traceback.format_exc(),
        )
        return np.array([[0, 0, 0, 0, 0, 0]], dtype=float)

# --------------------------------------------------------------------------- #
# SINGLE-SYMBOL WebSocket worker (manual-ping edition)
# --------------------------------------------------------------------------- #
class _BinancePriceStream(threading.Thread):
    """Dedicated daemon thread per symbol (<symbol>@trade)."""

    _MANUAL_PING_SEC = 50         # how often to send client ping
    _SERVER_IDLE_SEC = 180        # if no ticks ≥ this → reconnect

    def __init__(self, symbol: str) -> None:
        super().__init__(name=f"{symbol}-ws", daemon=True)
        self.symbol_raw = symbol.lower()
        self.symbol = symbol.upper()
        self.url = f"wss://stream.binance.com:9443/ws/{self.symbol_raw}@trade"
        self._stopped = threading.Event()
        self.started_at = time.time()

    # public
    def stop(self) -> None:
        self._stopped.set()

    # internal coroutine
    async def _stream(self) -> None:
        backoff = 1
        while not self._stopped.is_set():
            try:
                _logger.info("Connecting to %s", self.url)
                async with websockets.connect(
                    self.url,
                    ping_interval=None,   # disable library pings
                    ping_timeout=None,
                    close_timeout=1,
                    max_queue=None,
                ) as ws:
                    backoff = 1
                    last_rx = time.time()
                    last_manual_ping = time.time()

                    while not self._stopped.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            now = time.time()
                            if now - last_manual_ping >= self._MANUAL_PING_SEC:
                                await ws.ping()
                                last_manual_ping = now
                            if now - last_rx >= self._SERVER_IDLE_SEC:
                                raise RuntimeError("No ticks from server")
                            continue

                        last_rx = time.time()
                        data = json.loads(msg)
                        price = float(data["p"])
                        ts_ms = int(data["T"])
                        PriceCache.set(self.symbol, price, ts_ms=ts_ms)
                        CandleCache.update_all(self.symbol, price, ts_ms)

            except Exception as err:
                _logger.warning(
                    "WebSocket error for %s: %s – reconnect in %ss",
                    self.symbol, err, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 32)

    # thread entrypoint
    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._stream())
        finally:
            loop.close()
            _logger.info("WebSocket thread for %s terminated", self.symbol)

# --------------------------------------------------------------------------- #
# COMBINED WebSocket worker – several symbols in one connection
# --------------------------------------------------------------------------- #
class _CombinedBinancePriceStream(threading.Thread):
    """Single WebSocket stream subscribed to multiple *trade* channels."""

    _MANUAL_PING_SEC = 50
    _SERVER_IDLE_SEC = 180

    def __init__(self, symbols: List[str]):
        super().__init__(name=f"combined-{'-'.join(sorted(symbols))}-ws", daemon=True)
        self.symbols = [s.upper() for s in symbols]
        self.started_at = time.time()
        self._stopped = threading.Event()
        streams = "/".join(f"{s.lower()}@trade" for s in self.symbols)
        self.url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    def stop(self) -> None:
        self._stopped.set()

    async def _stream(self) -> None:
        backoff = 1
        while not self._stopped.is_set():
            try:
                _logger.info("Connecting to combined stream %s", self.url)
                async with websockets.connect(
                    self.url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=1,
                    max_queue=None,
                ) as ws:
                    backoff = 1
                    last_rx = time.time()
                    last_manual_ping = time.time()

                    while not self._stopped.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            now = time.time()
                            if now - last_manual_ping >= self._MANUAL_PING_SEC:
                                await ws.ping()
                                last_manual_ping = now
                            if now - last_rx >= self._SERVER_IDLE_SEC:
                                raise RuntimeError("No ticks from server")
                            continue

                        last_rx = time.time()
                        payload = json.loads(msg)
                        data = payload.get("data")
                        if not data:
                            continue
                        symbol = data["s"].upper()
                        price = float(data["p"])
                        ts_ms = int(data["T"])
                        PriceCache.set(symbol, price, ts_ms=ts_ms)
                        CandleCache.update_all(symbol, price, ts_ms)

            except Exception as err:
                _logger.warning(
                    "Combined WebSocket error (%s): %s – reconnect in %ss",
                    ",".join(self.symbols), err, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 32)

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._stream())
        finally:
            loop.close()
            _logger.info(
                "Combined WebSocket thread for %s terminated", ",".join(self.symbols)
            )

# --------------------------------------------------------------------------- #
# REGISTRIES & LOCKS
# --------------------------------------------------------------------------- #
_STREAM_REGISTRY: Dict[str, _BinancePriceStream] = {}
_REGISTRY_LOCK = threading.RLock()

_COMBINED_REGISTRY: Dict[Tuple[str, ...], _CombinedBinancePriceStream] = {}
_COMBINED_LOCK = threading.RLock()

# --------------------------------------------------------------------------- #
# PUBLIC API – start single / combined streams
# --------------------------------------------------------------------------- #
def start_price_stream(
    symbol: str,
    *,
    klines: Optional[Dict[int, int]] = None,
) -> _BinancePriceStream:
    """Start (or return) a **single-symbol** WebSocket stream."""
    symbol = symbol.upper()
    if klines:
        for interval, limit in klines.items():
            CandleCache.register(symbol, interval, limit)

    with _REGISTRY_LOCK:
        stream = _STREAM_REGISTRY.get(symbol)
        if stream is None or not stream.is_alive():
            stream = _BinancePriceStream(symbol)
            _STREAM_REGISTRY[symbol] = stream
            stream.start()
            _logger.info("Started websocket stream for %s", symbol)
        return stream


def start_price_streams(
    symbols: List[str],
    *,
    klines: Optional[Dict[int, int]] = None,
) -> _CombinedBinancePriceStream:
    """
    Start (or return) a **combined** WebSocket stream for several symbols.

    Example::
        start_price_streams(["BTCUSDT", "ETHUSDT"], klines={60: 200})
    """
    symbols_norm = [s.upper() for s in symbols]
    for s in symbols_norm:
        if klines:
            for interval, limit in klines.items():
                CandleCache.register(s, interval, limit)

    key = tuple(sorted(symbols_norm))
    with _COMBINED_LOCK:
        stream = _COMBINED_REGISTRY.get(key)
        if stream is None or not stream.is_alive():
            stream = _CombinedBinancePriceStream(list(key))
            _COMBINED_REGISTRY[key] = stream
            stream.start()
            _logger.info("Started combined websocket stream for %s", ",".join(key))
        return stream

# --------------------------------------------------------------------------- #
# INTERNAL HELPERS: stop / restart single streams
# --------------------------------------------------------------------------- #
def _stop_stream(symbol: str, join_timeout: float = 5.0) -> None:
    symbol = symbol.upper()
    with _REGISTRY_LOCK:
        stream = _STREAM_REGISTRY.get(symbol)
    if not stream:
        return
    try:
        stream.stop()
        stream.join(join_timeout)
    except Exception:
        pass
    with _REGISTRY_LOCK:
        if _STREAM_REGISTRY.get(symbol) is stream:
            _STREAM_REGISTRY.pop(symbol, None)


def restart_price_stream(
    symbol: str,
    *,
    klines: Optional[Dict[int, int]] = None,
    join_timeout: float = 5.0,
) -> _BinancePriceStream:
    """Hard restart of a single-symbol stream."""
    symbol = symbol.upper()
    _logger.info("Restarting stream for %s ...", symbol)
    _stop_stream(symbol, join_timeout)
    return start_price_stream(symbol, klines=klines)

# --------------------------------------------------------------------------- #
# DIAGNOSTICS & HEALTH-CHECK
# --------------------------------------------------------------------------- #
def get_stream_diagnostics(symbol: str) -> Dict[str, object]:
    """Summary for ONE symbol (works for combined too)."""
    symbol = symbol.upper()
    with _REGISTRY_LOCK:
        stream = _STREAM_REGISTRY.get(symbol)
    price, age = PriceCache.get_with_age(symbol)
    started_at = stream.started_at if stream else None
    thread_alive = stream.is_alive() if stream else False
    return {
        "symbol": symbol,
        "thread_alive": thread_alive,
        "started_at": started_at,
        "age_seconds": age,
        "last_update_ms": PriceCache.last_update_ms(symbol),
        "price": price,
    }


def ensure_stream_alive(
    symbol: str,
    *,
    max_stale_seconds: float = 15.0,
    startup_grace_seconds: float = 5.0,
    auto_restart: bool = True,
) -> bool:
    """Verify that a single stream is alive and fresh; restart if needed."""
    symbol = symbol.upper()
    diag = get_stream_diagnostics(symbol)

    if not diag["thread_alive"]:
        if auto_restart:
            restart_price_stream(symbol)
            return True
        return False

    age = diag["age_seconds"]
    if age is None:
        if (time.time() - (diag["started_at"] or time.time())) > (
            max_stale_seconds + startup_grace_seconds
        ):
            _logger.warning("No ticks for %s since start – restarting.", symbol)
            if auto_restart:
                restart_price_stream(symbol)
                return True
            return False
        return True

    if age > max_stale_seconds:
        _logger.warning("Price for %s is stale (%.2fs) – restarting.", symbol, age)
        if auto_restart:
            restart_price_stream(symbol)
            return True
        return False
    return True


def ensure_streams_alive_for_symbols(
    symbols: List[str],
    *,
    max_stale_seconds: float = 15.0,
    startup_grace_seconds: float = 5.0,
    auto_restart: bool = True,
) -> Dict[str, bool]:
    """
    Check a set of symbols (single or combined – no difference).
    Returns {symbol: True|False}
    """
    result: Dict[str, bool] = {}
    for s in symbols:
        ok = ensure_stream_alive(
            s,
            max_stale_seconds=max_stale_seconds,
            startup_grace_seconds=startup_grace_seconds,
            auto_restart=auto_restart,
        )
        result[s.upper()] = ok
    return result


def monitor_all_streams(
    *,
    max_stale_seconds: float = 15.0,
    startup_grace_seconds: float = 5.0,
    auto_restart: bool = True,
) -> Dict[str, bool]:
    """
    Check all active single-symbol streams (combined are not listed).
    Returns {symbol: ok_bool}
    """
    with _REGISTRY_LOCK:
        symbols = list(_STREAM_REGISTRY.keys())
    out: Dict[str, bool] = {}
    for s in symbols:
        out[s] = ensure_stream_alive(
            s,
            max_stale_seconds=max_stale_seconds,
            startup_grace_seconds=startup_grace_seconds,
            auto_restart=auto_restart,
        )
    return out
