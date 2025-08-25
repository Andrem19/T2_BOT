# spot_price_feed.py
"""
Realtime SPOT price stream for Binance symbols (no klines).

Use cases
---------
1) One symbol:

       from spot_price_feed import start_spot_price_stream, SpotPriceCache
       start_spot_price_stream("BTCUSDT")
       price = SpotPriceCache.get("BTCUSDT")  # or SpotPriceCache("BTCUSDT")

2) Several symbols in one combined WebSocket:

       from spot_price_feed import start_spot_price_streams
       stream = start_spot_price_streams(["BTCUSDT", "ETHUSDT", "SOLUSDT"])

3) Health monitoring / diagnostics:

       from spot_price_feed import (
           ensure_spot_stream_alive,
           ensure_spot_streams_alive_for_symbols,
           get_spot_stream_diagnostics,
           monitor_all_spot_streams,
       )

Design goals
------------
- Fully independent from other modules (distinct cache, registries, logger).
- SPOT only (wss://stream.binance.com:9443).
- No klines/candles, only last trade price per symbol.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

import websockets  # pip install websockets

# =============================================================================
# Logging
# =============================================================================
_logger = logging.getLogger("spot_price_feed")
_logger.setLevel(logging.INFO)
if not _logger.hasHandlers():  # avoid duplicate handlers when reloaded
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-8s [%(name)s] %(message)s")
    )
    _logger.addHandler(_handler)

# =============================================================================
# Configuration (SPOT endpoints + keepalive)
# =============================================================================
_BINANCE_SPOT_WS_SINGLE = "wss://stream.binance.com:9443/ws/{stream}"
_BINANCE_SPOT_WS_MULTI = "wss://stream.binance.com:9443/stream?streams={streams}"

# Manual keepalive and liveness thresholds
_MANUAL_PING_SEC = 50          # send client ping every N seconds
_SERVER_IDLE_SEC = 180         # if no messages for >= N seconds -> reconnect
_RECONNECT_MAX_BACKOFF = 32    # seconds

# =============================================================================
# SpotPriceCache – last price + timestamp per symbol (thread-safe)
# =============================================================================
class _SpotPriceCache:
    """Thread-safe cache of last prices and their update timestamps (ms)."""

    def __init__(self) -> None:
        self._data: Dict[str, float] = {}
        self._ts_ms: Dict[str, int] = {}
        self._lock = threading.RLock()

    def set(self, symbol: str, price: float, ts_ms: Optional[int] = None) -> None:
        symbol = symbol.upper()
        with self._lock:
            self._data[symbol] = float(price)
            self._ts_ms[symbol] = int(ts_ms if ts_ms is not None else time.time() * 1000)

    def get(
        self,
        symbol: str,
        *,
        wait: bool = False,
        timeout: Optional[float] = None,
    ) -> Optional[float]:
        symbol = symbol.upper()
        start = time.time()
        while True:
            with self._lock:
                val = self._data.get(symbol)
            if val is not None or not wait:
                return val
            if timeout is not None and (time.time() - start) >= timeout:
                return None
            time.sleep(0.05)

    def last_update_ms(self, symbol: str) -> Optional[int]:
        symbol = symbol.upper()
        with self._lock:
            return self._ts_ms.get(symbol)

    def age_seconds(self, symbol: str) -> Optional[float]:
        ts = self.last_update_ms(symbol)
        return None if ts is None else max(0.0, time.time() - ts / 1000.0)

    def get_with_age(self, symbol: str):
        symbol = symbol.upper()
        with self._lock:
            price = self._data.get(symbol)
            ts = self._ts_ms.get(symbol)
        age = None if ts is None else max(0.0, time.time() - ts / 1000.0)
        return price, age

    # convenient shorthand: SpotPriceCache("BTCUSDT")
    def __call__(self, symbol: str, *a, **kw) -> Optional[float]:
        return self.get(symbol, *a, **kw)


SpotPriceCache = _SpotPriceCache()

# =============================================================================
# Single-symbol SPOT WebSocket stream
# =============================================================================
class _SingleSpotPriceStream(threading.Thread):
    """Dedicated daemon thread per SPOT symbol (<symbol>@trade on stream.binance.com)."""

    def __init__(self, symbol: str) -> None:
        super().__init__(name=f"spot-{symbol.upper()}-ws", daemon=True)
        self.symbol = symbol.upper()
        self.stream_name = f"{self.symbol.lower()}@trade"
        self.url = _BINANCE_SPOT_WS_SINGLE.format(stream=self.stream_name)
        self._stopped = threading.Event()
        self.started_at = time.time()

    def stop(self) -> None:
        self._stopped.set()

    async def _stream(self) -> None:
        backoff = 1
        while not self._stopped.is_set():
            try:
                _logger.info("SPOT: connecting single %s", self.url)
                async with websockets.connect(
                    self.url,
                    ping_interval=None,  # manual pings
                    ping_timeout=None,
                    close_timeout=1,
                    max_queue=None,
                ) as ws:
                    backoff = 1
                    last_rx = time.time()
                    last_ping = time.time()

                    while not self._stopped.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            now = time.time()
                            # manual ping
                            if now - last_ping >= _MANUAL_PING_SEC:
                                await ws.ping()
                                last_ping = now
                            # idle detection
                            if now - last_rx >= _SERVER_IDLE_SEC:
                                raise RuntimeError("No messages from server (single)")
                            continue

                        last_rx = time.time()
                        data = json.loads(msg)
                        # SPOT trade payload: price 'p', time 'T', symbol 's'
                        price = float(data["p"])
                        ts_ms = int(data["T"])
                        SpotPriceCache.set(self.symbol, price, ts_ms=ts_ms)

            except Exception as err:
                _logger.warning(
                    "SPOT single stream error for %s: %s – reconnect in %ss",
                    self.symbol, err, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_MAX_BACKOFF)

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._stream())
        finally:
            loop.close()
            _logger.info("SPOT single stream for %s terminated", self.symbol)

# =============================================================================
# Combined SPOT WebSocket stream (several symbols in one connection)
# =============================================================================
class _CombinedSpotPriceStream(threading.Thread):
    """One SPOT WebSocket subscribed to multiple <symbol>@trade channels."""

    def __init__(self, symbols: List[str]) -> None:
        syms = [s.upper() for s in symbols]
        super().__init__(name=f"spot-combined-{'-'.join(sorted(syms))}-ws", daemon=True)
        self.symbols = syms
        streams = "/".join(f"{s.lower()}@trade" for s in self.symbols)
        self.url = _BINANCE_SPOT_WS_MULTI.format(streams=streams)
        self._stopped = threading.Event()
        self.started_at = time.time()

    def stop(self) -> None:
        self._stopped.set()

    async def _stream(self) -> None:
        backoff = 1
        while not self._stopped.is_set():
            try:
                _logger.info("SPOT: connecting combined %s", self.url)
                async with websockets.connect(
                    self.url,
                    ping_interval=None,  # manual pings
                    ping_timeout=None,
                    close_timeout=1,
                    max_queue=None,
                ) as ws:
                    backoff = 1
                    last_rx = time.time()
                    last_ping = time.time()

                    while not self._stopped.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            now = time.time()
                            if now - last_ping >= _MANUAL_PING_SEC:
                                await ws.ping()
                                last_ping = now
                            if now - last_rx >= _SERVER_IDLE_SEC:
                                raise RuntimeError("No messages from server (combined)")
                            continue

                        last_rx = time.time()
                        payload = json.loads(msg)
                        data = payload.get("data")
                        if not data:
                            continue
                        symbol = data["s"].upper()
                        price = float(data["p"])
                        ts_ms = int(data["T"])
                        SpotPriceCache.set(symbol, price, ts_ms=ts_ms)

            except Exception as err:
                _logger.warning(
                    "SPOT combined stream error (%s): %s – reconnect in %ss",
                    ",".join(self.symbols), err, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_MAX_BACKOFF)

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._stream())
        finally:
            loop.close()
            _logger.info("SPOT combined stream for %s terminated", ",".join(self.symbols))

# =============================================================================
# Registries (isolated from other modules)
# =============================================================================
_SINGLE_REGISTRY: Dict[str, _SingleSpotPriceStream] = {}
_SINGLE_LOCK = threading.RLock()

_COMBINED_REGISTRY: Dict[Tuple[str, ...], _CombinedSpotPriceStream] = {}
_COMBINED_LOCK = threading.RLock()

# Helper: find which stream (single/combined) currently serves a symbol
def _find_stream_for_symbol(symbol: str):
    sym = symbol.upper()
    with _SINGLE_LOCK:
        s = _SINGLE_REGISTRY.get(sym)
        if s is not None:
            return ("single", sym, s)
    with _COMBINED_LOCK:
        for key, stream in _COMBINED_REGISTRY.items():
            if sym in key:
                return ("combined", key, stream)
    return (None, None, None)

# =============================================================================
# Public API – start/stop/restart (single & combined)
# =============================================================================
def start_spot_price_stream(symbol: str) -> _SingleSpotPriceStream:
    """Start (or return) a single-symbol SPOT WebSocket stream."""
    sym = symbol.upper()
    with _SINGLE_LOCK:
        stream = _SINGLE_REGISTRY.get(sym)
        if stream is None or not stream.is_alive():
            stream = _SingleSpotPriceStream(sym)
            _SINGLE_REGISTRY[sym] = stream
            stream.start()
            _logger.info("Started SPOT single stream for %s", sym)
        return stream

def start_spot_price_streams(symbols: List[str]) -> _CombinedSpotPriceStream:
    """Start (or return) a combined SPOT WebSocket stream for several symbols."""
    symbols_norm = [s.upper() for s in symbols]
    key = tuple(sorted(set(symbols_norm)))
    with _COMBINED_LOCK:
        stream = _COMBINED_REGISTRY.get(key)
        if stream is None or not stream.is_alive():
            stream = _CombinedSpotPriceStream(list(key))
            _COMBINED_REGISTRY[key] = stream
            stream.start()
            _logger.info("Started SPOT combined stream for %s", ",".join(key))
        return stream

def _stop_single(sym: str, join_timeout: float = 5.0) -> None:
    sym = sym.upper()
    with _SINGLE_LOCK:
        stream = _SINGLE_REGISTRY.get(sym)
    if not stream:
        return
    try:
        stream.stop()
        stream.join(join_timeout)
    except Exception:
        pass
    with _SINGLE_LOCK:
        if _SINGLE_REGISTRY.get(sym) is stream:
            _SINGLE_REGISTRY.pop(sym, None)

def _stop_combined(key: Tuple[str, ...], join_timeout: float = 5.0) -> None:
    with _COMBINED_LOCK:
        stream = _COMBINED_REGISTRY.get(key)
    if not stream:
        return
    try:
        stream.stop()
        stream.join(join_timeout)
    except Exception:
        pass
    with _COMBINED_LOCK:
        if _COMBINED_REGISTRY.get(key) is stream:
            _COMBINED_REGISTRY.pop(key, None)

def restart_spot_price_stream(symbol: str, *, join_timeout: float = 5.0):
    """Restart the stream (single or combined) that currently serves the symbol."""
    kind, key, stream = _find_stream_for_symbol(symbol)
    if kind is None:
        # If not running, start a single stream by default
        return start_spot_price_stream(symbol)

    if kind == "single":
        sym = key  # type: ignore[assignment]
        _logger.info("Restarting SPOT single stream for %s ...", sym)
        _stop_single(sym, join_timeout=join_timeout)
        return start_spot_price_stream(sym)

    # combined
    comb_key = key  # type: ignore[assignment]
    _logger.info("Restarting SPOT combined stream for %s ...", ",".join(comb_key))
    _stop_combined(comb_key, join_timeout=join_timeout)
    return start_spot_price_streams(list(comb_key))

# =============================================================================
# Diagnostics & Health-check
# =============================================================================
def get_spot_stream_diagnostics(symbol: str) -> Dict[str, object]:
    """Diagnostics for a symbol (works for both single and combined streams)."""
    sym = symbol.upper()
    kind, key, stream = _find_stream_for_symbol(sym)
    price, age = SpotPriceCache.get_with_age(sym)
    started_at = getattr(stream, "started_at", None) if stream else None
    thread_alive = stream.is_alive() if stream else False

    return {
        "symbol": sym,
        "stream_type": kind,         # 'single' | 'combined' | None
        "stream_key": key,           # e.g. 'BTCUSDT' or ('BTCUSDT','ETHUSDT')
        "thread_alive": thread_alive,
        "started_at": started_at,
        "age_seconds": age,
        "last_update_ms": SpotPriceCache.last_update_ms(sym),
        "price": price,
    }

def ensure_spot_stream_alive(
    symbol: str,
    *,
    max_stale_seconds: float = 15.0,
    startup_grace_seconds: float = 5.0,
    auto_restart: bool = True,
    autostart_if_missing: bool = False,
) -> bool:
    """
    Verify that a stream serving `symbol` is alive and fresh; restart if needed.
    Works with both single and combined streams.
    """
    sym = symbol.upper()
    diag = get_spot_stream_diagnostics(sym)

    if not diag["thread_alive"]:
        if autostart_if_missing and diag["stream_type"] is None:
            _logger.warning("SPOT stream for %s not running – starting.", sym)
            start_spot_price_stream(sym)
            return True
        if auto_restart and diag["stream_type"] is not None:
            restart_spot_price_stream(sym)
            return True
        return False

    age = diag["age_seconds"]
    if age is None:
        # No data yet – allow short grace period since start
        started_at = diag["started_at"] or time.time()
        if (time.time() - started_at) > (max_stale_seconds + startup_grace_seconds):
            _logger.warning("SPOT: no ticks for %s since start – restarting.", sym)
            if auto_restart:
                restart_spot_price_stream(sym)
                return True
            return False
        return True

    if age > max_stale_seconds:
        _logger.warning("SPOT: price for %s is stale (%.2fs) – restarting.", sym, age)
        if auto_restart:
            restart_spot_price_stream(sym)
            return True
        return False
    return True

def ensure_spot_streams_alive_for_symbols(
    symbols: List[str],
    *,
    max_stale_seconds: float = 15.0,
    startup_grace_seconds: float = 5.0,
    auto_restart: bool = True,
    autostart_if_missing: bool = False,
) -> Dict[str, bool]:
    """Check a set of symbols; returns {symbol: ok_bool}."""
    result: Dict[str, bool] = {}
    for s in symbols:
        ok = ensure_spot_stream_alive(
            s,
            max_stale_seconds=max_stale_seconds,
            startup_grace_seconds=startup_grace_seconds,
            auto_restart=auto_restart,
            autostart_if_missing=autostart_if_missing,
        )
        result[s.upper()] = ok
    return result

def monitor_all_spot_streams(
    *,
    max_stale_seconds: float = 15.0,
    startup_grace_seconds: float = 5.0,
    auto_restart: bool = True,
) -> Dict[str, bool]:
    """
    Check all active symbols across single and combined SPOT streams.
    Returns {symbol: ok_bool}
    """
    symbols: List[str] = []
    with _SINGLE_LOCK:
        symbols.extend(list(_SINGLE_REGISTRY.keys()))
    with _COMBINED_LOCK:
        for key in _COMBINED_REGISTRY.keys():
            symbols.extend(list(key))
    # de-duplicate
    symbols = sorted(set(symbols))

    out: Dict[str, bool] = {}
    for s in symbols:
        out[s] = ensure_spot_stream_alive(
            s,
            max_stale_seconds=max_stale_seconds,
            startup_grace_seconds=startup_grace_seconds,
            auto_restart=auto_restart,
            autostart_if_missing=False,
        )
    return out

# =============================================================================
# Graceful shutdown helpers (optional)
# =============================================================================
def stop_all_spot_streams(join_timeout: float = 5.0) -> None:
    """Stop all SPOT streams (single and combined)."""
    with _SINGLE_LOCK:
        singles = list(_SINGLE_REGISTRY.keys())
    with _COMBINED_LOCK:
        combined = list(_COMBINED_REGISTRY.keys())

    for sym in singles:
        _stop_single(sym, join_timeout=join_timeout)
    for key in combined:
        _stop_combined(key, join_timeout=join_timeout)
