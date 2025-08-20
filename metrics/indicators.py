# -*- coding: utf-8 -*-
"""
intel.py — безопасный сборщик рыночных сигналов с файловым кэшем.

Что внутри:
- Дисковый кэш (persist): .intel_cache/ (можно сменить переменной INTEL_CACHE_DIR).
- Авто-фолбэк кэша на ~/.intel_cache_tbot → /var/tmp/intel_cache_tbot → /tmp/intel_cache_tbot.
- Длинные TTL для перекрывающихся окон (aggTrades/klines) — reuse между часовыми запусками.
- Stale-while-revalidate: при сетевой ошибке отдаём последнюю копию из кэша.
- Потокобезопасные FileCache, RateLimiter и Http (Session с пулом соединений).
- Параллелизация независимых блоков snapshot() с соблюдением лимитов хостов.
- Бережная обработка 429/Retry-After (экспоненциальный бэкофф с джиттером).
- Адаптивный сбор aggTrades с бюджетом вызовов и кэшированием каждой страницы.

Функциональность (как была): Binance (цена/OI/funding/basis/flows/depth/ratios),
кросс-биржи (Bybit/OKX/Deribit), macro (ES/NQ/DXY со Stooq + fallback).
"""

from __future__ import annotations

import os
import sys
import time
import json
import math
import random
import hashlib
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter


# =========================
# Конфиг: кэш и лимиты
# =========================

CACHE_DIR = os.environ.get("INTEL_CACHE_DIR", ".intel_cache")
CACHE_CLEAN_PROB = float(os.environ.get("INTEL_CACHE_CLEAN_PROB", "0.02"))  # 2% шанс чистить протухшее при каждом сохранении

# Конкурентность сборки снапшота
INTEL_CONCURRENCY = int(os.environ.get("INTEL_CONCURRENCY", "1"))

# Таймауты HTTP
HTTP_CONNECT_TIMEOUT = float(os.environ.get("INTEL_HTTP_CONNECT_TIMEOUT", "5.0"))
HTTP_READ_TIMEOUT = float(os.environ.get("INTEL_HTTP_READ_TIMEOUT", "8.0"))

# Минимальная пауза между запросами к одному хосту (сек)
MIN_GAP_PER_HOST = {
    "api.binance.com": 0.25,
    "fapi.binance.com": 0.25,
    "api.bybit.com": 0.25,
    "www.okx.com": 0.25,
    "www.deribit.com": 0.25,
    "stooq.com": 0.50,
    "stooq.pl": 0.50,
}

# Максимум «условных весов» на 60 секунд
MAX_WEIGHT_PER_MIN = {
    "api.binance.com": 300,
    "fapi.binance.com": 300,
    "api.bybit.com": 120,
    "www.okx.com": 120,
    "www.deribit.com": 120,
    "stooq.com": 60,
    "stooq.pl": 60,
}

def endpoint_weight(host: str, path: str) -> int:
    p = path.lower()
    if "aggtrades" in p:
        return 10
    if "ticker/24hr" in p:
        return 4
    if "klines" in p:
        return 2
    if "depth" in p:
        return 3
    if "openinterest" in p:
        return 2
    if "futures/data" in p:
        return 2
    if "premiumindex" in p:
        return 1
    return 1

def endpoint_ttl_seconds(host: str, path: str, params: Dict[str, Any]) -> int:
    """TTL увеличены так, чтобы часовые запуски реально переиспользовали кэш."""
    p = path.lower()
    # Stooq snapshot
    if (host.endswith("stooq.com") or host.endswith("stooq.pl")) and p.startswith("/q/l"):
        return 300  # 5 минут
    # Binance breadth
    if p.endswith("/api/v3/ticker/24hr"):
        return 120  # 2 минуты
    # Ratios / OI
    if "/futures/data/" in p:
        return 600  # 10 минут
    # Klines (index/continuous/premiumIndexKlines)
    if p.endswith("/fapi/v1/indexpriceklines") or p.endswith("/fapi/v1/continuousklines"):
        return 3600  # 1 час
    if p.endswith("/fapi/v1/premiumindexklines"):
        return 3600  # 1 час
    # Premium index (mark/index/funding)
    if p.endswith("/fapi/v1/premiumindex"):
        return 20
    # Spot price (стейблы)
    if p.endswith("/api/v3/ticker/price"):
        return 30
    # Depth
    if p.endswith("/api/v3/depth"):
        return 5
    # Futures/spot klines
    if p.endswith("/fapi/v1/klines"):
        return 3600  # 1 час
    # AggTrades — кэшируем на 2 часа для межчасового reuse
    if p.endswith("/api/v3/aggtrades") or p.endswith("/fapi/v1/aggtrades"):
        return 7200  # 2 часа
    # Остальное
    return 60


# =========================
# Дисковый кэш (потокобезопасный + авто-фолбэк)
# =========================

class FileCache:
    def __init__(self, root: str = CACHE_DIR):
        self._lock = threading.Lock()
        # 1) Пробуем указанный путь
        root = self._ensure_writable(root)
        # 2) Если нет — фолбэки
        if root is None:
            for alt in (
                os.path.expanduser("~/.intel_cache_tbot"),
                "/var/tmp/intel_cache_tbot",
                "/tmp/intel_cache_tbot",
            ):
                root = self._ensure_writable(alt)
                if root:
                    break
        # 3) Если совсем никак — кэш будет «пустым»
        self.root = root or ""
        if not self.root:
            # Важно: продолжаем работу без кэша (это лучше, чем падать)
            pass

    def _ensure_writable(self, path: str) -> Optional[str]:
        try:
            if not path:
                return None
            os.makedirs(path, exist_ok=True)
            testfile = os.path.join(path, ".write_test")
            with open(testfile, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(testfile)
            return path
        except Exception:
            return None

    @staticmethod
    def _key_to_name(key: str) -> str:
        h = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return f"{h}.json"

    def _path_for(self, key: str) -> Optional[str]:
        if not self.root:
            return None
        return os.path.join(self.root, self._key_to_name(key))

    def get_with_meta(self, key: str) -> Tuple[Optional[Any], bool]:
        path = self._path_for(key)
        if not path:
            return None, True
        try:
            with self._lock:
                with open(path, "r", encoding="utf-8") as f:
                    obj = json.load(f)
            exp = float(obj.get("expires", 0.0))
            is_expired = (exp < time.time())
            ctype = obj.get("ctype")
            if ctype in ("json", "text"):
                return obj.get("value"), is_expired
            return None, True
        except FileNotFoundError:
            return None, True
        except Exception:
            return None, True

    def get(self, key: str) -> Optional[Any]:
        val, expired = self.get_with_meta(key)
        return val if not expired else None

    def set(self, key: str, value: Any, ttl_seconds: int, ctype: str):
        path = self._path_for(key)
        if not path:
            return  # кэш выключен (нет доступного каталога)
        obj = {
            "expires": time.time() + max(1, int(ttl_seconds)),
            "ctype": ctype,
            "value": value,
        }
        tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
        try:
            with self._lock:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(obj, f)
                os.replace(tmp, path)
        except Exception:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
        # редкая уборка
        if random.random() < CACHE_CLEAN_PROB:
            self._cleanup()

    def _cleanup(self):
        if not self.root:
            return
        now = time.time()
        try:
            with self._lock:
                names = list(os.listdir(self.root))
            for name in names:
                if not name.endswith(".json"):
                    continue
                path = os.path.join(self.root, name)
                try:
                    with self._lock:
                        with open(path, "r", encoding="utf-8") as f:
                            obj = json.load(f)
                    if float(obj.get("expires", 0.0)) < now:
                        with self._lock:
                            try:
                                os.remove(path)
                            except Exception:
                                pass
                except Exception:
                    try:
                        with self._lock:
                            os.remove(path)
                    except Exception:
                        pass
        except Exception:
            pass


# =========================
# RateLimiter (потокобезопасный)
# =========================

class RateLimiter:
    def __init__(self, host: str):
        self.host = host
        self.min_gap = float(MIN_GAP_PER_HOST.get(host, 0.25))
        self.max_per_min = int(MAX_WEIGHT_PER_MIN.get(host, 120))
        self._last_ts = 0.0
        self._win = deque()
        self._win_weight = 0
        self._lock = threading.Lock()

    def _prune_locked(self, now: float):
        while self._win and now - self._win[0][0] >= 60.0:
            _, w = self._win.popleft()
            self._win_weight -= w

    def acquire(self, weight: int):
        weight = max(1, int(weight))
        while True:
            sleep_for = 0.0
            with self._lock:
                now = time.time()
                # min-gap
                gap = now - self._last_ts
                if gap < self.min_gap:
                    sleep_for = self.min_gap - gap
                else:
                    # вес/мин
                    self._prune_locked(now)
                    if self._win_weight + weight > self.max_per_min:
                        oldest_ts = self._win[0][0] if self._win else now
                        sleep_for = max(0.05, 60.0 - (now - oldest_ts))
                    else:
                        # можно проходить
                        self._win.append((now, weight))
                        self._win_weight += weight
                        self._last_ts = now
                        sleep_for = 0.0
            if sleep_for > 0.0:
                time.sleep(min(sleep_for, 5.0))
                continue
            return


# =========================
# Утилиты
# =========================

def _now_ms() -> int:
    return int(time.time() * 1000)

def _safe_float(x: Any, default: float = float("nan")) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _pct(a: float, b: float) -> float:
    if not math.isfinite(a) or a == 0 or not math.isfinite(b):
        return float("nan")
    return (b - a) / a


# =========================
# HTTP клиент (кэш + лимит + stale fallback + пул)
# =========================

class Http:
    def __init__(self, base_url: str, cache: FileCache):
        self.base = base_url.rstrip("/")
        self.s = requests.Session()
        # Пул соединений для многопоточности / keep-alive
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0)
        self.s.mount("https://", adapter)
        self.s.mount("http://", adapter)

        self.host = self.base.split("://", 1)[-1]
        self.limiter = RateLimiter(self.host)
        self.cache = cache

        self.timeout_connect = HTTP_CONNECT_TIMEOUT
        self.timeout_read = HTTP_READ_TIMEOUT
        self.max_retries = 6
        self.base_backoff = 0.6
        self.max_backoff = 8.0
        self.small_jitter = 0.2

        self.default_headers = {
            "User-Agent": "intel-safe-client/1.0",
            "Accept": "application/json,text/csv;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

        self._io_lock = threading.Lock()  # Session.get — под замком для чистоты потоков

    def _make_key(self, path: str, params: Optional[Dict[str, Any]]) -> str:
        q = ""
        if params:
            p2 = {k: v for k, v in params.items() if v is not None}
            q = "?" + urlencode(sorted(p2.items()), doseq=True, safe=",:")
        return f"{self.base}{path}{q}"

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        key = self._make_key(path, params)
        ttl = endpoint_ttl_seconds(self.host, path, params or {})
        # Пробуем кэш (с метаданными, чтобы уметь отдать «просрочку» при аварии)
        cached_val, cached_expired = self.cache.get_with_meta(key)
        if cached_val is not None and not cached_expired:
            return cached_val

        url = f"{self.base}{path}"
        w = endpoint_weight(self.host, path)
        last_err = None
        backoff = self.base_backoff

        for _ in range(self.max_retries + 1):
            self.limiter.acquire(w)
            try:
                with self._io_lock:
                    r = self.s.get(
                        url,
                        params=params,
                        headers={**self.default_headers, **(headers or {})},
                        timeout=(self.timeout_connect, self.timeout_read),
                    )
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    if ra:
                        try:
                            wait = float(ra)
                        except Exception:
                            wait = backoff
                    else:
                        wait = backoff
                    wait = min(wait + random.uniform(0, self.small_jitter), self.max_backoff)
                    time.sleep(wait)
                    backoff = min(backoff * 2.0, self.max_backoff)
                    continue
                if r.status_code == 418:
                    raise RuntimeError(f"HTTP 418 from {url} — hard rate limit triggered.")
                r.raise_for_status()

                ctype = (r.headers.get("Content-Type") or "").lower()
                if "application/json" in ctype:
                    val = r.json()
                    self.cache.set(key, val, ttl, "json")
                    return val
                # текст (CSV Stooq и т.п.)
                txt = r.text
                self.cache.set(key, txt, ttl, "text")
                return txt

            except requests.RequestException as e:
                last_err = e
                wait = min(backoff + random.uniform(0, self.small_jitter), self.max_backoff)
                time.sleep(wait)
                backoff = min(backoff * 2.0, self.max_backoff)

        # Все попытки не удались — если в кэше была просроченная копия, отдадим её.
        if cached_val is not None:
            return cached_val

        raise RuntimeError(f"GET failed {url} params={params}: {last_err}")


# =========================
# API-клиенты
# =========================

BINANCE_SPOT = "https://api.binance.com"
BINANCE_FAPI = "https://fapi.binance.com"
BYBIT = "https://api.bybit.com"
OKX = "https://www.okx.com"
DERIBIT = "https://www.deribit.com"
STOOQ_BASES = ["https://stooq.com", "https://stooq.pl"]

class BinancePublic:
    def __init__(self, cache: FileCache):
        self.spot = Http(BINANCE_SPOT, cache)
        self.fapi = Http(BINANCE_FAPI, cache)

    # Klines/цена/базис
    def fapi_klines(self, symbol: str, interval: str, start_time: int, end_time: int, limit: int = 500) -> List[list]:
        return self.fapi.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "startTime": start_time, "endTime": end_time, "limit": limit})

    def fapi_premium_index(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        return self.fapi.get("/fapi/v1/premiumIndex", {"symbol": symbol} if symbol else {})

    def fapi_premium_index_klines(self, symbol: str, interval: str, start_time: int, end_time: int, limit: int = 500) -> List[list]:
        # Важно: endTime (не EndTime)
        return self.fapi.get("/fapi/v1/premiumIndexKlines", {"symbol": symbol, "interval": interval, "startTime": start_time, "endTime": end_time, "limit": limit})

    def fapi_continuous_klines(self, pair: str, contract_type: str, interval: str, start_time: int, end_time: int, limit: int = 500) -> List[list]:
        return self.fapi.get("/fapi/v1/continuousKlines", {"pair": pair, "contractType": contract_type, "interval": interval, "startTime": start_time, "endTime": end_time, "limit": limit})

    def fapi_index_price_klines(self, pair: str, interval: str, start_time: int, end_time: int, limit: int = 500) -> List[list]:
        return self.fapi.get("/fapi/v1/indexPriceKlines", {"pair": pair, "interval": interval, "startTime": start_time, "endTime": end_time, "limit": limit})

    # OI / ratios
    def fapi_open_interest_hist(self, symbol: str, period: str, start_time: int, end_time: int, limit: int = 200) -> List[Dict[str, Any]]:
        return self.fapi.get("/futures/data/openInterestHist", {"symbol": symbol, "period": period, "limit": limit, "startTime": start_time, "endTime": end_time})

    def futures_data_taker_long_short_ratio(self, symbol: str, period: str, limit: int = 30, start_time: Optional[int] = None, end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        p = {"symbol": symbol, "period": period, "limit": limit}
        if start_time: p["startTime"] = start_time
        if end_time:   p["endTime"] = end_time
        return self.fapi.get("/futures/data/takerlongshortRatio", p)

    def futures_data_global_long_short_account_ratio(self, symbol: str, period: str, limit: int = 30, start_time: Optional[int] = None, end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        p = {"symbol": symbol, "period": period, "limit": limit}
        if start_time: p["startTime"] = start_time
        if end_time:   p["endTime"] = end_time
        return self.fapi.get("/futures/data/globalLongShortAccountRatio", p)

    def futures_data_top_long_short_account_ratio(self, symbol: str, period: str, limit: int = 30, start_time: Optional[int] = None, end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        p = {"symbol": symbol, "period": period, "limit": limit}
        if start_time: p["startTime"] = start_time
        if end_time:   p["endTime"] = end_time
        return self.fapi.get("/futures/data/topLongShortAccountRatio", p)

    def futures_data_top_long_short_position_ratio(self, symbol: str, period: str, limit: int = 30, start_time: Optional[int] = None, end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        p = {"symbol": symbol, "period": period, "limit": limit}
        if start_time: p["startTime"] = start_time
        if end_time:   p["endTime"] = end_time
        return self.fapi.get("/futures/data/topLongShortPositionRatio", p)

    # Лента/книга/тикеры
    def spot_agg_trades(self, symbol: str, start_time: int, end_time: int, limit: int = 1000) -> List[Dict[str, Any]]:
        return self.spot.get("/api/v3/aggTrades", {"symbol": symbol, "startTime": start_time, "endTime": end_time, "limit": min(limit, 1000)})

    def fapi_agg_trades(self, symbol: str, start_time: int, end_time: int, limit: int = 1000) -> List[Dict[str, Any]]:
        return self.fapi.get("/fapi/v1/aggTrades", {"symbol": symbol, "startTime": start_time, "endTime": end_time, "limit": min(limit, 1000)})

    def spot_depth(self, symbol: str, limit: int = 5000) -> Dict[str, Any]:
        return self.spot.get("/api/v3/depth", {"symbol": symbol, "limit": min(limit, 5000)})

    def spot_ticker_24hr_all(self) -> List[Dict[str, Any]]:
        return self.spot.get("/api/v3/ticker/24hr", None)

    def spot_ticker_price(self, symbol: str) -> Dict[str, Any]:
        return self.spot.get("/api/v3/ticker/price", {"symbol": symbol})


class BybitPublic:
    def __init__(self, cache: FileCache):
        self.http = Http(BYBIT, cache)
    def linear_ticker(self, symbol: str) -> Dict[str, Any]:
        data = self.http.get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        if isinstance(data, dict) and "result" in data and "list" in data["result"] and data["result"]["list"]:
            return data["result"]["list"][0]
        return {}


class OkxPublic:
    def __init__(self, cache: FileCache):
        self.http = Http(OKX, cache)
    def swap_ticker(self, inst_id: str = "BTC-USDT-SWAP") -> Dict[str, Any]:
        data = self.http.get("/api/v5/market/ticker", {"instId": inst_id})
        if isinstance(data, dict) and "data" in data and data["data"]:
            return data["data"][0]
        return {}
    def funding_rate(self, inst_id: str = "BTC-USDT-SWAP") -> Dict[str, Any]:
        data = self.http.get("/api/v5/public/funding-rate", {"instId": inst_id})
        if isinstance(data, dict) and "data" in data and data["data"]:
            return data["data"][0]
        return {}


class DeribitPublic:
    def __init__(self, cache: FileCache):
        self.http = Http(DERIBIT, cache)
    def book_summary_perpetual(self, instrument: str = "BTC-PERPETUAL") -> Dict[str, Any]:
        data = self.http.get("/api/v2/public/get_book_summary_by_instrument", {"instrument_name": instrument})
        if isinstance(data, dict) and "result" in data and data["result"]:
            return data["result"][0]
        return {}


class StooqPublic:
    def __init__(self, cache: FileCache):
        self.clients = [Http(base, cache) for base in STOOQ_BASES]

    def _try_one(self, http: Http, symbol: str) -> Optional[Dict[str, Any]]:
        txt = http.get("/q/l/", {"s": symbol.lower(), "f": "sd2t2ohlcv", "h": "", "e": "csv"})
        if not isinstance(txt, str) or not txt.strip():
            return None
        lines = [ln.strip() for ln in txt.strip().splitlines() if ln.strip()]
        if len(lines) < 2:
            return None
        header = [h.strip().lower() for h in lines[0].split(",")]
        row = [c.strip() for c in lines[1].split(",")]
        idx = {name: i for i, name in enumerate(header)}
        def col(name: str) -> Optional[str]:
            i = idx.get(name)
            return row[i] if i is not None and i < len(row) else None
        o = _safe_float(col("open")); h = _safe_float(col("high"))
        l = _safe_float(col("low"));  c = _safe_float(col("close"))
        v = _safe_float(col("volume"))
        date_s = col("date") or ""
        time_s = col("time") or ""
        return {
            "symbol": (col("symbol") or symbol.upper()),
            "date": date_s, "time": time_s,
            "open": o, "high": h, "low": l, "close": c, "volume": v,
            "intraday_change_pct": _pct(o, c),
            "valid": (math.isfinite(o) and math.isfinite(c)),
        }

    def quote_snapshot(self, symbol: str) -> Dict[str, Any]:
        last_err = None
        for http in self.clients:
            try:
                q = self._try_one(http, symbol)
                if q and q.get("valid"):
                    return q
            except Exception as e:
                last_err = e
                continue
        return {"symbol": symbol.upper(), "valid": False, "_partial": True, "_reason": (str(last_err) if last_err else "unavailable")}


# =========================
# Вспомогательные вычисления
# =========================

def _sum_quote_from_aggtrades(trades: List[Dict[str, Any]]) -> Tuple[float, float]:
    buy_q, sell_q = 0.0, 0.0
    for t in trades:
        price = _safe_float(t.get("p") or t.get("price"))
        qty   = _safe_float(t.get("q") or t.get("qty") or t.get("baseQty"))
        if not (math.isfinite(price) and math.isfinite(qty)):
            continue
        quote = price * qty
        is_buyer_maker = bool(t.get("m"))
        if is_buyer_maker:
            sell_q += quote   # taker SELL
        else:
            buy_q += quote    # taker BUY
    return buy_q, sell_q

def _orderbook_tilt(depth: Dict[str, Any], mid: float, pct_radius: float) -> Dict[str, float]:
    hi = mid * (1 + pct_radius)
    lo = mid * (1 - pct_radius)
    bids = depth.get("bids") or []
    asks = depth.get("asks") or []
    bid_vol = 0.0
    ask_vol = 0.0
    for p, q in bids:
        fp, fq = _safe_float(p), _safe_float(q)
        if math.isfinite(fp) and math.isfinite(fq) and lo <= fp <= hi:
            bid_vol += fq
    for p, q in asks:
        fp, fq = _safe_float(p), _safe_float(q)
        if math.isfinite(fp) and math.isfinite(fq) and lo <= fp <= hi:
            ask_vol += fq
    tilt = (bid_vol / ask_vol) if (ask_vol > 0) else float("inf")
    return {"bid_vol_in_band": bid_vol, "ask_vol_in_band": ask_vol, "tilt_bid_over_ask": tilt}

def _interval_to_ms(period: str) -> int:
    period = period.strip().lower()
    if period.endswith("ms"): return int(period[:-2])
    if period.endswith("s"):  return int(period[:-1]) * 1000
    if period.endswith("m"):  return int(period[:-1]) * 60_000
    if period.endswith("h"):  return int(period[:-1]) * 3_600_000
    if period.endswith("d"):  return int(period[:-1]) * 86_400_000
    raise ValueError(f"Unknown period: {period}")


# =========================
# Основной фасад
# =========================

class MarketIntel:
    def __init__(self):
        self.cache = FileCache(CACHE_DIR)
        self.binance = BinancePublic(self.cache)
        self.bybit = BybitPublic(self.cache)
        self.okx = OkxPublic(self.cache)
        self.deribit = DeribitPublic(self.cache)
        self.stooq = StooqPublic(self.cache)

        # Бюджет aggTrades на сторону в одном снапшоте
        self.max_agg_calls_per_side = int(os.environ.get("INTEL_MAX_AGG_CALLS", "20"))
        self.agg_throttle_range = (0.05, 0.15)

    # ---- БАЗОВЫЕ БЛОКИ Binance ----

    def price_block(self, symbol: str, lookback_hours: float, interval: str = "5m") -> Dict[str, Any]:
        end = _now_ms(); start = end - int(lookback_hours * 3_600_000)
        kl = self.binance.fapi_klines(symbol, interval, start, end)
        if not kl:
            return {}
        o = _safe_float(kl[0][1]); c = _safe_float(kl[-1][4])
        chg = _pct(o, c)
        return {"open": o, "close": c, "change_pct": chg, "bars": len(kl),
                "t_start": kl[0][0], "t_end": kl[-1][6] if len(kl[-1]) > 6 else kl[-1][0]}

    def open_interest_block(self, symbol: str, lookback_hours: float, period: str = "5m") -> Dict[str, Any]:
        end = _now_ms(); start = end - int(lookback_hours * 3_600_000)
        hist = self.binance.fapi_open_interest_hist(symbol, period, start, end, limit=200)
        if not hist:
            return {}
        oi_then = _safe_float(hist[0].get("sumOpenInterest"))
        oi_now  = _safe_float(hist[-1].get("sumOpenInterest"))
        chg = _pct(oi_then, oi_now)
        return {"oi_then": oi_then, "oi_now": oi_now, "oi_change_pct": chg, "points": len(hist),
                "t_start": int(hist[0].get("timestamp") or 0), "t_end": int(hist[-1].get("timestamp") or 0)}

    def funding_basis_block(self, symbol: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        pi = self.binance.fapi_premium_index(symbol)
        end = _now_ms(); start = end - 3_600_000
        pik = self.binance.fapi_premium_index_klines(symbol, "5m", start, end, limit=24)
        basis_last_close = _safe_float(pik[-1][4]) if pik else float("nan")
        basis_then_open  = _safe_float(pik[0][1]) if pik else float("nan")
        basis_now = float("nan")
        if pi:
            mark = _safe_float(pi.get("markPrice"))
            index = _safe_float(pi.get("indexPrice"))
            if math.isfinite(mark) and math.isfinite(index) and index != 0:
                basis_now = (mark - index) / index
        funding_rate = _safe_float(pi.get("lastFundingRate")) if pi else float("nan")
        who_pays = None
        if math.isfinite(funding_rate):
            who_pays = "longs_pay_shorts" if funding_rate > 0 else "shorts_pay_longs" if funding_rate < 0 else "neutral"
        funding = {
            "rates": [], "avg_rate": float("nan"),
            "last_funding_rate": funding_rate, "who_pays_now": who_pays,
            "mark_price": _safe_float(pi.get("markPrice")) if pi else float("nan"),
            "index_price": _safe_float(pi.get("indexPrice")) if pi else float("nan"),
            "snapshot_time": int(pi.get("time") or 0) if pi else None,
        }
        basis = {
            "basis_now": basis_now,
            "basis_last_close": basis_last_close,
            "basis_then_open": basis_then_open,
            "basis_change_abs": (basis_now - basis_then_open) if (math.isfinite(basis_now) and math.isfinite(basis_then_open)) else float("nan"),
            "bars": len(pik),
        }
        return funding, basis

    # --- Кэш-дружественный сбор aggTrades ---
    def _collect_agg_trades_safe(self, fetch_fn, symbol: str, lookback_hours: float) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        end_ms = _now_ms()
        start_ms = end_ms - int(lookback_hours * 3_600_000)

        # Выравнивание по 1 минуте, базовый chunk 5 минут
        MIN_UNIT = 60_000
        CHUNK = 5 * MIN_UNIT

        def align_down(ts: int, unit: int = MIN_UNIT) -> int:
            return (ts // unit) * unit

        cur = align_down(start_ms, MIN_UNIT)
        end_aligned = align_down(end_ms, MIN_UNIT)

        chunks: List[Tuple[int, int, int]] = []  # (start, end, size_ms)
        while cur < end_aligned:
            a = cur
            b = min(end_aligned, a + CHUNK)
            chunks.append((a, b, CHUNK))
            cur = b

        results: List[Dict[str, Any]] = []
        calls_made = 0
        partial = False

        for (a, b, size_ms) in chunks:
            if calls_made >= self.max_agg_calls_per_side:
                partial = True
                break
            page = fetch_fn(symbol, start_time=a, end_time=b, limit=1000) or []
            results.extend(page); calls_made += 1
            time.sleep(random.uniform(*self.agg_throttle_range))

            n = len(page)
            if n >= 1000 and size_ms > MIN_UNIT and calls_made < self.max_agg_calls_per_side:
                mid = a + size_ms // 2
                sub1 = fetch_fn(symbol, start_time=a,   end_time=mid, limit=1000) or []
                results.extend(sub1); calls_made += 1
                time.sleep(random.uniform(*self.agg_throttle_range))
                if calls_made < self.max_agg_calls_per_side:
                    sub2 = fetch_fn(symbol, start_time=mid, end_time=b,   limit=1000) or []
                    results.extend(sub2); calls_made += 1
                    time.sleep(random.uniform(*self.agg_throttle_range))

        results = [t for t in results if int(t.get("T") or t.get("time") or 0) >= start_ms]
        meta = {"_calls_made": calls_made, "_max_calls": self.max_agg_calls_per_side, "_partial": partial,
                "_window_ms": (end_ms - start_ms)}
        return results, meta

    # def flows_block(self, spot_symbol: str, perp_symbol: str, lookback_hours: float) -> Dict[str, Any]:
    #     spot_tr, spot_meta = self._collect_agg_trades_safe(self.binance.spot_agg_trades, spot_symbol, lookback_hours)
    #     perp_tr, perp_meta = self._collect_agg_trades_safe(self.binance.fapi_agg_trades,  perp_symbol,  lookback_hours)

    #     sb, ss = _sum_quote_from_aggtrades(spot_tr)
    #     pb, ps = _sum_quote_from_aggtrades(perp_tr)

    #     spot_net = sb - ss
    #     perp_net = pb - ps
    #     return {
    #         "spot": {
    #             "taker_buy_quote": sb, "taker_sell_quote": ss, "taker_net_quote": spot_net,
    #             "sense": "net_taker_buy" if spot_net > 0 else "net_taker_sell" if spot_net < 0 else "balanced",
    #         },
    #         "perp": {
    #             "taker_buy_quote": pb, "taker_sell_quote": ps, "taker_net_quote": perp_net,
    #             "sense": "net_taker_buy" if perp_net > 0 else "net_taker_sell" if perp_net < 0 else "balanced",
    #         },
    #         "spot_vs_perp": {
    #             "spot_net_minus_perp_net": spot_net - perp_net,
    #             "spot_stronger_than_perp": (spot_net > perp_net),
    #         },
    #         "_meta": {"spot": spot_meta, "perp": perp_meta}
    #     }
    def flows_block(self, spot_symbol: str, perp_symbol: str, lookback_hours: float) -> Dict[str, Any]:
        """
        Быстрый расчёт тэйкерных потоков в $ по споту и перпету через Klines:
          - taker_buy_quote: сумма 'taker buy quote asset volume' за окно
          - taker_sell_quote: total_quote - taker_buy_quote
          - taker_net_quote: разница покупок и продаж тэйкеров
        Возвращает ту же структуру полей, что и прежняя версия на aggTrades.
        """
        end_ms = _now_ms()
        start_ms = end_ms - int(lookback_hours * 3_600_000)

        # Подбираем минимальный интервал, чтобы уложиться в 1000 баров (лимит Binance)
        def _choose_interval(window_ms: int) -> Tuple[str, int]:
            # (interval_str, interval_ms) — отсортировано от более тонкого к более грубому
            candidates = [
                ("1m",   60_000),
                ("3m",  180_000),
                ("5m",  300_000),
                ("15m", 900_000),
                ("30m", 1_800_000),
                ("1h",  3_600_000),
            ]
            for iv, ms in candidates:
                # +1 бар на хвост, чтобы точно покрыть окно
                need = (window_ms + ms - 1) // ms + 1
                if need <= 1000:
                    return iv, ms
            return "1h", 3_600_000  # крайний случай (длинные окна)

        interval, _ = _choose_interval(end_ms - start_ms)

        def _sum_from_klines(klines: List[List[Any]]) -> Tuple[float, float, Dict[str, Any]]:
            """
            Возвращает (taker_buy_quote, taker_sell_quote, meta)
            Индексы Binance Klines:
              [7]  = quote asset volume
              [10] = taker buy quote asset volume
              [0]  = open time (ms)
              [6]  = close time (ms)
            """
            total_quote = 0.0
            taker_buy_quote = 0.0
            t_start = klines[0][0] if klines else None
            t_end = klines[-1][6] if klines and len(klines[-1]) > 6 else (klines[-1][0] if klines else None)
            for k in klines:
                q_all = _safe_float(k[7])
                q_tbq = _safe_float(k[10])
                if math.isfinite(q_all):
                    total_quote += q_all
                if math.isfinite(q_tbq):
                    taker_buy_quote += q_tbq
            taker_sell_quote = total_quote - taker_buy_quote
            meta = {
                "source": "klines",
                "interval": interval,
                "bars": len(klines),
                "t_start": t_start,
                "t_end": t_end,
                "_calls_made": 1 if klines else 0,
                "_max_calls": 1,
                "_partial": False,
                "_window_ms": (end_ms - start_ms),
            }
            return taker_buy_quote, taker_sell_quote, meta

        # --- Загрузка Klines ---
        # Spot klines (через self.binance.spot Http-клиент)
        spot_kl = []
        try:
            spot_kl = self.binance.spot.get(
                "/api/v3/klines",
                {"symbol": spot_symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
            ) or []
            # Binance может вернуть больше, чем нужно — обрежем строго по окну
            spot_kl = [k for k in spot_kl if isinstance(k, list) and k and start_ms <= int(k[0]) <= end_ms]
        except Exception as e:
            spot_kl = []

        # Perp klines (USD-M futures)
        perp_kl = []
        try:
            perp_kl = self.binance.fapi_klines(perp_symbol, interval, start_ms, end_ms, limit=1000) or []
            perp_kl = [k for k in perp_kl if isinstance(k, list) and k and start_ms <= int(k[0]) <= end_ms]
        except Exception as e:
            perp_kl = []

        # --- Агрегация ---
        sb, ss, spot_meta = _sum_from_klines(spot_kl)
        pb, ps, perp_meta = _sum_from_klines(perp_kl)

        spot_net = sb - ss
        perp_net = pb - ps

        return {
            "spot": {
                "taker_buy_quote": sb,
                "taker_sell_quote": ss,
                "taker_net_quote": spot_net,
                "sense": "net_taker_buy" if spot_net > 0 else "net_taker_sell" if spot_net < 0 else "balanced",
            },
            "perp": {
                "taker_buy_quote": pb,
                "taker_sell_quote": ps,
                "taker_net_quote": perp_net,
                "sense": "net_taker_buy" if perp_net > 0 else "net_taker_sell" if perp_net < 0 else "balanced",
            },
            "spot_vs_perp": {
                "spot_net_minus_perp_net": spot_net - perp_net,
                "spot_stronger_than_perp": (spot_net > perp_net),
            },
            "_meta": {"spot": spot_meta, "perp": perp_meta},
        }


    def orderbook_block(self, spot_symbol: str, use_price: Optional[float] = None) -> Dict[str, Any]:
        depth = self.binance.spot_depth(spot_symbol, limit=5000)
        best_bid = _safe_float(depth.get("bids", [[math.nan]])[0][0])
        best_ask = _safe_float(depth.get("asks", [[math.nan]])[0][0])
        mid = use_price if (use_price and math.isfinite(use_price)) else (best_bid + best_ask) / 2.0
        bands = {}
        for pct_band in (0.005, 0.01):
            k = f"{pct_band*100:.2f}%"
            bands[k] = _orderbook_tilt(depth, mid, pct_band)
        return {"mid": mid, "bands": bands}

    # ---- Кросс-биржи/календарь/рацио/ширина/стейблы/макро ----

    def cross_exchange_perp_snapshot(self, symbol_binance: str = "BTCUSDT",
                                     bybit_symbol: str = "BTCUSDT",
                                     okx_inst: str = "BTC-USDT-SWAP",
                                     deribit_instr: str = "BTC-PERPETUAL") -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        try:
            b = self.binance.fapi_premium_index(symbol_binance)
            if b:
                mark = _safe_float(b.get("markPrice")); index = _safe_float(b.get("indexPrice"))
                basis = (mark - index) / index if (math.isfinite(mark) and math.isfinite(index) and index) else float("nan")
                fr = _safe_float(b.get("lastFundingRate"))
                out["binance"] = {"mark": mark, "index": index, "basis": basis,
                                  "fundingRate": fr,
                                  "whoPays": "longs_pay_shorts" if (math.isfinite(fr) and fr > 0) else "shorts_pay_longs" if (math.isfinite(fr) and fr < 0) else "neutral",
                                  "ts": int(b.get("time") or 0)}
        except Exception as e:
            out["binance"] = {"_partial": True, "_reason": str(e)}
        try:
            y = self.bybit.linear_ticker(bybit_symbol)
            if y:
                mark = _safe_float(y.get("markPrice")); index = _safe_float(y.get("indexPrice"))
                basis = (mark - index) / index if (math.isfinite(mark) and math.isfinite(index) and index) else float("nan")
                fr = _safe_float(y.get("fundingRate"))
                out["bybit"] = {"mark": mark, "index": index, "basis": basis,
                                "fundingRate": fr,
                                "whoPays": "longs_pay_shorts" if (math.isfinite(fr) and fr > 0) else "shorts_pay_longs" if (math.isfinite(fr) and fr < 0) else "neutral",
                                "ts": int(y.get("ts") or 0)}
        except Exception as e:
            out["bybit"] = {"_partial": True, "_reason": str(e)}
        try:
            o_ticker = self.okx.swap_ticker(okx_inst)
            o_funding = self.okx.funding_rate(okx_inst)
            if o_ticker:
                mark = _safe_float(o_ticker.get("markPx")); index = _safe_float(o_ticker.get("idxPx"))
                basis = (mark - index) / index if (math.isfinite(mark) and math.isfinite(index) and index) else float("nan")
                fr = _safe_float((o_funding or {}).get("fundingRate"))
                out["okx"] = {"mark": mark, "index": index, "basis": basis,
                              "fundingRate": fr,
                              "whoPays": "longs_pay_shorts" if (math.isfinite(fr) and fr > 0) else "shorts_pay_longs" if (math.isfinite(fr) and fr < 0) else "neutral",
                              "ts": int(o_ticker.get("ts") or 0)}
        except Exception as e:
            out["okx"] = {"_partial": True, "_reason": str(e)}
        try:
            d = self.deribit.book_summary_perpetual(deribit_instr)
            if d:
                mark = _safe_float(d.get("mark_price")); index = _safe_float(d.get("index_price"))
                basis = (mark - index) / index if (math.isfinite(mark) and math.isfinite(index) and index) else float("nan")
                out["deribit"] = {"mark": mark, "index": index, "basis": basis, "fundingRate": None, "whoPays": None, "ts": int(d.get("timestamp") or 0)}
        except Exception as e:
            out["deribit"] = {"_partial": True, "_reason": str(e)}
        return out

    def calendar_basis_block(self, pair: str = "BTCUSDT", interval: str = "5m", lookback_hours: float = 2.0) -> Dict[str, Any]:
        end = _now_ms(); start = end - int(lookback_hours * 3_600_000)
        def last_basis(contract_type: str) -> Dict[str, Any]:
            try:
                fut = self.binance.fapi_continuous_klines(pair, contract_type, interval, start, end, limit=200)
                idx = self.binance.fapi_index_price_klines(pair, interval, start, end, limit=200)
            except Exception as e:
                return {"_partial": True, "_reason": str(e), "bars": 0}
            if not fut or not idx:
                return {"basis_now": float("nan"), "basis_then_open": float("nan"), "bars": 0}
            f_close = _safe_float(fut[-1][4]); i_close = _safe_float(idx[-1][4])
            f_open0 = _safe_float(fut[0][1]);  i_open0 = _safe_float(idx[0][1])
            now  = (f_close - i_close) / i_close if (math.isfinite(f_close) and math.isfinite(i_close) and i_close) else float("nan")
            then = (f_open0 - i_open0) / i_open0 if (math.isfinite(f_open0) and math.isfinite(i_open0) and i_open0) else float("nan")
            return {"basis_now": now, "basis_then_open": then, "basis_change_abs": (now - then) if math.isfinite(now) and math.isfinite(then) else float("nan"),
                    "bars": min(len(fut), len(idx))}
        return {"current_quarter": last_basis("CURRENT_QUARTER"), "next_quarter": last_basis("NEXT_QUARTER")}

    def sentiment_ratios_block(self, symbol: str, period: str = "5m", lookback_points: int = 24) -> Dict[str, Any]:
        end = _now_ms(); start = end - (lookback_points * _interval_to_ms(period))
        def safeget(fn, **kw):
            try: return fn(**kw)
            except Exception as e: return {"_partial": True, "_reason": str(e)}
        taker = safeget(self.binance.futures_data_taker_long_short_ratio, symbol=symbol, period=period, limit=lookback_points, start_time=start, end_time=end)
        glob  = safeget(self.binance.futures_data_global_long_short_account_ratio, symbol=symbol, period=period, limit=lookback_points, start_time=start, end_time=end)
        top_a = safeget(self.binance.futures_data_top_long_short_account_ratio, symbol=symbol, period=period, limit=lookback_points, start_time=start, end_time=end)
        top_p = safeget(self.binance.futures_data_top_long_short_position_ratio, symbol=symbol, period=period, limit=lookback_points, start_time=start, end_time=end)
        def last_ratio(arr: Any, field: str) -> Optional[float]:
            if not isinstance(arr, list) or not arr: return None
            v = arr[-1].get(field) or arr[-1].get("buySellRatio")
            return _safe_float(v)
        return {
            "taker_buy_sell_ratio": last_ratio(taker, "buySellRatio"),
            "global_long_short_ratio": last_ratio(glob, "longShortRatio"),
            "top_trader_accounts_ratio": last_ratio(top_a, "longShortRatio"),
            "top_trader_positions_ratio": last_ratio(top_p, "longShortRatio"),
            "points": {"taker": len(taker) if isinstance(taker, list) else 0,
                       "global": len(glob) if isinstance(glob, list) else 0,
                       "top_accounts": len(top_a) if isinstance(top_a, list) else 0,
                       "top_positions": len(top_p) if isinstance(top_p, list) else 0}
        }

    def market_breadth_spot_usdt(self, top_n_by_quote_vol: int = 50) -> Dict[str, Any]:
        arr = self.binance.spot_ticker_24hr_all()
        usdt = [r for r in arr if isinstance(r, dict) and str(r.get("symbol", "")).endswith("USDT")]
        usdt.sort(key=lambda x: _safe_float(x.get("quoteVolume"), 0.0), reverse=True)
        top = usdt[:top_n_by_quote_vol]
        up = sum(1 for r in top if _safe_float(r.get("priceChangePercent")) > 0)
        down = sum(1 for r in top if _safe_float(r.get("priceChangePercent")) < 0)
        flat = len(top) - up - down
        return {"universe": len(usdt), "considered": len(top), "up": up, "down": down, "flat": flat, "advance_decline": up - down}

    def stablecoin_deviation(self, symbols: List[str] = ("USDCUSDT", "FDUSDUSDT", "USDPUSDT")) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for s in symbols:
            try:
                px = self.binance.spot_ticker_price(s); p = _safe_float(px.get("price"))
                out[s] = {"last": p, "deviation_from_1": (p - 1.0) if math.isfinite(p) else float("nan")}
            except Exception as e:
                out[s] = {"_partial": True, "_reason": str(e), "last": float("nan"), "deviation_from_1": float("nan")}
        return out

    def macro_weather_block(self) -> Dict[str, Any]:
        def get_one(sym: str) -> Dict[str, Any]:
            try:
                q = self.stooq.quote_snapshot(sym)
                if not q or not q.get("valid"):
                    return {"symbol": sym.upper(), "ok": False}
                intr = q.get("intraday_change_pct")
                sense = "up" if (isinstance(intr, float) and intr > 0) else "down" if (isinstance(intr, float) and intr < 0) else "flat"
                return {"symbol": q["symbol"], "date": q["date"], "time": q["time"],
                        "open": q["open"], "high": q["high"], "low": q["low"], "close": q["close"], "volume": q["volume"],
                        "intraday_change_pct": intr, "sense": sense, "ok": True}
            except Exception as e:
                return {"symbol": sym.upper(), "ok": False, "_partial": True, "_reason": str(e)}
        es = get_one("ES.F"); nq = get_one("NQ.F"); dx = get_one("DX.F")
        if not dx.get("ok"): dx = get_one("USD_I")
        def lean(s: Optional[str]) -> int: return 1 if s == "up" else -1 if s == "down" else 0
        return {"ES": es, "NQ": nq, "DXY": dx, "macro_lean_score": lean(es.get("sense")) + lean(nq.get("sense")) - lean(dx.get("sense"))}

    # ---- Компоновка ----

    def snapshot(self, symbol: str = "BTCUSDT", lookback_hours: float = 2.0, asof_utc: Optional[datetime] = None) -> Dict[str, Any]:
        if asof_utc is None:
            asof_utc = datetime.now(timezone.utc)

        # 1) Быстрый ценовой блок (нужен для orderbook mid)
        price = self.price_block(symbol, lookback_hours, interval="5m")

        # 2) Параллельно остальное (независимые блоки)
        results: Dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=max(2, INTEL_CONCURRENCY)) as ex:
            futs = {
                "open_interest": ex.submit(self.open_interest_block, symbol, lookback_hours, "5m"),
                "funding_basis": ex.submit(self.funding_basis_block, symbol),
                "flows":         ex.submit(self.flows_block, "BTCUSDT", symbol, lookback_hours),
                "orderbook":     ex.submit(self.orderbook_block, "BTCUSDT", price.get("close") if price else None),
                "x_perp":        ex.submit(self.cross_exchange_perp_snapshot, symbol, "BTCUSDT", "BTC-USDT-SWAP", "BTC-PERPETUAL"),
                "calendar_basis":ex.submit(self.calendar_basis_block, "BTCUSDT", "5m", lookback_hours),
                "sentiment":     ex.submit(self.sentiment_ratios_block, symbol, "5m", int(lookback_hours * 12)),
                "breadth":       ex.submit(self.market_breadth_spot_usdt, 50),
                "stablecoins":   ex.submit(self.stablecoin_deviation),
                "macro":         ex.submit(self.macro_weather_block),
            }
            for name, fut in futs.items():
                try:
                    results[name] = fut.result()
                except Exception as e:
                    if name == "funding_basis":
                        results[name] = ({"rates": [], "avg_rate": float("nan"),
                                          "last_funding_rate": float("nan"), "who_pays_now": None,
                                          "mark_price": float("nan"), "index_price": float("nan"),
                                          "snapshot_time": None},
                                         {"basis_now": float("nan"), "basis_last_close": float("nan"),
                                          "basis_then_open": float("nan"), "basis_change_abs": float("nan"), "bars": 0, "_partial": True, "_reason": str(e)})
                    else:
                        results[name] = {"_partial": True, "_reason": str(e)}

        funding, basis = results.get("funding_basis") if isinstance(results.get("funding_basis"), tuple) else ({"rates": [], "avg_rate": float("nan"),
                                                                                                                "last_funding_rate": float("nan"), "who_pays_now": None,
                                                                                                                "mark_price": float("nan"), "index_price": float("nan"),
                                                                                                                "snapshot_time": None},
                                                                                                               {"basis_now": float("nan"), "basis_last_close": float("nan"),
                                                                                                                "basis_then_open": float("nan"), "basis_change_abs": float("nan"), "bars": 0})

        open_interest = results.get("open_interest", {})
        flows = results.get("flows", {})
        orderbook = results.get("orderbook", {})
        x_perp = results.get("x_perp", {})
        cal_basis = results.get("calendar_basis", {})
        sent = results.get("sentiment", {})
        breadth = results.get("breadth", {})
        stables = results.get("stablecoins", {})
        macro = results.get("macro", {})

        # 3) Подсказки — как раньше
        hints: List[str] = []
        if open_interest and isinstance(open_interest.get("oi_change_pct"), float):
            if open_interest["oi_change_pct"] < 0:
                hints.append("Снижение OI — движение может быть закрытием позиций (хуже для продолжения).")
            elif open_interest["oi_change_pct"] > 0:
                hints.append("Рост OI — чаще набор новых позиций (устойчивее).")
        if basis and isinstance(basis.get("basis_change_abs"), float) and math.isfinite(basis["basis_change_abs"]):
            if basis["basis_change_abs"] > 0:
                hints.append("Базис расширяется вверх — поддерживает бычий сценарий.")
            elif basis["basis_change_abs"] < 0:
                hints.append("Базис сжимается — осторожность.")
        if isinstance(macro.get("macro_lean_score"), int):
            if macro["macro_lean_score"] > 0:
                hints.append("ES/NQ вверх и/или DXY вниз — внешний фон поддерживает рост крипто.")
            elif macro["macro_lean_score"] < 0:
                hints.append("ES/NQ вниз и/или DXY вверх — внешний фон против роста крипто.")

        return {
            "asof_utc": asof_utc.isoformat(),
            "symbol": symbol,
            "lookback_hours": lookback_hours,
            "price": price,
            "open_interest": open_interest,
            "funding": funding,
            "basis": basis,
            "flows": flows,
            "orderbook": orderbook,
            "x_perp": x_perp,
            "calendar_basis": cal_basis,
            "sentiment": sent,
            "breadth": breadth,
            "stablecoins": stables,
            "macro_weather": macro,
            "hints": hints,
        }
