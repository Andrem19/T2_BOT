# deribit_option_hub.py
"""
Универсальный «hub» для работы с опционами Deribit.
Полностью совместим по интерфейсу с bybit_option_hub.py.

© 2025 Andrew
"""
from __future__ import annotations

# ----------------------------------------------------------------------
#                              IMPORTS
# ----------------------------------------------------------------------
import json
import os
import queue
import threading
import time
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, List, Iterable, Optional, Set, Tuple, Union

import requests
import websocket                      # websocket-client ≥1.2
from dotenv import load_dotenv

__all__ = [
    "DeribitOptionHub",
    "TradingAPI",
    "MarketDataFeed",
    "ChainUtils",
    "PriceEstimator",
    "OptionTickerSnapshot",
    "OptionOrderbookSnapshot",
]

log = logging.getLogger(__name__)

# ----------------------------------------------------------------------
#                      1.  LOW-LEVEL REST CLIENT
# ----------------------------------------------------------------------
_MAIN_URL = "https://www.deribit.com/api/v2/"
_TEST_URL = "https://test.deribit.com/api/v2/"
_REST_TO  = 10         # seconds
_SPEC_CACHE: Dict[str, Dict[str, Any]] = {}


class _DeribitREST:
    """Один REST-клиент на пару client_id / client_secret (auto token-refresh)."""

    def __init__(self, *, client_id: str, client_secret: str,
                 testnet: bool = False) -> None:
        self._base   = _TEST_URL if testnet else _MAIN_URL
        self._cid    = client_id
        self._sec    = client_secret
        self._token: Optional[str] = None
        self._exp_ms = 0
        self._lock   = threading.RLock()

    # -------- auth --------
    def _auth(self) -> None:
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "public/auth",
            "params": {"grant_type": "client_credentials",
                       "client_id": self._cid,
                       "client_secret": self._sec},
        }
        r = requests.post(self._base, json=payload, timeout=_REST_TO)
        r.raise_for_status()
        res = r.json()["result"]
        self._token  = res["access_token"]
        self._exp_ms = int(time.time()*1000 + res["expires_in"]*1000)

    def _ensure_token(self) -> None:
        if self._token and time.time()*1000 < self._exp_ms - 5_000:
            return
        with self._lock:
            if self._token and time.time()*1000 < self._exp_ms - 5_000:
                return
            self._auth()

    # -------- rpc ---------
    def rpc(self, method: str, params: Dict[str, Any] | None = None,
            *, auth: bool | None = None) -> Any:
        if params is None:
            params = {}
        needs_auth = (auth if auth is not None
                      else not method.startswith("public/"))
        headers = {"Content-Type": "application/json"}
        if needs_auth:
            self._ensure_token()
            headers["Authorization"] = f"bearer {self._token}"  # type: ignore[arg-type]
        payload = {"jsonrpc": "2.0", "id": int(time.time()*1000),
                   "method": method, "params": params}
        r = requests.post(self._base, json=payload,
                          headers=headers, timeout=_REST_TO)
        try:
            resp = r.json()
        except ValueError:
            r.raise_for_status()
            raise
        if resp.get("error"):
            raise RuntimeError(f"Deribit error → {resp['error']}")
        return resp["result"]


# ----------------------------------------------------------------------
#                    2.  SESSION POOL  (account_idx → REST)
# ----------------------------------------------------------------------
class _SessionPool:
    _sessions: Dict[int, _DeribitREST] = {}
    _lock = threading.RLock()

    @classmethod
    def _load_env(cls) -> Dict[int, Dict[str, str]]:
        load_dotenv()
        out: Dict[int, Dict[str, str]] = {}
        i = 1
        while True:
            cid = os.getenv(f"DERBIT_ID_{i}")
            sec = os.getenv(f"DERBIT_SECRET_{i}")
            if not cid or not sec:
                break
            out[i] = {"client_id": cid, "client_secret": sec}
            i += 1
        if not out:
            raise RuntimeError("Не найдены DERBIT_ID_x / DERBIT_SECRET_x в .env")
        return out

    @classmethod
    def build_all(cls, *, testnet: bool) -> None:
        creds = cls._load_env()
        with cls._lock:
            for idx, cr in creds.items():
                if idx not in cls._sessions:
                    cls._sessions[idx] = _DeribitREST(**cr, testnet=testnet)

    @classmethod
    def get(cls, idx: int = 1) -> _DeribitREST:
        try:
            return cls._sessions[idx]
        except KeyError as e:
            raise RuntimeError("Session pool not initialised") from e


# ----------------------------------------------------------------------
#                         3.  DATA MODELS
# ----------------------------------------------------------------------
def _to_dec(v: Any) -> Optional[Decimal]:
    if v in (None, "", "0"*0):
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return None


class OptionTickerSnapshot(dict):
    __slots__ = ("updated_ts",)
    def __init__(self, d: Dict[str, Any], ts: int):
        super().__init__(d); self.updated_ts = ts
    bid       = property(lambda s: _to_dec(s.get("bidPrice")))
    bid_size  = property(lambda s: _to_dec(s.get("bidSize")))
    ask       = property(lambda s: _to_dec(s.get("askPrice")))
    ask_size  = property(lambda s: _to_dec(s.get("askSize")))
    mark      = property(lambda s: _to_dec(s.get("markPrice")))
    last      = property(lambda s: _to_dec(s.get("lastPrice")))
    delta     = property(lambda s: _to_dec(s.get("delta")))
    gamma     = property(lambda s: _to_dec(s.get("gamma")))
    vega      = property(lambda s: _to_dec(s.get("vega")))
    theta     = property(lambda s: _to_dec(s.get("theta")))
    hours_to_exp = property(lambda s: s.get("hours_to_exp"))


class OptionOrderbookSnapshot:
    __slots__ = ("bids", "asks", "updated_ts")

    def __init__(self,
                 bids: List[Tuple[Decimal, Decimal]],
                 asks: List[Tuple[Decimal, Decimal]],
                 ts: int):
        self.bids = sorted(bids, key=lambda x: x[0], reverse=True)
        self.asks = sorted(asks, key=lambda x: x[0])
        self.updated_ts = ts

    @staticmethod
    def _fill(levels: List[Tuple[Decimal, Decimal]], qty: Decimal
              ) -> Optional[Tuple[Decimal, Decimal, Decimal]]:
        left, cost = qty, Decimal("0")
        worst = Decimal("0"); worst_sz = Decimal("0")
        for px, sz in levels:
            if left <= 0:
                break
            take = sz if sz <= left else left
            cost += take * px
            worst, worst_sz = px, sz
            left -= take
        return None if left > 0 else (cost, worst, worst_sz)

    @staticmethod
    def _to_dec(v: Union[int, float, str, Decimal]) -> Decimal:
        return v if isinstance(v, Decimal) else Decimal(str(v))

    def ask_price_for_qty(self, qty, *, vwap=True):
        res = self._fill(self.asks, self._to_dec(qty))
        if not res:
            return None, None
        cost, worst, worst_sz = res
        return (cost/ self._to_dec(qty) if vwap else worst), worst_sz

    def bid_price_for_qty(self, qty, *, vwap=True):
        res = self._fill(self.bids, self._to_dec(qty))
        if not res:
            return None, None
        cost, worst, worst_sz = res
        return (cost/ self._to_dec(qty) if vwap else worst), worst_sz

    vwap_buy         = lambda s, q: s.ask_price_for_qty(q)[0]
    vwap_sell        = lambda s, q: s.bid_price_for_qty(q)[0]
    worst_buy_price  = lambda s, q: s.ask_price_for_qty(q, vwap=False)[0]
    worst_sell_price = lambda s, q: s.bid_price_for_qty(q, vwap=False)[0]

    total_bid_size = property(lambda s: sum(q for _, q in s.bids))
    total_ask_size = property(lambda s: sum(q for _, q in s.asks))



# -----------------------------------------------------------------------------
#                    4. OPTION DATA CACHE  (thread-safe)
# -----------------------------------------------------------------------------
class _OptionDataCache:
    def __init__(self) -> None:
        self._lock    = threading.RLock()
        self._tickers = {}     # symbol → OptionTickerSnapshot
        self._obooks  = {}     # symbol → OptionOrderbookSnapshot
        self._inst    = {}     # symbol → instrument-info dict

    # --- update ---
    def up_inst(self, sym: str, info: Dict[str, Any]) -> None:
        with self._lock:
            self._inst[sym] = info

    def up_tick(self, sym: str, data: Dict[str, Any], ts: int) -> None:
        with self._lock:
            self._tickers[sym] = OptionTickerSnapshot(data, ts)

    def up_ob(self, sym: str, bids_raw, asks_raw, ts: int) -> None:
        bids, asks = [], []
        for p, q in bids_raw:
            dp, dq = _to_dec(p), _to_dec(q)
            if dp and dq:
                bids.append((dp, dq))
        for p, q in asks_raw:
            dp, dq = _to_dec(p), _to_dec(q)
            if dp and dq:
                asks.append((dp, dq))
        with self._lock:
            self._obooks[sym] = OptionOrderbookSnapshot(bids, asks, ts)

    # --- get ---
    def get_ticker(self, sym: str) -> Optional[OptionTickerSnapshot]:
        with self._lock:
            return self._tickers.get(sym)

    def get_ob(self, sym: str) -> Optional[OptionOrderbookSnapshot]:
        with self._lock:
            return self._obooks.get(sym)

# ---------------------------------------------------------------------
# 5.  WEBSOCKET FEED  (ticker + orderbook)  — thread-safe singleton
# ---------------------------------------------------------------------
class _WSFeed:
    """
    Публичный WebSocket Deribit (каналы ticker.raw и book.<depth>ms).
    Используется всеми подсистемами через classmethod get().
    """
    _INSTANCE: "_WSFeed | None" = None
    _LOCK = threading.Lock()

    def __init__(self, *, testnet: bool, depth: int) -> None:
        self.url   = ("wss://test.deribit.com/ws/api/v2"
                      if testnet else "wss://www.deribit.com/ws/api/v2")
        self.depth = depth
        self.cache = _OptionDataCache()

        self._ws:   Optional[websocket.WebSocketApp] = None
        self._thr:  Optional[threading.Thread] = None
        self._stop = threading.Event()

        self._pending: "queue.Queue[str]" = queue.Queue()   # symbols-to-subscribe
        self._subs: set[str] = set()                        # already subscribed

        self._id   = 0
        self._id_lock = threading.Lock()

    # ---------- singleton ----------
    @classmethod
    def get(cls, *, testnet: bool, depth: int) -> "_WSFeed":
        if cls._INSTANCE:
            return cls._INSTANCE
        with cls._LOCK:
            if cls._INSTANCE:
                return cls._INSTANCE
            cls._INSTANCE = cls(testnet=testnet, depth=depth)
            cls._INSTANCE._start()
            return cls._INSTANCE

    # ---------- public helpers ----------
    def add_symbol(self, sym: str) -> None:
        self._pending.put(sym.upper())

    def add_symbols(self, syms: Iterable[str]) -> None:
        for s in syms:
            self.add_symbol(s)

    def ticker(self, sym: str) -> Optional[OptionTickerSnapshot]:
        return self.cache.get_ticker(sym.upper())

    def orderbook(self, sym: str) -> Optional[OptionOrderbookSnapshot]:
        return self.cache.get_ob(sym.upper())

    # ---------- internal ----------
    def _start(self) -> None:
        if self._thr and self._thr.is_alive():
            return
        self._ws = websocket.WebSocketApp(
            self.url,
            on_open   = lambda *_: log.info("Deribit WS opened"),
            on_close  = lambda *_: log.info("Deribit WS closed"),
            on_error  = lambda *_: log.error("Deribit WS error"),
            on_message= self._on_msg,
        )
        self._thr = threading.Thread(target=self._ws.run_forever,
                                     name="DeribitWS", daemon=True)
        self._thr.start()

        threading.Thread(target=self._subs_loop,
                         name="DeribitWS-subs", daemon=True).start()

    def _next_id(self) -> int:
        with self._id_lock:
            self._id += 1
            return self._id

    def _ws_send(self, obj: Dict[str, Any]) -> None:
        if self._ws and self._ws.sock and self._ws.sock.connected:
            try:
                self._ws.send(json.dumps(obj))
            except Exception:
                pass

    # ---------- subscribe loop ----------
    def _subs_loop(self) -> None:
        while not self._stop.is_set():
            try:
                sym = self._pending.get(timeout=0.3)
            except queue.Empty:
                sym = None
            if sym and sym not in self._subs:
                self._subscribe(sym)
            time.sleep(0.05)

    def _subscribe(self, sym: str) -> None:
        # REST-lookup → instrument-info
        try:
            inst = _SessionPool.get().rpc("public/get_instrument",
                                          {"instrument_name": sym},
                                          auth=False)
            self.cache.up_inst(sym, inst)
        except Exception:
            pass

        channels = [f"ticker.{sym}.raw", f"book.{sym}.{self.depth}ms"]
        self._ws_send({"jsonrpc": "2.0", "id": self._next_id(),
                       "method": "public/subscribe",
                       "params": {"channels": channels}})
        self._subs.add(sym)
        log.info("WS subscribed %s", sym)

    # ---------- message dispatcher ----------
    def _on_msg(self, _ws, message: str) -> None:
        try:
            j = json.loads(message)
        except ValueError:
            return
        if j.get("method") != "subscription":
            return
        p = j.get("params", {})
        ch = p.get("channel", "")
        d  = p.get("data", {})
        if ch.startswith("ticker."):
            self._tick(ch, d)
        elif ch.startswith("book."):
            self._book(ch, d)

    def _tick(self, ch: str, d: Dict[str, Any]) -> None:
        sym = ch.split(".", 2)[1]
        snap = {
            "bidPrice": d.get("best_bid_price"),
            "bidSize":  d.get("best_bid_amount"),
            "askPrice": d.get("best_ask_price"),
            "askSize":  d.get("best_ask_amount"),
            "markPrice": d.get("mark_price"),
            "lastPrice": d.get("last_price"),
            "delta": d.get("delta"), "gamma": d.get("gamma"),
            "vega": d.get("vega"),   "theta": d.get("theta"),
            "underlyingPrice": d.get("underlying_price"),
        }
        inst = self.cache._inst.get(sym)
        if inst and inst.get("expiration_timestamp"):
            exp_ms = int(inst["expiration_timestamp"])
            snap["deliveryTime"] = exp_ms
            now_ms = int(time.time()*1000)
            snap["hours_to_exp"] = round((exp_ms - now_ms)/3_600_000, 4)
        ts = int(d.get("timestamp", time.time()*1000))
        self.cache.up_tick(sym, snap, ts)

    def _book(self, ch: str, d: Dict[str, Any]) -> None:
        sym = ch.split(".", 2)[1]
        bids, asks = d.get("bids", []), d.get("asks", [])
        ts = int(d.get("timestamp", time.time()*1000))
        self.cache.up_ob(sym, bids, asks, ts)



# ----------------------------------------------------------------------
#             5.  HELPER FUNCTIONS  (spec cache, price fmt)
# ----------------------------------------------------------------------
def _get_spec(symbol: str, rest: _DeribitREST) -> Dict[str, Any]:
    if symbol in _SPEC_CACHE:
        return _SPEC_CACHE[symbol]
    info = rest.rpc("public/get_instrument",
                    {"instrument_name": symbol}, auth=False)
    _SPEC_CACHE[symbol] = info
    return info


def _fmt_price(symbol: str, price: float | Decimal,
               rest: _DeribitREST) -> str:
    spec = _get_spec(symbol, rest)
    tick = Decimal(str(spec["tick_size"]))
    prec = abs(tick.as_tuple().exponent)
    dec  = Decimal(str(price))
    steps = (dec / tick).quantize(Decimal(0), ROUND_DOWN)
    return f"{(steps*tick):.{prec}f}"


def _num(v: Any) -> Optional[Union[int, float]]:
    if v in (None, "", "null"):
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        return float(v)
    except Exception:
        return None


# ----------------------------------------------------------------------
#                        6.  TRADING API
# ----------------------------------------------------------------------
class TradingAPI:
    """REST-методы Deribit с сигнатурами как у Bybit TradingAPI."""

    # ---------- positions ----------
    @staticmethod
    def get_open_positions(*,
                           account_idx: int = 1,
                           underlying: Optional[str] = None
                           ) -> List[Dict[str, Any]]:
        rest = _SessionPool.get(account_idx)
        cur = (underlying or "BTC").upper()
        pos = rest.rpc("private/get_positions",
                       {"currency": cur, "kind": "option"})
        out = [{k: _num(v) for k, v in p.items()} for p in pos]
        if underlying:
            prefix = f"{cur}-"
            out = [p for p in out if p.get("instrument_name", "").startswith(prefix)]
        return out

    # ---------- balance ----------
    @staticmethod
    def get_wallet_balance(*, account_idx: int = 1,
                           currency: str = "BTC") -> Dict[str, Any]:
        rest = _SessionPool.get(account_idx)
        return rest.rpc("private/get_account_summary",
                        {"currency": currency.upper(), "extended": True})

    @staticmethod
    def get_total_equity(*, account_idx: int = 1,
                         currency: str = "BTC") -> float:
        bal = TradingAPI.get_wallet_balance(account_idx=account_idx,
                                            currency=currency)
        return float(bal["equity"])

    # ---------- kline ----------
    @staticmethod
    def get_kline(symbol: str, *,
                  account_idx: int = 1,
                  interval: str = "1",
                  bars: int = 500):
        sec = int(interval) * 60
        rest = _SessionPool.get(account_idx)
        end, start = int(time.time()), int(time.time()) - sec*bars
        return rest.rpc("public/get_tradingview_chart_data",
                        {"instrument_name": symbol,
                         "start_timestamp": start*1000,
                         "end_timestamp": end*1000,
                         "resolution": sec},
                        auth=False)

    # ---------- orders ----------
    @staticmethod
    def place_market_order(*,
                           account_idx: int = 1,
                           symbol: str,
                           side: str,
                           qty: float,
                           order_link_id: Optional[str] = None,
                           **__) -> Dict[str, Any]:
        rest = _SessionPool.get(account_idx)
        mtd = "private/buy" if side.lower() == "buy" else "private/sell"
        prm = {"instrument_name": symbol, "amount": qty, "type": "market"}
        if order_link_id:
            prm["label"] = order_link_id
        return rest.rpc(mtd, prm)

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
        rest = _SessionPool.get(account_idx)
        mtd = "private/buy" if side.lower() == "buy" else "private/sell"
        prm = {"instrument_name": symbol,
               "amount": qty,
               "type": "limit",
               "price": _fmt_price(symbol, price, rest),
               "time_in_force": time_in_force.lower()}
        if order_link_id:
            prm["label"] = order_link_id
        return rest.rpc(mtd, prm)

    @staticmethod
    def cancel_order(*,
                     account_idx: int = 1,
                     symbol: str,
                     order_id: Optional[str] = None,
                     order_link_id: Optional[str] = None
                     ) -> Dict[str, Any]:
        rest = _SessionPool.get(account_idx)
        if order_id:
            return rest.rpc("private/cancel", {"order_id": order_id})
        if order_link_id:
            return rest.rpc("private/cancel_by_label", {"label": order_link_id})
        raise ValueError("order_id or order_link_id required")

    @staticmethod
    def cancel_all_orders(*,
                          account_idx: int = 1,
                          underlying: Optional[str] = None
                          ) -> Dict[str, Any]:
        rest = _SessionPool.get(account_idx)
        cur = (underlying or "BTC").upper()
        return rest.rpc("private/cancel_all_by_currency",
                        {"currency": cur, "kind": "option"})

    # ---------- chain snapshot ----------
    @staticmethod
    def get_option_chain_snapshot(currency: str = "BTC",
                                  *,
                                  account_idx: int = 1,
                                  with_greeks: bool = True):
        return ChainUtils.get_chain_full(currency,
                                         account_idx=account_idx,
                                         with_greeks=with_greeks)

    # ---------- misc ----------
    @staticmethod
    def get_server_time(*, account_idx: int = 1) -> int:
        rest = _SessionPool.get(account_idx)
        res = rest.rpc("public/get_time", auth=False)
        if isinstance(res, (int, float)):
            return int(float(res)*1000)
        if isinstance(res, dict):
            if "time_now" in res:
                return int(float(res["time_now"])*1000)
            if "time" in res:
                return res["time"] if res["time"] > 1e11 else res["time"]*1000
        raise RuntimeError(f"Unexpected get_time payload: {res!r}")

    compose_symbol = staticmethod(lambda base, exp, strike, t:
                                  f"{base.upper()}-{exp.day:02d}{exp.strftime('%b').upper()}{exp.year%100:02d}-{strike}-{t.upper()}")


# ----------------------------------------------------------------------
#               7.  CHAIN UTILITIES (full / snapshot)
# ----------------------------------------------------------------------
def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "null"):
            return None
        return float(v)
    except Exception:
        return None

class ChainUtils:
    """Option-chain со 100 % совпадением ключей с Buybit-версией."""

    # -------- helpers --------
    @staticmethod
    def _norm_tick(t: Dict[str, Any]) -> Dict[str, Any]:
        def _pick(*keys):  # noqa: ANN001
            for k in keys:
                v = t.get(k)
                if v not in (None, "", "0"*0):
                    return v
            return None
        raw = {
            "bidPrice": _pick("bid_price"),
            "bidSize":  _pick("bid_amount"),
            "askPrice": _pick("ask_price"),
            "askSize":  _pick("ask_amount"),
            "markPrice": _pick("mark_price"),
            "lastPrice": _pick("last_price"),
            "underlyingPrice": _pick("underlying_price"),
            "delta": _pick("delta"),
            "gamma": _pick("gamma"),
            "vega":  _pick("vega"),
            "theta": _pick("theta"),
            "iv":    _pick("iv"),
        }
        return {k: _to_float(v) for k, v in raw.items()}

    # -------- full chain --------
    @staticmethod
    def get_chain_full(currency: str = "BTC",
                       days: int = 2,
                       *,
                       account_idx: int = 1,
                       with_greeks: bool = True):
        rest = _SessionPool.get(account_idx)
        base = currency.upper()
        inst = rest.rpc("public/get_instruments",
                        {"currency": base, "kind": "option", "expired": False},
                        auth=False)
        if not inst:
            inst = rest.rpc("public/get_instruments",
                            {"currency": f"{base}_USDC",
                             "kind": "option", "expired": False},
                            auth=False)
        if not inst:
            return []

        settle = inst[0]["settlement_currency"]
        summ = rest.rpc("public/get_book_summary_by_currency",
                        {"currency": settle, "kind": "option"}, auth=False)
        smap = {s["instrument_name"]: s for s in summ}

        res: List[Dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        for i in inst:
            sym = i["instrument_name"]
            tck = ChainUtils._norm_tick(smap.get(sym, {}))
            exp_ms = i["expiration_timestamp"]
            exp_dt = datetime.fromtimestamp(exp_ms/1000, tz=timezone.utc)
            hold   = max((exp_dt - now).total_seconds()/86400, 0.0)

            strike = float(i["strike"])
            is_put = str(i.get("option_type","")).lower().startswith("p")
            ul_px  = tck["underlyingPrice"]

            intrinsic = (max(strike - ul_px, 0.0) if is_put else
                         max(ul_px - strike, 0.0)) if ul_px is not None else 0.0
            ask_px = tck["askPrice"] or 0.0
            extr   = max(ask_px - intrinsic, 0.0)
            extr_f = extr / ask_px if ask_px else None

            snap = {
                "symbol": sym,
                "status": i.get("instrument_state","active"),
                "baseCoin": base,
                "settleCoin": settle,
                "displayName": sym,
                "optionsType": "Put" if is_put else "Call",
                "strike": strike,
                **tck,
                "deliveryTime": exp_ms,
                "launchTime": i.get("creation_timestamp"),
                "holdDays": hold,
                "intrinsicNow": intrinsic,
                "extrinsicNow": extr,
                "extrinsicFrac": extr_f,
            }
            if not with_greeks:
                for g in ("delta","gamma","vega","theta","iv"):
                    snap.pop(g, None)
            res.append(snap)

        res.sort(key=lambda d: (d["deliveryTime"], d["strike"]))
        return res

    # -------- single snapshot --------
    @staticmethod
    def get_snapshot(symbol: str, *, account_idx: int = 1,
                     with_greeks: bool = True):
        rest = _SessionPool.get(account_idx)
        try:
            ins = rest.rpc("public/get_instrument",
                           {"instrument_name": symbol}, auth=False)
        except Exception:
            return None
        try:
            tck_raw = rest.rpc("public/ticker",
                               {"instrument_name": symbol}, auth=False)
        except Exception:
            tck_raw = {}
        tck = ChainUtils._norm_tick(tck_raw)
        if not with_greeks:
            for g in ("delta","gamma","vega","theta","iv"):
                tck.pop(g, None)
        return {**ins, **tck}


# ----------------------------------------------------------------------
#                 8.  PRICE ESTIMATOR
# ----------------------------------------------------------------------
class PriceEstimator:
    @staticmethod
    def from_local(symbol: str, qty, *, buy=True, vwap=True):
        ob = _WSFeed.get(testnet=False, depth=25).orderbook(symbol)
        if not ob:
            return None
        price, _ = (ob.ask_price_for_qty if buy else ob.bid_price_for_qty)(qty,
                                                                          vwap=vwap)
        return price

    @staticmethod
    def from_rest(symbol: str, qty, *, buy=True, vwap=True,
                  depth=200, account_idx=1):
        rest = _SessionPool.get(account_idx)
        side = "asks" if buy else "bids"
        ob = rest.rpc("public/get_order_book",
                      {"instrument_name": symbol, "depth": depth},
                      auth=False)
        lvls = [(Decimal(str(px)), Decimal(str(sz))) for px, sz in ob.get(side, [])]
        qty_d = Decimal(str(qty))
        left, cost = qty_d, Decimal("0")
        px = Decimal("0")
        for px, sz in lvls:
            if left <= 0:
                break
            take = sz if sz <= left else left
            cost += take * px
            left -= take
        if left > 0:
            return None
        return cost/qty_d if vwap else px  # type: ignore

    @staticmethod
    def smart(symbol: str, qty, *, buy=True, vwap=True,
              account_idx=1, depth_rest=200):
        p = PriceEstimator.from_local(symbol, qty, buy=buy, vwap=vwap)
        if p is not None:
            return p
        return PriceEstimator.from_rest(symbol, qty, buy=buy, vwap=vwap,
                                        depth=depth_rest, account_idx=account_idx)


# -----------------------------------------------------------------------------
#                     7.  PUBLIC  FACADE  (DeribitOptionHub)
# -----------------------------------------------------------------------------
class DeribitOptionHub:
    """
    Фасад – импортируйте именно его:
        from exchanges.deribit_option_hub import DeribitOptionHub as DB
    """

    _ready = False

    # -------- init ----------
    @staticmethod
    def initialise(*, testnet: bool = False, ws_depth_levels: int = 25) -> None:
        if DeribitOptionHub._ready:
            return
        _SessionPool.build_all(testnet=testnet)
        _WSFeed.get(testnet=testnet, depth=ws_depth_levels)
        DeribitOptionHub._ready = True
        log.info("DeribitOptionHub initialised (testnet=%s)", testnet)

    # -------- subscribe -----
    @staticmethod
    def subscribe(symbols: str | Iterable[str]) -> None:
        feed = _WSFeed.get(testnet=False, depth=25)
        if isinstance(symbols, str):
            feed.add_symbol(symbols)
        else:
            feed.add_symbols(symbols)

    # -------- data access ---
    @staticmethod
    def get_ticker(symbol: str) -> Optional[OptionTickerSnapshot]:
        return _WSFeed.get(testnet=False, depth=25).ticker(symbol)

    @staticmethod
    def get_orderbook(symbol: str) -> OptionOrderbookSnapshot:
        """
        Сначала пытаемся вернуть данные из WS-кэша.
        Если там нет (None), делаем REST-запрос public/get_order_book
        и строим OptionOrderbookSnapshot вручную.
        """
        # 1) WebSocket-кэш
        ob = _WSFeed.get(testnet=False, depth=25).orderbook(symbol)
        if ob is not None:
            return ob

        # 2) REST-фоллбек
        from decimal import Decimal
        rest = _SessionPool.get(1)
        data = rest.rpc(
            "public/get_order_book",
            {"instrument_name": symbol, "depth": 200},
            auth=False
        )
        # собираем уровни
        bids = [(Decimal(str(px)), Decimal(str(sz))) for px, sz in data.get("bids", [])]
        asks = [(Decimal(str(px)), Decimal(str(sz))) for px, sz in data.get("asks", [])]
        ts = int(time.time() * 1000)
        return OptionOrderbookSnapshot(bids, asks, ts)
    
    @staticmethod
    def stop() -> None:
        if _WSFeed._INSTANCE:
            _WSFeed._INSTANCE.stop()


DeribitOptionHub.Trading   = TradingAPI
DeribitOptionHub.Chain     = ChainUtils
DeribitOptionHub.Estimator = PriceEstimator