import threading
import time
from datetime import datetime, timezone
import traceback
from zoneinfo import ZoneInfo
from typing import Dict, Optional

import eth_account
from eth_account.signers.local import LocalAccount
from decouple import config
from retry import retry

from hyperliquid.utils import constants
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils.signing import get_timestamp_ms, sign_l1_action, order_request_to_order_wire
from hyperliquid.utils.constants import MAINNET_API_URL
import shared_vars as sv

# --------------------------------------------------------------------------- #
#  Глобальные константы
# --------------------------------------------------------------------------- #
MAXIMUM_NUMBER_OF_API_CALL_TRIES = 3   # кол‑во повторов при ошибках сети/API

# --------------------------------------------------------------------------- #
#  Вспомогательная фабрика Exchange/Info
# --------------------------------------------------------------------------- #
def _make_exchange(
    master_addr: str,
    api_secret: str,
    vault_addr: str | None,
):
    """
    master_addr — адрес кошелька‑подписанта  (HLAPI_1)
    api_secret  — приватный ключ этого кошелька (HLSECRET_x)
    vault_addr  — адрес суб‑аккаунта.  Если None → работаем на мастере.
    """
    account: LocalAccount = eth_account.Account.from_key(api_secret)
    info = Info(constants.MAINNET_API_URL, True)

    # передаём vaultAddress ТОЛЬКО когда он отличается от master_addr
    kwargs = {"account_address": master_addr}
    if vault_addr and vault_addr.lower() != master_addr.lower():
        kwargs["vault_address"] = vault_addr

    exchange = Exchange(
        account,
        constants.MAINNET_API_URL,
        **kwargs
    )
    return info, exchange



# --------------------------------------------------------------------------- #
#  Класс‑обёртка: работа с несколькими аккаунтами
# --------------------------------------------------------------------------- #
class HL:
    """
    account_idx = 1 → главный счёт      (HLAPI_1 / HLSECRET_1)
    account_idx = 2 → суб‑аккаунт №1    (HLAPI_2 / HLSECRET_2  или HLSECRET_1)
    account_idx = 3 → суб‑аккаунт №2    (HLAPI_3 / HLSECRET_3  или HLSECRET_1)
    …
    """

    _clients: Dict[int, Dict[str, object]] = {}   # кэш Info/Exchange по idx
    _lock = threading.Lock()

    # -------- внутренний помощник -------------------------------------------
    @staticmethod
    def _get_client(account_idx: int = 1) -> Dict[str, object]:
        with HL._lock:
            if account_idx in HL._clients:
                return HL._clients[account_idx]

            master_addr = config("HLAPI_1")
            api_secret  = config(f"HLSECRET_{account_idx}",
                                 default=config("HLSECRET_1"))
            vault_addr  = None if account_idx == 1 else config(f"HLAPI_{account_idx}")

            info, exchange = _make_exchange(master_addr, api_secret, vault_addr)

            HL._clients[account_idx] = {
                "info":       info,
                "exchange":   exchange,
                "vault_addr": vault_addr or master_addr,
            }
            return HL._clients[account_idx]

    # --------------------------------------------------------------------- #
    #                              BALANCE
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def get_balance(account_idx: int = 1) -> float:
        cl = HL._get_client(account_idx)
        state = cl["info"].user_state(cl["vault_addr"])
        return float(state["crossMarginSummary"]["accountValue"])

    # --------------------------------------------------------------------- #
    #                           POSITIONS / FILLS
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def get_position(coin: str = None, account_idx: int = 1) -> Optional[dict]:
        cl = HL._get_client(account_idx)
        name = None
        if coin is not None:
            name = coin[:-4]
        
        state = cl["info"].user_state(cl["vault_addr"])
        for pos in state.get("assetPositions", []):
            if name is None:
                sz = float(pos["position"]["szi"])
                return {
                    "positionValue": float(pos["position"]["positionValue"]),
                    "unrealizedPnl": float(pos["position"]["unrealizedPnl"]),
                    "size":          sz,
                    "entryPx":       float(pos["position"]["entryPx"]),
                    "side":          1 if sz > 0 else 2,
                }
            elif pos["position"]["coin"] == name:
                sz = float(pos["position"]["szi"])
                return {
                    "positionValue": float(pos["position"]["positionValue"]),
                    "unrealizedPnl": float(pos["position"]["unrealizedPnl"]),
                    "size":          sz,
                    "entryPx":       float(pos["position"]["entryPx"]),
                    "side":          1 if sz > 0 else 2,
                }
        return None

    # время открытия позиции по последнему fill‑у
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def get_open_time(coin: str, account_idx: int = 1) -> Optional[datetime]:
        cl = HL._get_client(account_idx)
        fills = cl["info"].user_fills_by_time(cl["vault_addr"], 0, None)
        if not fills:
            return None
        ts = fills[-1]["time"] / 1000          # ms → s
        local_dt = datetime.fromtimestamp(ts, tz=ZoneInfo("Europe/London"))
        return str(local_dt.astimezone(timezone.utc))

    # --------------------------------------------------------------------- #
    #                           MARKET DATA & META
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def instrument_info(symbol: str, account_idx: int = 1) -> dict:
        cl = HL._get_client(account_idx)
        name = symbol[:-4]
        for cont in cl["info"].meta()["universe"]:
            if cont["name"] == name:
                return cont
        raise ValueError(f"{symbol}: contract not found.")

    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def is_contract_exist(symbol: str, account_idx: int = 1) -> tuple[bool, list[str]]:
        cl = HL._get_client(account_idx)
        tickers = [f'{c["name"]}USDT' for c in cl["info"].meta()["universe"]]
        return symbol in tickers, tickers

    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def get_last_price(symbol: str, account_idx: int = 1) -> float:
        cl = HL._get_client(account_idx)
        return float(cl["info"].all_mids()[symbol[:-4]])

    # --------------------------------------------------------------------- #
    #                            LEVERAGE
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def set_leverage(symbol: str, leverage: int, account_idx: int = 1):
        cl = HL._get_client(account_idx)
        cl["exchange"].update_leverage(leverage, symbol[:-4])

    # --------------------------------------------------------------------- #
    #                        OPEN / CLOSE MARKET ORDER
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=3)
    def open_market_order(
        coin: str,
        sd: str,
        amount_usdt: int,
        reduce_only: bool,
        amount_coins: float = 0.0,
        account_idx: int = 1,
    ):
        try:
            print(coin, sd, amount_usdt, reduce_only, amount_coins, account_idx)
            cl   = HL._get_client(account_idx)
            name = coin[:-4]

            info = HL.instrument_info(coin, account_idx)
            dec  = int(info["szDecimals"])
            size = round(amount_coins or amount_usdt / HL.get_last_price(coin, account_idx), dec)

            lev = min(20, int(info["maxLeverage"]))
            HL.set_leverage(coin, lev, account_idx)

            is_buy = sd == "Buy"
            if reduce_only:
                order = cl["exchange"].market_close(name, size, None, 0.01)
            else:
                order = cl["exchange"].market_open(name, is_buy, size, None, 0.01)

            
            if order is None:
                raise Exception("Received None order response, will retry.")
            
            print(order)
            if order["status"] == "ok":
                for st in order["response"]["data"]["statuses"]:
                    if "filled" in st:
                        f = st["filled"]
                        return f["oid"], f["avgPx"]
            return 0, HL.get_last_price(coin, account_idx)
        except Exception as e:
            print(e)
            print(traceback.format_exc())

    # --------------------------------------------------------------------- #
    #                      STOP‑LOSS  (reduce‑only true)
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def open_SL(
        coin: str,
        sd: str,
        amount_lot: float,
        open_price: float,
        sl_perc: float,
        account_idx: int = 1,
    ):
        cl   = HL._get_client(account_idx)
        name = coin[:-4]

        if sd == "Buy":
            px = open_price * (1 - sl_perc)
            trig = px * (1 + 0.001)
            is_buy = False
        else:
            px = open_price * (1 + sl_perc)
            trig = px * (1 - 0.001)
            is_buy = True

        order_type = {
            "trigger": {
                "triggerPx": round(float(f"{trig:.5g}"), 6),
                "isMarket":  True,
                "tpsl":      "sl",
            }
        }

        res = cl["exchange"].order(name, is_buy, amount_lot,
                                   round(float(f"{px:.5g}"), 6),
                                   order_type,
                                   reduce_only=True)
        print(res)
        return res

    # --------------------------------------------------------------------- #
    #                      TAKE‑PROFIT (reduce‑only true)
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def open_TP(
        coin: str,
        sd: str,
        amount_lot: float,
        open_price: float,
        tp_perc: float,
        account_idx: int = 1,
    ):
        cl   = HL._get_client(account_idx)
        name = coin[:-4]

        if sd == "Buy":
            px   = open_price * (1 + tp_perc)
            trig = px * (1 - 0.001)
            is_buy = False
        else:
            px   = open_price * (1 - tp_perc)
            trig = px * (1 + 0.001)
            is_buy = True

        order_type = {
            "trigger": {
                "triggerPx": round(float(f"{trig:.5g}"), 6),
                "isMarket":  True,
                "tpsl":      "tp",
            }
        }

        res = cl["exchange"].order(name, is_buy, amount_lot,
                                   round(float(f"{px:.5g}"), 6),
                                   order_type,
                                   reduce_only=True)
        print(res)
        return res

    # --------------------------------------------------------------------- #
    #                            OPEN ORDERS
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def get_open_orders(account_idx: int = 1) -> list[dict]:
        cl = HL._get_client(account_idx)
        return cl["info"].open_orders(cl["vault_addr"])

    # --------------------------------------------------------------------- #
    #                       CANCEL ALL ORDERS by COIN
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def cancel_all_orders(coin: str, account_idx: int = 1):
        cl   = HL._get_client(account_idx)
        name = coin[:-4]
        for od in cl["info"].open_orders(cl["vault_addr"]):
            if od["coin"] == name:
                print(f"Cancel {od}")
                cl["exchange"].cancel(name, od["oid"])

    # --------------------------------------------------------------------- #
    #                    LIST OF ALL OPEN POSITIONS
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def is_any_position_exists(account_idx: int = 1):
        cl = HL._get_client(account_idx)
        out = []
        state = cl["info"].user_state(cl["vault_addr"])
        for p in state.get("assetPositions", []):
            sd  = "Buy" if float(p["position"]["szi"]) > 0 else "Sell"
            amt = abs(float(p["position"]["szi"]))
            out.append([f'{p["position"]["coin"]}USDT', sd, amt])
        return out


    # --------------------------------------------------------------------- #
    #                      LIMIT (POST-ONLY) + авто-догон (фикс)
    # --------------------------------------------------------------------- #
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def open_limit_post_only(
        coin: str,
        sd: str,
        amount_usdt: int,
        reduce_only: bool,
        amount_coins: float = 0.0,
        account_idx: int = 1,
        offset_bps: float = 2.0,
        reprice_interval_sec: int = 30,
        max_wait_cycles: int = 2,
        reprice_threshold_bps: float = 2.0,
        min_offset_ticks: int = 1,   # ← гарант от пересечения
    ) -> Optional[dict]:
        """
        Лимитный ордер строго post-only (TIF="Alo") как можно ближе к рынку с авто-перестановкой.
        Правки:
          • целимся от bestBid/bestAsk, а не от mid;
          • направленное округление к сетке тика;
          • на ALO-отклонение — немедленный пересчёт по текущему l2Book.

        Возвращает:
            dict | None — открытую позицию по инструменту (как в HL.get_position),
            либо None, если позиции нет (например, reduce_only и нет позиции).
        """
        trace_id = f"limit_post_only:{coin}:{sd}:{int(time.time()*1000)}"
        sv.logger.info(
            "[%s] start: coin=%s sd=%s amount_usdt=%s amount_coins=%s reduce_only=%s "
            "acct=%s offset_bps=%.4f reprice_interval=%ss max_wait_cycles=%s reprice_thr_bps=%.4f min_offset_ticks=%s",
            trace_id, coin, sd, amount_usdt, amount_coins, reduce_only,
            account_idx, offset_bps, reprice_interval_sec, max_wait_cycles, reprice_threshold_bps, min_offset_ticks,
        )

        cl   = HL._get_client(account_idx)
        name = coin[:-4]
        is_buy = (sd == "Buy")

        # ---------- Метаданные и разрядности ----------
        try:
            meta = HL.instrument_info(coin, account_idx)
            sz_dec = int(meta["szDecimals"])
            px_dec = max(0, 6 - sz_dec)  # цена для перпов HL (см. SDK)
            sv.logger.debug(
                "[%s] meta: name=%s szDecimals=%d -> size_decimals=%d, price_decimals=%d, maxLev=%s",
                trace_id, name, sz_dec, sz_dec, px_dec, meta.get("maxLeverage"),
            )
        except Exception:
            sv.logger.exception("[%s] failed to fetch meta/instrument_info", trace_id)
            raise

        # ---------- Вспомогательные ----------
        def _round_to_decimals_floor(x: float, dec: int) -> float:
            if dec <= 0:
                return float(int(x))
            s = 10.0 ** dec
            return float(int(x * s)) / s

        def _round_to_decimals_ceil(x: float, dec: int) -> float:
            if dec <= 0:
                return float(int(x) if x == int(x) else int(x) + 1)
            s = 10.0 ** dec
            v = x * s
            iv = int(v)
            if v == iv:
                return float(iv) / s
            return float(iv + 1) / s

        def _book_best_and_tick() -> tuple[float, float, float]:
            """bestBid, bestAsk, tick. Определяем стороны относительно mid и оцениваем тик из книги."""
            mid = float(cl["info"].all_mids()[name])
            book = cl["info"].l2_snapshot(name)
            levels = book.get("levels", [])
            if not levels or len(levels) < 2:
                # запасной путь — только mid, тик по dec
                tick = 10.0 ** (-px_dec)
                sv.logger.warning("[%s] l2_snapshot empty; fallback mid/tick: mid=%.12f tick=%.*f",
                                  trace_id, mid, px_dec, tick)
                return mid - tick, mid + tick, tick

            side0 = [float(x["px"]) for x in levels[0] if "px" in x]
            side1 = [float(x["px"]) for x in levels[1] if "px" in x]
            if not side0 or not side1:
                tick = 10.0 ** (-px_dec)
                sv.logger.warning("[%s] l2 sides empty; fallback mid/tick: mid=%.12f tick=%.*f",
                                  trace_id, mid, px_dec, tick)
                return mid - tick, mid + tick, tick

            s0_min, s0_max = min(side0), max(side0)
            s1_min, s1_max = min(side1), max(side1)

            # Определим, где бид, где аск, относительно mid:
            # - у бидов max <= mid
            # - у асков min >= mid
            # если обе стороны «по обе стороны» от mid (бывает при шуме), решаем по среднему значению
            def classify(prices):
                mn, mx = min(prices), max(prices)
                if mx <= mid: return "bid"
                if mn >= mid: return "ask"
                # неоднозначно — решим по расстоянию до mid
                avg = sum(prices) / len(prices)
                return "bid" if avg < mid else "ask"

            c0 = classify(side0)
            c1 = classify(side1)
            if c0 == c1:
                # как страховку: возьмём меньшие цены как бид, большие — как аск
                bid_side = side0 if (s0_max <= s1_min) else side1
                ask_side = side1 if bid_side is side0 else side0
            else:
                bid_side = side0 if c0 == "bid" else side1
                ask_side = side1 if c1 == "ask" else side0

            best_bid = max(bid_side)
            best_ask = min(ask_side)

            # Оценим тик: минимальная положительная разница внутри каждой стороны
            def min_diff(prs):
                prs = sorted(set(prs))
                diffs = [prs[i+1]-prs[i] for i in range(len(prs)-1)]
                diffs = [d for d in diffs if d > 0]
                return min(diffs) if diffs else 0.0

            tick_candidates = [min_diff(bid_side), min_diff(ask_side), 10.0 ** (-px_dec)]
            tick = min([t for t in tick_candidates if t > 0]) if any(t > 0 for t in tick_candidates) else 10.0 ** (-px_dec)

            sv.logger.debug(
                "[%s] book: bestBid=%.12f bestAsk=%.12f tick=%.*f mid=%.12f",
                trace_id, best_bid, best_ask, px_dec, tick, mid,
            )
            return best_bid, best_ask, tick

        def _target_px_from_book(best_bid: float, best_ask: float, tick: float, want_buy: bool, mkt_px: float) -> float:
            # целевой отступ в б.п. от "рынка"
            base = mkt_px
            off = base * (offset_bps / 1e4)
            raw = (base - off) if want_buy else (base + off)
            # приближаем к лучшей стороне с безопасным отступом в тиках
            if want_buy:
                # не выше bestAsk - min_offset_ticks*tick
                lim = best_ask - max(min_offset_ticks, 1) * tick
                raw = min(raw, lim)
                # и не ниже bestBid (чтобы «стоять в книге», а не глубоко)
                raw = max(raw, best_bid)
                # округляем ВНИЗ
                px = _round_to_decimals_floor(raw, px_dec)
                # на всякий случай строго < bestAsk
                if px >= best_ask:
                    px = _round_to_decimals_floor(best_ask - tick, px_dec)
            else:
                # не ниже bestBid + min_offset_ticks*tick
                lim = best_bid + max(min_offset_ticks, 1) * tick
                raw = max(raw, lim)
                # и не выше bestAsk (старайся быть у топа книги)
                raw = min(raw, best_ask)
                # округляем ВВЕРХ
                px = _round_to_decimals_ceil(raw, px_dec)
                # на всякий случай строго > bestBid
                if px <= best_bid:
                    px = _round_to_decimals_ceil(best_bid + tick, px_dec)
            return px

        # ---------- Размер позиции ----------
        try:
            last = HL.get_last_price(coin, account_idx)
            size = amount_coins or (amount_usdt / last)
            size = round(float(size), sz_dec)
            if size <= 0:
                sv.logger.error(
                    "[%s] invalid size after rounding: size=%s sz_dec=%s (last=%.12f)",
                    trace_id, size, sz_dec, last,
                )
                raise ValueError("Order size is zero after rounding. Check amount_usdt/amount_coins.")
            sv.logger.info(
                "[%s] size: last=%.12f amount_usdt=%s amount_coins=%s -> size=%.*f",
                trace_id, last, amount_usdt, amount_coins, sz_dec, size,
            )
        except Exception:
            sv.logger.exception("[%s] failed to compute order size / last price", trace_id)
            raise

        # ---------- Плечо ----------
        try:
            lev = min(20, int(meta["maxLeverage"]))
            HL.set_leverage(coin, lev, account_idx)
            sv.logger.info("[%s] leverage set: lev=%s", trace_id, lev)
        except Exception:
            sv.logger.exception("[%s] failed to set leverage", trace_id)
            raise

        # ---------- Первичная постановка ALO (по стакану) ----------
        try:
            best_bid, best_ask, tick = _book_best_and_tick()
            mkt_px = float(cl["info"].all_mids()[name])
            px     = _target_px_from_book(best_bid, best_ask, tick, is_buy, mkt_px)
            sv.logger.info(
                "[%s] place initial ALO: side=%s size=%.*f px=%.*f (bb=%.12f ba=%.12f tick=%.*f) reduce_only=%s",
                trace_id, sd, sz_dec, size, px_dec, px, best_bid, best_ask, px_dec, tick, reduce_only,
            )
            order = cl["exchange"].order(
                name,
                is_buy,
                size,
                px,
                {"limit": {"tif": "Alo"}},
                reduce_only=reduce_only,
            )
            sv.logger.debug("[%s] initial order response: %s", trace_id, order)
            if order is None:
                raise RuntimeError("Exchange.order returned None")
        except Exception:
            sv.logger.exception("[%s] failed to place initial ALO order", trace_id)
            raise

        # ---------- Разбор ответа ----------
        oid = None
        try:
            status0 = order["response"]["data"]["statuses"][0]
        except Exception:
            status0 = {}
            sv.logger.warning("[%s] unexpected order response shape, no statuses[0]", trace_id)

        def _status_dump(st: dict) -> str:
            try:
                return f"keys={list(st.keys())} content={st}"
            except Exception:
                return repr(st)

        if "error" in status0:
            sv.logger.warning("[%s] initial ALO error: %s", trace_id, _status_dump(status0))

        if "filled" in status0:
            sv.logger.info("[%s] initial ALO filled immediately. Fetching position…", trace_id)
            time.sleep(0.2)
            pos = HL.get_position(coin, account_idx)
            sv.logger.info("[%s] position after immediate fill: %s", trace_id, pos)
            return pos

        if "resting" in status0:
            oid = status0["resting"]["oid"]
            sv.logger.info("[%s] initial order resting: oid=%s px=%.*f", trace_id, oid, px_dec, px)
        else:
            # ALO отклонён — немедленно перечитать книгу и поставить безопаснее относительно bestBid/Ask
            try:
                best_bid, best_ask, tick = _book_best_and_tick()
                mkt_px = float(cl["info"].all_mids()[name])
                px2 = _target_px_from_book(best_bid, best_ask, tick * 2, is_buy, mkt_px)  # + запас 2 тика
                sv.logger.info(
                    "[%s] ALO rejected; retry anchored to book: px2=%.*f (bb=%.12f ba=%.12f tick*2=%.*f)",
                    trace_id, px_dec, px2, best_bid, best_ask, px_dec, tick * 2,
                )
                order2 = cl["exchange"].order(
                    name,
                    is_buy,
                    size,
                    px2,
                    {"limit": {"tif": "Alo"}},
                    reduce_only=reduce_only,
                )
                sv.logger.debug("[%s] second order response: %s", trace_id, order2)
                if order2 and order2.get("status") == "ok":
                    st = order2["response"]["data"]["statuses"][0]
                    if "error" in st:
                        sv.logger.warning("[%s] second ALO error: %s", trace_id, _status_dump(st))
                    if "filled" in st:
                        sv.logger.info("[%s] second ALO filled. Fetching position…", trace_id)
                        time.sleep(0.2)
                        pos = HL.get_position(coin, account_idx)
                        sv.logger.info("[%s] position after second fill: %s", trace_id, pos)
                        return pos
                    if "resting" in st:
                        oid = st["resting"]["oid"]
                        px = px2
                        sv.logger.info("[%s] second ALO resting: oid=%s px=%.*f", trace_id, oid, px_dec, px)
            except Exception:
                sv.logger.exception("[%s] retry ALO (book-anchored) failed", trace_id)

            if oid is None:
                sv.logger.warning(
                    "[%s] unable to place ALO resting order; fallback to market open", trace_id
                )
                HL.open_market_order(coin, sd, amount_usdt, reduce_only, amount_coins, account_idx)
                time.sleep(0.2)
                pos = HL.get_position(coin, account_idx)
                sv.logger.info("[%s] position after market fallback: %s", trace_id, pos)
                return pos

        # ---------- Мониторинг и перестановка ----------
        last_check = time.time()
        cycles = 0
        sv.logger.info(
            "[%s] monitoring loop: reprice_interval=%ss max_cycles=%s",
            trace_id, reprice_interval_sec, max_wait_cycles,
        )

        while True:
            # Ордер всё ещё открыт?
            try:
                open_ords = HL.get_open_orders(account_idx)
            except Exception:
                sv.logger.exception("[%s] get_open_orders failed", trace_id)
                open_ords = []

            still_open = any(o.get("oid") == oid for o in open_ords)
            sv.logger.debug(
                "[%s] order state: oid=%s still_open=%s open_count=%d",
                trace_id, oid, still_open, len(open_ords),
            )
            if not still_open:
                sv.logger.info("[%s] order not found in open list; fetching position…", trace_id)
                time.sleep(0.2)
                pos = HL.get_position(coin, account_idx)
                sv.logger.info("[%s] position after disappearance from open list: %s", trace_id, pos)
                return pos

            now = time.time()
            if now - last_check >= reprice_interval_sec:
                cycles += 1
                last_check = now

                try:
                    # Всегда якоримся на АКТУАЛЬНОМ стакане
                    best_bid, best_ask, tick = _book_best_and_tick()
                    new_mkt = float(cl["info"].all_mids()[name])
                except Exception:
                    sv.logger.exception("[%s] book/mid fetch failed during monitoring", trace_id)
                    best_bid, best_ask = (None, None)
                    new_mkt = None

                sv.logger.debug(
                    "[%s] reprice tick #%d: new_mkt=%s bb=%s ba=%s",
                    trace_id, cycles,
                    f"{new_mkt:.12f}" if new_mkt is not None else "NA",
                    f"{best_bid:.12f}" if best_bid is not None else "NA",
                    f"{best_ask:.12f}" if best_ask is not None else "NA",
                )

                # Решение о перестановке: либо рынок «ушёл», либо хотим подтянуться к топу книги
                moved = False
                if new_mkt is not None:
                    thr = (reprice_threshold_bps / 1e4) * (new_mkt if new_mkt else 1.0)
                    moved = abs(new_mkt - mkt_px) >= thr
                if moved and best_bid is not None and best_ask is not None:
                    mkt_px = new_mkt
                    new_px = _target_px_from_book(best_bid, best_ask, tick, is_buy, mkt_px)
                    sv.logger.info(
                        "[%s] modify_order: oid=%s side=%s size=%.*f new_px=%.*f (bb=%.12f ba=%.12f tick=%.*f, ALO)",
                        trace_id, oid, sd, sz_dec, size, px_dec, new_px, best_bid, best_ask, px_dec, tick,
                    )
                    try:
                        res = cl["exchange"].modify_order(
                            oid,
                            name,
                            is_buy,
                            size,
                            new_px,
                            {"limit": {"tif": "Alo"}},
                            reduce_only=reduce_only,
                        )
                        sv.logger.debug("[%s] modify_order response: %s", trace_id, res)
                        if res and res.get("status") == "ok":
                            st = res["response"]["data"]["statuses"][0]
                            if "error" in st:
                                sv.logger.warning("[%s] modify_order ALO error: %s", trace_id, _status_dump(st))
                            if "filled" in st:
                                sv.logger.info("[%s] modify_order -> filled. Fetching position…", trace_id)
                                time.sleep(0.2)
                                pos = HL.get_position(coin, account_idx)
                                sv.logger.info("[%s] position after modify fill: %s", trace_id, pos)
                                return pos
                            if "resting" in st:
                                px = new_px
                                oid = st["resting"]["oid"]
                                sv.logger.info("[%s] modify_order -> resting: oid=%s px=%.*f",
                                               trace_id, oid, px_dec, px)
                    except Exception:
                        sv.logger.exception("[%s] modify_order failed", trace_id)

                # Исчерпали лимит ожидания — отменяем и берём маркетом
                if cycles >= max_wait_cycles:
                    sv.logger.warning(
                        "[%s] max_wait_cycles reached (%d). Cancel all & market fallback.",
                        trace_id, cycles,
                    )
                    try:
                        HL.cancel_all_orders(coin, account_idx)
                        sv.logger.info("[%s] cancel_all_orders done", trace_id)
                    except Exception:
                        sv.logger.exception("[%s] cancel_all_orders failed", trace_id)
                    HL.open_market_order(coin, sd, amount_usdt, reduce_only, amount_coins, account_idx)
                    time.sleep(0.2)
                    pos = HL.get_position(coin, account_idx)
                    sv.logger.info("[%s] position after market fallback: %s", trace_id, pos)
                    return pos

            time.sleep(1)

    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def place_limit_post_only(
        coin: str,
        sd: str,
        limit_px: float,
        amount_usdt: float,
        amount_coins: float = 0.0,
        reduce_only: bool = False,
        account_idx: int = 1,
    ) -> bool:
        """
        Простой постинг лимитной заявки с post-only (TIF="Alo") на указанной цене.
        Возвращает:
            True  — если заявка была успешно размещена (resting) или мгновенно исполнена (filled),
            False — если заявка не была размещена (ошибка/отклонение) или размер после округления равен 0.
        Дополнительно: при статусе 'resting' выполняется короткая проверка через open_orders,
        чтобы убедиться, что заявка действительно находится в книге.
        """
        cl   = HL._get_client(account_idx)
        name = coin[:-4]
        is_buy = (sd == "Buy")

        # --- метаданные и разрядности ---
        try:
            meta   = HL.instrument_info(coin, account_idx)
            sz_dec = int(meta["szDecimals"])
            px_dec = max(0, 6 - sz_dec)  # для перпов правило точности цены
        except Exception:
            sv.logger.exception("[place_limit_post_only:%s] failed to read instrument meta", coin)
            return False

        def _round_px(x: float) -> float:
            return float(f"{x:.{px_dec}f}") if px_dec > 0 else float(int(x))

        # --- размер ---
        try:
            last = HL.get_last_price(coin, account_idx)
            size = amount_coins or (amount_usdt / last)
            size = round(float(size), sz_dec)
            if size <= 0:
                sv.logger.warning("[place_limit_post_only:%s] size rounded to zero; abort", coin)
                return False
        except Exception:
            sv.logger.exception("[place_limit_post_only:%s] failed to compute size/last px", coin)
            return False

        # --- цена (как передана, лишь аккуратное округление под тик) ---
        try:
            px = _round_px(float(limit_px))
        except Exception:
            sv.logger.exception("[place_limit_post_only:%s] invalid limit price", coin)
            return False

        # --- отправка post-only ордера ---
        try:
            order = cl["exchange"].order(
                name,
                is_buy,
                size,
                px,
                {"limit": {"tif": "Alo"}},   # строго post-only
                reduce_only=reduce_only,
            )
        except Exception:
            sv.logger.exception("[place_limit_post_only:%s] exchange.order failed", coin)
            return False

        if not order or order.get("status") != "ok":
            sv.logger.warning("[place_limit_post_only:%s] bad order response: %s", coin, order)
            return False

        # --- разбор ответа и верификация ---
        try:
            st = order["response"]["data"]["statuses"][0]
        except Exception:
            sv.logger.warning("[place_limit_post_only:%s] unexpected response shape", coin)
            return False

        # Если сразу исполнилось — считаем успешным размещением
        if "filled" in st:
            return True

        # Если стоит в книге — проверим, что действительно отображается в open_orders
        if "resting" in st:
            oid = st["resting"].get("oid")
            if not oid:
                return False

            # короткая верификация наличия в списке открытых заявок
            for _ in range(3):
                try:
                    open_ords = HL.get_open_orders(account_idx)
                    if any(o.get("oid") == oid for o in open_ords):
                        return True
                except Exception:
                    sv.logger.exception("[place_limit_post_only:%s] get_open_orders failed", coin)
                time.sleep(0.2)

            # не нашли — считаем, что заявки нет
            sv.logger.warning("[place_limit_post_only:%s] resting OID %s not found in open_orders", coin, oid)
            return False

        # Если пришёл error/нет нужных полей — неуспех
        sv.logger.warning("[place_limit_post_only:%s] neither filled nor resting: %s", coin, st)
        return False



    @staticmethod
    def _post_position_tpsl(
        coin: str,
        is_buy_close: bool,
        limit_px: float,
        trigger_px: float,
        tpsl_kind: str,  # "tp" или "sl"
        account_idx: int = 1,
    ) -> dict:
        """
        Внутренний помощник: ставит позиционный (grouping=positionTpsl) TP/SL
        на ВЕСЬ доступный размер позиции при срабатывании (sz=0), reduce_only=True.

        Возвращает сырой ответ API Hyperliquid.
        """
        cl   = HL._get_client(account_idx)
        name = coin[:-4]

        # Собираем OrderRequest и проводку с привязкой к asset id
        order_req = {
            "coin":      name,
            "is_buy":    is_buy_close,
            "sz":        0.0,  # ключевое: "на весь размер позиции при триггере"
            "limit_px":  round(float(f"{limit_px:.5g}"), 6),
            "order_type": {
                "trigger": {
                    "triggerPx": round(float(f"{trigger_px:.5g}"), 6),
                    "isMarket":  True,
                    "tpsl":      tpsl_kind,  # "tp" | "sl"
                }
            },
            "reduce_only": True,
        }
        asset_id   = cl["info"].name_to_asset(name)
        order_wire = order_request_to_order_wire(order_req, asset_id)

        # Строим action с ГЛАВНЫМ параметром: grouping="positionTpsl"
        action = {
            "type":    "order",
            "orders":  [order_wire],
            "grouping": "positionTpsl",
        }

        # Подписываем так же, как делает SDK внутри Exchange.bulk_orders(...)
        exchange   = cl["exchange"]
        timestamp  = get_timestamp_ms()
        signature  = sign_l1_action(
            exchange.wallet,
            action,
            exchange.vault_address,
            timestamp,
            exchange.expires_after,
            exchange.base_url == MAINNET_API_URL,
        )
        # Отправляем
        return exchange._post_action(action, signature, timestamp)

    # ----------------------------- POSITION SL -----------------------------
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def open_SL_position(
        coin: str,
        sd: str,               # "Buy" (лонг) или "Sell" (шорт) исходной ПОЗИЦИИ
        open_price: float,
        sl_perc: float,        # 0.01 = 1%
        account_idx: int = 1,
    ) -> dict:
        """
        Ставит позиционный Stop-Loss (grouping=positionTpsl) на весь доступный размер позиции.
        Для лонга — is_buy_close = False, для шорта — True.
        Limit_px задаём консервативно рядом с triggerPx (исп. ваша логика округления).
        """
        if sd == "Buy":
            px   = open_price * (1 - sl_perc)
            trig = px * (1 + 0.001)
            is_buy_close = False
        else:
            px   = open_price * (1 + sl_perc)
            trig = px * (1 - 0.001)
            is_buy_close = True

        return HL._post_position_tpsl(coin, is_buy_close, px, trig, "sl", account_idx)

    # ----------------------------- POSITION TP -----------------------------
    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def open_TP_position(
        coin: str,
        sd: str,               # "Buy" (лонг) или "Sell" (шорт) исходной ПОЗИЦИИ
        open_price: float,
        tp_perc: float,        # 0.01 = 1%
        account_idx: int = 1,
    ) -> dict:
        """
        Ставит позиционный Take-Profit (grouping=positionTpsl) на весь доступный размер позиции.
        Для лонга — is_buy_close = False, для шорта — True.
        """
        if sd == "Buy":
            px   = open_price * (1 + tp_perc)
            trig = px * (1 - 0.001)
            is_buy_close = False
        else:
            px   = open_price * (1 - tp_perc)
            trig = px * (1 + 0.001)
            is_buy_close = True

        return HL._post_position_tpsl(coin, is_buy_close, px, trig, "tp", account_idx)

    @staticmethod
    @retry(Exception, tries=MAXIMUM_NUMBER_OF_API_CALL_TRIES, delay=2)
    def close_position_post_only(
        coin: str,
        account_idx: int = 1,
        offset_bps: float = 2.0,            # целевой отступ в б.п. от рынка
        reprice_interval_sec: int = 15,     # как часто подтягиваться к топу книги
        reprice_threshold_bps: float = 0.5, # когда считать, что рынок «ушёл»
        min_offset_ticks: int = 1,          # гарантия не пересечь лучшую сторону
    ) -> Optional[dict]:
        """
        Закрыть позицию как мейкер (post-only). Если UPNL отрицательный — закрыть по рынку.
        Иначе — постинг post-only лимитника и поддержание у топа книги до полного закрытия.

        Возвращает итоговое состояние позиции по инструменту (как HL.get_position),
        т.е. обычно None (позиция закрыта). Не имеет лимита попыток.
        """
        cl   = HL._get_client(account_idx)
        name = coin[:-4]

        # ------- состояние позиции -------
        pos = HL.get_position(coin, account_idx)
        if not pos or abs(float(pos.get("size", 0.0))) <= 0.0:
            sv.logger.info("[close_post_only:%s] no position found", coin)
            return None

        sz  = abs(float(pos["size"]))
        pnl = float(pos.get("unrealizedPnl", 0.0))
        sd_close = "Sell" if pos["size"] > 0 else "Buy"
        is_buy_close = (sd_close == "Buy")
        sv.logger.info(
            "[close_post_only:%s] start: size=%.10f side_close=%s upnl=%.6f",
            coin, sz, sd_close, pnl
        )

        # ------- если UPNL < 0 — закрываем по рынку -------
        if pnl < 0:
            sv.logger.warning("[close_post_only:%s] UPNL negative -> market close", coin)
            try:
                HL.open_market_order(
                    coin=coin,
                    sd=sd_close,
                    amount_usdt=0,
                    reduce_only=True,
                    amount_coins=sz,
                    account_idx=account_idx,
                )
            except Exception:
                sv.logger.exception("[close_post_only:%s] market close failed", coin)
                raise
            time.sleep(0.2)
            return HL.get_position(coin, account_idx)

        # ------- meta/decimals -------
        try:
            meta   = HL.instrument_info(coin, account_idx)
            sz_dec = int(meta["szDecimals"])
            px_dec = max(0, 6 - sz_dec)  # правило точности цены у HL перпов
        except Exception:
            sv.logger.exception("[close_post_only:%s] failed to fetch meta", coin)
            raise

        # ------- вспомогательные -------
        def _round_floor(x: float, dec: int) -> float:
            if dec <= 0:
                return float(int(x))
            s = 10.0 ** dec
            return float(int(x * s)) / s

        def _round_ceil(x: float, dec: int) -> float:
            if dec <= 0:
                v = int(x)
                return float(v if x == v else v + 1)
            s = 10.0 ** dec
            v = x * s
            iv = int(v)
            return float(iv if v == iv else iv + 1) / s

        def _book_best_and_tick() -> tuple[float, float, float, float]:
            """Возвращает (best_bid, best_ask, tick, mid). Надёжен к пустому снапшоту."""
            mid = float(cl["info"].all_mids()[name])
            book = cl["info"].l2_snapshot(name)
            levels = book.get("levels", [])
            # дефолтный тик из точности цены
            default_tick = 10.0 ** (-px_dec)

            if not levels or len(levels) < 2:
                return mid - default_tick, mid + default_tick, default_tick, mid

            side0 = [float(x["px"]) for x in levels[0] if "px" in x]
            side1 = [float(x["px"]) for x in levels[1] if "px" in x]
            if not side0 or not side1:
                return mid - default_tick, mid + default_tick, default_tick, mid

            def min_diff(prs):
                prs = sorted(set(prs))
                diffs = [prs[i+1]-prs[i] for i in range(len(prs)-1)]
                diffs = [d for d in diffs if d > 0]
                return min(diffs) if diffs else 0.0

            # грубая классификация сторон вокруг mid
            s0_min, s0_max = min(side0), max(side0)
            s1_min, s1_max = min(side1), max(side1)
            def classify(prices):
                mn, mx = min(prices), max(prices)
                if mx <= mid: return "bid"
                if mn >= mid: return "ask"
                avg = sum(prices) / len(prices)
                return "bid" if avg < mid else "ask"

            c0, c1 = classify(side0), classify(side1)
            if c0 == c1:
                bid_side = side0 if (s0_max <= s1_min) else side1
                ask_side = side1 if bid_side is side0 else side0
            else:
                bid_side = side0 if c0 == "bid" else side1
                ask_side = side1 if c1 == "ask" else side0

            best_bid = max(bid_side)
            best_ask = min(ask_side)
            tick_candidates = [min_diff(bid_side), min_diff(ask_side), default_tick]
            tick = min([t for t in tick_candidates if t > 0]) if any(t > 0 for t in tick_candidates) else default_tick
            return best_bid, best_ask, tick, mid

        def _target_px(best_bid: float, best_ask: float, tick: float, want_buy: bool, mkt_px: float) -> float:
            # базовая цель — отступ от "рынка" (mid) на offset_bps
            base = mkt_px
            off = base * (offset_bps / 1e4)
            raw = (base - off) if want_buy else (base + off)

            if want_buy:
                # закрываем шорт покупкой — стоим на бидах, не пересекаем аск
                lim = best_ask - max(min_offset_ticks, 1) * tick
                raw = min(raw, lim)      # не выше почти ask
                raw = max(raw, best_bid) # но не глубже, чем bid
                px = _round_floor(raw, px_dec)
                if px >= best_ask:
                    px = _round_floor(best_ask - tick, px_dec)
            else:
                # закрываем лонг продажей — стоим на асках, не пересекаем бид
                lim = best_bid + max(min_offset_ticks, 1) * tick
                raw = max(raw, lim)      # не ниже почти bid
                raw = min(raw, best_ask) # но не выше топа ask
                px = _round_ceil(raw, px_dec)
                if px <= best_bid:
                    px = _round_ceil(best_bid + tick, px_dec)
            return px

        # ------- актуальный размер к закрытию с округлением -------
        def _remaining_size_dec() -> float:
            p = HL.get_position(coin, account_idx)
            if not p:
                return 0.0
            return round(abs(float(p["size"])), sz_dec)

        # В начале на всякий случай уберём чужие активные лимитки по этому инструменту
        try:
            HL.cancel_all_orders(coin, account_idx)
        except Exception:
            sv.logger.exception("[close_post_only:%s] cancel_all_orders failed (ignored)", coin)

        # ------- первичная постановка ALO -------
        oid = None
        last_mid = None
        while True:
            # если позицию уже закрыли между попытками — выходим
            rem = _remaining_size_dec()
            if rem <= 0.0:
                sv.logger.info("[close_post_only:%s] position already flat", coin)
                return HL.get_position(coin, account_idx)

            try:
                best_bid, best_ask, tick, mid = _book_best_and_tick()
                last_mid = mid
                px = _target_px(best_bid, best_ask, tick, is_buy_close, mid)
                sv.logger.info(
                    "[close_post_only:%s] place ALO close: side=%s size=%.*f px=%.*f (bb=%.12f ba=%.12f tick=%.*f)",
                    coin, sd_close, sz_dec, rem, px_dec, px, best_bid, best_ask, px_dec, tick
                )
                order = cl["exchange"].order(
                    name,
                    is_buy_close,
                    rem,
                    px,
                    {"limit": {"tif": "Alo"}},
                    reduce_only=True,
                )
            except Exception:
                sv.logger.exception("[close_post_only:%s] initial ALO failed, retrying…", coin)
                time.sleep(1)
                continue

            try:
                st = order["response"]["data"]["statuses"][0]
            except Exception:
                st = {}
                sv.logger.warning("[close_post_only:%s] unexpected order response, retry place", coin)

            if "filled" in st:
                sv.logger.info("[close_post_only:%s] immediately filled", coin)
                time.sleep(0.2)
                return HL.get_position(coin, account_idx)

            if "resting" in st:
                oid = st["resting"]["oid"]
                break

            # ALO отклонён — попробуем безопаснее (увеличим запас на 2 тика) и ещё раз
            try:
                best_bid, best_ask, tick, mid = _book_best_and_tick()
                last_mid = mid
                px2 = _target_px(best_bid, best_ask, tick * 2, is_buy_close, mid)
                sv.logger.info(
                    "[close_post_only:%s] ALO rejected -> retry: px2=%.*f (bb=%.12f ba=%.12f tick*2=%.*f)",
                    coin, px_dec, px2, best_bid, best_ask, px_dec, tick * 2
                )
                order2 = cl["exchange"].order(
                    name,
                    is_buy_close,
                    rem,
                    px2,
                    {"limit": {"tif": "Alo"}},
                    reduce_only=True,
                )
                st2 = order2["response"]["data"]["statuses"][0]
                if "filled" in st2:
                    sv.logger.info("[close_post_only:%s] second attempt filled", coin)
                    time.sleep(0.2)
                    return HL.get_position(coin, account_idx)
                if "resting" in st2:
                    oid = st2["resting"]["oid"]
                    break
            except Exception:
                sv.logger.exception("[close_post_only:%s] retry ALO failed, will loop", coin)
                time.sleep(1)

        # ------- мониторинг и перестановка до полного закрытия -------
        sv.logger.info(
            "[close_post_only:%s] monitoring started: reprice_interval=%ss threshold_bps=%.4f",
            coin, reprice_interval_sec, reprice_threshold_bps
        )
        last_check = time.time()
        while True:
            # позиция уже закрыта?
            rem = _remaining_size_dec()
            if rem <= 0.0:
                sv.logger.info("[close_post_only:%s] closed successfully", coin)
                return HL.get_position(coin, account_idx)

            # наш ордер всё ещё открыт?
            try:
                open_ords = HL.get_open_orders(account_idx)
            except Exception:
                sv.logger.exception("[close_post_only:%s] get_open_orders failed", coin)
                open_ords = []

            still_open = any(o.get("oid") == oid for o in open_ords)
            if not still_open:
                # либо полностью исполнилось (и позиция уже ~0), либо биржа перевыдала oid
                sv.logger.info("[close_post_only:%s] oid=%s not in open list; will re-place if still not flat", coin, oid)
                if _remaining_size_dec() <= 0.0:
                    return HL.get_position(coin, account_idx)
                # перевыставим ALO заново на остаток
                oid = None
                try:
                    best_bid, best_ask, tick, mid = _book_best_and_tick()
                    last_mid = mid
                    px = _target_px(best_bid, best_ask, tick, is_buy_close, mid)
                    order = cl["exchange"].order(
                        name,
                        is_buy_close,
                        rem,
                        px,
                        {"limit": {"tif": "Alo"}},
                        reduce_only=True,
                    )
                    st = order["response"]["data"]["statuses"][0]
                    if "filled" in st:
                        time.sleep(0.2)
                        return HL.get_position(coin, account_idx)
                    if "resting" in st:
                        oid = st["resting"]["oid"]
                except Exception:
                    sv.logger.exception("[close_post_only:%s] re-place ALO failed", coin)
                time.sleep(1)
                continue

            # периодическая подтяжка цены
            now = time.time()
            if now - last_check >= reprice_interval_sec:
                last_check = now
                try:
                    best_bid, best_ask, tick, mid = _book_best_and_tick()
                except Exception:
                    sv.logger.exception("[close_post_only:%s] book fetch failed in monitor", coin)
                    time.sleep(1)
                    continue

                moved = False
                if last_mid is not None:
                    thr = (reprice_threshold_bps / 1e4) * (mid if mid else 1.0)
                    moved = abs(mid - last_mid) >= thr

                if moved:
                    last_mid = mid
                    new_sz = _remaining_size_dec()
                    if new_sz <= 0.0:
                        sv.logger.info("[close_post_only:%s] closed during monitor", coin)
                        return HL.get_position(coin, account_idx)

                    new_px = _target_px(best_bid, best_ask, tick, is_buy_close, mid)
                    sv.logger.info(
                        "[close_post_only:%s] modify_order: oid=%s side=%s new_sz=%.*f new_px=%.*f "
                        "(bb=%.12f ba=%.12f tick=%.*f, ALO)",
                        coin, oid, sd_close, sz_dec, new_sz, px_dec, new_px, best_bid, best_ask, px_dec, tick
                    )
                    try:
                        res = cl["exchange"].modify_order(
                            oid,
                            name,
                            is_buy_close,
                            new_sz,
                            new_px,
                            {"limit": {"tif": "Alo"}},
                            reduce_only=True,
                        )
                        if res and res.get("status") == "ok":
                            st = res["response"]["data"]["statuses"][0]
                            if "filled" in st:
                                sv.logger.info("[close_post_only:%s] modify -> filled", coin)
                                time.sleep(0.2)
                                return HL.get_position(coin, account_idx)
                            if "resting" in st:
                                oid = st["resting"]["oid"]
                    except Exception:
                        sv.logger.exception("[close_post_only:%s] modify_order failed", coin)

            time.sleep(1)
