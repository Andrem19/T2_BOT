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
        min_offset_ticks: int = 1,
    ) -> Optional[dict]:
        """
        Лимитный ордер строго post-only (TIF="Alo") максимально близко к рынку с авто-перестановкой.
        Ключевые изменения против прежней версии:
        • Ведём строгий учёт ИМЕННО остатка (remaining) относительно стартовой позиции;
        • На каждом цикле переставляем ордер ТОЛЬКО на remaining — никогда не больше;
        • Перед маркет-фолбэком всегда cancel_all по инструменту и повторный расчёт remaining;
        • Для reduce_only объём ограничивается текущей позицией на соответствующей стороне;
        • Упрощённый и детерминированный цикл: cancel → place ALO на остаток → наблюдение → (опционально) маркет на остаток.

        Возвращает:
            dict | None — актуальную позицию по инструменту (как в HL.get_position),
            либо None, если нечего было исполнять (например, reduce_only при отсутствии обратной позиции).
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
        side_mult = 1.0 if is_buy else -1.0

        # ---------- Метаданные и разрядности ----------
        try:
            meta = HL.instrument_info(coin, account_idx)
            sz_dec = int(meta["szDecimals"])
            px_dec = max(0, 6 - sz_dec)  # согласно SDK HL для perp
            min_tick_by_dec = 10.0 ** (-px_dec)
            sv.logger.debug(
                "[%s] meta: name=%s szDecimals=%d price_decimals=%d maxLev=%s",
                trace_id, name, sz_dec, px_dec, meta.get("maxLeverage"),
            )
        except Exception:
            sv.logger.exception("[%s] failed to fetch meta/instrument_info", trace_id)
            raise

        # ---------- Утилиты округления ----------
        def _round_floor(x: float, dec: int) -> float:
            if dec <= 0:
                return float(int(x))
            s = 10.0 ** dec
            return float(int(x * s)) / s

        def _round_ceil(x: float, dec: int) -> float:
            if dec <= 0:
                return float(int(x) if x == int(x) else int(x) + 1)
            s = 10.0 ** dec
            v = x * s
            iv = int(v)
            return float(iv if v == iv else iv + 1) / s

        # ---------- Книга и целевая цена ----------
        def _book_best_and_tick() -> tuple[float, float, float]:
            """bestBid, bestAsk, tick (оценка тика по книге, с фолбэком по dec)."""
            mid = float(cl["info"].all_mids()[name])
            book = cl["info"].l2_snapshot(name)
            levels = book.get("levels", [])
            if not levels or len(levels) < 2:
                return mid - min_tick_by_dec, mid + min_tick_by_dec, min_tick_by_dec

            side0 = [float(x["px"]) for x in levels[0] if "px" in x]
            side1 = [float(x["px"]) for x in levels[1] if "px" in x]
            if not side0 or not side1:
                return mid - min_tick_by_dec, mid + min_tick_by_dec, min_tick_by_dec

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

            def min_diff(prs):
                prs = sorted(set(prs))
                diffs = [prs[i+1]-prs[i] for i in range(len(prs)-1)]
                diffs = [d for d in diffs if d > 0]
                return min(diffs) if diffs else 0.0

            tick_candidates = [min_diff(bid_side), min_diff(ask_side), min_tick_by_dec]
            tick = min([t for t in tick_candidates if t > 0]) if any(t > 0 for t in tick_candidates) else min_tick_by_dec
            return best_bid, best_ask, tick

        def _target_px(best_bid: float, best_ask: float, tick: float, want_buy: bool, ref_px: float) -> float:
            """Целевая цена с учётом offset_bps и минимального безопасного отступа от противоположной лучшей."""
            off_abs = ref_px * (offset_bps / 1e4)
            raw = (ref_px - off_abs) if want_buy else (ref_px + off_abs)
            if want_buy:
                lim_top = best_ask - max(1, min_offset_ticks) * tick
                raw = min(raw, lim_top)
                raw = max(raw, best_bid)
                px = _round_floor(raw, px_dec)
                if px >= best_ask:
                    px = _round_floor(best_ask - tick, px_dec)
            else:
                lim_bot = best_bid + max(1, min_offset_ticks) * tick
                raw = max(raw, lim_bot)
                raw = min(raw, best_ask)
                px = _round_ceil(raw, px_dec)
                if px <= best_bid:
                    px = _round_ceil(best_bid + tick, px_dec)
            return px

        # ---------- Позиция и объём ----------
        def _signed_pos() -> float:
            """Подпись: long > 0, short < 0. Унифицируем разные формы ответа HL.get_position."""
            try:
                p = HL.get_position(coin, account_idx)
                if not p:
                    return 0.0
                # Частые варианты:
                if "szi" in p and p["szi"] not in (None, ""):
                    return float(p["szi"])
                if "size" in p and p.get("side") in ("Buy", "Sell"):
                    sz = float(p["size"])
                    return sz if p["side"] == "Buy" else -sz
                # Последний шанс: ищем любой числовой ключ с 'sz'
                for k, v in p.items():
                    if "sz" in k.lower():
                        try:
                            return float(v)
                        except Exception:
                            continue
            except Exception:
                sv.logger.exception("[%s] _signed_pos() failed", trace_id)
            return 0.0

        # Рассчёт требуемого размера заявки
        try:
            last = HL.get_last_price(coin, account_idx)
            base_size = amount_coins if amount_coins > 0 else (amount_usdt / last)
            target_abs = round(float(base_size), sz_dec)
            if target_abs <= 0:
                raise ValueError("Order size <= 0 after rounding. Check amount_usdt/amount_coins.")
        except Exception:
            sv.logger.exception("[%s] failed to compute target size", trace_id)
            raise

        # Лимит плеча (не меняем бизнес-логику)
        try:
            lev = min(20, int(meta["maxLeverage"]))
            HL.set_leverage(coin, lev, account_idx)
            sv.logger.info("[%s] leverage set: lev=%s", trace_id, lev)
        except Exception:
            sv.logger.exception("[%s] failed to set leverage", trace_id)
            raise

        start_pos = _signed_pos()
        sv.logger.info("[%s] start_pos=%.*f, requested_abs=%.*f, side=%s reduce_only=%s",
                    trace_id, sz_dec, start_pos, sz_dec, target_abs, sd, reduce_only)

        # Для reduce_only ограничим объём доступной обратной частью позиции
        if reduce_only:
            avail_to_reduce = max(0.0, -start_pos) if is_buy else max(0.0, start_pos)
            target_abs = min(target_abs, round(avail_to_reduce, sz_dec))
            if target_abs <= 0:
                sv.logger.info("[%s] nothing to reduce (reduce_only). Returning current position.", trace_id)
                return HL.get_position(coin, account_idx)

        desired_delta = side_mult * target_abs  # на сколько хотим изменить позицию относительно start_pos

        def _remaining() -> float:
            """Сколько ещё нужно ИЗМЕНИТЬ позицию (signed) от текущего состояния до цели."""
            cur = _signed_pos()
            executed = cur - start_pos
            # учитываем только компонент в нужном направлении
            if desired_delta > 0:
                executed_aligned = max(0.0, executed)
            else:
                executed_aligned = min(0.0, executed)
            rem = desired_delta - executed_aligned
            # округлим к шагу размера, чтобы избежать бесконечного хвоста
            if rem > 0:
                rem = max(0.0, round(rem, sz_dec))
            else:
                rem = min(0.0, round(rem, sz_dec))
            return rem

        def _place_alo_for_remaining() -> Optional[str]:
            """Отменяем существующие по инструменту и ставим ALO ровно на текущий остаток."""
            # Отменяем все свои лимитные, чтобы избежать дублирования
            try:
                HL.cancel_all_orders(coin, account_idx)
            except Exception:
                sv.logger.exception("[%s] cancel_all before place failed (ignored)", trace_id)

            # Пересчитываем остаток после отмены
            rem = _remaining()
            if abs(rem) <= (10 ** -sz_dec):
                sv.logger.info("[%s] nothing to place after cancel_all; remaining≈0", trace_id)
                return None

            want_buy = (rem > 0)
            rem_abs = abs(rem)
            best_bid, best_ask, tick = _book_best_and_tick()
            ref_px  = float(cl["info"].all_mids()[name])
            px      = _target_px(best_bid, best_ask, tick, want_buy, ref_px)

            sv.logger.info(
                "[%s] place ALO: side=%s rem=%.*f px=%.*f (bb=%.12f ba=%.12f tick=%.*f) reduce_only=%s",
                trace_id, "Buy" if want_buy else "Sell", sz_dec, rem_abs, px_dec, px, best_bid, best_ask, px_dec, tick, reduce_only
            )
            try:
                order = cl["exchange"].order(
                    name, want_buy, rem_abs, px, {"limit": {"tif": "Alo"}}, reduce_only=reduce_only
                )
                st = None
                if order and order.get("status") == "ok":
                    st = order["response"]["data"]["statuses"][0]
                else:
                    sv.logger.warning("[%s] exchange.order returned non-ok: %s", trace_id, order)

                # Быстрые случаи: filled/resting/ALO reject
                if st and "filled" in st:
                    sv.logger.info("[%s] ALO filled immediately (rem=%.*f).", trace_id, sz_dec, rem_abs)
                    return None
                if st and "resting" in st:
                    oid = st["resting"]["oid"]
                    sv.logger.info("[%s] ALO resting: oid=%s", trace_id, oid)
                    return oid

                # Если ALO отклонён — сделаем попытку с запасом 2 тика от противоположной лучшей
                best_bid, best_ask, tick2 = _book_best_and_tick()
                px2 = _target_px(best_bid, best_ask, tick2 * 2, want_buy, ref_px)
                sv.logger.info(
                    "[%s] ALO rejected; retry with extra ticks: px2=%.*f", trace_id, px_dec, px2
                )
                order2 = cl["exchange"].order(
                    name, want_buy, rem_abs, px2, {"limit": {"tif": "Alo"}}, reduce_only=reduce_only
                )
                if order2 and order2.get("status") == "ok":
                    st2 = order2["response"]["data"]["statuses"][0]
                    if "filled" in st2:
                        sv.logger.info("[%s] ALO (retry) filled immediately.", trace_id)
                        return None
                    if "resting" in st2:
                        oid = st2["resting"]["oid"]
                        sv.logger.info("[%s] ALO (retry) resting: oid=%s", trace_id, oid)
                        return oid

                sv.logger.warning("[%s] unable to place resting ALO for remaining.", trace_id)
                return None
            except Exception:
                sv.logger.exception("[%s] placing ALO failed", trace_id)
                return None

        # ---------- Основной цикл ----------
        last_ref_px = float(cl["info"].all_mids()[name])
        oid = _place_alo_for_remaining()
        cycles = 0
        last_tick_ts = time.time()

        sv.logger.info(
            "[%s] monitoring: reprice_interval=%ss max_wait_cycles=%s", trace_id, reprice_interval_sec, max_wait_cycles
        )

        while True:
            # Проверяем достижение цели
            rem = _remaining()
            if abs(rem) <= (10 ** -sz_dec):
                sv.logger.info("[%s] target reached; cancel leftovers and return.", trace_id)
                try:
                    HL.cancel_all_orders(coin, account_idx)
                except Exception:
                    sv.logger.exception("[%s] cancel_all at finish failed (ignored)", trace_id)
                return HL.get_position(coin, account_idx)

            # Периодическая перестановка и «подтяжка» к книге
            now = time.time()
            if now - last_tick_ts >= reprice_interval_sec:
                cycles += 1
                last_tick_ts = now

                try:
                    new_mid = float(cl["info"].all_mids()[name])
                except Exception:
                    new_mid = last_ref_px

                moved = abs(new_mid - last_ref_px) >= (reprice_threshold_bps / 1e4) * (new_mid if new_mid else 1.0)
                last_ref_px = new_mid

                if moved or oid is None:
                    sv.logger.info("[%s] reprice tick #%d; moved=%s", trace_id, cycles, moved)
                    oid = _place_alo_for_remaining()

                # Достигли лимита ожидания — уходим в маркет РОВНО на остаток
                if cycles >= max_wait_cycles:
                    sv.logger.warning("[%s] max_wait_cycles reached. Proceeding to market fallback.", trace_id)
                    try:
                        HL.cancel_all_orders(coin, account_idx)
                        sv.logger.info("[%s] cancel_all before market fallback done", trace_id)
                    except Exception:
                        sv.logger.exception("[%s] cancel_all before market fallback failed", trace_id)

                    # ОБЯЗАТЕЛЬНО пересчёт остатка ПОСЛЕ отмены — защита от «перевхода»
                    rem = _remaining()
                    if abs(rem) > (10 ** -sz_dec):
                        m_side = "Buy" if rem > 0 else "Sell"
                        rem_abs = abs(rem)
                        sv.logger.info(
                            "[%s] market fallback: side=%s rem=%.*f (reduce_only=%s)",
                            trace_id, m_side, sz_dec, rem_abs, reduce_only
                        )
                        # Маркет на ровно rem_abs
                        HL.open_market_order(
                            coin,
                            m_side,
                            amount_usdt=0,
                            reduce_only=reduce_only,
                            amount_coins=rem_abs,
                            account_idx=account_idx,
                        )
                    time.sleep(0.2)
                    return HL.get_position(coin, account_idx)

            time.sleep(0.5)


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
        Постинг лимитной заявки с post-only (TIF="Alo") на указанной цене, с
        корректной подгонкой под тики Hyperliquid.

        Возвращает:
            True  — если заявка успешно размещена (resting) или была мгновенно исполнена (filled),
            False — если заявка отклонена/ошибка/некорректные параметры.

        Особенности:
          - Учитывает одновременно два ограничения Hyperliquid:
              (а) не более 5 значащих цифр у цены,
              (б) не более (MAX_DECIMALS - szDecimals) знаков после запятой
                  (для perps MAX_DECIMALS=6, для spot MAX_DECIMALS=8).
          - Эффективный тик = max(тик по значащим цифрам, тик по десятичным).
          - Квантизация: для Buy — вниз (floor), для Sell — вверх (ceil).
          - Короткая верификация наличия resting-ордера в open_orders.
        """
        import math
        import time
        from decimal import Decimal, ROUND_DOWN, ROUND_UP

        cl = HL._get_client(account_idx)
        name = coin[:-4]
        is_buy = (sd == "Buy")

        # --- метаданные и разрядности ---
        try:
            meta = HL.instrument_info(coin, account_idx)
            sz_dec = int(meta["szDecimals"])
            # По документации HL: MAX_DECIMALS=6 для перпов, 8 для спота.
            # В этом методе работаем с перпами (exchange.order), поэтому:
            MAX_DECIMALS = 6
            dec_limit = max(0, MAX_DECIMALS - sz_dec)  # лимит знаков после запятой
        except Exception:
            sv.logger.exception("[place_limit_post_only:%s] failed to read instrument meta", coin)
            return False

        # --- утилиты квантизации цены под сетку HL ---
        def _compute_effective_tick(px: float) -> Decimal:
            """
            Эффективный тик = max(тик по значащим цифрам, тик по десятичным):
              - по значащим: 10^(floor(log10(px)) - 4), ограничение ≤ 5 sigfigs;
              - по десятичным: 10^(-dec_limit).
            Для px<=0 — используем только десятичный тик.
            """
            if not (isinstance(px, (int, float)) and math.isfinite(px)) or px <= 0:
                base_tick = Decimal(10) ** Decimal(-dec_limit)
                return base_tick

            order_mag = math.floor(math.log10(px))  # порядок
            # Шаг, чтобы не превысить 5 значащих цифр (последняя — 10^(order_mag-4))
            sig_tick = Decimal(10) ** Decimal(order_mag - 4)
            base_tick = Decimal(10) ** Decimal(-dec_limit)
            return max(sig_tick, base_tick)

        def _quantize_price(px: float, is_buy: bool) -> float:
            tick = _compute_effective_tick(px)
            if tick <= 0:
                return 0.0

            # Сначала грубая квантизация по тикам (floor/ceil)
            px_dec = Decimal(str(px))
            q = (px_dec / tick)
            q = (q.to_integral_value(rounding=ROUND_DOWN if is_buy else ROUND_UP)) * tick

            # Приведём число к приемлемому количеству знаков (dec_limit), без дрожи float
            fmt_quant = Decimal(10) ** Decimal(-dec_limit)  # квант для форматирования
            q = q.quantize(fmt_quant, rounding=ROUND_DOWN if is_buy else ROUND_UP)

            # В редких случаях при очень больших ценах decimals запрещены — тик>=1 и q уже целое
            return float(q)

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

        # --- цена: аккуратно квантизируем под тики HL ---
        try:
            raw_px = float(limit_px)
            px = _quantize_price(raw_px, is_buy)
            if px <= 0:
                sv.logger.warning("[place_limit_post_only:%s] non-positive px after quantization; abort", coin)
                return False
            if abs(px - raw_px) / max(1.0, abs(raw_px)) > 1e-8:
                sv.logger.info("[place_limit_post_only:%s] px quantized: raw=%s -> px=%s", coin, raw_px, px)
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

        # Явная обработка ошибок, которые возвращает HL (в т.ч. tick size)
        if "error" in st:
            sv.logger.warning("[place_limit_post_only:%s] order rejected: %s", coin, st.get("error"))
            return False

        # Если вдруг сразу исполнилось (обычно с Alo не должно) — успех
        if "filled" in st:
            return True

        # Если стоит в книге — проверяем по open_orders наличие OID
        if "resting" in st:
            oid = st["resting"].get("oid")
            if not oid:
                return False

            for _ in range(3):
                try:
                    open_ords = HL.get_open_orders(account_idx)
                    if any(o.get("oid") == oid for o in open_ords):
                        return True
                except Exception:
                    sv.logger.exception("[place_limit_post_only:%s] get_open_orders failed", coin)
                time.sleep(0.2)

            sv.logger.warning("[place_limit_post_only:%s] resting OID %s not found in open_orders", coin, oid)
            return False

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
