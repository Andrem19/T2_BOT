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
    #                      LIMIT (POST-ONLY) + авто-догон
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
    ) -> Optional[dict]:
        """
        Лимитный ордер строго post-only (TIF="Alo") как можно ближе к рынку с авто-перестановкой.
        Каждые `reprice_interval_sec` секунд проверяем: если рынок ушёл от нас дальше, двигаем ордер.
        Если спустя `max_wait_cycles` переоценок позиция не открыта — снимаем лимитки и берём по рынку.

        Возвращает:
            dict | None — открытая позиция по инструменту (как в HL.get_position),
            либо None, если позиции нет (например, reduce_only и нет позиции).
        """
        trace_id = f"limit_post_only:{coin}:{sd}:{int(time.time()*1000)}"
        sv.logger.info(
            "[%s] start: coin=%s sd=%s amount_usdt=%s amount_coins=%s reduce_only=%s "
            "acct=%s offset_bps=%.4f reprice_interval=%ss max_wait_cycles=%s reprice_thr_bps=%.4f",
            trace_id, coin, sd, amount_usdt, amount_coins, reduce_only,
            account_idx, offset_bps, reprice_interval_sec, max_wait_cycles, reprice_threshold_bps,
        )

        cl   = HL._get_client(account_idx)
        name = coin[:-4]
        is_buy = (sd == "Buy")

        # ---------- Метаданные и разрядности ----------
        try:
            meta = HL.instrument_info(coin, account_idx)
            sz_dec = int(meta["szDecimals"])
            px_dec = max(0, 6 - sz_dec)  # правило цены для перпов HL
            sv.logger.debug(
                "[%s] meta: name=%s szDecimals=%d -> size_decimals=%d, price_decimals=%d, maxLev=%s",
                trace_id, name, sz_dec, sz_dec, px_dec, meta.get("maxLeverage"),
            )
        except Exception:
            sv.logger.exception("[%s] failed to fetch meta/instrument_info", trace_id)
            raise

        def _round_px(x: float) -> float:
            if px_dec > 0:
                return float(f"{x:.{px_dec}f}")
            return float(int(x))

        def _target_px(mkt_px: float, want_buy: bool) -> float:
            off = mkt_px * (offset_bps / 1e4)
            raw = (mkt_px - off) if want_buy else (mkt_px + off)
            px  = max(1e-12, _round_px(raw))
            sv.logger.debug(
                "[%s] price_target: mkt=%.12f off=%.12f want_buy=%s -> px=%.12f",
                trace_id, mkt_px, off, want_buy, px,
            )
            return px

        def _moved_away(old_mkt: float, new_mkt: float, want_buy: bool) -> bool:
            thr = old_mkt * (reprice_threshold_bps / 1e4)
            moved = (new_mkt - old_mkt) >= thr if want_buy else (old_mkt - new_mkt) >= thr
            sv.logger.debug(
                "[%s] moved_away: old=%.12f new=%.12f thr=%.12f want_buy=%s -> %s",
                trace_id, old_mkt, new_mkt, thr, want_buy, moved,
            )
            return moved

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

        # ---------- Первичная постановка ALO ----------
        try:
            mkt_px = last
            px     = _target_px(mkt_px, is_buy)
            sv.logger.info(
                "[%s] place initial ALO: side=%s size=%.*f px=%.*f reduce_only=%s",
                trace_id, sd, sz_dec, size, px_dec, px, reduce_only,
            )
            order = cl["exchange"].order(
                name,
                is_buy,
                size,
                px,
                {"limit": {"tif": "Alo"}},  # post-only
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

        if "filled" in status0:
            sv.logger.info("[%s] initial ALO filled immediately (rare). Fetching position…", trace_id)
            time.sleep(1)
            pos = HL.get_position(coin, account_idx)
            sv.logger.info("[%s] position after immediate fill: %s", trace_id, pos)
            return pos

        if "resting" in status0:
            oid = status0["resting"]["oid"]
            sv.logger.info("[%s] initial order resting: oid=%s px=%.*f", trace_id, oid, px_dec, px)
        else:
            # Слишком агрессивная цена для ALO: пробуем отступить шире и повторить
            try:
                adj = 2.0 * offset_bps / 1e4
                px2 = _round_px((mkt_px - mkt_px * adj) if is_buy else (mkt_px + mkt_px * adj))
                sv.logger.info(
                    "[%s] ALO rejected/none-resting, retry with wider offset: px2=%.*f (adj=%.6f)",
                    trace_id, px_dec, px2, adj,
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
                    if "filled" in st:
                        sv.logger.info("[%s] second ALO filled immediately. Fetching position…", trace_id)
                        time.sleep(1)
                        pos = HL.get_position(coin, account_idx)
                        sv.logger.info("[%s] position after second fill: %s", trace_id, pos)
                        return pos
                    if "resting" in st:
                        oid = st["resting"]["oid"]
                        px = px2
                        sv.logger.info("[%s] second ALO resting: oid=%s px=%.*f", trace_id, oid, px_dec, px)
            except Exception:
                sv.logger.exception("[%s] retry ALO (wider offset) failed", trace_id)

            if oid is None:
                sv.logger.warning(
                    "[%s] unable to place ALO resting order; fallback to market open", trace_id
                )
                HL.open_market_order(coin, sd, amount_usdt, reduce_only, amount_coins, account_idx)
                time.sleep(1)
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
                # Считаем, что исполнился/снялся где-то вовне — проверим позицию
                sv.logger.info("[%s] order not found in open list; fetching position…", trace_id)
                time.sleep(1)
                pos = HL.get_position(coin, account_idx)
                sv.logger.info("[%s] position after disappearance from open list: %s", trace_id, pos)
                return pos

            now = time.time()
            if now - last_check >= reprice_interval_sec:
                cycles += 1
                last_check = now

                try:
                    new_mkt = HL.get_last_price(coin, account_idx)
                except Exception:
                    sv.logger.exception("[%s] get_last_price failed during monitoring", trace_id)
                    new_mkt = mkt_px

                sv.logger.debug(
                    "[%s] reprice tick #%d: mkt_old=%.12f mkt_new=%.12f",
                    trace_id, cycles, mkt_px, new_mkt,
                )

                if _moved_away(mkt_px, new_mkt, is_buy):
                    mkt_px = new_mkt
                    new_px = _target_px(mkt_px, is_buy)
                    sv.logger.info(
                        "[%s] modify_order: oid=%s side=%s size=%.*f new_px=%.*f (ALO)",
                        trace_id, oid, sd, sz_dec, size, px_dec, new_px,
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
                            if "filled" in st:
                                sv.logger.info("[%s] modify_order -> filled. Fetching position…", trace_id)
                                time.sleep(1)
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
                    time.sleep(1)
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
    ):
        """
        Простой постинг лимитной заявки с post-only (TIF="Alo") на указанной цене.
        Без автопереноса, без дополнительных проверок и смены плеча.
        Возвращает:
            oid ордера (если заявка resting/filled), иначе 0.
        """
        cl   = HL._get_client(account_idx)
        name = coin[:-4]
        is_buy = (sd == "Buy")

        # --- метаданные и разрядности ---
        meta   = HL.instrument_info(coin, account_idx)
        sz_dec = int(meta["szDecimals"])
        px_dec = max(0, 6 - sz_dec)  # для перпов правило точности цены

        def _round_px(x: float) -> float:
            return float(f"{x:.{px_dec}f}") if px_dec > 0 else float(int(x))

        # --- размер ---
        last = HL.get_last_price(coin, account_idx)
        size = amount_coins or (amount_usdt / last)
        size = round(float(size), sz_dec)

        # --- цена (как передана, лишь аккуратное округление под тик) ---
        px = _round_px(float(limit_px))

        # --- отправка post-only ордера ---
        order = cl["exchange"].order(
            name,
            is_buy,
            size,
            px,
            {"limit": {"tif": "Alo"}},   # строго post-only
            reduce_only=reduce_only,
        )

        # --- разбор ответа: возвращаем oid если есть ---
        try:
            st = order["response"]["data"]["statuses"][0]
            if "resting" in st:
                return st["resting"]["oid"]
            if "filled" in st:
                return st["filled"]["oid"]
        except Exception:
            pass
        return 0


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