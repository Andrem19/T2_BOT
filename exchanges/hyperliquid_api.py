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

            lev = min(8, int(info["maxLeverage"]))
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
