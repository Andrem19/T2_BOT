# futures_client/drift_api.py
# futures_client/drift_api.py
import threading
import asyncio
import os
import time
import json
import random
from typing import Optional, Any, List
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Optional, List, Tuple
from solders.signature import Signature as SolderSignature

from decouple import config
from retry import retry
import shared_vars as sv
from solana.rpc.async_api import AsyncClient
from anchorpy.provider import Wallet

from driftpy.drift_client import DriftClient
from driftpy.keypair import load_keypair
from driftpy.decode.utils import decode_name
from driftpy.constants.numeric_constants import (
    BASE_PRECISION,
    QUOTE_PRECISION,
    PRICE_PRECISION,
)
from driftpy.types import (
    OrderParams,
    OrderType,
    PositionDirection,
    MarketType,
    PostOnlyParams,
    OrderTriggerCondition,
)

from driftpy.account_subscription_config import AccountSubscriptionConfig

import helpers.tlg as tlg
from shared_vars import logger  # ваш централизованный логгер

from futures_client.custom_cached_drift import CachedDriftClient


def _uid() -> int:                                  # уникальные id
    
    uid = sv.next_uid
    sv.next_uid = 1 if sv.next_uid >= 2**31 - 1 else sv.next_uid + 1
    return uid

MAX_TRIES = 3
_DEFAULT_SUB_ID = 0
_SOL_STEP_INT_FALLBACK = 10_000_000

class DRIFT:
    """
    DRIFT-менеджер с потокобезопасным кешем клиентов **и «ленивой» инициализацией субаккаунтов**.
    """

    _tls = threading.local()  # thread-local storage

    # ───────────────── internal helpers ───────────────── #

    @classmethod
    def _clients(cls) -> Dict[str, DriftClient]:
        if not hasattr(cls._tls, "clients"):
            cls._tls.clients = {}
        return cls._tls.clients  # type: ignore[attr-defined]

    @classmethod
    def _active_key(cls) -> str:
        key_env = getattr(cls._tls, "active_key", None)
        if key_env is None:
            raise RuntimeError(
                "DRIFT client not initialised in this thread. "
                "Call `await DRIFT.init(\"YOUR_KEY_ENV\")` first."
            )
        return key_env

    @classmethod
    def _client(cls, *, key_env: Optional[str] = None) -> DriftClient:
        key_env = key_env or cls._active_key()
        try:
            return cls._clients()[key_env]
        except KeyError:
            raise RuntimeError(
                f"DRIFT client for key_env='{key_env}' is not initialised. "
                f"Call `await DRIFT.init(\"{key_env}\")` first."
            ) from None

    # ---------- NEW: lazy subaccount helper ---------- #
    # ---------- FIXED: lazy sub-account helper ---------- #
    # ---------- FIXED & SIMPLIFIED: lazy sub-account helper ---------- #
    @staticmethod
    async def _ensure_sub_account(client: DriftClient, sub_account_id: int) -> None:
        """
        Гарантирует, что саб-аккаунт:
        1. Присутствует в локальном клиенте (`add_user`);
        2. Создан on-chain (`initialize_user`);
        3. Подписан (idempotent `subscribe()`).

        Поддерживает как новые, так и более старые версии driftpy.
        """

        # 1) Есть ли объект-слушатель в памяти?
        try:
            user_obj = client.get_user(sub_account_id, skip_initialization=True)
        except TypeError:                     # старый driftpy без аргумента
            user_obj = client.users.get(sub_account_id)
        if user_obj is None:
            await client.add_user(sub_account_id)
            user_obj = client.get_user(sub_account_id)

        # 2) Попытка прочитать аккаунт ⇒ если data == None → значит PDA не существует
        try:
            ua_slot = user_obj.account_subscriber.get_user_account_and_slot()
            need_init = ua_slot is None or ua_slot.data is None
        except Exception:                     # крайне старый SDK
            need_init = True

        if need_init:
            try:
                await client.initialize_user(sub_account_id=sub_account_id)
            except Exception as exc:
                # Если уже создан → игнорируем «already in use» и подобные
                msg = str(exc).lower()
                if "already" not in msg and "exist" not in msg:
                    raise

        # 3) Гарантированно активируем подписку (без проверки – безопасно)
        await client.subscribe()


    # ─────────────── public API ─────────────── #

    @staticmethod
    @retry(Exception, tries=MAX_TRIES, delay=1)
    async def init(
        key_env: str = "KEYPAIR_PATH_1",
        *,
        sub_account_ids: Optional[List[int]] = None,
        force: bool = False,
    ) -> None:
        if sub_account_ids is None:
            sub_account_ids = [_DEFAULT_SUB_ID]

        clients = DRIFT._clients()
        if key_env in clients and not force:
            client = clients[key_env]
            # подцепляем недостающие карманы
            for sa in sub_account_ids:
                try:
                    exists = client.get_user(sa, skip_initialization=True)
                except TypeError:
                    exists = client._users.get(sa)
                if exists is None:
                    await client.add_user(sa)
            DRIFT._tls.active_key = key_env
            return

        # 1) Keypair / Wallet
        keyfile = config(key_env, default="~/.config/solana/mainnet-id.json")
        keyfile = os.path.expanduser(keyfile)
        kp = load_keypair(keyfile)
        wallet = Wallet(kp)

        # 2) RPC
        rpc_url = config(
            "MAINNET_RPC_ENDPOINT", default="https://api.mainnet-beta.solana.com"
        )
        conn = AsyncClient(rpc_url, timeout=30)

        # 3) WebSocket-подписки
        sub_cfg = AccountSubscriptionConfig("websocket")

        # 4) DriftClient
        client = CachedDriftClient(
            connection=conn,
            wallet=wallet,
            env="mainnet",
            account_subscription=sub_cfg,
            perp_market_indexes=[0],
            spot_market_indexes=[0],
            active_sub_account_id=sub_account_ids[0],
            sub_account_ids=sub_account_ids,
        )

        # 5) Добавляем юзеров
        for sa in sub_account_ids:
            await client.add_user(sa)

        await client.subscribe()
        await client.fetch_market_lookup_table_accounts()

        # 6) Кешируем
        clients[key_env] = client
        DRIFT._tls.active_key = key_env

    # ────────────────── вспомогательные методы ────────────────── #

    @staticmethod
    async def _find_market_index(symbol: str, *, key_env: Optional[str] = None) -> int:
        s = symbol.upper()
        if not s.endswith("-PERP"):
            s = f"{s}-PERP"
        client = DRIFT._client(key_env=key_env)
        for m in client.get_perp_market_accounts():
            if decode_name(m.name).strip().upper() == s:
                return m.market_index
        raise ValueError(f"Perp-market «{symbol}» not found")

    # ────────────────── бизнес-методы ────────────────── #

    @staticmethod
    @retry(Exception, tries=MAX_TRIES, delay=1)
    async def get_balance(
        asset: str = "USDC",
        *,
        sub_account_id: int = _DEFAULT_SUB_ID,
        key_env: Optional[str] = None,
    ) -> float:
        client = DRIFT._client(key_env=key_env)
        await DRIFT._ensure_sub_account(client, sub_account_id)  # --- ADDED
        user = client.get_user(sub_account_id)
        total = user.get_total_collateral()
        return float(total) / QUOTE_PRECISION

    @staticmethod
    @retry(Exception, tries=MAX_TRIES, delay=1)
    async def get_last_price(symbol: str, *, key_env: Optional[str] = None) -> float:
        client = DRIFT._client(key_env=key_env)
        pd = client.get_oracle_price_data_for_perp_market(0)
        return float(pd.price) / PRICE_PRECISION

    @staticmethod
    def _clamp_reduce_size(client: DriftClient, sub_account_id: int, want_base: float) -> int:
        """
        Возвращает размер (в int, perp precision) не больше фактической позиции.
        Применяется для reduce-only ордеров (SL/TP).
        Если позиции нет, возвращает 0.
        """
        try:
            user = client.get_user(sub_account_id)
            pos = user.get_perp_position(0)
        except Exception:
            pos = None

        onchain = 0
        if pos and getattr(pos, "base_asset_amount", 0):
            onchain = abs(pos.base_asset_amount)

        if onchain == 0:
            return 0

        want_int = int(want_base * BASE_PRECISION)
        if want_int <= 0:
            return 0

        return min(want_int, onchain)


    @staticmethod
    @retry(Exception, tries=MAX_TRIES, delay=1)
    async def open_limit_order(
        coin: str,
        sd: str,
        amount_usdt: float,
        limit_price: float,
        reduce_only: bool,
        amount_coins: float = 0.0,
        user_order_id: int = 0,
        *,
        sub_account_id: int = _DEFAULT_SUB_ID,
        key_env: Optional[str] = None,
    ) -> str:
        """
        Лимитка по SOL-PERP (market_index=0).
        Проверяем минимальный шаг и (если reduce_only=True) clamp к позиции.
        """
        client = DRIFT._client(key_env=key_env)
        await DRIFT._ensure_sub_account(client, sub_account_id)

        step_int = DRIFT._sol_order_step_int(client)
        step_f = step_int / BASE_PRECISION

        # размер
        if amount_coins and amount_coins > 0:
            size_base = Decimal(str(amount_coins))
        elif amount_usdt and amount_usdt > 0:
            size_base = Decimal(str(amount_usdt)) / Decimal(str(limit_price))
        else:
            size_base = Decimal("0")

        if reduce_only:
            base_amt = DRIFT._clamp_reduce_size(client, sub_account_id, float(size_base))
        else:
            base_amt = int(size_base * BASE_PRECISION)

        if base_amt < step_int:
            logger.warning(
                "DRIFT.open_limit_order size=%.8f int=%s < min_step=%.8f(%s) -> skip.",
                float(size_base), base_amt, step_f, step_int
            )
            raise ValueError("size below min step")

        direction = PositionDirection.Long() if sd.lower() == "buy" else PositionDirection.Short()

        last_px = await DRIFT.get_last_price(coin, key_env=key_env)
        is_breakout = (
            (sd.lower() == "buy" and limit_price > last_px)
            or (sd.lower() == "sell" and limit_price < last_px)
        )

        if not user_order_id:
            user_order_id = _uid()

        price_i = int(limit_price * PRICE_PRECISION)

        if is_breakout:
            cond = OrderTriggerCondition.Above() if sd.lower() == "buy" else OrderTriggerCondition.Below()
            params = OrderParams(
                order_type=OrderType.TriggerLimit(),
                market_type=MarketType.Perp(),
                market_index=0,
                direction=direction,
                base_asset_amount=base_amt,
                price=price_i,
                reduce_only=reduce_only,
                trigger_price=price_i,
                trigger_condition=cond,
                post_only=PostOnlyParams.NONE(),
                user_order_id=user_order_id,
            )
        else:
            params = OrderParams(
                order_type=OrderType.Limit(),
                market_type=MarketType.Perp(),
                market_index=0,
                direction=direction,
                base_asset_amount=base_amt,
                price=price_i,
                reduce_only=reduce_only,
                post_only=PostOnlyParams.MustPostOnly(),
                user_order_id=user_order_id,
            )

        logger.info(
            "DRIFT.open_limit_order %s size=%s(%s) min_step=%.8f(%s) px=%.8f breakout=%s reduce_only=%s sub=%s uoid=%s",
            sd.upper(), str(size_base), base_amt, step_f, step_int, limit_price, is_breakout, reduce_only, sub_account_id, user_order_id
        )
        logger.debug("DRIFT.open_limit_order params=%r", params)

        tx_sig = await client.place_perp_order(params, sub_account_id=sub_account_id)
        logger.info("DRIFT.open_limit_order tx_sig=%s", tx_sig)
        return tx_sig

        
    @staticmethod
    def _sol_order_step_int(client: DriftClient) -> int:
        """
        Возвращает минимальный шаг base_asset_amount (int в perp precision) для SOL-PERP (market_index=0).
        Читаем из on-chain; fallback на _SOL_STEP_INT_FALLBACK, если поле отсутствует.
        """
        try:
            mk = client.get_perp_market_account(0)
            step_int = int(mk.amm.order_step_size)
            # у некоторых версий есть min_order_size; возьмём максимум для надёжности
            try:
                min_int = int(getattr(mk.amm, "min_order_size", 0))
                if min_int > 0:
                    step_int = max(step_int, min_int)
            except Exception:
                pass
            if step_int > 0:
                return step_int
        except Exception:
            pass
        return _SOL_STEP_INT_FALLBACK

    @staticmethod
    @retry(Exception, tries=MAX_TRIES, delay=2)
    async def get_position(
        symbol: str,
        *,
        sub_account_id: int = _DEFAULT_SUB_ID,
        key_env: Optional[str] = None,
    ):
        client = DRIFT._client(key_env=key_env)
        await DRIFT._ensure_sub_account(client, sub_account_id)  # --- ADDED
        user = client.get_user(sub_account_id)
        pos = user.get_perp_position(0)

        if pos and pos.base_asset_amount != 0:
            size = pos.base_asset_amount / BASE_PRECISION
            entry_px = (
                pos.quote_entry_amount / pos.base_asset_amount
            ) * BASE_PRECISION / PRICE_PRECISION
            last_px = await DRIFT.get_last_price(symbol, key_env=key_env)
            return {
                "positionValue": abs(size) * last_px,
                "unrealizedPnl": user.get_unrealized_pnl(True, 0) / QUOTE_PRECISION,
                "size": abs(size),
                "entryPx": abs(entry_px),
                "side": 1 if size > 0 else 2,
            }
        return None


    @staticmethod
    async def instrument_info(symbol: str, *, key_env: Optional[str] = None):
        client = DRIFT._client(key_env=key_env)
        mk = client.get_perp_market_account(0)
        try:
            tick_size = 1 / mk.tick_size_denominator
        except AttributeError:
            tick_size = 1 / PRICE_PRECISION

        return {
            "name": decode_name(mk.name).rstrip("\x00"),
            "marketIndex": 0,
            "initialMarginRatio": mk.margin_ratio_initial / 1e4,
            "maintenanceMarginRatio": mk.margin_ratio_maintenance / 1e4,
            "tickSize": tick_size,
            "baseLotSize": 1 / BASE_PRECISION,
        }

    @staticmethod
    async def set_leverage(symbol: str, leverage: int) -> None:  # noqa: ARG001
        return

    @staticmethod
    async def is_any_position_exists(
        *,
        sub_account_id: int | None = None,
        key_env: Optional[str] = None,
    ):
        client = DRIFT._client(key_env=key_env)
        sub_ids = (
            [sub_account_id] if sub_account_id is not None else client.get_sub_account_ids()
        )
        out = []
        for sa in sub_ids:
            await DRIFT._ensure_sub_account(client, sa)  # --- ADDED
            user = client.get_user(sa)
            for pos in user.get_user_account().perp_positions:
                if pos.base_asset_amount == 0:
                    continue
                size = pos.base_asset_amount / BASE_PRECISION
                sym = decode_name(
                    client.get_perp_market_account(pos.market_index).name
                ).rstrip("\x00")
                out.append([f"SA{sa}:{sym}", "Buy" if size > 0 else "Sell", abs(size)])
        return out

    @staticmethod
    async def cancel_order(
        coin: str,  # noqa: ARG001
        user_order_id: int,
        *,
        sub_account_id: int = _DEFAULT_SUB_ID,
        key_env: Optional[str] = None,
    ) -> None:
        client = DRIFT._client(key_env=key_env)
        await DRIFT._ensure_sub_account(client, sub_account_id)  # --- ADDED
        await client.cancel_order_by_user_id(
            user_order_id=user_order_id, sub_account_id=sub_account_id
        )

    @staticmethod
    async def cancel_all_orders(
        symbol: str,
        *,
        sub_account_id: int = _DEFAULT_SUB_ID,
        key_env: Optional[str] = None,
    ) -> None:
        client = DRIFT._client(key_env=key_env)
        await DRIFT._ensure_sub_account(client, sub_account_id)  # --- ADDED
        await client.cancel_orders(
            market_type=MarketType.Perp(), market_index=0, sub_account_id=sub_account_id
        )
        print("All orders cancelled")


    @staticmethod
    @retry(Exception, tries=MAX_TRIES, delay=3)
    async def open_market_order(
        symbol: str,
        side: str,
        amount_usdc: float,
        reduce_only: bool = False,
        amount_base: float = 0.0,
        *,
        sub_account_id: int = _DEFAULT_SUB_ID,
        key_env: Optional[str] = None,
    ) -> tuple[str, float]:
        """
        Маркет-ордер по SOL-PERP (market_index=0).

        Если amount_base==0, то размер считается из amount_usdc / текущая цена.
        Перед отправкой проверяем минимальный шаг (0.01 SOL по текущим параметрам).
        Если размер ниже шага:
            - при reduce_only=True: clamp к фактической позиции (см. _clamp_reduce_size).
            - иначе: НЕ отправляем ордер (ValueError); вызывающий код сам решает, что делать.
        """
        client = DRIFT._client(key_env=key_env)
        await DRIFT._ensure_sub_account(client, sub_account_id)

        ref_price = await DRIFT.get_last_price(symbol, key_env=key_env)

        if amount_base and amount_base > 0:
            base_sz = float(amount_base)
        else:
            base_sz = float(amount_usdc) / ref_price if amount_usdc > 0 else 0.0

        step_int = DRIFT._sol_order_step_int(client)
        step_f = step_int / BASE_PRECISION

        if reduce_only:
            # clamp к позиции
            base_amount_int = DRIFT._clamp_reduce_size(client, sub_account_id, base_sz)
            if base_amount_int < step_int:
                logger.warning(
                    "DRIFT.open_market_order reduce_only clamp<step: side=%s req=%.8f int=%s step=%s -> skip.",
                    side, base_sz, base_amount_int, step_int,
                )
                raise ValueError("reduce_only size below min step")
        else:
            base_amount_int = int(base_sz * BASE_PRECISION)
            if base_amount_int < step_int:
                logger.warning(
                    "DRIFT.open_market_order size %.8f < min_step %.8f -> skip.",
                    base_sz, step_f,
                )
                raise ValueError("size below min step")

        direction = PositionDirection.Long() if side.lower() == "buy" else PositionDirection.Short()

        params = OrderParams(
            order_type=OrderType.Market(),
            market_type=MarketType.Perp(),
            market_index=0,
            direction=direction,
            base_asset_amount=base_amount_int,
            price=0,
            reduce_only=reduce_only,
        )

        logger.info(
            "DRIFT.open_market_order side=%s base=%.8f(%s) min_step=%.8f(%s) reduce_only=%s sub=%s",
            side, base_sz, base_amount_int, step_f, step_int, reduce_only, sub_account_id
        )
        logger.debug("DRIFT.open_market_order params=%r", params)

        tx_sig = await client.place_perp_order(params, sub_account_id=sub_account_id)
        logger.info("DRIFT.open_market_order tx_sig=%s", tx_sig)
        return tx_sig, ref_price


    @staticmethod
    async def open_TP(
        symbol: str,
        side: str,
        amount_base: float,
        tp_price: float,
        *,
        sub_account_id: int = _DEFAULT_SUB_ID,
        key_env: Optional[str] = None,
        user_order_id: int | None = None,
        use_market: bool = True,
    ) -> None:
        """
        Тейк-профит. По умолчанию Trigger-Market (надежнее), опционально Limit.
        Объём clamped к фактической позиции. Проверяем min_step.
        """
        client = DRIFT._client(key_env=key_env)
        await DRIFT._ensure_sub_account(client, sub_account_id)

        side_lower = side.lower()
        if side_lower not in ("buy", "sell"):
            raise ValueError("side must be either 'buy' or 'sell'")

        step_int = DRIFT._sol_order_step_int(client)

        clamped_int = DRIFT._clamp_reduce_size(client, sub_account_id, amount_base)
        if clamped_int < step_int:
            logger.warning(
                "DRIFT.open_TP skipped: clamped_int=%s < step=%s (no/too-small position). side=%s sub=%s",
                clamped_int, step_int, side_lower, sub_account_id
            )
            return

        direction = PositionDirection.Short() if side_lower == "buy" else PositionDirection.Long()
        trig_cond = OrderTriggerCondition.Above() if side_lower == "buy" else OrderTriggerCondition.Below()

        if user_order_id is None:
            user_order_id = _uid()

        if use_market:
            params = OrderParams(
                order_type=OrderType.TriggerMarket(),
                market_type=MarketType.Perp(),
                market_index=0,
                direction=direction,
                base_asset_amount=clamped_int,
                trigger_price=int(tp_price * PRICE_PRECISION),
                trigger_condition=trig_cond,
                reduce_only=True,
                user_order_id=user_order_id,
            )
        else:
            price_i = int(tp_price * PRICE_PRECISION)
            params = OrderParams(
                order_type=OrderType.TriggerLimit(),
                market_type=MarketType.Perp(),
                market_index=0,
                direction=direction,
                base_asset_amount=clamped_int,
                price=price_i,
                trigger_price=price_i,
                trigger_condition=trig_cond,
                reduce_only=True,
                post_only=PostOnlyParams.NONE(),
                user_order_id=user_order_id,
            )

        logger.info(
            "DRIFT.open_TP trig=%.8f base_int=%s market=%s side=%s sub=%s uoid=%s",
            tp_price, clamped_int, use_market, side_lower, sub_account_id, user_order_id
        )
        logger.debug("DRIFT.open_TP params=%r", params)

        await client.place_perp_order(params, sub_account_id=sub_account_id)


    @staticmethod
    async def open_SL(
        symbol: str,
        side: str,
        amount_base: float,
        sl_price: float,
        *,
        sub_account_id: int = _DEFAULT_SUB_ID,
        key_env: Optional[str] = None,
        user_order_id: int | None = None,
    ) -> None:
        """
        Стоп-лосс (Trigger-Market) для закрытия позиции (reduce-only=True).
        Объём clamped к фактической позиции. Проверяем min_step.
        """
        client = DRIFT._client(key_env=key_env)
        await DRIFT._ensure_sub_account(client, sub_account_id)

        side_lower = side.lower()
        if side_lower not in ("buy", "sell"):
            raise ValueError("side must be either 'buy' or 'sell'")

        step_int = DRIFT._sol_order_step_int(client)

        clamped_int = DRIFT._clamp_reduce_size(client, sub_account_id, amount_base)
        if clamped_int < step_int:
            logger.warning(
                "DRIFT.open_SL skipped: clamped_int=%s < step=%s (no/too-small position). side=%s sub=%s",
                clamped_int, step_int, side_lower, sub_account_id
            )
            return

        direction = PositionDirection.Short() if side_lower == "buy" else PositionDirection.Long()

        if user_order_id is None:
            user_order_id = _uid()

        params = OrderParams(
            order_type=OrderType.TriggerMarket(),
            market_type=MarketType.Perp(),
            market_index=0,
            direction=direction,
            base_asset_amount=clamped_int,
            trigger_price=int(sl_price * PRICE_PRECISION),
            trigger_condition=(
                OrderTriggerCondition.Below() if side_lower == "buy" else OrderTriggerCondition.Above()
            ),
            reduce_only=True,
            user_order_id=user_order_id,
        )

        logger.info(
            "DRIFT.open_SL trig=%.8f base_int=%s side=%s sub=%s uoid=%s",
            sl_price, clamped_int, side_lower, sub_account_id, user_order_id
        )
        logger.debug("DRIFT.open_SL params=%r", params)

        await client.place_perp_order(params, sub_account_id=sub_account_id)


    @staticmethod
    async def _safe_open_limit(side: str, price: float,
                               qty: Decimal, key_path, subaccount) -> int | None:
        """Лимитка с retry; при ошибке 6059 удваиваем объём."""
        retries, cur_qty = 3, qty
        for n in range(1, retries + 1):
            try:
                uid = _uid()
                await DRIFT.open_limit_order(
                    coin          = 'SOL',
                    sd            = side,
                    amount_usdt   = 0,
                    limit_price   = price,
                    reduce_only   = (side == "Sell"),
                    amount_coins  = float(cur_qty),
                    user_order_id = uid,
                    key_env=key_path,
                    sub_account_id=subaccount
                )
                return uid
            except Exception as exc:
                msg = str(exc)
                too_small = ("6059" in msg or "Order Amount Too Small" in msg)
                if too_small and n < retries:
                    cur_qty *= Decimal("2")
                    await asyncio.sleep(0.2)
                    continue
                if n < retries:
                    await asyncio.sleep(0.5 * n)
                    continue
                try:
                    await tlg.send_inform_message(
                        "COLLECTOR_API",
                        f"‼️ Limit {side} {float(cur_qty):.6f}@{price} failed: {msg}",
                        "", False)
                except Exception:
                    pass
                return None

