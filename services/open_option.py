import shared_vars as sv
from shared_vars import logger
from helpers.safe_sender import safe_send
import helpers.tools as ts
import asyncio
import uuid
import time
import traceback
from exchanges.bybit_option_hub import BybitOptionHub as BB
from typing import Dict, Any

_MAX_ASK_OVER_MARK_MULT = 5.0
_POLL_INTERVAL          = 5.00
_DEDLINE = 50

def _tp_id(tp_order) -> str | None:
    ord_ = tp_order
    if isinstance(ord_, dict):
        r = ord_.get("result") or {}
        return r.get("orderLinkId") or r.get("orderId")
    return None

def _pos_size(p_: Dict[str, Any]) -> float:
    for k in ("size", "positionSize", "qty", "positionQty", "position"):
        if k in p_ and p_[k] not in (None, "", "0", "0.0"):
            try:
                return float(p_[k])
            except Exception:  # noqa: BLE001
                pass
    return 0.0

def get_required_position():
    """
    Определяет, какая позиция требуется, на основании полей объекта sv.

    Args:
        sv: объект с атрибутами
            - close_1 (bool)
            - close_2 (bool)
            - stages (dict), где stages['first']['exist'] и stages['second']['exist'] — булевы флаги

    Returns:
        str: одна из {'nothing', 'first', 'second'}
    """
    # Значение по умолчанию
    which_pos_we_need = 'nothing'

    if (sv.close_1 and sv.close_2) or (
        sv.stages['second']['exist'] and not sv.stages['first']['exist']
    ):
        which_pos_we_need = 'nothing'
    elif (
        not sv.stages['first']['exist']
        and not sv.stages['second']['exist']
        and sv.close_1
        and not sv.close_2
    ):
        which_pos_we_need = 'second'
    elif (
        not sv.stages['first']['exist']
        and not sv.stages['second']['exist']
        and sv.close_2
        and not sv.close_1
    ):
        which_pos_we_need = 'first'
    elif (
        not sv.stages['first']['exist']
        and not sv.stages['second']['exist']
        and not sv.close_1
        and not sv.close_2
    ):
        which_pos_we_need = 'first'
    elif (
        not sv.stages['second']['exist']
        and sv.stages['first']['exist']
        and not sv.close_2
    ):
        which_pos_we_need = 'second'

    return which_pos_we_need

async def open_opt(position: dict, which_pos_we_need: str):
    try:
        if which_pos_we_need == 'nothing':
            return False
        acc = 2 if which_pos_we_need =='first' else 1
        v = sv.instruments[position['name']]
        contracts = round(sv.stages[which_pos_we_need]['amount']*v['kof'], v['round'])
        overpay_pad = v['overpay_pad']
        symbol = position['symbol']
        qty_abs = abs(contracts)
        desired_price = position['ask_original']
        tp_price = position['best_targ_bid']
        side_in = 'Buy'
        
        snapshot = BB.Chain.get_snapshot(symbol=symbol, testnet=False, with_greeks=False)
        
        askPrice = snapshot['askPrice']
        markPrice = snapshot['markPrice']
        
        if askPrice > desired_price + overpay_pad:
            reason = f"Ask {askPrice} > allowed {desired_price}+{overpay_pad}."
            logger.warning("open_opt(normal): %s %s", symbol, reason)
            return False
        if markPrice > 0.0 and askPrice / markPrice > _MAX_ASK_OVER_MARK_MULT:
            reason = f"Ask/Mark {askPrice/markPrice:.2f} > {_MAX_ASK_OVER_MARK_MULT}."
            logger.warning("open_opt(normal): %s %s", symbol, reason)
        
        limit_price = min(desired_price, askPrice)
        link_in = f"h-{uuid.uuid4().hex[:18]}"
        
        try:
            entry_order = BB.Trading.place_limit_order(
                account_idx=acc,
                symbol=symbol,
                side=side_in,
                price=limit_price,
                qty=qty_abs,
                time_in_force="FOK",
                order_link_id=link_in,
            )
        except Exception as e:
            logger.exception(f'ERROR (func) place_limit_order: {e}\n\n{traceback.format_exc()}')
            return False

        entry_res  = entry_order.get("result", {}) if isinstance(entry_order, dict) else {}
        entry_oid  = entry_res.get("orderId")
        entry_olid = entry_res.get("orderLinkId", link_in)
        
        deadline = time.time() + float(_DEDLINE)
        filled   = False
        pos_data = None
        

        while time.time() < deadline:
            pos = BB.Trading.get_open_positions(account_idx=acc)
            if pos:
                sz = _pos_size(pos[0])
                if abs(sz) >= qty_abs:
                    filled = True
                    pos_data = pos[0]
                    break
            await asyncio.sleep(_POLL_INTERVAL)
        if not filled:
            try:
                BB.Trading.cancel_order(account_idx=acc, symbol=symbol, order_id=entry_oid, order_link_id=entry_olid)
            except Exception:
                pos = BB.Trading.get_open_positions(account_idx=acc)
                if pos:
                    sz = _pos_size(pos[0])
                    if abs(sz) >= qty_abs:
                        filled = True
                        pos_data = pos[0]
                logger.exception("execute_hedge: cancel_entry failed %s", symbol)

            if not filled:
                reason = f"Entry not filled in {_DEDLINE}s."
                logger.warning("open_opt: %s %s", symbol, reason)
                return False

        #=====open take profit=========
        tp_order = None
        side_tp = "Sell"
        link_tp = f"h-TP-{uuid.uuid4().hex[:18]}"

        try:
            tp_order = BB.Trading.place_take_profit_limit_order(
                account_idx=acc,
                symbol=symbol,
                qty=contracts,
                trigger_price=tp_price,
                side=side_tp,
                order_link_id=link_tp,
            )
        except Exception as exc:
            logger.exception("Open_opt: place TP error %s", symbol)
            await safe_send("TELEGRAM_API", f'OPEN_OPT: The option is open but TAKE PROFIT fall\n\n{exc}\n\n{traceback.format_exc()}', '', False)
        
        tp_id = _tp_id(tp_order) if tp_order else None
        sv.stages[which_pos_we_need]['position']['leg'] = {
            'deliveryTime': snapshot['deliveryTime'],
            'hours_to_exp': ts.time_to_expiry(snapshot['deliveryTime']),
            'exist'    : True,
            'name'     : snapshot['symbol'],
            'contracts': round(sv.stages[which_pos_we_need]['amount']*v['kof'], v['round']),
            'tp_id'    : tp_id,
            'info'     : {}
        }
        BB.subscribe(sv.stages[which_pos_we_need]['position']['leg'].get('name'))
        logger.info(f"option leg filled; Take profit order seted up : {tp_id}")
        return True
            
    except Exception as e:
        logger.exception(f'OPEN OPTION ERROR: {e}\n\n{traceback.format_exc()}')
        return False



    
    