from shared_vars import logger
import shared_vars as sv
from exchanges.bybit_option_hub import BybitOptionHub as BB
from helpers.safe_sender import safe_send
from typing import Dict, Any
import helpers.tools as tools
import traceback
import asyncio
import math

async def refresh_opt(counter):
    try:
        for stage_key in ('first', 'second'):
            stage = sv.stages[stage_key]
            leg = stage['position']['leg']
            acc = 2 if stage_key =='first' else 1
            # Полное обновление информации об опционах раз в три итерации, если позиция существует
            if counter % 3 == 0 and leg.get('exist', False):
                open_options = BB.Trading.get_open_positions(account_idx=acc)

                if not open_options:
                    sv.stages[stage_key]['position']['leg']['exist'] = False
                else:
                    current_name = leg.get('name', '')
                    opt_raw = None
                    if current_name:
                        # Ищем в списке открытых опционов тот, который мы уже отслеживали
                        opt_raw = next((o for o in open_options if o['symbol'] == current_name), open_options[0])
                    else:
                        opt_raw = open_options[0]
                    
                    if opt_raw is None:
                        sv.stages[stage_key]['position']['leg']['exist'] = False
                    else:
                        # Извлекаем и сохраняем ключевую информацию
                        opt_info = extract_selected_info(opt_raw)

                        leg['info'] = opt_info

                        # Подписываемся на изменения стакана и даём время на установку подписки
                        BB.subscribe(leg['name'])
                        await asyncio.sleep(2)
                        
                        if 'deliveryTime' not in leg or leg['deliveryTime'] == 0:
                            snap = BB.get_ticker(leg['name'])
                            leg['deliveryTime'] = snap.deliveryTime

                        hours_to_exp = tools.time_to_expiry(leg['deliveryTime'])

                        leg['hours_to_exp'] = hours_to_exp
                        # Обновляем PnL по последней цене из стакана
                        ob       = BB.get_orderbook(leg['name'])

                        raw_bid, max_qty_at_price  = ob.bid_price_for_qty(leg['contracts'], vwap=False)

                        real_bid = tools.safe_float(raw_bid, 0.0)
                        avg_px   = leg['info'].get('avgPrice', 0.0)
                        size     = leg['contracts']

                        leg['info']['unrealisedPnl'] = (real_bid - avg_px) * size
                        leg['info']['used_bid']      = real_bid
                        leg['info']['max_size']      = max_qty_at_price


            # Промежуточный расчёт PnL между полными обновлениями
            elif leg.get('exist', False) and leg.get('name', ''):
                hours_to_exp = tools.time_to_expiry(leg['deliveryTime'])
                leg['hours_to_exp'] = hours_to_exp
                ob       = BB.get_orderbook(leg['name'])
                raw_bid, max_qty_at_price  = ob.bid_price_for_qty(leg['contracts'], vwap=False)
                real_bid = safe_float(raw_bid, 0.0)
                avg_px   = leg['info'].get('avgPrice', 0.0)
                size     = leg['contracts']

                leg['info']['unrealisedPnl'] = (real_bid - avg_px) * size
                leg['info']['used_bid']      = real_bid
                leg['info']['max_size']      = max_qty_at_price

    except Exception as e:
        logger.error(f"Error updating options info for stage '{stage_key}': {e}\n{traceback.format_exc()}")
        await safe_send(
            'TELEGRAM_API',
            f'ERROR (REFRESH OPTIONS INFO {stage_key.upper()}) leg name: {leg["name"]}\nobject ob: {ob}\nERROR: {e}'
        )
                
                
def extract_selected_info(info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Извлекает из info нужные поля: числовые приводятся к float,
    строковые оставляются как есть.
    """
    float_keys = [
        "avgPrice",
        "curRealisedPnl",
        "markPrice",
        "positionValue",
        "size",
        "unrealisedPnl",
        "used_bid",
    ]
    str_keys = [
        "symbol",
        "side",
    ]

    extracted: Dict[str, Any] = {}

    for key in float_keys:
        raw = info.get(key)
        if raw is None:
            continue
        try:
            extracted[key] = float(raw)
        except (TypeError, ValueError):
            continue

    for key in str_keys:
        raw = info.get(key)
        if raw is not None:
            extracted[key] = raw

    return extracted

def safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        # Отрицательные и NaN считаем некорректными для цены bid
        if math.isnan(v):
            return default
        return v
    except (TypeError, ValueError):
        return default