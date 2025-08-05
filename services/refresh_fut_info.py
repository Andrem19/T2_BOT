from shared_vars import logger
import shared_vars as sv
from exchanges.hyperliquid_api import HL
from helpers.price_feed import PriceCache
from helpers.safe_sender import safe_send
import traceback

async def refresh_fut(counter):
    try:
        # Проверяем существование позиции 1
        for stage_key in ('first', 'second'):
            coin = sv.stages[stage_key]['base_coin']
            symbol_fut = coin + 'USDT'
            last_px = PriceCache.get(f'{coin}USDT')
            if sv.stages[stage_key]['exist']:
                # Обновляем информацию раз в три итерации
                if counter % 3 == 0:
                    acc = 2 if stage_key == 'first' else 1
                    position = HL.get_position(symbol_fut, acc)
                    if position is None:
                        sv.stages[stage_key]['position']['exist'] = False
                        logger.info(f"Position {stage_key} no longer exists; updated sv.stages['first']['exist'] = False")
                    else:
                        sv.stages[stage_key]['position']['exist'] = True
                        sv.stages[stage_key]["position"]["position_info"] = position
                # Рассчитываем промежуточный PnL между обновлениями
                else:
                    entry_price = sv.stages[stage_key]["position"]["position_info"].get("entryPx", 0)
                    size        = sv.stages[stage_key]["position"]["position_info"].get("size", 0)

                    try:
                        unrealized_pnl = (float(last_px) - float(entry_price)) * float(size)
                    except (TypeError, ValueError):
                        unrealized_pnl = 0.0

                    sv.stages[stage_key]["position"]["position_info"]["unrealizedPnl"] = unrealized_pnl

    except Exception as e:
        logger.error(f"Error updating position {stage_key} state: {e}\n\n{traceback.format_exc()}")
        await safe_send('TELEGRAM_API', f'ERROR (REFRESH POSITION INFO): {e}\n\n{traceback.format_exc()}')
                    