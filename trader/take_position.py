from exchanges.hyperliquid_api import HL
from shared_vars import logger
from helpers.safe_sender import safe_send

async def open_position(signal: int):
    side = 'Buy' if signal == 1 else 'Sell' if signal == 2 else 0
    if signal == 0:
        return
    coin = 'BTCUSDT'
    new_position = HL.open_limit_post_only(coin, side, 0, False, 0.001, 1, max_wait_cycles=4)
    if new_position:
        entry_px = float(new_position['entryPx'])
        sl_px = 0.0065
        
        HL.open_SL_position(coin, side, entry_px, sl_px+0.001, 1)
        logger.info(f"Trade Stop Loss opened.")
        
        tp_px = 0.0060
        
        HL.open_TP_position(coin, side, entry_px, tp_px+0.001, 1)
        logger.info(f"Trade Take Profit opened.")
        await safe_send("TELEGRAM_API", f'Trade position {side} was placed', '', False)

async def close_position():
    HL.close_position_post_only('BTCUSDT', 1)
    await safe_send("TELEGRAM_API", f'Trade position was closed', '', False)