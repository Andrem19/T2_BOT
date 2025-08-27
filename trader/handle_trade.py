from exchanges.hyperliquid_api import HL
import trader.take_position as tp
from datetime import datetime, timezone
from database.signaler import Signaler
import trader.xsignal as xs
from shared_vars import logger

async def trade_logic():
    logger.info(f"Start trade logic.")
    dt = datetime.now(timezone.utc)
    signaler = Signaler.get_instance()
    if dt.minute in [1, 2] and signaler.refresh_time + 120 > dt.timestamp():
        logger.info(f"In action trade logic.")
        position = HL.get_position('BTCUSDT', 1)
        signal = xs.get_signal(signaler)
        if not position:
            if signal in [1, 2]:
                await tp.open_position(signal)
        else:
            if position['size'] < 0 and signal in [2, 0]:
                return
            elif position['size'] > 0 and signal in [1, 0]:
                return
            else:
                await tp.close_position()