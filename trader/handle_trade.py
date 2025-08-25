from exchanges.hyperliquid_api import HL
import trader.take_position as tp
from datetime import datetime, timezone
from database.signaler import Signaler
import trader.xsignal as xs

async def trade_logic():
    dt = datetime.now(timezone.utc)
    signaler = Signaler.get_instance()
    if dt.minute in [1, 2] and signaler.refresh_time + 120 > dt.timestamp():
        position = HL.get_position('BTCUSDT', 1)
        signal = xs.get_signal(signaler)
        if not position:
            if signal in [1, 2]:
                await tp.open_position(signal)
        else:
            if position['size'] < 0 and signal == 2:
                return
            elif position['size'] > 0 and signal == 1:
                return
            else:
                await tp.close_position()