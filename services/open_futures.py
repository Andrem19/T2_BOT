from exchanges.hyperliquid_api import HL
import shared_vars as sv
from shared_vars import logger
from helpers.safe_sender import safe_send
import helpers.tools as ts
import asyncio
import traceback

async def open_futures(position: dict, which_pos_we_need: str):
    try:
        try:
            sv.stages[which_pos_we_need]['lower_perc'] = position['lower_perc']
            sv.stages[which_pos_we_need]['upper_perc'] = position['upper_perc']
            sv.stages[which_pos_we_need]['over_strike'] = position['p_t']
            sv.stages[which_pos_we_need]['to_exp_init'] = sv.stages['simulation']['time_to_exp']
            sv.stages[which_pos_we_need]['rel_atr'] = sv.stages['simulation']['atr'][1]
            sv.stages[which_pos_we_need]['rsi'] = sv.stages['simulation']['rsi']
        except Exception as e:
            logger.exception(f'ERROR open_futures define variables in dict {e}')
        
        
        acc = 2 if which_pos_we_need =='first' else 1
        symbol = position['name']+'USDT'
        sv.stages[which_pos_we_need]['position']['pnl_target'] = (position['pnl_upper']*0.95)*sv.stages[which_pos_we_need]['amount']
        new_position = {}
        
        fut_amt = position['qty']*sv.stages[which_pos_we_need]['amount']
        
        side = 'Buy' if position['type'] == 'put' else 'Sell'
        HL.open_market_order(symbol, side, 0, False, fut_amt, acc)
        for _ in range(3):
            new_position = HL.get_position(symbol, acc)
            if new_position:
                break
            await asyncio.sleep(4)
        
        #==============SECOND TRY================
        if not new_position:
            HL.cancel_all_orders()
            await asyncio.sleep(10)
            new_position = HL.get_position(symbol, acc)
            if not new_position:
                HL.open_market_order(symbol, side, 0, False, fut_amt, acc)
                for _ in range(4):
                    new_position = HL.get_position(symbol, acc)
                    if new_position:
                        break
                    await asyncio.sleep(4)
        
        if new_position and abs(new_position.get('size', 0)) > 0:
            logger.info(f"Futures position {symbol} - {fut_amt} opened.")
            sv.stages[which_pos_we_need]['exist'] = True
            sv.stages[which_pos_we_need]['base_coin'] = position['name']
            sv.stages[which_pos_we_need]['position']['exist'] = True
            sv.stages[which_pos_we_need]['position']['position_info'] = new_position
            
            entry_px = float(new_position['entryPx'])
            size_px  = float(new_position['size'])
            
            sl_px = position['lower_perc'] if size_px > 0 else position['upper_perc']
            
            HL.open_SL(symbol, side, abs(size_px), entry_px, sl_px, acc)
            logger.info("P1 Stop Loss opened.")
            
            tp_px = position['upper_perc'] if size_px > 0 else position['lower_perc']
            
            HL.open_TP(symbol, side, abs(size_px), entry_px, tp_px, acc)
            logger.info("P1 Take Profit opened.")
            
            open_time = HL.get_open_time(symbol, acc)
            sv.stages[which_pos_we_need]['position']['open_time'] = open_time

            return True
        else:
            #ALARM
            await safe_send("TELEGRAM_API", f'OPEN_FUT: The option is open but FUTURES fall. WARNING: Options remain standalone!!!\n\nNEED to {side} qty: {fut_amt}', '', False)
            await asyncio.sleep(1)
            await safe_send("TELEGRAM_API", f'OPEN_FUT: The option is open but FUTURES fall. WARNING: Options remain standalone!!!\n\nNEED to {side} qty: {fut_amt}', '', False)
            await asyncio.sleep(1)
            await safe_send("TELEGRAM_API", f'OPEN_FUT: The option is open but FUTURES fall. WARNING: Options remain standalone!!!\n\nNEED to {side} qty: {fut_amt}', '', False)
            return False
    except Exception as e:
        logger.exception(f'OPEN FUTURES ERROR: {e}\n\n{traceback.format_exc()}')
        return False
    