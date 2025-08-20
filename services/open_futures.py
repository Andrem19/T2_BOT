from exchanges.hyperliquid_api import HL
import shared_vars as sv
from shared_vars import logger
from helpers.safe_sender import safe_send
import asyncio
import services.serv as serv
import helpers.tools as tools
import traceback
from helpers.price_feed import PriceCache

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
        currentPx = PriceCache.get(symbol)
        pnl_target = max(position['pnl_upper'], position['pnl_lower'])
        sv.stages[which_pos_we_need]['position']['pnl_target'] = (pnl_target*0.96)*sv.stages[which_pos_we_need]['amount']
        new_position = {}
        side = 'Buy' if position['type'] == 'put' else 'Sell'
        tp_perc = position['upper_perc'] if side == 'Buy' else position['lower_perc']
        fut_full_amt = (position['qty']*sv.stages[which_pos_we_need]['amount'])
        fut_amt = tools.qty_for_target_profit(currentPx, tp_perc, position['ask']*sv.stages[which_pos_we_need]['amount']*1.15)
        second_stage_qty = fut_full_amt - fut_amt
        if second_stage_qty < 0.001 or not sv.partial_pos:
            fut_amt = fut_full_amt

        new_position = HL.open_limit_post_only(symbol, side, 0, False, fut_amt, acc, max_wait_cycles=4)
        
        if new_position and abs(new_position.get('size', 0)) > 0:
            logger.info(f"Futures position {symbol} - {fut_amt} opened.")
            sv.stages[which_pos_we_need]['exist'] = True
            sv.stages[which_pos_we_need]['base_coin'] = position['name']
            sv.stages[which_pos_we_need]['position']['exist'] = True
            sv.stages[which_pos_we_need]['position']['position_info'] = new_position
            sv.stages[which_pos_we_need]['position']['fut_full_amt'] = fut_full_amt
                
            entry_px = float(new_position['entryPx'])
            size_px  = float(new_position['size'])
            
            sl_px = position['lower_perc'] if size_px > 0 else position['upper_perc']
            
            HL.open_SL_position(symbol, side, entry_px, sl_px+0.001, acc)
            logger.info(f"{which_pos_we_need} Stop Loss opened.")
            
            tp_px = position['upper_perc'] if size_px > 0 else position['lower_perc']
            
            HL.open_TP_position(symbol, side, entry_px, tp_px+0.001, acc)
            logger.info(f"{which_pos_we_need} Take Profit opened.")
            
            if second_stage_qty >= 0.001 and sv.partial_pos:
                need_dist = position['lower_perc']*0.30 if side == 'Buy' else position['upper_perc']*0.30
                
                add_price = entry_px * (1+need_dist) if side == 'Sell' else entry_px * (1-need_dist)
                sv.stages[which_pos_we_need]['position']['second_stage_px'] = add_price
                HL.place_limit_post_only(symbol, side, add_price, 0, second_stage_qty, False, acc)
                logger.info(f"{which_pos_we_need} add position order is opened")
            else:
                sv.stages[which_pos_we_need]['position']['second_stage_px'] = 0
                logger.info(f"{which_pos_we_need} quantity to small for add order {second_stage_qty} partial_pos is {sv.partial_pos}")
            
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
    