from exchanges.hyperliquid_api import HL
import shared_vars as sv
from shared_vars import logger
from helpers.safe_sender import safe_send
import asyncio
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
        sv.stages[which_pos_we_need]['position']['pnl_target'] = (position['pnl_upper']*0.95)*sv.stages[which_pos_we_need]['amount']
        new_position = {}
        side = 'Buy' if position['type'] == 'put' else 'Sell'
        tp_perc = position['upper_perc'] if side == 'Buy' else position['lower_perc']
        fut_full_amt = (position['qty']*sv.stages[which_pos_we_need]['amount'])
        fut_amt = tools.qty_for_target_profit(currentPx, tp_perc, position['ask']*sv.stages[which_pos_we_need]['amount']*1.10)
        second_stage_qty = fut_full_amt - fut_amt
        
        
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
            sv.stages[which_pos_we_need]['position']['second_taken'] = False
            if second_stage_qty > 0:
                sv.stages[which_pos_we_need]['position']['second_stage_qty'] = second_stage_qty
            
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

async def second_stage_check(which_pos_we_need: str):
    try:
        if sv.stages[which_pos_we_need]['position']['second_taken']:
            return
        acc = 2 if which_pos_we_need =='first' else 1
        symbol = sv.stages[which_pos_we_need]['base_coin'] + 'USDT'
        size = sv.stages[which_pos_we_need]['position']['position_info']['size']
        entryPx = sv.stages[which_pos_we_need]['position']['position_info']['entryPx']
        current_px = PriceCache.get(symbol)
        
        side=''
        need_to_add = False
        lower_perc = sv.stages[which_pos_we_need]['lower_perc']
        upper_perc = sv.stages[which_pos_we_need]['upper_perc']
        if size > 0:
            need_dist = lower_perc*0.40
            side = 'Buy'
            need_to_add = current_px < entryPx and abs((current_px-entryPx)/current_px) > need_dist
        else:
            need_dist = upper_perc*0.40
            side = 'Sell'
            need_to_add = current_px > entryPx and abs((current_px-entryPx)/current_px) > need_dist
        if need_to_add:
            qty = sv.stages[which_pos_we_need]['position']['second_stage_qty']
            if qty>0:
                await open_fut_sec(which_pos_we_need, symbol, qty, acc, side, lower_perc, upper_perc, entryPx)
                sv.stages[which_pos_we_need]['position']['second_taken'] = True
    except Exception as e:
        await safe_send("TELEGRAM_API", f'second_stage_check ERROR: {e}', '', False)
        
        
        
async def open_fut_sec(which_pos_we_need, symbol, size, acc, side, lower_perc, upper_perc, oldEntryPx):
    try:
        HL.open_market_order(symbol, side, 0, False, size, acc)
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
                HL.open_market_order(symbol, side, 0, False, size, acc)
                for _ in range(4):
                    new_position = HL.get_position(symbol, acc)
                    if new_position:
                        break
                    await asyncio.sleep(4)
        
        if new_position and abs(new_position.get('size', 0)) > 0:
            HL.cancel_all_orders()
            await asyncio.sleep(1)
            logger.info(f"Futures position {symbol} - {size} opened.")
            sv.stages[which_pos_we_need]['position']['position_info'] = new_position
            
            entry_px = oldEntryPx
            size_px  = float(new_position['size'])
            
            sl_px = lower_perc if size_px > 0 else upper_perc
            
            HL.open_SL(symbol, side, abs(size_px), entry_px, sl_px, acc)
            logger.info("P1 Stop Loss add opened.")
            
            tp_px = upper_perc if size_px > 0 else lower_perc
            
            HL.open_TP(symbol, side, abs(size_px), entry_px, tp_px, acc)
            logger.info("P1 Take Profit add opened.")
            
            open_time = HL.get_open_time(symbol, acc)
            sv.stages[which_pos_we_need]['position']['open_time'] = open_time

            return True
        else:
            #ALARM
            await safe_send("TELEGRAM_API", f'OPEN_FUT_ADD: The option is open but ADD_FUTURES fall. WARNING: Options remain standalone!!!\n\nNEED to {side} qty: {fut_amt}', '', False)
            await asyncio.sleep(1)
            await safe_send("TELEGRAM_API", f'OPEN_FUT_ADD: The option is open but ADD_FUTURES fall. WARNING: Options remain standalone!!!\n\nNEED to {side} qty: {fut_amt}', '', False)
            await asyncio.sleep(1)
            await safe_send("TELEGRAM_API", f'OPEN_FUT_ADD: The option is open but ADD_FUTURES fall. WARNING: Options remain standalone!!!\n\nNEED to {side} qty: {fut_amt}', '', False)
            return False
    except Exception as e:
        logger.exception(f'OPEN ADD_FUTURES ERROR: {e}\n\n{traceback.format_exc()}')
        return False
        
    