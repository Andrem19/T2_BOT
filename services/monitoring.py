import shared_vars as sv
from shared_vars import logger
import services.close_position as close
from helpers.safe_sender import safe_send
import traceback
    
    
async def process_position(last_px: float, pos_name: str): 
    try:
        #=======COMMAND============
        close_command = sv.close_1 if pos_name == 'first' else sv.close_2
        if close_command:
            await close.close_position_fully(pos_name=pos_name, reason='close_command', last_px=last_px)
            return True
        
        #========PNL TARGET=========
        pnl_target = sv.stages[pos_name]['position']['pnl_target']*1.15
        tot_pnl = sv.stages[pos_name]['position']['position_info'].get('unrealizedPnl', 0) + sv.stages[pos_name]['position']['leg']['info'].get('unrealisedPnl', 0)
        if pnl_target > 0 and tot_pnl >= pnl_target:
            flag = '_fut' if sv.stages[pos_name]['position']['position_info'].get('unrealizedPnl', 0) > 0 and sv.stages[pos_name]['position']['leg']['info'].get('unrealisedPnl', 0) < 0 else '_opt'
            await close.close_position_fully(pos_name=pos_name, reason=f'pnl_hit{flag}', last_px=last_px)
            return True


        #=========ELSE=============
        fut_size = sv.stages[pos_name]['position']["position_info"].get("size", 0)
        entry_px = sv.stages[pos_name]['position']["position_info"].get("entryPx", 0)

        # уровни (из params & entry_px; если entry=0, будут мусорные, но мы защитимся)
        up_lvl = entry_px * (1+sv.stages[pos_name]["upper_perc"])
        lv_lvl = entry_px * (1-sv.stages[pos_name]["lower_perc"])
        
        
        triggered_tp = None
        triggered_sl = None
        
        fut_gone = not sv.stages[pos_name]['position']['exist'] and sv.stages[pos_name]['position']["position_info"]
        
        
        opt_type = sv.stages[pos_name]['position']['leg']['name'][-6]
        
        if opt_type == 'P':
            triggered_tp = (entry_px != 0) and (last_px is not None) and (last_px >= up_lvl)
            triggered_sl = (entry_px != 0) and (last_px is not None) and (last_px <= lv_lvl)
        else:
            triggered_tp = (entry_px != 0) and (last_px is not None) and (last_px <= lv_lvl)
            triggered_sl = (entry_px != 0) and (last_px is not None) and (last_px >= up_lvl)
                        
        opt_gone = not sv.stages[pos_name]['position']['leg']['exist']

        if triggered_tp or triggered_sl or opt_gone or fut_gone:
            
            flag = '_tp' if triggered_tp and not triggered_sl else '_sl' if triggered_sl and not triggered_tp else '_none'
            
            reason = "tp_hit" if triggered_tp else "sl_hit" if triggered_sl else 'no_opt' if opt_gone else f'no_fut{flag}'
            
            logger.info(
                "%s EXIT condition (%s). last_px=%s entry=%s up_lvl=%s lv_lvl=%s fut_size=%s",
                pos_name, reason, last_px, entry_px, up_lvl, lv_lvl, fut_size,
            )
            await close.close_position_fully(pos_name=pos_name, reason=reason, last_px=last_px)
            return True
        return False
    except Exception as e:
        await safe_send("TELEGRAM_API", f'PROCCESS POSITION ERROR: {e}\n\n{traceback.format_exc()}', '', False)
        logger.error(f'{e}')