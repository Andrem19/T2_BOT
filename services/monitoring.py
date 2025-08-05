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
            return
        
        #========PNL TARGET=========
        pnl_target = sv.stages[pos_name]['position']['pnl_target']
        tot_pnl = sv.stages[pos_name]['position']['position_info'].get('unrealizedPnl', 0) + sv.stages[pos_name]['position']['leg']['info'].get('unrealisedPnl', 0)
        if pnl_target > 0 and tot_pnl >= pnl_target:
            await close.close_position_fully(pos_name=pos_name, reason='pnl_hit', last_px=last_px)
            return


        #=========ELSE=============
        fut_size = sv.stages[pos_name]['position']["position_info"].get("size", 0)
        entry_px = sv.stages[pos_name]['position']["position_info"].get("entryPx", 0)

        # уровни (из params & entry_px; если entry=0, будут мусорные, но мы защитимся)
        tp_lvl = entry_px * (1+sv.stages[pos_name]["upper_perc"])
        sl_lvl = entry_px * (1-sv.stages[pos_name]["lower_perc"])

        triggered_tp = (entry_px != 0) and (last_px is not None) and (last_px >= tp_lvl)
        triggered_sl = (entry_px != 0) and (last_px is not None) and (last_px <= sl_lvl)
        fut_gone = not sv.stages[pos_name]['position']['exist']
        opt_gone = not sv.stages[pos_name]['position']['leg']['exist']

        if triggered_tp or triggered_sl or opt_gone or fut_gone:
            reason = "tp_hit" if triggered_tp else "sl_hit" if triggered_sl else 'no_opt' if opt_gone else 'no_fut'
            logger.info(
                "%s EXIT condition (%s). last_px=%s entry=%s tp=%s sl=%s fut_size=%s",
                pos_name, reason, last_px, entry_px, tp_lvl, sl_lvl, fut_size,
            )
            await close.close_position_fully(pos_name=pos_name, reason=reason, last_px=last_px)
            return
    except Exception as e:
        await safe_send("TELEGRAM_API", f'PROCCESS POSITION ERROR: {e}\n\n{traceback.format_exc()}', '', False)
        logger.error(f'{e}')