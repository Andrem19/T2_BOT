import shared_vars as sv
from datetime import datetime, timezone
from helpers.price_feed import PriceCache
from database.commands_tab import Commands

def secon_dec():
    try:
        if sv.stages['first']['exist'] and not sv.stages['second']['exist']:
            dt = datetime.now(timezone.utc)
            if dt.hour in [20, 21, 22, 23, 0, 1, 2]:
                sv.logger.info("secon_dec: hour is within decision window [20..23,0..2]")
                upper_perc = sv.stages['first']['upper_perc']*0.5
                lower_perc = sv.stages['first']['lower_perc']*0.5
                entry_px = sv.stages['first']['position']['position_info']['entryPx']
                upper_bord = entry_px*(1+upper_perc)
                lower_bord = entry_px*(1-lower_perc)
                current_px = PriceCache.get('BTCUSDT')
                if current_px > upper_bord:
                    sv.logger.info(
                        "secon_dec: current_px > upper_bord -> "
                        "set fut_perc_c=0.80, fut_perc_p=0.20 and return True"
                    )
                    sv.stages['simulation']['fut_perc_c'] = 0.80
                    sv.stages['simulation']['fut_perc_p'] = 0.20
                    return True
                elif current_px < lower_bord:
                    sv.logger.info(
                        "secon_dec: current_px < lower_bord -> "
                        "set fut_perc_c=0.20, fut_perc_p=0.80 and return True"
                    )
                    sv.stages['simulation']['fut_perc_c'] = 0.20
                    sv.stages['simulation']['fut_perc_p'] = 0.80
                    return True
        return False
    except Exception as e:
        sv.logger.exception(f'[secon_dec] {e}')
        return False