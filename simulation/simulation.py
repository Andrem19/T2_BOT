import shared_vars as sv

import simulation.run as run
import simulation.st_calc as st_calc



def simulation(data, fut_calc, params: dict):
    run.run_logic(data, fut_calc, params)
    
    if len(sv.data_list) <2:
        sv.data_list.clear()
        return {}, 0, 0
    
    stats = st_calc.build_stats(sv.data_list)
    best_stats = st_calc.print_stats(stats, False)


    sv.data_list.clear()
    
    return best_stats, stats['pnl_per_day'], stats['trades_total']