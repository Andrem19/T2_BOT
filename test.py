import helpers.tools as tools
import asyncio
import sys
from exchanges.bybit_option_hub import BybitOptionHub as BB
from exchanges.deribit_option_hub import DeribitOptionHub as DB
import shared_vars as sv
from datetime import datetime
from heapq import nsmallest
from simulation.load_data import load_candles
from simulation.simulation import simulation


START_DATE  = "01-01-2024"
END_DATE    = "01-01-2025"

bids_sol = {
    0.01: 2.0,
    0.025: 3.7,
    0.045: 5.0
}
bids_eth = {
    0.01: 45,
    0.025: 84,
    0.045: 145
}
bids_btc = {
    0.01: 1100,
    0.025: 2700,
    0.045: 4700
}

perc_t = [0.025, 0.03, 0.04, 0.05]
perc_tp = [0.02, 0.025, 0.03, 0.04]



async def main():
    data_eth = load_candles(path=sv.data_path_eth, start_date=START_DATE, end_date=END_DATE)
    BB.initialise(testnet=False)
    chain = BB.Chain.get_chain_full(underlying='ETH', days=2, with_greeks=False)
    
    print(len(chain))
    print(chain[0])
    
    h = datetime.now().hour
    day_opt = 0 if h >= 0 and h < 8 else 1
    opt_day_1, _ = tools.get_next_friday_day(day_opt)

    filtered_chain_0 = tools.filter_otm_options(chain, opt_day_1, 'C', 6)
    print(1, len(filtered_chain_0))
    filtered_chain_0 = tools.filter_options_by_distance(filtered_chain_0, 0.03)
    print(2, len(filtered_chain_0))

    left_to_exp = tools.time_to_expiry(filtered_chain_0[0]['deliveryTime'])
    
    best_pnl = -sys.float_info.max
    best_diff = 0
    best_params = {}
    best_opt_qty = {}
    best_stat = {}
    
    for o in filtered_chain_0:
        symbol = o['symbol']
        current_px = float(o['underlyingPrice'])
        strike = float(o['strike'])
        ask = float(o['askPrice'])
        mode = o['optionsType'].lower()
        index_put = tools.index(ask,strike,left_to_exp,current_px, opt_type=mode)
        
        if mode == 'put':
            diff = tools.calculate_percent_difference(strike, current_px)
        else:
            diff = tools.calculate_percent_difference(current_px, strike)
        

        for p_t in perc_t:
            for p_tp in perc_tp:
                print('='*24)
                take_profit = 0
                target = 0
                if mode == 'put':
                    take_profit = current_px * (1+p_tp)
                    target = strike * (1-p_t)
                    tp_pct, sl_pct = tools.calc_tp_sl_pct(current_px, take_profit, target)
                elif mode == 'call':
                    take_profit = current_px * (1-p_tp)
                    target = strike * (1+p_t)
                    tp_pct, sl_pct = tools.calc_tp_sl_pct(current_px, target, take_profit)
                
                targ_bid = tools.calc_bid(bids_eth, p_t)
                params = {
                    'lower_perc': sl_pct,
                    'upper_perc': tp_pct,
                    'hours': left_to_exp,
                    'mode': mode,
                }
                try:
                    if mode == 'put':
                        opt_qty = tools.calc_futures_qty(take_profit, target, current_px, ask*0.1, (targ_bid-ask)*0.1, mode, share_target=0.7)
                    else:
                        opt_qty = tools.calc_futures_qty(target, take_profit, current_px,(targ_bid-ask)*0.1, ask*0.1, mode, share_target=0.7)
                except Exception as e:
                    continue
                
                
                opt_qty['ask'] = ask*0.1
                opt_qty['p_t'] = p_t
                
                stat, pnl, n = simulation(data_eth, opt_qty, params)
                if pnl > best_pnl:
                    best_pnl = pnl
                    best_diff = diff
                    best_params = params
                    best_opt_qty = opt_qty
                    best_stat = stat
                    print('SYMBOL: ', symbol)
                    print('INDEX: ', index_put)
                    print('DIFF: ', best_diff)
                    print('ASK: ', best_opt_qty['ask'])
                    print('LOWER_PERC: ', best_params['lower_perc'])
                    print('UPPER_PERC: ', best_params['upper_perc'])
                    print(best_opt_qty)
                    print('PNL: ', best_pnl)
                    print(best_stat)
                    
    print('SYMBOL: ', symbol)
    print('INDEX: ', index_put)
    print('DIFF: ', best_diff)
    print('ASK: ', best_opt_qty['ask'])
    print('LOWER_PERC: ', best_params['lower_perc'])
    print('UPPER_PERC: ', best_params['upper_perc'])
    print(best_opt_qty)
    print('PNL: ', best_pnl)
    print(best_stat)
    


if __name__ == "__main__":
    asyncio.run(main())
    
    
# Position
# [{'symbol': 'ETH-6AUG25-3525-P-USDT', 'leverage': '', 'autoAddMargin': 0, 'avgPrice': '6.1', 
# 'liqPrice': '', 'delta': '-0.010189780', 'riskLimitValue': '', 'takeProfit': '', 'theta': '-1.075580960', 
# 'positionValue': '0.59364515', 'isReduceOnly': False, 'positionIMByMp': '', 'tpslMode': 'Full', 
# 'riskId': 0, 'trailingStop': '', 'unrealisedPnl': '-0.01635484', 'markPrice': '5.93645152', 
# 'adlRankIndicator': 0, 'cumRealisedPnl': '', 'positionMM': '0', 'createdTime': '1754389716600', 
# 'positionIdx': 0, 'positionIM': '0', 'positionMMByMp': '', 'seq': 40001503367, 'updatedTime': 
# '1754389793845', 'side': 'Buy', 'bustPrice': '', 'positionBalance': '', 'leverageSysUpdatedTime': '', 
# 'curRealisedPnl': '-0.0427', 'size': '0.1', 'positionStatus': 'Normal', 'mmrSysUpdatedTime': '', 
# 'stopLoss': '', 'tradeMode': 0, 'sessionAvgPrice': '', 'gamma': '0.000131746', 'vega': '0.032370268'}]