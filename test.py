import helpers.tools as tools
import asyncio
import sys
from exchanges.bybit_option_hub import BybitOptionHub as BB
from exchanges.deribit_option_hub import DeribitOptionHub as DB
import shared_vars as sv
from datetime import datetime, timezone
from exchanges.hyperliquid_api import HL
from heapq import nsmallest
from simulation.load_data import load_candles
from simulation.simulation import simulation
from helpers.safe_sender import safe_send
from database.simple_orm import initialize
import services.serv as serv
import helpers.tlg as tlg
from helpers.metrics import analyze_option_slice, pic_best_opt
from database.hist_trades import Trade
from database.simulation import Simulation
from commander.service import format_trades_report
from metrics.hourly_scheduler import start_hourly_57_scheduler
from openai import OpenAI
from decouple import config
import json
import re

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


#!/usr/bin/env python3
# example_collect_news.py
# Минимальный запуск: собрать и распечатать новости

from metrics.market_watch import collect_news

# async def main() -> None:
#     api = config('DEEPSEEK_API')
#     client = OpenAI(api_key=api, base_url="https://api.deepseek.com")
#     # Собираем новости с дефолтных лент (crypto + finance), горизонтом 24ч
#     news = collect_news(
#         horizon_hours=24,          # можно изменить при желании
#         max_items_per_feed=40,     # ограничение на каждый фид
#         max_summary_chars=260      # длина краткого описания
#     )

#     # Печатаем
#     print(f"Всего новостей: {len(news)}\n")
#     # for i, n in enumerate(news, 1):
#     #     print(f"{i:03d}. [{n.category}/{n.source}] {n.title}")
#     #     print(f"     Время:  {n.published_utc}")
#     #     print(f"     Кратко: {n.summary_short}")
#     #     print(f"     Ссылка: {n.url}\n")
    
#     dt = datetime.now(timezone.utc)
#     prompt = f'''Your task is to analyze the list of these news items. 
#     Each news item should be assigned a weight on a seven-point scale from 1 to 7 depending on how important it is for the next few hours 2-5 hours in terms of its impact on the cryptocurrency market. 
#     Also look at how long ago it was released. Today is now {dt}. After that, analyze each news item, is it positive for BTC growth or negative, and sum up all the points, plus if positive, minus if negative, and finally give me the final assessment in the format json "score": val '''
#     response = client.chat.completions.create(
#         model="deepseek-reasoner",
#         messages=[
#             {"role": "system", "content": "You are a financial analyst of crypto markets"},
#             {"role": "user", "content": f"{prompt}\n\n{str(news)}"},
#         ],
#         stream=False
#     )
#     print(response)
#     # 1. Достаем основной текстовый ответ ассистента (content)
#     main_content = response.choices[0].message.content
#     print("ОТВЕТ АССИСТЕНТА:")
#     print(main_content)
#     print("-" * 50)
        
#     reasoning_content = response.choices[0].message.reasoning_content
#     print("РАССУЖДЕНИЯ МОДЕЛИ:")
#     print(reasoning_content)
#     print("-" * 50)

#     # Ищем блок кода, обрамленный ```json ... ```
#     json_match = re.search(r'```json\s*(.*?)\s*```', main_content, re.DOTALL)
#     if json_match:
#         json_str = json_match.group(1)
#         try:
#             data = json.loads(json_str)
#             final_score = data['score']
#             print(f"ФИНАЛЬНЫЙ SCORE: {final_score}")
#         except json.JSONDecodeError as e:
#             print(f"Ошибка парсинга JSON: {e}")
#             final_score = None
#     else:
#         print("JSON блок не найден в ответе.")
#         final_score = None

#     print("-" * 50)

async def main():
#     api = config('DEEPSEEK_API')
#     client = OpenAI(api_key=api, base_url="https://api.deepseek.com")

#     response = client.chat.completions.create(
#         model="deepseek-reasoner",
#         messages=[
#             {"role": "system", "content": "You are a helpful assistant"},
#             {"role": "user", "content": "Hello"},
#         ],
#         stream=False
#     )

#     print(response.choices[0].message.content)
    start_hourly_57_scheduler(
        metrics_filename="metrics.json",
        allow_overlap=False,
        warn_after_sec=240,
        worker_max_workers=1
    )
    while True:
       await asyncio.sleep(2)
    # fut_full_amt = (0.005*2)
    # fut_amt = tools.qty_for_target_profit(115440, 0.0153, 6.15*2*1.10)
    # second_stage_qty = fut_full_amt - fut_amt
    # print(fut_full_amt, fut_amt, second_stage_qty)
    # pos = HL.place_limit_post_only('BTCUSDT', 'Buy', 113200, 0, 0.001, False, 1)
    # print(pos)
    # res = HL.open_SL_position('BTCUSDT', 'Buy', 113961, 0.011, 1)
    # res = HL.open_TP_position('BTCUSDT', 'Buy', 113961, 0.011, 1)
    # print(res)
    # BB.initialise()
    # chain = BB.Chain.get_chain_full(underlying='BTC', days=2, with_greeks=True) or []
    # opt_day_1, _ = tools.get_next_friday_day(1)
    # filtered_chain_0 = []

    # filtered_chain_calls = tools.filter_otm_options(chain, opt_day_1, 'C', 7)
    # filtered_chain_0.extend(filtered_chain_calls)

    # filtered_chain_puts = tools.filter_otm_options(chain, opt_day_1, 'P', 7)
    # filtered_chain_0.extend(filtered_chain_puts)
    # res = analyze_option_slice(filtered_chain_0)
    # opt = pic_best_opt(res)
    # print(opt)
    # res = HL.open_TP('BTCUSDT', 'Sell', 0.00366, 119826, 0.02)
    # print(res)
    # res = tools.qty_for_target_profit(120000, 0.01, 4)
    # print(res)
    # h = datetime.now(timezone.utc).hour
    # print(h)
    # BB.initialise(testnet=False)
    # bybit_bal_1 = BB.Trading.get_wallet_balance(account_idx=2)
    # print(bybit_bal_1['result']['list'][0]['totalEquity'])
    # initialize("tbot.db")
    # hist = Trade.last_n_days(2)
    # hist_dicts = [vars(obj) for obj in hist]
    # result = format_trades_report(hist_dicts)
    # today_dict = Simulation.full_day_dict_by_offset(0)
    # pretty_str = tools.dict_to_pretty_string(today_dict)
    # print(pretty_str)
    # print(datetime.now().hour )
    # test_dict = {
    #     'strike_perc': 0.01,
    #     'pnl': 0.44,
    #     'upper_perc': 0.05
    # }
    # msg = serv.format_option_message_html(test_dict)
    # await tlg.send_option_message('COLLECTOR_API', msg, '', False)
    # data_eth = load_candles(path=sv.data_path_eth, start_date=START_DATE, end_date=END_DATE)
    # BB.initialise(testnet=False)
    # chain = BB.Chain.get_chain_full(underlying='ETH', days=2, with_greeks=False)
    
    # print(len(chain))
    # print(chain[0])
    
    # h = datetime.now().hour
    # day_opt = 0 if h >= 0 and h < 8 else 1
    # opt_day_1, _ = tools.get_next_friday_day(day_opt)

    # filtered_chain_0 = tools.filter_otm_options(chain, opt_day_1, 'C', 6)
    # print(1, len(filtered_chain_0))
    # filtered_chain_0 = tools.filter_options_by_distance(filtered_chain_0, 0.03)
    # print(2, len(filtered_chain_0))

    # left_to_exp = tools.time_to_expiry(filtered_chain_0[0]['deliveryTime'])
    
    # best_pnl = -sys.float_info.max
    # best_diff = 0
    # best_params = {}
    # best_opt_qty = {}
    # best_stat = {}
    
    # for o in filtered_chain_0:
    #     symbol = o['symbol']
    #     current_px = float(o['underlyingPrice'])
    #     strike = float(o['strike'])
    #     ask = float(o['askPrice'])
    #     mode = o['optionsType'].lower()
    #     index_put = tools.index(ask,strike,left_to_exp,current_px, opt_type=mode)
        
    #     if mode == 'put':
    #         diff = tools.calculate_percent_difference(strike, current_px)
    #     else:
    #         diff = tools.calculate_percent_difference(current_px, strike)
        

    #     for p_t in perc_t:
    #         for p_tp in perc_tp:
    #             print('='*24)
    #             take_profit = 0
    #             target = 0
    #             if mode == 'put':
    #                 take_profit = current_px * (1+p_tp)
    #                 target = strike * (1-p_t)
    #                 tp_pct, sl_pct = tools.calc_tp_sl_pct(current_px, take_profit, target)
    #             elif mode == 'call':
    #                 take_profit = current_px * (1-p_tp)
    #                 target = strike * (1+p_t)
    #                 tp_pct, sl_pct = tools.calc_tp_sl_pct(current_px, target, take_profit)
                
    #             targ_bid = tools.calc_bid(bids_eth, p_t)
    #             params = {
    #                 'lower_perc': sl_pct,
    #                 'upper_perc': tp_pct,
    #                 'hours': left_to_exp,
    #                 'mode': mode,
    #             }
    #             try:
    #                 if mode == 'put':
    #                     opt_qty = tools.calc_futures_qty(take_profit, target, current_px, ask*0.1, (targ_bid-ask)*0.1, mode, share_target=0.7)
    #                 else:
    #                     opt_qty = tools.calc_futures_qty(target, take_profit, current_px,(targ_bid-ask)*0.1, ask*0.1, mode, share_target=0.7)
    #             except Exception as e:
    #                 continue
                
                
    #             opt_qty['ask'] = ask*0.1
    #             opt_qty['p_t'] = p_t
                
    #             stat, pnl, n = simulation(data_eth, opt_qty, params)
    #             if pnl > best_pnl:
    #                 best_pnl = pnl
    #                 best_diff = diff
    #                 best_params = params
    #                 best_opt_qty = opt_qty
    #                 best_stat = stat
    #                 print('SYMBOL: ', symbol)
    #                 print('INDEX: ', index_put)
    #                 print('DIFF: ', best_diff)
    #                 print('ASK: ', best_opt_qty['ask'])
    #                 print('LOWER_PERC: ', best_params['lower_perc'])
    #                 print('UPPER_PERC: ', best_params['upper_perc'])
    #                 print(best_opt_qty)
    #                 print('PNL: ', best_pnl)
    #                 print(best_stat)
                    
    # print('SYMBOL: ', symbol)
    # print('INDEX: ', index_put)
    # print('DIFF: ', best_diff)
    # print('ASK: ', best_opt_qty['ask'])
    # print('LOWER_PERC: ', best_params['lower_perc'])
    # print('UPPER_PERC: ', best_params['upper_perc'])
    # print(best_opt_qty)
    # print('PNL: ', best_pnl)
    # print(best_stat)
    


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