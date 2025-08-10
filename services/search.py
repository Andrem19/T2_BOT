import shared_vars as sv
from shared_vars import logger
from datetime import datetime, timezone
from exchanges.bybit_option_hub import BybitOptionHub as BB
from helpers.price_feed import PriceCache, CandleCache
import services.serv as serv
import helpers.tools as tools
from typing import Optional, Dict, Any
from simulation.simulation import simulation
from database.simulation import Simulation
import traceback
import copy
import copy
import talib
import sys

def define_sim_pos(new_simulation: dict):
    sv.stages['simulation']['position_3'] = copy.deepcopy(sv.stages['simulation']['position_2'])
    sv.stages['simulation']['position_2'] = copy.deepcopy(sv.stages['simulation']['position_1'])
    sv.stages['simulation']['position_1'] = copy.deepcopy(new_simulation)
    
    
async def search(which_pos_we_need: str):
    try:
        start = datetime.now().timestamp()
        sv.stages['simulation']['we_need'] = which_pos_we_need
        best_position = {
            'type': 'Put',
            'qty': 0,
            'ask_indicators': [98, 1],
            'symbol': 'ETH-6AUG25-3550-P-USDT',
            'strike_perc': 0.02,
            'p_t': 0.04,
            'lower_perc': 0.0592,
            'upper_perc': 0.03,
            'best_targ_bid': 0,
            'ask': 0.735,
            'ask_original': 14.7,
            'max_amount': 14.6,
            'pnl': -sys.float_info.max
        }
        #==========SEARCHING LOOP=========
        for k, v in sv.instruments.items():
            if k not in sv.symbols:
                continue
            logger.info(f'Start calculation for {k} {which_pos_we_need}')
            

            klines_1h = CandleCache.get(f"{k}USDT", 60)
            last_price = PriceCache.get(f"{k}USDT")
            sv.stages['simulation']['last_px'] = round(last_price, 2)
            atr_last, rel_atr = serv.prepare_atr_and_rel(klines=klines_1h, last_px=last_price, period=24, symbol=f"{k}USDT")
            rsi = talib.RSI(klines_1h[:, 4])
            sv.stages['simulation']['atr'] = [round(atr_last), round(rel_atr, 6)]
            sv.stages['simulation']['rsi'] = round(rsi[-1], 2)
            
            chain = BB.Chain.get_chain_full(underlying=k, days=2, with_greeks=False) or []
            
            h = datetime.now(timezone.utc).hour
            
            day_opt = 0 if h >= 0 and h < 8 else 1
            opt_day_1, _ = tools.get_next_friday_day(day_opt)

            
            filtered_chain_0 = []
            if 'call' in sv.opt_types:
                filtered_chain_calls = tools.filter_otm_options(chain, opt_day_1, 'C', 7)
                filtered_chain_0.extend(filtered_chain_calls)
            if 'put' in sv.opt_types:
                filtered_chain_puts = tools.filter_otm_options(chain, opt_day_1, 'P', 7)
                filtered_chain_0.extend(filtered_chain_puts)

            
            left_to_exp = tools.time_to_expiry(filtered_chain_0[0]['deliveryTime'])
            sv.stages['simulation']['time_to_exp'] = left_to_exp
            
            avg = Simulation.avg_for_current_period_last_days(4)
            sv.stages['simulation']['period_avg_pnl'] = avg['pnl']
            sv.stages['simulation']['period_avg_dist'] = avg['dist']
            print(avg)
            distance = 0.013 if left_to_exp < 10 else 0.016 if left_to_exp < 15 or which_pos_we_need == 'second' else 0.022
            filtered_chain_0 = tools.filter_options_by_distance(filtered_chain_0, distance)
            
            amount_for_est = 1 if which_pos_we_need =='nothing' else sv.stages[which_pos_we_need]['amount']*v['kof']
            for f in filtered_chain_0:
                try:
                    ask_raw, max_qty = BB.Estimator.smart(f['symbol'], qty=amount_for_est, buy=True)
                    ask_val = ask_raw if isinstance(ask_raw, (int, float)) else f.get("askPrice", 0)
                    # print('ask_val', f.get('symbol'), ask_val)
                    f['askPrice'] = float(ask_val)
                    f['askSize'] = float(max_qty)
                except Exception as e:
                    logger.exception(e)
            for o in filtered_chain_0:
                try:
                    symbol = o['symbol']
                    current_px = float(o['underlyingPrice'])
                    strike = float(o['strike'])
                    ask = float(o['askPrice'])
                    mode = o['optionsType'].lower()
                    index_put = tools.index(ask,strike, left_to_exp, current_px, opt_type=mode)
                    ask_indicator = tools.option_ask_indicator(left_to_exp, strike, last_price, ask, mode, rel_atr)
                    
                    diff = 0
                    if mode == 'put':
                        diff = tools.calculate_percent_difference(strike, current_px)
                    else:
                        diff = tools.calculate_percent_difference(current_px, strike)
                    

                    for p_t in sv.perc_t:
                        for p_tp in sv.perc_tp:
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
                            
                            targ_bid = tools.calc_bid(v['bids'], p_t)
                            params = {
                                'lower_perc': sl_pct,
                                'upper_perc': tp_pct,
                                'hours': left_to_exp,
                                'mode': mode,
                            }
                            try:
                                if mode == 'put':
                                    opt_qty = tools.calc_futures_qty(take_profit, target, current_px, ask*v['kof'], (targ_bid-ask)*v['kof'], mode, share_target=0.8)
                                else:
                                    opt_qty = tools.calc_futures_qty(target, take_profit, current_px,(targ_bid-ask)*v['kof'], ask*v['kof'], mode, share_target=0.8)
                            except Exception as e:
                                print(e)
                                continue
                            
                            
                            opt_qty['ask'] = ask*v['kof']
                            opt_qty['p_t'] = p_t
                            
                            stat, pnl, n = simulation(v['data'], opt_qty, params)
                            if pnl > best_position['pnl']:
                                best_position['type'] = mode
                                best_position['qty'] = opt_qty['qty']
                                best_position['ask_indicators'] = [round(index_put, 2), round(ask_indicator, 2)]
                                best_position['name'] = symbol[:3]
                                best_position['symbol'] = symbol
                                best_position['strike_perc'] = diff
                                best_position['p_t'] = p_t
                                best_position['lower_perc'] = params['lower_perc']
                                best_position['upper_perc'] = params['upper_perc']
                                best_position['best_targ_bid'] = targ_bid
                                best_position['pnl_upper'] = opt_qty['pnl_upper']
                                best_position['ask'] = opt_qty['ask']
                                best_position['ask_original'] = ask
                                best_position['max_amount'] = o['askSize']
                                best_position['pnl'] = pnl
                                print(mode, pnl)
                                define_sim_pos(copy.deepcopy(best_position))
                                
                    
                except Exception as e:
                    logger.exception(f'SEARCH INNER LOOP ERROR: {e}\n\n{traceback.format_exc()}')
        await read_and_update_current_utc_hour(sv.stages['simulation']['position_1'])
        speed_sec = (datetime.now().timestamp() - start)
        logger.info(f'speed: {speed_sec}')
    except Exception as e:
        logger.exception(f'SEARCH ERROR: {e}\n\n{traceback.format_exc()}')


async def read_and_update_current_utc_hour(new_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Держим "лучшее" значение в пределах текущего UTC-часа по метрике `pnl` (максимум).
    Поведение:
      - Если записи за день нет — создаётся.
      - Если за текущий час значение отсутствует — пишем new_payload.
      - Если есть — сравниваем old['pnl'] и new['pnl']; сохраняем только если new лучше (больше).
      - Возвращаем предыдущее значение (dict) или None, если его не было.

    Важно:
      - Работает в UTC (дата и час берутся из datetime.now(timezone.utc)).
      - Значения хранятся как JSON-строка; чтение возвращает dict благодаря обвязке Simulation.
    """
    try:
        new_payload = {
            'pnl': new_payload['pnl'],
            'dist': round(new_payload['strike_perc'], 4)
        }
        # 1) UTC-дата и час
        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour  # 0..23

        # 2) Гарантируем суточную запись
        sim = Simulation.get_or_create_for_date(now_utc)

        # 3) Текущее значение за час (dict | None)
        old_value = sim.get_hour(hour)

        # 4) Валидация входа: должен быть dict c числовым new_pnl
        if not isinstance(new_payload, dict):
            sv.logger.warning("read_and_update_current_utc_hour: new_payload is not a dict -> ignored")
            return old_value

        new_pnl_raw = new_payload.get("pnl", None)
        try:
            new_pnl = float(new_pnl_raw) if new_pnl_raw is not None else None
        except (TypeError, ValueError):
            new_pnl = None

        if new_pnl is None:
            sv.logger.warning("read_and_update_current_utc_hour: new_payload['pnl'] is missing or non-numeric -> ignored")
            return old_value

        # 5) Если старого нет — пишем новое
        if old_value is None:
            sim.set_hour(hour, new_payload)   # автосериализация dict -> JSON
            return None  # предыдущее отсутствовало

        # 6) Если старое есть — сравниваем pnl
        old_pnl_raw = old_value.get("pnl", None)
        try:
            old_pnl = float(old_pnl_raw) if old_pnl_raw is not None else None
        except (TypeError, ValueError):
            old_pnl = None

        # Если старый pnl невалидный — считаем, что новое лучше по определению
        if old_pnl is None or new_pnl > old_pnl:
            sim.set_hour(hour, new_payload)   # перезапись на лучшее
            return old_value  # возвращаем прежнее значение (до улучшения)

        # 7) Иначе новое не лучше — ничего не меняем
        return old_value

    except Exception as e:
        sv.logger.exception(f"ERROR: (read_and_update_current_utc_hour) {e}\n\n{traceback.format_exc()}")
        return None
