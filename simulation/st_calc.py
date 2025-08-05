import statistics
import math
from typing import List, Dict, Any
from collections import Counter

def build_stats(
    results: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Собирает статистику по списку результатов позиций и автоматически
    вычисляет период по полю 'start_timestamp_ms' в первой и последней записях:
      • общее число сделок,
      • средние/медианные/σ-значения по длительности и PnL,
      • breakdown по всем встречающимся значениям 'breakout',
      • среднее число позиций в день,
      • средний PnL в день.

    Каждая запись results должна содержать ключ 'start_timestamp_ms' (ms),
    а также 'breakout', 'period', 'all_fees', 'pos_values_pnl', 'total_pnl'.
    """
    if not results:
        raise ValueError("results list is empty")

    # --- вычисляем границы периода из start_timestamp_ms
    ts_list = [r['start_timestamp_ms'] for r in results]
    ts0, ts1 = min(ts_list), max(ts_list)
    duration_days = (ts1 - ts0) / 86_400_000
    if duration_days <= 0:
        duration_days = 1.0  # во избежание деления на ноль

    # --- собираем сырые списки
    n          = len(results)
    durations  = [r['period']        for r in results]
    fee_pnls   = [r['all_fees']      for r in results]
    value_pnls = [r['pos_values_pnl'] for r in results]
    total_pnls = [r['total_pnl']     for r in results]
    breakouts  = [r['breakout']      for r in results]

    def basic(xs: List[float]) -> Dict[str, float]:
        return {
            'mean':   statistics.mean(xs),
            'median': statistics.median(xs),
            'stdev':  statistics.pstdev(xs) if len(xs) > 1 else 0.0,
            'min':    min(xs),
            'max':    max(xs),
        }

    # --- PnL aggregate + per trade
    total_fee   = sum(fee_pnls)
    total_val   = sum(value_pnls)
    total_pnl   = sum(total_pnls)
    wins        = [p for p in total_pnls if p > 0]
    losses      = [p for p in total_pnls if p < 0]

    pnl = {
        'aggregate': {
            'total_pnl':       total_pnl,
            'total_fee_pnl':   total_fee,
            'total_value_pnl': total_val,
        },
        'per_trade': {
            **basic(total_pnls),
            'win_rate_pct':  round(len(wins) * 100 / n, 2),
            'avg_win':       statistics.mean(wins)   if wins   else 0.0,
            'avg_loss':      statistics.mean(losses) if losses else 0.0,
            'profit_factor': round((sum(wins) / abs(sum(losses))) if losses else math.inf, 4),
        }
    }

    # --- Duration + подробная статистика
    # Сортируем и считаем основные перцентили
    sorted_durations = sorted(durations)
    n_dur = len(sorted_durations)
    # Перцентиль на позиции 10% и 90%
    p10 = sorted_durations[int(0.10 * (n_dur - 1))]
    p90 = sorted_durations[int(0.90 * (n_dur - 1))]
    # Квартильная разбивка (25%, 50%, 75%)
    q1, q2, q3 = statistics.quantiles(durations, n=4, method='inclusive')

    dur = {
        'count':  n_dur,
        'sum':    sum(durations),
        'mean':   statistics.mean(durations),
        'median': statistics.median(durations),
        'mode':   (statistics.mode(durations)
                if len(set(durations)) < n_dur else None),
        'stdev':  statistics.pstdev(durations) if n_dur > 1 else 0.0,
        'min':    sorted_durations[0],
        'max':    sorted_durations[-1],
        'range':  sorted_durations[-1] - sorted_durations[0],
        '10%':    p10,
        '25%':    q1,
        '50%':    q2,
        '75%':    q3,
        '90%':    p90,
        'IQR':    q3 - q1,
    }

    # --- Breakouts: считаем все уникальные значения breakout
    cnt = Counter(breakouts)
    bo = {}
    for breakout_value, count in cnt.items():
        items = [r for r in results if r['breakout'] == breakout_value]
        total_dur = sum(r['period']    for r in items)
        total_pnl_bo = sum(r['total_pnl'] for r in items)
        bo[breakout_value] = {
            'count':     count,
            'count_pct': round(count * 100 / n, 2),
            'avg_dur':   (total_dur / count)    if count else 0.0,
            'avg_pnl':   (total_pnl_bo / count) if count else 0.0,
        }

    # --- Средние в день
    trades_per_day = n / duration_days
    pnl_per_day    = total_pnl / duration_days

    return {
        'trades_total':   n,
        'trades_per_day': round(trades_per_day, 2),
        'pnl_per_day':    round(pnl_per_day, 2),
        'duration':       dur,
        'pnl':            pnl,
        'breakouts':      bo,
    }


def print_stats(stats: Dict[str, Any], prn: bool) -> str:
    """
    Компактный вывод статистики, полученной из build_stats(),
    и возврат той же строки для повторного вывода.
    """
    lines = []

    # Заголовок
    lines.append(f"=== Всего позиций: {stats['trades_total']} ===")
    lines.append(f" Средн. позиций/день : {stats['trades_per_day']}")
    # продолжительность в часах
    d = stats['duration']
    dur_hours = round((stats['trades_per_day'] * d['mean']) / 60, 2)
    lines.append(f" Продолж/день      : {dur_hours}")
    lines.append(f" Средн. PnL/день   : {stats['pnl_per_day']}")
    lines.append("")  # пустая строка

    # Статистика по длительности
    mode_str = f"{d['mode']:.2f}" if d['mode'] is not None else "None"
    lines.append(
        "Duration (мин): "
        f"count={d['count']}, sum={d['sum']:.2f}, "
        f"mean={d['mean']:.2f}, median={d['median']:.2f}, mode={mode_str}, "
        f"σ={d['stdev']:.2f}, min={d['min']:.2f}, max={d['max']:.2f}, "
        f"range={d['range']:.2f}, 10%={d['10%']:.2f}, 25%={d['25%']:.2f}, "
        f"50%={d['50%']:.2f}, 75%={d['75%']:.2f}, 90%={d['90%']:.2f}, IQR={d['IQR']:.2f}"
    )
    lines.append("")

    # PnL
    a  = stats['pnl']['aggregate']
    pt = stats['pnl']['per_trade']
    kof = a['total_fee_pnl'] / abs(a['total_value_pnl']) if a['total_value_pnl'] != 0 else float('inf')
    lines.append(
        f"PnL Agg: total={a['total_pnl']:.2f}, "
        f"fee={a['total_fee_pnl']:.2f}, value={a['total_value_pnl']:.2f}"
    )
    lines.append(f"kof: {kof:.2f}")
    lines.append(
        f"PnL/trade: mean={pt['mean']:.2f}, median={pt['median']:.2f}, "
        f"σ={pt['stdev']:.2f}, win%={pt['win_rate_pct']:.2f}, "
        f"pf={pt['profit_factor']:.2f}"
    )
    lines.append("")

    # Breakouts
    lines.append("Breakouts:")
    for k, v in stats['breakouts'].items():
        lines.append(
            f"  {k.upper():<5} cnt={v['count']}({v['count_pct']}%), "
            f"avg_dur={v['avg_dur']:.2f}, avg_pnl={v['avg_pnl']:.2f}"
        )
    lines.append("")  # финальный перевод строки

    # Собираем и выводим
    output = "\n".join(lines)
    if prn:
        print(output)
    return output




from statistics import mean, median, pstdev

def print_summary(results):
    if not results: print("Нет данных."); return
    n = len(results)

    sum_pnl   = sum(r.total_pnl for r in results)
    opt_sum   = sum(r.opt_pnl   for r in results)
    fut_sum   = sum(r.fut_pnl   for r in results)
    fund_sum  = sum(r.funding   for r in results)
    fee_sum   = sum(r.once_fee  for r in results)
    slip_sum  = sum(r.slip_cost for r in results)
    hedges    = sum(r.hedge_count for r in results)

    minutes_sum = sum(r.minutes for r in results)

    print("┌──────────────────────────────────────────────┐")
    print(f"│  Сделок: {n:<5}  Win-rate: {sum(r.total_pnl>0 for r in results)/n*100:5.1f}% │")
    print("├──────────────────────────────────────────────┤")
    print(f"│  Σ PnL          : {sum_pnl:12.2f}")
    print(f"│   • опционы     : {opt_sum:12.2f}")
    print(f"│   • фьючерсы    : {fut_sum:12.2f}")
    print(f"│   • funding     : {-fund_sum:12.2f}")
    print(f"│   • once-fee    : {-fee_sum:12.2f}")
    print(f"│   • спреды      : {-slip_sum:12.2f}")
    print("├──────────────────────────────────────────────┤")
    print(f"│  Средний PnL/тр : {sum_pnl/n:12.2f}")
    print(f"│  Средний funding/день : { (fund_sum/(minutes_sum/1440)) :10.4f}")
    print(f"│  Средн. хеджей/тр     : { hedges/n:10.2f}")
    print("└──────────────────────────────────────────────┘")