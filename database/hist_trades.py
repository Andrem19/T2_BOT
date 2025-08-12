from database.simple_orm import BaseModel
from typing import Any, Dict, Optional, List
from datetime import datetime, timezone, timedelta
import shared_vars as sv

class Trade(BaseModel):
    time_open: str
    base_symb: str
    name_opt: str
    fut_entrypx: float
    opt_entrypx: float
    fut_closepx: float
    opt_closepx: float
    opt_profit: float
    fut_profit: float
    profit: float
    target_perc: float
    take_profit_perc: float
    over_strike: float
    to_exp_init: float
    targ_pnl: float
    type_close: str
    time_close: str
    stage: str
    est_fee: float = 0,0
    rel_atr: float = 0.08
    rsi: float = 55.4

    @classmethod
    def last_n_days(
        cls,
        n: int,
        *,
        include_today: bool = True,
        now_utc: Optional[datetime] = None,
    ) -> List["Trade"]:
        """
        Вернуть все записи за последние n календарных дней по UTC, фильтрация по time_open.

        Интервал:
          - include_today=True (по умолчанию): [UTC 00:00 (today-(n-1)); now_utc]
          - include_today=False:                [UTC 00:00 (today-n); UTC 00:00 today)

        Требования к формату поля time_open в БД:
          'YYYY-MM-DD HH:MM:SS.ffffff+00:00'  (лексикографическое сравнение корректно для UTC)

        ПРИМЕЧАНИЕ: создаётся индекс по time_open, если его ещё нет.
        """
        if n <= 0:
            return []

        cls._ensure_idx_time_open()

        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if include_today:
            start_date = now.date() - timedelta(days=n - 1)
        else:
            start_date = now.date() - timedelta(days=n)

        start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
        start_str = start_dt.isoformat(sep=" ", timespec="microseconds")  # 'YYYY-MM-DD HH:MM:SS.ffffff+00:00'

        sql = f"""
            SELECT * FROM {cls._tbl_name()}
            WHERE time_open >= ?
            ORDER BY time_open ASC, id ASC
        """
        cur = cls._conn.execute(sql, (start_str,))
        rows = cur.fetchall()
        return [cls._row_to_obj(r) for r in rows]

    # --------------------- ВНУТРЕННИЕ ХЕЛПЕРЫ ---------------------

    @classmethod
    def _ensure_idx_time_open(cls) -> None:
        """Нефункциональный индекс по time_open для ускорения диапазонных запросов."""
        cls._conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{cls._tbl_name()}_time_open ON {cls._tbl_name()}(time_open)"
        )
        cls._conn.commit()

async def save_trade(stage, last_px, type_close):
    try:
        # Безопасное получение данных по этапу
        stage_data = sv.stages.get(stage, {})

        # Позиция и её вложенные словари
        position = stage_data.get('position', {})
        pos_info = position.get('position_info', {})
        leg = position.get('leg', {})
        info = leg.get('info', {})

        # Извлечение параметров с дефолтами
        qty = pos_info.get('size', 0)
        cont = leg.get('contracts', 0)
        base_symb = 'SOL'
        name_opt = leg.get('name', '')
        fut_entrypx = float(pos_info.get('entryPx', 0) or 0)
        opt_entrypx = float(info.get('avgPrice', 0) or 0)
        fut_closepx = last_px
        opt_closepx = float(info.get('used_bid', 0) or 0)
        open_time = position.get('open_time', '')

        # Расчёт прибыли
        est_fee = calc_est_fee(cont, opt_entrypx, qty, fut_entrypx)
        profit = (((last_px - fut_entrypx) * qty) + ((opt_closepx - opt_entrypx) * cont))-est_fee

        fut_profit = (last_px - fut_entrypx) * qty
        opt_profit = (opt_closepx - opt_entrypx) * cont
        # Закрытие и время
        t_close = type_close
        time_close = str(datetime.now())

        # Параметры таргетов и индикаторов
        targ_perc = stage_data.get('lower_perc', 0)
        take_prof_perc = stage_data.get('upper_perc', 0)
        over_strike = stage_data.get('over_strike', 0)
        to_exp_init = stage_data.get('to_exp_init', 0)
        targ_pnl = position.get('pnl_target', 0)
        rel_atr = stage_data.get('rel_atr', 0)
        rsi = stage_data.get('rsi', 0)
        
        

        # Создание и сохранение записи Trade
        t = Trade.create(
            time_open=str(open_time),
            base_symb=base_symb,
            name_opt=name_opt,
            fut_entrypx=fut_entrypx,
            opt_entrypx=opt_entrypx,
            fut_closepx=fut_closepx,
            opt_closepx=opt_closepx,
            fut_profit = fut_profit,
            opt_profit = opt_profit,
            profit=profit,
            target_perc=targ_perc,
            take_profit_perc=take_prof_perc,
            over_strike=over_strike,
            to_exp_init=to_exp_init,
            targ_pnl=targ_pnl,
            type_close=t_close,
            time_close=time_close,
            stage=stage,
            est_fee=est_fee,
            rel_atr=rel_atr,
            rsi=rsi
        )
        t.save()
    except Exception as e:
        sv.logger.exception(f'save_trade: {e}')


def calc_est_fee(contracts, avg_px, qty, entry_px):
    opt_fee = ((contracts * avg_px)*0.07)*2
    fut_fee = ((qty*entry_px)*0.00045)*2
    return fut_fee + opt_fee