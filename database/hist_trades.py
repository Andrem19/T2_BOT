from database.simple_orm import BaseModel
from datetime import datetime
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
    rel_atr: float = 0.08
    rsi: float = 55.4


from datetime import datetime

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
        profit = ((last_px - fut_entrypx) * qty) + ((opt_closepx - opt_entrypx) * cont)

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
            rel_atr=rel_atr,
            rsi=rsi
        )
        t.save()
    except Exception as e:
        sv.logger.exception(f'save_trade: {e}')
