from database.commands_tab import Commands
import shared_vars as sv
import json
import os
import math
import talib
from shared_vars import logger
import numpy as np
from html import escape
from typing import Any, Dict, List
from datetime import datetime, timedelta, timezone
from exchanges.hyperliquid_api import HL
from exchanges.bybit_option_hub import BybitOptionHub as BB
import asyncio

TEMP_FILE_PATH = "params_temp.json"

def save_stages(stages: dict, file_path: str = TEMP_FILE_PATH) -> None:
    """
    Сериализует словарь stages вместе с текущей датой сохранения (UTC)
    и записывает в файл, всегда полностью перезаписывая его.
    Любые объекты, не поддерживаемые JSON напрямую (например, datetime),
    будут автоматически приведены к строке.
    """
    data = {
        "saved_at": datetime.now(timezone.utc),
        "stages": stages
    }
    with open(file_path, 'w', encoding='utf-8') as f:
        # default=str конвертирует datetime → строка, а также любые другие неподдерживаемые объекты
        json.dump(data, f, ensure_ascii=False, indent=4, default=str)

def load_stages(file_path: str = TEMP_FILE_PATH) -> dict | None:
    """
    Читает JSON‑файл и возвращает словарь stages, если:
      1. Файл существует и корректно парсится.
      2. В нём есть строка saved_at в ISO‑формате.
      3. Дата сохранения не старше 2 дней.
    В противном случае возвращает None.
    """
    if not os.path.exists(file_path):
        return None

    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return None

    saved_at_str = data.get("saved_at")
    if not isinstance(saved_at_str, str):
        return None

    try:
        saved_at = datetime.fromisoformat(saved_at_str)
    except ValueError:
        return None

    if datetime.now(timezone.utc) - saved_at > timedelta(days=2):
        return None

    return data.get("stages")

async def refresh_commands_from_bd():
    try:
        com = Commands.get_instance()
        sv.stages['first']['amount'] = com.amount_1
        sv.stages['second']['amount'] = com.amount_2
        sv.stages['simulation']['fut_perc_c'] = com.fut_perc_c
        sv.stages['simulation']['fut_perc_p'] = com.fut_perc_p
        sv.timer_msg = com.timer
        sv.close_1 = com.close_1
        sv.close_2 = com.close_2
        sv.day_opt = com.day_opt
        symbols = []
        if com.btc:
            symbols.append('BTC')
        if com.eth:
            symbols.append('ETH')
        if com.sol:
            symbols.append('SOL')
        sv.symbols = symbols
        sv.simulation = com.simulation
        opt_types = []
        if com.put:
            opt_types.append('put')
        if com.call:
            opt_types.append('call')
        sv.opt_types = opt_types
        
        sv.stages['first']['expect'] = com.expect_1_btc

        sv.stages['second']['expect'] = com.expect_2_btc
        return com
    except Exception as e:
        logger.exception(e)
    


def prepare_atr_and_rel(
    klines,
    last_px,
    period: int = 14,
    symbol: str = "SOLUSDT",
    timeframe_sec: int = 60,
):
    """
    Нормализует массив свечей, безопасно считает ATR(last) и относительный ATR (ATR/last_px).
    Возвращает: (klines_np, atr_last, rel_atr)

    klines ожидается в формате np.ndarray или последовательности с колонками:
    [time, open, high, low, close, ...]
    """
    # Нормализация источника свечей
    if klines is None:
        if 'logger' in globals():
            logger.warning("No klines returned for %s %ss; using empty array.", symbol, timeframe_sec)
        klines_np = np.empty((0, 6), dtype=float)
    elif isinstance(klines, np.ndarray):
        try:
            klines_np = klines.astype(float, copy=False)
        except Exception:
            klines_np = np.asarray(klines, dtype=float)
    else:
        klines_np = np.asarray(klines, dtype=float) if klines else np.empty((0, 6), dtype=float)

    # Базовые проверки размеров
    if klines_np.shape[1] < 5:
        if 'logger' in globals():
            logger.warning(
                "Not enough kline columns for ATR calc: shape=%s; expected >=5 columns.",
                klines_np.shape
            )

    rows_ok = klines_np.shape[0] >= (period + 1)
    if not rows_ok and 'logger' in globals():
        logger.warning(
            "Not enough kline rows for ATR calc: got %s, need >= %s.",
            klines_np.shape[0], period + 1
        )

    # Извлечение столбцов
    highs  = klines_np[:, 2] if klines_np.shape[0] else np.array([], dtype=float)
    lows   = klines_np[:, 3] if klines_np.shape[0] else np.array([], dtype=float)
    closes = klines_np[:, 4] if klines_np.shape[0] else np.array([], dtype=float)

    # ATR(last)
    if rows_ok and highs.size and lows.size and closes.size:
        try:
            atr_arr = talib.ATR(highs, lows, closes, timeperiod=period)
            atr_last = float(atr_arr[-1]) if atr_arr.size else 0.0
            if math.isnan(atr_last) or math.isinf(atr_last):
                atr_last = 0.0
        except Exception as e:
            if 'logger' in globals():
                logger.exception("ATR calculation failed: %s", e)
            atr_last = 0.0
    else:
        atr_last = 0.0

    # rel_atr = ATR / last_px (безопасно)
    try:
        last_px_f = float(last_px)
    except (TypeError, ValueError):
        last_px_f = 0.0

    if last_px_f <= 0 or math.isnan(last_px_f) or math.isinf(last_px_f) or atr_last <= 0:
        rel_atr = 0.0
    else:
        rel_atr = atr_last / last_px_f

    return atr_last, rel_atr

def get_state_dict(stages):

    return {
        'time': str(datetime.now()),
        'exist': True,
        'stages': stages,
    }
        


def format_option_message_html(data: Dict[str, Any]) -> str:
    """
    Возвращает HTML-строку для Telegram (отправлять с parse_mode=HTML).
    Исключает: name, pnl_upper, qty, type.
    Преобразует ключи в человекочитаемый вид (Strike Perc вместо strike_perc).
    Жирным: 'pnl', 'strike_perc'.
    Визуальная "раскраска" полей через цветные эмодзи, маркер стоит перед ключом.
    """
    EXCLUDE_KEYS = {"name"}
    BOLD_FIELDS = {"pnl", "strike_perc"}

    FIELD_ORDER: List[str] = [
        "symbol",
        "strike_perc",
        "pnl",
        "ask_indicators",
        "p_t",
        "lower_perc",
        "upper_perc",
        "best_targ_bid",
        "ask",
        "ask_original",
        "max_amount",
    ]

    COLOR_MARKS: Dict[str, str] = {
        "symbol": "🔷",
        "strike_perc": "🟦",
        "pnl": "🟩",
        "ask_indicators": "🟣",
        "p_t": "🟪",
        "lower_perc": "🟠",
        "upper_perc": "🟡",
        "best_targ_bid": "🔵",
        "ask": "🟤",
        "ask_original": "🔶",
        "max_amount": "⬛",
    }
    DEFAULT_MARK = "⬜"

    def _format_number(val: float) -> str:
        if abs(val) >= 100:
            return f"{val:,.2f}".replace(",", " ")
        s = f"{val:.4f}".rstrip("0").rstrip(".")
        return s if s else "0"

    def _format_value(value: Any) -> str:
        if isinstance(value, float):
            return _format_number(value)
        if isinstance(value, int):
            return str(value)
        if isinstance(value, list):
            parts = []
            for x in value:
                if isinstance(x, (int, float)):
                    parts.append(_format_number(float(x)))
                else:
                    parts.append(str(x))
            return ", ".join(parts)
        return str(value)

    def _pretty_key(key: str) -> str:
        return " ".join(word.capitalize() for word in key.split("_"))

    # Фильтрация
    filtered = {k: v for k, v in data.items() if k not in EXCLUDE_KEYS}

    # Упорядочиваем
    ordered_keys = [k for k in FIELD_ORDER if k in filtered]
    tail_keys = [k for k in filtered.keys() if k not in ordered_keys]
    keys = ordered_keys + tail_keys

    # Формируем строки
    lines: List[str] = []
    for key in keys:
        raw_val = filtered[key]
        pretty_val = _format_value(raw_val)

        key_html = escape(_pretty_key(key))
        val_html = escape(str(pretty_val))

        if key in BOLD_FIELDS:
            val_html = f"<b>{val_html}</b>"

        mark = COLOR_MARKS.get(key, DEFAULT_MARK)
        # Теперь маркер в начале строки
        lines.append(f"{mark} <b>{key_html}:</b> {val_html}")

    return "\n".join(lines)


async def get_balances():
    try:      
        hl_bal_1 = HL.get_balance(account_idx=1)
        await asyncio.sleep(1)
        hl_bal_2 = HL.get_balance(account_idx=2)
        bybit_bal_1 = BB.Trading.get_total_equity(account_idx=1)
        await asyncio.sleep(1)
        bybit_bal_2 = BB.Trading.get_total_equity(account_idx=2)
        
        total = hl_bal_1+hl_bal_2+bybit_bal_1+bybit_bal_2
        bal = {
            "HL": {
                "1": hl_bal_1,
                "2": hl_bal_2
            },
            "BB": {
                "1": bybit_bal_1,
                "2": bybit_bal_2
            },
            "total": total
        }
        msg = f"hl_bal_1: {hl_bal_1}\nhl_bal_2: {hl_bal_2}\nbybit_bal_1: {bybit_bal_1}\nbybit_bal_2: {bybit_bal_2}\n\nTOTAL: {total}"
        return bal, msg
    except Exception as e:
        logger.exception(f'ERROR: (serv.get_balances) {e}')


def get_distance(symbol: str = 'BTC', left_to_exp: float = 10.0, which_pos_we_need: str = 'second'):
    distance = 0.012
    if symbol in ['ETH', 'SOL']:
        distance = 0.013 if left_to_exp < 10 else 0.016 if left_to_exp < 15 or which_pos_we_need == 'second' else 0.022
    elif symbol in ['BTC']:
        distance = 0.006
    return distance



def hours_until_next_8utc() -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=8, minute=0, second=0, microsecond=0)

    # Если уже после 8 утра — сдвигаем на следующий день
    if now >= target:
        target += timedelta(days=1)

    diff = target - now
    return diff.total_seconds() / 3600.0

def get_expect(com: Commands, which_pos_we_need: str = 'second', symbol: str = 'BTC'):
    expect = 10000
    if which_pos_we_need == 'first':
        if symbol == 'BTC':
            sv.stages['first']['expect'] = com.expect_1_btc
        elif symbol == 'ETH':
            sv.stages['first']['expect'] = com.expect_1_eth
        elif symbol == 'SOL':
            sv.stages['first']['expect'] = com.expect_1_sol
        expect = sv.stages['first']['expect']
    elif which_pos_we_need == 'second':
        if symbol == 'BTC':
            sv.stages['second']['expect'] = com.expect_2_btc
        elif symbol == 'ETH':
            sv.stages['second']['expect'] = com.expect_2_eth
        elif symbol == 'SOL':
            sv.stages['second']['expect'] = com.expect_2_sol
        expect = sv.stages['second']['expect']
    return expect

def auto_set_expect(h):
    com = Commands.get_instance()
    exp = sv.exp_rel[h]+com.exp_correct
    Commands.set_expect_2(exp, 'BTC')
    Commands.set_expect_1(exp-1.5, 'BTC')
    sv.stages['first']['expect'] = exp-1.5
    sv.stages['second']['expect'] = exp
    logger.info(f'New expect: 1={exp-1} 2={exp}')
    
def get_best():
    try:
        if sv.actual_bd.aloud_only == 0:
            return sv.stages['simulation']['position_1']
        elif sv.actual_bd.aloud_only == 3:
            return None
        elif sv.actual_bd.aloud_only == 1:
            if sv.stages['simulation']['position_1']['type'].lower() =='put':
                return sv.stages['simulation']['position_1']
            elif sv.stages['simulation']['position_2']['type'].lower() =='put':
                return sv.stages['simulation']['position_2']
            elif sv.stages['simulation']['position_3']['type'].lower() =='put':
                return sv.stages['simulation']['position_3']
            else:
                return None
        elif sv.actual_bd.aloud_only == 2:
            if sv.stages['simulation']['position_1']['type'].lower() =='call':
                return sv.stages['simulation']['position_1']
            elif sv.stages['simulation']['position_2']['type'].lower() =='call':
                return sv.stages['simulation']['position_2']
            elif sv.stages['simulation']['position_3']['type'].lower() =='call':
                return sv.stages['simulation']['position_3']
            else:
                return None
        else:
            return None
    except Exception as e:
        logger.error(f'ERROR in  get_best() {e}')