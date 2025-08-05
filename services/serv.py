from database.commands_tab import Commands
import shared_vars as sv
import json
import os
import math
import talib
from shared_vars import logger
import numpy as np
from datetime import datetime, timedelta, timezone

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
        sv.stages['first']['expect'] = com.expect_1
        sv.stages['second']['expect'] = com.expect_2
        sv.timer_msg = com.timer
        sv.close_1 = com.close_1
        sv.close_2 = com.close_2
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
        