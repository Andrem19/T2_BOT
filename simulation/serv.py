from numba import jit
import numpy as np
import talib
import pandas as pd
from numba import njit
import numpy as np
from datetime import datetime, timezone
from typing import Dict, Iterable, Any
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:
    ZoneInfo = None  # fallback на UTC ниже 
import json
import os
from pathlib import Path


def save_weekday_stats(
    stats: Dict[str, Any],
    folder_path: str = "_logs",
    filename: str = "days_stat.json",
) -> str:
    """
    Сохраняет словарь статистики по дням недели в JSON.
    Файл каждый раз перезаписывается (атомарная запись через временный файл).
    Возвращает абсолютный путь к записанному файлу.

    Параметры:
        stats: dict — данные для сохранения
        folder_path: str — каталог для файла (по умолчанию "_logs")
        filename: str — имя файла (по умолчанию "days_stat.json")
    """
    # Готовим путь и каталог
    folder = Path(folder_path)
    folder.mkdir(parents=True, exist_ok=True)
    target_path = folder / filename

    # Пишем во временный файл в том же каталоге и затем атомарно заменяем
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")

    # ensure_ascii=False — чтобы корректно сохранить русские ключи 'пн', 'вт', ...
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2, sort_keys=True)

    # os.replace — атомарно (на поддерживаемых ОС) заменяет целевой файл
    os.replace(temp_path, target_path)

    return str(target_path.resolve())


def load_weekday_stats(
    folder_path: str = "_logs",
    filename: str = "days_stat.json",
) -> Dict[str, Any]:
    """
    Читает JSON со статистикой и возвращает словарь.
    Если файла нет или содержимое некорректно — возвращает пустой словарь {}.
    """
    path = Path(folder_path) / filename
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # Гарантируем, что возвращаем именно dict
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


@jit(nopython=True)
def convert_timeframe(opens: np.ndarray, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, timeframe: int, ln: int):
    lenth_opens = len(opens)
    length = lenth_opens // timeframe if ln == 0 else ln

    new_opens = np.zeros(length)
    new_highs = np.zeros(length)
    new_lows = np.zeros(length)
    new_closes = np.zeros(length)

    for i in range(length):
        start = lenth_opens - (i + 1) * timeframe
        end = lenth_opens - i * timeframe

        new_opens[-(i + 1)] = opens[start]
        new_highs[-(i + 1)] = np.max(highs[start:end])
        new_lows[-(i + 1)] = np.min(lows[start:end])
        new_closes[-(i + 1)] = closes[end - 1]

    return new_opens, new_highs, new_lows, new_closes



# ---------- Wilder ATR по последовательности TR, возвращает последнее значение ----------
@njit(cache=True, fastmath=True)
def _wilder_atr_last(tr_seq: np.ndarray, p: int) -> float:
    n = tr_seq.shape[0]
    s = 0.0
    for k in range(p):
        s += tr_seq[k]
    atr = s / p
    for k in range(p, n):
        atr = (atr * (p - 1) + tr_seq[k]) / p
    return atr


@njit(cache=True, fastmath=True)
def _compute_minute_atr_out(TR: np.ndarray, step: int, p: int, bars_for_atr: int) -> np.ndarray:
    """
    Для каждой минуты i считает последнее значение ATR(p),
    где ATR строится по bars_for_atr "часовым" TR, заканчивающимся в i, i-step, ...
    TR[i] — TR часового окна [i-59 .. i] (инклюзивно).
    """
    N = TR.shape[0]
    atr_out = np.empty(N, dtype=np.float64)
    atr_out[:] = np.nan

    first_i = step * bars_for_atr - 1  # первый индекс, где доступно 15 баров

    for i in range(first_i, N):
        seq = np.empty(bars_for_atr, dtype=np.float64)
        valid = True
        # индексы [i - 14*step, ..., i - step, i]
        for j in range(bars_for_atr):
            idx = i - (bars_for_atr - 1 - j) * step
            v = TR[idx]
            if np.isnan(v):
                valid = False
                break
            seq[j] = v

        if not valid:
            continue

        if bars_for_atr < p:
            continue

        atr_out[i] = _wilder_atr_last(seq, p)

    return atr_out


def prepare_hourly(candles_1m: np.ndarray,
                   hour_window: int = 60,
                   atr_period: int = 7,
                   bars_for_atr: int = 15,
                   relative: bool = False) -> tuple[pd.DataFrame, np.ndarray]:
    """
    Строит на КАЖДОЙ минуте i ATR(atr_period), рассчитанный по последовательности
    из bars_for_atr "часовых" баров, где каждый часовой бар — окно [i-59 .. i] (ИНКЛЮЗИВНО).

    Вход:
        candles_1m : np.ndarray Nx5+  -> [ts_ms, open, high, low, close, ...]
        hour_window: размер окна "часа" в минутах (обычно 60)
        atr_period : период ATR Уайлдера (обычно 7)
        bars_for_atr: число часовых баров в расчёте (обычно 15)
        relative   : если True, делим ATR на текущую цену close[i]; колонка остаётся 'atr7_60'

    Выход:
        df_1m  : DataFrame минутных свечей с колонкой 'atr7_60'
        atr_arr: np.ndarray длины N, синхронизированный с df_1m.index
    """
    arr = np.asarray(candles_1m)
    if arr.ndim != 2 or arr.shape[1] < 5:
        raise ValueError("candles_1m должен иметь форму Nx5+: [ts, open, high, low, close, ...]")

    ts     = arr[:, 0].astype(np.int64)
    opens  = arr[:, 1].astype(np.float64)
    highs  = arr[:, 2].astype(np.float64)
    lows   = arr[:, 3].astype(np.float64)
    closes = arr[:, 4].astype(np.float64)
    N = arr.shape[0]

    # --- экстремумы/закрытия для часового окна, заканчивающегося в i (ИНКЛЮЗИВНО) ---
    s_high  = pd.Series(highs)
    s_low   = pd.Series(lows)
    s_close = pd.Series(closes)

    high_hour = s_high.rolling(window=hour_window, min_periods=hour_window).max().to_numpy()
    low_hour  = s_low .rolling(window=hour_window, min_periods=hour_window).min().to_numpy()
    close_hour = closes  # закрытие часового бара, заканчивающегося в i, равно close[i]

    prev_close_hour = np.full(N, np.nan, dtype=np.float64)
    prev_close_hour[hour_window:] = close_hour[:-hour_window]

    # --- TR для каждого i ---
    diff_hl = high_hour - low_hour
    diff_hc = np.abs(high_hour - prev_close_hour)
    diff_lc = np.abs(low_hour  - prev_close_hour)
    TR = np.nanmax(np.stack([diff_hl, diff_hc, diff_lc], axis=0), axis=0)

    # --- ATR по 15 часам на каждой минуте ---
    atr_out = _compute_minute_atr_out(TR, step=hour_window, p=atr_period, bars_for_atr=bars_for_atr)

    # --- Относительный ATR при необходимости (деление на текущую цену close[i]) ---
    if relative:
        # безопасное деление: 0 -> NaN
        with np.errstate(divide='ignore', invalid='ignore'):
            atr_rel = atr_out / closes
            atr_rel[~np.isfinite(atr_rel)] = np.nan
        atr_out = atr_rel

    # --- формирование DataFrame ---
    df = pd.DataFrame({
        "ts": ts,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
    })
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("datetime", inplace=True)
    df["atr7_60"] = atr_out

    return atr_out




def weekday_profit_stats(rows: Iterable[dict],
                         tz: str = "UTC") -> Dict[str, dict]:
    """
    Статистика прибыли/убытка по дням недели на основе времени ОТКРЫТИЯ позиции.

    Аргументы:
        rows: итерируемая коллекция словарей, где у каждого элемента есть:
              - "start_timestamp_ms": int|float (Unix-время в миллисекундах)
              - "period": int|float (продолжительность в минутах) — не используется
              - "total_pnl": int|float (прибыль >0 или убыток <0)
        tz:   IANA-таймзона, например "UTC" или "Europe/London". Используется,
              чтобы определить локальный день недели по времени открытия.

    Возвращает:
        Словарь с ключами 'пн','вт','ср','чт','пт','сб','вс'.
        Для каждого дня:
            {
              'sum_pnl': float,   # суммарный PnL за день недели
              'count':   int,     # количество сделок
              'avg_pnl': float    # средний PnL на сделку
            }

    Примечание:
        - Некорректные элементы (нет ключей, нечисловые значения, NaN/inf) пропускаются.
        - Группировка ведётся по дню недели начала сделки. Если нужна группировка
          по дате закрытия, надо будет передавать время закрытия.
    """
    day_keys = ['пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс']
    stats = {d: {'sum_pnl': 0.0, 'count': 0, 'avg_pnl': 0.0} for d in day_keys}

    # Определяем tzinfo
    if tz.upper() == "UTC":
        tzinfo = timezone.utc
    else:
        if ZoneInfo is None:
            # Если нет zoneinfo (очень старый Python), безопасный фолбэк — UTC
            tzinfo = timezone.utc
        else:
            tzinfo = ZoneInfo(tz)

    for row in rows:
        # Проверяем наличие ключей
        if not isinstance(row, dict):
            continue
        if "start_timestamp_ms" not in row or "total_pnl" not in row:
            continue

        ts_ms = row["start_timestamp_ms"]
        pnl = row["total_pnl"]

        # Пробуем привести к числам
        try:
            ts_ms = float(ts_ms)
            pnl = float(pnl)
        except (TypeError, ValueError):
            continue

        # Отбрасываем NaN/inf
        if pnl != pnl or pnl in (float("inf"), float("-inf")):
            continue

        # Переводим миллисекунды в datetime с нужной TZ
        try:
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=tzinfo)
        except (OverflowError, OSError, ValueError):
            continue

        # 0=понедельник ... 6=воскресенье
        idx = dt.weekday()
        key = day_keys[idx]

        stats[key]['sum_pnl'] += pnl
        stats[key]['count'] += 1

    # Финализируем среднее
    for d in day_keys:
        cnt = stats[d]['count']
        stats[d]['avg_pnl'] = stats[d]['sum_pnl'] / cnt if cnt else 0.0

    return stats
