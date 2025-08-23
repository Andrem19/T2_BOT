# -*- coding: utf-8 -*-
"""
BTC candles + overlay of indicators (-1..1) on secondary Y-axis.
STRICT alignment: feature timestamps are exactly candle open times (UTC).
Supports any Binance interval (e.g., '5m', '15m', '1h'). Indicators are anchored to hour-open bars.

By default plots ALL keys found in dicts (except 'time').
You can exclude any subset via exclude_keys.
"""

from __future__ import annotations
import os
import time
from typing import Any, Dict, List, Tuple, Optional, Iterable
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
from dateutil import parser as dtparser
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc

# -------------------- Config --------------------

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"

# Базовые цвета для известных ключей; прочим выдадим цвета из tab20
BASE_COLOR_MAP: Dict[str, Any] = {
    "price_oi_funding": "#1f77b4",
    "basis":            "#ff7f0e",
    "flows":            "#2ca02c",
    "orderbook":        "#d62728",
    "cross":            "#9467bd",
    "calendar":         "#8c564b",
    "sentiment":        "#e377c2",
    "breadth":          "#7f7f7f",
    "stables":          "#bcbd22",
    "macro":            "#17becf",
    "news_score":       "#000000",
    "rr25":             "#17a2b8",
    "iv":               "#ffbb78",
    "overall":          "#0439E9",
}

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000,
    "1w": 604_800_000, "1M": 2_592_000_000,
}


# -------------------- Time parsing (YY-MM-DD priority) --------------------

def _parse_time_utc(s: str) -> datetime:
    """
    Ожидаемый приоритет форматов:
      1) YY-MM-DD HH:MM:SS  (e.g. '25-08-23 09:00:00' -> 2025-08-23 09:00:00Z)
      2) YYYY-MM-DD HH:MM:SS
      3) ISO с bias yearfirst=True
      4) DD-MM-YY HH:MM:SS
      5) DD-MM-YYYY HH:MM:SS
    Требует строго начало часа (мин=сек=микросек=0). Возвращает UTC-aware.
    """
    s = (s or "").strip()

    def _finish(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        if not (dt.minute == 0 and dt.second == 0 and dt.microsecond == 0):
            raise ValueError(f"Время должно быть на открытии часа: '{s}' → {dt.isoformat()}")
        return dt

    try:
        return _finish(datetime.strptime(s, "%y-%m-%d %H:%M:%S"))
    except Exception:
        pass
    try:
        return _finish(datetime.strptime(s, "%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass
    try:
        dt = dtparser.parse(s, yearfirst=True, dayfirst=False)
        return _finish(dt)
    except Exception:
        pass
    try:
        return _finish(datetime.strptime(s, "%d-%m-%y %H:%M:%S"))
    except Exception:
        pass
    try:
        return _finish(datetime.strptime(s, "%d-%m-%Y %H:%M:%S"))
    except Exception:
        pass

    raise ValueError(
        f"Не удалось распарсить время: '{s}'. Ожидаю 'YY-MM-DD HH:MM:SS' (UTC), напр.: '25-08-23 09:00:00'."
    )


# -------------------- Fetch klines (generic) --------------------

def _fetch_binance_klines_utc(
    start_dt: datetime,
    end_dt: datetime,
    interval: str,
    session: Optional[requests.Session] = None
) -> pd.DataFrame:
    """
    Скачивает свечи BTCUSDT за [start_dt, end_dt] ВКЛЮЧИТЕЛЬНО для произвольного interval.
    Индекс — время ОТКРЫТИЯ свечи (UTC). Колонки: ['open','high','low','close','volume'].
    """
    if start_dt.tzinfo is None or end_dt.tzinfo is None:
        raise ValueError("start_dt и end_dt должны быть tz-aware в UTC.")
    if start_dt > end_dt:
        raise ValueError("start_dt > end_dt.")
    if interval not in INTERVAL_MS:
        raise ValueError(f"Неподдерживаемый interval='{interval}'.")

    s = session or requests.Session()
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000) + (INTERVAL_MS[interval] - 1)  # включительный конец

    all_rows: List[List[Any]] = []
    cur = start_ms
    while cur <= end_ms:
        params = {"symbol": SYMBOL, "interval": interval, "startTime": cur, "endTime": end_ms, "limit": 1000}
        r = s.get(BINANCE_KLINES_URL, params=params, timeout=12)
        if r.status_code != 200:
            raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")
        batch = r.json()
        if not batch:
            break
        all_rows.extend(batch)
        last_open = batch[-1][0]
        next_cur = last_open + INTERVAL_MS[interval]
        if next_cur <= cur:
            break
        cur = next_cur
        time.sleep(0.12)

    if not all_rows:
        raise RuntimeError("Binance вернул пустые данные по свечам за указанный интервал.")

    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "qav", "num_trades", "taker_base_vol", "taker_quote_vol", "ignore"]
    df = pd.DataFrame(all_rows, columns=cols)[["open_time", "open", "high", "low", "close", "volume"]]
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("open_time", inplace=True)
    df.sort_index(inplace=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Обрезаем точно по окну (включительно)
    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
    if df.empty:
        raise RuntimeError("После обрезки окно свечей пусто. Проверьте диапазон дат/интервал.")
    return df


# -------------------- Validation --------------------

def _assert_strict_open_alignment(metric_times: pd.DatetimeIndex, candle_open_index: pd.DatetimeIndex):
    """
    Проверяет:
    1) все метки признаков tz-aware (UTC),
    2) каждая метка — ровно начало часа,
    3) каждая метка присутствует среди open_time свечей выбранного интервала.
    """
    if any(ts.tzinfo is None for ts in metric_times):
        raise AssertionError("Все времена признаков должны быть tz-aware (UTC).")
    not_hour = [ts for ts in metric_times if (ts.minute, ts.second, ts.microsecond) != (0, 0, 0)]
    if not_hour:
        ex = ", ".join(t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in not_hour[:8])
        tail = "" if len(not_hour) <= 8 else f" … и ещё {len(not_hour)-8}"
        raise AssertionError(f"Обнаружены метки НЕ на начале часа: {ex}{tail}")

    missing = [ts for ts in metric_times if ts not in candle_open_index]
    if missing:
        ex = ", ".join(t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in missing[:8])
        tail = "" if len(missing) <= 8 else f" … и ещё {len(missing)-8}"
        raise AssertionError(
            "Для некоторых меток признаков нет свечи (open_time) в выбранном интервале: "
            f"{ex}{tail}. Возможно, интервал слишком крупный (например, '2h')."
        )


# -------------------- Helpers --------------------

def _derive_indicator_keys(data_list: List[Dict[str, Any]], exclude_keys: Optional[Iterable[str]]) -> List[str]:
    """
    Берём все ключи, встречающиеся в словарях, кроме 'time' и исключённых; порядок — по первому появлению.
    """
    exclude = set(exclude_keys or [])
    exclude.add("time")
    seen, ordered = set(), []
    for d in data_list:
        for k in d.keys():
            if k in exclude or k in seen:
                continue
            seen.add(k)
            ordered.append(k)
    if not ordered:
        raise ValueError("Нет ни одного индикатора для отрисовки (после исключений).")
    return ordered


def _build_metrics_frame(
    data_list: List[Dict[str, Any]],
    indicator_keys: List[str],
) -> Tuple[pd.DataFrame, datetime, datetime]:
    """
    Строит DataFrame метрик по заданным ключам. Индекс — UTC на начале часа.
    news_score (если есть) делится на 100.
    """
    if not isinstance(data_list, list) or not data_list:
        raise ValueError("Ожидается непустой список словарей с метриками.")

    rows = []
    for d in data_list:
        if "time" not in d:
            raise ValueError("В одном из словарей отсутствует ключ 'time'.")
        dt = _parse_time_utc(str(d["time"]))
        row = {"time": dt}
        for k in indicator_keys:
            v = d.get(k, None)
            row[k] = float(v) if v is not None else np.nan
        rows.append(row)

    df = pd.DataFrame(rows).sort_values("time")
    df = df.groupby("time", as_index=False).last()       # на случай дублей по часу
    df.set_index("time", inplace=True)
    df.index = pd.to_datetime(df.index, utc=True)
    df.sort_index(inplace=True)

    if "news_score" in indicator_keys and "news_score" in df.columns:
        df["news_score"] = pd.to_numeric(df["news_score"], errors="coerce") / 100.0

    t_min, t_max = df.index.min(), df.index.max()
    if pd.isna(t_min) or pd.isna(t_max):
        raise ValueError("Не удалось определить диапазон времени метрик.")
    return df, t_min, t_max


def _build_color_map(indicator_keys: List[str]) -> Dict[str, Any]:
    """
    Возвращает карту цветов для всех ключей: известные из BASE_COLOR_MAP, прочим — tab20.
    """
    color_map: Dict[str, Any] = {}
    for k in indicator_keys:
        if k in BASE_COLOR_MAP:
            color_map[k] = BASE_COLOR_MAP[k]
    unknowns = [k for k in indicator_keys if k not in color_map]
    if unknowns:
        cmap = plt.cm.get_cmap("tab20")
        for i, k in enumerate(unknowns):
            color_map[k] = cmap(i % 20)
    return color_map


def _distribute_labels(y_vals: List[float], min_gap: float = 0.045) -> List[float]:
    """
    Простейший вертикальный “анти-оверлап” для подписей в диапазоне [-1,1].
    """
    if not y_vals:
        return []
    ys = y_vals[:]
    ys[0] = max(min(ys[0], 1.0), -1.0)
    for i in range(1, len(ys)):
        ys[i] = max(ys[i], ys[i-1] + min_gap)
    overflow = ys[-1] - 1.0
    if overflow > 0:
        ys = [y - overflow for y in ys]
    if ys[0] < -1.0:
        shift = -1.0 - ys[0]
        ys = [y + shift for y in ys]
    return [max(min(y, 1.0), -1.0) for y in ys]


# -------------------- Main rendering --------------------

def render_btc_indicators_chart(
    data_list: List[Dict[str, Any]],
    out_path: Optional[str] = None,
    title: str = "BTCUSDT — Candles + Indicators (labels at right, exact window)",
    figsize: Tuple[int, int] = (26, 14),
    dpi: int = 220,
    exclude_keys: Optional[Iterable[str]] = None,
    interval: str = "1h",
) -> str:
    """
    Рисует свечи BTCUSDT за точное окно метрик и накладывает ВСЕ индикаторы (кроме 'time'),
    с возможностью исключения через exclude_keys. Таймфрейм — любой Binance interval (e.g., '5m', '15m', '1h').
    Жёстко проверяет, что каждая метка признака — ровно на открытии часа и присутствует среди open_time свечей.
    """
    # 0) Набор ключей и метрики
    indicator_keys = _derive_indicator_keys(data_list, exclude_keys)
    metrics_df, t_min, t_max = _build_metrics_frame(data_list, indicator_keys)

    # 1) Свечи — ровно [t_min, t_max] включительно
    candles = _fetch_binance_klines_utc(t_min, t_max, interval=interval)

    # 2) Жёсткая проверка соответствия таймстемпов
    _assert_strict_open_alignment(metrics_df.index, candles.index)

    # 3) Подготовка данных для свечей
    ohlc = candles.loc[:, ["open", "high", "low", "close"]].copy()
    ohlc["mdates"] = mdates.date2num(ohlc.index.to_pydatetime())
    ohlc_plot = ohlc[["mdates", "open", "high", "low", "close"]].values

    # Ширина баров = 0.7 * медианный шаг по X (в долях суток)
    if len(ohlc) >= 2:
        step_days = float(np.median(np.diff(ohlc["mdates"].values)))
        bar_width = max(step_days * 0.7, 0.0002)
    else:
        bar_width = max((INTERVAL_MS.get(interval, 3_600_000) / 86_400_000.0) * 0.7, 0.0002)

    # 4) Рендер
    plt.close("all")
    fig, ax_price = plt.subplots(figsize=figsize, dpi=dpi)
    ax_price.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.35)
    ax_price.set_title(f"{title}  [{interval}]", fontsize=16)

    candlestick_ohlc(ax_price, ohlc_plot, width=bar_width, colorup="#26a69a", colordown="#ef5350", alpha=0.92)
    ax_price.set_ylabel("Price (USDT)")
    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M", tz=timezone.utc))
    fig.autofmt_xdate()

    # Вторая ось: индикаторы
    ax_ind = ax_price.twinx()
    ax_ind.set_ylabel("Indicators (−1 … 1)")
    ax_ind.set_ylim(-1.05, 1.05)
    ax_ind.axhline(0.0, color="#bdbdbd", linewidth=1.0, linestyle="--", alpha=0.65)

    # X-лимиты: окно метрик + правый запас под подписи
    x_min = mdates.date2num(t_min.to_pydatetime())
    x_max = mdates.date2num(t_max.to_pydatetime())
    pad_days = max((x_max - x_min) * 0.06, 0.12)
    ax_price.set_xlim(left=x_min - 0.02, right=x_max + pad_days)

    # Цвета
    color_map = _build_color_map(indicator_keys)

    # Линии индикаторов — ВАЖНО: рисуем ТОЛЬКО по часовым точкам (present.index), без 5-минутной сетки
    last_points: List[Tuple[str, float, float, Any]] = []
    plotted_any = False

    for key in indicator_keys:
        if key not in metrics_df.columns:
            continue
        present = metrics_df[key].dropna()
        if present.empty:
            continue

        # Доп. страховка: оставим только те метки, которые реально есть у свечей (должно выполняться по проверке выше)
        present = present[present.index.isin(candles.index)]
        if present.empty:
            continue

        color = color_map.get(key, "#000000")
        ax_ind.plot(
            present.index.to_pydatetime(),
            present.values,
            color=color,
            linewidth=1.8,
            linestyle="-",
            alpha=0.95,
            zorder=3,
        )
        plotted_any = True

        last_idx = present.index[-1]
        last_x = mdates.date2num(last_idx.to_pydatetime())
        last_y = float(present.iloc[-1])
        last_points.append((key, last_x, last_y, color))

    if not plotted_any:
        raise RuntimeError("Нет ни одной непустой серии индикаторов для отрисовки после применения exclude_keys.")

    # Подписи справа
    if last_points:
        last_points.sort(key=lambda x: x[2])
        y_sorted = [p[2] for p in last_points]
        y_adjusted = _distribute_labels(y_sorted, min_gap=0.045)
        x_text = x_max + pad_days * 0.92
        x_conn = x_max + pad_days * 0.55
        for (adj_y, (name, x_last, y_last, color)) in zip(y_adjusted, last_points):
            ax_ind.plot([x_last, x_conn], [y_last, adj_y], color=color, linewidth=0.9, alpha=0.85)
            ax_ind.text(
                x_text, adj_y, name,
                color=color, fontsize=11, va="center", ha="left",
                bbox=dict(facecolor="white", edgecolor=color, boxstyle="round,pad=0.25", alpha=0.7)
            )

    # Сохранение
    if out_path is None:
        out_path = os.path.abspath(
            f"btc_indicators_{interval}_{t_min.strftime('%Y%m%dT%H%MZ')}_{t_max.strftime('%Y%m%dT%H%MZ')}.png"
        )
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path