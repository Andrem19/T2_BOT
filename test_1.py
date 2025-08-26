# build_stable_metric_charts.py
# Запуск:  python build_stable_metric_charts.py --csv stables_metrics.csv
# Результат: stable_metric/stable_m.png (или stable_m_1.png, stable_m_2.png, ... если свечей > 200)

import os
import time
import math
import argparse
from typing import List, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import requests

BINANCE_BASE = "https://api.binance.com"
KLINES_PATH = "/api/v3/klines"

def _parse_time_utc_series(raw: pd.Series) -> pd.Series:
    """
    Надёжно извлекает числовые значения timestamp из столбца с типом object/str/float.
    Поддерживает:
      - "1726123456"
      - 1.726123456e+09
      - "1726123456.789"
      - "1,726,123,456.0"
      - значения с лишними пробелами/кавычками.
    Возвращает float (могут быть десятичные секунды).
    """
    # Преобразуем всё в строки, нормализуем десятичный разделитель и убираем "мусор".
    s = raw.astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)                 # запятые -> точки (десятичные)
    s = s.str.replace(r"[^\d\.\-\+eE]", "", regex=True)      # оставляем цифры, . + - e E

    # Извлекаем первое число в строке (на случай, если есть текст вокруг)
    s = s.str.extract(r"([-+]?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)", expand=False)

    # to_numeric: получим float (NaN для мусора)
    ts = pd.to_numeric(s, errors="coerce")
    return ts

def load_metrics(csv_path: str) -> Tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    """
    Читает CSV со столбцами: time_utc, stable_score, confidence.
    Надёжно парсит time_utc (секунды ИЛИ миллисекунды), строит dt (UTC),
    возвращает (df, start_ts, end_ts). В df есть столбец dt и числовые метрики.
    """
    df = pd.read_csv(csv_path)

    required = {"time_utc", "stable_score", "confidence"}
    if not required.issubset(df.columns):
        raise ValueError(f"CSV must contain columns {required}, got {df.columns.tolist()}")

    # Метрики -> float
    df = df.copy()
    df["stable_score"] = pd.to_numeric(df["stable_score"], errors="coerce")
    df["confidence"]   = pd.to_numeric(df["confidence"],   errors="coerce")

    # Время -> float (секунды с дробной частью или миллисекунды)
    ts = _parse_time_utc_series(df["time_utc"])
    df = df.loc[ts.notna()].copy()
    ts = ts.loc[ts.notna()]

    if ts.empty:
        raise ValueError("No valid numeric values in 'time_utc' after parsing.")

    # Определяем единицы (сек/мс) по масштабу: медиана > 1e12 => мс, иначе секунды
    median_val = float(ts.median())
    is_ms = median_val > 1e12

    if is_ms:
        # миллисекунды -> целые мс
        ts_int = np.floor(ts.values).astype("int64")
        df["dt"] = pd.to_datetime(ts_int, unit="ms", utc=True)
    else:
        # секунды -> целые секунды
        ts_int = np.floor(ts.values).astype("int64")
        df["dt"] = pd.to_datetime(ts_int, unit="s", utc=True)

    # чистим
    df = df.dropna(subset=["dt"]).sort_values("dt")

    if df.empty:
        raise ValueError("All rows became invalid after datetime parsing.")

    start = df["dt"].min().floor("T")
    end   = df["dt"].max().ceil("T")

    # Вернём только нужное
    return df[["dt", "stable_score", "confidence"]].reset_index(drop=True), start, end

def _respect_rate_limits(resp: requests.Response, default_sleep: float = 0.5):
    if resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After")
        try:
            wait_s = float(retry_after)
        except (TypeError, ValueError):
            wait_s = 2.0
        time.sleep(wait_s)
    else:
        time.sleep(default_sleep)

def fetch_binance_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    session: requests.Session,
    limit_per_req: int = 1000,
    pause_sec: float = 0.5,
    max_retries: int = 5,
) -> List[list]:
    assert interval == "1m", "This helper expects '1m' interval."
    ms_per_candle = 60_000
    chunk_ms = limit_per_req * ms_per_candle
    data: List[list] = []
    cur = int(start_ms); end_ms = int(end_ms)
    while cur <= end_ms:
        chunk_end = min(cur + chunk_ms - 1, end_ms)
        params = {"symbol": symbol, "interval": interval, "startTime": cur, "endTime": chunk_end, "limit": limit_per_req}
        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(BINANCE_BASE + KLINES_PATH, params=params, timeout=15)
                if resp.status_code in (200, 304):
                    arr = resp.json()
                    if not isinstance(arr, list):
                        raise ValueError(f"Unexpected response: {arr}")
                    if not arr:
                        break
                    data.extend(arr)
                    last_open = int(arr[-1][0])
                    cur = last_open + ms_per_candle
                    break
                else:
                    _respect_rate_limits(resp, default_sleep=pause_sec)
                    if attempt == max_retries:
                        resp.raise_for_status()
            except requests.RequestException as e:
                time.sleep(min(2 ** attempt, 30))
                if attempt == max_retries:
                    raise e
        if cur <= chunk_end:
            cur = chunk_end + 1
        time.sleep(pause_sec)
    return data

def klines_to_dataframe(klines: List[list]) -> pd.DataFrame:
    if not klines:
        raise ValueError("No klines returned for the requested period.")
    cols = ["open_time","open","high","low","close","volume","close_time","quote_volume","trades","taker_buy_base","taker_buy_quote","ignore"]
    df = pd.DataFrame(klines, columns=cols)
    for c in ["open","high","low","close","volume","quote_volume","taker_buy_base","taker_buy_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time_dt"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True)
    df["close_time_dt"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df.set_index("open_time_dt").sort_index()

def reindex_metrics_to_minutes(metrics_df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    idx = pd.date_range(start, end, freq="T", tz="UTC")
    m = metrics_df[["dt","stable_score","confidence"]].copy().set_index("dt").sort_index()
    m = m[~m.index.duplicated(keep="last")]
    return m.reindex(idx).ffill()

def draw_candlesticks(ax, ohlc_df: pd.DataFrame):
    x = np.arange(len(ohlc_df))
    opens  = ohlc_df["open"].to_numpy()
    highs  = ohlc_df["high"].to_numpy()
    lows   = ohlc_df["low"].to_numpy()
    closes = ohlc_df["close"].to_numpy()
    ax.vlines(x, lows, highs, linewidth=1)
    width = 0.6
    for i in range(len(ohlc_df)):
        y = float(min(opens[i], closes[i]))
        h = float(abs(closes[i] - opens[i]))
        rect = patches.Rectangle((x[i] - width/2.0, y), width, h if h > 0 else 1e-9, linewidth=0.5)
        ax.add_patch(rect)
    ax.set_xlim(-1, len(ohlc_df))
    ax.grid(True, which="both", axis="both", linestyle=":")

def format_xticks(ax, idx: pd.DatetimeIndex):
    n = len(idx)
    if n == 0: return
    pos = [0]; lab = [idx[0].strftime("%Y-%m-%d %H:%M")]
    if n > 1:
        mid = n // 2
        pos += [mid, n-1]
        lab += [idx[mid].strftime("%Y-%m-%d %H:%M"), idx[-1].strftime("%Y-%m-%d %H:%M")]
    ax.set_xticks(pos); ax.set_xticklabels(lab, rotation=0, ha="center")

def save_charts(ohlc: pd.DataFrame, metrics_minute: pd.DataFrame, out_dir: str, base_name="stable_m", max_candles_per_image=200):
    os.makedirs(out_dir, exist_ok=True)
    n = len(ohlc)
    if n == 0:
        raise ValueError("No OHLC rows to plot.")
    chunks = math.ceil(n / max_candles_per_image)
    for i in range(chunks):
        sl = ohlc.iloc[i*max_candles_per_image : min((i+1)*max_candles_per_image, n)].copy()
        fig = plt.figure(figsize=(14,7), dpi=200)
        ax1 = fig.gca()
        draw_candlesticks(ax1, sl)
        ax2 = ax1.twinx()
        s_slice = metrics_minute.loc[sl.index, "stable_score"]
        c_slice = metrics_minute.loc[sl.index, "confidence"]
        ax2.plot(np.arange(len(sl)), s_slice.to_numpy(), linewidth=1.5, label="stable_score")
        ax2.plot(np.arange(len(sl)), c_slice.to_numpy(), linewidth=1.2, linestyle="--", label="confidence")
        ymin = float(np.nanmin([s_slice.min(), c_slice.min()]))
        ymax = float(np.nanmax([s_slice.max(), c_slice.max()]))
        if np.isfinite(ymin) and np.isfinite(ymax):
            pad = max(0.05*(ymax-ymin), 0.05)
            ax2.set_ylim(ymin - pad, ymax + pad)
        format_xticks(ax1, sl.index)
        ax1.set_title(f"BTCUSDT 1m — {sl.index[0].strftime('%Y-%m-%d %H:%M UTC')} → {sl.index[-1].strftime('%Y-%m-%d %H:%M UTC')}  (candles: {len(sl)})")
        ax1.set_xlabel("UTC time"); ax1.set_ylabel("Price"); ax2.set_ylabel("score / confidence"); ax2.legend(loc="upper left")
        out_path = os.path.join(out_dir, f"{base_name}.png" if chunks == 1 else f"{base_name}_{i+1}.png")
        fig.tight_layout(); fig.savefig(out_path); plt.close(fig)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default="stables_metrics.csv")
    ap.add_argument("--symbol", type=str, default="BTCUSDT")
    ap.add_argument("--interval", type=str, default="1m")
    ap.add_argument("--output-dir", type=str, default="stable_metric")
    ap.add_argument("--max-per-image", type=int, default=200)
    ap.add_argument("--pause-sec", type=float, default=0.5)
    args = ap.parse_args()

    df_metrics, start_ts, end_ts = load_metrics(args.csv)
    minute_index = pd.date_range(start_ts, end_ts, freq="T", tz="UTC")
    start_ms = int(start_ts.timestamp() * 1000); end_ms = int(end_ts.timestamp() * 1000)

    with requests.Session() as sess:
        sess.headers.update({"User-Agent": "stable-metrics-chart/1.0"})
        klines = fetch_binance_klines(args.symbol, args.interval, start_ms, end_ms, sess, limit_per_req=1000, pause_sec=args.pause_sec)

    df_ohlc = klines_to_dataframe(klines)
    df_ohlc = df_ohlc.reindex(minute_index).dropna(subset=["open","high","low","close"], how="any")
    metrics_min = reindex_metrics_to_minutes(df_metrics, start_ts, end_ts)

    save_charts(df_ohlc, metrics_min, args.output_dir, base_name="stable_m", max_candles_per_image=max(1,int(args.max_per_image)))
    print(f"Charts saved to: {os.path.abspath(args.output_dir)}")

if __name__ == "__main__":
    main()
