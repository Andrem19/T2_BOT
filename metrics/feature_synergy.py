# feature_synergy.py
# -*- coding: utf-8 -*-

import math
import time
import json
import itertools
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import numpy as np


# ==============================
#           I/O: Binance
# ==============================

BINANCE_BASES = {
    "spot": "https://api.binance.com/api/v3/klines",
    "um":   "https://fapi.binance.com/fapi/v1/klines",  # USD-M futures
}

def _dt_parse_to_utc(dt_str: str) -> datetime:
    """
    Робастный парсер:
    Принимает 'YY-MM-DD HH:MM:SS' или 'YY-MM-DD' (в UTC).
    Примеры: '25-08-23 10:00:00' -> 2025-08-23 10:00:00+00:00
             '25-08-23'          -> 2025-08-23 00:00:00+00:00
    """
    dt_str = dt_str.strip()
    fmts = ["%y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M", "%y-%m-%d"]
    last_err = None
    for fmt in fmts:
        try:
            dt = datetime.strptime(dt_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception as e:
            last_err = e
    raise ValueError(f"Не удалось распарсить время '{dt_str}'. Последняя ошибка: {last_err}")

def _to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def _ceil_hour(dt: datetime) -> datetime:
    """Округление вверх к началу следующего часа (UTC), если есть минуты/секунды."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        return dt
    base = dt.replace(minute=0, second=0, microsecond=0)
    return base + timedelta(hours=1)

def _floor_hour(dt: datetime) -> datetime:
    """Округление вниз к началу часа (UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0)

def fetch_binance_klines(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: str = "1h",
    market: str = "spot",
    limit: int = 1000,
    request_timeout: int = 10,
    pause_sec_between_calls: float = 0.2,
) -> pd.DataFrame:
    """
    Скачивает свечи c Binance (spot или UM-фьючерсы) в UTC.
    Возвращает DataFrame с колонками:
      ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time']
    open_time/close_time — в миллисекундах Unix.
    """
    base_url = BINANCE_BASES.get(market)
    if base_url is None:
        raise ValueError("market должен быть 'spot' или 'um'")

    if interval != "1h":
        raise ValueError("В рамках задачи используем именно interval='1h'.")

    # Binance использует открытие свечи в миллисекундах от UTC.
    start_ms = _to_ms(_floor_hour(start_time))
    # endTime в Binance - эксклюзивная граница; чтобы гарантировать включение последней свечи,
    # сдвигаем +1мс от конца часа.
    end_ms = _to_ms(_ceil_hour(end_time))

    all_rows = []
    cur = start_ms
    while cur < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cur,
            "endTime": end_ms,
            "limit": limit,
        }
        resp = requests.get(base_url, params=params, timeout=request_timeout)
        if resp.status_code != 200:
            raise RuntimeError(f"Binance API error {resp.status_code}: {resp.text}")
        rows = resp.json()
        if not rows:
            break

        # rows: [ [openTime, open, high, low, close, volume, closeTime, ...], ... ]
        all_rows.extend(rows)

        last_open = rows[-1][0]  # ms
        # Следующий запрос начинаем с (последний open + 1мс), чтобы не зациклиться
        next_start = last_open + 1
        if next_start <= cur:
            # страховка от зацикливания
            next_start = cur + 3600_000
        cur = next_start
        time.sleep(pause_sec_between_calls)

    if not all_rows:
        return pd.DataFrame(columns=["open_time","open","high","low","close","volume","close_time"])

    arr = np.array(all_rows, dtype=object)
    df = pd.DataFrame({
        "open_time":  arr[:, 0].astype(np.int64),
        "open":       arr[:, 1].astype(np.float64),
        "high":       arr[:, 2].astype(np.float64),
        "low":        arr[:, 3].astype(np.float64),
        "close":      arr[:, 4].astype(np.float64),
        "volume":     arr[:, 5].astype(np.float64),
        "close_time": arr[:, 6].astype(np.int64),
    })
    # на всякий случай уберём дубликаты и отсортируем
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)
    return df


# ==============================
#      Преобразование входа
# ==============================

def indicators_to_df(indicators: List[Dict]) -> pd.DataFrame:
    """
    Из списка словарей формирует DataFrame.
    Добавляет столбец open_time (UTC, миллисекунды) как время открытия соответствующей свечи.
    Доп. правка: безопасное приведение типов без deprecated errors='ignore'.
    """
    if not indicators:
        raise ValueError("Список индикаторов пуст.")

    rows = []
    for row in indicators:
        if "time" not in row:
            raise ValueError("Каждый словарь обязан иметь ключ 'time'.")
        dt = _dt_parse_to_utc(str(row["time"]))
        dt = _floor_hour(dt)  # строго к началу часа
        open_ms = _to_ms(dt)

        feat = {k: v for k, v in row.items() if k != "time"}
        feat["open_time"] = open_ms
        rows.append(feat)

    df = pd.DataFrame(rows)

    # Аккуратно приводим потенциально числовые фичи к float,
    # но только если конвертация действительно проходит без ошибок.
    for c in df.columns:
        if c == "open_time":
            continue
        if df[c].dtype == object:
            try:
                converted = pd.to_numeric(df[c])
                df[c] = converted
            except Exception:
                pass

    return df



# ==============================
#    Матчинг, таргеты, метрики
# ==============================

def build_targets(
    df_feats: pd.DataFrame,
    df_klines: pd.DataFrame,
) -> pd.DataFrame:
    """
    Жёстко матчует индикаторы к свече по open_time и рассчитывает таргеты:
      - ret_same_hour: (close - open)/open для той же свечи
      - ret_next_hour: (next_close - this_open)/this_open
      - target_same_hour: 'UP'/'DOWN'/'FLAT'
      - target_next_hour: 'UP'/'DOWN'/'FLAT'
    Возвращает объединённый DataFrame (только те строки, где нашлась свеча).
    """
    if df_feats.empty or df_klines.empty:
        return pd.DataFrame()

    k = df_klines.set_index("open_time").sort_index()
    k["next_close"] = k["close"].shift(-1)

    m = df_feats.merge(
        k[["open", "close", "next_close"]],
        left_on="open_time", right_index=True,
        how="inner",
    ).copy()

    m["ret_same_hour"] = (m["close"] - m["open"]) / m["open"]
    m["ret_next_hour"] = (m["next_close"] - m["open"]) / m["open"]

    def labeler(x: float) -> str:
        if pd.isna(x) or abs(x) < 1e-12:
            return "FLAT"
        return "UP" if x > 0 else "DOWN"

    m["target_same_hour"] = m["ret_same_hour"].apply(labeler)
    m["target_next_hour"] = m["ret_next_hour"].apply(labeler)

    m = m.dropna(subset=["ret_same_hour", "ret_next_hour"]).reset_index(drop=True)
    return m


# ==============================
#    Поиск «связок признаков»
# ==============================

def _bin_features(df: pd.DataFrame, cols: List[str], bins: int = 3) -> Tuple[pd.DataFrame, Dict[str, np.ndarray]]:
    """
    Квантильное бинингование признаков. Возвращает:
      - df_binned: DataFrame с *_bin колонками (строки 'low'/'mid'/'high' при bins=3)
      - cut_map: границы бинов (массивы квантилей) для каждого признака
    """
    df_binned = df.copy()
    cut_map: Dict[str, np.ndarray] = {}
    labels = None
    if bins == 2:
        labels = ["low", "high"]
    elif bins == 3:
        labels = ["low", "mid", "high"]
    elif bins == 4:
        labels = ["q1","q2","q3","q4"]
    elif bins == 5:
        labels = ["q1","q2","q3","q4","q5"]

    for c in cols:
        if not np.issubdtype(df_binned[c].dtype, np.number):
            continue
        if df_binned[c].nunique(dropna=True) < bins:
            continue
        try:
            cats, bins_idx = pd.qcut(df_binned[c], q=bins, labels=labels, duplicates="drop", retbins=True)
            df_binned[c + "_bin"] = cats.astype(str)
            cut_map[c] = bins_idx
        except Exception:
            continue
    return df_binned, cut_map

def _proportion_z_test(p1: float, n1: int, p0: float, n0: int) -> float:
    """
    Z-тест на разность долей (апрокс. нормалью).
    Возвращает z-score (чем выше |z|, тем значимее отличие).
    """
    if n1 == 0 or n0 == 0:
        return 0.0
    se = math.sqrt(p0 * (1 - p0) * (1.0/n1 + 1.0/n0))
    if se == 0:
        return 0.0
    return (p1 - p0) / se

def _combo_report(
    df: pd.DataFrame,
    mask: pd.Series,
    base_up_rate: float,
    base_ret_mean: float,
    target_col: str,
    ret_col: str,
) -> Optional[Dict]:
    """
    Строит метрики по подвыборке mask (связка условий).
    Возвращает словарь с n, up_rate, lift, z, mean_ret, ret_lift.
    """
    sub = df[mask]
    n1 = len(sub)
    if n1 == 0:
        return None

    up_rate = (sub[target_col] == "UP").mean()
    lift = up_rate - base_up_rate
    z = _proportion_z_test(up_rate, n1, base_up_rate, len(df))

    mean_ret = sub[ret_col].mean()
    ret_lift = mean_ret - base_ret_mean

    return {
        "n": int(n1),
        "up_rate": float(up_rate),
        "lift": float(lift),
        "z": float(z),
        "mean_ret": float(mean_ret),
        "ret_lift": float(ret_lift),
    }

def find_feature_synergies(
    df_labeled: pd.DataFrame,
    use_target: str = "target_next_hour",
    use_return: str = "ret_next_hour",
    bins: int = 3,
    min_support: int = 30,
    k_max: int = 2,
    topn: int = 20,
    exclude_cols: Optional[List[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Ищет «связки признаков» по квантильным бинам (пары и, опционально, тройки).
    Возвращает:
      - pairs, triples: найденные правила,
      - base: базовые метрики,
      - pair_support: диагностическая таблица по поддержке каждой комбинации бинов.
    """
    if exclude_cols is None:
        exclude_cols = ["open_time", "open", "close", "next_close",
                        "ret_same_hour", "ret_next_hour",
                        "target_same_hour", "target_next_hour"]

    feature_cols = [c for c in df_labeled.columns
                    if c not in exclude_cols
                    and np.issubdtype(df_labeled[c].dtype, np.number)]

    # Биннинг
    df_binned, cut_map = _bin_features(df_labeled, feature_cols, bins=bins)

    binned_cols = [c for c in feature_cols if c + "_bin" in df_binned.columns]
    if len(binned_cols) < 2:
        raise ValueError("Недостаточно признаков для построения связок (биннинг не прошёл).")

    # База
    base_up_rate = (df_binned[use_target] == "UP").mean()
    base_ret_mean = df_binned[use_return].mean()

    # Диагностика: поддержка для всех пар (до фильтров)
    sup_rows = []
    for a, b in itertools.combinations(binned_cols, 2):
        avs = df_binned[a + "_bin"].dropna().unique()
        bvs = df_binned[b + "_bin"].dropna().unique()
        for av in avs:
            for bv in bvs:
                n = int(((df_binned[a + "_bin"] == av) &
                         (df_binned[b + "_bin"] == bv)).sum())
                sup_rows.append({"feat_a": a, "bin_a": str(av),
                                 "feat_b": b, "bin_b": str(bv),
                                 "n": n})
    pair_support = pd.DataFrame(sup_rows).sort_values("n", ascending=False).reset_index(drop=True)

    # Поиск пар
    results_pairs = []
    for _, r in pair_support.iterrows():
        if r["n"] < min_support:
            continue
        mask = ((df_binned[r["feat_a"] + "_bin"] == r["bin_a"]) &
                (df_binned[r["feat_b"] + "_bin"] == r["bin_b"]))
        rep = _combo_report(df_binned, mask, base_up_rate, base_ret_mean,
                            use_target, use_return)
        if rep:
            results_pairs.append({**r.to_dict(), **rep})

    df_pairs = pd.DataFrame(results_pairs)
    if not df_pairs.empty:
        df_pairs["score"] = df_pairs["z"].abs() * np.log1p(df_pairs["n"]) + 5.0 * df_pairs["lift"].abs()
        df_pairs = df_pairs.sort_values(["score","n","z"], ascending=[False, False, False]).head(topn).reset_index(drop=True)

    # Поиск троек (опционально)
    results_triples = []
    if k_max >= 3 and len(binned_cols) >= 3:
        for a, b, c in itertools.combinations(binned_cols, 3):
            a_vals = df_binned[a + "_bin"].dropna().unique()
            b_vals = df_binned[b + "_bin"].dropna().unique()
            c_vals = df_binned[c + "_bin"].dropna().unique()
            for av in a_vals:
                for bv in b_vals:
                    for cv in c_vals:
                        mask = ((df_binned[a + "_bin"] == av) &
                                (df_binned[b + "_bin"] == bv) &
                                (df_binned[c + "_bin"] == cv))
                        n = int(mask.sum())
                        if n < min_support:
                            continue
                        rep = _combo_report(df_binned, mask, base_up_rate, base_ret_mean,
                                            use_target, use_return)
                        if rep:
                            results_triples.append({
                                "feat_a": a, "bin_a": str(av),
                                "feat_b": b, "bin_b": str(bv),
                                "feat_c": c, "bin_c": str(cv),
                                **rep
                            })
        df_triples = pd.DataFrame(results_triples)
        if not df_triples.empty:
            df_triples["score"] = df_triples["z"].abs() * np.log1p(df_triples["n"]) + 5.0 * df_triples["lift"].abs()
            df_triples = df_triples.sort_values(["score","n","z"], ascending=[False, False, False]).head(topn).reset_index(drop=True)
    else:
        df_triples = pd.DataFrame(columns=["feat_a","bin_a","feat_b","bin_b","feat_c","bin_c","n","up_rate","lift","z","mean_ret","ret_lift","score"])

    base_df = pd.DataFrame([{
        "N_total": int(len(df_binned)),
        "base_up_rate": float(base_up_rate),
        "base_ret_mean": float(base_ret_mean),
        "bins": int(bins),
        "min_support": int(min_support),
    }])

    return {
        "pairs": df_pairs,
        "triples": df_triples,
        "base": base_df,
        "pair_support": pair_support,
        # Возвращаем для прозрачноcти (используется ниже для последнего сигнала)
        "df_binned": df_binned,
        "cut_map": cut_map,
    }



# ==============================
#      Главный конвейер
# ==============================

def _compute_latest_signal(
    df_labeled: pd.DataFrame,
    df_pairs: pd.DataFrame,
    bins: int,
    exclude_cols: Optional[List[str]] = None,
) -> Dict:
    """
    Рассчитывает «последний» сигнал по топ-5 правилам (пары).
    Алгоритм:
      - заново бинируем признаки на df_labeled,
      - берём последнюю строку (макс. open_time),
      - среди топ-5 правил оставляем только те, которые совпадают по бинам,
      - голос каждого правила: raw = 2*up_rate - 1 ([-1..1]); вес = score>=0;
      - итоговый балл = взвешенная сумма raw по совпавшим правилам, нормированная на сумму весов.
      - если совпавших нет — берём базовый raw: 2*base_up_rate - 1.
    Возвращает словарь со всеми деталями.
    """
    if df_labeled.empty:
        return {
            "latest_score": 0.0,
            "latest_open_time": None,
            "latest_bins": {},
            "latest_matched_rules": pd.DataFrame(columns=["feat_a","bin_a","feat_b","bin_b","matched","weight","raw","contribution","z","n","up_rate","lift","ret_lift","score"]),
            "bin_edges": {},
        }

    # Определим последний open_time
    last_open_time = int(df_labeled["open_time"].max())
    last_row = df_labeled.loc[df_labeled["open_time"].idxmax()]

    # Те же исключения, что и в поиске правил
    if exclude_cols is None:
        exclude_cols = ["open_time", "open", "close", "next_close",
                        "ret_same_hour", "ret_next_hour",
                        "target_same_hour", "target_next_hour"]

    feature_cols = [c for c in df_labeled.columns
                    if c not in exclude_cols
                    and np.issubdtype(df_labeled[c].dtype, np.number)]

    # Биннингуем на всей выборке (как при построении правил)
    df_binned, cut_map = _bin_features(df_labeled, feature_cols, bins=bins)
    binned_cols = [c for c in feature_cols if c + "_bin" in df_binned.columns]

    # Словарь бинов последней строки
    last_idx = df_binned["open_time"].idxmax()
    latest_bins = {}
    for c in binned_cols:
        latest_bins[c] = str(df_binned.loc[last_idx, c + "_bin"])

    # Если пар нет — fallback к базе
    if df_pairs is None or df_pairs.empty:
        base_up_rate = (df_binned["target_next_hour"] == "UP").mean()
        base_raw = float(2.0 * base_up_rate - 1.0)
        return {
            "latest_score": float(np.clip(base_raw, -1.0, 1.0)),
            "latest_open_time": last_open_time,
            "latest_bins": latest_bins,
            "latest_matched_rules": pd.DataFrame(columns=["feat_a","bin_a","feat_b","bin_b","matched","weight","raw","contribution","z","n","up_rate","lift","ret_lift","score"]),
            "bin_edges": {k: v for k, v in cut_map.items()},
        }

    # Берём топ-5 по score (вход df_pairs уже отсортирован и усечён в find_feature_synergies)
    top5 = df_pairs.head(5).copy()

    rows = []
    num = 0.0
    den = 0.0
    for _, r in top5.iterrows():
        a, b = r["feat_a"], r["feat_b"]
        a_bin_need, b_bin_need = str(r["bin_a"]), str(r["bin_b"])
        a_bin_is = latest_bins.get(a, None)
        b_bin_is = latest_bins.get(b, None)
        matched = (a_bin_is == a_bin_need) and (b_bin_is == b_bin_need)

        # Базовый «направленный» сигнал правила: +1 … -1
        raw = float(2.0 * r["up_rate"] - 1.0)
        weight = float(max(0.0, r.get("score", 0.0)))  # неотрицательный вес

        contrib = raw * weight if matched else 0.0
        if matched and weight > 0:
            num += contrib
            den += weight

        rows.append({
            "feat_a": a, "bin_a": a_bin_need,
            "feat_b": b, "bin_b": b_bin_need,
            "matched": bool(matched),
            "weight": weight,
            "raw": raw,
            "contribution": contrib,
            "z": float(r["z"]),
            "n": int(r["n"]),
            "up_rate": float(r["up_rate"]),
            "lift": float(r["lift"]),
            "ret_lift": float(r["ret_lift"]),
            "score": float(r.get("score", 0.0)),
        })

    matched_df = pd.DataFrame(rows)

    if den > 0:
        final_score = float(np.clip(num / den, -1.0, 1.0))
    else:
        # Если ни одно топ-правило не совпало — fallback к базе по всей выборке
        base_up_rate = (df_binned["target_next_hour"] == "UP").mean()
        final_score = float(np.clip(2.0 * base_up_rate - 1.0, -1.0, 1.0))

    return {
        "latest_score": final_score,
        "latest_open_time": last_open_time,
        "latest_bins": latest_bins,
        "latest_matched_rules": matched_df,
        "bin_edges": {k: v for k, v in cut_map.items()},
    }


def analyze_feature_synergies(
    indicators: List[Dict],
    symbol: str = "BTCUSDT",
    market: str = "spot",            # 'spot' или 'um'
    bins: int = 3,
    min_support: int = 30,
    k_max: int = 2,
    topn: int = 20,
) -> Dict[str, pd.DataFrame]:
    """
    Полный конвейер:
      1) парсинг входа и определение диапазона,
      2) скачивание часовых свечей,
      3) матчинг + таргеты,
      4) поиск связок признаков,
      5) ОЦЕНКА ПОСЛЕДНЕГО НАБОРА ИНДИКАТОРОВ: вычисление итогового балла [-1..1]
         по топ-5 правил (пары), учитывая, в какие бины попадает последняя строка.
    Возвращает словари:
      'base', 'pairs', 'triples', 'pair_support', 'labeled',
      а также:
      'latest_score', 'latest_open_time', 'latest_bins',
      'latest_matched_rules', 'bin_edges'.
    """
    # 1) Вход -> DF
    df_feats_raw = indicators_to_df(indicators)
    if df_feats_raw.empty:
        raise ValueError("После парсинга признаков данных нет.")

    # Диапазон (строго по часам)
    min_open_ms = int(df_feats_raw["open_time"].min())
    max_open_ms = int(df_feats_raw["open_time"].max())
    start_dt = datetime.fromtimestamp(min_open_ms / 1000.0, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(max_open_ms / 1000.0, tz=timezone.utc)

    # 2) Свечи
    df_kl = fetch_binance_klines(
        symbol=symbol,
        start_time=start_dt,
        end_time=end_dt,
        interval="1h",
        market=market,
    )
    if df_kl.empty:
        raise RuntimeError("Свечи не получены — проверьте символ/диапазон/рынок.")

    # 3) Матчинг + таргеты
    df_labeled = build_targets(df_feats_raw, df_kl)
    if df_labeled.empty:
        raise RuntimeError("Не удалось сматчить индикаторы к свечам (несовпадение open_time?).")

    # 4) Связки
    packs = find_feature_synergies(
        df_labeled=df_labeled,
        use_target="target_next_hour",   # по умолчанию оцениваем «что будет на следующей свече»
        use_return="ret_next_hour",
        bins=bins,
        min_support=min_support,
        k_max=k_max,
        topn=topn,
        exclude_cols=None,
    )

    # 5) Последний сигнал по топ-5 пар
    latest_info = _compute_latest_signal(
        df_labeled=df_labeled,
        df_pairs=packs.get("pairs"),
        bins=bins,
        exclude_cols=None,
    )

    # Сохраняем всё в выходной словарь
    packs["labeled"] = df_labeled
    packs["latest_score"] = latest_info["latest_score"]
    packs["latest_open_time"] = latest_info["latest_open_time"]
    packs["latest_bins"] = latest_info["latest_bins"]
    packs["latest_matched_rules"] = latest_info["latest_matched_rules"]
    packs["bin_edges"] = latest_info["bin_edges"]

    return packs


# ==============================
#     Утилиты и пример запуска
# ==============================

def pretty_print_synergies(result: Dict[str, pd.DataFrame]) -> None:
    """
    Печатает базовую статистику, ТОП-связки и «Последний сигнал».
    Если связок нет — выводит диагностику: почему (поддержка/порог).
    """
    base = result["base"].iloc[0].to_dict()
    N_total = int(base.get("N_total", 0))
    base_up = float(base.get("base_up_rate", 0.0))
    base_ret = float(base.get("base_ret_mean", 0.0))
    bins = int(base.get("bins", 0))
    min_support = int(base.get("min_support", 0))

    print(f"Всего наблюдений: {N_total}")
    print(f"Базовая доля UP (next hour): {base_up:.3f}")
    print(f"Базовая средн. доходность (next hour): {base_ret:.5f}")
    print(f"(bins={bins}, min_support={min_support})\n")

    def _fmt_row_pair(r: pd.Series) -> str:
        return (f"[{int(r['n']):>4}] z={r['z']:+5.2f} lift={r['lift']:+.3f} "
                f"ret_lift={r['ret_lift']:+.5f}  "
                f"{r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}")

    pairs = result.get("pairs", pd.DataFrame())
    if not pairs.empty:
        print("ТОП-связки (пары):")
        for _, row in pairs.iterrows():
            print("  " + _fmt_row_pair(row))
        print()
    else:
        print("Связки (пары) не найдены при текущих порогах.\nДиагностика (топ по поддержке):")
        sup = result.get("pair_support", pd.DataFrame())
        if not sup.empty:
            head = sup.head(10)
            for _, r in head.iterrows():
                mark = " < min_support" if int(r["n"]) < min_support else ""
                print(f"  [{int(r['n']):>4}] {r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}{mark}")
            print("\nРекомендации: уменьшите min_support или снизьте bins до 2, либо расширьте период данных.")
        print()

    triples = result.get("triples", pd.DataFrame())
    if not triples.empty:
        print("ТОП-связки (тройки):")
        for _, r in triples.iterrows():
            print(f"  [{int(r['n']):>4}] z={r['z']:+5.2f} lift={r['lift']:+.3f} ret_lift={r['ret_lift']:+.5f}  "
                  f"{r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']} & {r['feat_c']}={r['bin_c']}")
        print()

    # ---- Последний сигнал ----
    latest_score = result.get("latest_score", None)
    latest_open_time = result.get("latest_open_time", None)
    latest_bins = result.get("latest_bins", {})
    latest_matched_rules = result.get("latest_matched_rules", pd.DataFrame())

    if latest_open_time is not None:
        ts = datetime.fromtimestamp(latest_open_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print("Последний сигнал:")
        print(f"  Час (open_time): {ts}")
        print(f"  Итоговый балл [-1..1]: {latest_score:+.3f}")
        if latest_matched_rules is not None and not latest_matched_rules.empty:
            print("  Совпавшие правила (вклад):")
            for _, r in latest_matched_rules[latest_matched_rules["matched"]].iterrows():
                print(f"    {r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}  "
                      f"raw={r['raw']:+.3f}, weight={r['weight']:.2f}, contrib={r['contribution']:+.3f}")
        else:
            print("  Совпавших топ-правил нет, использована базовая оценка.")
        print()


def format_synergies(result: Dict[str, pd.DataFrame], diagnostics_top: int = 10) -> str:
    """
    Формирует человекочитаемую строку-отчёт по результатам анализа связок
    и «Последнему сигналу», без печати (возвращает str).
    """
    lines = []

    base = result["base"].iloc[0].to_dict()
    N_total = int(base.get("N_total", 0))
    base_up = float(base.get("base_up_rate", 0.0))
    base_ret = float(base.get("base_ret_mean", 0.0))
    bins = int(base.get("bins", 0))
    min_support = int(base.get("min_support", 0))

    lines.append(f"Всего наблюдений: {N_total}")
    lines.append(f"Базовая доля UP (next hour): {base_up:.3f}")
    lines.append(f"Базовая средн. доходность (next hour): {base_ret:.5f}")
    if bins or min_support:
        lines.append(f"(bins={bins}, min_support={min_support})")
    lines.append("")

    def _fmt_row_pair(r: pd.Series) -> str:
        return (f"[{int(r['n']):>4}] z={r['z']:+5.2f} lift={r['lift']:+.3f} "
                f"ret_lift={r['ret_lift']:+.5f}  "
                f"{r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}")

    pairs = result.get("pairs", pd.DataFrame())
    if not pairs.empty:
        lines.append("ТОП-связки (пары):")
        for _, row in pairs.iterrows():
            lines.append("  " + _fmt_row_pair(row))
        lines.append("")
    else:
        lines.append("Связки (пары) не найдены при текущих порогах.")
        lines.append("Диагностика (топ по поддержке):")
        sup = result.get("pair_support", pd.DataFrame())
        if not sup.empty:
            head = sup.head(diagnostics_top)
            for _, r in head.iterrows():
                mark = " < min_support" if int(r["n"]) < min_support else ""
                lines.append(f"  [{int(r['n']):>4}] {r['feat_a']}={r['bin_a']} & "
                             f"{r['feat_b']}={r['bin_b']}{mark}")
            lines.append("")
            lines.append("Рекомендации: уменьшите min_support или снизьте bins до 2, "
                         "либо расширьте период данных.")
        lines.append("")

    triples = result.get("triples", pd.DataFrame())
    if not triples.empty:
        lines.append("ТОП-связки (тройки):")
        for _, r in triples.iterrows():
            lines.append(
                f"  [{int(r['n']):>4}] z={r['z']:+5.2f} lift={r['lift']:+.3f} "
                f"ret_lift={r['ret_lift']:+.5f}  "
                f"{r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']} & {r['feat_c']}={r['bin_c']}"
            )
        lines.append("")

    # ---- Последний сигнал ----
    latest_score = result.get("latest_score", None)
    latest_open_time = result.get("latest_open_time", None)
    latest_bins = result.get("latest_bins", {})
    latest_matched_rules = result.get("latest_matched_rules", pd.DataFrame())

    if latest_open_time is not None:
        ts = datetime.fromtimestamp(latest_open_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines.append("Последний сигнал:")
        lines.append(f"  Час (open_time): {ts}")
        lines.append(f"  Итоговый балл [-1..1]: {latest_score:+.3f}")
        if latest_matched_rules is not None and not latest_matched_rules.empty:
            lines.append("  Совпавшие правила (вклад):")
            for _, r in latest_matched_rules[latest_matched_rules["matched"]].iterrows():
                lines.append(f"    {r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}  "
                             f"raw={r['raw']:+.3f}, weight={r['weight']:.2f}, contrib={r['contribution']:+.3f}")
        else:
            lines.append("  Совпавших топ-правил нет, использована базовая оценка.")
        lines.append("")

    return "\n".join(lines)