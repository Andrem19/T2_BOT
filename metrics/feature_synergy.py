# feature_synergy.py
# -*- coding: utf-8 -*-

import math
import time
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

    start_ms = _to_ms(_floor_hour(start_time))
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

        all_rows.extend(rows)
        last_open = rows[-1][0]
        next_start = last_open + 1
        if next_start <= cur:
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
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)
    return df


# ==============================
#      Преобразование входа
# ==============================

def indicators_to_df(indicators: List[Dict]) -> pd.DataFrame:
    """
    Из списка словарей формирует DataFrame.
    Добавляет столбец open_time (UTC, миллисекунды) как время открытия соответствующей свечи.
    Безопасное приведение типов без deprecated errors='ignore'.
    """
    if not indicators:
        raise ValueError("Список индикаторов пуст.")

    rows = []
    for row in indicators:
        if "time" not in row:
            raise ValueError("Каждый словарь обязан иметь ключ 'time'.")
        dt = _dt_parse_to_utc(str(row["time"]))
        dt = _floor_hour(dt)
        open_ms = _to_ms(dt)

        feat = {k: v for k, v in row.items() if k != "time"}
        feat["open_time"] = open_ms
        rows.append(feat)

    df = pd.DataFrame(rows)

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
    Матчит индикаторы к свече по open_time и рассчитывает:
      - ret_same_hour: (close - open)/open для той же свечи
      - ret_next_hour: (next_close - this_open)/this_open (может быть NaN для последней строки)
      - target_same_hour: 'UP'/'DOWN'/'FLAT'
      - target_next_hour: 'UP'/'DOWN'/'FLAT' (может быть NaN для последней строки)
      - has_next: bool — есть ли следующий час
    ВАЖНО: Последняя строка теперь НЕ отбрасывается, даже если нет next_close.
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
    # Для target_next_hour оставляем NaN, если ret_next_hour NaN
    m["target_next_hour"] = m["ret_next_hour"].apply(lambda x: labeler(x) if pd.notna(x) else np.nan)

    m["has_next"] = m["ret_next_hour"].notna()

    # ВНИМАНИЕ: НЕ дропаем последнюю строку — она нужна для «последнего сигнала»
    m = m.reset_index(drop=True)
    return m


# ==============================
#    Поиск «связок признаков»
# ==============================

def _labels_for_bins(n: int) -> List[str]:
    if n == 2:  return ["low","high"]
    if n == 3:  return ["low","mid","high"]
    if n == 4:  return ["q1","q2","q3","q4"]
    if n == 5:  return ["q1","q2","q3","q4","q5"]
    # fallback
    return [f"b{i+1}" for i in range(n)]

def _bin_features(df: pd.DataFrame, cols: List[str], bins: int = 3) -> Tuple[pd.DataFrame, Dict[str, np.ndarray], Dict[str, List[str]]]:
    """
    Квантильное бинингование признаков на ДАННОМ df.
    Возвращает:
      - df_binned: df + *_bin колонки
      - cut_map:   {col: np.ndarray(bin_edges)}
      - label_map: {col: список меток для этих edges}
    """
    df_binned = df.copy()
    cut_map: Dict[str, np.ndarray] = {}
    label_map: Dict[str, List[str]] = {}

    for c in cols:
        if not np.issubdtype(df_binned[c].dtype, np.number):
            continue
        if df_binned[c].nunique(dropna=True) < min(2, bins):
            continue
        try:
            cats, edges = pd.qcut(df_binned[c], q=bins, labels=None, retbins=True, duplicates="drop")
            n_bins = len(edges) - 1
            labels = _labels_for_bins(n_bins)
            df_binned[c + "_bin"] = pd.cut(df_binned[c], bins=edges, labels=labels, include_lowest=True).astype(str)
            cut_map[c] = edges
            label_map[c] = labels
        except Exception:
            continue
    return df_binned, cut_map, label_map

def _assign_bin_single(value: float, edges: np.ndarray, labels: List[str]) -> str:
    """Классифицирует одно значение по заранее известным границам и меткам."""
    try:
        cat = pd.cut(pd.Series([value]), bins=edges, labels=labels, include_lowest=True).astype(str).iloc[0]
        return str(cat)
    except Exception:
        return "nan"

def _proportion_z_test(p1: float, n1: int, p0: float, n0: int) -> float:
    """Z-тест на разность долей (апрокс. нормалью)."""
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
    """Метрики по подвыборке mask."""
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
    Ищет «связки признаков» по квантильным бинам.
    ВНИМАНИЕ: обучение ведём ТОЛЬКО на строках, где есть следующий час (has_next=True),
    чтобы не искажать базовые метрики.
    """
    if exclude_cols is None:
        exclude_cols = ["open_time", "open", "close", "next_close",
                        "ret_same_hour", "ret_next_hour",
                        "target_same_hour", "target_next_hour", "has_next"]

    # --- Обучающая часть (есть следующий час) ---
    df_train = df_labeled[df_labeled["has_next"] == True].copy()
    if df_train.empty:
        raise ValueError("Нет строк с известным 'следующим часом' для обучения правил.")

    feature_cols = [c for c in df_train.columns
                    if c not in exclude_cols
                    and np.issubdtype(df_train[c].dtype, np.number)]

    # Биннинг ТОЛЬКО на обучении
    df_binned_train, cut_map, label_map = _bin_features(df_train, feature_cols, bins=bins)

    # База (только train)
    base_up_rate = (df_binned_train[use_target] == "UP").mean()
    base_ret_mean = df_binned_train[use_return].mean()

    # Диагностика: поддержка по всем парам
    sup_rows = []
    binned_cols = [c for c in feature_cols if c + "_bin" in df_binned_train.columns]
    for a, b in itertools.combinations(binned_cols, 2):
        avs = df_binned_train[a + "_bin"].dropna().unique()
        bvs = df_binned_train[b + "_bin"].dropna().unique()
        for av in avs:
            for bv in bvs:
                n = int(((df_binned_train[a + "_bin"] == av) &
                         (df_binned_train[b + "_bin"] == bv)).sum())
                sup_rows.append({"feat_a": a, "bin_a": str(av),
                                 "feat_b": b, "bin_b": str(bv),
                                 "n": n})
    pair_support = pd.DataFrame(sup_rows).sort_values("n", ascending=False).reset_index(drop=True)

    # Поиск пар
    results_pairs = []
    for _, r in pair_support.iterrows():
        if r["n"] < min_support:
            continue
        mask = ((df_binned_train[r["feat_a"] + "_bin"] == r["bin_a"]) &
                (df_binned_train[r["feat_b"] + "_bin"] == r["bin_b"]))
        rep = _combo_report(df_binned_train, mask, base_up_rate, base_ret_mean,
                            use_target, use_return)
        if rep:
            results_pairs.append({**r.to_dict(), **rep})

    df_pairs = pd.DataFrame(results_pairs)
    if not df_pairs.empty:
        df_pairs["score"] = df_pairs["z"].abs() * np.log1p(df_pairs["n"]) + 5.0 * df_pairs["lift"].abs()
        df_pairs = df_pairs.sort_values(["score","n","z"], ascending=[False, False, False]).head(topn).reset_index(drop=True)

    # Тройки (опционально)
    results_triples = []
    if k_max >= 3 and len(binned_cols) >= 3:
        for a, b, c in itertools.combinations(binned_cols, 3):
            a_vals = df_binned_train[a + "_bin"].dropna().unique()
            b_vals = df_binned_train[b + "_bin"].dropna().unique()
            c_vals = df_binned_train[c + "_bin"].dropna().unique()
            for av in a_vals:
                for bv in b_vals:
                    for cv in c_vals:
                        mask = ((df_binned_train[a + "_bin"] == av) &
                                (df_binned_train[b + "_bin"] == bv) &
                                (df_binned_train[c + "_bin"] == cv))
                        n = int(mask.sum())
                        if n < min_support:
                            continue
                        rep = _combo_report(df_binned_train, mask, base_up_rate, base_ret_mean,
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
        "N_total": int(len(df_binned_train)),
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
        # для вычисления «последнего» сигнала:
        "df_binned_train": df_binned_train,
        "cut_map": cut_map,
        "label_map": label_map,
        "feature_cols": binned_cols,
    }


# ==============================
#      Последний сигнал (всегда по самой последней свече)
# ==============================

def _compute_latest_signal(
    df_labeled_all: pd.DataFrame,
    pairs: pd.DataFrame,
    df_binned_train: pd.DataFrame,
    cut_map: Dict[str, np.ndarray],
    label_map: Dict[str, List[str]],
    feature_cols: List[str],
) -> Dict:
    """
    Считает «последний» сигнал ДЛЯ САМОЙ ПОСЛЕДНЕЙ ЗАПИСИ (макс. open_time),
    даже если у неё нет следующего часа. Бины присваиваются по порогам cut_map/label_map,
    которые получены на обучающей части (без утечки).
    """
    if df_labeled_all.empty:
        return {
            "latest_score": 0.0,
            "latest_open_time": None,
            "latest_bins": {},
            "latest_matched_rules": pd.DataFrame(columns=["feat_a","bin_a","feat_b","bin_b","matched","weight","raw","contribution","z","n","up_rate","lift","ret_lift","score"]),
            "latest_used_fallback": True,
        }

    # Самая последняя строка (даже если has_next=False)
    last_idx = df_labeled_all["open_time"].idxmax()
    last_row = df_labeled_all.loc[last_idx]
    last_time_ms = int(last_row["open_time"])

    # Сформируем бины для последней строки по тренировочным порогам
    latest_bins: Dict[str, str] = {}
    for c in feature_cols:
        val = float(last_row[c])
        edges = cut_map.get(c, None)
        labels = label_map.get(c, None)
        if edges is not None and labels is not None:
            latest_bins[c] = _assign_bin_single(val, edges, labels)
        else:
            latest_bins[c] = "nan"

    # Если правил нет — fallback к базе train
    if pairs is None or pairs.empty:
        base_up_rate = (df_binned_train["target_next_hour"] == "UP").mean()
        base_raw = float(2.0 * base_up_rate - 1.0)
        return {
            "latest_score": float(np.clip(base_raw, -1.0, 1.0)),
            "latest_open_time": last_time_ms,
            "latest_bins": latest_bins,
            "latest_matched_rules": pd.DataFrame(columns=["feat_a","bin_a","feat_b","bin_b","matched","weight","raw","contribution","z","n","up_rate","lift","ret_lift","score"]),
            "latest_used_fallback": True,
        }

    # Голосование top-5 правил
    top5 = pairs.head(5).copy()
    rows = []
    num = 0.0
    den = 0.0
    for _, r in top5.iterrows():
        a, b = r["feat_a"], r["feat_b"]
        need_a, need_b = str(r["bin_a"]), str(r["bin_b"])
        is_a = latest_bins.get(a, None)
        is_b = latest_bins.get(b, None)
        matched = (is_a == need_a) and (is_b == need_b)

        raw = float(2.0 * r["up_rate"] - 1.0)          # направление правила [-1..1]
        weight = float(max(0.0, r.get("score", 0.0)))  # неотрицательный вес
        contrib = raw * weight if matched else 0.0

        if matched and weight > 0:
            num += contrib
            den += weight

        rows.append({
            "feat_a": a, "bin_a": need_a,
            "feat_b": b, "bin_b": need_b,
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
        used_fallback = False
    else:
        base_up_rate = (df_binned_train["target_next_hour"] == "UP").mean()
        final_score = float(np.clip(2.0 * base_up_rate - 1.0, -1.0, 1.0))
        used_fallback = True

    return {
        "latest_score": final_score,
        "latest_open_time": last_time_ms,
        "latest_bins": latest_bins,
        "latest_matched_rules": matched_df,
        "latest_used_fallback": used_fallback,
    }


# ==============================
#      Главный конвейер
# ==============================

def format_latest_signal_brief(result: Dict[str, pd.DataFrame],
                               max_rules: int = 10) -> str:
    """
    Возвращает краткий блок:
      Последний сигнал:
        Час (open_time): YYYY-MM-DD HH:MM:SS UTC
        Итоговый балл [-1..1]: +0.000
        Совпавшие правила (вклад):
          feat_a=bin_a & feat_b=bin_b  raw=+0.123, weight=1.23, contrib=+0.151
          ...
    Если совпадений нет — выводит соответствующее сообщение (с пометкой о fallback при необходимости).
    """
    from datetime import datetime, timezone
    import pandas as pd

    # Время
    ts_ms = result.get("latest_open_time")
    ts_str = (datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
              .strftime("%Y-%m-%d %H:%M:%S UTC")) if ts_ms is not None else "N/A"

    # Балл
    score = result.get("latest_score")
    score_str = f"{float(score):+.3f}" if score is not None else "N/A"

    # Совпадения / fallback
    used_fallback = bool(result.get("latest_used_fallback", False))
    matched_df = result.get("latest_matched_rules", pd.DataFrame())

    lines = []
    lines.append("Последний сигнал:")
    lines.append(f"  Час (open_time): {ts_str}")
    lines.append(f"  Итоговый балл [-1..1]: {score_str}")

    matched_any = (
        isinstance(matched_df, pd.DataFrame)
        and not matched_df.empty
        and "matched" in matched_df.columns
        and bool(matched_df["matched"].any())
    )

    if matched_any:
        # Подготовим и выведем сами правила
        m = matched_df[matched_df["matched"]].copy()

        # Если нет готового contribution — посчитаем
        if "contribution" not in m.columns and {"raw", "weight"} <= set(m.columns):
            m["contribution"] = m["raw"].astype(float) * m["weight"].astype(float)

        # Сортировка: сначала по |contribution|, иначе по weight
        if "contribution" in m.columns:
            m = m.sort_values("contribution", key=lambda s: s.abs(), ascending=False)
        elif "weight" in m.columns:
            m = m.sort_values("weight", ascending=False)

        lines.append("  Совпавшие правила (вклад):")
        cols_have = set(m.columns)
        show_metrics = {"raw", "weight", "contribution"} <= cols_have

        for _, r in m.head(max_rules).iterrows():
            rule = f"    {r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}"
            if show_metrics:
                rule += (f"  raw={float(r['raw']):+0.3f}, "
                         f"weight={float(r['weight']):.2f}, "
                         f"contrib={float(r['contribution']):+0.3f}")
            lines.append(rule)
    else:
        if used_fallback:
            lines.append("  Совпавших топ-правил нет. Использована базовая оценка обучающей выборки.")
        else:
            lines.append("  Совпавших топ-правил нет.")

    return "\n".join(lines)



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
      3) матчинг + таргеты (сохраняем последнюю строку даже без next_close),
      4) поиск связок на обучении (has_next=True),
      5) «Последний сигнал» — ВСЕГДА по самой последней записи индикаторов.
    """
    # 1) Вход
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

    # 3) Матчинг + таргеты (НЕ отбрасываем последнюю строку)
    df_labeled_all = build_targets(df_feats_raw, df_kl)
    if df_labeled_all.empty:
        raise RuntimeError("Не удалось сматчить индикаторы к свечам (несовпадение open_time?).")

    # 4) Связки (обучение только на has_next=True)
    packs = find_feature_synergies(
        df_labeled=df_labeled_all,
        use_target="target_next_hour",
        use_return="ret_next_hour",
        bins=bins,
        min_support=min_support,
        k_max=k_max,
        topn=topn,
        exclude_cols=None,
    )

    # 5) Последний сигнал для самой последней строки
    latest_info = _compute_latest_signal(
        df_labeled_all=df_labeled_all,
        pairs=packs.get("pairs"),
        df_binned_train=packs.get("df_binned_train"),
        cut_map=packs.get("cut_map"),
        label_map=packs.get("label_map"),
        feature_cols=packs.get("feature_cols", []),
    )

    # Сохраняем результаты
    packs["labeled"] = df_labeled_all
    packs["latest_score"] = latest_info["latest_score"]
    packs["latest_open_time"] = latest_info["latest_open_time"]
    packs["latest_bins"] = latest_info["latest_bins"]
    packs["latest_matched_rules"] = latest_info["latest_matched_rules"]
    packs["latest_used_fallback"] = latest_info["latest_used_fallback"]

    return packs


# ==============================
#     Утилиты и вывод
# ==============================

def pretty_print_synergies(result: Dict[str, pd.DataFrame]) -> None:
    """
    Печатает базовую статистику, ТОП-связки и «Последний сигнал».
    Чётко указывает, использован ли fallback.
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

    # ---- Последний сигнал (всегда самая последняя запись) ----
    latest_score = result.get("latest_score", None)
    latest_open_time = result.get("latest_open_time", None)
    latest_matched_rules = result.get("latest_matched_rules", pd.DataFrame())
    used_fallback = bool(result.get("latest_used_fallback", False))

    if latest_open_time is not None:
        ts = datetime.fromtimestamp(latest_open_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print("Последний сигнал:")
        print(f"  Час (open_time): {ts}")
        print(f"  Итоговый балл [-1..1]: {latest_score:+.3f}")
        if latest_matched_rules is not None and not latest_matched_rules.empty and latest_matched_rules["matched"].any():
            print("  Совпавшие правила (вклад):")
            for _, r in latest_matched_rules[latest_matched_rules["matched"]].iterrows():
                print(f"    {r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}  "
                      f"raw={r['raw']:+.3f}, weight={r['weight']:.2f}, contrib={r['contribution']:+.3f}")
        else:
            print("  Совпавших топ-правил нет.", end=" ")
            if used_fallback:
                print("Использована базовая оценка обучающей выборки.")
            else:
                print()
        print()


def format_synergies(result: Dict[str, pd.DataFrame], diagnostics_top: int = 10) -> str:
    """
    Возвращает красиво отформатированный текст отчёта (без печати),
    включая секцию «Последний сигнал» и пометку про fallback.
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
                lines.append(f"  [{int(r['n']):>4}] {r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}{mark}")
            lines.append("")
            lines.append("Рекомендации: уменьшите min_support или снизьте bins до 2, либо расширьте период данных.")
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

    latest_score = result.get("latest_score", None)
    latest_open_time = result.get("latest_open_time", None)
    latest_matched_rules = result.get("latest_matched_rules", pd.DataFrame())
    used_fallback = bool(result.get("latest_used_fallback", False))

    if latest_open_time is not None:
        ts = datetime.fromtimestamp(latest_open_time/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines.append("Последний сигнал:")
        lines.append(f"  Час (open_time): {ts}")
        lines.append(f"  Итоговый балл [-1..1]: {latest_score:+.3f}")
        if latest_matched_rules is not None and not latest_matched_rules.empty and latest_matched_rules["matched"].any():
            lines.append("  Совпавшие правила (вклад):")
            for _, r in latest_matched_rules[latest_matched_rules["matched"]].iterrows():
                lines.append(f"    {r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}  "
                             f"raw={r['raw']:+.3f}, weight={r['weight']:.2f}, contrib={r['contribution']:+.3f}")
        else:
            if used_fallback:
                lines.append("  Совпавших топ-правил нет. Использована базовая оценка обучающей выборки.")
            else:
                lines.append("  Совпавших топ-правил нет.")
        lines.append("")

    return "\n".join(lines)

