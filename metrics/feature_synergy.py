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
                # если конвертация прошла, заменяем столбец
                df[c] = converted
            except Exception:
                # оставить как есть (категориальный/строковый)
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

    # Индекс по open_time для быстрого джойна
    k = df_klines.set_index("open_time").sort_index()
    # Чтобы посчитать next_close, построим сдвиг:
    k["next_close"] = k["close"].shift(-1)

    # Мёрдж 1:1 по open_time
    m = df_feats.merge(
        k[["open", "close", "next_close"]],
        left_on="open_time", right_index=True,
        how="inner",
    ).copy()

    # Доходности и таргеты
    m["ret_same_hour"] = (m["close"] - m["open"]) / m["open"]
    m["ret_next_hour"] = (m["next_close"] - m["open"]) / m["open"]

    def labeler(x: float) -> str:
        if pd.isna(x) or abs(x) < 1e-12:
            return "FLAT"
        return "UP" if x > 0 else "DOWN"

    m["target_same_hour"] = m["ret_same_hour"].apply(labeler)
    m["target_next_hour"] = m["ret_next_hour"].apply(labeler)

    # Последняя свеча не имеет next_close — выкинем такие строки для next_hour анализа
    m = m.dropna(subset=["ret_same_hour", "ret_next_hour"]).reset_index(drop=True)
    return m


# ==============================
#    Поиск «связок признаков»
# ==============================

def _bin_features(df: pd.DataFrame, cols: List[str], bins: int = 3) -> Tuple[pd.DataFrame, Dict[str, pd.IntervalIndex]]:
    """
    Квантильное бинингование признаков. Возвращает:
      - df_binned: DataFrame с *_bin колонками (строки 'low'/'mid'/'high' при bins=3)
      - cut_map: границы бинов для каждого признака
    """
    df_binned = df.copy()
    cut_map = {}
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
        # пропустим константные/нечисловые
        if not np.issubdtype(df_binned[c].dtype, np.number):
            continue
        if df_binned[c].nunique(dropna=True) < bins:
            continue
        try:
            cats, bins_idx = pd.qcut(df_binned[c], q=bins, labels=labels, duplicates="drop", retbins=True)
            df_binned[c + "_bin"] = cats.astype(str)
            cut_map[c] = bins_idx
        except Exception:
            # если qcut неудачен (много дублей), пропустим
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
    }



# ==============================
#      Главный конвейер
# ==============================

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
      4) поиск связок признаков.
    Возвращает словари с таблицами 'base', 'pairs', 'triples', а также
    'labeled' с объединёнными данными на каждый час/наблюдение.
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

    packs["labeled"] = df_labeled
    return packs


# ==============================
#     Утилиты и пример запуска
# ==============================

def pretty_print_synergies(result: Dict[str, pd.DataFrame]) -> None:
    """
    Печатает базовую статистику и ТОП-связки.
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


def format_synergies(result: Dict[str, pd.DataFrame], diagnostics_top: int = 10) -> str:
    """
    Формирует человекочитаемую строку-отчёт по результатам анализа связок,
    аналогичную pretty_print_synergies, но без печати (возвращает str).

    :param result: словарь с ключами 'base', 'pairs', 'triples', 'pair_support'
    :param diagnostics_top: сколько строк показать в диагностике поддержки, если пары не найдены
    :return: многострочная строка отчёта
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
    lines.append("")  # пустая строка

    def _fmt_row_pair(r: pd.Series) -> str:
        return (f"[{int(r['n']):>4}] z={r['z']:+5.2f} lift={r['lift']:+.3f} "
                f"ret_lift={r['ret_lift']:+.5f}  "
                f"{r['feat_a']}={r['bin_a']} & {r['feat_b']}={r['bin_b']}")

    # Пары
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

    # Тройки (если есть)
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

    return "\n".join(lines)