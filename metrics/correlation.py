# -*- coding: utf-8 -*-
"""
Comprehensive feature ↔ market analysis with STRICT open→open alignment.
All feature timestamps are treated as values AT the hour open (UTC).
Forward returns are computed from Open(t) to Open(t+h).
"""

from __future__ import annotations
import os
import time
from typing import Any, Dict, List, Tuple, Optional, Iterable
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
from dateutil import parser as dtparser
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.covariance import GraphicalLassoCV
from sklearn.decomposition import PCA
from sklearn.feature_selection import mutual_info_regression

# ----- Константы/ключи -----

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1h"
MS_IN_HOUR = 60 * 60 * 1000

INDICATOR_KEYS = [
    "price_oi_funding", "basis", "flows", "orderbook", "cross",
    "calendar", "sentiment", "breadth", "stables", "macro",
    "news_score", "rr25", "iv", "overall",
]

DEFAULT_GROUPS: Dict[str, List[str]] = {
    "microstructure": ["flows", "orderbook", "price_oi_funding", "cross"],
    "sentiment_news": ["sentiment", "calendar", "news_score"],
    "breadth_stables": ["breadth", "stables"],
    "options": ["rr25", "iv"],
    "macro": ["macro"],
    "composite": ["overall"],
}

# ----- Вспомогательные (парсинг, загрузка) -----

def _parse_time_utc(s: str) -> datetime:
    s = s.strip()
    try:
        dt = datetime.strptime(s, "%d-%m-%y %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.replace(minute=0, second=0, microsecond=0)
    except Exception:
        pass
    try:
        dt = dtparser.parse(s)
        dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        return dt.replace(minute=0, second=0, microsecond=0)
    except Exception:
        pass
    try:
        dt = datetime.strptime(s, "%d-%m-%Y %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.replace(minute=0, second=0, microsecond=0)
    except Exception:
        pass
    raise ValueError(f"Не удалось распарсить время: '{s}'")


def _fetch_binance_klines_1h_utc(start_dt: datetime, end_dt: datetime, session: Optional[requests.Session] = None) -> pd.DataFrame:
    if start_dt.tzinfo is None or end_dt.tzinfo is None:
        raise ValueError("start_dt и end_dt должны быть tz-aware в UTC.")
    if start_dt > end_dt:
        raise ValueError("start_dt > end_dt.")
    s = session or requests.Session()
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    all_rows: List[List[Any]] = []
    cur = start_ms
    while cur <= end_ms:
        params = {"symbol": SYMBOL, "interval": INTERVAL, "startTime": cur, "endTime": end_ms, "limit": 1000}
        r = s.get(BINANCE_KLINES_URL, params=params, timeout=10)
        if r.status_code != 200:
            raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")
        batch = r.json()
        if not batch:
            break
        all_rows.extend(batch)
        last_open = batch[-1][0]
        next_cur = last_open + MS_IN_HOUR
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
    return df


def _assert_strict_open_alignment(metric_times: pd.DatetimeIndex, candle_open_index: pd.DatetimeIndex):
    if any(ts.tzinfo is None for ts in metric_times):
        raise AssertionError("Все времена признаков должны быть tz-aware (UTC).")
    not_hour = [ts for ts in metric_times if (ts.minute, ts.second, ts.microsecond) != (0, 0, 0)]
    if not_hour:
        sample = ", ".join(t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in not_hour[:10])
        more = "" if len(not_hour) <= 10 else f" ... и ещё {len(not_hour)-10}"
        raise AssertionError(f"Обнаружены метки НЕ на начале часа: {sample}{more}")
    missing = [ts for ts in metric_times if ts not in candle_open_index]
    if missing:
        sample = ", ".join(t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in missing[:10])
        more = "" if len(missing) <= 10 else f" ... и ещё {len(missing)-10}"
        raise AssertionError(f"Для некоторых меток признаков нет свечи Binance (open_time): {sample}{more}")

# ----- Построение метрик -----

def _build_metrics_frame(data_list: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, datetime, datetime]:
    if not isinstance(data_list, list) or not data_list:
        raise ValueError("Ожидается непустой список словарей с метриками.")
    rows = []
    for d in data_list:
        if "time" not in d:
            raise ValueError("В одном из словарей отсутствует ключ 'time'.")
        dt = _parse_time_utc(str(d["time"]))
        row = {"time": dt}
        for k in INDICATOR_KEYS:
            v = d.get(k, None)
            try:
                row[k] = float(v) if v is not None else np.nan
            except Exception:
                row[k] = np.nan
        rows.append(row)

    df = pd.DataFrame(rows).sort_values("time")
    df = df.groupby("time", as_index=False).last()
    df.set_index("time", inplace=True)
    df.index = pd.to_datetime(df.index, utc=True)
    df.sort_index(inplace=True)

    if "news_score" in df.columns:
        df["news_score"] = pd.to_numeric(df["news_score"], errors="coerce") / 100.0

    t_min, t_max = df.index.min(), df.index.max()
    if pd.isna(t_min) or pd.isna(t_max):
        raise ValueError("Не удалось определить диапазон времени метрик.")
    return df, t_min, t_max

# ----- Финансовые расчёты -----

def _forward_returns(price_open: pd.Series, horizons: Iterable[int]) -> pd.DataFrame:
    """
    Форвардные доходности по OPEN→OPEN:
      r_h(t) = Open[t+h] / Open[t] - 1
    Индекс результата — t (час открытия).
    """
    out = {}
    for h in horizons:
        out[f"ret_fwd_{h}h"] = (price_open.shift(-h) / price_open) - 1.0
    return pd.DataFrame(out, index=price_open.index)

def _zscore(df: pd.DataFrame) -> pd.DataFrame:
    return (df - df.mean(axis=0)) / df.std(axis=0, ddof=0)

# ----- Графики (heatmap/rolling/partial/PCA/MI) -----

def _ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def _heatmap(ax, data: np.ndarray, xlabels: List[str], ylabels: List[str], title: str):
    im = ax.imshow(data, aspect='auto', origin='lower', interpolation='nearest')
    ax.set_xticks(np.arange(len(xlabels))); ax.set_xticklabels(xlabels, rotation=45, ha='right')
    ax.set_yticks(np.arange(len(ylabels))); ax.set_yticklabels(ylabels)
    ax.set_title(title, fontsize=14); ax.grid(False)
    cbar = plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04); cbar.ax.set_ylabel('corr / MI', rotation=90, va='center')

def _plot_corr_heatmap(df_corr: pd.DataFrame, title: str, out_path: str, figsize=(18, 12), dpi=220):
    import matplotlib.pyplot as plt
    plt.close("all")
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    _heatmap(ax, df_corr.values, list(df_corr.columns), list(df_corr.index), title)
    fig.tight_layout(); _ensure_dir(out_path); fig.savefig(out_path, bbox_inches="tight"); plt.close(fig)

def _plot_rolling_corr(features_df: pd.DataFrame, ret_target: pd.Series, top_features: List[str],
                       window: int, out_path: str, figsize=(24, 14), dpi=220):
    import matplotlib.pyplot as plt
    aligned = pd.concat([features_df[top_features], ret_target.rename("target")], axis=1).dropna()
    roll = aligned.rolling(window=window, min_periods=max(20, window//3)).corr()
    plt.close("all"); fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    ax.set_title(f"Rolling correlations (window={window}h, OPEN→OPEN)", fontsize=16)
    ax.axhline(0.0, color="#bbbbbb", linestyle='--', linewidth=1.0)
    for feat in top_features:
        series = roll.xs(feat, level=1)["target"]
        ax.plot(series.index, series.values, linewidth=1.4, label=feat)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0))
    ax.set_ylabel("Corr with target"); ax.grid(True, linestyle='--', alpha=0.35)
    fig.autofmt_xdate(); _ensure_dir(out_path); fig.tight_layout(); fig.savefig(out_path, bbox_inches="tight"); plt.close(fig)

def _plot_partial_corr_network(partial_corr: np.ndarray, names: List[str], out_path: str,
                               figsize=(22, 16), dpi=220, thr: float = 0.20):
    import matplotlib.pyplot as plt
    n = len(names)
    angles = np.linspace(0, 2*np.pi, n, endpoint=False)
    coords = np.c_[np.cos(angles), np.sin(angles)]
    plt.close("all"); fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    ax.set_title(f"Partial correlation network (|pcorr| ≥ {thr:.2f})", fontsize=16); ax.axis('off')
    for i in range(n):
        for j in range(i+1, n):
            w = partial_corr[i, j]
            if abs(w) >= thr:
                ax.plot([coords[i,0], coords[j,0]], [coords[i,1], coords[j,1]],
                        linewidth=1.0 + 4.0*abs(w), alpha=0.6, color="#1f77b4" if w>0 else "#d62728")
    ax.scatter(coords[:,0], coords[:,1], s=600, c="#f5f5f5", edgecolors="#444444", linewidths=1.2)
    for (x, y), name in zip(coords, names):
        ax.text(x, y, name, ha="center", va="center", fontsize=10)
    _ensure_dir(out_path); fig.tight_layout(); fig.savefig(out_path, bbox_inches="tight"); plt.close(fig)

def _pca_biplot(features_std: pd.DataFrame, target_ret_6h: pd.Series, out_path: str, figsize=(20, 14), dpi=220):
    import matplotlib.pyplot as plt
    df = pd.concat([features_std, target_ret_6h.rename("ret6h")], axis=1).dropna()
    if df.shape[0] < 10:
        return
    X = df[features_std.columns].values; y = df["ret6h"].values
    pca = PCA(n_components=2); X2 = pca.fit_transform(X); comps = pca.components_.T
    q = np.nanpercentile(y, [33.33, 66.67]); colors = np.where(y <= q[0], -1, np.where(y >= q[1], 1, 0))
    plt.close("all"); fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    ax.set_title("PCA biplot (colored by OPEN→OPEN ret_fwd_6h terciles)", fontsize=16)
    ax.scatter(X2[colors==-1,0], X2[colors==-1,1], s=10, alpha=0.5, label="low ret6h")
    ax.scatter(X2[colors== 0,0], X2[colors== 0,1], s=10, alpha=0.5, label="mid ret6h")
    ax.scatter(X2[colors== 1,0], X2[colors== 1,1], s=10, alpha=0.5, label="high ret6h")
    for i, name in enumerate(features_std.columns):
        ax.arrow(0, 0, comps[i,0]*2.5, comps[i,1]*2.5, head_width=0.03, head_length=0.05, fc='k', ec='k', alpha=0.6)
        ax.text(comps[i,0]*2.7, comps[i,1]*2.7, name, fontsize=9, ha='center', va='center')
    ax.set_xlabel("PC1"); ax.set_ylabel("PC2"); ax.legend(loc="upper right"); ax.grid(True, linestyle="--", alpha=0.3)
    _ensure_dir(out_path); fig.tight_layout(); fig.savefig(out_path, bbox_inches="tight"); plt.close(fig)

# ----- Основной анализ -----

def analyze_features_vs_market(
    data_list: List[Dict[str, Any]],
    out_dir: str = "./analysis_out",
    horizons_hours: Tuple[int, ...] = (1, 3, 6, 12, 24),
    rolling_window_hours: int = 168,
    groups: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    """
    STRICT alignment to hour-open (UTC). Features at time t describe state AT t (open).
    Forward returns computed OPEN→OPEN.
    Returns dict with saved image paths and key tables.
    """
    groups = groups or DEFAULT_GROUPS
    os.makedirs(out_dir, exist_ok=True)

    # 1) Метрики и границы
    metrics_df, t_min, t_max = _build_metrics_frame(data_list)

    # 2) Свечи: расширим на максимальный горизонт
    pad = max(horizons_hours) + 2
    start_dt = (t_min - timedelta(hours=pad)).replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end_dt   = (t_max + timedelta(hours=pad)).replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    candles = _fetch_binance_klines_1h_utc(start_dt, end_dt)

    # 3) Жёсткая проверка: метрики == open_time свечей
    _assert_strict_open_alignment(metrics_df.index, candles.index)

    # 4) OPEN→OPEN форвардные доходности
    ret_df = _forward_returns(candles["open"], horizons_hours)

    # 5) Рабочее окно ровно по меткам признаков
    full_df = pd.concat([metrics_df, ret_df], axis=1).loc[metrics_df.index.min():metrics_df.index.max()]
    feat_df = full_df[INDICATOR_KEYS]
    feat_df_z = (feat_df - feat_df.mean()) / feat_df.std(ddof=0)

    ret_cols = [c for c in full_df.columns if c.startswith("ret_fwd_")]
    ret_work = full_df[ret_cols]

    # 6) Корреляции признак↔доходность (Пирсон/Спирмен)
    corr_pearson = pd.DataFrame(index=INDICATOR_KEYS, columns=ret_cols, dtype=float)
    corr_spearman = pd.DataFrame(index=INDICATOR_KEYS, columns=ret_cols, dtype=float)
    for f in INDICATOR_KEYS:
        for r in ret_cols:
            pair = pd.concat([feat_df_z[f], ret_work[r]], axis=1).dropna()
            corr_pearson.loc[f, r] = pair[f].corr(pair[r], method='pearson') if len(pair) >= 10 else np.nan
            corr_spearman.loc[f, r] = pair[f].corr(pair[r], method='spearman') if len(pair) >= 10 else np.nan

    # 7) Межпризнаковые корреляции
    corr_feat_feat = feat_df_z.corr(method='pearson')

    # 8) Top-features по |corr| для целевого горизонта (6h если есть)
    target_col = "ret_fwd_6h" if "ret_fwd_6h" in ret_cols else ret_cols[min(2, len(ret_cols)-1)]
    abs_r = corr_pearson[target_col].abs().sort_values(ascending=False).dropna()
    top_features = list(abs_r.head(6).index) if not abs_r.empty else INDICATOR_KEYS[:6]

    # 9) Группы (z-среднее) и их корреляции
    group_scores = {}
    for g, keys in (groups or {}).items():
        cols = [k for k in keys if k in feat_df_z.columns]
        if cols:
            group_scores[g] = feat_df_z[cols].mean(axis=1)
    group_scores_df = pd.DataFrame(group_scores)
    group_corr = group_scores_df.corr() if not group_scores_df.empty else pd.DataFrame()
    group_ret_corr = pd.DataFrame(index=group_scores_df.columns, columns=ret_cols, dtype=float)
    for g in group_scores_df.columns:
        for r in ret_cols:
            pair = pd.concat([group_scores_df[g], ret_work[r]], axis=1).dropna()
            group_ret_corr.loc[g, r] = pair[g].corr(pair[r]) if len(pair) >= 10 else np.nan

    # 10) Частичные корреляции (Graphical Lasso) — по признакам
    feats_clean = feat_df_z.dropna()
    partial_corr = None
    if feats_clean.shape[0] >= feats_clean.shape[1] + 5:
        gl = GraphicalLassoCV()
        gl.fit(feats_clean.values)
        precision = gl.precision_
        d = np.sqrt(np.diag(precision))
        partial_corr = -precision / np.outer(d, d)
        np.fill_diagonal(partial_corr, 1.0)

    # 11) PCA биплот (окраска по OPEN→OPEN ret_fwd_6h)
    # 12) Взаимная информация (MI)
    mi_table = pd.DataFrame(index=INDICATOR_KEYS, columns=ret_cols, dtype=float)
    for r in ret_cols:
        y = ret_work[r].values
        for f in INDICATOR_KEYS:
            x = feat_df_z[f].values
            mask = np.isfinite(x) & np.isfinite(y)
            if mask.sum() >= 50:
                mi = mutual_info_regression(x[mask].reshape(-1,1), y[mask], random_state=42)
                mi_table.loc[f, r] = float(mi[0])
            else:
                mi_table.loc[f, r] = np.nan

    # ----- Рендер графиков -----
    paths = {}

    # (A) features vs forward returns
    p = os.path.join(out_dir, "corr_feat_ret.png")
    _plot_corr_heatmap(corr_pearson, "Pearson corr: features vs forward OPEN→OPEN returns", p)
    paths["corr_feat_ret"] = os.path.abspath(p)

    # (B) feature vs feature
    p = os.path.join(out_dir, "corr_feat_feat.png")
    _plot_corr_heatmap(corr_feat_feat, "Pearson corr: feature vs feature", p)
    paths["corr_feat_feat"] = os.path.abspath(p)

    # (C) rolling corr (top features vs target)
    p = os.path.join(out_dir, "rolling_corr.png")
    _plot_rolling_corr(feat_df_z, ret_work[target_col], top_features, rolling_window_hours, p)
    paths["rolling_corr"] = os.path.abspath(p)

    # (D) group vs returns
    if not group_scores_df.empty:
        p = os.path.join(out_dir, "group_corr_ret.png")
        _plot_corr_heatmap(group_ret_corr, "Groups vs forward OPEN→OPEN returns (Pearson)", p)
        paths["group_corr_ret"] = os.path.abspath(p)
        # (E) group vs group
        p = os.path.join(out_dir, "group_corr_group.png")
        _plot_corr_heatmap(group_corr, "Group vs group (Pearson)", p)
        paths["group_corr_group"] = os.path.abspath(p)

    # (F) partial-corr network
    if partial_corr is not None:
        p = os.path.join(out_dir, "partial_corr_network.png")
        _plot_partial_corr_network(partial_corr, list(feat_df_z.columns), p, thr=0.20)
        paths["partial_corr_network"] = os.path.abspath(p)

    # (G) PCA biplot
    p = os.path.join(out_dir, "pca_biplot.png")
    _pca_biplot(feat_df_z, ret_work.get("ret_fwd_6h", pd.Series(index=feat_df_z.index)), p)
    paths["pca_biplot"] = os.path.abspath(p)

    # (H) mutual information
    p = os.path.join(out_dir, "mi_feat_ret.png")
    _plot_corr_heatmap(mi_table, "Mutual information: features → forward OPEN→OPEN returns", p)
    paths["mi_feat_ret"] = os.path.abspath(p)

    return {
        "paths": paths,
        "tables": {
            "corr_feat_ret_pearson": corr_pearson,
            "corr_feat_ret_spearman": corr_spearman,
            "corr_feat_feat_pearson": corr_feat_feat,
            "group_scores": group_scores_df,
            "mi_feat_ret": mi_table,
        }
    }
