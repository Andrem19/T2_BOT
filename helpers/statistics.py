from __future__ import annotations

import os
from typing import Any, Dict, List
import matplotlib.pyplot as plt
import matplotlib.dates as mdates        # ← NEW
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta, timezone



# ──────────────────────────────────────────────────────────────────────────────
# ↓↓↓ INTERNAL HELPERS – NOT EXPORTED ↓↓↓
# ──────────────────────────────────────────────────────────────────────────────
def _profile_numeric(series: pd.Series, prefix: str) -> Dict[str, float]:
    """
    Unified descriptor for any numeric metric.
    Produces consistent set of summary statistics with a given name-prefix.
    """
    series = series.dropna()
    if series.empty:
        return {f"{prefix}_{k}": float("nan") for k in (
            "mean", "median", "min", "max", "std", "p10", "p25", "p75", "p90"
        )}

    return {
        f"{prefix}_mean":   float(series.mean()),
        f"{prefix}_median": float(series.median()),
        f"{prefix}_min":    float(series.min()),
        f"{prefix}_max":    float(series.max()),
        f"{prefix}_std":    float(series.std(ddof=0)),
        f"{prefix}_p10":    float(np.percentile(series, 10)),
        f"{prefix}_p25":    float(np.percentile(series, 25)),
        f"{prefix}_p75":    float(np.percentile(series, 75)),
        f"{prefix}_p90":    float(np.percentile(series, 90)),
    }


def _agg_basic(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Canonical block of core statistics — reused for every slice.
    Handles profit metrics **plus** newly added *RSI* and *rel_atr* fields.
    """
    out: Dict[str, Any] = {"trades": len(df)}

    # ── PROFIT ────────────────────────────────────────────────────────────
    out.update(_profile_numeric(df["profit"], "profit"))

    # Target/Win-loss/duration extras
    out.update(
        {
            "total_profit": float(df["profit"].sum()),
            "target_hit_rate": float((df["profit"] >= df["targ_pnl"]).mean()),
            "win_rate": float((df["profit"] > 0).mean()),
            "loss_rate": float((df["profit"] < 0).mean()),
            "avg_duration_hours": float(
                (df["time_close"] - df["time_open"]).dt.total_seconds().mean() / 3600
            ),
            "avg_profit_pct_of_fut": float(
                (df["profit"] / df["fut_entrypx"].abs()).mean()
            ),
        }
    )

    # ── RSI & rel_atr ─────────────────────────────────────────────────────
    if "rsi" in df.columns:
        out.update(_profile_numeric(df["rsi"], "rsi"))
        # Correlation with profit (if ≥2 non-NaN rows)
        if df[["profit", "rsi"]].dropna().shape[0] > 1:
            out["corr_profit_rsi"] = float(df[["profit", "rsi"]].corr().iloc[0, 1])

    if "rel_atr" in df.columns:
        out.update(_profile_numeric(df["rel_atr"], "rel_atr"))
        if df[["profit", "rel_atr"]].dropna().shape[0] > 1:
            out["corr_profit_rel_atr"] = float(
                df[["profit", "rel_atr"]].corr().iloc[0, 1]
            )

    return out


# ──────────────────────────────────────────────────────────────────────────────
# ↑↑↑ INTERNAL HELPERS – NOT EXPORTED ↑↑↑
# ──────────────────────────────────────────────────────────────────────────────


def compute_trade_stats(
    trades: List[Dict[str, Any]],
    n_days: int,
    detailed: bool = False,
    tz: str | timezone = "UTC",
) -> Dict[str, Any]:
    """
    Build a comprehensive statistical report for combined **futures-option**
    deals — now including *RSI* and *rel_atr* factors.

    Parameters
    ----------
    trades
        Sequence of raw trade dictionaries.
    n_days
        Look-back window (calendar days) counting back from «now».
    detailed
        ``False`` → compact summary; ``True`` → deep multi-dimensional report.
    tz
        Time-zone for ‘now’ and window start. Accepts zone-name or tzinfo.

    Returns
    -------
    Dict[str, Any]
        JSON-serialisable hierarchy of statistics. Empty dict if no rows in
        the selected window.
    """
    df = pd.DataFrame(trades)
    if df.empty:
        return {}

    # Parse timestamps ↦ UTC
    df["time_open"] = pd.to_datetime(df["time_open"], utc=True, errors="coerce")
    df["time_close"] = pd.to_datetime(df["time_close"], utc=True, errors="coerce")
    df["effective_time"] = df["time_close"].fillna(df["time_open"])

    # Window filtering
    tzinfo = tz if isinstance(tz, timezone) else timezone.utc
    now = datetime.now(timezone.utc).astimezone(tzinfo)
    window_start = now - timedelta(days=n_days)
    df = df[df["effective_time"] >= window_start]

    if df.empty:
        return {}

    # ── TOP-LEVEL SUMMARY ──────────────────────────────────────────────────
    stats: Dict[str, Any] = {
        "period": {
            "now": now.isoformat(),
            "lookback_days": n_days,
            "window_start": window_start.isoformat(),
            "data_rows": len(df),
        },
        "aggregate": _agg_basic(df),
    }

    if not detailed:
        return stats  # ─── early exit in concise mode ───

    # ── ONE-DIMENSIONAL BREAKDOWNS ────────────────────────────────────────
    stats["by_stage"] = {
        stage: _agg_basic(g) for stage, g in df.groupby("stage", dropna=False)
    }
    stats["by_type_close"] = {
        tcls: _agg_basic(g) for tcls, g in df.groupby("type_close", dropna=False)
    }
    stats["by_base_symb"] = {
        sym: _agg_basic(g) for sym, g in df.groupby("base_symb", dropna=False)
    }
    stats["by_option_name"] = {
        opt: _agg_basic(g) for opt, g in df.groupby("name_opt", dropna=False)
    }

    # ── CROSS-DIMENSIONAL VIEW (stage × type_close) ───────────────────────
    stats["stage×type"] = {
        f"{stg}|{tcls}": _agg_basic(sub)
        for (stg, tcls), sub in df.groupby(["stage", "type_close"], dropna=False)
    }

    # ── DAILY TIME-SERIES SNAPSHOT ────────────────────────────────────────
    daily_tbl = (
        df.groupby(df["effective_time"].dt.date)
        .agg(
            trades=("profit", "count"),
            net_profit=("profit", "sum"),
            mean_profit=("profit", "mean"),
            mean_rsi=("rsi", "mean"),
            mean_rel_atr=("rel_atr", "mean"),
        )
        .to_dict("index")
    )
    stats["daily"] = {str(k): v for k, v in daily_tbl.items()}  # JSON-friendly

    # ── BIVARIATE DIAGNOSTICS (whole window) ───────────────────────────────
    corr_block: Dict[str, float] = {}
    if df[["profit", "rsi"]].dropna().shape[0] > 1:
        corr_block["profit_vs_rsi"] = float(df[["profit", "rsi"]].corr().iloc[0, 1])
    if df[["profit", "rel_atr"]].dropna().shape[0] > 1:
        corr_block["profit_vs_rel_atr"] = float(
            df[["profit", "rel_atr"]].corr().iloc[0, 1]
        )
    if corr_block:
        stats["correlations"] = corr_block

    return stats




def visualize_trade_stats(  # ← главное “API”
    trades: List[Dict[str, Any]],
    stats: Dict[str, Any],
    out_dir: str = "charts",
) -> Dict[str, str]:
    """
    Строит ключевые графики по сделкам и сохраняет их в PNG-файлы.

    См. подробности в docstring исходной версии. Логика осталась прежней,
    но три графика переработаны:

      • Cumulative PnL – теперь начинается с нуля (step-plot «post»).
      • Distribution of Trade PnL – раздельные гистограммы profit / loss,
        добавлены легенда и ось 0.
      • Daily Net Profit – столбцы окрашиваются по знаку, добавлены ось 0,
        форматирование дат через mdates.

    Остальные графики (RSI / rel_ATR) без изменений.
    """
    # ─────────────────────────────────────────────────────────────── setup
    os.makedirs(out_dir, exist_ok=True)
    df = pd.DataFrame(trades)
    if df.empty:
        return {}

    # корректное время сделки (закрытие > открытие, если оно NaT)
    df["effective_time"] = (
        pd.to_datetime(df["time_close"], utc=True, errors="coerce")
        .fillna(pd.to_datetime(df["time_open"], utc=True, errors="coerce"))
    )
    df = df.sort_values("effective_time")

    chart_paths: Dict[str, str] = {}

    # Имя периода для заголовков
    period_label = ""
    if isinstance(stats, dict):
        wnd = stats.get("period", {})
        if wnd:
            period_label = f' ({wnd.get("window_start", "")} … {wnd.get("now", "")})'

    # ─────────────────────────────────────────────── 1. Cumulative PnL curve
    pnl_curve_path = os.path.join(out_dir, "pnl_curve.png")
    cum_profit = df["profit"].cumsum().to_numpy()
    times = df["effective_time"].to_numpy()

    # вставляем начальную точку «0» в момент первой сделки
    times_ext = np.insert(times, 0, times[0])
    cum_ext = np.insert(cum_profit, 0, 0.0)

    plt.figure()
    plt.step(times_ext, cum_ext, where="post", marker="o")
    plt.axhline(0, linewidth=0.8, color="grey")
    plt.xlabel("Time")
    plt.ylabel("Cumulative PnL")
    plt.title(f"Cumulative PnL{period_label}")
    plt.grid(True, axis="y")
    plt.tight_layout()
    plt.savefig(pnl_curve_path, dpi=150, bbox_inches="tight")
    plt.close()
    chart_paths["pnl_curve"] = os.path.abspath(pnl_curve_path)

    # ───────────────────────────────────────── 2. Distribution of Trade PnL
    hist_path = os.path.join(out_dir, "profit_histogram.png")
    pos = df["profit"][df["profit"] > 0]
    neg = df["profit"][df["profit"] <= 0]
    bins = max(5, min(50, len(df)))

    plt.figure()
    plt.hist(pos, bins=bins, alpha=0.7, color="green",
             label=f"Profitable ({len(pos)})")
    plt.hist(neg, bins=bins, alpha=0.7, color="red",
             label=f"Unprofitable ({len(neg)})")
    plt.axvline(0, color="black", linewidth=1)
    plt.xlabel("Trade Profit")
    plt.ylabel("Frequency")
    plt.title(f"Distribution of Trade PnL{period_label}")
    plt.legend()
    plt.grid(True, axis="y")
    plt.tight_layout()
    plt.savefig(hist_path, dpi=150, bbox_inches="tight")
    plt.close()
    chart_paths["profit_histogram"] = os.path.abspath(hist_path)

    # ──────────────────────────────────────────────── 3. Daily Net Profit
    daily_path = os.path.join(out_dir, "daily_net_profit.png")
    df["date"] = df["effective_time"].dt.date
    daily_profit = df.groupby("date")["profit"].sum()
    dates = pd.to_datetime(daily_profit.index)

    colors = ["green" if v > 0 else "red" for v in daily_profit.values]

    plt.figure()
    plt.bar(dates, daily_profit.values, color=colors)
    plt.axhline(0, linewidth=0.8, color="black")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xlabel("Date")
    plt.ylabel("Net Profit")
    plt.title(f"Daily Net Profit{period_label}")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, axis="y", linestyle=":")
    plt.tight_layout()
    plt.savefig(daily_path, dpi=150, bbox_inches="tight")
    plt.close()
    chart_paths["daily_net_profit"] = os.path.abspath(daily_path)

    # ────────────────────────────────────────────────── 4. RSI scatter
    if "rsi" in df.columns and df["rsi"].notna().any():
        rsi_path = os.path.join(out_dir, "rsi_vs_profit.png")
        plt.figure()
        plt.scatter(df["rsi"], df["profit"])
        plt.xlabel("RSI at Entry")
        plt.ylabel("Profit")
        plt.title(f"RSI vs Profit{period_label}")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(rsi_path, dpi=150, bbox_inches="tight")
        plt.close()
        chart_paths["rsi_vs_profit"] = os.path.abspath(rsi_path)

    # ─────────────────────────────────────────────── 5. rel_ATR scatter
    if "rel_atr" in df.columns and df["rel_atr"].notna().any():
        atr_path = os.path.join(out_dir, "rel_atr_vs_profit.png")
        plt.figure()
        plt.scatter(df["rel_atr"], df["profit"])
        plt.xlabel("rel_atr at Entry")
        plt.ylabel("Profit")
        plt.title(f"rel_atr vs Profit{period_label}")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(atr_path, dpi=150, bbox_inches="tight")
        plt.close()
        chart_paths["rel_atr_vs_profit"] = os.path.abspath(atr_path)

    return chart_paths

