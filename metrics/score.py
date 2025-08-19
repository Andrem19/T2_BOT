# snapshot_scoring.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple

def _finite(x: Any) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception:
        return False

def _val(x: Any, default: float = float("nan")) -> float:
    try:
        f = float(x)
        return f if math.isfinite(f) else default
    except Exception:
        return default

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _sign(x: float) -> int:
    return 1 if x > 0 else -1 if x < 0 else 0

def _safe_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _ratio_to_score(ratio: Optional[float], center: float = 1.0, width: float = 0.2, invert: bool = False) -> float:
    """Преобразует отношение >0 в [-1..+1] вокруг center. width задаёт 'полуширину' до насыщения."""
    if ratio is None or not _finite(ratio):
        return 0.0
    x = (ratio - center) / max(1e-12, width)  # ~0 => нейтрально, +1 => насыщение вверх
    s = _clamp(x, -1.0, 1.0)
    return -s if invert else s

def _norm_change(x: Optional[float], width: float) -> float:
    """Нормализует изменение (в долях) в [-1..1] с полушириной width."""
    if x is None or not _finite(x):
        return 0.0
    return _clamp(x / width, -1.0, 1.0)

def score_snapshot(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    Принимает снимок (как у вас) и возвращает:
    {
      "per_metric": { "<metric>": {"score": float[-1..1], "weight": float, "weight_used": float, "label": str, "explanation": str, "inputs": {...}}, ... },
      "overall": {"score": float[-1..1], "index": int[-100..100], "label": "bullish|bearish|neutral", "suggestion": "long|short|flat", "confidence": float[0..1]},
      "weights": {"nominal_sum": 1.0, "used_sum": float}
    }
    """
    # -------- 1) Извлекаем исходные значения
    pc  = _val(_safe_get(report, "price", "change_pct"), 0.0)                    # изменение цены (доля)
    oic = _val(_safe_get(report, "open_interest", "oi_change_pct"), 0.0)         # изменение OI (доля)
    fr  = _val(_safe_get(report, "funding", "last_funding_rate"), 0.0)           # фандинг (доля за период)
    who = _safe_get(report, "funding", "who_pays_now")                            # "longs_pay_shorts"/"shorts_pay_longs"/None

    basis_now = _val(_safe_get(report, "basis", "basis_now"))
    basis_then= _val(_safe_get(report, "basis", "basis_then_open"))
    basis_d   = basis_now - basis_then if (_finite(basis_now) and _finite(basis_then)) else float("nan")

    spot_net = _val(_safe_get(report, "flows", "spot", "taker_net_quote"))
    perp_net = _val(_safe_get(report, "flows", "perp", "taker_net_quote"))
    total_net= spot_net + perp_net

    ob_tilt_05 = _val(_safe_get(report, "orderbook", "bands", "0.50%", "tilt_bid_over_ask"))
    ob_tilt_10 = _val(_safe_get(report, "orderbook", "bands", "1.00%", "tilt_bid_over_ask"))

    # cross exchanges (усредним базис по доступным)
    x_basis_vals = []
    for ex in ("binance", "bybit", "okx", "deribit"):
        b = _safe_get(report, "x_perp", ex, "basis")
        if _finite(b):
            x_basis_vals.append(float(b))
    x_basis_avg = sum(x_basis_vals)/len(x_basis_vals) if x_basis_vals else float("nan")

    # calendar basis
    cb_c_now  = _val(_safe_get(report, "calendar_basis", "current_quarter", "basis_now"))
    cb_c_then = _val(_safe_get(report, "calendar_basis", "current_quarter", "basis_then_open"))
    cb_n_now  = _val(_safe_get(report, "calendar_basis", "next_quarter", "basis_now"))
    cb_n_then = _val(_safe_get(report, "calendar_basis", "next_quarter", "basis_then_open"))
    cb_c_d    = (cb_c_now - cb_c_then) if (_finite(cb_c_now) and _finite(cb_c_then)) else float("nan")
    cb_n_d    = (cb_n_now - cb_n_then) if (_finite(cb_n_now) and _finite(cb_n_then)) else float("nan")

    # sentiment ratios
    r_taker = _safe_get(report, "sentiment", "taker_buy_sell_ratio")
    r_glob  = _safe_get(report, "sentiment", "global_long_short_ratio")
    r_topa  = _safe_get(report, "sentiment", "top_trader_accounts_ratio")
    r_topp  = _safe_get(report, "sentiment", "top_trader_positions_ratio")

    # breadth
    adv_dec = _val(_safe_get(report, "breadth", "advance_decline"))
    considered = max(1.0, _val(_safe_get(report, "breadth", "considered"), 50.0))

    # stables (среднее отклонение)
    stabs = _safe_get(report, "stablecoins") or {}
    devs = []
    for sym, rec in stabs.items():
        d = _val(rec.get("deviation_from_1"))
        if _finite(d):
            devs.append(d)
    stab_mean_dev = (sum(devs)/len(devs)) if devs else 0.0

    # macro
    macro_score = _val(_safe_get(report, "macro_weather", "macro_lean_score"))
    macro_norm  = _clamp(macro_score/3.0, -1.0, 1.0) if _finite(macro_score) else 0.0

    # -------- 2) Считаем суб-оценки в [-1..+1]

    # 2.1 Цена + OI + Фандинг (совместная картина)
    bull = bear = 0.0
    # направление цены
    if pc > 0:
        bull += _norm_change(pc, 0.003)  # ~0.3% насыщение
    elif pc < 0:
        bear += _norm_change(-pc, 0.003)
    # синергия с OI
    if _finite(oic) and oic != 0:
        if _sign(pc) == _sign(oic) and pc != 0:
            # в ту же сторону — усиливаем
            if pc > 0: bull += _norm_change(abs(oic), 0.01)
            else:      bear += _norm_change(abs(oic), 0.01)
        else:
            # в противофазе — ослабляем текущий ход
            if pc > 0: bear += 0.5 * _norm_change(abs(oic), 0.01)
            elif pc < 0: bull += 0.5 * _norm_change(abs(oic), 0.01)
    # фандинг как модератор качества
    if _finite(fr) and fr != 0 and pc != 0:
        fn = _norm_change(abs(fr), 0.0005)  # ~0.05% насыщение
        if pc > 0:
            # рост при положительном фандинге — хрупче
            if fr > 0: bear += 0.6 * fn
            else:      bull += 0.8 * fn
        else:  # pc < 0
            # падение при положительном фандинге — хуже для лонгов (медвежий плюс)
            if fr > 0: bear += 0.8 * fn
            else:      bull += 0.4 * fn  # шорты платят — слегка смягчает падение
    cof_den = max(1e-9, bull + bear)
    s_price_oi_funding = _clamp((bull - bear)/cof_den, -1.0, 1.0)
    exp_price_oi_funding = "Цена/OI/Фандинг: " \
        f"priceΔ={pc:.4f}, OIΔ={oic:.4f}, funding={fr:.6f}, итог {s_price_oi_funding:+.2f}"

    # 2.2 Базис перпа (динамика)
    s_basis = 0.0
    if _finite(basis_d):
        s_basis = _norm_change(basis_d, 0.001)  # 0.10% шаг к насыщению
    exp_basis = f"Базис перпа: Δ={basis_d:.6f} → {s_basis:+.2f}"

    # 2.3 Потоки спот vs перп
    s_flows = 0.0
    if _finite(total_net) and _finite(spot_net) and _finite(perp_net):
        dir_sign = _sign(total_net)
        mag = _clamp(abs(total_net) / 200_000_000.0, -1.0, 1.0)  # 200 млн $ → насыщение
        sup = 1 if _sign(spot_net) == dir_sign and dir_sign != 0 else (-1 if _sign(spot_net) == -dir_sign and dir_sign != 0 else 0)
        factor = 0.7 + 0.3 * sup  # 0.4…1.0
        s_flows = dir_sign * mag * factor
    exp_flows = f"Потоки: spotNet={spot_net:.0f}, perpNet={perp_net:.0f}, итог {s_flows:+.2f}"

    # 2.4 Книга (0.5% кольцо важнее)
    s_ob = 0.0
    if _finite(ob_tilt_05):
        s_ob = _clamp((ob_tilt_05 - 1.0) / 0.25, -1.0, 1.0)  # tilt 1.25 ~ насыщение вверх
    exp_ob = f"Книга 0.5%: tilt={ob_tilt_05:.3f} → {s_ob:+.2f}"

    # 2.5 Кросс-биржевой базис (средний знак)
    s_cross = 0.0
    if _finite(x_basis_avg):
        # 0.10% базиса → насыщение
        s_cross = _norm_change(x_basis_avg, 0.001)
    exp_cross = f"Кросс-перпы: avgBasis={x_basis_avg:.6f} → {s_cross:+.2f}"

    # 2.6 Календарный базис (контанго/бэквордация, динамика)
    s_cal = 0.0
    cb_parts = []
    if _finite(cb_c_d):
        cb_parts.append(_norm_change(cb_c_d, 0.002))
    if _finite(cb_n_d):
        cb_parts.append(_norm_change(cb_n_d, 0.002))
    if cb_parts:
        s_cal = sum(cb_parts) / len(cb_parts)
    exp_cal = f"Календарный базис: ΔCQ={cb_c_d:.6f}, ΔNQ={cb_n_d:.6f} → {s_cal:+.2f}"

    # 2.7 «Сентимент» (taker — по направлению; глобал — контр; топы — слабее по весу)
    s_sent = 0.0
    parts = []
    parts.append( 0.50 * _ratio_to_score(_val(r_taker), center=1.0, width=0.25, invert=False) )   # основной сигнал
    parts.append( 0.25 * _ratio_to_score(_val(r_glob),  center=1.0, width=0.50, invert=True) )    # толпа — контр
    parts.append( 0.15 * _ratio_to_score(_val(r_topa),  center=1.0, width=0.50, invert=False) )   # топ-аккаунты
    parts.append( 0.10 * _ratio_to_score(_val(r_topp),  center=1.0, width=0.50, invert=False) )   # топ-позиции
    s_sent = sum(parts)
    s_sent = _clamp(s_sent, -1.0, 1.0)
    exp_sent = f"Сентимент: taker={r_taker}, global={r_glob}, topA={r_topa}, topP={r_topp} → {s_sent:+.2f}"

    # 2.8 Ширина рынка (доля up-down)
    s_breadth = 0.0
    if _finite(adv_dec) and considered > 0:
        s_breadth = _clamp(adv_dec / considered, -1.0, 1.0)
    exp_breadth = f"Ширина: A-D={adv_dec:.0f}/{int(considered)} → {s_breadth:+.2f}"

    # 2.9 Стейблы (средняя девиация)
    s_stables = _norm_change(stab_mean_dev, 0.005)  # 0.5% → насыщение
    exp_stables = f"Стейблы: meanDev={stab_mean_dev:+.4f} → {s_stables:+.2f}"

    # 2.10 Макро (ES/NQ/DXY)
    s_macro = macro_norm
    exp_macro = f"Макро: lean={_val(_safe_get(report, 'macro_weather', 'macro_lean_score'))} → {s_macro:+.2f}"

    # -------- 3) Веса (по вашей иерархии), динамическая перенормировка
    weights_nominal = {
        "price_oi_funding": 0.28,
        "basis":             0.14,
        "flows":             0.12,
        "orderbook":         0.07,
        "cross":             0.07,
        "calendar":          0.05,
        "sentiment":         0.08,
        "breadth":           0.12,
        "stables":           0.02,
        "macro":             0.05,
    }

    raw = {
        "price_oi_funding": (s_price_oi_funding, exp_price_oi_funding, {"price_change": pc, "oi_change": oic, "funding": fr, "who_pays": who}),
        "basis":             (s_basis,           exp_basis,            {"basis_now": basis_now, "basis_then": basis_then}),
        "flows":             (s_flows,           exp_flows,            {"spot_net": spot_net, "perp_net": perp_net}),
        "orderbook":         (s_ob,              exp_ob,               {"tilt_0.5%": ob_tilt_05, "tilt_1.0%": ob_tilt_10}),
        "cross":             (s_cross,           exp_cross,            {"avg_basis": x_basis_avg, "n_exchanges": len(x_basis_vals)}),
        "calendar":          (s_cal,             exp_cal,              {"ΔCQ": cb_c_d, "ΔNQ": cb_n_d}),
        "sentiment":         (s_sent,            exp_sent,             {"taker": r_taker, "global": r_glob, "topA": r_topa, "topP": r_topp}),
        "breadth":           (s_breadth,         exp_breadth,          {"advance_decline": adv_dec, "considered": considered}),
        "stables":           (s_stables,         exp_stables,          {"mean_dev": stab_mean_dev}),
        "macro":             (s_macro,           exp_macro,            {"macro_lean_score": macro_score}),
    }

    # Выкидываем метрики, у которых score не финитен (на всякий)
    used_sum = 0.0
    for k, w in list(weights_nominal.items()):
        s, _, _ = raw[k]
        if not _finite(s):
            weights_nominal[k] = 0.0
        used_sum += weights_nominal[k]
    if used_sum <= 0:
        used_sum = 1.0

    # Перенормируем веса к 1.0
    weights_used = {k: (w / used_sum) for k, w in weights_nominal.items()}

    # -------- 4) Главный индекс
    total = 0.0
    per_metric: Dict[str, Any] = {}
    for k, (s, exp, inputs) in raw.items():
        w_used = weights_used[k]
        total += s * w_used
        label = "bullish" if s > 0.2 else ("bearish" if s < -0.2 else "neutral")
        per_metric[k] = {
            "score": float(s),
            "weight": float(weights_nominal[k]),
            "weight_used": float(w_used),
            "label": label,
            "explanation": exp,
            "inputs": inputs,
        }

    overall_score = _clamp(total, -1.0, 1.0)
    index = int(round(overall_score * 100))
    overall_label = "bullish" if overall_score > 0.20 else ("bearish" if overall_score < -0.20 else "neutral")
    suggestion = "long" if overall_label == "bullish" else ("short" if overall_label == "bearish" else "flat")

    result = {
        "per_metric": per_metric,
        "overall": {
            "score": overall_score,
            "index": index,                    # -100..+100
            "label": overall_label,            # bullish|bearish|neutral
            "suggestion": suggestion,          # long|short|flat
            "confidence": used_sum,            # насколько полно использованы веса (0..1)
        },
        "weights": {"nominal_sum": 1.0, "used_sum": used_sum, "used": weights_used},
    }
    return result
