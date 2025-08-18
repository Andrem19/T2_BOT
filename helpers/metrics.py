# -*- coding: utf-8 -*-
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone

def analyze_option_slice(
    options: List[Dict[str, Any]],
    target_delta_abs: float = 0.25,
    call_pick_delta_range: Tuple[float, float] = (0.22, 0.35),
    put_pick_delta_range:  Tuple[float, float] = (-0.35, -0.22),
    convention: str = "call_minus_put"
) -> Dict[str, Any]:
    """
    Комплексный анализ «среза» опционов одной экспирации (одной монеты и даты):
      - RR25 по IV с дельта-матчингом (интерполяция по дельте),
      - ATM-IV (робастно), BF25 (выпуклость), wing-ratio,
      - Склоны улыбки dIV/dΔ для call/put,
      - Метрики ликвидности (спреды, глубина),
      - Эффективности для выбора контракта (|theta|/gamma, premium/gamma, |theta|/mid),
      - Рекомендация лучшего call (для схемы long CALL + short perp),
        и лучшего put (для схемы long PUT + long perp).

    Вход:
      options: список словарей по опционам одной экспирации.
               Требуются поля: 'optionsType' ('Call'/'Put' или 'C'/'P'),
                               'delta' (float), 'iv' (float),
                               'bidPrice','askPrice' (float), 'markPrice' (float, можно None),
                               'strike' (float), 'underlyingPrice' (float),
                               'deliveryTime' (ms epoch).
      target_delta_abs: модуль целевой дельты для RR25 (по умолчанию 0.25).
      call_pick_delta_range: диапазон дельт для выбора лучшего CALL.
      put_pick_delta_range:  диапазон дельт для выбора лучшего PUT.
      convention: 'call_minus_put' (стандарт) или 'put_minus_call' для RR25.

    Выход: словарь с метриками и диагностикой.
    """

    # ---------------------- ВСПОМОГАТЕЛЬНО ----------------------
    def _now_ms() -> int:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    def _mid(o: Dict[str, Any]) -> Optional[float]:
        mp = o.get("markPrice", None)
        if isinstance(mp, (int, float)) and mp > 0:
            return float(mp)
        b, a = o.get("bidPrice"), o.get("askPrice")
        if isinstance(b, (int, float)) and isinstance(a, (int, float)) and b > 0 and a > 0:
            return (float(b) + float(a)) / 2.0
        return None

    def _side(options_list, side: str) -> List[Dict[str, Any]]:
        s = side.lower()
        out = []
        for o in options_list:
            typ = str(o.get("optionsType", "")).lower()
            is_call = typ.startswith("c")
            is_put  = typ.startswith("p")
            if (s == "call" and is_call) or (s == "put" and is_put):
                d = o.get("delta"); v = o.get("iv")
                if d is None or v is None:
                    continue
                try:
                    d = float(d); v = float(v)
                except Exception:
                    continue
                m = _mid(o)
                if m is None:
                    continue
                out.append({
                    "symbol": o.get("symbol"),
                    "delta": d,
                    "iv": v,
                    "strike": o.get("strike"),
                    "bidPrice": o.get("bidPrice"),
                    "askPrice": o.get("askPrice"),
                    "markPrice": m,
                    "gamma": o.get("gamma"),
                    "vega": o.get("vega"),
                    "theta": o.get("theta"),
                    "underlyingPrice": o.get("underlyingPrice"),
                    "deliveryTime": o.get("deliveryTime"),
                    "askSize": o.get("askSize"),
                    "bidSize": o.get("bidSize"),
                })
        out.sort(key=lambda r: r["delta"])
        return out

    def _interp_iv_at_delta(rows: List[Dict[str, Any]], target_delta: float):
        """
        Линейная интерполяция IV по дельте.
        Возвращает (iv_est, lower_row, upper_row, t, used_exact_or_nearest)
        """
        if not rows:
            return None, None, None, None, False
        # точное совпадение
        tol = 1e-12
        for r in rows:
            if abs(r["delta"] - target_delta) <= tol:
                return float(r["iv"]), r, r, None, True
        # брекет
        lower = None; upper = None
        for i in range(len(rows) - 1):
            a, b = rows[i], rows[i+1]
            if a["delta"] <= target_delta <= b["delta"]:
                lower, upper = a, b
                break
        if lower is not None and upper is not None and upper["delta"] != lower["delta"]:
            t = (target_delta - lower["delta"]) / (upper["delta"] - lower["delta"])
            iv_est = lower["iv"] + t * (upper["iv"] - lower["iv"])
            return float(iv_est), lower, upper, float(t), False
        # иначе — ближайшая точка
        nearest = min(rows, key=lambda r: abs(r["delta"] - target_delta))
        return float(nearest["iv"]), nearest, nearest, None, True

    def _linear_fit(xs: List[float], ys: List[float]) -> Optional[Dict[str, float]]:
        """
        Простая линейная регрессия (без внешних библиотек).
        Возвращает slope и intercept, либо None если данных мало.
        """
        n = len(xs)
        if n < 2:
            return None
        sx = sum(xs); sy = sum(ys)
        sxx = sum(x*x for x in xs); sxy = sum(x*y for x, y in zip(xs, ys))
        denom = n*sxx - sx*sx
        if abs(denom) < 1e-12:
            return None
        slope = (n*sxy - sx*sy) / denom
        intercept = (sy - slope*sx) / n
        return {"slope": slope, "intercept": intercept}

    def _rel_spread(o: Dict[str, Any]) -> Optional[float]:
        b, a = o.get("bidPrice"), o.get("askPrice")
        m = _mid(o)
        if m is None or not isinstance(b, (int, float)) or not isinstance(a, (int, float)) or m <= 0:
            return None
        return (float(a) - float(b)) / m

    # ---------------------- ОРИГИНАЛЬНЫЕ ОБЪЕКТЫ (для возврата «как есть») ----------------------
    # Мэппинг symbol -> оригинальный словарь, чтобы вернуть top3_* в полном виде
    symbol_to_original: Dict[str, Dict[str, Any]] = {}
    for o in options:
        sym = o.get("symbol")
        if isinstance(sym, str):
            symbol_to_original[sym] = o

    # ---------------------- ПОДГОТОВКА ----------------------
    calls = _side(options, "call")
    puts  = _side(options, "put")
    if not calls or not puts:
        raise ValueError("Требуются и коллы, и путы для анализа (одна экспирация).")

    S0 = None
    for r in (calls + puts):
        u = r.get("underlyingPrice")
        if isinstance(u, (int, float)):
            S0 = float(u); break
    if S0 is None:
        raise ValueError("Нет underlyingPrice в данных.")

    exp_ms = None
    for r in (calls + puts):
        dt = r.get("deliveryTime")
        if isinstance(dt, (int, float)):
            exp_ms = int(dt); break
    if exp_ms is None:
        raise ValueError("Нет deliveryTime в данных.")

    now_ms = _now_ms()
    hours_to_expiry = max(0.0, (exp_ms - now_ms) / 3_600_000.0)
    expiry_iso = datetime.fromtimestamp(exp_ms/1000, tz=timezone.utc).isoformat()
    asof_iso   = datetime.fromtimestamp(now_ms/1000, tz=timezone.utc).isoformat()

    # ---------------------- RR25 (IV, Δ-матч) ----------------------
    call_target = abs(float(target_delta_abs))
    put_target  = -abs(float(target_delta_abs))
    call_iv, c_lo, c_hi, c_t, c_exact = _interp_iv_at_delta(calls, call_target)
    put_iv,  p_lo, p_hi, p_t, p_exact = _interp_iv_at_delta(puts,  put_target)
    if call_iv is None or put_iv is None:
        raise ValueError("Не удалось оценить IV на целевых дельтах для RR25.")

    if convention not in ("call_minus_put", "put_minus_call"):
        raise ValueError("convention должен быть 'call_minus_put' или 'put_minus_call'.")
    rr25 = (call_iv - put_iv) if convention == "call_minus_put" else (put_iv - call_iv)
    skew_ratio = (call_iv / put_iv) if (put_iv and put_iv > 0) else None

    rr25_block = {
        "target_delta_abs": float(target_delta_abs),
        "convention": (
            f"RR25 = IV_call(Δ=+{target_delta_abs:.2f}) − IV_put(Δ=−{target_delta_abs:.2f})"
            if convention == "call_minus_put"
            else f"RR25 = IV_put(Δ=−{target_delta_abs:.2f}) − IV_call(Δ=+{target_delta_abs:.2f})"
        ),
        "call25_iv": float(call_iv),
        "put25_iv": float(put_iv),
        "rr25": float(rr25),
        "rr25_percent_points": float(rr25 * 100.0),
        "skew_ratio": float(skew_ratio) if skew_ratio is not None else None,
        "call_bracket": {
            "lower": c_lo, "upper": c_hi, "t_in_[0..1]": c_t,
            "used_exact_or_nearest": bool(c_exact)
        },
        "put_bracket": {
            "lower": p_lo, "upper": p_hi, "t_in_[0..1]": p_t,
            "used_exact_or_nearest": bool(p_exact)
        }
    }

    # ---------------------- ATM-IV (робастно) ----------------------
    call_atm = min(calls, key=lambda r: abs(r["delta"] - 0.5))
    put_atm  = min(puts,  key=lambda r: abs(r["delta"] + 0.5))
    atm_iv_candidates = [call_atm["iv"], put_atm["iv"]]
    atm_iv = sum(atm_iv_candidates) / 2.0

    # ---------------------- BF25, Wing-ratio ----------------------
    bf25 = 0.5*(call_iv + put_iv) - atm_iv
    wing_ratio = (0.5*(call_iv + put_iv)) / atm_iv if atm_iv > 0 else None

    smile_block = {
        "atm_iv": float(atm_iv),
        "bf25": float(bf25),
        "bf25_percent_points": float(bf25 * 100.0),
        "wing_ratio": float(wing_ratio) if wing_ratio is not None else None,
    }

    # ---------------------- Склоны улыбки ----------------------
    calls_otm = [r for r in calls if 0.05 <= r["delta"] <= 0.5]
    puts_otm  = [r for r in puts  if -0.5 <= r["delta"] <= -0.05]
    def _linear_fit(xs: List[float], ys: List[float]) -> Optional[Dict[str, float]]:
        n = len(xs)
        if n < 2:
            return None
        sx = sum(xs); sy = sum(ys)
        sxx = sum(x*x for x in xs); sxy = sum(x*y for x, y in zip(xs, ys))
        denom = n*sxx - sx*sx
        if abs(denom) < 1e-12:
            return None
        slope = (n*sxy - sx*sy) / denom
        intercept = (sy - slope*sx) / n
        return {"slope": slope, "intercept": intercept}
    fit_calls = _linear_fit([r["delta"] for r in calls_otm], [r["iv"] for r in calls_otm])
    fit_puts  = _linear_fit([r["delta"] for r in puts_otm],  [r["iv"] for r in puts_otm])
    smile_slope_block = {
        "call_iv_vs_delta_slope": (fit_calls["slope"] if fit_calls else None),
        "put_iv_vs_delta_slope":  (fit_puts["slope"]  if fit_puts  else None),
        "notes": "Склон положительный у call означает рост IV к ATM; у put — интерпретировать с учётом знака дельты."
    }

    # ---------------------- Ликвидность ----------------------
    def _agg_spreads(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
        rels = [x for r in rows if (x := _rel_spread(r)) is not None]
        abss = [ (float(r["askPrice"]) - float(r["bidPrice"]))
                 for r in rows
                 if isinstance(r.get("askPrice"), (int,float)) and isinstance(r.get("bidPrice"), (int,float)) ]
        def _avg(a):
            return sum(a)/len(a) if a else None
        def _median(a):
            if not a: return None
            b = sorted(a); n = len(b)
            return b[n//2] if n%2==1 else 0.5*(b[n//2 - 1] + b[n//2])
        return {
            "avg_rel_spread": _avg(rels),
            "med_rel_spread": _median(rels),
            "avg_abs_spread": _avg(abss),
            "med_abs_spread": _median(abss),
        }
    liq_calls = _agg_spreads(calls)
    liq_puts  = _agg_spreads(puts)
    liquidity_block = {
        "calls": liq_calls,
        "puts":  liq_puts,
        "notes": "rel_spread = (ask - bid) / mid; abs_spread в валюте премии."
    }

    # ---------------------- Эффективности и выбор лучших контрактов ----------------------
    def _eff_row(r: Dict[str, Any]) -> Dict[str, Any]:
        m = r["markPrice"]
        th = r.get("theta")
        theta_per_hour = float(th)/24.0 if isinstance(th, (int, float)) else None
        g = r.get("gamma")
        gamma = float(g) if isinstance(g, (int, float)) else None

        theta_per_gamma = None
        if theta_per_hour is not None and gamma and abs(gamma) > 1e-12:
            theta_per_gamma = abs(theta_per_hour) / abs(gamma)

        premium_per_gamma = None
        if gamma and abs(gamma) > 1e-12:
            premium_per_gamma = m / abs(gamma)

        theta_burn_per_100 = None
        if theta_per_hour is not None and m and m > 0:
            theta_burn_per_100 = abs(theta_per_hour) / m * 100.0

        return {
            "symbol": r["symbol"],
            "delta": r["delta"],
            "iv": r["iv"],
            "strike": r["strike"],
            "mark": m,
            "bid": r["bidPrice"],
            "ask": r["askPrice"],
            "rel_spread": _rel_spread(r),
            "askSize": r.get("askSize"),
            "bidSize": r.get("bidSize"),
            "gamma": gamma,
            "theta_per_hour": theta_per_hour,
            "theta_per_gamma": theta_per_gamma,
            "premium_per_gamma": premium_per_gamma,
            "theta_burn_per_100": theta_burn_per_100,
        }

    eff_calls_all = [_eff_row(r) for r in calls]
    eff_puts_all  = [_eff_row(r)  for r in puts]

    def _in_range(x: float, lo: float, hi: float) -> bool:
        return (x is not None) and (lo <= x <= hi)

    cand_calls = [e for e in eff_calls_all if _in_range(e["delta"], call_pick_delta_range[0], call_pick_delta_range[1])]
    cand_puts  = [e for e in eff_puts_all  if _in_range(e["delta"], put_pick_delta_range[0],  put_pick_delta_range[1])]

    def _sort_key_call(e):
        return (
            float('inf') if e["theta_per_gamma"] is None else e["theta_per_gamma"],
            float('inf') if e["premium_per_gamma"] is None else e["premium_per_gamma"],
            float('inf') if e["rel_spread"] is None else e["rel_spread"],
            -(e["askSize"] if isinstance(e.get("askSize"), (int,float)) else 0.0)
        )
    def _sort_key_put(e):
        return _sort_key_call(e)

    cand_calls_sorted = sorted(cand_calls, key=_sort_key_call) if cand_calls else []
    cand_puts_sorted  = sorted(cand_puts,  key=_sort_key_put)  if cand_puts  else []

    best_call = cand_calls_sorted[0] if cand_calls_sorted else None
    best_put  = cand_puts_sorted[0]  if cand_puts_sorted  else None

    # ---------------------- ВОЗВРАТ TOP3_* В ПОЛНОМ ВИДЕ (ОРИГИНАЛЬНЫЕ ОБЪЕКТЫ) ----------------------
    top3_calls_symbols = [e["symbol"] for e in cand_calls_sorted[:3]]
    top3_puts_symbols  = [e["symbol"] for e in cand_puts_sorted[:3]]

    top3_calls_full = [symbol_to_original[sym] for sym in top3_calls_symbols if sym in symbol_to_original]
    top3_puts_full  = [symbol_to_original[sym] for sym in top3_puts_symbols  if sym in symbol_to_original]

    picks_block = {
        "best_call_for_short_perp": best_call,   # оставляем агрегаты как есть
        "best_put_for_long_perp": best_put,      # оставляем агрегаты как есть
        "top3_calls": top3_calls_full,           # ← теперь в полном виде, как пришли в options
        "top3_puts":  top3_puts_full,            # ← теперь в полном виде, как пришли в options
        "notes": (
            "best_call — под схему long CALL + short perp (RR25<0: коллы относительно дешевле). "
            "Критерий: минимизируем |θ|/γ (меньше тетты на единицу гаммы), "
            "при равенстве — минимальный premium/γ, затем спред/глубина."
        )
    }

    # ---------------------- Сводка и возврат ----------------------
    return {
        "meta": {
            "asof_utc": asof_iso,
            "underlying": S0,
            "expiry_utc": expiry_iso,
            "hours_to_expiry": hours_to_expiry,
            "n_calls": len(calls),
            "n_puts": len(puts),
        },
        "rr25_block": rr25_block,
        "smile_block": smile_block,
        "smile_slope_block": smile_slope_block,
        "liquidity_block": liquidity_block,
        "efficiency_block": {
            "calls_all": eff_calls_all,
            "puts_all":  eff_puts_all
        },
        "picks_block": picks_block,
        "notes": (
            "ATM-IV оценен как среднее IV колла с Δ≈+0.5 и пута с Δ≈-0.5. "
            "BF25 = 0.5*(IV_call25+IV_put25) - IV_ATM. "
            "Склоны dIV/dΔ — линейная аппроксимация в OTM-зонах (calls: Δ∈[0.05;0.5], puts: Δ∈[-0.5;-0.05]). "
            "θ интерпретирована как валюта премии в день → деление на 24 даёт часовой дренаж."
        )
    }


def pic_best_opt(metrics):
    rr25 = metrics['rr25_block']['rr25']
    if rr25 <= 0:
        return rr25, metrics['picks_block']['top3_calls']
    else:
        return rr25, metrics['picks_block']['top3_puts']