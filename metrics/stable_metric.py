from typing import Any, Dict, Optional, Tuple, List
import math

def compute_stablecoins_metrics(
    usdcusdt: Optional[float],
    fdusdusdt: Optional[float],
    usdpusdt: Optional[float],
    *,
    bps_soft: float = 15.0,   # «мягкий» масштаб для направления (средний перекос ~15 б.п. даёт ~1.0 по модулю)
    bps_hard: float = 60.0    # «жёсткий» масштаб для стресса/дисперсии (60 б.п. ≈ насыщение)
) -> Dict[str, Any]:
    """
    Считает полный набор метрик по стейблам + единую интегральную метрику (score).
    Вход — ТРИ цены последних сделок (или mid, если подставите сами):
      - USDCUSDT, FDUSDUSDT, USDPUSDT (могут быть None/str/float).

    Возвращает словарь:
      {
        "USDCUSDT": {"last": ..., "deviation_from_1": ..., "deviation_bps": ...},
        "FDUSDUSDT": {...},
        "USDPUSDT": {...},
        "pairwise": {
          "USDC/FDUSD": {"ratio": ..., "deviation_from_1": ..., "deviation_bps": ...},
          "USDC/USDP":  {...},
          "FDUSD/USDP": {...}
        },
        "summary": {
          "count_valid": <int>,
          "mean_bps": <float>, "median_bps": <float>,
          "max_abs_bps": <float>, "mean_abs_bps": <float>,
          "pairwise_max_abs_bps": <float>, "pairwise_mean_abs_bps": <float>
        },
        "composite": {
          "score": <float in [-1,1]>,               # итоговая единая метрика: знак — направление, модуль — уверенность
          "directional_bps_mean": <float>,          # средний подписанный перекос (в б.п.) относительно USDT
          "stress_bps": <float>,                    # max |отклонения| по одиночным парам в б.п.
          "dispersion_pairwise_bps": <float>,       # max |отклонения| по попарным соотношениям в б.п.
          "confidence": <float in [0,1]>,           # «здоровье» (1 — всё ровно; 0 — сильный стресс/дисперсия)
          "params": {"bps_soft": ..., "bps_hard": ...}
        }
      }

    Примечание:
      - База сравнения — USDT, не «чистый USD». Положительное значение deviation_bps означает премию данного стейбла к USDT.
      - Единая метрика `score` учитывает и общее направление (средний перекос), и степень стресса/рассинхрона между стейблами (дисперсию).
    """

    def _to_float(x) -> float:
        try:
            fx = float(x)
            return fx if math.isfinite(fx) else float("nan")
        except Exception:
            return float("nan")

    def _devs(symbol_to_price: Dict[str, float]) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        for s, p in symbol_to_price.items():
            if math.isfinite(p):
                dev_abs = p - 1.0
                out[s] = {
                    "last": p,
                    "deviation_from_1": dev_abs,
                    "deviation_bps": dev_abs * 10_000.0,
                }
            else:
                out[s] = {
                    "last": float("nan"),
                    "deviation_from_1": float("nan"),
                    "deviation_bps": float("nan"),
                }
        return out

    def _pairwise(symbol_to_price: Dict[str, float]) -> Dict[str, Dict[str, float]]:
        pairs: List[Tuple[str, str]] = [
            ("USDCUSDT", "FDUSDUSDT"),
            ("USDCUSDT", "USDPUSDT"),
            ("FDUSDUSDT", "USDPUSDT"),
        ]
        out: Dict[str, Dict[str, float]] = {}
        for a, b in pairs:
            pa, pb = symbol_to_price.get(a, float("nan")), symbol_to_price.get(b, float("nan"))
            key = f"{a.replace('USDT','')}/{b.replace('USDT','')}".replace("USDCUSDT","USDC").replace("FDUSDUSDT","FDUSD").replace("USDPUSDT","USDP")
            if math.isfinite(pa) and math.isfinite(pb) and pb != 0.0:
                r = pa / pb
                dev_abs = r - 1.0
                out[key] = {
                    "ratio": r,
                    "deviation_from_1": dev_abs,
                    "deviation_bps": dev_abs * 10_000.0,
                }
            else:
                out[key] = {
                    "ratio": float("nan"),
                    "deviation_from_1": float("nan"),
                    "deviation_bps": float("nan"),
                }
        return out

    def _median(values: List[float]) -> float:
        vals = [v for v in values if math.isfinite(v)]
        if not vals:
            return float("nan")
        vals.sort()
        n = len(vals)
        mid = n // 2
        if n % 2 == 1:
            return vals[mid]
        return 0.5 * (vals[mid - 1] + vals[mid])

    # --- 1) Нормализация входа
    prices = {
        "USDCUSDT": _to_float(usdcusdt),
        "FDUSDUSDT": _to_float(fdusdusdt),
        "USDPUSDT": _to_float(usdpusdt),
    }

    # --- 2) Метрики по каждому символу
    per_symbol = _devs(prices)

    # --- 3) Попарные соотношения стейблов (для выявления «кто выбивается»)
    pairwise = _pairwise(prices)

    # --- 4) Сводная статистика
    dev_bps_list = [per_symbol[s]["deviation_bps"] for s in ("USDCUSDT", "FDUSDUSDT", "USDPUSDT")]
    dev_bps_ok = [x for x in dev_bps_list if math.isfinite(x)]
    mean_bps = (sum(dev_bps_ok) / len(dev_bps_ok)) if dev_bps_ok else float("nan")
    median_bps = _median(dev_bps_ok) if dev_bps_ok else float("nan")
    max_abs_bps = max((abs(x) for x in dev_bps_ok), default=float("nan"))
    mean_abs_bps = (sum(abs(x) for x in dev_bps_ok) / len(dev_bps_ok)) if dev_bps_ok else float("nan")

    pair_devs = [pairwise[k]["deviation_bps"] for k in pairwise.keys()]
    pair_devs_ok = [x for x in pair_devs if math.isfinite(x)]
    pairwise_max_abs_bps = max((abs(x) for x in pair_devs_ok), default=float("nan"))
    pairwise_mean_abs_bps = (sum(abs(x) for x in pair_devs_ok) / len(pair_devs_ok)) if pair_devs_ok else float("nan")

    summary = {
        "count_valid": len(dev_bps_ok),
        "mean_bps": mean_bps,
        "median_bps": median_bps,
        "max_abs_bps": max_abs_bps,
        "mean_abs_bps": mean_abs_bps,
        "pairwise_max_abs_bps": pairwise_max_abs_bps,
        "pairwise_mean_abs_bps": pairwise_mean_abs_bps,
    }

    # --- 5) ЕДИНАЯ МЕТРИКА (score)
    # Идея:
    #   - направление = средний подписанный перекос (mean_bps) относительно USDT, нормированный по bps_soft;
    #   - «здоровье/уверенность» = 1 - взвешенная сумма стрессов (по одиночным и попарным отклонениям), нормированная по bps_hard;
    #   - итоговый score = направление * уверенность  ∈ [-1, 1].
    def _clip01(x: float) -> float:
        return 0.0 if not math.isfinite(x) else 0.0 if x < 0.0 else 1.0 if x > 1.0 else x

    # Направление (подписанное): чем больше средний bps, тем «положительнее» (стейблы дороже USDT).
    directional = 0.0
    if math.isfinite(mean_bps):
        directional = max(-1.0, min(1.0, mean_bps / bps_soft))

    # Стрессовые компоненты: насыщаются к 1 при достижении bps_hard
    stress_single = _clip01((max_abs_bps if math.isfinite(max_abs_bps) else 0.0) / bps_hard)
    stress_pair   = _clip01((pairwise_max_abs_bps if math.isfinite(pairwise_max_abs_bps) else 0.0) / bps_hard)

    # Уверенность (health/confidence): чем меньше стрессов, тем ближе к 1
    confidence = 1.0 - (0.6 * stress_single + 0.4 * stress_pair)
    if confidence < 0.0:
        confidence = 0.0
    if confidence > 1.0:
        confidence = 1.0

    score = directional * confidence

    composite = {
        "score": score,
        "directional_bps_mean": mean_bps,
        "stress_bps": max_abs_bps,
        "dispersion_pairwise_bps": pairwise_max_abs_bps,
        "confidence": confidence,
        "params": {"bps_soft": bps_soft, "bps_hard": bps_hard},
    }

    # --- 6) Итоговый ответ
    return {
        "USDCUSDT": per_symbol["USDCUSDT"],
        "FDUSDUSDT": per_symbol["FDUSDUSDT"],
        "USDPUSDT": per_symbol["USDPUSDT"],
        "pairwise": pairwise,
        "summary": summary,
        "composite": composite,
    }
