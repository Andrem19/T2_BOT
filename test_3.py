from exchanges.hyperliquid_api import HL
import time
from helpers.spot_price_feed import start_spot_price_streams, SpotPriceCache

from typing import Dict, Optional
import math

from typing import Any, Dict, Optional, Tuple, List
import math

from typing import Any, Dict, Optional, Tuple, List
import math

def compute_stablecoins_metrics(
    usdcusdt: Optional[float],
    fdusdusdt: Optional[float],
    usdpusdt: Optional[float],
) -> Dict[str, Any]:
    """
    Глобально-совместимая логика для блока stablecoins:
    - Источник цен (last/mid) вы подаёте извне — здесь НИЧЕГО не меняется.
    - Всё остальное (нормализация, отклонения, сводка, единый score) совпадает с глобальной логикой:
        * равномерная работа с NaN;
        * взвешенное направление по ликвидности (USDC>FDUSD>USDP);
        * масштабирование направления «мягким» коэффициентом;
        * confidence через стресс по одиночным и попарным отклонениям;
        * итоговый score = direction * confidence в диапазоне [-1..1].

    Возвращаемая структура:
    {
      "USDCUSDT": {"last": <float>, "deviation_from_1": <float>, "deviation_bps": <float>},
      "FDUSDUSDT": {...},
      "USDPUSDT": {...},
      "pairwise": {
        "USDC/FDUSD": {"ratio": <float>, "deviation_from_1": <float>, "deviation_bps": <float>},
        "USDC/USDP":  {...},
        "FDUSD/USDP": {...}
      },
      "summary": {
        "count_valid": <int>,
        "mean_bps": <float>,
        "weighted_mean_bps": <float>,
        "median_bps": <float>,
        "max_abs_bps": <float>,
        "mean_abs_bps": <float>,
        "pairwise_max_abs_bps": <float>,
        "pairwise_mean_abs_bps": <float>
      },
      "composite": {
        "score": <float in [-1,1]>,
        "directional_bps_weighted_mean": <float>,
        "stress_bps": <float>,
        "dispersion_pairwise_bps": <float>,
        "confidence": <float in [0,1]>,
        "params": {"bps_soft": 30.0, "bps_hard": 80.0, "weights": {"USDCUSDT":0.5,"FDUSDUSDT":0.3,"USDPUSDT":0.2}}
      }
    }
    """

    # --- Константы «как в глобальной логике» ---
    BPS_SOFT = 30.0   # мягкая нормировка направления (≈ масштаб обычных перекосов)
    BPS_HARD = 80.0   # нормировка стрессов (насыщение около 80 bps)
    WEIGHTS  = {"USDCUSDT": 0.50, "FDUSDUSDT": 0.30, "USDPUSDT": 0.20}

    def _to_float(x) -> float:
        try:
            fx = float(x)
            return fx if math.isfinite(fx) else float("nan")
        except Exception:
            return float("nan")

    def _pack_symbol(price: float) -> Dict[str, float]:
        if math.isfinite(price):
            dev_abs = price - 1.0
            return {
                "last": price,
                "deviation_from_1": dev_abs,
                "deviation_bps": dev_abs * 10_000.0,
            }
        return {"last": float("nan"), "deviation_from_1": float("nan"), "deviation_bps": float("nan")}

    def _median(values: List[float]) -> float:
        vals = [v for v in values if math.isfinite(v)]
        if not vals:
            return float("nan")
        vals.sort()
        n = len(vals)
        m = n // 2
        return vals[m] if (n % 2 == 1) else 0.5 * (vals[m - 1] + vals[m])

    def _clip01(x: float) -> float:
        if not math.isfinite(x):
            return 0.0
        if x < 0.0:
            return 0.0
        if x > 1.0:
            return 1.0
        return x

    # --- 1) Нормализация входа (источник цен — внешний) ---
    prices = {
        "USDCUSDT": _to_float(usdcusdt),
        "FDUSDUSDT": _to_float(fdusdusdt),
        "USDPUSDT": _to_float(usdpusdt),
    }

    # --- 2) Пер-символьные метрики (точно как в глобальном блоке) ---
    per_symbol = {
        "USDCUSDT": _pack_symbol(prices["USDCUSDT"]),
        "FDUSDUSDT": _pack_symbol(prices["FDUSDUSDT"]),
        "USDPUSDT": _pack_symbol(prices["USDPUSDT"]),
    }

    # --- 3) Попарные соотношения (для дисперсии между стейблами) ---
    def _pair_ratio(a_sym: str, b_sym: str) -> Dict[str, float]:
        pa, pb = prices.get(a_sym, float("nan")), prices.get(b_sym, float("nan"))
        if math.isfinite(pa) and math.isfinite(pb) and pb != 0.0:
            r = pa / pb
            dev_abs = r - 1.0
            return {"ratio": r, "deviation_from_1": dev_abs, "deviation_bps": dev_abs * 10_000.0}
        return {"ratio": float("nan"), "deviation_from_1": float("nan"), "deviation_bps": float("nan")}

    pairwise = {
        "USDC/FDUSD": _pair_ratio("USDCUSDT", "FDUSDUSDT"),
        "USDC/USDP":  _pair_ratio("USDCUSDT", "USDPUSDT"),
        "FDUSD/USDP": _pair_ratio("FDUSDUSDT", "USDPUSDT"),
    }

    # --- 4) Сводные показатели (mean/median/max/…); mean — невзвешенный, weighted_mean — как в глобале ---
    dev_bps_list = [
        per_symbol["USDCUSDT"]["deviation_bps"],
        per_symbol["FDUSDUSDT"]["deviation_bps"],
        per_symbol["USDPUSDT"]["deviation_bps"],
    ]
    dev_bps_ok = [x for x in dev_bps_list if math.isfinite(x)]
    mean_bps = (sum(dev_bps_ok) / len(dev_bps_ok)) if dev_bps_ok else float("nan")
    median_bps = _median(dev_bps_ok)

    # Взвешенное среднее по ликвидности; при NaN — нормируем веса на валидные
    wm_num, wm_den = 0.0, 0.0
    for sym, w in WEIGHTS.items():
        v = per_symbol[sym]["deviation_bps"]
        if math.isfinite(v):
            wm_num += w * v
            wm_den += w
    weighted_mean_bps = (wm_num / wm_den) if wm_den > 0.0 else float("nan")

    max_abs_bps = max((abs(x) for x in dev_bps_ok), default=float("nan"))
    mean_abs_bps = (sum(abs(x) for x in dev_bps_ok) / len(dev_bps_ok)) if dev_bps_ok else float("nan")

    pair_devs = [pairwise[k]["deviation_bps"] for k in ("USDC/FDUSD", "USDC/USDP", "FDUSD/USDP")]
    pair_devs_ok = [x for x in pair_devs if math.isfinite(x)]
    pairwise_max_abs_bps = max((abs(x) for x in pair_devs_ok), default=float("nan"))
    pairwise_mean_abs_bps = (sum(abs(x) for x in pair_devs_ok) / len(pair_devs_ok)) if pair_devs_ok else float("nan")

    summary = {
        "count_valid": len(dev_bps_ok),
        "mean_bps": mean_bps,
        "weighted_mean_bps": weighted_mean_bps,
        "median_bps": median_bps,
        "max_abs_bps": max_abs_bps,
        "mean_abs_bps": mean_abs_bps,
        "pairwise_max_abs_bps": pairwise_max_abs_bps,
        "pairwise_mean_abs_bps": pairwise_mean_abs_bps,
    }

    # --- 5) Итоговая единая метрика (как в глобальном блоке) ---
    # Direction: взвешенный средний перекос (bps) нормируем мягко
    if math.isfinite(weighted_mean_bps):
        direction = max(-1.0, min(1.0, weighted_mean_bps / BPS_SOFT))
    else:
        direction = 0.0

    # Стресс: одиночный и попарный, нормированные жёстко; confidence — чем меньше стресс, тем выше
    stress_single = _clip01((max_abs_bps if math.isfinite(max_abs_bps) else 0.0) / BPS_HARD)
    stress_pair   = _clip01((pairwise_max_abs_bps if math.isfinite(pairwise_max_abs_bps) else 0.0) / BPS_HARD)
    confidence    = 1.0 - (0.6 * stress_single + 0.4 * stress_pair)
    if confidence < 0.0:
        confidence = 0.0
    if confidence > 1.0:
        confidence = 1.0

    score = direction * confidence
    if score < -1.0: score = -1.0
    if score >  1.0: score =  1.0

    composite = {
        "score": score,
        "directional_bps_weighted_mean": weighted_mean_bps,
        "stress_bps": max_abs_bps,
        "dispersion_pairwise_bps": pairwise_max_abs_bps,
        "confidence": confidence,
        "params": {"bps_soft": BPS_SOFT, "bps_hard": BPS_HARD, "weights": WEIGHTS},
    }

    # --- 6) Итог ---
    return {
        "USDCUSDT": per_symbol["USDCUSDT"],
        "FDUSDUSDT": per_symbol["FDUSDUSDT"],
        "USDPUSDT": per_symbol["USDPUSDT"],
        "pairwise": pairwise,
        "summary": summary,
        "composite": composite,
    }




start_spot_price_streams(['USDCUSDT', 'FDUSDUSDT', 'USDPUSDT'])

while True:
    price_USDC = SpotPriceCache.get('USDCUSDT')
    price_FDUSD = SpotPriceCache.get('FDUSDUSDT')
    price_USDP = SpotPriceCache.get('USDPUSDT')

    result = compute_stablecoins_metrics(price_USDC, price_FDUSD, price_USDP)
    print(f"USDCUSDT: {result['USDCUSDT']['last']}")
    print(f"FDUSDUSDT: {result['FDUSDUSDT']['last']}")
    print(f"USDPUSDT: {result['USDPUSDT']['last']}")
    print(f"Score: {result['composite']['score']}")
    time.sleep(3)