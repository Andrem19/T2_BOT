from train import predict_proba, predict_label
from typing import Any, Dict, List, Tuple
import joblib
import requests

def fetch_last_60_btcusdt_futures_1m(timeout: float = 10.0):
    """
    Забирает последние 60 минутных (1m) свечей BTCUSDT с Binance USDⓈ-M Futures.
    Возвращает список словарей в порядке возрастания времени (oldest -> newest).

    Поля в каждой свече:
      - open_time:  int (ms)
      - open:       float
      - high:       float
      - low:        float
      - close:      float
      - volume:     float
      - close_time: int (ms)
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 60}

    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    out = []
    for k in data:
        out.append({
            "open_time":  int(k[0]),
            "open":       float(k[1]),
            "high":       float(k[2]),
            "low":        float(k[3]),
            "close":      float(k[4]),
            "volume":     float(k[5]),
            "close_time": int(k[6]),
        })
    return out


def last_two_as_pairs(items: List[Dict[str, Any]]) -> Dict[str, Tuple[Any, Any]]:
    """
    Возвращает словарь вида:
        { key: (prev_value, last_value), ... }
    по двум последним словарям входного списка.
    """
    if len(items) < 2:
        return {}

    prev_d = items[-2]
    last_d = items[-1]

    # Если ключи гарантированно одинаковы — идём по одному набору ключей
    return {k: (prev_d.get(k), last_d.get(k)) for k in prev_d.keys()}

async def prediction(metrics: list):
    klines1m = fetch_last_60_btcusdt_futures_1m()
    bundle = joblib.load("model_bundle.joblib")
    inputm = {}
    pairs = last_two_as_pairs(metrics)
    inputm['dir'] = (klines1m[-1]['close']-klines1m[0]['open'])/klines1m[0]['open']
    for key, (prev_v, last_v) in pairs.items():
        if key == "time":
            continue
        inputm[f'{key}_trend'] = last_v-prev_v
        inputm[key] = last_v
    
    probs = predict_proba(bundle, inputm)
    return probs