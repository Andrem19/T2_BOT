from train import predict_proba, predict_label
from typing import Any, Dict, List, Tuple
import joblib

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
    bundle = joblib.load("model_bundle.joblib")
    inputm = {}
    pairs = last_two_as_pairs(metrics)
    for key, (prev_v, last_v) in pairs.items():
        if key == "time":
            continue
        inputm[f'{key}_trend'] = last_v-prev_v
        inputm[key] = last_v
    
    probs = predict_proba(bundle, inputm)
    return probs