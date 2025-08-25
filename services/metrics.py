import shared_vars as sv
from helpers.spot_price_feed import SpotPriceCache
from datetime import datetime, timezone
from metrics.stable_metric import compute_stablecoins_metrics

import csv
from pathlib import Path
from typing import Mapping, Any, Union

def save_stable_score(
    stable_score: Mapping[str, Any],
    path: Union[str, Path] = "stables_metrics.csv",
) -> None:
    """
    Сохраняет запись вида:
        {
            'time_utc': <что-то>,
            'stable_score': <float>,
            'confidence': <float>,
        }
    в CSV-файл `stables_metrics.csv` (или в указанный `path`).

    Поведение:
      - Если файл отсутствует или пуст — создаёт, пишет хедер и строку.
      - Если файл существует и непуст — дописывает строку в конец.
    """
    # Жёстко фиксируем порядок колонок
    fieldnames = ["time_utc", "stable_score", "confidence"]

    # Гарантируем существование директории
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Определяем, нужно ли писать хедер
    file_exists_and_nonempty = path.exists() and path.stat().st_size > 0
    mode = "a" if file_exists_and_nonempty else "w"

    # Готовим строку строго в нужных полях (если ключа нет — пустая строка)
    row = {
        "time_utc": stable_score.get("time_utc", ""),
        "stable_score": stable_score.get("stable_score", ""),
        "confidence": stable_score.get("confidence", ""),
    }

    with path.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists_and_nonempty:
            writer.writeheader()
        writer.writerow(row)


async def refresh_stables():
    price_USDC = SpotPriceCache.get('USDCUSDT')
    price_FDUSD = SpotPriceCache.get('FDUSDUSDT')
    price_USDP = SpotPriceCache.get('USDPUSDT')

    result = compute_stablecoins_metrics(price_USDC, price_FDUSD, price_USDP)
    score = result['composite']['score']
    confidence = result['composite']['confidence']
    sv.stages['simulation']['stable_score'] = score
    sv.stages['simulation']['stable_conf'] = confidence
    time_utc = datetime.now(timezone.utc).timestamp()
    stable_score = {
        'time_utc': time_utc,
        'stable_score': score,
        'confidence': confidence,
    }
    save_stable_score(stable_score)


