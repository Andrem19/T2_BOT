from datetime import datetime
import asyncio
from exchanges.hyperliquid_api import HL


# np.set_printoptions(legacy='1.25')

import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any


def _round_to_nearest_hour_utc(dt: datetime) -> datetime:
    """
    Округление к ближайшему часу по UTC.
    Правило: если минут < 30 — вниз, если минут ≥ 30 — вверх.
    Пример: 11:59:41Z -> 12:00:00Z; 12:05:00Z -> 12:00:00Z; 12:30:00Z -> 13:00:00Z.
    """
    if dt.tzinfo is None:
        # Считаем, что вход всегда в UTC, но на всякий случай делаем явным:
        dt = dt.replace(tzinfo=timezone.utc)

    base = dt.replace(minute=0, second=0, microsecond=0)
    half_hour = base + timedelta(minutes=30)
    return base if dt < half_hour else base + timedelta(hours=1)


def _parse_time_utc_to_dt(s: str) -> datetime:
    """
    Парсит строки вида '2025-08-20T11:59:41Z' (или с таймзоной) в datetime с tz=UTC.
    """
    # fromisoformat не понимает 'Z', поэтому заменяем на '+00:00'
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _format_dt_to_time_utc(dt: datetime) -> str:
    """Форматирует datetime (UTC) в строку 'YYYY-MM-DDTHH:MM:SSZ'."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_metrics_file(path: str) -> List[Dict[str, Any]]:
    """
    Читает файл построчно, десериализует каждый JSON-объект в dict.
    Пропускает пустые строки. Если строка повреждена — поднимет ValueError с номером строки.
    """
    records: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError(f"JSON at line {lineno} is not an object")
                records.append(obj)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON at line {lineno}: {e}") from e
    return records


def _write_metrics_file_atomic(path: str, records: List[Dict[str, Any]]) -> None:
    """
    Записывает файл тем же форматом: по одному JSON-объекту на строку.
    Пишет во временный файл и атомарно заменяет оригинал.
    - ensure_ascii=False: сохраняем кириллицу как есть;
    - separators=(',', ':'): компактная запись без лишних пробелов.
    """
    dir_name = os.path.dirname(os.path.abspath(path)) or "."
    tmp_path = os.path.join(dir_name, ".metrics.json.tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")

    os.replace(tmp_path, path)


def process_metrics_file_round_time_utc(path: str) -> List[Dict[str, Any]]:
    """
    Основная функция:
      1) читает все строки из metrics.json -> список словарей;
      2) округляет поле "time_utc" в каждом словаре к ближайшему часу по правилу <30 вниз, ≥30 вверх;
      3) сохраняет файл обратно в исходном «построчном JSON»-формате;
      4) возвращает список словарей (уже с обновлёнными time_utc).

    Если в объекте нет ключа "time_utc" или он не строка — запись остаётся без изменений.
    """
    records = _read_metrics_file(path)

    for rec in records:
        t = rec.get("time_utc")
        if isinstance(t, str) and t.strip():
            dt = _parse_time_utc_to_dt(t)
            rounded = _round_to_nearest_hour_utc(dt)
            rec["time_utc"] = _format_dt_to_time_utc(rounded)

    _write_metrics_file_atomic(path, records)
    return records

async def main() -> None:
    HL.place_limit_post_only('BTCUSDT', 'Sell', 112180, 0, 0.0012, False, 2)
    # process_metrics_file_round_time_utc('metrics.json')
    # data = ld.load_candles('../MARKET_DATA/_crypto_data/BTCUSDT/BTCUSDT_1h.csv', datetime(2020, 1, 1), datetime(2025, 6, 1))

    # for i in range(54, len(data)):
    #     now = data[i-1]
    #     dt = datetime.fromtimestamp(now[0]/1000)
    #     hour = dt.hour
    #     weekday = dt.weekday()
        
    #     diff = (now[2]-now[3])/now[1]


    #     if hour not in dataset[weekday]:
    #         dataset[weekday][hour] = diff
    #     else:
    #         dataset[weekday][hour] += diff
    
    # print(dataset)
    # print(len(data))
    # plot_dataset(dataset)
        


if __name__ == "__main__":
    asyncio.run(main())
