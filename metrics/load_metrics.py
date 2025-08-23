import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

def _extract_score(value: Any) -> Optional[float]:
    """
    Унифицированное извлечение численного 'score':
    - Если value — словарь, возвращает value.get('score').
    - Если value — число (int/float), возвращает как есть.
    - Иначе None.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get("score")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _parse_time_utc_to_str(ts: str) -> Optional[Tuple[datetime, str]]:
    """
    Парсит время из строки (например, '2025-08-22 11:00:00+00:00' или '...Z')
    и возвращает кортеж: (datetime_utc, 'YY-MM-DD HH:MM:SS').
    При отсутствии или ошибке парсинга — None.
    """
    if not ts:
        return None
    s = ts.strip()
    # Нормализуем суффикс 'Z' -> '+00:00' для совместимости с fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt: Optional[datetime] = None

    # 1) ISO с таймзоной
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        dt = None

    # 2) Без таймзоны 'YYYY-MM-DD HH:MM:SS'
    if dt is None:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    # Гарантируем UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)

    # Формат 'YY-MM-DD HH:MM:SS' (пример: '23-08-25 00:00:00')
    pretty = dt_utc.strftime("%y-%m-%d %H:%M:%S")
    return dt_utc, pretty


def load_compact_metrics(path: str) -> List[Dict[str, Any]]:
    """
    Читает JSON-Lines файл `path` (каждая строка — отдельный JSON-словарь),
    отбирает записи, где в per_metric есть news_score, rr25 и iv,
    и возвращает список компактных словарей с ключами:

        time, price_oi_funding, basis, flows, orderbook, cross,
        calendar, sentiment, breadth, stables, macro,
        news_score, rr25, iv, overall

    Список отсортирован по времени (от начала к концу).
    Значения метрик — это поля 'score' соответствующих подпоказателей,
    если они заданы словарями; если какие-то из необязательных метрик отсутствуют,
    в итоговом словаре для них будет None.
    """
    required_keys = ("news_score", "rr25", "iv")
    result: List[Tuple[datetime, Dict[str, Any]]] = []

    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception:
                # Некорректная строка — пропускаем
                continue

            per = obj.get("per_metric") or {}

            # Проверяем наличие всех трёх обязательных полей
            if not all(k in per for k in required_keys):
                continue

            news_score = _extract_score(per.get("news_score"))
            rr25 = _extract_score(per.get("rr25"))
            iv = _extract_score(per.get("iv"))

            if news_score is None or rr25 is None or iv is None:
                # Если любое из обязательных значений не извлеклось — пропуск
                continue

            # Время
            ts_raw = obj.get("time_utc") or obj.get("time") or obj.get("timestamp")
            parsed = _parse_time_utc_to_str(ts_raw)
            if parsed is None:
                continue
            dt_utc, time_str = parsed

            # Удобный accessor
            def m(key: str) -> Optional[float]:
                return _extract_score(per.get(key))

            compact = {
                "time": time_str,
                "price_oi_funding": m("price_oi_funding"),
                "basis": m("basis"),
                "flows": m("flows"),
                "orderbook": m("orderbook"),
                "cross": m("cross"),
                "calendar": m("calendar"),
                "sentiment": m("sentiment"),
                "breadth": m("breadth"),
                "stables": m("stables"),
                "macro": m("macro"),
                "news_score": news_score,
                "rr25": rr25,
                "iv": iv,
                "overall": _extract_score((obj.get("overall") or {})),
            }

            result.append((dt_utc, compact))

    # Сортировка по времени: от ранних к поздним
    result.sort(key=lambda x: x[0])
    return [item for _, item in result]