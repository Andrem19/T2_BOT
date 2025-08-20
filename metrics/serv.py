# utils/metrics_tail.py
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List


def read_last_metrics(n: int, path: str = "metrics.json", encoding: str = "utf-8") -> List[Dict[str, Any]]:
    """
    Прочитать последние n JSON-строк из файла `path` и вернуть их как список словарей
    в исходной последовательности (как в файле).

    Параметры:
        n (int): сколько последних строк взять. Если n <= 0 — вернёт пустой список.
        path (str): путь к файлу. По умолчанию "metrics.json".
        encoding (str): кодировка файла, по умолчанию UTF-8.

    Исключения:
        FileNotFoundError: если файл отсутствует.
        ValueError: если обнаружена некорректная JSON-строка среди выбранных.
    """
    if n <= 0:
        return []

    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    chunk_size = 64 * 1024  # 64 KiB
    buf = bytearray()
    lines_found = 0

    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()

        while pos > 0 and lines_found <= n:
            read_size = chunk_size if pos >= chunk_size else pos
            pos -= read_size
            f.seek(pos)
            chunk = f.read(read_size)
            buf[:0] = chunk
            lines_found = buf.count(b"\n")

        byte_lines = buf.splitlines()
        tail_lines = byte_lines[-n:] if n < len(byte_lines) else byte_lines

    result: List[Dict[str, Any]] = []
    for i, bline in enumerate(tail_lines, 1):
        line = bline.decode(encoding).strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            snippet = (line[:200] + "…") if len(line) > 200 else line
            raise ValueError(f"Invalid JSON on selected tail line #{i}: {e}. Line snippet: {snippet}") from e
        if not isinstance(obj, dict):
            raise ValueError(f"Expected a JSON object (dict) on tail line #{i}, got {type(obj).__name__}")
        result.append(obj)

    return result


def map_time_to_score(items: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Преобразовать список метрик в словарь { 'YY-MM-DD HH:MM:SS': score }.

    Ожидается, что в каждом элементе есть:
      - ключ 'overall' со вложенным числовым 'score';
      - ключ 'time_utc' в ISO 8601, напр. '2025-08-19T23:58:33Z'.

    Правила:
      - Время форматируется как 'YY-MM-DD HH:MM:SS' (пример: '25-08-19 23:58:33').
      - Порядок вставки ключей совпадает с порядком элементов входного списка.
      - Если время повторяется, последнее значение перезапишет предыдущее.

    Исключения:
      ValueError — если отсутствует 'overall.score' или 'time_utc', либо тип неверный.
    """
    out: Dict[str, float] = {}

    for idx, entry in enumerate(items, 1):
        # Извлекаем score
        try:
            score_raw = entry["overall"]["score"]
            news_score = entry["per_metric"].get("news_score", {}).get("score", 0)
            score = float(score_raw)
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"Item #{idx}: missing or non-numeric overall.score") from e

        # Извлекаем и парсим время
        try:
            t_str = str(entry["time_utc"]).strip()
        except KeyError as e:
            raise ValueError(f"Item #{idx}: missing time_utc") from e

        # Поддержка 'Z' (UTC) и возможных смещений
        # datetime.fromisoformat до недавнего времени не понимал 'Z', поэтому нормализуем.
        iso = t_str[:-1] + "+00:00" if t_str.endswith("Z") else t_str
        try:
            dt = datetime.fromisoformat(iso)
        except Exception as e:
            raise ValueError(f"Item #{idx}: invalid time_utc '{t_str}'") from e

        # Формат 'YY-MM-DD HH:MM:SS'
        key = dt.strftime("%y-%m-%d %H:%M")
        out[key] = f'mk: {round(score, 2)} | ns: {news_score/100}'

    return out


def extract_scores_by_time(entries: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    result = {}

    for entry in entries:
        # Преобразуем время
        raw_time = entry.get("time_utc")
        if not raw_time:
            continue
        try:
            dt = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue  # Пропускаем неверный формат
        formatted_time = dt.strftime("%d-%m-%y %H:%M:%S")

        scores = {}

        # Собираем оценки из per_metric
        per_metric = entry.get("per_metric", {})
        for metric_name, metric_data in per_metric.items():
            score = metric_data.get("score")
            if score is not None:
                scores[metric_name] = round(score, 2)

        # Добавим общий overall score
        overall = entry.get("overall", {})
        overall_score = overall.get("score")
        if overall_score is not None:
            scores["overall"] = round(overall_score, 2)

        result[formatted_time] = scores

    return result