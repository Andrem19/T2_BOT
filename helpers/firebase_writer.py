"""
firebase_writer.py
==================
Простой враппер для записи словаря в Firebase Realtime Database.
Тестировался на Python 3.10+ и firebase-admin == 6.x.

Установка зависимостей:
    pip install firebase-admin backoff

Не забудьте:
  • создать в консоли Firebase service-account-ключ (JSON)
  • включить Realtime Database и задать правила доступа
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Iterable
import os
import time
import math
from datetime import date, datetime
from decimal import Decimal

import backoff               # лёгкая библиотека для повторов (pip install backoff)
import firebase_admin
from firebase_admin import credentials, db
from shared_vars import logger as _logger

# numpy — опционально: если установлен, конвертируем его типы в примитивы
try:
    import numpy as _np
    _HAS_NUMPY = True
except Exception:
    _HAS_NUMPY = False


def _to_jsonable(obj: Any) -> Any:
    """
    Рекурсивно конвертирует произвольные объекты в JSON-совместимые структуры:
      - Decimal -> float (если число конечное) иначе str
      - float NaN/Inf -> None (Firebase/JSON не принимают)
      - datetime/date -> ISO-строка
      - numpy scalar -> базовый Python-тип
      - numpy.ndarray -> list
      - set/tuple -> list
      - dict -> dict со строковыми ключами
      - всё иное -> str как безопасный fallback
    """
    # Базовые примитивы
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj

    # float с нормализацией NaN/Inf
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None

    # Decimal -> float/str
    if isinstance(obj, Decimal):
        try:
            f = float(obj)
            return f if math.isfinite(f) else str(obj)
        except Exception:
            return str(obj)

    # Даты/время -> ISO
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    # numpy-типы
    if _HAS_NUMPY:
        if isinstance(obj, _np.generic):  # np.int64/np.float32/np.bool_/np.str_ и т.п.
            return _to_jsonable(obj.item())
        if isinstance(obj, _np.ndarray):
            return [_to_jsonable(x) for x in obj.tolist()]

    # Словари/маппинги -> dict со строковыми ключами
    if isinstance(obj, Mapping):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            key = str(k)  # Firebase Realtime DB требует строковые ключи
            out[key] = _to_jsonable(v)
        return out

    # Итерируемые (list/tuple/set/...) -> list
    if isinstance(obj, Iterable) and not isinstance(obj, (bytes, bytearray)):
        return [_to_jsonable(x) for x in obj]

    # Запасной вариант — строка
    return str(obj)


class FirebaseWriter:
    """
    Простой интерфейс:
        fw = FirebaseWriter(db_url, cred_path, node="dashboard")
        while True:
            fw.write({"foo": 1, "bar": 2.3})
            time.sleep(3)
    """

    def __init__(
        self,
        db_url: str,
        cred_path: str,
        node: str = "dashboard",
        *,
        initialize_kwargs: Dict[str, Any] | None = None,
    ) -> None:
        """
        Parameters
        ----------
        db_url        : URL вида https://<project-id>.firebaseio.com/
        cred_path     : путь к вашему serviceAccountKey.json
        node          : относительный путь узла для записи
        initialize_kwargs : передаются как есть в firebase_admin.initialize_app()
        """
        self._node = node.strip("/")
        initialize_kwargs = initialize_kwargs or {}

        # Инициализируем SDK только один раз на весь процесс
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(
                cred, {"databaseURL": db_url, **initialize_kwargs}
            )
            _logger.info("Firebase Admin инициализирован")

        self._ref = db.reference(self._node)
        _logger.info("Ссылка на узел '%s' готова", self._node)

    # -------------------------- low-level helpers -------------------------- #

    @staticmethod
    def _with_retry():
        """Декоратор от backoff с логированием."""
        return backoff.on_exception(
            backoff.expo,
            Exception,
            max_tries=3,
            jitter=backoff.full_jitter,
            logger=_logger,
        )

    @_with_retry()  # type: ignore[misc]
    def _set(self, data: Dict[str, Any]) -> None:
        self._ref.set(data)

    @_with_retry()  # type: ignore[misc]
    def _update(self, data: Dict[str, Any]) -> None:
        self._ref.update(data)

    # -------------------------- public API -------------------------- #

    def write(self, data: Dict[str, Any], *, merge: bool = False) -> None:
        """
        Записать словарь в узел.
        merge=False (по-умолчанию) → ref.set(data)  — перезаписывает узел целиком.
        merge=True                → ref.update(data) — обновляет только указанные ключи.

        Перед записью данные рекурсивно приводятся к JSON-совместимому виду,
        что предотвращает ошибки вида: TypeError: Object of type Decimal is not JSON serializable
        """
        jsonable = _to_jsonable(data)

        # Небольшая защита от пустых dict (на всякий случай)
        if not isinstance(jsonable, dict):
            _logger.warning(
                "Ожидался dict для записи в Firebase, получено %s — оберну в корневой ключ",
                type(jsonable).__name__,
            )
            jsonable = {"value": jsonable}

        if merge:
            self._update(jsonable)
        else:
            self._set(jsonable)

    # метод close() оставлен «на всякий» — Firebase Admin не требует явного закрытия
    def close(self) -> None:  # pragma: no cover
        """Заглушка: Admin SDK управляет соединениями автоматически."""
        self._ref = None  # type: ignore[assignment]
        _logger.info("FirebaseWriter закрыт")
