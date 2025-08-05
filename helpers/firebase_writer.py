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

from typing import Any, Dict
import time
import os

import backoff               # лёгкая библиотека для повторов (pip install backoff)
import firebase_admin
from firebase_admin import credentials, db
from shared_vars import logger as _logger

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
        """
        if merge:
            self._update(data)
        else:
            self._set(data)

    # метод close() оставлен «на всякий» — Firebase Admin не требует явного закрытия
    def close(self) -> None:  # pragma: no cover
        """Заглушка: Admin SDK управляет соединениями автоматически."""
        self._ref = None  # type: ignore[assignment]
        _logger.info("FirebaseWriter закрыт")

