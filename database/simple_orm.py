"""
simple_orm.py
=============

Мини-ORM для SQLite ≥ 3.35 с автоматической миграцией схемы
и минималистичным API.  Подходит для небольших настольных
или скриптовых проектов, где **не нужны** отдельные файлы миграций.

© 2025 — распространяется под MIT License.
"""
from __future__ import annotations
import json as _json
import decimal as _decimal
import datetime as _dt
import os as _os
import sqlite3 as _sqlite
import threading as _threading
from dataclasses import (
    dataclass as _dataclass,
    field as _field,
    fields as _dc_fields,
    MISSING as _MISSING,
)
from typing import Any, ClassVar, Dict, Iterable, List, Sequence, Tuple, Type, TypeVar


_T = TypeVar("_T", bound="BaseModel")

_SQL_TYPES: Dict[type, str] = {
    int: "INTEGER",
    str: "TEXT",
    float: "REAL",
    bool: "INTEGER",
    _dt.datetime: "TEXT",
}

_SQL_DATETIME_FMT = "%Y-%m-%dT%H:%M:%S"


def _py_to_sql(value: Any) -> Any:
    if callable(value) and not isinstance(value, type):
        try:
            value = value()
        except TypeError:
            pass
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.strftime(_SQL_DATETIME_FMT)
    if isinstance(value, _dt.date):
        return value.isoformat()
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, _decimal.Decimal):
        return float(value)
    if isinstance(value, (list, dict, tuple, set)):
        return _json.dumps(value)
    return value


def _sql_to_py(value: Any, py_type: type) -> Any:
    if value is None:
        return None
    if py_type is bool:
        return bool(value)
    if py_type is _dt.datetime:
        return _dt.datetime.strptime(value, _SQL_DATETIME_FMT)
    if py_type is _dt.date:
        return _dt.date.fromisoformat(value)
    if py_type is _decimal.Decimal:
        return _decimal.Decimal(str(value))
    if py_type in (list, dict, tuple, set):
        try:
            decoded = _json.loads(value)
            return py_type(decoded) if py_type is not tuple else tuple(decoded)
        except Exception:
            return value
    try:
        return py_type(value)
    except Exception:
        return value


class _ModelMeta(type):
    _registry: List[Type["BaseModel"]] = []

    def __new__(mcls, name, bases, namespace):
        cls = super().__new__(mcls, name, bases, namespace)
        cls = _dataclass(cls)
        if name != "BaseModel":
            _ModelMeta._registry.append(cls)
        return cls


class BaseModel(metaclass=_ModelMeta):
    """
    Базовая модель ORM.
    """

    # Глобальный коннект и глобальная блокировка для сериализации всех операций.
    _conn: ClassVar[_sqlite.Connection] = None
    _lock: ClassVar[_threading.RLock] = _threading.RLock()

    id: int = _field(init=False, default=None, repr=False)

    # ---------- CRUD ----------

    @classmethod
    def create(cls: Type[_T], **kwargs) -> _T:
        obj = cls(**kwargs)
        obj.save()
        return obj

    def save(self) -> None:
        cls = self.__class__
        if cls._conn is None:
            raise RuntimeError("Database is not initialized. Call initialize(path) first.")

        field_names = cls._field_names()
        values = [_py_to_sql(getattr(self, f)) for f in field_names]

        with cls._lock:
            if getattr(self, "id", None) is None:  # INSERT
                placeholders = ", ".join("?" for _ in field_names)
                sql = f"INSERT INTO {cls._tbl_name()} ({', '.join(field_names)}) VALUES ({placeholders})"
                cur = cls._conn.execute(sql, values)
                self.id = cur.lastrowid
            else:  # UPDATE
                assignments = ", ".join(f"{f}=?" for f in field_names)
                sql = f"UPDATE {cls._tbl_name()} SET {assignments} WHERE id=?"
                cls._conn.execute(sql, values + [self.id])
            cls._conn.commit()

    @classmethod
    def get(cls: Type[_T], **filters) -> _T | None:
        objs = cls.filter(**filters)
        return objs[0] if objs else None

    @classmethod
    def all(cls: Type[_T]) -> List[_T]:
        return cls.filter()

    @classmethod
    def filter(cls: Type[_T], **filters) -> List[_T]:
        if cls._conn is None:
            raise RuntimeError("Database is not initialized. Call initialize(path) first.")
        if filters:
            cond = " AND ".join(f"{k}=?" for k in filters)
            sql = f"SELECT * FROM {cls._tbl_name()} WHERE {cond}"
            params = [_py_to_sql(v) for v in filters.values()]
        else:
            sql = f"SELECT * FROM {cls._tbl_name()}"
            params = []
        with cls._lock:
            cur = cls._conn.execute(sql, params)
            rows = cur.fetchall()
        return [cls._row_to_obj(row) for row in rows]

    def delete(self) -> None:
        cls = self.__class__
        if cls._conn is None:
            raise RuntimeError("Database is not initialized. Call initialize(path) first.")
        if getattr(self, "id", None) is None:
            return
        with cls._lock:
            cls._conn.execute(f"DELETE FROM {cls._tbl_name()} WHERE id=?", (self.id,))
            cls._conn.commit()
        self.id = None

    # ---------- Вспомогательные ----------

    @classmethod
    def _tbl_name(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def _field_defs(cls) -> Dict[str, Tuple[type, Any]]:
        out: Dict[str, Tuple[type, Any]] = {}
        for f in _dc_fields(cls):
            if f.name == "id":
                continue
            if f.default is not _MISSING:
                default_val = f.default
            elif getattr(f, "default_factory", _MISSING) is not _MISSING:  # type: ignore[attr-defined]
                default_val = f.default_factory()                           # type: ignore[attr-defined]
            else:
                default_val = None
            out[f.name] = (f.type, default_val)
        return out

    @classmethod
    def _field_names(cls) -> List[str]:
        return list(cls._field_defs().keys())

    @classmethod
    def _row_to_obj(cls: Type[_T], row: _sqlite.Row) -> _T:
        kwargs = {}
        for name, (py_type, _) in cls._field_defs().items():
            kwargs[name] = _sql_to_py(row[name], py_type)
        obj = cls(**kwargs)
        obj.id = row["id"]
        return obj

    # ---------- Миграции ----------

    @classmethod
    def _ensure_table(cls) -> None:
        with cls._lock:
            cur = cls._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (cls._tbl_name(),)
            )
            if cur.fetchone() is None:
                cls._create_table()
            else:
                cls._alter_table()

    @classmethod
    def _create_table(cls) -> None:
        cols_sql: List[str] = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for col, (py_type, default) in cls._field_defs().items():
            sql_type = _SQL_TYPES.get(py_type)
            if sql_type is None:
                raise TypeError(f"Unsupported field type: {py_type} in {cls.__name__}.{col}")
            default_sql = f" DEFAULT {_py_to_sql(default)}" if default is not None else ""
            cols_sql.append(f"{col} {sql_type}{default_sql}")
        sql = f"CREATE TABLE {cls._tbl_name()} ({', '.join(cols_sql)})"
        with cls._lock:
            cls._conn.execute(sql)
            cls._conn.commit()

    @classmethod
    def _alter_table(cls) -> None:
        with cls._lock:
            cur = cls._conn.execute(f"PRAGMA table_info({cls._tbl_name()})")
            existing_cols = {row["name"]: row for row in cur.fetchall()}  # type: ignore[index]
            model_cols = cls._field_defs()

            for col, (py_type, default) in model_cols.items():
                if col not in existing_cols:
                    sql_type = _SQL_TYPES[py_type]
                    default_sql = f" DEFAULT {_py_to_sql(default)}" if default is not None else ""
                    cls._conn.execute(
                        f"ALTER TABLE {cls._tbl_name()} ADD COLUMN {col} {sql_type}{default_sql}"
                    )

            cols_to_drop = [c for c in existing_cols if c != "id" and c not in model_cols]
            if cols_to_drop:
                vers_tuple = tuple(map(int, _sqlite.sqlite_version.split(".")))
                if vers_tuple >= (3, 35, 0):
                    for col in cols_to_drop:
                        cls._conn.execute(f"ALTER TABLE {cls._tbl_name()} DROP COLUMN {col}")
                else:
                    cls._rebuild_table_without(cols_to_drop)
            cls._conn.commit()

    @classmethod
    def _rebuild_table_without(cls, drop_cols: Sequence[str]) -> None:
        keep_cols = ["id"] + [c for c in cls._field_names() if c not in drop_cols]
        col_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for c in cls._field_names():
            if c in drop_cols:
                continue
            py_type, default = cls._field_defs()[c]
            sql_type = _SQL_TYPES[py_type]
            default_sql = f" DEFAULT {_py_to_sql(default)}" if default is not None else ""
            col_defs.append(f"{c} {sql_type}{default_sql}")

        temp = f"__tmp_{cls._tbl_name()}"
        with cls._lock:
            cls._conn.execute(f"CREATE TABLE {temp} ({', '.join(col_defs)})")
            cls._conn.execute(
                f"INSERT INTO {temp} ({', '.join(keep_cols)}) "
                f"SELECT {', '.join(keep_cols)} FROM {cls._tbl_name()}"
            )
            cls._conn.execute(f"DROP TABLE {cls._tbl_name()}")
            cls._conn.execute(f"ALTER TABLE {temp} RENAME TO {cls._tbl_name()}")



# ----------------------------------------------------------------------
# Функция-точка входа
# ----------------------------------------------------------------------

def initialize(path: str) -> None:
    """
    Открывает (или создаёт) SQLite-файл *path*, проверяет/обновляет
    все зарегистрированные модели.

    ВАЖНО: создаём коннект с check_same_thread=False и сериализуем доступ
    глобальной блокировкой BaseModel._lock — это позволяет безопасно
    обращаться к одному коннекту из разных потоков (например, основной цикл
    и отдельный поток планировщика/asyncio-loop).
    """
    first_start = not _os.path.exists(path)

    # Разрешаем использование коннекта из разных потоков.
    conn = _sqlite.connect(path, check_same_thread=False, timeout=30.0)
    conn.row_factory = _sqlite.Row

    # Консервативные PRAGMA для многопоточности
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA legacy_alter_table = OFF")
    except Exception:
        pass

    BaseModel._conn = conn

    # Схема таблиц
    for model in _ModelMeta._registry:
        model._ensure_table()

    if first_start:
        conn.commit()
