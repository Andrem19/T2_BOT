import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Union

from database.simple_orm import BaseModel


class Simulation(BaseModel):
    """
    Таблица «по одному экземпляру на день».
    Поле `datetime` хранит дату дня в ISO (YYYY-MM-DD) как строку:
    удобно читать глазами и легко конвертировать в date.

    Почасовые поля хранятся как TEXT с JSON-строкой.
    Обвязка автоматически сериализует dict->str при записи и
    десериализует str->dict при чтении.
    """

    # Ключ (дата дня в ISO): '2025-08-09'
    datetime: str

    # 24 почасовых поля (TEXT). Дефолт — NULL: запись можно создать с датой,
    # а часы дозаполнять по мере появления данных.
    hour_1_pnl: str = None
    hour_2_pnl: str = None
    hour_3_pnl: str = None
    hour_4_pnl: str = None
    hour_5_pnl: str = None
    hour_6_pnl: str = None
    hour_7_pnl: str = None
    hour_8_pnl: str = None
    hour_9_pnl: str = None
    hour_10_pnl: str = None
    hour_11_pnl: str = None
    hour_12_pnl: str = None
    hour_13_pnl: str = None
    hour_14_pnl: str = None
    hour_15_pnl: str = None
    hour_16_pnl: str = None
    hour_17_pnl: str = None
    hour_18_pnl: str = None
    hour_19_pnl: str = None
    hour_20_pnl: str = None
    hour_21_pnl: str = None
    hour_22_pnl: str = None
    hour_23_pnl: str = None
    hour_24_pnl: str = None

    # ------------------------- ПУБЛИЧНОЕ API -------------------------

    @classmethod
    def get_or_create_for_date(
        cls,
        day: Optional[Union[dt.date, dt.datetime, str]] = None
    ) -> "Simulation":
        """
        Гарантированно вернуть единственную запись на указанный день.
        Если её нет — создать. Параллельно обеспечиваем уникальный индекс по дате.
        """
        cls._ensure_unique_index()

        day_str = cls._normalize_date(day)
        obj = cls.get(datetime=day_str)
        if obj is not None:
            return obj
        return cls.create(datetime=day_str)

    @classmethod
    def upsert_hour(
        cls,
        when: Optional[Union[dt.datetime, dt.date, str]] = None,
        value: Optional[Union[Dict[str, Any], str]] = None,
        hour: Optional[int] = None,
    ) -> "Simulation":
        """
        Записать значение за конкретный час.
        - when: datetime/date/'YYYY-MM-DD'/None
        - value: dict (автосериализация в JSON) | str (как есть) | None (очистка)
        - hour: 0..23 или 1..24; если None и when=datetime — берём when.hour
        """
        day_str, hour_1_based = cls._resolve_day_and_hour(when, hour)
        obj = cls.get_or_create_for_date(day_str)
        setattr(obj, cls._hour_field(hour_1_based), cls._to_storage(value))
        obj.save()
        return obj

    @classmethod
    def last_days(
        cls,
        n: int = 7,
        until: Optional[Union[dt.date, dt.datetime, str]] = None
    ) -> List["Simulation"]:
        """
        Вернуть все экземпляры за последние n дней, включая `until` (по умолчанию — сегодня).
        Сортировка по возрастанию даты.
        """
        end_str = cls._normalize_date(until)
        end_date = dt.date.fromisoformat(end_str)
        start_date = end_date - dt.timedelta(days=max(0, n - 1))
        start_str = start_date.isoformat()

        sql = f"""
            SELECT * FROM {cls._tbl_name()}
            WHERE datetime BETWEEN ? AND ?
            ORDER BY datetime ASC, id ASC
        """
        cur = cls._conn.execute(sql, (start_str, end_str))
        rows = cur.fetchall()
        return [cls._row_to_obj(row) for row in rows]

    def get_hour(self, hour: int) -> Optional[Dict[str, Any]]:
        """
        Вернуть значение часа как dict (автодесериализация JSON).
        hour: 0..23 или 1..24.
        """
        i = self._normalize_hour_to_1_based(hour)
        raw = getattr(self, self._hour_field(i))
        return self._from_storage(raw)

    def set_hour(self, hour: int, value: Optional[Union[Dict[str, Any], str]]) -> None:
        """
        Установить значение часа (dict -> JSON) и сохранить запись.
        """
        i = self._normalize_hour_to_1_based(hour)
        setattr(self, self._hour_field(i), self._to_storage(value))
        self.save()

    def day(self) -> dt.date:
        """Получить дату дня как объект date."""
        return dt.date.fromisoformat(self.datetime)

    def as_parsed_dict(self) -> Dict[str, Any]:
        """
        Удобное представление всей записи:
        {
            "datetime": "YYYY-MM-DD",
            "hours": {1: {...}|None, ..., 24: {...}|None}
        }
        """
        hours: Dict[int, Any] = {}
        for i in range(1, 25):
            raw = getattr(self, self._hour_field(i))
            hours[i] = self._from_storage(raw)
        return {"datetime": self.datetime, "hours": hours}

    @classmethod
    def full_day_dict_by_offset(cls, offset_days: int) -> Dict[str, Any]:
        """
        Вернуть полную запись-день (все 24 часа) как dict для UTC-даты:
          offset_days=0 -> сегодня (UTC),
          1 -> вчера (UTC),
          2 -> позавчера (UTC), и т.д.

        Если записи ещё нет — создаётся пустая запись и возвращается
        предсказуемая структура.
        """
        if offset_days < 0:
            raise ValueError("offset_days must be >= 0")

        today_utc = dt.datetime.now(dt.timezone.utc).date()
        target_day = today_utc - dt.timedelta(days=offset_days)
        sim = cls.get_or_create_for_date(target_day)
        return sim.as_parsed_dict()

    # --------- АГРЕГАЦИЯ ДЛЯ ТЕКУЩЕГО UTC-ПЕРИОДА ---------

    @classmethod
    def avg_for_current_period_last_days(cls, days: int) -> Dict[str, Optional[float]]:
        """
        Средний dict {"pnl": ..., "dist": ...} для ТЕКУЩЕГО UTC-ПЕРИОДА
        по последним `days` дням (включая сегодня).

        Периоды (UTC, строго 0..23):
          1:  [8, 9, 10, 11]
          2:  [12, 13, 14, 15]
          3:  [16, 17, 18, 19]
          4:  [20, 21, 22, 23]
          5:  [0, 1, 2, 3]
          6:  [4, 5, 6, 7]

        Алгоритм:
          1) Определяем текущий UTC-час и период (1..6).
          2) Для каждого дня считаем среднее по доступным часам периода.
          3) Затем среднее по дням.
        """
        now_utc = dt.datetime.now(dt.timezone.utc)
        hour0 = now_utc.hour  # 0..23

        period_hours_0 = cls._current_period_hours_0_based(hour0)
        if not period_hours_0:
            return {"pnl": None, "dist": 0}

        sims = cls.last_days(days, until=now_utc)

        from typing import Tuple  # локальный импорт для ясности типов
        day_avgs: List[Tuple[float, float]] = []
        for sim in sims:
            sum_pnl = 0.0
            cnt = 0
            for h in period_hours_0:
                v = sim.get_hour(h)
                if not v:
                    continue
                try:
                    pnl = float(v.get("pnl"))
                except (TypeError, ValueError):
                    continue
                sum_pnl += pnl
                cnt += 1
            if cnt > 0:
                day_avgs.append((sum_pnl / cnt))

        if not day_avgs:
            return {"pnl": None, "dist": 0}

        avg_pnl = sum(p for p in day_avgs) / len(day_avgs)
        return {"pnl": avg_pnl, "dist": 0}

    # ----------------------- ВНУТРЕННИЕ ХЕЛПЕРЫ -----------------------

    @classmethod
    def _ensure_unique_index(cls) -> None:
        """Уникальный индекс по дате (одна запись на день)."""
        cls._conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{cls._tbl_name()}_datetime ON {cls._tbl_name()}(datetime)"
        )
        cls._conn.commit()

    @staticmethod
    def _normalize_date(
        day: Optional[Union[dt.date, dt.datetime, str]]
    ) -> str:
        """Привести к ISO 'YYYY-MM-DD'. None -> сегодня (локальная дата)."""
        if day is None:
            return dt.date.today().isoformat()
        if isinstance(day, dt.datetime):
            return day.date().isoformat()
        if isinstance(day, dt.date):
            return day.isoformat()
        # строка — проверим и нормализуем
        return dt.date.fromisoformat(day).isoformat()

    @classmethod
    def _resolve_day_and_hour(
        cls,
        when: Optional[Union[dt.datetime, dt.date, str]],
        hour: Optional[int],
    ) -> Tuple[str, int]:
        """
        Вернуть (day_str, hour_1_based).
          - Если передан hour: допускаем 0..23 или 1..24.
          - Если hour=None и when=datetime: используем when.hour.
          - Если hour=None и when не datetime: ошибка.
        """
        if isinstance(when, dt.datetime):
            day_str = when.date().isoformat()
            h = when.hour if hour is None else hour
        else:
            day_str = cls._normalize_date(when)
            if hour is None:
                raise ValueError("hour must be provided when `when` has no time component")
            h = hour

        hour_1_based = cls._normalize_hour_to_1_based(h)
        return day_str, hour_1_based

    @staticmethod
    def _normalize_hour_to_1_based(hour: int) -> int:
        """
        Принять 0..23 или 1..24 и вернуть 1..24.
        """
        if 0 <= hour <= 23:
            return hour + 1
        if 1 <= hour <= 24:
            return hour
        raise ValueError("hour must be in 0..23 or 1..24")

    @staticmethod
    def _hour_field(hour_1_based: int) -> str:
        return f"hour_{hour_1_based}_pnl"

    @staticmethod
    def _to_storage(value: Optional[Union[Dict[str, Any], str]]) -> Optional[str]:
        """
        dict -> JSON (compact), str -> как есть, None -> NULL.
        """
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _from_storage(value: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        TEXT (JSON-строка) -> dict; если пусто/не JSON/не dict — None.
        """
        if not value:
            return None
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    # --------- ПЕРИОДЫ (UTC, часы 0..23, без дублей и без «24») ---------

    @staticmethod
    def _periods_utc_raw() -> List[List[int]]:
        """
        Периоды в часах UTC, строго 0..23:
          1:  [8, 9, 10, 11]
          2:  [12, 13, 14, 15]
          3:  [16, 17, 18, 19]
          4:  [20, 21, 22, 23]
          5:  [0, 1, 2, 3]
          6:  [4, 5, 6, 7]
        """
        return [
            [8, 9, 10, 11],     # 1
            [12, 13, 14, 15],   # 2
            [16, 17, 18, 19],   # 3
            [20, 21, 22, 23],   # 4
            [0, 1, 2, 3],       # 5
            [4, 5, 6, 7],       # 6
        ]

    @classmethod
    def _current_period_hours_0_based(cls, hour0: int) -> List[int]:
        """Для текущего UTC-часа (0..23) вернуть список часов периода (0..23)."""
        for block in cls._periods_utc_raw():
            if hour0 in block:
                return block[:]  # копия
        return []
