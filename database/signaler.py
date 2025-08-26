# signaler.py

from database.simple_orm import BaseModel

class Signaler(BaseModel):
    refresh_time: float = 1.0
    signal: int = 0
    score: float = 0
    yscore: float = 0
    rules_count: int = 0

    @classmethod
    def get_instance(cls) -> "Signaler":
        """
        Возвращает единственный «рабочий» экземпляр.
        Если в таблице пусто — создаёт запись с дефолтными значениями.
        """
        inst = cls.get()  # без фильтров: берём первую запись, если есть
        if inst is None:
            inst = cls.create()  # создаст строку с дефолтами
        return inst

    # --- сеттеры, всегда работают с единственным экземпляром ---

    @classmethod
    def set_signal(cls, value: int):
        inst = cls.get_instance()
        inst.signal = value
        inst.save()
        return inst

    @classmethod
    def set_score(cls, value: float):
        inst = cls.get_instance()
        inst.score = value
        inst.save()
        return inst
    @classmethod
    def set_yscore(cls, value: float):
        inst = cls.get_instance()
        inst.yscore = value
        inst.save()
        return inst

    @classmethod
    def set_rules_count(cls, value: int):
        inst = cls.get_instance()
        inst.rules_count = value
        inst.save()
        return inst

    @classmethod
    def set_time(cls, value: float):
        inst = cls.get_instance()
        inst.refresh_time = value
        inst.save()
        return inst

    # (опционально, как в Commands)
    @classmethod
    def get_all(cls):
        return cls.all()

    @classmethod
    def get_field(cls, name: str):
        return getattr(cls.get_instance(), name)
