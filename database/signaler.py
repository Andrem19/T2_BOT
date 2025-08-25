from database.simple_orm import BaseModel

class Signaler(BaseModel):
    refresh_time: float = 1
    signal: int = 0
    score: float = 0.0
    rules_count: int = 0

    @classmethod
    def set_signal(cls, value: int):
        inst = cls.get_instance()
        inst.signal = value
        inst.save()
        return inst
    