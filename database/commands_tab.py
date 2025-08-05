# commands.py

from database.simple_orm import BaseModel

class Commands(BaseModel):
    close_1: bool = False
    close_2: bool = False
    amount_1: int = 2
    amount_2: int = 4
    expect_1: float = 0.20
    expect_2: float = 0.40
    timer: int = 600
    process_id: int = 0
    man_pid: int = 0
    btc: bool = False
    eth: bool = True
    sol: bool = False
    
    @classmethod
    def set_btc(cls, value: bool):
        inst = cls.get_instance()
        inst.btc = value
        inst.save()
        return inst
    
    @classmethod
    def set_eth(cls, value: bool):
        inst = cls.get_instance()
        inst.eth = value
        inst.save()
        return inst
    
    @classmethod
    def set_sol(cls, value: bool):
        inst = cls.get_instance()
        inst.sol = value
        inst.save()
        return inst

    @classmethod
    def get_instance(cls):
        inst = cls.get()
        if inst is None:
            inst = cls.create()
        return inst

    @classmethod
    def set_close_1(cls, value: bool):
        inst = cls.get_instance()
        inst.close_1 = value
        inst.save()
        return inst
    
    @classmethod
    def set_close_2(cls, value: bool):
        inst = cls.get_instance()
        inst.close_2 = value
        inst.save()
        return inst

    @classmethod
    def set_proc_id(cls, value: int):
        inst = cls.get_instance()
        inst.process_id = value
        inst.save()
        return inst

    @classmethod
    def set_man_pid(cls, value: int):
        inst = cls.get_instance()
        inst.man_pid = value
        inst.save()
        return inst

    @classmethod
    def set_amount_1(cls, value: int):
        inst = cls.get_instance()
        inst.amount_1 = value
        inst.save()
        return inst

    @classmethod
    def set_amount_2(cls, value: int):
        inst = cls.get_instance()
        inst.amount_2 = value
        inst.save()
        return inst
    
    
    
    @classmethod
    def set_expect_1(cls, value: float):
        inst = cls.get_instance()
        inst.expect_1 = value
        inst.save()
        return inst

    @classmethod
    def set_expect_2(cls, value: float):
        inst = cls.get_instance()
        inst.expect_2 = value
        inst.save()
        return inst

    @classmethod
    def set_timer(cls, value: int):
        inst = cls.get_instance()
        inst.timer = value
        inst.save()
        return inst

    @classmethod
    def get_all(cls):
        return cls.all()

    @classmethod
    def get_field(cls, name: str):
        return getattr(cls.get_instance(), name)
