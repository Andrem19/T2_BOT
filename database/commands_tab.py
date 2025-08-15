# commands.py

from database.simple_orm import BaseModel

class Commands(BaseModel):
    close_1: bool = False
    close_2: bool = False
    amount_1: int = 2
    amount_2: int = 4
    expect_1_sol: float = 0.20
    expect_2_sol: float = 0.40
    expect_1_eth: float = 1.5
    expect_2_eth: float = 2.7
    expect_1_btc: float = 3.20
    expect_2_btc: float = 5.20
    timer: int = 600
    process_id: int = 0
    man_pid: int = 0
    simulation: bool = True
    fut_perc: float = 0.8
    day_opt: int = 0
    call: bool = False
    put: bool = True
    btc: bool = False
    eth: bool = True
    sol: bool = False
    aloud_only: int = 0
    
    @classmethod
    def set_aloud_only(cls, value: str) :
        inst = cls.get_instance()
        inst.aloud_only = value
        inst.save()
        return inst

    @classmethod
    def set_expect_1(cls, value: float, symbol: str):
        inst = cls.get_instance()
        if symbol == 'SOL':
            inst.expect_1_sol = value
        if symbol == 'ETH':
            inst.expect_1_eth = value
        if symbol == 'BTC':
            inst.expect_1_btc = value
        inst.save()
        return inst

    @classmethod
    def set_expect_2(cls, value: float, symbol: str):
        inst = cls.get_instance()
        if symbol == 'SOL':
            inst.expect_2_sol = value
        if symbol == 'ETH':
            inst.expect_2_eth = value
        if symbol == 'BTC':
            inst.expect_2_btc = value
        inst.save()
        return inst

    @classmethod
    def set_put(cls, value: bool) :
        inst = cls.get_instance()
        inst.put = value
        inst.save()
        return inst

    @classmethod
    def set_call(cls, value: bool) :
        inst = cls.get_instance()
        inst.call = value
        inst.save()
        return inst
    
    @classmethod
    def set_simulation(cls, value: bool) :
        inst = cls.get_instance()
        inst.simulation = value
        inst.save()
        return inst
    
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
    def set_day_opt(cls, value: int):
        inst = cls.get_instance()
        inst.day_opt = value
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
    def set_fut_perc(cls, value: float):
        inst = cls.get_instance()
        inst.fut_perc = value
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
