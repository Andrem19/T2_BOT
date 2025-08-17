from commander.com import Commander
from logging.handlers import TimedRotatingFileHandler
import logging, sys
from database.commands_tab import Commands

commander: Commander = None
START_DATE  = "01-01-2024"
END_DATE    = "01-01-2025"

stages = {
    'simulation': {
        'fut_perc_c': 0.80,
        'fut_perc_p': 0.80,
        'period_avg_pnl': 0.30,
        'period_avg_dist': 0.01,
        'time_to_exp': 10,
        'atr': [0, 0],
        'rsi': 0,
        'last_px': 0,
        'we_need': 'first',
        'position_1': {
            'iv': 0.2,
            'q_frac': 0.1,
            'type': 'Put',
            'qty': 0,
            'ask_indicators': [98, 1],
            'name': 'ETH',
            'symbol': 'ETH-6AUG25-3550-P-USDT',
            'pnl_upper': 0,
            'strike_perc': 0.02,
            'p_t': 0.04,
            'lower_perc': 0.0592,
            'upper_perc': 0.03,
            'best_targ_bid': 0,
            'ask': 0.735,
            'ask_original': 14.7,
            'max_amount': 14.6,
            'pnl': 0
        },
        'position_2': {
            
        },
        'position_3': {
            
        }
    },
    'first': {
        'base_coin': 'SOL',
        'expect': 0.06,
        'amount': 2,
        'exist': False,
        'upper_perc': 0.02,
        'lower_perc': 0.03,
        'over_strike': 0.2,
        'to_exp_init': 0,
        'rel_atr': 0.08,
        'rsi': 55.4,
        'position': {
            'second_taken': False,
            'open_time': '',
            'exist': False,
            'pnl_target': 0,
            'position_info': {
                'positionValue': 0,
                'unrealizedPnl': 0,
                'size': 0,
                'entryPx': 0,
                'side': 0
            },
            'leg': {
                'deliveryTime': 0,
                'hours_to_exp': 0,
                'exist': False,
                'name': '',
                'contracts': 0,
                'tp_id': None,
                'info': {
                    "avgPrice": 0,
                    "curRealisedPnl": 0,
                    "markPrice": 0,
                    "positionValue": 0,
                    "size": 0,
                    "unrealisedPnl": 0,
                    "used_bid": 0,
                }
            }
        }
    },
    'second': {
        'base_coin': 'SOL',
        'expect': 0.20,
        'amount': 6,
        'exist': False,
        'upper_perc': 0.02,
        'lower_perc': 0.03,
        'over_strike': 0.2,
        'to_exp_init': 0,
        'rel_atr': 0.08,
        'rsi': 55.4,
        'position': {
            'second_taken': False,
            'open_time': '',
            'exist': False,
            'pnl_target': 0,
            'position_info': {
                'positionValue': 0,
                'unrealizedPnl': 0,
                'size': 0,
                'entryPx': 0,
                'side': 0
            },
            'leg': {
                'deliveryTime': 0,
                'hours_to_exp': 0,
                'exist': False,
                'name': '',
                'contracts': 0,
                'tp_id': None,
                'info': {
                    "avgPrice": 0,
                    "curRealisedPnl": 0,
                    "markPrice": 0,
                    "positionValue": 0,
                    "size": 0,
                    "unrealisedPnl": 0,
                    "used_bid": 0,
                }
            }
        }
    }
}

actual_bd: Commands = None


    #[{'symbol': 'SOL-16JUL25-155-P-USDT', 'leverage': '', 'autoAddMargin': 0, 'avgPrice': '0.28', 'liqPrice': '', 
    # 'delta': '-0.11228368', 'riskLimitValue': '', 'takeProfit': '', 
    # 'theta': '-0.41549507', 'positionValue': '0.23991034', 'isReduceOnly': False, 'positionIMByMp': '', 
    # 'tpslMode': 'Full', 'riskId': 0, 'trailingStop': '', 'unrealisedPnl': '-0.04008966', 'markPrice': '0.23991034', 'adlRankIndicator': 0, 
    # 'cumRealisedPnl': '', 'positionMM': '0', 'createdTime': '1752609379325', 'positionIdx': 0, 'positionIM': '0', 'positionMMByMp': '', 
    # 'seq': 11052468603, 'updatedTime': '1752609380799', 'side': 'Buy', 'bustPrice': '', 'positionBalance': '', 'leverageSysUpdatedTime': '', 
    # 'curRealisedPnl': '-0.035', 'size': '1', 'positionStatus': 'Normal', 'mmrSysUpdatedTime': '', 'stopLoss': '', 'tradeMode': 0, 
    # 'sessionAvgPrice': '', 'gamma': '0.02196793', 'vega': '0.01132556'}]

next_uid = 1
symbols = [
    "BTCUSDT", 
    "ETHUSDT", 
    "SOLUSDT"
    ]
bids_sol = {
    0.01: 2.0,
    0.025: 3.7,
    0.045: 5.0
}
bids_eth = {
    0.01: 45,
    0.025: 84,
    0.045: 145
}
bids_btc = {
    0.01: 1100,
    0.025: 2700,
    0.045: 4700
}

perc_t = [0.025, 0.03, 0.04]
perc_tp = [0.02, 0.025, 0.03]

instruments = {
    'ETH': {
        'bids': bids_eth,
        'data': None,
        'kof': 0.3,
        'round': 1,
        'overpay_pad': 0.2,
        },
    'BTC': {
        'bids': bids_btc,
        'data': None,
        'kof': 0.01,
        'round': 2,
        'overpay_pad': 0.50,
        },
    'SOL': {
        'bids': bids_sol,
        'data': None,
        'kof':7,
        'round': 0,
        'overpay_pad': 0.01,
        }
    }



# ===================LOGER=========================
class SuppressTelegramAPIRequestsFilter(logging.Filter):
    def filter(self, record):
        # Оставляем всё, кроме httpx-логов, содержащих api.telegram.org (т.е. запросы к Telegram)
        if record.name.startswith("httpx"):
            try:
                msg = record.getMessage()
            except Exception:
                return True
            if "api.telegram.org" in msg:
                return False
        return True
    
    
# 1. Формируем корневой логгер
logging.getLogger("telegram").setLevel(logging.WARNING)
httpx_logger = logging.getLogger("httpx")
httpx_logger.addFilter(SuppressTelegramAPIRequestsFilter())
httpcore_logger = logging.getLogger("httpcore")
httpcore_logger.addFilter(SuppressTelegramAPIRequestsFilter())
root = logging.getLogger()
root.setLevel(logging.INFO)

# 2. Хендлер для консоли (как у вас сейчас)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
))
root.addHandler(console)

# 3. Тime-based ротация: каждый час новый файл, храним 8 последних
file_handler = TimedRotatingFileHandler(
    filename="_logs/bot.log",    # ваш лог-файл
    when="H",              # единица времени — часы
    interval=1,            # ротировать каждый 1 час
    backupCount=12,         # хранить только 8 «старых» файлов
    encoding="utf-8",
    utc=True               # если хотите единообразие по UTC
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d] %(message)s"
))
root.addHandler(file_handler)

# 4. Ваш модуль-логгер
logger = logging.getLogger(__name__)
#================LOGER END=============================

sum = 0
data_list = []
#==================FLAGS===============================
timer_msg = 600
close_1 = False
close_2 = False
simulation = True
opt_types = ['put', 'call']

data_path_sol = "../MARKET_DATA/_crypto_data/SOLUSDT/SOLUSDT_1m.csv"
data_path_eth = "../MARKET_DATA/_crypto_data/ETHUSDT/ETHUSDT_1m.csv"
data_path_btc = "../MARKET_DATA/_crypto_data/BTCUSDT/BTCUSDT_1m.csv"

day_opt = 0
exp_rel = {
    8: 4.8,
    9: 5.1,
    10: 5.4,
    11: 5.8,
    12: 6.1,
    13: 6.4,
    14: 6.7,
    15: 6.9,
    16: 7.2,
    17: 7.4,
    18: 7.6,
    19: 7.9,
    20: 8.2,
    21: 8.5,
    22: 8.8,
    23: 9.3,
    0: 9.7,
    1: 10.1,
    2: 10.6,
    3: 11.0,
    4: 12.0,
    5: 12.0,
    6: 12.0,
    7: 4.4
}