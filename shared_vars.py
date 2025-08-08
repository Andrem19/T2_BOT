from commander.com import Commander
from logging.handlers import TimedRotatingFileHandler
import logging, sys

commander: Commander = None
START_DATE  = "01-01-2024"
END_DATE    = "01-01-2025"


# === Best Option Strategy ===
# P&L: 0.01
# ATR: [33.35, 0.009208]
# RSI: 46.67
# Symbol: ETH-6AUG25-3550-P-USDT
# Last px: 3622.23 (0.0203)
# Total Trades: 622
# Time to Expiry: 17.897982738055557
# Ask Indicator: (60.96, 2.08)

# Future Calculation:
#   qty: 0.0246
#   pnl_upper: 1.9367
#   pnl_lower: 0.4842
#   up_share: 0.8
#   feasible_min: 0.0068
#   feasible_max: 0.0268
#   clipped: False
#   ask: 0.735
#   ask_original: 14.7
#   p_t: 0.04

# Max amount we can take: 14.6
# Target Bid: 129.75
# Lower Target: 3408.0 (-0.0592%)
# Upper Target: 3730.95 (+0.03%)
# Position we need: first
# Speed sec: 13.41

stages = {
    'simulation': {
        'time_to_exp': 10,
        'atr': [0, 0],
        'rsi': 0,
        'last_px': 0,
        'we_need': 'first',
        'position_1': {
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
        'kof': 0.05,
        'round': 1,
        'overpay_pad': 0.2,
        },
    'BTC': {
        'bids': bids_btc,
        'data': None,
        'kof': 0.002,
        'round': 2,
        'overpay_pad': 0.50,
        },
    'SOL': {
        'bids': bids_sol,
        'data': None,
        'kof':1,
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

data_path_sol = "/media/Disk_2/PYTHON/MARKET_DATA/_crypto_data/SOLUSDT/SOLUSDT_1m.csv"
data_path_eth = "/media/Disk_2/PYTHON/MARKET_DATA/_crypto_data/ETHUSDT/ETHUSDT_1m.csv"
data_path_btc = "/media/Disk_2/PYTHON/MARKET_DATA/_crypto_data/BTCUSDT/BTCUSDT_1m.csv"