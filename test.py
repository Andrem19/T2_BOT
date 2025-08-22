import helpers.tools as tools
import asyncio
import sys
from exchanges.bybit_option_hub import BybitOptionHub as BB
from exchanges.deribit_option_hub import DeribitOptionHub as DB
import shared_vars as sv
from datetime import datetime, timezone
from exchanges.hyperliquid_api import HL
from heapq import nsmallest
from simulation.load_data import load_candles
from simulation.simulation import simulation
from helpers.safe_sender import safe_send
from database.simple_orm import initialize
import services.serv as serv
import helpers.tlg as tlg
from helpers.metrics import analyze_option_slice, pic_best_opt
from database.hist_trades import Trade
from database.simulation import Simulation
from commander.service import format_trades_report
from metrics.hourly_scheduler import start_hourly_57_scheduler
from openai import OpenAI
from decouple import config
import json
from metrics.serv import get_rr25_iv
import re
from metrics.indicators import MarketIntel

START_DATE  = "01-01-2024"
END_DATE    = "01-01-2025"

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

perc_t = [0.025, 0.03, 0.04, 0.05]
perc_tp = [0.02, 0.025, 0.03, 0.04]


#!/usr/bin/env python3
# example_collect_news.py
# Минимальный запуск: собрать и распечатать новости

from metrics.market_watch import collect_news

async def main() -> None:
    res = HL.place_limit_post_only('BTCUSDT', 'Sell', 116784, 0, 0.002, False, 2)
    print(res)
   


if __name__ == "__main__":
    asyncio.run(main())