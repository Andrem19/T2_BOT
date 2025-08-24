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
from metrics.vizualize import render_btc_indicators_chart
from metrics.load_metrics import load_compact_metrics
from metrics.feature_synergy import analyze_feature_synergies, format_synergies
from metrics.correlation import analyze_features_vs_market
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
import json
import helpers.tlg as tel
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


async def main() -> None:
        sample = load_compact_metrics('metrics.json')
        
        keys=['stables', 'breadth', 'macro', 'price_oi_funding', 'sentiment', 'calendar', 'basis', 'orderbook', 'flows']
        exclude = []

        exclude = keys
            
        path = render_btc_indicators_chart(sample, exclude_keys=exclude, interval='15m')
        
        res = analyze_feature_synergies(sample, symbol="BTCUSDT", market="um",
                                    bins=2, min_support=8, k_max=3, topn=10)
        result = format_synergies(res)
        print('=====================================')
        print(res)
        
        # await tel.send_inform_message("COLLECTOR_API", '', path, True)
        # await asyncio.sleep(2)
        # await tel.send_inform_message("COLLECTOR_API", result, '', False)

if __name__ == "__main__":
    asyncio.run(main())