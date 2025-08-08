import asyncio
import traceback
import shared_vars as sv
from shared_vars import logger
from decouple import config
from helpers.price_feed import start_price_streams, ensure_streams_alive_for_symbols, PriceCache
from helpers.safe_sender import safe_send
from helpers.firebase_writer import FirebaseWriter
import database.simple_orm as DataBase
from database.commands_tab import Commands
from exchanges.hyperliquid_api import HL
from exchanges.bybit_option_hub import BybitOptionHub as BB
from exchanges.bybit_option_hub import update_leg_subscriptions, ensure_option_feed_alive
import services.monitoring as monitoring
import services.open_option as open_opt
import helpers.tlg as tlg
import services.open_futures as open_fut
import services.search as search
import services.refresh_fut_info as refresh_fut
import services.refresh_opt_info as refresh_opt
import services.serv as serv
import helpers.tools as ts
import simulation.load_data as load_data
from datetime import datetime


async def main():
    #===========INITIALIZE============
    DataBase.initialize("tbot.db")
    await asyncio.sleep(1)
    await serv.refresh_commands_from_bd()
    fw = FirebaseWriter(
        db_url=config('FIREBASE_DB_URL'),
        cred_path=config('FIREBASE_JSON_KEY'),
        node="dashboard",
    )
    BB.initialise(testnet=False)
    symbs = [s+ 'USDT' for s in sv.symbols]
    start_price_streams(symbs, klines={60: 200})
    await asyncio.sleep(2)
    logger.info("Services sucsessfuly initialized")
    logger.info("Loading candles %s → %s…", sv.START_DATE, sv.END_DATE)
    if 'SOL' in sv.symbols:
        sv.instruments['SOL']['data'] = load_data.load_candles(path=sv.data_path_sol, start_date=sv.START_DATE, end_date=sv.END_DATE)
    if 'ETH' in sv.symbols:
        sv.instruments['ETH']['data'] = load_data.load_candles(path=sv.data_path_eth, start_date=sv.START_DATE, end_date=sv.END_DATE)
    if 'BTC' in sv.symbols:
        sv.instruments['BTC']['data'] = load_data.load_candles(path=sv.data_path_btc, start_date=sv.START_DATE, end_date=sv.END_DATE)

    #=========CHECK POSITIONS===========
    position_1 = HL.get_position(account_idx=1)
    position_2 = HL.get_position(account_idx=2)
    Commands.set_close_1(False)
    Commands.set_close_2(False)
    commands = Commands.get_instance()
    logger.info('Commands: %s', commands)

    if position_1 or position_2:
        st = serv.load_stages()
        if st is not None:
            sv.stages = st
        if not position_2:
            sv.stages['first']['exist'] = False
        if not position_1:
            sv.stages['second']['exist'] = False
                  
    logger.info(sv.stages)
            
    logger.info("Start info loaded!")
    await asyncio.sleep(2)
    
    rare_timer = datetime.now().timestamp()
    counter = 0
    #==============MAIN LOOP=================
    while True:
        try:
            #======COMMANDS==========
            
            await serv.refresh_commands_from_bd()
            
            #========REFRESH POSITION INFO=========
            
            await refresh_fut.refresh_fut(counter)
            
            #========REFRESH OPTIONS INFO==========
            
            await refresh_opt.refresh_opt(counter)
            
            #===========CALCULATION================
            
            which_pos_we_need = open_opt.get_required_position()
            if counter%6==0 and sv.simulation:
                await search.search(which_pos_we_need)
            
            #===========OPEN POSITION==============
            
            if which_pos_we_need != 'nothing' and datetime.now().hour not in [4,5,6,7]:
                
                best_simulation = sv.stages['simulation']['position_1']
                
                if best_simulation['pnl'] >= sv.stages[which_pos_we_need]['expect']:
                    print(best_simulation)
                    opt_is_open = await open_opt.open_opt(best_simulation, which_pos_we_need)
                    if opt_is_open:
                        fut_is_open = await open_fut.open_futures(best_simulation, which_pos_we_need)
                        if fut_is_open:
                            serv.save_stages(sv.stages)
                            await tlg.send_option_message('COLLECTOR_API', f"✅✅✅\nPosition was opened SUCCESSFULY!!!\n\n{serv.format_option_message_html(sv.stages['simulation']['position_1'])}", '', False)

            #===========MONITORING=================
            
            try:
                fb_dict = serv.get_state_dict(sv.stages)
                fw.write(fb_dict)
            except Exception as e:
                logger.exception(f'ERROR when saving stages in realtime database: {e}')
            
            ensure_streams_alive_for_symbols(symbs, max_stale_seconds=30)
            
            if sv.stages['second']['exist']:
                last_pr = PriceCache.get(sv.stages['second']['base_coin'] + 'USDT')
                await monitoring.process_position(last_pr, 'second')
                
            if sv.stages['first']['exist']:
                last_pr = PriceCache.get(sv.stages['first']['base_coin'] + 'USDT')
                await monitoring.process_position(last_pr, 'first')

            if sv.stages['second']['exist'] or sv.stages['first']['exist']:
                ensure_option_feed_alive()
            
            #======================================
            time_now = datetime.now().timestamp()
            if rare_timer+3600<time_now:
                update_leg_subscriptions(sv.stages)
                rare_timer = time_now
            counter+=1
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.exception("Unhandled exception in main loop: %s", e)
            await safe_send("TELEGRAM_API", f'MAIN LOOP ERROR: {e}\n\n{traceback.format_exc()}', '', False)

if __name__ == "__main__":
    asyncio.run(main())