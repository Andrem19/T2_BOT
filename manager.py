import asyncio
from commander.telegram_commander import init_commander, setup_telegram_monitor, check_and_handle_message
from database.simple_orm import initialize
from database.commands_tab import Commands
from commander.process_utils import is_process_alive, announce_self_pid
from exchanges.bybit_option_hub import BybitOptionHub as BB
from helpers.safe_sender import safe_send
from metrics.hourly_scheduler import start_hourly_57_scheduler

async def main():
    initialize("tbot.db")
    BB.initialise(testnet=False)
    init_commander()
    await setup_telegram_monitor()
    start_hourly_57_scheduler(
        metrics_filename="metrics.json",
        allow_overlap=False,
        warn_after_sec=240,
        worker_max_workers=1
    )
    announce_self_pid()
    
    
    while True:
        try:
            com = Commands.get_instance()
            if com.process_id != 0:
                res = is_process_alive(com.process_id)
                if not res:
                    await safe_send('TELEGRAM_API', f'Process {com.process_id} is not alive')
            await check_and_handle_message()
            await asyncio.sleep(1)
        except Exception as e:
            await safe_send('TELEGRAM_API', f'Manager {e}')

if __name__ == "__main__":
    asyncio.run(main())