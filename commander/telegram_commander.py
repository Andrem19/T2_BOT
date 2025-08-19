from __future__ import annotations

import asyncio
import traceback
import time
import uuid
from typing import Optional

import shared_vars as sv
from commander.com import Commander

# Используем тот же стек, что и раньше
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.request import HTTPXRequest
from decouple import config


from database.commands_tab import Commands
import helpers.tools as tools
import helpers.tlg as tel
from aiohttp import web
import commander.process_utils as proc
from database.hist_trades import Trade
from database.simulation import Simulation
import services.serv as serv
from helpers.statistics import compute_trade_stats, visualize_trade_stats
from commander.service import format_trades_report, tail_log


# ---------------------------------------------------------------------------
# Команды
# ---------------------------------------------------------------------------


async def wings(val: str):
    try:
        Commands.set_perc_wings(float(val))
        await tel.send_inform_message("COLLECTOR_API", f"perc_wings: {val}", "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f"{e}", "", False)

async def exp_cor(val: str):
    try:
        Commands.set_exp_kor(float(val))
        await tel.send_inform_message("COLLECTOR_API", f"exp_kor: {val}", "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f"{e}", "", False)

async def aloud_mode(val: str):
    try:
        mode = 0 if val.lower() == 'all' else 1 if val.lower() == 'put' else 2 if val.lower() == 'call' else 3
        Commands.set_aloud_only(mode)
        await tel.send_inform_message("COLLECTOR_API", f"aloud_only: {val}", "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f"{e}", "", False)


async def amount(cont: str, pos) -> None:
    try:
        p=int(pos)
        if p == 1:
            Commands.set_amount_1(int(cont))
            await tel.send_inform_message("COLLECTOR_API", f"New base amount first: {cont}", "", False)
        else:
            Commands.set_amount_2(int(cont))
            await tel.send_inform_message("COLLECTOR_API", f"New base amount second: {cont}", "", False)
    except Exception as e:
        print(e)


async def types(type_P_C: str, flag_0_1: str):
    try:
        flag = int(flag_0_1)
        commands = {}
        if type_P_C == 'p':
            commands = Commands.set_put(flag)
        elif type_P_C == 'c':
            commands = Commands.set_call(flag)
        await tel.send_inform_message("COLLECTOR_API", f"{commands}", "", False)
    except Exception as e:
        print(e)
    
async def futperc(val: str, C_P: str) -> None:
    try:
        if C_P.lower() == 'c':
            Commands.set_fut_perc_c(float(val))
        else:
            Commands.set_fut_perc_p(float(val))
        await tel.send_inform_message("COLLECTOR_API", f"New fut_perc_{C_P.lower()}: {val}", "", False)
    except Exception as e:
        print(e)
    
async def expect(val: str, pos: str, symbol: str) -> None:
    try:
        p=int(pos)
        if p == 1:
            Commands.set_expect_1(float(val), symbol.upper())
            await tel.send_inform_message("COLLECTOR_API", f"New expect {symbol.upper()} first: {val}", "", False)
        else:
            Commands.set_expect_2(float(val), symbol.upper())
            await tel.send_inform_message("COLLECTOR_API", f"New expect {symbol.upper()} second: {val}", "", False)
    except Exception as e:
        print(e)

async def simulation(flag_0_1: str) -> None:
    flag = int(flag_0_1)
    commands = Commands.set_simulation(flag)
    await tel.send_inform_message("COLLECTOR_API", f"{commands}", "", False)

async def symb(symb: str, flag_0_1: str) -> None:
    symbol = symb.upper()
    flag = int(flag_0_1)
    commands = {}
    if symbol == 'BTC':
        commands = Commands.set_btc(flag)
    if symbol == 'ETH':
        commands = Commands.set_eth(flag)
    if symbol == 'SOL':
        commands = Commands.set_sol(flag)
    await tel.send_inform_message("COLLECTOR_API", f"{commands}\n\nmain.py will be restarted", "", False)
    await off()
    await asyncio.sleep(8)
    await start()
    
async def trade_hist(days: str):
    try:
        hist = Trade.last_n_days(int(days))
        hist_dicts = [vars(obj) for obj in hist]
        report = format_trades_report(hist_dicts)
        await tel.send_inform_message("COLLECTOR_API", f"{report}", "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f"{e}", "", False)

async def sim_hist(day: str):
    try:
        today_dict = Simulation.full_day_dict_by_offset(int(day))
        pretty_str = tools.dict_to_pretty_string(today_dict)
        await tel.send_inform_message("COLLECTOR_API", f"{pretty_str}", "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f"{e}", "", False)

async def close(pos) -> None:
    p=int(pos)
    if p == 1:
        Commands.set_close_1(True)
    else:
        Commands.set_close_2(True)


async def open(pos) -> None:
    p=int(pos)
    if p == 1:
        Commands.set_close_1(False)
    else:
        Commands.set_close_2(False)


async def stat(days: str):
    try:
        hist = Trade.last_n_days(int(days))
        hist_dicts = [vars(obj) for obj in hist]  # включая id
        stat = compute_trade_stats(hist_dicts, int(days), True)
        paths = visualize_trade_stats(hist_dicts, stat, out_dir="_charts")
        for k, v in paths.items():
            await tel.send_inform_message("COLLECTOR_API", f"", v, True)
            await asyncio.sleep(2)
    except Exception as e:
        print(e)

async def stattext(days: str):
    try:
        hist = Trade.last_n_days(int(days))
        hist_dicts = [vars(obj) for obj in hist]  # включая id
        stat = compute_trade_stats(hist_dicts, int(days), False)
        await tel.send_inform_message("COLLECTOR_API", f'{tools.dict_to_pretty_string(stat)}', '', False)
    except Exception as e:
        print(e)

async def timer(sec: str):
    try:
        Commands.set_timer(int(sec))
        await tel.send_inform_message("COLLECTOR_API", f"New timer value: {sec}", "", False)
    except Exception as e:
        print(e)
        
async def balances():
    try:
        _, msg = await serv.get_balances()
        await tel.send_inform_message("COLLECTOR_API", f"{msg}", "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f"{e}", "", False)


async def tail(b_me_mo: str, lines: str):
    try:
        f = 'bot.log' if b_me_mo == 'b' else 'manager.err' if b_me_mo == 'me' else 'manager.out' if b_me_mo == 'mo' else 'bot.log'
        log = tail_log(file_path=f'./_logs/{f}', lines_count=int(lines))
        await tel.send_inform_message("COLLECTOR_API", f"{log}", "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f"{e}", "", False)

async def day(day: str):
    try:
        Commands.set_day_opt(int(day))
        await tel.send_inform_message("COLLECTOR_API", f"day_opt seted up to : {day}", "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f"{e}", "", False)

async def get_pids():
    com = Commands.get_instance()
    msg = f'PID manager: {com.man_pid} PID main: {com.process_id}'
    await tel.send_inform_message("COLLECTOR_API", msg, "", False)

async def start():
    try:
        proc_pid = proc.start_main()
        Commands.set_proc_id(proc_pid)
        if proc_pid:
            await tel.send_inform_message("COLLECTOR_API", f'Process main.py started successfuly. PID: {proc_pid}', "", False)
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f'{e}', "", False)
        
async def off():
    try:
        com = Commands.get_instance()
        result = proc.kill_process(com.process_id)
        
        if result:
            await tel.send_inform_message("COLLECTOR_API", f'Procces id {com.process_id} successfuly killed', "", False)
            Commands.set_proc_id(0)
        
    except Exception as e:
        await tel.send_inform_message("COLLECTOR_API", f'{e}', "", False)


async def info() -> None:
    await tel.send_inform_message("COLLECTOR_API", sv.commander.show_tree(), "", False)

async def commands_db() -> None:
    com = Commands.get_instance()
    await tel.send_inform_message("COLLECTOR_API", f'{tools.dict_to_pretty_string(com.__dict__)}', "", False)

def init_commander():
    sv.commander = Commander(logs=True)
    sv.commander.add_command(["info"], info)
    sv.commander.add_command(["close"], close)
    sv.commander.add_command(["open"], open)
    sv.commander.add_command(["timer"], timer)
    sv.commander.add_command(["amount"], amount)
    sv.commander.add_command(["expect"], expect)
    sv.commander.add_command(["futperc"], futperc)
    sv.commander.add_command(["pids"], get_pids)
    sv.commander.add_command(["com"], commands_db)
    sv.commander.add_command(["types"], types)
    sv.commander.add_command(["mode"], aloud_mode)
    sv.commander.add_command(["cor"], exp_cor)
    sv.commander.add_command(["tail"], tail)
    sv.commander.add_command(["wings"], wings)
    sv.commander.add_command(["day"], day)
    sv.commander.add_command(["hist"], trade_hist)
    sv.commander.add_command(["bal"], balances)
    sv.commander.add_command(["simhist"], sim_hist)
    sv.commander.add_command(["simulation"], simulation)
    sv.commander.add_command(["symb"], symb)
    sv.commander.add_command(["stat"], stat)
    sv.commander.add_command(["statt"], stattext)
    sv.commander.add_command(["run"], start)
    sv.commander.add_command(["off"], off)


# ---------------------------------------------------------------------------
# Монитор Telegram: Webhook + Polling fallback
# ---------------------------------------------------------------------------

class TelegramMonitor:
    """
    Управляет приёмом апдейтов через webhook (основной режим) с фолбэком на polling.
    Предоставляет метод heartbeat(), который вызывается в главном цикле приложения.
    """

    def __init__(
        self,
        api_token: str,
        commander: Commander,
        *,
        chat_id: str,
        webhook_url: Optional[str] = None,
        secret_token: Optional[str] = None,
        listen_host: str = "0.0.0.0",
        listen_port: int = 8081,
        path: str = "/telegram",
        alive_timeout: float = 120.0,
        max_retries_before_fallback: int = 3,
        logs: bool = True,
    ) -> None:
        self.api_token = api_token
        self.commander = commander
        self.chat_id = str(chat_id)

        self.webhook_url = webhook_url  # публичный URL (https://domain/path)
        self.secret_token = secret_token or uuid.uuid4().hex
        self.listen_host = listen_host
        self.listen_port = int(listen_port)
        self.path = path

        self.alive_timeout = alive_timeout
        self.max_retries_before_fallback = max_retries_before_fallback
        self.logs = logs

        # runtime
        self._mode: str = "idle"  # idle | webhook | polling
        self._last_update_ts: float = 0.0
        self._server_app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._server_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._restart_attempts: int = 0
        self._lock = asyncio.Lock()
        self._offset: int = 0  # для polling

        # таймауты/задержки
        self._connect_timeout = 10.0
        self._read_timeout = 30.0

    # ------------------- Публичные методы -------------------

    async def start(self) -> None:
        """Пытаемся запуститься с вебхуком; при неудаче — polling."""
        async with self._lock:
            if self.webhook_url:
                ok = await self._start_webhook()
                if ok:
                    return
            await self._start_polling()

    async def stop(self) -> None:
        """Останавливает текущий режим и удаляет вебхук."""
        async with self._lock:
            await self._stop_polling()
            await self._stop_webhook(delete=True)
            self._mode = "idle"

    async def heartbeat(self) -> None:
        """
        Вызывайте в главном цикле.
        Проверяет состояние и при необходимости перезапускает режим.
        """
        now = time.time()

        # Быстрый выход, если пока ничего не запущено
        if self._mode == "idle":
            await self.start()
            return

        # Если режим webhook
        if self._mode == "webhook":
            # Задача сервера умерла?
            if self._server_task and self._server_task.done():
                if self.logs:
                    print("Webhook task finished unexpectedly, restarting...")
                await self._restart_webhook_or_fallback()
                return

            # Долго нет апдейтов — возможно, связь оборвалась
            if self._last_update_ts and now - self._last_update_ts > self.alive_timeout:
                if self.logs:
                    print("Webhook stale, re-registering...")
                await self._restart_webhook_or_fallback()
                return

        # Если режим polling
        if self._mode == "polling":
            if self._poll_task and self._poll_task.done():
                if self.logs:
                    print("Polling task finished unexpectedly, restarting polling...")
                await self._start_polling()
                return

            # Периодически пробуем вернуться на webhook, если он был настроен
            if self.webhook_url and (now - self._last_update_ts > self.alive_timeout):
                if self.logs:
                    print("Trying to switch back to webhook...")
                ok = await self._start_webhook()
                if ok:
                    return

    def is_alive(self) -> bool:
        """Простой health-check: получали ли мы апдейты недавно."""
        if not self._last_update_ts:
            return False
        return (time.time() - self._last_update_ts) <= self.alive_timeout

    # ------------------- Внутренняя логика -------------------

    async def _restart_webhook_or_fallback(self) -> None:
        self._restart_attempts += 1
        await self._stop_webhook(delete=False)
        ok = await self._start_webhook()
        if ok:
            self._restart_attempts = 0
            return
        if self._restart_attempts >= self.max_retries_before_fallback:
            if self.logs:
                print("Webhook restart limit reached. Falling back to polling.")
            await self._start_polling()
            self._restart_attempts = 0

    async def _start_webhook(self) -> bool:
        try:
            # 1) Поднимаем aiohttp сервер
            await self._create_server()

            # 2) Регистрируем webhook в Telegram
            request = HTTPXRequest(connect_timeout=self._connect_timeout, read_timeout=self._read_timeout)
            bot = tel.Bot(token=self.api_token, request=request)

            # Важно: удалить прежний вебхук и дропнуть «хвосты»
            await bot.delete_webhook(drop_pending_updates=True)

            await bot.set_webhook(
                url=self.webhook_url,
                secret_token=self.secret_token,
                allowed_updates=["message"],
                drop_pending_updates=True,
                # certificate=<InputFile>  # при использовании self-signed, см. доки
            )

            self._mode = "webhook"
            if self.logs:
                print(f"Webhook started on {self.listen_host}:{self.listen_port}{self.path} -> {self.webhook_url}")
            return True
        except Exception as e:
            if self.logs:
                print(f"Failed to start webhook: {e}")
                print(traceback.format_exc())
            await self._stop_webhook(delete=False)
            self._mode = "idle"
            return False

    async def _stop_webhook(self, delete: bool) -> None:
        # Останавливаем HTTP‑сервер
        try:
            if self._site:
                await self._site.stop()
            if self._runner:
                await self._runner.cleanup()
        finally:
            self._server_app = None
            self._runner = None
            self._site = None

        # Отключаем webhook на стороне Telegram (по необходимости)
        if delete:
            try:
                request = HTTPXRequest(connect_timeout=self._connect_timeout, read_timeout=self._read_timeout)
                bot = tel.Bot(token=self.api_token, request=request)
                await bot.delete_webhook(drop_pending_updates=True)
            except Exception as e:
                if self.logs:
                    print(f"Failed to delete webhook: {e}")

        # Останавливаем задачу
        if self._server_task:
            self._server_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._server_task
        self._server_task = None

    async def _create_server(self) -> None:
        # Хэндлер
        async def handle_update(request: web.Request) -> web.StreamResponse:
            try:
                # Защита по секрету
                token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
                if token != self.secret_token:
                    raise web.HTTPForbidden(reason="Bad secret token")

                data = await request.json()

                # фиксируем активность
                self._last_update_ts = time.time()

                message = data.get("message")
                if not message:
                    return web.json_response({"ok": True})

                chat = message.get("chat", {})
                text = message.get("text")
                chat_id = str(chat.get("id"))

                if not text or chat_id != self.chat_id:
                    return web.json_response({"ok": True})

                # Исполняем команду
                try:
                    await self.commander.exec_command(text)
                except Exception as e:
                    print(f"Error executing command: {e}")
                    print(traceback.format_exc())

                return web.json_response({"ok": True})
            except web.HTTPException:
                raise
            except Exception as e:
                print(f"Webhook handler error: {e}")
                print(traceback.format_exc())
                # Даже при ошибках отвечаем 200, чтобы TG не ретраил бесконечно одно и то же
                return web.json_response({"ok": True})

        self._server_app = web.Application()
        self._server_app.router.add_post(self.path, handle_update)
        self._runner = web.AppRunner(self._server_app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=self.listen_host, port=self.listen_port)
        await self._site.start()

        # Держим задачу живой (для контроля .done())
        async def server_keepalive():
            while True:
                await asyncio.sleep(3600)

        self._server_task = asyncio.create_task(server_keepalive(), name="telegram-webhook-keepalive")

    async def _start_polling(self) -> None:
        await self._stop_polling()  # на всякий случай

        async def poll_loop():
            request = HTTPXRequest(connect_timeout=self._connect_timeout, read_timeout=self._read_timeout)
            bot = tel.Bot(token=self.api_token, request=request)

            retries = 0
            while True:
                try:
                    updates = await bot.get_updates(timeout=10, offset=self._offset, allowed_updates=["message"])
                    if not updates:
                        # без апдейтов — это ок
                        continue

                    for upd in updates:
                        self._offset = max(self._offset, upd.update_id + 1)

                        message = getattr(upd, "message", None)
                        if not message:
                            continue

                        text = getattr(message, "text", None)
                        chat_id = str(getattr(message.chat, "id", ""))
                        if not text or chat_id != self.chat_id:
                            continue

                        self._last_update_ts = time.time()
                        try:
                            await self.commander.exec_command(text)
                        except Exception as e:
                            print(f"Error executing command: {e}")
                            print(traceback.format_exc())

                    retries = 0  # успешный цикл — сбрасываем счётчик

                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                except (NetworkError, TimedOut) as e:
                    retries += 1
                    wait_time = min(2 ** retries, 30)
                    print(f"Polling network error: {e}. Retrying in {wait_time}s... ({retries})")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    print(f"Unexpected polling error: {e}")
                    print(traceback.format_exc())
                    retries += 1
                    await asyncio.sleep(min(2 ** retries, 30))

        self._poll_task = asyncio.create_task(poll_loop(), name="telegram-polling")
        self._mode = "polling"
        if self.logs:
            print("Polling started")

    async def _stop_polling(self) -> None:
        if self._poll_task:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
        self._poll_task = None


# ---------------------------------------------------------------------------
# Инициализация/адаптер для существующего главного цикла
# ---------------------------------------------------------------------------

import contextlib

_monitor: Optional[TelegramMonitor] = None


async def setup_telegram_monitor() -> None:
    """
    Вызывайте один раз при старте приложения.
    """
    global _monitor

    api_token = config("COLLECTOR_API")
    chat_id = tel.config("CHAT_ID")

    # Параметры вебхука. Если TELEGRAM_WEBHOOK_URL не задан, автоматически запустится polling.
    webhook_url = config("TELEGRAM_WEBHOOK_URL", default=None)
    secret_token = config("TELEGRAM_WEBHOOK_SECRET", default=None)
    listen_host = config("TELEGRAM_WEBHOOK_HOST", default="0.0.0.0")
    listen_port = int(config("TELEGRAM_WEBHOOK_PORT", default=8081))
    path = config("TELEGRAM_WEBHOOK_PATH", default="/telegram")

    _monitor = TelegramMonitor(
        api_token=api_token,
        commander=sv.commander,
        chat_id=str(chat_id),
        webhook_url=webhook_url,
        secret_token=secret_token,
        listen_host=listen_host,
        listen_port=listen_port,
        path=path,
        alive_timeout=float(config("TELEGRAM_ALIVE_TIMEOUT", default=120.0)),
        max_retries_before_fallback=int(config("TELEGRAM_MAX_RETRIES", default=3)),
        logs=True,
    )
    await _monitor.start()


async def check_and_handle_message():
    """
    СУЩЕСТВУЮЩИЙ ИНТЕРФЕЙС ДЛЯ ВАШЕГО ГЛАВНОГО ЦИКЛА.
    Теперь просто дергает heartbeat(), который сам следит за живостью и перезапуском.
    """
    global _monitor
    if _monitor is None:
        # Ленивый старт, если забыли вызвать setup_telegram_monitor()
        await setup_telegram_monitor()
    try:
        await _monitor.heartbeat()
    except Exception as e:
        print(f"Critical error in heartbeat: {e}")
        print(traceback.format_exc())


# Удобный метод для ручной проверки в любом месте
def telegram_is_alive() -> bool:
    global _monitor
    return _monitor.is_alive() if _monitor else False
