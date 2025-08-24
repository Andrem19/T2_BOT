# hourly_scheduler.py
# Планировщик: каждый час в :57 запускает асинхронную джобу и пишет JSON-строку в metrics.json.
# В ФАЙЛ ЗАПИСЫВАЕТСЯ ТОЛЬКО РЕЗУЛЬТАТ task_two + поле time_utc (UTC ISO8601 с 'Z').
# Устойчив к долгим задачам и ошибкам. Основной поток не блокируется.

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from metrics.feature_synergy import analyze_feature_synergies
import uuid
from metrics.load_metrics import load_compact_metrics
from metrics.serv import map_time_to_score, get_rr25_iv
import helpers.tools as tools
import helpers.tlg as tel
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from concurrent.futures import Future, TimeoutError as FutTimeoutError

from metrics.indicators import MarketIntel
from metrics.score import score_snapshot
from metrics.news_metric import news_metric

# ---------------------------- ЛОГИРОВАНИЕ ------------------------------------
_logger = logging.getLogger("hourly_57_scheduler")
if not _logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# ----------------------- ВСПОМОГАТЕЛЬНЫЕ СТРУКТУРЫ ---------------------------
@dataclass
class RunReport:
    run_id: str
    ts_local: str
    ts_utc: str
    hour_started_local: str
    hour_ending_local: str
    elapsed_sec: float
    status: str                 # "ok" | "error"
    error: Optional[str] = None # текст ошибки (если была)

# ------------------------------- УТИЛИТЫ/ПУТИ --------------------------------
def _resolve_project_root() -> Path:
    """
    Определяем корень проекта:
      1) $PROJECT_ROOT, если задан,
      2) поднимаемся вверх от текущего файла в поисках .git/pyproject.toml/requirements.txt,
      3) иначе — текущая рабочая директория.
    """
    env = os.getenv("PROJECT_ROOT")
    if env:
        return Path(env).expanduser().resolve()

    here = Path(__file__).resolve().parent
    markers = {".git", "pyproject.toml", "requirements.txt"}
    for p in [here, *here.parents]:
        if any((p / m).exists() for m in markers):
            return p
    return Path.cwd().resolve()

def _round_to_nearest_hour_utc(dt: datetime) -> datetime:
    """
    Округление к ближайшему часу по UTC.
    Правило: если минут < 30 — вниз, если минут ≥ 30 — вверх.
    Примеры:
      11:59:41Z -> 12:00:00Z
      12:05:00Z -> 12:00:00Z
      12:30:00Z -> 13:00:00Z
    """
    if dt.tzinfo is None:
        # Если пришёл наивный datetime — считаем, что он в UTC
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        # Нормализуем в UTC на случай другого часового пояса
        dt = dt.astimezone(timezone.utc)

    base = dt.replace(minute=0, second=0, microsecond=0)
    half_hour = base + timedelta(minutes=30)
    return base if dt < half_hour else base + timedelta(hours=1)


def _append_json_line(file_path: Path, payload: Dict[str, Any]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    with file_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def _next_trigger_57(now: datetime) -> datetime:
    """
    Ближайшее локальное время с минутой = 57 и секундами = 0.
    Если уже прошли :57 текущего часа — берём следующий час.
    """
    target = now.replace(minute=58, second=0, microsecond=0)
    if now >= target:
        target = (target + timedelta(hours=1)).replace(minute=58, second=0, microsecond=0)
    return target

# ------------------------------ РЕАЛЬНЫЕ ЗАДАЧИ ------------------------------
# Оставлены как пример. Реальные вызовы внутри можно делать через await.
# Если используете синхронные библиотеки — оборачивайте в asyncio.to_thread.

async def task_one() -> Dict[str, Any]:
    """
    Пример: снимок рыночных метрик. Синхронный вызов завернут в to_thread, чтобы не блокировать loop.
    """
    def _sync_snapshot() -> Dict[str, Any]:
        mi = MarketIntel()
        return mi.snapshot(symbol="BTCUSDT", lookback_hours=2.0)

    snap = await asyncio.to_thread(_sync_snapshot)
    # Для отладки (можно заменить на _logger.debug):
    print(json.dumps(snap, ensure_ascii=False, indent=2))
    return snap

async def task_two(snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    Пример: вычисление скоринга. Синхронный вызов — в to_thread.
    """
    def _sync_score() -> Dict[str, Any]:
        return score_snapshot(snap)

    scored = await asyncio.to_thread(_sync_score)
    print(json.dumps(scored, ensure_ascii=False, indent=2))
    return scored

# ------------------------------ КОНФИГУРАЦИЯ ---------------------------------
class SchedulerConfig:
    def __init__(
        self,
        metrics_filename: str = "metrics.json",
        allow_overlap: bool = False,     # если True — допускаем параллельные запуски по часам
        warn_after_sec: int = 180,       # предупредить в лог, если выполнение > N сек
    ) -> None:
        self.project_root = _resolve_project_root()
        self.metrics_path = (self.project_root / metrics_filename).resolve()
        self.allow_overlap = allow_overlap
        self.warn_after_sec = max(1, int(warn_after_sec))

# ------------------------------ СКЕДУЛЕР (ASYNC) -----------------------------
class HourlyAt57Scheduler:
    """
    Архитектура:
      - Поток А: "scheduler" — спит до :57 и по тику ставит задачу в event loop.
      - Поток B: "asyncio-loop" — крутит asyncio loop, исполняет корутины.
      - Основной поток приложения не блокируется.

    Перекрытия:
      - allow_overlap=False: если предыдущая задача ещё идёт — текущий тик пропускаем (в файл НЕ пишем).
    """
    def __init__(self, config: Optional[SchedulerConfig] = None) -> None:
        self.cfg = config or SchedulerConfig()
        self._stop_event = threading.Event()

        # Поток планировщика (:57)
        self._sched_thread: Optional[threading.Thread] = None
        # Поток с asyncio loop
        self._loop_thread: Optional[threading.Thread] = None
        self._loop_ready = threading.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Для контроля перекрытий
        self._last_future: Optional[Future] = None
        self._lock = threading.Lock()

    # ----------- Публичное API -----------
    def start(self) -> None:
        if (self._sched_thread and self._sched_thread.is_alive()) or (self._loop_thread and self._loop_thread.is_alive()):
            _logger.warning("Планировщик уже запущен; повторный запуск проигнорирован.")
            return

        # Запуск event loop в отдельном daemon-потоке
        self._loop_thread = threading.Thread(target=self._loop_worker, name="hourly_asyncio_loop", daemon=True)
        self._loop_thread.start()
        # Ждём готовности loop
        self._loop_ready.wait(timeout=5.0)
        if not self._loop:
            raise RuntimeError("Не удалось запустить asyncio event loop.")

        # Запуск потока планировщика
        self._stop_event.clear()
        self._sched_thread = threading.Thread(target=self._runner, name="hourly_57_scheduler", daemon=True)
        self._sched_thread.start()

        _logger.info("Планировщик запущен. Файл метрик: %s. Триггер: каждый час в :57 (локальное время).",
                     self.cfg.metrics_path)

    def stop(self, join_timeout: Optional[float] = 3.0) -> None:
        """
        Мягкая остановка:
          1) останавливаем расписание (:57),
          2) ждём немного текущую задачу,
          3) если задачи нет/завершилась — останавливаем loop;
             иначе оставляем loop работать в daemon-потоке, чтобы не прервать задачу.
        """
        self._stop_event.set()
        if self._sched_thread and self._sched_thread.is_alive():
            self._sched_thread.join(timeout=join_timeout)

        # Если есть активная джоба — подождём немного её завершения
        if self._last_future and not self._last_future.done():
            try:
                self._last_future.result(timeout=max(0.1, float(join_timeout or 0)))
            except FutTimeoutError:
                _logger.warning(
                    "Задача ещё выполняется; оставляем event loop работать в daemon-потоке. "
                    "Процесс завершится — поток завершится вместе с ним."
                )
                return  # не трогаем loop

        # Без активных задач — можно останавливать loop
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=join_timeout)
        _logger.info("Планировщик остановлен.")

    # ----------- Поток с asyncio loop -----------
    def _loop_worker(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._loop_ready.set()
        try:
            loop.run_forever()
        except Exception:
            _logger.exception("Необработанная ошибка в asyncio loop.")
        finally:
            try:
                # Акуратно закрываем loop
                pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                for t in pending:
                    t.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            finally:
                loop.close()

    # ----------- Поток планировщика (:57) -----------
    def _runner(self) -> None:
        try:
            while not self._stop_event.is_set():
                now = datetime.now()
                target = _next_trigger_57(now)
                sleep_sec = max(0.0, (target - now).total_seconds())

                _logger.debug("До следующего запуска %.3f сек, целевое время %s", sleep_sec, target)
                if self._stop_event.wait(timeout=sleep_sec):
                    break  # мягкая остановка

                # Сработал часовой триггер
                self._on_tick()
        except Exception:
            _logger.exception("Необработанная ошибка в теле планировщика (:57).")
        finally:
            _logger.info("Выход из потока планировщика.")

    def _on_tick(self) -> None:
        """
        Обработка одного тика (:57). Ставит выполнение асинхронной работы в event loop.
        Контролирует перекрытия запусков по часам.
        """
        with self._lock:
            if (not self.cfg.allow_overlap) and self._last_future and not self._last_future.done():
                _logger.warning("Пропуск запуска в :57 из-за перекрытия (allow_overlap=False).")
                return

            # Ставим корутину в event loop
            fut = asyncio.run_coroutine_threadsafe(self._run_job_once_safely(), self._loop)
            self._last_future = fut

    # ------------------------- ОСНОВНАЯ АСИНХРОННАЯ РАБОТА --------------------
    async def _run_job_once_safely(self) -> RunReport:
        """
        Асинхронная джоба одного часа:
          - последовательно выполняет task_one() и task_two(),
          - любые исключения ловятся и логируются,
          - в файл metrics.json пишется ТОЛЬКО результат task_two + time_utc (UTC).
        """
        started_local = datetime.now()
        hour_started = started_local.replace(minute=0, second=0, microsecond=0)
        hour_ending = hour_started + timedelta(hours=1)
        run_id = uuid.uuid4().hex

        status = "ok"
        err_text: Optional[str] = None

        # Сторожок: однократное предупреждение, если выполнение затянулось
        async def _watchdog() -> None:
            try:
                await asyncio.sleep(self.cfg.warn_after_sec)
                _logger.warning(
                    "Долгое выполнение hourly job run_id=%s: > %d сек.",
                    run_id, self.cfg.warn_after_sec
                )
            except asyncio.CancelledError:
                # нормальное завершение сторожа
                return

        wd_task = asyncio.create_task(_watchdog(), name=f"hourly_watchdog_{run_id}")

        # Основная логика
        try:
            _logger.info("Запуск hourly job run_id=%s на %s", run_id, started_local)

            news_score = await news_metric()
            rr25, iv = await get_rr25_iv()
            # 1) task_one (await)
            t1_res: Dict[str, Any] = await task_one()

            # 2) task_two (await)
            t2_res: Dict[str, Any] = await task_two(t1_res)
            
            payload: Dict[str, Any] = dict(t2_res) if isinstance(t2_res, dict) else {"result": t2_res}
            date = datetime.now(timezone.utc)
            rounded = _round_to_nearest_hour_utc(date)
            new_d = rounded.strftime('%Y-%m-%dT%H:%M:%SZ')
            payload["time_utc"] = new_d
            payload["per_metric"]["news_score"] = news_score or {'score': 0}
            payload["per_metric"]["rr25"] = {'score': rr25} or {'score': 0}
            payload["per_metric"]["iv"] = {'score': iv} or {'score': 0}

            minify_dict = map_time_to_score([payload])

            # 3) Пишем в файл ТОЛЬКО результат task_two + time_utc (UTC)
            try:
                _append_json_line(self.cfg.metrics_path, payload)
                sample = load_compact_metrics('metrics.json')
                res = analyze_feature_synergies(sample, symbol="BTCUSDT", market="um",
                                            bins=2, min_support=8, k_max=3, topn=10)
                score = res['latest_score']
                ts_ms = res['latest_open_time']
                latest_matched_rules = res['latest_matched_rules']
                ts_utc = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
                minify_dict['fin_score'] = f'{ts_utc}: {round(score, 4)}'
                minify_dict['latest_matched_rules'] = latest_matched_rules
                pretty_str = tools.dict_to_pretty_string(minify_dict)
                await tel.send_inform_message("COLLECTOR_API", f"{pretty_str}", "", False)
                _logger.info("Метрика записана (%s). Только task_two + time_utc.", self.cfg.metrics_path)
            except Exception as write_exc:
                status = "error"
                err_text = f"WriteError: {type(write_exc).__name__}: {write_exc}"
                _logger.exception("Не удалось записать метрику в %s", self.cfg.metrics_path)

        except Exception as exc:
            status = "error"
            err_text = f"{type(exc).__name__}: {exc}"
            _logger.exception("Ошибка в hourly job run_id=%s", run_id)
        finally:
            # Останавливаем сторожок
            wd_task.cancel()
            try:
                await wd_task
            except Exception:
                pass

        elapsed = (datetime.now() - started_local).total_seconds()
        return RunReport(
            run_id=run_id,
            ts_local=started_local.isoformat(timespec="seconds"),
            ts_utc=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            hour_started_local=hour_started.isoformat(timespec="seconds"),
            hour_ending_local=hour_ending.isoformat(timespec="seconds"),
            elapsed_sec=elapsed,
            status=status,
            error=err_text,
        )

# ------------------------------ УДОБНЫЕ ОБЁРТКИ ------------------------------
_scheduler: Optional[HourlyAt57Scheduler] = None

def start_hourly_57_scheduler(
    metrics_filename: str = "metrics.json",
    allow_overlap: bool = False,
    warn_after_sec: int = 180,
    # параметр worker_max_workers больше не используется (async-версия), оставлен для совместимости
    worker_max_workers: int = 1,  # noqa: ARG002  (сохранён для обратной совместимости)
) -> None:
    """
    Упрощённый запуск планировщика.
    - metrics_filename: имя файла метрик в корне проекта (JSONL: по одной строке на час)
    - allow_overlap: разрешить ли перекрытие запусков, если предыдущий ещё идёт
    - warn_after_sec: через сколько секунд выполнения вывести предупреждение
    """
    global _scheduler
    if _scheduler:
        _logger.warning("Планировщик уже запущен; повторный запуск проигнорирован.")
        return
    cfg = SchedulerConfig(
        metrics_filename=metrics_filename,
        allow_overlap=allow_overlap,
        warn_after_sec=warn_after_sec,
    )
    _scheduler = HourlyAt57Scheduler(cfg)
    _scheduler.start()

def stop_hourly_57_scheduler(join_timeout: Optional[float] = 3.0) -> None:
    global _scheduler
    if _scheduler:
        _scheduler.stop(join_timeout=join_timeout)
        _scheduler = None

