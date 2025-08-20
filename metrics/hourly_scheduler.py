# hourly_scheduler.py
# Планировщик: каждый час в :57 ставит запуск двух задач и пишет JSON-строку в metrics.json.
# В ФАЙЛ ЗАПИСЫВАЕТСЯ ТОЛЬКО РЕЗУЛЬТАТ task_two + поле time_utc. Устойчив к долгим задачам и ошибкам.

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, Future

from helpers.safe_sender import safe_send
from metrics.indicators import MarketIntel
from metrics.score import score_snapshot

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
    task_one: Optional[Dict[str, Any]]
    task_two: Optional[Dict[str, Any]]
    status: str                 # "ok" | "error" | "skipped_overlap"
    error: Optional[str] = None # Текст ошибки, если был сбой

# ------------------------------- ПУТИ/ФАЙЛ -----------------------------------
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

def _append_json_line(file_path: Path, payload: Dict[str, Any]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    with file_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

# ---------------------------- ВЫЧИСЛЕНИЕ ТРИГГЕРА ----------------------------
def _next_trigger_57(now: datetime) -> datetime:
    """
    Ближайшее локальное время с минутой = 57 и секундами = 0.
    Если уже прошли :57 текущего часа — берём следующий час.
    """
    target = now.replace(minute=55, second=0, microsecond=0)
    if now >= target:
        target = (target + timedelta(hours=1)).replace(minute=55, second=0, microsecond=0)
    return target

# ------------------------------ РЕАЛЬНЫЕ ЗАДАЧИ ------------------------------
def task_one() -> Dict[str, Any]:
    mi = MarketIntel()
    snap = mi.snapshot(symbol="BTCUSDT", lookback_hours=2.0)
    print(json.dumps(snap, ensure_ascii=False, indent=2))
    return snap

def task_two(snap: Dict[str, Any]) -> Dict[str, Any]:
    scored = score_snapshot(snap)
    print(json.dumps(scored, ensure_ascii=False, indent=2))
    return scored

# ------------------------------ КОНФИГУРАЦИЯ ---------------------------------
class SchedulerConfig:
    def __init__(
        self,
        metrics_filename: str = "metrics.json",
        allow_overlap: bool = False,     # если True — допускаем параллельные часы
        warn_after_sec: int = 180,       # предупредить в лог, если выполнение > N сек
        worker_max_workers: int = 1,     # пул потоков для задач (1 → без перекрытий внутри пула)
    ) -> None:
        self.project_root = _resolve_project_root()
        self.metrics_path = (self.project_root / metrics_filename).resolve()
        self.allow_overlap = allow_overlap
        self.warn_after_sec = max(1, int(warn_after_sec))
        self.worker_max_workers = max(1, int(worker_max_workers))

# ------------------------------ САМОТЕЛО СКЕДУЛЕРА ---------------------------
class HourlyAt57Scheduler:
    def __init__(self, config: Optional[SchedulerConfig] = None) -> None:
        self.cfg = config or SchedulerConfig()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._executor = ThreadPoolExecutor(
            max_workers=self.cfg.worker_max_workers,
            thread_name_prefix="hourly_worker"
        )
        self._last_future: Optional[Future] = None
        self._lock = threading.Lock()  # защита от гонок при постановке задач

    # ----------- Публичное API -----------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            _logger.warning("Планировщик уже запущен; повторный запуск проигнорирован.")
            return
        self._stop_event.clear()
        _logger.info("Файл метрик: %s", self.cfg.metrics_path)
        self._thread = threading.Thread(target=self._runner, name="hourly_57_scheduler", daemon=True)
        self._thread.start()
        _logger.info("Планировщик запущен. Триггер: каждый час в :57 (локальное время).")

    def stop(self, join_timeout: Optional[float] = 3.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=join_timeout)
        # мягко глушим executor (не прерывает уже выполняющиеся задачи)
        self._executor.shutdown(wait=False, cancel_futures=False)
        _logger.info("Планировщик остановлен.")

    # ----------- Внутреннее -----------
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
            _logger.exception("Необработанная ошибка в теле планировщика; он будет остановлен во избежание спама.")
        finally:
            _logger.info("Выход из потока планировщика.")

    def _on_tick(self) -> None:
        """
        Обработка одного тика (:57). Ставит выполнение задач в пул.
        Контролирует перекрытия запусков по часам.
        """
        with self._lock:
            if not self.cfg.allow_overlap and self._last_future and not self._last_future.done():
                # Предыдущая часовая задача ещё идёт: пропускаем текущую (в файл НЕ пишем)
                _logger.warning("Пропуск запуска в :57 из-за перекрытия (allow_overlap=False).")
                return

            # Ставим новую работу в пул
            future = self._executor.submit(self._run_job_once_safely)
            # Подвешиваем коллбек на запись метрики по завершении
            future.add_done_callback(self._on_job_done)
            self._last_future = future

    def _run_job_once_safely(self) -> RunReport:
        """
        Исполняет две пользовательские задачи последовательно внутри рабочего потока.
        Любая ошибка — в статус/лог; возврат RunReport.
        """
        started_local = datetime.now()
        hour_started = started_local.replace(minute=0, second=0, microsecond=0)
        hour_ending = hour_started + timedelta(hours=1)
        run_id = uuid.uuid4().hex

        t1_res: Optional[Dict[str, Any]] = None
        t2_res: Optional[Dict[str, Any]] = None
        status = "ok"
        err_text: Optional[str] = None

        # Сторожок «долгого» выполнения (только предупреждение; ничего не прерывает)
        def _watchdog(start_t: float) -> None:
            while True:
                time.sleep(1.0)
                elapsed = time.time() - start_t
                if elapsed > self.cfg.warn_after_sec:
                    _logger.warning(
                        "Долгое выполнение hourly job run_id=%s: %.0f сек (порог %d сек).",
                        run_id, elapsed, self.cfg.warn_after_sec
                    )
                    return
                if getattr(_wd_state, "done", False):
                    return

        class _wd_state:
            done = False

        wd_thread = threading.Thread(
            target=_watchdog,
            args=(time.time(),),
            name=f"hourly_watchdog_{run_id}",
            daemon=True
        )
        wd_thread.start()

        try:
            _logger.info("Запуск hourly job run_id=%s на %s", run_id, started_local)
            t1_res = task_one() or {}
            t2_res = task_two(t1_res) or {}
        except Exception as exc:
            status = "error"
            err_text = f"{type(exc).__name__}: {exc}"
            _logger.exception("Ошибка в hourly job run_id=%s", run_id)
        finally:
            _wd_state.done = True

        elapsed = (datetime.now() - started_local).total_seconds()
        report = RunReport(
            run_id=run_id,
            ts_local=started_local.isoformat(timespec="seconds"),
            ts_utc=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            hour_started_local=hour_started.isoformat(timespec="seconds"),
            hour_ending_local=hour_ending.isoformat(timespec="seconds"),
            elapsed_sec=elapsed,
            task_one=t1_res,
            task_two=t2_res,
            status=status,
            error=err_text,
        )
        return report

    def _on_job_done(self, fut: Future) -> None:
        """
        Коллбек по завершении работы: в файл пишем ТОЛЬКО результат task_two + time_utc.
        Коллбек сам по себе «пулезащищён»: не валит процесс при сбое.
        """
        try:
            report = fut.result()
            # Берём только результат task_two.
            if isinstance(report.task_two, dict):
                payload: Dict[str, Any] = dict(report.task_two)  # копия
            else:
                # На случай, если вернули не словарь — аккуратно упакуем.
                payload = {"result": report.task_two}

            # Добавляем UTC-метку сохранения (требование пользователя).
            payload["time_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

            _append_json_line(self.cfg.metrics_path, payload)
            _logger.info(
                "Метрика записана (%s). Только task_two + time_utc. elapsed=%.1f сек, статус=%s",
                self.cfg.metrics_path, report.elapsed_sec, report.status
            )
        except Exception as exc:
            _logger.exception(
                "Коллбек завершения hourly job: не удалось записать метрику (%s): %s",
                self.cfg.metrics_path, exc
            )

# ------------------------------ УДОБНЫЕ ОБЁРТКИ ------------------------------
_scheduler: Optional[HourlyAt57Scheduler] = None

def start_hourly_57_scheduler(
    metrics_filename: str = "metrics.json",
    allow_overlap: bool = False,
    warn_after_sec: int = 180,
    worker_max_workers: int = 1,
) -> None:
    """
    Упрощённый запуск планировщика.
    - metrics_filename: имя файла метрик в корне проекта (JSONL: по одной строке на час)
    - allow_overlap: разрешить ли перекрытие запусков, если предыдущий ещё идёт
    - warn_after_sec: через сколько секунд выполнения вывести предупреждение
    - worker_max_workers: размер пула потоков задач (1 — без перекрытий внутри пула)
    """
    global _scheduler
    if _scheduler:
        _logger.warning("Планировщик уже запущен; повторный запуск проигнорирован.")
        return
    cfg = SchedulerConfig(
        metrics_filename=metrics_filename,
        allow_overlap=allow_overlap,
        warn_after_sec=warn_after_sec,
        worker_max_workers=worker_max_workers,
    )
    _scheduler = HourlyAt57Scheduler(cfg)
    _scheduler.start()

def stop_hourly_57_scheduler(join_timeout: Optional[float] = 3.0) -> None:
    global _scheduler
    if _scheduler:
        _scheduler.stop(join_timeout=join_timeout)
        _scheduler = None


