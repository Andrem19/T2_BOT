# process_utils.py
# Запуск main.py БЕЗ аргументов + PID, проверка живости, завершение.
# Зависимости: стандартная библиотека. (Опционально: psutil)

from __future__ import annotations

import os
import platform
import sys
import signal
import subprocess
from pathlib import Path
from typing import Optional
from database.commands_tab import Commands

try:
    import psutil  # type: ignore
except Exception:
    psutil = None  # type: ignore


def _python_command() -> list[str]:
    exe = os.environ.get("PYTHON_EXECUTABLE")
    if exe:
        return [exe, "-u"]
    system = platform.system()
    if system in ("Linux", "Darwin"):
        return ["python3", "-u"]
    return ["python", "-u"]


def start_main(
    project_root: Path | str = ".",
    main_script: str = "main.py",
    stdout_mode: str = "inherit",  # 'inherit' | 'devnull' | 'file'
    log_file: Optional[Path | str] = None,
    append_log: bool = False,
) -> int:
    """
    Запускает main.py БЕЗ аргументов тем же интерпретатором, что и manager.py
    (sys.executable -> то же conda-окружение). Возвращает PID процесса.

    stdout_mode:
      - 'inherit' (по умолчанию): stdout/stderr наследуются от родителя.
      - 'devnull': весь stdout/stderr глушится.
      - 'file': stdout/stderr пишутся в файл log_file (перехватывает print()
        и StreamHandler(sys.stdout); ваши файловые хендлеры logging продолжают
        писать в свои файлы независимо).

    ВНИМАНИЕ: если у вас уже настроен файловый логгер, обычно используйте
    'inherit' или 'devnull', чтобы избежать дублей.
    """
    project_root = Path(project_root).resolve()
    script_path = project_root / main_script
    if not script_path.exists():
        raise FileNotFoundError(f"Не найден скрипт: {script_path}")

    # ВАЖНО: тот же интерпретатор, что у manager.py → то же conda-окружение
    python_exe = sys.executable  # напр., /home/ubuntu/miniconda3/envs/env6/bin/python
    cmd = [python_exe, "-u", str(script_path)]

    # Настройка вывода
    stdout = None
    stderr = None
    log_fp = None

    if stdout_mode == "devnull":
        stdout = subprocess.DEVNULL
        stderr = subprocess.STDOUT
    elif stdout_mode == "file":
        if log_file is None:
            log_file = "output.log"
        log_path = Path(log_file).resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if append_log else "w"
        # Текстовый режим, line-buffered
        log_fp = open(log_path, mode, buffering=1, encoding="utf-8")
        stdout = log_fp
        stderr = subprocess.STDOUT
    elif stdout_mode == "inherit":
        stdout = None
        stderr = None
    else:
        raise ValueError("stdout_mode must be 'inherit', 'devnull', or 'file'")

    # Параметры отделения процесса от текущей группы (на *nix)
    system = platform.system()
    start_new_session = system != "Windows"
    creationflags = 0
    if system == "Windows":
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        DETACHED_PROCESS = 0x00000008
        creationflags = CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(project_root),
            stdout=stdout,
            stderr=stderr,
            text=True,         # влияет только на PIPE; для файла/наследования безвредно
            bufsize=1,         # line-buffered для PIPE/текста
            start_new_session=start_new_session,  # вместо preexec_fn=os.setsid
            creationflags=creationflags,
        )
        return proc.pid
    finally:
        # Безопасно закрываем наш файловый объект: у дочернего процесса уже свой дескриптор
        if log_fp is not None:
            try:
                log_fp.close()
            except Exception:
                pass


def is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False

    if psutil is not None:
        try:
            p = psutil.Process(pid)
            return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False
        except Exception:
            pass

    system = platform.system()
    if system == "Windows":
        try:
            import ctypes
            from ctypes import wintypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            OpenProcess = kernel32.OpenProcess
            OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            OpenProcess.restype = wintypes.HANDLE
            CloseHandle = kernel32.CloseHandle
            CloseHandle.argtypes = [wintypes.HANDLE]
            CloseHandle.restype = wintypes.BOOL

            handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not handle:
                return False
            try:
                return True
            finally:
                CloseHandle(handle)
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        else:
            return True


def kill_process(pid: int, timeout: float = 5.0, force: bool = True) -> bool:
    if pid <= 0:
        return True

    if psutil is not None:
        try:
            p = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return True
        except Exception:
            p = None

        if p is not None:
            try:
                for ch in p.children(recursive=True):
                    try:
                        ch.terminate()
                    except psutil.NoSuchProcess:
                        pass
                p.terminate()
                psutil.wait_procs([p], timeout=timeout)
            except Exception:
                pass

            if is_process_alive(pid) and force:
                try:
                    for ch in p.children(recursive=True):
                        try:
                            ch.kill()
                        except psutil.NoSuchProcess:
                            pass
                    p.kill()
                except Exception:
                    pass

            return not is_process_alive(pid)

    system = platform.system()
    if system == "Windows":
        try:
            import ctypes
            from ctypes import wintypes

            PROCESS_TERMINATE = 0x0001
            SYNCHRONIZE = 0x00100000

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            OpenProcess = kernel32.OpenProcess
            OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            OpenProcess.restype = wintypes.HANDLE

            TerminateProcess = kernel32.TerminateProcess
            TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
            TerminateProcess.restype = wintypes.BOOL

            WaitForSingleObject = kernel32.WaitForSingleObject
            WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            WaitForSingleObject.restype = wintypes.DWORD

            CloseHandle = kernel32.CloseHandle
            CloseHandle.argtypes = [wintypes.HANDLE]
            CloseHandle.restype = wintypes.BOOL

            handle = OpenProcess(PROCESS_TERMINATE | SYNCHRONIZE, False, pid)
            if not handle:
                return True
            try:
                TerminateProcess(handle, 1)
                WaitForSingleObject(handle, int(timeout * 1000))
            finally:
                CloseHandle(handle)
            return not is_process_alive(pid)
        except Exception:
            return not is_process_alive(pid)
    else:
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            return True
        except PermissionError:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass

        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not is_process_alive(pid):
                return True
            time.sleep(0.1)

        if force and is_process_alive(pid):
            try:
                os.killpg(pid, signal.SIGKILL)
            except Exception:
                try:
                    os.kill(pid, signal.SIGKILL)
                except Exception:
                    pass

        return not is_process_alive(pid)



def announce_self_pid(
    write_to_file: Optional[str | Path] = None,
    print_group_info: bool = True,
) -> int:
    """
    Печатает PID текущего процесса. По желанию:
    - сохраняет PID в файл (write_to_file),
    - дублирует сообщение в лог (also_log),
    - на POSIX выводит PGID и SID (print_group_info).

    Возвращает сам PID.
    """
    pid = os.getpid()
    ppid = os.getppid()
    Commands.set_man_pid(pid)

    system = platform.system()
    pgid = None
    sid = None

    if print_group_info and system != "Windows":
        try:
            pgid = os.getpgid(0)
        except Exception:
            pgid = None
        try:
            sid = os.getsid(0)
        except Exception:
            sid = None

    # Формируем человекочитаемое сообщение
    parts = [f"PID={pid}", f"PPID={ppid}", f"OS={system}"]
    if pgid is not None:
        parts.append(f"PGID={pgid}")
    if sid is not None:
        parts.append(f"SID={sid}")
    msg = " | ".join(parts)

    # Печать в stdout — как вы и просили
    print(msg, flush=True)

    # Запись в файл .pid (если указали путь)
    if write_to_file:
        pid_path = Path(write_to_file)
        pid_path.parent.mkdir(parents=True, exist_ok=True)
        pid_path.write_text(str(pid), encoding="utf-8")

    return pid

