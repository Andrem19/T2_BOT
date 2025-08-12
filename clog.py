#!/usr/bin/env python3
# sudo systemctl restart t2-manager   # вручную, если нужно перезапустить
# sudo systemctl stop t2-manager
# sudo systemctl disable t2-manager
# sudo systemctl status t2-manager
# sudo systemctl stop sqlite-web      # остановить
# sudo systemctl start sqlite-web     # запустить
# sudo systemctl restart sqlite-web   # перезапустить
# sudo systemctl disable sqlite-web   # отключить автозапуск
import os
import shutil
import subprocess
import sys
from pathlib import Path

# ======= НАСТРОЙКИ (правь только это при необходимости) =======
HOST = "54.254.56.129"
USER = "ubuntu"
PORT = 22
KEY_PATH = Path("/home/jupiter/.ssh/lightsail.pem")
REMOTE_DIR = Path("/home/ubuntu/T2_BOT/_logs")
# Если нужно забрать один конкретный файл из REMOTE_DIR — укажи его имя (например, "trade.log").
# Если None или пусто — скопируется весь каталог REMOTE_DIR целиком.
COPY_ONLY_FILE = None#'bot.log'  # например: "trade.log" или "subdir/errors.log"
# ===============================================================

def check_prereqs():
    from shutil import which
    if which("scp") is None:
        print("Ошибка: не найден 'scp'. Установите openssh-client (Ubuntu: sudo apt-get install -y openssh-client).", file=sys.stderr)
        sys.exit(1)

def ensure_key_permissions(key_path: Path):
    if not key_path.exists():
        print(f"Ошибка: ключ не найден: {key_path}", file=sys.stderr)
        sys.exit(1)
    try:
        os.chmod(key_path, 0o600)
    except PermissionError:
        pass

def run_scp_copy_dir(host: str, user: str, port: int, key_path: Path, remote_dir: Path, dest_parent: Path):
    local_dir = dest_parent / remote_dir.name
    if local_dir.exists():
        print(f"Удаляю локальную папку: {local_dir}")
        shutil.rmtree(local_dir)

    print(f"Копирую каталог {user}@{host}:{remote_dir} -> {dest_parent}")
    cmd = [
        "scp",
        "-r",
        "-i", str(key_path),
        "-P", str(port),
        "-o", "StrictHostKeyChecking=accept-new",
        f"{user}@{host}:{remote_dir}",
        str(dest_parent)
    ]
    try:
        subprocess.run(cmd, check=True, text=True, capture_output=False)
    except subprocess.CalledProcessError as e:
        print("Ошибка при копировании scp каталога.", file=sys.stderr)
        sys.exit(e.returncode)

    if not local_dir.exists():
        print(f"Ошибка: после копирования не найден локальный каталог: {local_dir}", file=sys.stderr)
        sys.exit(1)
    print(f"Готово. Локальная папка: {local_dir}")

def run_scp_copy_file(host: str, user: str, port: int, key_path: Path, remote_dir: Path, file_relpath: str, dest_parent: Path):
    """
    Копирует ОДИН файл из remote_dir/file_relpath в dest_parent, сохраняя имя файла.
    Поддерживает вложенные подпапки в file_relpath (например 'sub/a.log').
    """
    file_relpath = file_relpath.strip().lstrip("/")  # на случай, если случайно дадут '/trade.log'
    remote_path = f"{user}@{host}:{remote_dir}/{file_relpath}"
    dest_path = dest_parent / Path(file_relpath).name  # кладём рядом, без структуры подпапок

    # Если хочешь сохранять подпапки локально — раскомментируй следующие две строки
    # dest_path = dest_parent / file_relpath
    # dest_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Копирую файл {remote_path} -> {dest_path}")
    cmd = [
        "scp",
        "-i", str(key_path),
        "-P", str(port),
        "-o", "StrictHostKeyChecking=accept-new",
        remote_path,
        str(dest_path)
    ]
    try:
        subprocess.run(cmd, check=True, text=True, capture_output=False)
    except subprocess.CalledProcessError as e:
        print("Ошибка при копировании scp файла.", file=sys.stderr)
        sys.exit(e.returncode)

    if not dest_path.exists():
        print(f"Ошибка: после копирования не найден локальный файл: {dest_path}", file=sys.stderr)
        sys.exit(1)
    print(f"Готово. Локальный файл: {dest_path}")

def main():
    dest_parent = Path.cwd()
    key_path = KEY_PATH

    check_prereqs()
    ensure_key_permissions(key_path)

    if COPY_ONLY_FILE and str(COPY_ONLY_FILE).strip():
        run_scp_copy_file(
            host=HOST,
            user=USER,
            port=PORT,
            key_path=key_path,
            remote_dir=REMOTE_DIR,
            file_relpath=str(COPY_ONLY_FILE),
            dest_parent=dest_parent,
        )
    else:
        run_scp_copy_dir(
            host=HOST,
            user=USER,
            port=PORT,
            key_path=key_path,
            remote_dir=REMOTE_DIR,
            dest_parent=dest_parent,
        )

if __name__ == "__main__":
    main()
