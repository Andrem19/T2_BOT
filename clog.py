#!/usr/bin/env python3
# sudo systemctl restart t2-manager   # вручную, если нужно перезапустить
# sudo systemctl stop t2-manager
# sudo systemctl disable t2-manager
# sudo systemctl status t2-manager
import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

def check_prereqs():
    from shutil import which
    if which("scp") is None:
        print("Ошибка: не найден 'scp'. Установите openssh-client (Ubuntu: sudo apt-get install -y openssh-client).", file=sys.stderr)
        sys.exit(1)

def ensure_key_permissions(key_path: Path):
    if not key_path.exists():
        print(f"Ошибка: ключ не найден: {key_path}", file=sys.stderr)
        sys.exit(1)
    # SSH требует права 600 на приватный ключ
    try:
        os.chmod(key_path, 0o600)
    except PermissionError:
        # Если нет прав менять chmod, просто предупреждаем
        pass

def run_scp_copy(host: str, user: str, port: int, key_path: Path, remote_dir: str, dest_parent: Path):
    # Копируем директорию целиком в текущую папку:
    #   scp -r -i KEY -P PORT -o StrictHostKeyChecking=accept-new ubuntu@host:/path/_logs .
    # Перед этим удалим локальный _logs, если он есть.
    local_dir = dest_parent / Path(remote_dir).name
    if local_dir.exists():
        print(f"Удаляю локальную папку: {local_dir}")
        shutil.rmtree(local_dir)

    print(f"Копирую {user}@{host}:{remote_dir} -> {dest_parent}")
    cmd = [
        "scp",
        "-r",
        "-i", str(key_path),
        "-P", str(port),
        "-o", "StrictHostKeyChecking=accept-new",
        f"{user}@{host}:{remote_dir}",
        str(dest_parent)
    ]
    # Выполняем и пробрасываем вывод
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=False)
    except subprocess.CalledProcessError as e:
        print("Ошибка при копировании scp.", file=sys.stderr)
        sys.exit(e.returncode)

    # Финальная проверка
    if not local_dir.exists():
        print(f"Ошибка: после копирования не найден локальный каталог: {local_dir}", file=sys.stderr)
        sys.exit(1)
    print(f"Готово. Локальная папка: {local_dir}")

def main():
    parser = argparse.ArgumentParser(
        description="Скопировать удалённый каталог /home/ubuntu/T2_BOT/_logs на локальную машину (в текущую директорию), заменив существующий."
    )
    parser.add_argument("--host", default='54.254.56.129', help="Статический IP или домен сервера Lightsail")
    parser.add_argument("--user", default="ubuntu", help="Пользователь SSH (по умолчанию: ubuntu)")
    parser.add_argument("--port", type=int, default=22, help="Порт SSH (по умолчанию: 22)")
    parser.add_argument("--key", default="/home/jupiter/.ssh/lightsail.pem", help="Путь к приватному ключу SSH")
    parser.add_argument("--remote-path", default="/home/ubuntu/T2_BOT/_logs", help="Удалённый путь к каталогу _logs")
    args = parser.parse_args()

    dest_parent = Path.cwd()
    key_path = Path(args.key)

    check_prereqs()
    ensure_key_permissions(key_path)
    run_scp_copy(
        host=args.host,
        user=args.user,
        port=args.port,
        key_path=key_path,
        remote_dir=args.remote_path.rstrip("/"),
        dest_parent=dest_parent
    )

if __name__ == "__main__":
    main()
