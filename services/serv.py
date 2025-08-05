from database.commands_tab import Commands
import shared_vars as sv
import json
import os
from datetime import datetime, timedelta, timezone

TEMP_FILE_PATH = "params_temp.json"

def save_stages(stages: dict, file_path: str = TEMP_FILE_PATH) -> None:
    """
    Сериализует словарь stages вместе с текущей датой сохранения (UTC)
    и записывает в файл, всегда полностью перезаписывая его.
    Любые объекты, не поддерживаемые JSON напрямую (например, datetime),
    будут автоматически приведены к строке.
    """
    data = {
        "saved_at": datetime.now(timezone.utc),
        "stages": stages
    }
    with open(file_path, 'w', encoding='utf-8') as f:
        # default=str конвертирует datetime → строка, а также любые другие неподдерживаемые объекты
        json.dump(data, f, ensure_ascii=False, indent=4, default=str)

def load_stages(file_path: str = TEMP_FILE_PATH) -> dict | None:
    """
    Читает JSON‑файл и возвращает словарь stages, если:
      1. Файл существует и корректно парсится.
      2. В нём есть строка saved_at в ISO‑формате.
      3. Дата сохранения не старше 2 дней.
    В противном случае возвращает None.
    """
    if not os.path.exists(file_path):
        return None

    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return None

    saved_at_str = data.get("saved_at")
    if not isinstance(saved_at_str, str):
        return None

    try:
        saved_at = datetime.fromisoformat(saved_at_str)
    except ValueError:
        return None

    if datetime.now(timezone.utc) - saved_at > timedelta(days=2):
        return None

    return data.get("stages")

async def refresh_commands_from_bd():
    com = Commands.get_instance()
    sv.stages['first']['amount'] = com.amount_1
    sv.stages['second']['amount'] = com.amount_2
    sv.stages['first']['expect'] = com.expect_1
    sv.stages['second']['expect'] = com.expect_2
    sv.timer_msg = com.timer
    sv.close_1 = com.close_1
    sv.close_2 = com.close_2
    symbols = []
    if com.btc:
        symbols.append('BTC')
    if com.eth:
        symbols.append('ETH')
    if com.sol:
        symbols.append('SOL')
    sv.symbols = symbols
