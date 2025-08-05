

import asyncio, json, time, logging, pathlib
from helpers import tlg

logger = logging.getLogger(__name__)
OUTBOX = pathlib.Path("outbox.jsonl")
RETRIES = 3
BACKOFF = 2          # секунд, будет *2 на каждом шаге

async def safe_send(chat_id: int, text: str, path: str = "", pic: bool = False) -> bool:
    for attempt in range(1, RETRIES + 1):
        try:
            await tlg.send_inform_message(chat_id, text, path, pic)
            return True
        except Exception as e:       # ловим конкретную ошибку вашу
            logger.warning("TG send failed %s/%s: %s", attempt, RETRIES, e)
            await asyncio.sleep(BACKOFF ** attempt)

    logger.error("Message permanently failed, saving to outbox")
    OUTBOX.write_text("") if not OUTBOX.exists() else None
    with OUTBOX.open("a") as f:
        json.dump(
            {"chat_id": chat_id, "text": text, "path": path,
             "pic": pic, "ts": time.time()}, f
        )
        f.write("\n")
    return False