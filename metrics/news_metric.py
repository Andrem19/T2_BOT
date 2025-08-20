from openai import OpenAI
from decouple import config
import json
import re
from datetime import datetime, timezone
from metrics.market_watch import collect_news
from helpers.safe_sender import safe_send
import shared_vars as sv

from openai import OpenAI
from decouple import config
import json
import re
from datetime import datetime, timezone
from metrics.market_watch import collect_news
from helpers.safe_sender import safe_send
import shared_vars as sv


async def news_metric():
    """
    Итоговый конвейер:
      1) deepseek-reasoner (без max_tokens/stop/response_format, как в рабочем исходнике)
      2) OpenAI (без Verify Org): gpt-4o → gpt-4.1 → o3-mini
      3) deepseek-chat
    В модель отправляем только нужное: title + published_utc (возраст). В COLLECTOR — сырой ответ.
    """
    try:
        # --- Ключи и клиенты ---
        api_deepseek = config('DEEPSEEK_API', default='')
        api_openai   = config('OPENAI_API',   default='')
        if not api_deepseek:
            raise RuntimeError("DEEPSEEK_API not configured")

        client_ds = OpenAI(api_key=api_deepseek, base_url="https://api.deepseek.com")
        client_oa = OpenAI(api_key=api_openai) if api_openai else None

        # Кандидаты OpenAI без Verify Org (переопределимо через ENV OPENAI_CANDIDATES)
        env_models = config('OPENAI_CANDIDATES', default='').strip()
        openai_candidates = [m.strip() for m in env_models.split(',') if m.strip()] or ["gpt-4o", "gpt-4.1", "o3-mini"]

        # --- Сбор новостей ---
        news_items = collect_news(horizon_hours=24, max_items_per_feed=40, max_summary_chars=260)
        sv.logger.info("Всего новостей: %d", len(news_items))

        # --- Нормализация (оставляем только то, что важно для скоринга) ---
        dt = datetime.now(timezone.utc)

        def _attr_or_key(obj, *names):
            for n in names:
                if hasattr(obj, n):
                    return getattr(obj, n)
                if isinstance(obj, dict) and n in obj:
                    return obj[n]
            return None

        def _age_minutes(ts_val) -> int:
            if not ts_val:
                return 0
            try:
                ts = datetime.fromisoformat(str(ts_val).replace("Z", "+00:00"))
                return max(0, int((dt - ts).total_seconds() // 60))
            except Exception:
                return 0

        # Формируем компактный плоский список строк вида: "<title> | age=<N>min"
        # Это почти как ваш прежний str(news), но без мусора: лаконично и стабильно.
        compact_lines = []
        for it in news_items:
            title = _attr_or_key(it, "title", "headline", "text", "name")
            published = _attr_or_key(it, "published_utc", "published_at", "published", "time_utc", "date")
            if not title:
                continue
            t = re.sub(r"\s+", " ", str(title)).strip()
            age_m = _age_minutes(published)
            compact_lines.append(f"{t} | age={age_m}min")

        if not compact_lines:
            raise ValueError("No usable news items after normalization.")

        # Ввод модели — обычный текстовый список, как в вашем исходнике (без JSON-моды)
        news_text = "\n".join(f"- {line}" for line in compact_lines)

        # --- Сообщения (в духе вашего исходника) ---
        system_msg = "You are a financial analyst of crypto markets"

        user_prompt = f'''Your task is to analyze the list of these news items. 
        Each news item should be assigned a weight on a seven-point scale from 1 to 7 depending on how important it is for the next few hours 2-5 hours in terms of its impact on the cryptocurrency market. 
        Also look at how long ago it was released. Today is now {dt}. After that, analyze each news item, is it positive for BTC growth or negative, and sum up all the points, plus if positive, minus if negative, and finally give me the final assessment in the format json "score": val '''

        # --- Универсальные вызовы ---
        def _call_reasoner(model_name: str):
            # ВАЖНО: без max_tokens, без stop, без response_format — как в вашем рабочем коде.
            return client_ds.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": f"{user_prompt}\n\nNEWS:\n{news_text}"},
                ],
                stream=False,
            )

        def _call_openai(model_name: str):
            # Для OpenAI также без строгих режимов — пусть вернёт как удобно, мы распарсим.
            return client_oa.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": f"{user_prompt}\n\nNEWS:\n{news_text}"},
                ],
                stream=False,
            )

        def _call_deepseek_chat():
            return client_ds.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": f"{user_prompt}\n\nNEWS:\n{news_text}"},
                ],
                stream=False,
            )

        # --- Парсинг ответа (как у вас + усиленный фолбэк) ---
        def _parse_score(text: str):
            if not text:
                return None, None
            # 1) fenced ```json ... ```
            m = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
            if m:
                block = m.group(1).strip()
                try:
                    d = json.loads(block)
                    return d, block
                except Exception:
                    pass
            # 2) первый объект {...}
            m2 = re.search(r"\{.*?\}", text, re.DOTALL)
            if m2:
                raw = m2.group(0)
                try:
                    d = json.loads(raw)
                    return d, raw
                except Exception:
                    # лёгкий санитайзер на хвостовые запятые
                    raw2 = re.sub(r",\s*}", "}", raw)
                    raw2 = re.sub(r",\s*]", "]", raw2)
                    try:
                        d = json.loads(raw2)
                        return d, raw2
                    except Exception:
                        pass
            # 3) просто число после "score"
            m3 = re.search(r'"score"\s*:\s*(-?\d+(\.\d+)?)', text)
            if m3:
                val = float(m3.group(1))
                return {"score": val}, f'{{"score": {val}}}'
            return None, None

        used_model = None
        final_data = None
        final_raw = None

        # 1) deepseek-reasoner
        try:
            resp = _call_reasoner("deepseek-reasoner")
            choice = resp.choices[0] if resp.choices else None
            raw = (getattr(choice.message, "content", None) or "").strip() if choice else ""
            sv.logger.info("DeepSeek-reasoner finish_reason=%s", getattr(choice, "finish_reason", None) if choice else None)
            parsed, raw_json = _parse_score(raw)
            if parsed:
                used_model = "deepseek:deepseek-reasoner"
                final_data = parsed
                final_raw = raw  # отправим полностью, без усечений
        except Exception as e:
            sv.logger.warning("DeepSeek-reasoner error: %s", e)

        # 2) OpenAI fallback
        if not final_data and client_oa:
            for mdl in openai_candidates:
                try:
                    resp_oa = _call_openai(mdl)
                    ch = resp_oa.choices[0] if resp_oa.choices else None
                    raw_oa = (getattr(ch.message, "content", None) or "").strip() if ch else ""
                    sv.logger.info("OpenAI %s finish_reason=%s", mdl, getattr(ch, "finish_reason", None) if ch else None)
                    parsed, raw_json = _parse_score(raw_oa)
                    if parsed:
                        used_model = f"openai:{mdl}"
                        final_data = parsed
                        final_raw = raw_oa
                        break
                except Exception as e:
                    msg = str(e).lower()
                    if any(k in msg for k in ("model_not_found", "verify", "insufficient", "quota", "billing", "payment", "404")):
                        sv.logger.warning("OpenAI %s access issue: %s", mdl, e)
                    else:
                        sv.logger.warning("OpenAI %s error: %s", mdl, e)
        elif not final_data and not client_oa:
            sv.logger.info("OPENAI_API not configured; skipping OpenAI fallback.")

        # 3) deepseek-chat fallback
        if not final_data:
            try:
                resp_c = _call_deepseek_chat()
                ch2 = resp_c.choices[0] if resp_c.choices else None
                raw_c = (getattr(ch2.message, "content", None) or "").strip() if ch2 else ""
                sv.logger.info("DeepSeek-chat finish_reason=%s", getattr(ch2, "finish_reason", None) if ch2 else None)
                parsed, raw_json = _parse_score(raw_c)
                if parsed:
                    used_model = "deepseek:deepseek-chat"
                    final_data = parsed
                    final_raw = raw_c
            except Exception as e:
                sv.logger.warning("DeepSeek-chat error: %s", e)

        if not final_data or final_raw is None:
            raise ValueError("No valid JSON from all providers (reasoner → openai(candidates) → chat).")

        # Итог
        final_score = float(final_data.get("score", 0))
        sv.logger.info("MODEL USED: %s", used_model)
        sv.logger.info("ФИНАЛЬНЫЙ SCORE: %s", final_score)

        # В COLLECTOR — сырой полный ответ модели
        await safe_send('COLLECTOR_API', f'{used_model}\n{final_raw}', '', False)
        return final_data

    except Exception as e:
        sv.logger.exception("%s", e)
        await safe_send('TELEGRAM_API', f'News Analyzer error: {e}', '', False)




# async def news_metric():
#     try:
#         api = config('DEEPSEEK_API')
#         client = OpenAI(api_key=api, base_url="https://api.deepseek.com")
#         # Собираем новости с дефолтных лент (crypto + finance), горизонтом 24ч
#         news = collect_news(
#             horizon_hours=24,          # можно изменить при желании
#             max_items_per_feed=40,     # ограничение на каждый фид
#             max_summary_chars=260      # длина краткого описания
#         )

#         sv.logger.info(f"Всего новостей: {len(news)}\n")

        
#         dt = datetime.now(timezone.utc)
#         prompt = f'''Your task is to analyze the list of these news items. 
#         Each news item should be assigned a weight on a seven-point scale from 1 to 7 depending on how important it is for the next few hours 2-5 hours in terms of its impact on the cryptocurrency market. 
#         Also look at how long ago it was released. Today is now {dt}. After that, analyze each news item, is it positive for BTC growth or negative, and sum up all the points, plus if positive, minus if negative, and finally give me the final assessment in the format json "score": val '''

#         response = client.chat.completions.create(
#             model="deepseek-reasoner",
#             messages=[
#                 {"role": "system", "content": "You are a financial analyst of crypto markets"},
#                 {"role": "user", "content": f"{prompt}\n\n{str(news)}"},
#             ],
#             stream=False
#         )
#         # 1. Достаем основной текстовый ответ ассистента (content)
#         main_content = response.choices[0].message.content
#         sv.logger.info("ОТВЕТ АССИСТЕНТА:")
#         sv.logger.info(main_content)
#         sv.logger.info("-" * 50)

#         # Ищем блок кода, обрамленный ```json ... ```
#         json_match = re.search(r'```json\s*(.*?)\s*```', main_content, re.DOTALL)
#         data = None
#         if json_match:
#             json_str = json_match.group(1)
#             try:
#                 data = json.loads(json_str)
#                 final_score = data['score']
#                 sv.logger.info(f"ФИНАЛЬНЫЙ SCORE: {final_score}")
#             except json.JSONDecodeError as e:
#                 sv.logger.exception(f"Ошибка парсинга JSON: {e}")
#                 final_score = None
#         else:
#             sv.logger.exception("JSON блок не найден в ответе.")
#             final_score = None

#         await safe_send('COLLECTOR_API', f'{main_content}')
#         return data
#     except Exception as e:
#         sv.logger.exception(f"{e}")
#         await safe_send('TELEGRAM_API', f'News Analizer {e}')