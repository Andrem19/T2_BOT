from openai import OpenAI
from decouple import config
import json
import re
import hashlib
import os
import asyncio
from datetime import datetime, timezone
from metrics.market_watch import collect_news
from helpers.safe_sender import safe_send
import shared_vars as sv

async def news_metric():
    """
    Итоговый конвейер с кэшированием:
      - Присваиваем каждой новости uid.
      - Если появились новые uid — вызываем модели и перезаписываем кэш.
      - Если новых нет — используем кэшированный сырый ответ модели.
      - В COLLECTOR отправляем краткую статистику: всего/новых.
    """

    try:
        # ------------------- Конфигурация/клиенты -------------------
        api_deepseek = config('DEEPSEEK_API', default='')
        api_openai   = config('OPENAI_API',   default='')

        if not api_deepseek:
            raise RuntimeError("DEEPSEEK_API not configured")

        client_ds = OpenAI(api_key=api_deepseek, base_url="https://api.deepseek.com")
        client_oa = OpenAI(api_key=api_openai) if api_openai else None

        env_models = config('OPENAI_CANDIDATES', default='').strip()
        openai_candidates = [m.strip() for m in env_models.split(',') if m.strip()] or ["gpt-4o", "gpt-4.1", "o3-mini"]

        # Файл кэша (полный перезапис при появлении хотя бы одной новой новости)
        CACHE_FILE = os.getenv("NEWS_MODEL_CACHE", ".news_model_cache.json")

        # ----------------------- Сбор новостей -----------------------
        news_items = collect_news(horizon_hours=24, max_items_per_feed=40, max_summary_chars=260)
        sv.logger.info("Всего новостей: %d", len(news_items))

        # Хелперы для безопасного доступа к полям
        dt_now = datetime.now(timezone.utc)

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
                return max(0, int((dt_now - ts).total_seconds() // 60))
            except Exception:
                return 0

        # Устойчивый uid для каждой новости
        def _news_uid(it) -> str:
            src  = str(_attr_or_key(it, "source", "src") or "")
            link = str(_attr_or_key(it, "url", "link") or "")
            pub  = str(_attr_or_key(it, "published_utc", "published_at", "published", "time_utc", "date") or "")
            title= str(_attr_or_key(it, "title", "headline", "name", "text") or "")
            base = link if link else f"{src}|{title}|{pub}"
            return hashlib.md5(base.encode("utf-8")).hexdigest()

        # Compact-линии для промпта и список uid
        compact_lines = []
        current_ids: list[str] = []
        for it in news_items:
            title = _attr_or_key(it, "title", "headline", "text", "name")
            published = _attr_or_key(it, "published_utc", "published_at", "published", "time_utc", "date")
            if not title:
                continue
            t = re.sub(r"\s+", " ", str(title)).strip()
            age_m = _age_minutes(published)
            compact_lines.append(f"{t} | age={age_m}min")
            current_ids.append(_news_uid(it))

        if not compact_lines:
            # Пусто — попробуем использовать кэш, если он есть
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                used_model = cached.get("used_model", "unknown")
                final_data = cached.get("final_data")
                final_raw  = cached.get("final_raw", "")
                stats_line = f"[news_total=0; new=0]"
                await safe_send('COLLECTOR_API', f'{stats_line}\n{used_model}\n{final_raw}', '', False)
                return final_data
            except Exception:
                raise ValueError("No usable news items after normalization and no cache available.")

        news_text = "\n".join(f"- {line}" for line in compact_lines)

        # ----------------------- Загрузка кэша -----------------------
        cached_ids: set[str] = set()
        cached_blob = None
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                cached_blob = json.load(f)
                cached_ids = set(cached_blob.get("news_ids", []))
        except Exception:
            cached_blob = None
            cached_ids = set()

        new_ids = [nid for nid in current_ids if nid not in cached_ids]
        new_count = len(new_ids)
        total_count = len(current_ids)

        # -------------------- Вызовы моделей (при необходимости) --------------------
        system_msg = "You are a financial analyst of crypto markets"

        user_prompt = f'''Your task is to analyze the list of these news items. 
        Each news item should be assigned a weight on a seven-point scale from 1 to 7 depending on how important it is for the next few hours 2-5 hours in terms of its impact on the cryptocurrency market. 
        Also look at how long ago it was released. Today is now {dt_now}. After that, analyze each news item, is it positive for BTC growth or negative, and sum up all the points, plus if positive, minus if negative, do not take into account small and insignificant news at all and concentrate on those that can move the market. And finally give me the final assessment in the format json "score": val '''

        def _call_reasoner(model_name: str):
            return client_ds.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": f"{user_prompt}\n\nNEWS:\n{news_text}"},
                ],
                stream=False,
            )

        def _call_openai(model_name: str):
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

        # --- универсальная обёртка: 60с таймаут, 2 попытки, далее фолбэк ---
        async def _call_with_timeout_twice(callable_fn, label: str):
            for attempt in (1, 2):
                try:
                    # выполняем блокирующий SDK-вызов в отдельном потоке с таймаутом 120с
                    return await asyncio.wait_for(asyncio.to_thread(callable_fn), timeout=180.0)
                except asyncio.TimeoutError:
                    sv.logger.warning("%s timed out after 60s (attempt %d/2)", label, attempt)
                    if attempt == 2:
                        sv.logger.warning("%s timed out twice — moving to fallback", label)
                        return None
                except Exception as e:
                    # не таймаут — пробрасываем наружу, чтобы сработал существующий catch и логика фолбэков
                    raise e

        def _parse_score(text: str):
            if not text:
                return None, None
            m = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
            if m:
                block = m.group(1).strip()
                try:
                    d = json.loads(block)
                    return d, block
                except Exception:
                    pass
            m2 = re.search(r"\{.*?\}", text, re.DOTALL)
            if m2:
                raw = m2.group(0)
                try:
                    d = json.loads(raw)
                    return d, raw
                except Exception:
                    raw2 = re.sub(r",\s*}", "}", raw)
                    raw2 = re.sub(r",\s*]", "]", raw2)
                    try:
                        d = json.loads(raw2)
                        return d, raw2
                    except Exception:
                        pass
            m3 = re.search(r'"score"\s*:\s*(-?\d+(\.\d+)?)', text)
            if m3:
                val = float(m3.group(1))
                return {"score": val}, f'{{"score": {val}}}'
            return None, None

        used_model = None
        final_data = None
        final_raw  = None

        # Если НЕТ новых новостей — используем кэшированный ответ без вызовов моделей
        if new_count == 0 and cached_blob:
            used_model = cached_blob.get("used_model", "unknown")
            final_data = cached_blob.get("final_data")
            final_raw  = cached_blob.get("final_raw", "")
            stats_line = f"[news_total={total_count}; new=0]"
            await safe_send('COLLECTOR_API', f'{stats_line}\n{used_model}\n{final_raw}', '', False)
            sv.logger.info("Нет новых новостей: использован кэш.")
            return final_data

        # Иначе — прогоняем как раньше (reasoner → openai → chat), но с таймаутами/повтором
        try:
            resp = await _call_with_timeout_twice(lambda: _call_reasoner("deepseek-reasoner"), "deepseek-reasoner")
            if resp is not None:
                choice = resp.choices[0] if resp.choices else None
                raw = (getattr(choice.message, "content", None) or "").strip() if choice else ""
                sv.logger.info("DeepSeek-reasoner finish_reason=%s", getattr(choice, "finish_reason", None) if choice else None)
                parsed, raw_json = _parse_score(raw)
                if parsed:
                    used_model = "deepseek:deepseek-reasoner"
                    final_data = parsed
                    final_raw  = raw
        except Exception as e:
            sv.logger.warning("DeepSeek-reasoner error: %s", e)

        if not final_data and client_oa:
            for mdl in openai_candidates:
                try:
                    resp_oa = await _call_with_timeout_twice(lambda: _call_openai(mdl), f"openai:{mdl}")
                    if resp_oa is None:
                        # обе попытки по таймауту — пробуем следующий кандидат
                        continue
                    ch = resp_oa.choices[0] if resp_oa.choices else None
                    raw_oa = (getattr(ch.message, "content", None) or "").strip() if ch else ""
                    sv.logger.info("OpenAI %s finish_reason=%s", mdl, getattr(ch, "finish_reason", None) if ch else None)
                    parsed, raw_json = _parse_score(raw_oa)
                    if parsed:
                        used_model = f"openai:{mdl}"
                        final_data = parsed
                        final_raw  = raw_oa
                        break
                except Exception as e:
                    msg = str(e).lower()
                    if any(k in msg for k in ("model_not_found", "verify", "insufficient", "quota", "billing", "payment", "404")):
                        sv.logger.warning("OpenAI %s access issue: %s", mdl, e)
                    else:
                        sv.logger.warning("OpenAI %s error: %s", mdl, e)
        elif not final_data and not client_oa:
            sv.logger.info("OPENAI_API not configured; skipping OpenAI fallback.")

        if not final_data:
            try:
                resp_c = await _call_with_timeout_twice(_call_deepseek_chat, "deepseek-chat")
                if resp_c is not None:
                    ch2 = resp_c.choices[0] if resp_c.choices else None
                    raw_c = (getattr(ch2.message, "content", None) or "").strip() if ch2 else ""
                    sv.logger.info("DeepSeek-chat finish_reason=%s", getattr(ch2, "finish_reason", None) if ch2 else None)
                    parsed, raw_json = _parse_score(raw_c)
                    if parsed:
                        used_model = "deepseek:deepseek-chat"
                        final_data = parsed
                        final_raw  = raw_c
            except Exception as e:
                sv.logger.warning("DeepSeek-chat error: %s", e)

        if not final_data or final_raw is None:
            raise ValueError("No valid JSON from all providers (reasoner → openai(candidates) → chat).")

        final_score = float(final_data.get("score", 0))
        sv.logger.info("MODEL USED: %s", used_model)
        sv.logger.info("ФИНАЛЬНЫЙ SCORE: %s", final_score)

        # ---------------------- Сохранение кэша (полная перезапись) ----------------------
        cache_payload = {
            "updated_at": dt_now.isoformat(),
            "news_ids": current_ids,              # Полный список текущих uid
            "used_model": used_model,
            "final_raw": final_raw,
            "final_data": final_data,
        }
        try:
            tmp_path = f"{CACHE_FILE}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(cache_payload, f, ensure_ascii=False)
            os.replace(tmp_path, CACHE_FILE)  # атомарная замена
        except Exception as e:
            sv.logger.warning("Не удалось записать кэш %s: %s", CACHE_FILE, e)

        # ---------------------- Отправка в COLLECTOR с краткой сводкой -------------------
        stats_line = f"[news_total={total_count}; new={new_count}]"
        await safe_send('COLLECTOR_API', f'{stats_line}\n{used_model}\n{final_raw}', '', False)
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