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
    Анализ новостей с гарантированным JSON-выводом.
    Усиления:
      • JSON-mode через response_format={"type":"json_object"}.
      • Два режима:
          – Дешёвые часы (UTC 16–23 и 00): подробный JSON {"score", "details":[...]}.
          – Дорогие часы (UTC 01–15): минимальный JSON {"score": N}.
      • Новости передаются как JSON.
      • Надёжный парсинг + fallback: если deepseek-reasoner вернул пусто, пробуем deepseek-chat.
      • Ни при каких условиях не прокидываем reasoning_content в следующий запрос.
    """
    try:
        api = config('DEEPSEEK_API')
        client = OpenAI(api_key=api, base_url="https://api.deepseek.com")

        # 1) Сбор новостей (горизонт 24ч)
        news = collect_news(
            horizon_hours=24,
            max_items_per_feed=40,
            max_summary_chars=260
        )
        sv.logger.info("Всего новостей: %d", len(news))

        # 2) Текущее время (UTC) и режимы
        dt = datetime.now(timezone.utc)
        CHEAP_HOURS_UTC = {16, 17, 18, 19, 20, 21, 22, 23, 0}  # дешёвые часы по вашему правилу
        cheap_mode = dt.hour in CHEAP_HOURS_UTC

        # 3) Сообщения
        system_msg = "You are a crypto market analyst. Output json only."

        if cheap_mode:
            # Подробный структурированный ответ
            user_msg = (
                f"Now (UTC): {dt.isoformat()}. Analyze the following crypto news items.\n"
                "For the next 2–5 hours: assign each headline an integer weight 1–7 for impact.\n"
                "Positive → add; negative → subtract. Sum all weights into one signed score.\n"
                "Return valid JSON ONLY using this schema:\n"
                "{\n"
                '  "score": <number>,\n'
                '  "details": [\n'
                '    {"headline": "<string>", "weight": <1-7>, "sign": "positive|negative", "age_minutes": <int>}\n'
                "  ]\n"
                "}\n"
                "No markdown, no extra text."
            )
            max_tokens = 2000
        else:
            # Минимальный ответ в дорогие часы
            user_msg = (
                f"Now (UTC): {dt.isoformat()}. Analyze the following crypto news items.\n"
                "For the next 2–5 hours: assign each headline an integer weight 1–7 for impact.\n"
                "Positive → add; negative → subtract. Sum all weights into a single signed score.\n"
                "Return exactly this valid JSON object and nothing else:\n"
                '{"score": <number>}'
            )
            max_tokens = 200

        # 4) Новости подаём в JSON
        news_json = json.dumps(news, ensure_ascii=False, default=str)

        def _extract_json(text: str) -> str:
            """Достаёт JSON-объект из текста (fenced-блок или последний {...})."""
            if not text:
                return ""
            m = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
            if m:
                return m.group(1).strip()
            start, end = text.rfind("{"), text.rfind("}")
            if start != -1 and end != -1 and end > start:
                candidate = text[start:end + 1].strip()
                try:
                    # нормализуем и возвращаем строку JSON
                    return json.dumps(json.loads(candidate), ensure_ascii=False)
                except Exception:
                    pass
            return ""

        def _call_model(model_name: str):
            return client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": user_msg + "\n\nNEWS_JSON:\n" + news_json},
                ],
                response_format={"type": "json_object"},
                temperature=0,
                max_tokens=max_tokens,
                stream=False,
            )

        # 5) Основной вызов: deepseek-reasoner
        response = _call_model("deepseek-reasoner")
        choice = response.choices[0]

        raw = (getattr(choice.message, "content", None) or "").strip()
        finish_reason = getattr(choice, "finish_reason", None)
        sv.logger.info("finish_reason=%s", finish_reason)
        sv.logger.info("ОТВЕТ АССИСТЕНТА (raw): %s", raw)
        sv.logger.info("-" * 50)

        # 6) Если пусто — пробуем вытащить JSON из reasoning_content (не логируем его целиком)
        if not raw:
            rc = (getattr(choice.message, "reasoning_content", None) or "")
            raw = _extract_json(rc)

        # 7) Если всё ещё пусто — fallback на deepseek-chat
        if not raw:
            sv.logger.warning("Empty content from deepseek-reasoner; falling back to deepseek-chat.")
            response2 = _call_model("deepseek-chat")
            choice2 = response2.choices[0]
            raw = (getattr(choice2.message, "content", None) or "").strip()
            if not raw:
                rc2 = (getattr(choice2.message, "reasoning_content", None) or "")
                raw = _extract_json(rc2)

        if not raw:
            raise ValueError("Model returned empty content")

        # 8) Снимаем возможные ограждения ```
        if raw.startswith("```"):
            s, e = raw.find("{"), raw.rfind("}")
            if s != -1 and e != -1 and e > s:
                raw = raw[s:e + 1].strip()

        data = json.loads(raw)  # JSON-mode должен гарантировать валидность
        final_score = float(data.get("score", 0))
        sv.logger.info("ФИНАЛЬНЫЙ SCORE: %s", final_score)

        await safe_send('COLLECTOR_API', raw, '', False)
        return data

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
#         only_json = '\nReturn only valid JSON. Do not explain anything. Do not include any additional text or markdown.'
#         if dt.hour not in [16, 17, 18, 19, 20, 21, 22, 23]:
#             prompt += only_json
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