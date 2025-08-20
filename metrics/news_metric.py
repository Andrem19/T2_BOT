from openai import OpenAI
from decouple import config
import json
import re
from datetime import datetime, timezone
from metrics.market_watch import collect_news
from helpers.safe_sender import safe_send
import shared_vars as sv

async def news_metric():
    try:
        api = config('DEEPSEEK_API')
        client = OpenAI(api_key=api, base_url="https://api.deepseek.com")
        # Собираем новости с дефолтных лент (crypto + finance), горизонтом 24ч
        news = collect_news(
            horizon_hours=24,          # можно изменить при желании
            max_items_per_feed=40,     # ограничение на каждый фид
            max_summary_chars=260      # длина краткого описания
        )

        sv.logger.info(f"Всего новостей: {len(news)}\n")

        
        dt = datetime.now(timezone.utc)
        prompt = f'''Your task is to analyze the list of these news items. 
        Each news item should be assigned a weight on a seven-point scale from 1 to 7 depending on how important it is for the next few hours 2-5 hours in terms of its impact on the cryptocurrency market. 
        Also look at how long ago it was released. Today is now {dt}. After that, analyze each news item, is it positive for BTC growth or negative, and sum up all the points, plus if positive, minus if negative, and finally give me the final assessment in the format json "score": val '''
        response = client.chat.completions.create(
            model="deepseek-reasoner",
            messages=[
                {"role": "system", "content": "You are a financial analyst of crypto markets"},
                {"role": "user", "content": f"{prompt}\n\n{str(news)}"},
            ],
            stream=False
        )
        # 1. Достаем основной текстовый ответ ассистента (content)
        main_content = response.choices[0].message.content
        sv.logger.info("ОТВЕТ АССИСТЕНТА:")
        sv.logger.info(main_content)
        sv.logger.info("-" * 50)

        # Ищем блок кода, обрамленный ```json ... ```
        json_match = re.search(r'```json\s*(.*?)\s*```', main_content, re.DOTALL)
        data = None
        if json_match:
            json_str = json_match.group(1)
            try:
                data = json.loads(json_str)
                final_score = data['score']
                sv.logger.info(f"ФИНАЛЬНЫЙ SCORE: {final_score}")
            except json.JSONDecodeError as e:
                sv.logger.exception(f"Ошибка парсинга JSON: {e}")
                final_score = None
        else:
            sv.logger.exception("JSON блок не найден в ответе.")
            final_score = None

        await safe_send('COLLECTOR_API', f'{main_content}')
        return data
    except Exception as e:
        sv.logger.exception(f"{e}")
        await safe_send('TELEGRAM_API', f'News Analizer {e}')