# quick_test.py
# Полный скрипт для быстрой проверки. Использует OpenAI-совместимый API Ollama.
# Устанавливаем зависимости: pip install --upgrade openai httpx
# sudo systemctl start ollama
# OLLAMA_KEEP_ALIVE=30m ollama serve
from openai import OpenAI

def main() -> None:
    client = OpenAI(
        base_url="http://localhost:11434/v1",
        api_key="ollama"  # любое непустое значение
    )

    # Короткий запрос, ограниченный max_tokens (быстро)
    resp = client.chat.completions.create(
        model="gpt-oss:20b",
        messages=[
            {"role": "system", "content": "Your answers are brief and to the point. This is the title and other data of the article. Your task is to determine the importance of this article for the crypto market (BTС, ETH) on a 7-point scale. And the duration of how many days to consider this news relevant (meaning it can still influence) You need to return in the json format an answer with two fields importance and duration and the corresponding values."},
            {"role": "user", "content": 
''' 
category	crypto
source	CoinDesk
title	Salomon Brothers Say It Has Completed Process of Notifying 'Abandoned' Crypto Wallets
published_utc	2025-08-07'''}
        ],
        temperature=0.2,
        max_tokens=200,
        top_p=0.9,
        stream=True,
    )

    print("Ответ модели:\n")
    for chunk in resp:
        delta = chunk.choices[0].delta.content or ""
        print(delta, end="", flush=True)
    print()

if __name__ == "__main__":
    main()
