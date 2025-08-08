from decouple import config
from telegram import Bot
import shared_vars as sv
from typing import Optional
from telegram.constants import ParseMode  # python-telegram-bot v21+

async def send_inform_message(telegram_token, message, image_path: str, send_pic: bool):
    try:
        sv.logger.info(message)
        api_token = config(telegram_token)
        chat_id = config("CHAT_ID")

        bot = Bot(token=api_token)

        response = None
        if send_pic:
            with open(image_path, 'rb') as photo:
                response = await bot.send_photo(chat_id=chat_id, photo=photo, caption=message)
        else:
            response = await bot.send_message(chat_id=chat_id, text=message)

        if response:
            pass
        else:
            print("Failed to send inform message.")
    except Exception as e:
        sv.logger.error("ERROR(send_inform_message): An error occurred:", str(e))
    
async def send_dict_as_markdown_table(telegram_token, data_dict):
    try:
        api_token = config(telegram_token)
        chat_id = config("CHAT_ID")

        bot = Bot(token=api_token)

        # Start the markdown table
        message = "```\n"

        # Add each key-value pair to the table
        for key, value in data_dict.items():
            message += f"{key} : {value}\n"

        # End the markdown table
        message += "```"

        # Send the message
        response = await bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown')

        if response:
            pass
        else:
            print("Failed to send inform message.")
    except Exception as e:
        print("An error occurred:", str(e))




async def send_option_message(
    telegram_token: str,
    message: str,
    image_path: Optional[str],
    send_pic: bool
):
    """
    Отправка сообщения (и, при необходимости, фото) в Telegram с поддержкой HTML.
    - message: строка с HTML-разметкой (экранированной), например, из format_option_message_html().
    - telegram_token: имя переменной окружения с токеном бота (для decouple.config).
    - image_path: путь к картинке (если send_pic=True).
    - send_pic: True -> отправить фото; False -> только текст.

    Замечания:
    - Для текста используем parse_mode=HTML.
    - Для фото, если caption > 1024 символов, сначала отправим текст отдельным сообщением,
      затем отправим фото без подписи (из-за лимита Telegram).
    """
    try:
        sv.logger.info("Sending inform message to Telegram")

        api_token = config(telegram_token)
        chat_id = config("CHAT_ID")

        # Безопасно открываем/закрываем HTTP-сессию бота
        async with Bot(token=api_token) as bot:
            # Если нужна картинка
            if send_pic:
                if not image_path:
                    sv.logger.warning("send_pic=True, но image_path не указан — отправляю только текст.")
                    await bot.send_message(
                        chat_id=chat_id,
                        text=message,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                    return

                # Если сообщение слишком длинное для caption — отправим текст отдельно
                if len(message) > 1024:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=message,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                    with open(image_path, "rb") as photo:
                        await bot.send_photo(chat_id=chat_id, photo=photo)
                else:
                    # Влезает в caption — отправим одним сообщением
                    with open(image_path, "rb") as photo:
                        await bot.send_photo(
                            chat_id=chat_id,
                            photo=photo,
                            caption=message,
                            parse_mode=ParseMode.HTML,
                        )
            else:
                # Только текстовое сообщение
                await bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

    except Exception:
        # Лаконично и полезно — полный stacktrace в логах
        sv.logger.exception("ERROR(send_inform_message): failed to send Telegram message")
