from decouple import config
from telegram import Bot
import shared_vars as sv
import time
import shared_vars as sv

async def send_queue():
    if len(sv.messages_queue)> 0:
        msg = ''
        for message in sv.messages_queue:
            msg+=message+'\n'
        await send_inform_message(sv.settings_gl.telegram_token, msg, '', False)
        sv.messages_queue.clear()

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
        print("An error occurred:", str(e))
    
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

