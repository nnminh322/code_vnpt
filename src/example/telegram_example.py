import os 
import requests

# download
# bot_token = "7795284458:AAGPJp_MRX9OGQu5adJWJNESEh2q0c8r0Cg"
# chat_id = "-1002184124681"
# file_id = "BQACAgUAAxkBAAE0EsNoCLo9a6JJf7Eo_IzJC2tmoSdJ_gACWRcAAmeqSFRGXtphkSAspDYE"
# url = f"https://api.telegram.org/bot{bot_token}/getFile?file_id={file_id}"
# proxy_server = {"http": "http://10.144.13.144:3129", "https": "http://10.144.13.144:3129"}

def get_file_id(bot_token, proxy_server):

    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    response = requests.get(url, proxies=proxy_server)

    return response.json()['result'][-1]['message']['document']['file_id']


def tele_down(bot_token, file_id, proxy_server, file_dir):
    file_id = get_file_id(bot_token, proxy_server)
    
    url = f"https://api.telegram.org/bot{bot_token}/getFile?file_id={file_id}"
    response = requests.get(url, proxies=proxy_server)
    response.raise_for_status()

    file_path = response.json()["result"]["file_path"]
    down_url = f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
    file_data = requests.get(down_url, proxies=proxy_server)

    with open(file_dir, "wb") as f:
        f.write(file_data.content)


def tele_send(bot_token, chat_id, proxy_server, file_dir):
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"

    with open(file_dir, 'rb') as file: 
        response = requests.post(
            url,
            data={'chat_id':chat_id},
            files={'document': file},
            proxies=proxy_server
        )