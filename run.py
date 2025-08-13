import os
import re
import time
import logging
import requests
import datetime

from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import vk_api
from vk_api.upload import VkUpload
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType

from load import city_data

load_dotenv()

VK_GROUP_TOKEN = os.getenv('TOKEN')
GROUP_ID = os.getenv('VK_GROUP_ID')

vk_session = vk_api.VkApi(token=VK_GROUP_TOKEN)
vk = vk_session.get_api()
longpoll = VkBotLongPoll(vk_session, GROUP_ID)

print('Бот запущен')

LOGIN = os.getenv('PIK_LOGIN')
PASSWORD = os.getenv('PIK_PASSWORD')
LOGIN_URL = "http://www.pogodaiklimat.ru/login.php"

API_KEYS = {
    "армавир": os.getenv("WEATHER_ARMAVIR"),
    "похвистнево": os.getenv("WEATHER_POHVISTNEVO")
}

STATION_IDS = {
    "армавир": "IARMAV7",
    "похвистнево": "IPOKHV1"
}

CITY_NAMES = {
    "армавир": "Армавир",
    "похвистнево": "Похвистнево"
}

def log(message):
    print(message)
    logging.info(message)

def check_api_keys():
    log("Проверка API ключей и stationId...")
    for city in API_KEYS:
        api_key = API_KEYS[city]
        station_id = STATION_IDS[city]
        url = f"https://api.weather.com/v2/pws/observations/current?stationId={station_id}&format=json&units=m&apiKey={api_key}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                log(f"❌ Проверка не пройдена для '{city}': статус {response.status_code} - {response.text}")
            elif not response.content:
                log(f"❌ Проверка не пройдена для '{city}': пустой ответ.")
            else:
                log(f"✅ Проверка успешна для '{city}'.")
        except Exception as e:
            log(f"❌ Ошибка при проверке '{city}': {e}")

def safe_get(data, path, default="н/д"):
    try:
        for key in path:
            data = data[key]
        return data
    except (KeyError, TypeError):
        return default

def get_risk_level(temperature, wind_speed_ms, wind_gust_ms, uv_index, pressure, humidity, dew_point):
    levels = []

    # Температура
    if temperature >= 45 or temperature <= -45:
        levels.append(5)
    elif temperature >= 40 or temperature <= -35:
        levels.append(4)
    elif temperature >= 35 or temperature <= -25:
        levels.append(3)
    elif temperature >= 30 or temperature <= -15:
        levels.append(2)
    else:
        levels.append(1)

    # Скорость ветра (постоянная)
    if wind_speed_ms >= 25:
        levels.append(5)
    elif wind_speed_ms >= 20:
        levels.append(4)
    elif wind_speed_ms >= 15:
        levels.append(3)
    elif wind_speed_ms >= 7:
        levels.append(2)
    else:
        levels.append(1)

    # Порывы ветра
    if wind_gust_ms >= 33:
        levels.append(5)
    elif wind_gust_ms >= 25:
        levels.append(4)
    elif wind_gust_ms >= 20:
        levels.append(3)
    elif wind_gust_ms >= 10:
        levels.append(2)
    else:
        levels.append(1)

    # УФ-индекс
    if uv_index >= 11:
        levels.append(5)
    elif uv_index >= 9:
        levels.append(4)
    elif uv_index >= 7:
        levels.append(3)
    elif uv_index >= 3:
        levels.append(2)
    else:
        levels.append(1)

    # Давление
    if pressure <= 950 or pressure >= 1080:
        levels.append(5)
    elif pressure <= 970 or pressure >= 1060:
        levels.append(4)
    elif pressure <= 980 or pressure >= 1040:
        levels.append(3)
    elif pressure <= 990 or pressure >= 1020:
        levels.append(2)
    else:
        levels.append(1)

    # Влажность — всегда 1 (удалены уровни опасности)
    levels.append(1)

    # Точка росы (высокая - опасность)
    if dew_point >= 25:
        levels.append(5)
    elif dew_point >= 20:
        levels.append(4)
    elif dew_point >= 16:
        levels.append(3)
    elif dew_point >= 12:
        levels.append(2)
    else:
        levels.append(1)

    max_level = max(levels)

    level_map = {
        1: "🟢 Дискомфорт отсутвует",
        2: "🟡 Лёгкий дискомфорт",
        3: "🟠 Повышенный дискомфорт",
        4: "🔴 Дискомфорт высокой опасности",
        5: "🟣 Дискомфорт экстремальной опасности"
    }

    return level_map[max_level]

def get_weather(city_key):
    if city_key not in API_KEYS:
        log(f"Ошибка: населённый пункт '{city_key}' не найден в конфигурации.")
        return (f"Данной АМС ({city_key.capitalize()}) нету в нашей базе.\n"
                "Если вы хотите её добавить, свяжитесь с нами:\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "E-mail: ✉️ meteovrn@inbox.ru\n"
                "Telegram: 📲 t.me/meteovrn\n"
                "ВКонтакте: 🌐 vk.com/meteoexplorer"
                )

    api_key = API_KEYS[city_key]
    station_id = STATION_IDS[city_key]
    url = f"https://api.weather.com/v2/pws/observations/current?stationId={station_id}&format=json&units=m&apiKey={api_key}"

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        log(f"Ошибка запроса к API: {e}")
        return "⚠️ Не удалось связаться с сервером погоды."

    if response.status_code != 200 or not response.content:
        log(f"Ошибка API: {response.status_code} - {response.text}")
        return "⚠️ Не удалось получить данные о погоде."

    try:
        data = response.json()
        obs = data['observations'][0]
        temperature = round(float(safe_get(obs, ['metric', 'temp'], 0)))
        humidity = round(float(safe_get(obs, ['humidity'], 0)))
        dewpt = round(float(safe_get(obs, ['metric', 'dewpt'], 0)))
        wind_speed_kmh = float(safe_get(obs, ['metric', 'windSpeed'], 0))
        wind_gust_kmh = float(safe_get(obs, ['metric', 'windGust'], 0))
        wind_speed_ms = round(wind_speed_kmh / 3.6, 1)
        wind_gust_ms = round(wind_gust_kmh / 3.6, 1)
        wind_dir = safe_get(obs, ['winddir'], 0)
        feelslike = round(float(safe_get(obs, ['metric', 'heatIndex'], 0)))
        uv_index = round(float(safe_get(obs, ['uv'], 0)))
        solar_radiation = round(float(safe_get(obs, ['solarRadiation'], 0)), 1)
        pressure = round(float(safe_get(obs, ['metric', 'pressure'], 0)), 1)
        precip_rate = round(float(safe_get(obs, ['metric', 'precipRate'], 0)), 1)
        precip_total = round(float(safe_get(obs, ['metric', 'precipTotal'], 0)), 1)
        obs_time = safe_get(obs, ['obsTimeLocal'])

        wind_direction = get_wind_direction(wind_dir)

        risk = get_risk_level(temperature, wind_speed_ms, wind_gust_ms, uv_index, pressure, humidity, dewpt)

        result = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🌍 Погода в {CITY_NAMES[city_key]}\n"
            f"🕑 Дата и время: {obs_time}\n"
            f"📡 Источник: АМС (автоматическая метеостанция)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"{risk}\n"
            f"\n"
            f"🌡 Температура воздуха: {temperature}°C\n"
            f"🤗 Ощущается как: {feelslike}°C\n"
            f"💧 Влажность воздуха: {humidity}%\n"
            f"💦 Точка росы: {dewpt}°C\n"
            f"🌬 Ветер: {wind_direction} {wind_speed_ms} м/с (порывы до {wind_gust_ms} м/с)\n"
            f"📈 Атм. давление: {pressure} гПа\n"
            f"🌧 Интенсивность осадков: {precip_rate} мм/ч\n"
            f"💦 Суммарные осадки: {precip_total} мм\n"
            f"🌞 УФ-индекс: {uv_index} ☀️\n"
            f"🔆 Солнечная радиация: {solar_radiation} Вт/м²\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )

        log(f"Данные для {CITY_NAMES[city_key]} успешно получены.")
        return result

    except Exception as e:
        log(f"Ошибка обработки данных: {e}")
        return "⚠️ Ошибка обработки данных погоды."

def get_wind_direction(degree):
    dirs = ['С', 'ССВ', 'СВ', 'ВСВ', 'В', 'ВЮВ', 'ЮВ', 'ЮЮВ', 'Ю', 'ЮЮЗ', 'ЮЗ', 'ЗЮЗ', 'З', 'ЗСЗ', 'СЗ', 'ССЗ']
    ix = round(degree / 22.5) % 16
    return dirs[ix]


def upload_photo(vk_session, file_path):
    upload = VkUpload(vk_session)
    photo = upload.photo_messages(file_path)[0]
    return f"photo{photo['owner_id']}_{photo['id']}"


def login_pik():
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' 
                      '(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    resp = session.get(LOGIN_URL, headers=headers)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'html.parser')

    hidden_inputs = soup.find_all("input", type="hidden")
    data = {inp['name']: inp.get('value', '') for inp in hidden_inputs}

    data.update({
        'username': LOGIN,
        'password': PASSWORD,
        'submit': 'Войти',
    })

    login_response = session.post(LOGIN_URL, data=data, headers=headers)
    login_response.encoding = 'utf-8'

    if "Выход" in login_response.text or "logout" in login_response.text.lower():
        print("Успешно авторизовались на сайте.")
        return session
    else:
        print("Не удалось авторизоваться. Проверь логин и пароль.")
        return None


def get_clean_text(td_element):
    if td_element is None:
        return "нет данных"
    text = td_element.get_text(strip=True)
    if text.startswith('+'):
        text = text[1:]
    return text


def parse_weather_data(session, station_id: str, date_str: str) -> str:
    try:
        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
        month = date_obj.month
        year = date_obj.year

        url = f"http://www.pogodaiklimat.ru/summary.php?m={month}&y={year}&id={station_id}"

        headers = {'User-Agent': 'Mozilla/5.0'}
        response = session.get(url, headers=headers)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", {"class": "tab"})
        if not table:
            return "⚠️ Таблица не найдена. Проверь код станции или дату."

        rows = table.find_all("tr")[2:]

        for row in rows:
            cols = row.find_all("td")
            if not cols or len(cols) < 15:
                continue

            date_in_row = cols[2].get_text(strip=True)

            if date_in_row == date_str:
                station_name = get_clean_text(cols[1])
                temp_avg = get_clean_text(cols[3])
                temp_dt_avg = get_clean_text(cols[4])
                temp_min = get_clean_text(cols[6])
                temp_max = get_clean_text(cols[7])
                humidity = get_clean_text(cols[8])
                humidity_min = get_clean_text(cols[9])
                eff_te_min = get_clean_text(cols[10])
                eff_te_max = get_clean_text(cols[11])
                eff_tes_max = get_clean_text(cols[12])
                wind = get_clean_text(cols[13])
                wind_gust = get_clean_text(cols[14])
                min_view = get_clean_text(cols[15])
                avg_pressure = float(get_clean_text(cols[16])) * 0.75
                min_pressure = float(get_clean_text(cols[17])) * 0.75
                max_pressure = float(get_clean_text(cols[18])) * 0.75
                avg_mark_cloud = get_clean_text(cols[22])
                low_mark_cloud = get_clean_text(cols[23])
                night_precip = get_clean_text(cols[24])
                day_precip = get_clean_text(cols[25])
                sum_precip = get_clean_text(cols[26])
                snow_cover = get_clean_text(cols[27])
                case_rain = get_clean_text(cols[29])
                case_snow = get_clean_text(cols[30])
                case_fog = get_clean_text(cols[31])
                case_mist = get_clean_text(cols[32])
                case_snowstorm = get_clean_text(cols[33])
                case_snowfall = get_clean_text(cols[34])
                case_thunderstorm = get_clean_text(cols[35])
                case_tornado = get_clean_text(cols[36])
                case_dust_storm = get_clean_text(cols[37])
                case_dustfall = get_clean_text(cols[38])
                case_hail = get_clean_text(cols[39])
                case_black_ice = get_clean_text(cols[40])

                return (
                    f"📊 Данные М/С {station_id} — {station_name} за {date_str}:\n\n"

                    f"🌡 Температуры:\n"
                    f"  • Максимальная: {temp_max} °C\n"
                    f"  • Средняя: {temp_avg} °C\n"
                    f"  • Минимальная: {temp_min} °C\n"
                    f"  • Температурная аномалия: {temp_dt_avg} °С\n"
                    f"  • Эффективная температура в тени (мин.): {eff_te_min} °С\n"
                    f"  • Эффективная температура в тени (макс.): {eff_te_max} °С\n"
                    f"  • Эффективная температура на Солнце (макс.): {eff_tes_max} °С\n\n"

                    f"📈 Давление (мм рт. ст.):\n"
                    f"  • Среднее: {str(avg_pressure)[:5]}\n"
                    f"  • Минимальное: {str(min_pressure)[:5]}\n"
                    f"  • Максимальное: {str(max_pressure)[:5]}\n\n"

                    f"💨 Ветер:\n"
                    f"  • Средняя скорость: {wind} м/с\n"
                    f"  • Порывы: {wind_gust} м/с\n\n"

                    f"👁 Видимость:\n"
                    f"  • Минимальная: {min_view}\n\n"

                    f"💦 Влажность:\n"
                    f"  • Средняя: {humidity} %\n"
                    f"  • Минимальная: {humidity_min} %\n\n"

                    f"🌧 Осадки (мм):\n"
                    f"  • Ночью: {night_precip if night_precip else '0.0'}\n"
                    f"  • Днём: {day_precip if day_precip else '0.0'}\n"
                    f"  • Суммарно: {sum_precip}\n\n"

                    f"☁️ Облачность:\n"
                    f"  • Средняя: {avg_mark_cloud} км\n"
                    f"  • Нижняя: {low_mark_cloud} км\n\n"

                    f"❄️ Снежный покров (см): {snow_cover if snow_cover else '—'}\n\n"

                    f"🌀 Явления (сроки):\n"
                    f"  • Снег: {case_snow if case_snow else '—'}\n"
                    f"  • Дождь: {case_rain if case_rain else '—'}\n"
                    f"  • Гололёд: {case_black_ice if case_black_ice else '—'}\n"
                    f"  • Туман: {case_fog if case_fog else '—'}\n"
                    f"  • Мгла: {case_mist if case_mist else '—'}\n"
                    f"  • Метель: {case_snowstorm if case_snowstorm else '—'}\n"
                    f"  • Позёмок: {case_snowfall if case_snowfall else '—'}\n"
                    f"  • Торнадо: {case_tornado if case_tornado else '—'}\n"
                    f"  • Пылевая буря: {case_dust_storm if case_dust_storm else '—'}\n"
                    f"  • Пылевой позёмок: {case_dustfall if case_dustfall else '—'}\n"
                    f"  • Гроза: {case_thunderstorm if case_thunderstorm else '—'}\n"
                    f"  • Град: {case_hail if case_hail else '—'}\n"
                )

        return "⚠️ Данных на эту дату нет или они не опубликованы."
    except Exception as e:
        return f"❌ Ошибка при обработке данных: {e}"

check_api_keys()
def main():
    session = login_pik()
    if not session:
        print("Программа завершена из-за ошибки авторизации.")
        return

    try:
        for event in longpoll.listen():
            if event.type == VkBotEventType.MESSAGE_NEW:
                message = event.message
                peer_id = message['peer_id']
                text = message['text'].strip()
                message_text = message['text']

                if peer_id < 2000000000:
                    continue
                
                log(f"Получено сообщение: '{message_text}' от {peer_id}")
                
                match = re.match(r"Факт\.данные\s+(\S+)\s+(\d{2}\.\d{2}\.\d{4})", text)
                if "/start" in message_text or "/help" in message_text:
                    vk.messages.send(
                        peer_id=peer_id,
                        message=
                        "Привет! Я — Метеорологический исследователь. Для навигации используйте команды ниже.\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "🗺️ МЕТЕОГРАММЫ\n\n"
                        "➤ gmap <населённый пункт>\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "📡 ДАННЫЕ С МЕТЕОСТАНЦИИ\n\n"
                        "➤ !погода <станция>\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "🌍 САММАРИ\n\n"
                        "➤ Факт.данные <код станции> <населённый пункт>\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "🔗 СВЯЗЬ С НАМИ\n\n"
                        "➤ Напишите Контакты\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                        random_id=0
                    )
                
                elif "Контакты".lower() in message_text:
                    vk.messages.send(
                        peer_id=peer_id,
                        message="📡 Наши контакты\n\n"
                        "Тех. поддержка: ✉️ meteovrn@inbox.ru\n\n"
                        "YouTube: ▶️ youtube.com/@MeteoVrn\n\n"
                        "Telegram: 📲 t.me/meteovrn\n\n"
                        "ВКонтакте: 🌐 vk.com/meteoexplorer\n\n"
                        "Веб-сайт:  💻  meteovrn.ru\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "Разработчики 👨‍💻\n\n"
                        "Abstrxctive\n"
                        "🔗GitHub: github.com/abstrxctive\n\n"
                        "Aron Sky:\n"
                        "🌐 ВКонтакте: vk.com/6om6a_fantastuk\n"
                        "💬 Telegram: @Andrey179ha",
                        random_id=0
                    )
                    
                elif match:
                    input_id_or_name = match.group(1)
                    date_str = match.group(2)

                    if input_id_or_name.isdigit():
                        station_id = input_id_or_name
                    else:
                        input_name = input_id_or_name.upper()
                        station = next((s for s in city_data if s['name'].upper() == input_name), None)
                        if not station:
                            vk.messages.send(
                                peer_id=peer_id,
                                message="⚠️ Станция не найдена. Проверьте название.",
                                random_id=0
                            )
                            continue
                        station_id = station['code']

                    start_time = time.time()
                    reply = parse_weather_data(session, station_id, date_str)
                    elapsed = time.time() - start_time

                    vk.messages.send(
                        peer_id=peer_id,
                        message=f"{reply}\n\n⏱ Затрачено: {elapsed:.2f} сек.",
                        random_id=0
                    )
                    
                elif 'gmap' in message_text:
            
                    start_time = time.time()
                    weather_city = message_text
                    city_name = weather_city[5:].strip().upper()
                    
                    city_info = next((city for city in city_data if city['eng_name'] == city_name), None)
                    
                    if city_info:
                        city_url = city_info['url']
                        response = requests.get(city_url)
                        
                        if response.status_code == 200:
                            temp_file = f"tmp/temp_{city_name.lower()}.png"
                            
                            with open(temp_file, 'wb') as f:
                                f.write(response.content)
                            
                            attachment = upload_photo(vk_session, temp_file)
                            
                            end_time = time.time()
                            elapsed_time = end_time - start_time
                            
                            vk.messages.send(
                                peer_id=peer_id,
                                message=f"Прогноз на 5 дней для города: {city_name.capitalize()}"
                                        f"\nВремя, затраченное на отправку: {elapsed_time:.2f} секунд",
                                attachment=attachment,
                                random_id=0
                            )
                            
                            os.remove(temp_file)
                        else:
                            vk.messages.send(
                                peer_id=peer_id,
                                message="Ошибка загрузки данных",
                                random_id=0
                            )
                    else:
                        vk.messages.send(
                            peer_id=peer_id,
                            message="Город не найден",
                            random_id=0
                        )

                elif "!погода" in message_text:
                    weather = get_weather(message_text[8:])
                    vk.messages.send(
                        peer_id=peer_id, 
                        message=weather, 
                        random_id=0
                    )
                
    except Exception as e:
        print(f"Ошибка основного цикла: {e}")

if __name__ == "__main__":
    main()
