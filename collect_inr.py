import requests
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from loguru import logger
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Настройка логирования
logger.add("logs/collect_inr.log", rotation="10 MB", retention="3 days")

CURRENCY = "INR"  # Индийская рупия


def create_session():
    """Создание сессии с повторными попытками"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy, pool_connections=10, pool_maxsize=20
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_currency_rate(session, date):
    """Получить курс валюты для конкретной даты с обработкой ошибок"""
    url = (
        f"https://www.cbr-xml-daily.ru/archive/{date.year}/"
        f"{date.month:02d}/{date.day:02d}/daily_json.js"
    )

    for attempt in range(3):
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if "Valute" in data and CURRENCY in data["Valute"]:
                    return data["Valute"][CURRENCY]["Value"]
                return None
            elif response.status_code == 404:
                return None
            else:
                if attempt < 2:
                    time.sleep(1)
                    continue
                return None
        except Exception as e:
            if attempt < 2:
                logger.warning(f"Ошибка для {date.date()}, попытка {attempt + 1}: {e}")
                time.sleep(2)
                continue
            else:
                logger.error(f"Не удалось загрузить {date.date()} после 3 попыток: {e}")
                return None
    return None


def save_temp_data(dates, rates, filename):
    """Сохранить промежуточные данные"""
    temp_df = pd.DataFrame({"date": dates, "rate": rates})
    temp_df.to_csv(filename, index=False, encoding="utf-8")


def collect_all_rates(start_date, end_date, save_every=100):
    """Собрать курсы за период с промежуточным сохранением"""
    all_dates = []
    all_rates = []

    current_date = start_date

    temp_file = "dataset/temp_rates.csv"
    if os.path.exists(temp_file):
        try:
            temp_df = pd.read_csv(temp_file)
            all_dates = pd.to_datetime(temp_df["date"]).tolist()
            all_rates = temp_df["rate"].tolist()
            if all_dates:
                current_date = all_dates[-1] + timedelta(days=1)
                logger.info(
                    f"Загружены промежуточные данные: {len(all_dates)} "
                    f"записей, продолжаем с {current_date.date()}"
                )
        except Exception as e:
            logger.warning(f"Не удалось загрузить временный файл: {e}")

    session = create_session()
    remaining_days = (end_date - current_date).days

    logger.info(f"Начинаем сбор данных с {current_date.date()} по {end_date.date()}")
    print(f"\n📊 Уже собрано: {len(all_dates)} записей")
    print(f"📅 Осталось дней: {remaining_days}")

    with tqdm(total=remaining_days, desc="Загрузка курсов", unit="день") as pbar:
        last_save = 0
        while current_date <= end_date:
            rate = get_currency_rate(session, current_date)

            if rate is not None:
                all_dates.append(current_date)
                all_rates.append(rate)

            current_date += timedelta(days=1)
            pbar.update(1)

            if rate is not None:
                pbar.set_postfix({"Последний курс": f"{rate:.4f}"})

            if len(all_dates) - last_save >= save_every:
                save_temp_data(all_dates, all_rates, temp_file)
                last_save = len(all_dates)

            time.sleep(0.02)

    if os.path.exists(temp_file):
        os.remove(temp_file)

    return all_dates, all_rates


def save_to_csv(dates, rates, filename="dataset/dataset.csv"):
    """Сохранить окончательные данные в CSV файл"""
    df = pd.DataFrame({"date": dates, "rate": rates})

    df.to_csv(filename, index=False, encoding="utf-8")
    logger.success(f"Сохранено {len(dates)} записей в {filename}")
    print(f"\n✅ Сохранено {len(dates)} записей в {filename}")

    if len(dates) > 0:
        print("\n📊 Статистика:")
        print(f"   Минимальный курс: {df['rate'].min():.4f} руб.")
        print(f"   Максимальный курс: {df['rate'].max():.4f} руб.")
        print(f"   Средний курс: {df['rate'].mean():.4f} руб.")
        print(
            f"   Период: с {df['date'].min().date()} " f"по {df['date'].max().date()}"
        )

        with open("dataset/metadata.txt", "w", encoding="utf-8") as f:
            f.write(f"Валюта: {CURRENCY} - Индийская рупия\n")
            f.write(f"Период: {df['date'].min().date()} - {df['date'].max().date()}\n")
            f.write(f"Количество дней: {len(dates)}\n")
            f.write(f"Минимальный курс: {df['rate'].min():.4f}\n")
            f.write(f"Максимальный курс: {df['rate'].max():.4f}\n")
            f.write(f"Средний курс: {df['rate'].mean():.4f}\n")
            f.write(f"Дата создания: {datetime.now()}\n")


def main():
    """Основная функция"""
    print("🚀 Оптимизированный сбор курса индийской рупии (INR)")
    print("=" * 50)

    print("Выберите режим:")
    print("1 - Тестовый режим (7 дней, март 2024)")
    print("2 - Полный сбор (с 2006 года по сегодня)")
    print("3 - Проверить существующий файл")

    choice = input("Ваш выбор (1, 2 или 3): ").strip()

    if choice == "1":
        start_date = datetime(2024, 3, 1)
        end_date = datetime(2024, 3, 7)
        print(f"\n📅 Тестовый режим: с {start_date.date()} по {end_date.date()}")
    elif choice == "2":
        start_date = datetime(2006, 1, 1)
        end_date = datetime.now()
        print(f"\n📅 Полный сбор: с {start_date.date()} по {end_date.date()}")
        print("⏱️ Ориентировочное время: 5-10 минут")
        print("💾 Данные будут сохраняться каждые 100 записей")
    elif choice == "3":
        if os.path.exists("dataset/dataset.csv"):
            df = pd.read_csv("dataset/dataset.csv")
            print("\n✅ Найден файл dataset/dataset.csv")
            print(f"   Записей: {len(df)}")
            print(f"   Период: {df['date'].min()} - {df['date'].max()}")
            print(
                f"   Последнее обновление: "
                f"{datetime.fromtimestamp(os.path.getmtime('dataset/dataset.csv'))}"
            )
        else:
            print("❌ Файл dataset/dataset.csv не найден")
        return
    else:
        print("❌ Неверный выбор. Запустите скрипт снова.")
        return

    print(f"💰 Валюта: {CURRENCY} - Индийская рупия")
    print("=" * 50)

    try:
        requests.get("https://www.cbr-xml-daily.ru", timeout=5)
        print("✅ Интернет-соединение есть")
    except requests.RequestException:
        print("❌ Нет интернет-соединения")
        return

    confirm = input("\nНачать загрузку? (да/нет): ").strip().lower()
    if confirm != "да":
        print("Загрузка отменена.")
        return

    start_time = time.time()
    dates, rates = collect_all_rates(start_date, end_date)
    elapsed_time = time.time() - start_time

    if dates:
        save_to_csv(dates, rates)

        print(f"\n⏱️ Время выполнения: {elapsed_time:.2f} секунд")

        print("\n📋 Примеры записей:")
        for i in range(min(3, len(dates))):
            print(f"   {dates[i].date()}: {rates[i]:.4f} руб.")

        if len(dates) > 3:
            print("   ...")
            for i in range(-3, 0):
                print(f"   {dates[i].date()}: {rates[i]:.4f} руб.")
    else:
        logger.error("Не удалось собрать ни одной записи")
        print("❌ Не удалось собрать данные")


if __name__ == "__main__":
    main()
