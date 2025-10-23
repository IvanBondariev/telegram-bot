# Телеграм-бот (Python)

Минимальный каркас телеграм-бота на Python с командами `/start` и echo-ответом.

## Требования
- Python 3.10+
- Токен бота из BotFather

## Настройка
1. Создайте бота через Telegram `@BotFather` и получите токен.
2. Скопируйте файл `.env.example` в `.env` и вставьте токен:
   ```env
   BOT_TOKEN=123456789:ABCDEF...your_token_here
   ```
3. Создайте и активируйте виртуальное окружение, установите зависимые пакеты:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

## Запуск
```bash
source .venv/bin/activate
python bot.py
```

Бот начнет опрос (`polling`). Остановить — `Ctrl+C`.

## Структура
- `bot.py` — основной файл с логикой бота
- `.env` — ваши секреты (токен), не коммитить
- `.env.example` — пример конфигурации
- `requirements.txt` — зависимости
- `.gitignore` — исключения для репозитория

## Дальнейшее развитие
- Добавить новые команды и обработчики
- Подключить вебхуки вместо polling (при необходимости)
- Логи и мониторинг