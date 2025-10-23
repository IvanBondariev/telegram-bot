# Telegram-бот команды профитов

Бот для фиксации профитов, личной и общей статистики, а также связи с админом.

## Возможности
- Отправка заявок на профит в личке, модерация админом.
- Публикация подтверждённых профитов в группе (опционально).
- Личная статистика по периодам: неделя, месяц, всё время.
- Общая статистика в группе по периодам.
- Быстрые кнопки в личке: `Добавить профит`, `Моя статистика`, `Статистика`, `Помощь`, `Предложения по улучшению`.
- Отправка предложений по улучшению администратору.

## Команды
- Личка:
  - `/start` — краткая справка и стартовые кнопки
  - `/profit` — отправить заявку на профит
  - `/cancel` — отменить текущую заявку
  - `/my` — личная статистика
  - `/suggest` — отправить предложение по улучшению
- Группа:
  - `/stats` — сводная статистика; переключение периода через кнопки «За неделю», «За месяц», «За всё время»
- Везде:
  - `/help` — список команд и пояснения

## Быстрые кнопки (личка)
Постоянно доступны под полем ввода: `Добавить профит`, `Моя статистика`, `Статистика`, `Помощь`, `Предложения по улучшению`. Работают даже во время диалога `/profit`.

## Настройка
1. Создайте бота через Telegram `@BotFather` и получите токен.
2. Скопируйте файл `.env.example` в `.env` и заполните переменные:
   ```env
   BOT_TOKEN=123456789:ABCDEF...your_token_here
   ADMIN_ID=123456789               # ID администратора (обязательно)
   GROUP_ID=-100123456789           # ID группы (опционально)
   APPROVED_STICKER_ID=CAACAgIA...  # Стикер для подтверждения в ЛС (опционально)
   GROUP_STICKER_ID_MAMONT=CAACAgIA # Стикер в группу при посте (опционально)
   TIMEZONE=Europe/Warsaw           # Таймзона для формата времени
   ```
3. Создайте и активируйте виртуальное окружение, установите зависимости:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

## Запуск локально
```bash
source .venv/bin/activate
python bot.py
```
Бот начнет опрос (`polling`). Остановить — `Ctrl+C`.

## Развёртывание (Linux, systemd)
- Папка проекта, например: `/opt/telegram-bot`
- Юнит-файл `/etc/systemd/system/telegram-bot.service`:
  ```ini
  [Unit]
  Description=Telegram Bot
  After=network.target

  [Service]
  Type=simple
  WorkingDirectory=/opt/telegram-bot
  EnvironmentFile=/opt/telegram-bot/.env
  ExecStart=/opt/telegram-bot/.venv/bin/python /opt/telegram-bot/bot.py
  Restart=always
  User=bot
  Group=bot

  [Install]
  WantedBy=multi-user.target
  ```
- Команды:
  ```bash
  sudo systemctl daemon-reload
  sudo systemctl enable telegram-bot
  sudo systemctl start telegram-bot
  sudo systemctl status telegram-bot
  ```

## Хранилище и БД
- SQLite-база: `bot.db` в корне проекта.
- Файлы (если используются): `storage/` — не коммитится в репозиторий.
- Бэкап: остановите сервис, скопируйте файл `bot.db`, запустите сервис.

## Структура
- `bot.py` — основной файл с логикой бота
- `db.py` — работа с базой данных
- `fs_storage.py` — файловое хранилище профитов
- `.env` — ваши секреты (не коммитить)
- `.env.example` — пример конфигурации
- `requirements.txt` — зависимости
- `.gitignore` — исключения для репозитория

## Обновление
```bash
git pull
sudo systemctl restart telegram-bot
```

## Дальнейшее развитие
- Дополнительные роли и права, расширение админ-панели
- Вебхуки вместо polling
- Логи, мониторинг, алерты