import os
import re
import warnings
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
    filters,
    TypeHandler,
    PicklePersistence,
)

# Подавляем депрекейшн-предупреждение от pkg_resources
warnings.filterwarnings("ignore", category=UserWarning, message=".*pkg_resources.*")

from db import init_db, create_profit_request, get_profit, update_final_amount, set_status, get_approved_profits_between, get_all_profits, reset_all_to_rejected, delete_all_profits, get_profits_by_user, reset_user_to_rejected, get_user_ids_by_username, ensure_user_seen, get_user_first_seen
from datetime import datetime, timedelta, timezone
from fs_storage import save_pending_profit, save_approved_profit, save_rejected_profit, purge_storage, purge_approved_and_pending, remove_files_for_profit_id
from filelock import FileLock
from zoneinfo import ZoneInfo

# Загружаем переменные окружения из .env
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID_STR = os.getenv("GROUP_ID")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
APPROVED_STICKER_ID = os.getenv("APPROVED_STICKER_ID")

GROUP_ID = int(GROUP_ID_STR) if GROUP_ID_STR else None
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR else None

if not BOT_TOKEN:
    raise RuntimeError("Переменная BOT_TOKEN не задана. Установите её в .env")

# Состояния диалога /profit
ASK_AMOUNT = 1
# Состояния диалога предложений
SUGGEST_WAIT_TEXT = 2


def fmt_uah(value: float) -> str:
    s = format(value, ",.2f")  # "1,234.50"
    s = s.replace(",", " ").replace(".", ",")
    int_part, sep, frac = s.partition(",")
    if frac:
        frac = frac.rstrip("0")
        s = f"{int_part},{frac}" if frac else int_part
    return f"{s} ₴"

# Унифицированные клавиатуры
def make_period_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("За неделю", callback_data=f"{prefix}:week"),
            InlineKeyboardButton("За месяц", callback_data=f"{prefix}:month"),
            InlineKeyboardButton("За всё время", callback_data=f"{prefix}:all"),
        ]
    ])


def make_start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Отправить профит", callback_data="start_profit"),
            InlineKeyboardButton("Моя статистика", callback_data="start:my"),
        ],
        [
            InlineKeyboardButton("Статистика", callback_data="start:stats"),
            InlineKeyboardButton("Помощь", callback_data="start:help"),
        ],
        [
            InlineKeyboardButton("Предложения по улучшению", callback_data="start:suggest"),
        ],
    ])


def make_admin_moderation_keyboard(req_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Подтвердить", callback_data=f"approve:{req_id}"),
            InlineKeyboardButton("Отклонить", callback_data=f"reject:{req_id}"),
        ],
        [
            InlineKeyboardButton("Изменить сумму", callback_data=f"edit:{req_id}"),
        ],
    ])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Фиксируем пользователя в БД (дата присоединения)
    user = update.effective_user
    ensure_user_seen(user.id, user.username, user.first_name)

    is_admin = update.effective_user.id == ADMIN_ID
    intro = (
        "Привет! Добро пожаловать в команду.\n"
        "Мы растём вместе, делимся результатами и поддерживаем дисциплину.\n"
        "Я помогу фиксировать профиты, следить за прогрессом и держать статистику прозрачной.\n\n"
        "Ниже — краткая подсказка по командам:"
    )
    common = (
        "\n\nКоманды для всех:\n"
        "• /profit — отправить заявку на профит (только в личке)\n"
        "• /my — личная статистика (только в личке)\n"
        "• /stats — показать сводную статистику (только в группе)\n"
        "• /help — подсказка по командам"
    )
    if is_admin:
        admin = (
            "\n\nКоманды администратора:\n"
            "• /reset_profits — аннулировать все профиты\n"
            "• /reset_user_profits <user_id или @username> — аннулировать профиты пользователя"
        )
        text = intro + common + admin
    else:
        note = "\n\nПримечание: админские команды скрыты и доступны только администраторам."
        text = intro + common + note
    # Показать закреплённые кнопки под полем ввода (reply keyboard)
    if update.message and update.effective_chat.type == "private":
        reply_kb = ReplyKeyboardMarkup(
            [
                [KeyboardButton("Добавить профит"), KeyboardButton("Моя статистика")],
                [KeyboardButton("Помощь")],
                [KeyboardButton("Предложения по улучшению")],
            ],
            resize_keyboard=True,
        )
        try:
            await update.message.reply_text("Быстрые кнопки доступны всегда:", reply_markup=reply_kb)
        except Exception:
            pass
    keyboard = make_start_keyboard()
    await update.message.reply_text(text, reply_markup=keyboard)


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Анти-спам: блокируем повторные ответы в одном чате на 3 секунды
    now = datetime.now().timestamp()
    last_ts = context.chat_data.get("stats_cooldown")
    if last_ts and now - last_ts < 3:
        return
    context.chat_data["stats_cooldown"] = now

    # Определяем период по умолчанию — неделю
    period = "week"
    text = build_stats_text(period)
    keyboard = make_period_keyboard("stats")
    if update.message:
        await update.message.reply_text(text, reply_markup=keyboard)
    else:
        # На случай, если stats() будет вызван из callback
        query = update.callback_query
        await context.bot.send_message(chat_id=query.message.chat.id, text=text, reply_markup=keyboard)


def _period_bounds(period: str):
    now = datetime.utcnow()
    if period == "week":
        start = now - timedelta(days=7)
        return start.isoformat(), None
    elif period == "month":
        start = now - timedelta(days=30)
        return start.isoformat(), None
    else:
        return None, None


def build_stats_text(period: str) -> str:
    start_iso, end_iso = _period_bounds(period)
    rows = get_approved_profits_between(start_iso, end_iso)
    if not rows:
        title = "Статистика за неделю" if period == "week" else ("Статистика за месяц" if period == "month" else "Статистика за всё время")
        return f"{title}\n\nЗа выбранный период нет подтверждённых профитов."

    # Агрегация по пользователям
    agg = {}
    total = 0.0
    for user_id, username, first_name, final_amount, approved_at in rows:
        if final_amount is None:
            continue
        total += final_amount
        name = f"@{username}" if username else (first_name or str(user_id))
        entry = agg.get(user_id)
        if not entry:
            agg[user_id] = {"name": name, "sum": final_amount, "count": 1}
        else:
            entry["sum"] += final_amount
            entry["count"] += 1

    # Сортировка по сумме по убыванию
    items = sorted(agg.values(), key=lambda x: x["sum"], reverse=True)

    title = "Статистика за неделю" if period == "week" else ("Статистика за месяц" if period == "month" else "Статистика за всё время")
    lines = [title, ""]
    prev_sum = None
    prev_rank = 0
    idx = 0
    for item in items:
        idx += 1
        cur_sum = round(item['sum'], 2)
        if prev_sum is None or cur_sum != prev_sum:
            rank = idx
            prev_rank = rank
            prev_sum = cur_sum
        else:
            rank = prev_rank
        # Золото/серебро/бронза без номера для топ‑3, дальше — номера
        medal = "🥇" if rank == 1 else ("🥈" if rank == 2 else ("🥉" if rank == 3 else ""))
        if medal:
            prefix = f"{medal}"
        else:
            prefix = f"{rank}."
        lines.append(f"{prefix} {item['name']} — {fmt_uah(item['sum'])}")
    lines.append("")
    lines.append(f"Итого: {fmt_uah(total)}")
    return "\n".join(lines)


async def profit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Фиксируем пользователя в БД (дата присоединения)
    user = update.effective_user
    ensure_user_seen(user.id, user.username, user.first_name)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏰ Поставить текущее время", callback_data="profit_set_time")],
        [InlineKeyboardButton("❌ Отменить", callback_data="profit_cancel")]
    ])
    prompt_text = (
        "💰 <b>Добавление профита</b>\n\n"
        "📝 Укажите сумму профита в гривнах:\n"
        "• Только числовое значение\n"
        "• Можно использовать десятичные дроби\n\n"
        "📋 <b>Примеры:</b>\n"
        "• <code>1000</code>\n"
        "• <code>1500.50</code>\n"
        "• <code>1 500</code>\n\n"
        "⚡️ Для быстрого ввода можете отметить текущее время"
    )
    # Поддержка входа как по команде, так и по callback
    if update.message:
        msg = await update.message.reply_text(
            prompt_text,
            parse_mode='HTML',
            disable_notification=True,
            reply_markup=keyboard,
        )
        chat_id = update.effective_chat.id
    else:
        query = update.callback_query
        await query.answer()
        msg = await context.bot.send_message(
            chat_id=query.message.chat.id,
            text=prompt_text,
            parse_mode='HTML',
            disable_notification=True,
            reply_markup=keyboard,
        )
        chat_id = query.message.chat.id
    # Сохраняем id сообщения для компактного режима (редактирование вместо новых сообщений)
    context.user_data["profit_session_message_id"] = msg.message_id
    context.user_data["profit_chat_id"] = chat_id
    return ASK_AMOUNT


async def profit_cancel_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Обновляем сообщение запроса суммы и завершаем диалог
    try:
        await query.edit_message_text(
            "Заявка отменена. Если передумаете — отправьте новую командой /profit."
        )
    except Exception:
        try:
            await context.bot.send_message(
                chat_id=query.message.chat.id,
                text="Заявка отменена. Если передумаете — отправьте новую командой /profit.",
            )
        except Exception:
            pass
    context.user_data.pop("profit_session_message_id", None)
    return ConversationHandler.END


async def profit_set_time_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Сохраняем отметку времени в UTC
    context.user_data["profit_time_label"] = datetime.utcnow().isoformat()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏰ Поставить текущее время", callback_data="profit_set_time")],
        [InlineKeyboardButton("❌ Отменить", callback_data="profit_cancel")]
    ])
    prompt_text = (
        "💰 <b>Добавление профита</b>\n\n"
        "✅ <b>Время отмечено!</b>\n\n"
        "📝 Укажите сумму профита в гривнах:\n"
        "• Только числовое значение\n"
        "• Можно использовать десятичные дроби\n\n"
        "📋 <b>Примеры:</b>\n"
        "• <code>1000</code>\n"
        "• <code>1500.50</code>\n"
        "• <code>1 500</code>\n\n"
        "⚡️ Для быстрого ввода можете отметить текущее время"
    )
    try:
        await query.edit_message_text(text=prompt_text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        try:
            await context.bot.send_message(chat_id=query.message.chat.id, text=prompt_text, parse_mode='HTML', reply_markup=keyboard)
        except Exception:
            pass
    return ASK_AMOUNT

async def profit_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Авто-таймаут: аккуратно завершаем диалог, чистим состояние
    session_msg_id = context.user_data.get("profit_session_message_id")
    chat_id = context.user_data.get("profit_chat_id") or (update.effective_chat.id if update and update.effective_chat else None)
    text = "Время на ввод истекло. Заявка отменена. Чтобы начать заново, используйте /profit."
    if session_msg_id and chat_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=session_msg_id, text=text)
        except Exception:
            try:
                await context.bot.send_message(chat_id=chat_id, text=text)
            except Exception:
                pass
    elif chat_id:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            pass
    context.user_data.pop("profit_session_message_id", None)
    context.user_data.pop("profit_chat_id", None)
    return ConversationHandler.END


async def profit_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Отмена через команду /cancel
    session_msg_id = context.user_data.get("profit_session_message_id")
    chat_id = context.user_data.get("profit_chat_id") or (update.effective_chat.id if update and update.effective_chat else None)
    text = "Заявка отменена. Чтобы начать заново, используйте /profit."
    if session_msg_id and chat_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=session_msg_id, text=text)
        except Exception:
            try:
                await context.bot.send_message(chat_id=chat_id, text=text)
            except Exception:
                pass
    elif chat_id:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            pass
    context.user_data.pop("profit_session_message_id", None)
    context.user_data.pop("profit_chat_id", None)
    return ConversationHandler.END


async def profit_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    session_msg_id = context.user_data.get("profit_session_message_id")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отменить", callback_data="profit_cancel")]
    ])
    # Ищем первое число (целое или десятичное), допускаем пробелы как разделители тысяч
    match = re.search(r"(\d[\d\s]*([\.,]\d{1,2})?)", text)
    if not match:
        error_text = (
            "❌ <b>Ошибка ввода</b>\n\n"
            "🔍 Не удалось распознать сумму в вашем сообщении.\n\n"
            "📋 <b>Правильные примеры:</b>\n"
            "• <code>1500</code>\n"
            "• <code>2000.50</code>\n"
            "• <code>1 500</code>\n\n"
            "💡 Попробуйте ещё раз или отмените операцию"
        )
        if session_msg_id:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=session_msg_id,
                text=error_text,
                parse_mode='HTML',
                reply_markup=keyboard,
            )
        else:
            await update.message.reply_text(
                error_text,
                parse_mode='HTML',
                reply_markup=keyboard,
            )
        # Пытаемся удалить пользовательское сообщение, чтобы не засорять чат
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
        except Exception:
            pass
        return ASK_AMOUNT

    amount_str = match.group(1).replace(" ", "").replace(',', '.')
    try:
        amount = float(amount_str)
    except ValueError:
        error_text = (
            "❌ <b>Ошибка формата</b>\n\n"
            "🔢 Некорректное числовое значение.\n\n"
            "📋 <b>Правильные примеры:</b>\n"
            "• <code>1500</code>\n"
            "• <code>2000.50</code>\n"
            "• <code>1 500</code>\n\n"
            "💡 Попробуйте ещё раз или отмените операцию"
        )
        if session_msg_id:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=session_msg_id,
                text=error_text,
                parse_mode='HTML',
                reply_markup=keyboard,
            )
        else:
            await update.message.reply_text(
                error_text,
                parse_mode='HTML',
                reply_markup=keyboard,
            )
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
        except Exception:
            pass
        return ASK_AMOUNT

    if amount <= 0:
        error_text = (
            "❌ <b>Некорректная сумма</b>\n\n"
            "⚠️ Сумма профита должна быть больше нуля.\n\n"
            "📋 <b>Правильные примеры:</b>\n"
            "• <code>1500</code>\n"
            "• <code>2000.50</code>\n"
            "• <code>1 500</code>\n\n"
            "💡 Попробуйте ещё раз или отмените операцию"
        )
        if session_msg_id:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=session_msg_id,
                text=error_text,
                parse_mode='HTML',
                reply_markup=keyboard,
            )
        else:
            await update.message.reply_text(error_text, parse_mode='HTML', reply_markup=keyboard)
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
        except Exception:
            pass
        return ASK_AMOUNT

    amount = round(amount, 2)

    note = text
    user = update.effective_user

    # Создаём заявку в БД
    profit_id = create_profit_request(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        amount=amount,
        note=note,
    )

    # Сохраняем в файловое хранилище как pending
    row = get_profit(profit_id)
    if row:
        save_pending_profit(row)

    # Отправляем админу/в группу заявку с кнопками
    admin_keyboard = make_admin_moderation_keyboard(profit_id)
    name = f"@{user.username}" if user.username else (user.first_name or str(user.id))
    time_iso = context.user_data.get("profit_time_label") or (row[8] if row else None)
    time_str = f" • время: {format_time_local(time_iso)}" if time_iso else ""
    admin_text = f"Новый профит от {name}: {fmt_uah(amount)}{time_str}"
    target_sent = False
    # Сначала пытаемся отправить в личку админу
    if ADMIN_ID:
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=admin_text, reply_markup=admin_keyboard)
            target_sent = True
        except Exception:
            pass
    # Если не получилось — отправляем в группу (fallback)
    if not target_sent and GROUP_ID:
        try:
            await context.bot.send_message(chat_id=GROUP_ID, text=admin_text, reply_markup=admin_keyboard)
            target_sent = True
        except Exception:
            pass
    # Если никуда не удалось отправить — предупредим пользователя
    if not target_sent:
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Внимание: не удалось уведомить администратора. Проверьте настройки GROUP_ID/ADMIN_ID.")
        except Exception:
            pass

    # Уведомляем пользователя (редактируем предыдущее сообщение, чтобы не плодить новые)
    if session_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=session_msg_id,
                text=f"Заявка на профит {fmt_uah(amount)} отправлена администратору на проверку.",
            )
        except Exception:
            # Если не удалось отредактировать, отправим новое
            await update.message.reply_text(
                f"Заявка на профит {fmt_uah(amount)} отправлена администратору на проверку."
            )
    else:
        await update.message.reply_text(
            f"Заявка на профит {fmt_uah(amount)} отправлена администратору на проверку."
        )

    # Чистим состояние
    context.user_data.pop("profit_session_message_id", None)
    context.user_data.pop("profit_chat_id", None)
    context.user_data.pop("profit_time_label", None)
    return ConversationHandler.END


async def admin_edit_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Если админ редактирует заявку, мы ожидаем id в контексте
    editing_id = context.user_data.get("editing_request_id")
    if not editing_id:
        return
    text = update.message.text.strip()
    # Парсим число аналогично
    match = re.match(r"^([0-9]+(?:[\s,\.][0-9]{3})*(?:[\.,][0-9]+)?|[0-9]+)$", text)
    if not match:
        await update.message.reply_text("Некорректное число. Введите заново.")
        return
    amount_str = match.group(1).replace(" ", "").replace(',', '.')
    try:
        amount = float(amount_str)
    except ValueError:
        await update.message.reply_text("Некорректное число. Введите заново.")
        return
    amount = round(amount, 2)

    update_final_amount(editing_id, amount)
    row = get_profit(editing_id)
    if row:
        save_approved_profit(row)

    context.user_data.pop("editing_request_id", None)

    keyboard = make_admin_moderation_keyboard(editing_id)
    await update.message.reply_text(
        f"Заявка #{editing_id} обновлена. Итоговая сумма: {fmt_uah(amount)}",
        reply_markup=keyboard,
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # Маршрутизация стартового экрана
    if data == "start_profit":
        await profit_command(update, context)
        return
    if data == "start:stats":
        msg = "Статистика доступна только в группе: используйте /stats там"
        try:
            await context.bot.send_message(chat_id=query.message.chat.id, text=msg)
        except Exception:
            await query.edit_message_text(text=msg)
        return
    if data == "start:help":
        await help_command(update, context)
        return
    if data == "start:my":
        await my_command(update, context)
        return
    # Удалено: if data == "start:suggest": await suggest_start_conv(update, context); return

    # Статистика: периоды
    if data.startswith("stats:"):
        period = data.split(":", 1)[1]
        text = build_stats_text(period)
        keyboard = make_period_keyboard("stats")
        try:
            await query.edit_message_text(text=text, reply_markup=keyboard)
        except Exception:
            # Если редактирование недоступно, не отправляем дубликаты
            pass
        return

    # Моя статистика: периоды
    if data.startswith("my:"):
        period = data.split(":", 1)[1]
        user_id = update.effective_user.id
        text = build_my_text(user_id, period)
        keyboard = make_period_keyboard("my")
        try:
            await query.edit_message_text(text=text, reply_markup=keyboard)
        except Exception:
            # Если редактирование недоступно, не отправляем дубликаты
            pass
        return

    # Одобрение/отклонение админом
    if data.startswith("approve:") or data.startswith("reject:") or data.startswith("edit:"):
        # Только админ
        if update.effective_user.id != ADMIN_ID:
            try:
                await query.answer(text="Только администратор может выполнять это действие.", show_alert=True)
            except Exception:
                pass
            return
        action, profit_id_str = data.split(":", 1)
        try:
            profit_id = int(profit_id_str)
        except ValueError:
            await context.bot.send_message(chat_id=query.message.chat.id, text="Некорректный идентификатор профита.")
            return

        if action == "edit":
            # Переводим администратора в режим редактирования суммы (в личке)
            context.user_data["editing_request_id"] = profit_id
            try:
                await context.bot.send_message(chat_id=update.effective_user.id, text=f"Введите новую сумму для профита в личном чате.")
            except Exception:
                # На случай, если нельзя написать в личку, подскажем в текущем чате
                await context.bot.send_message(chat_id=query.message.chat.id, text="Откройте личный чат с ботом и введите новую сумму.")
            return

        # Общие данные заявки
        row = get_profit(profit_id)
        if not row:
            await context.bot.send_message(chat_id=query.message.chat.id, text=f"Профит не найден.")
            return
        user_id = row[1]
        final_amount = row[5] or row[4] or 0.0

        if action == "approve":
            # Обновляем статус и файловое хранилище
            set_status(profit_id, "approved", approver_id=update.effective_user.id)
            row2 = get_profit(profit_id)
            try:
                save_approved_profit(row2)
            except Exception:
                pass

            # Уведомляем пользователя в личке
            dm_text = f"Ваш профит подтверждён: {fmt_uah(final_amount)} 🎉 Отличная работа!"
            try:
                await context.bot.send_message(chat_id=user_id, text=dm_text)
                if APPROVED_STICKER_ID:
                    try:
                        await context.bot.send_sticker(chat_id=user_id, sticker=APPROVED_STICKER_ID)
                    except Exception:
                        pass
            except Exception:
                pass


            # Обновляем сообщение для админа/группы
            name = f"@{row[2]}" if row[2] else (row[3] or str(user_id))
            time_str = f" • время: {format_time_local(row[8])}" if row and row[8] else ""
            admin_text = f"✅ Подтверждён профит {fmt_uah(final_amount)} от {name}{time_str}"
            try:
                await query.edit_message_text(text=admin_text)
            except Exception:
                await context.bot.send_message(chat_id=query.message.chat.id, text=admin_text)
            
            # Публикуем итог в группе
            try:
                if GROUP_ID:
                    name = f"@{row[2]}" if row[2] else (row[3] or str(user_id))
                    group_text = f"💸 Плюс профит от {name}: {fmt_uah(final_amount)} — красавец!"
                    await context.bot.send_message(chat_id=GROUP_ID, text=group_text)

                    sticker_id = os.getenv('GROUP_STICKER_ID_MAMONT', '').strip()
                    if sticker_id:
                        try:
                            await context.bot.send_sticker(chat_id=GROUP_ID, sticker=sticker_id)
                        except Exception as e:
                            print(f"[warn] Failed to send sticker to group: {e}")

                    follow_up = "🦣 Мамонт в ловушке! Это был отличный залив, но нужно ещё. Продолжаем охоту! 🪤"
                    await context.bot.send_message(chat_id=GROUP_ID, text=follow_up)
            except Exception:
                pass
            return

        if action == "reject":
            set_status(profit_id, "rejected", approver_id=update.effective_user.id)
            row2 = get_profit(profit_id)
            try:
                save_rejected_profit(row2)
            except Exception:
                pass

            # Уведомляем пользователя в личке об отклонении
            try:
                dm_text = f"Ваш профит отклонён: {fmt_uah(final_amount)}."
                await context.bot.send_message(chat_id=user_id, text=dm_text)
            except Exception:
                pass

            # Обновляем сообщение для админа/группы без номера заявки и с временем
            name = f"@{row[2]}" if row[2] else (row[3] or str(user_id))
            time_str = f" • время: {format_time_local(row[8])}" if row and row[8] else ""
            admin_text = f"❌ Отклонён профит {fmt_uah(final_amount)} от {name}{time_str}"
            try:
                await query.edit_message_text(text=admin_text)
            except Exception:
                await context.bot.send_message(chat_id=query.message.chat.id, text=admin_text)
            return


async def reset_profits_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Только админ
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Эта команда доступна только администратору.")
        return

    # Переводим все в rejected
    reset_all_to_rejected()
    purge_approved_and_pending()
    await update.message.reply_text("Все заявки переведены в отклонённые.")


async def reset_user_profits_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Только админ
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Эта команда доступна только администратору.")
        return

    args = getattr(context, "args", [])
    if not args:
        await update.message.reply_text("Использование: /reset_user_profits <user_id или @username>")
        return

    arg = args[0].strip()
    target_user_id = None
    target_username = None

    if arg.startswith("@"):
        target_username = arg[1:]
        ids = get_user_ids_by_username(target_username)
        if not ids:
            await update.message.reply_text(f"Не найдено профитов для пользователя @{target_username}.")
            return
        target_user_id = ids[0]
    else:
        try:
            target_user_id = int(arg)
        except ValueError:
            await update.message.reply_text("Укажите корректный идентификатор пользователя (число) или @username.")
            return

    rows = get_profits_by_user(target_user_id)
    if not rows:
        await update.message.reply_text("У пользователя нет заявок на профит.")
        return

    # Переводим всё в rejected в БД
    reset_user_to_rejected(target_user_id)

    # Чистим файловое хранилище
    for row in rows:
        profit_id = row[0]
        remove_files_for_profit_id(profit_id)

    await update.message.reply_text("Профиты пользователя аннулированы.")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_admin = update.effective_user.id == ADMIN_ID
    is_private = update.effective_chat.type == "private"
    lines = ["Подсказка по командам:"]
    if is_private:
        lines += [
            "",
            "Личные сообщения:",
            "• /start — краткая справка и начальный экран",
            "• /profit — отправить заявку на профит",
            "• /cancel — отменить текущую заявку",
            "• /my — личная статистика",
            "• /suggest — отправить предложение по улучшению",
        ]
        if is_admin:
            lines += [
                "",
                "Команды администратора:",
                "• /reset_profits — аннулировать все профиты",
                "• /reset_user_profits <user_id или @username> — аннулировать профиты пользователя",
            ]
        lines += [
            "",
            "Статистика доступна только в группе: используйте /stats там",
        ]
    else:
        lines += [
            "",
            "Группа:",
            "• /stats — показать сводную статистику; используйте кнопки ‘За неделю’, ‘За месяц’, ‘За всё время’",
        ]
    text = "\n".join(lines)
    if update.message:
        await update.message.reply_text(text)
    else:
        query = update.callback_query
        try:
            await context.bot.send_message(chat_id=query.message.chat.id, text=text)
        except Exception:
                try:
                    await query.edit_message_text(text=text)
                except Exception:
                    pass


async def suggest_start_conv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user_seen(user.id, user.username, user.first_name)
    prompt = "Опишите ваше предложение по улучшению одним сообщением. Для отмены — /cancel."
    if update.message:
        await update.message.reply_text(prompt)
    else:
        query = update.callback_query
        await query.answer()
        try:
            await context.bot.send_message(chat_id=query.message.chat.id, text=prompt)
        except Exception:
            try:
                await query.edit_message_text(text=prompt)
            except Exception:
                pass
    return SUGGEST_WAIT_TEXT

async def suggest_receive_conv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    user = update.effective_user
    if not text:
        await update.message.reply_text("Пустое сообщение. Напишите текст предложения.")
        return SUGGEST_WAIT_TEXT
    admin_text = (
        f"Предложение по улучшению:\n"
        f"От: {user.first_name or ''} (@{user.username or '—'}, id={user.id})\n\n"
        f"{text}"
    )
    if ADMIN_ID:
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=admin_text)
        except Exception:
            pass
    await update.message.reply_text("Спасибо! Ваше предложение отправлено администратору.")
    return ConversationHandler.END

async def suggest_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END

async def suggest_start_inside_profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_suggestion"] = True
    prompt = "Опишите ваше предложение по улучшению одним сообщением. После этого вернёмся к вводу суммы профита."
    await update.message.reply_text(prompt)
    return ASK_AMOUNT

async def route_private_text_in_profit_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting_suggestion"):
        text = (update.message.text or "").strip()
        user = update.effective_user
        context.user_data["awaiting_suggestion"] = False
        if not text:
            await update.message.reply_text("Пустое сообщение. Напишите текст предложения.")
            context.user_data["awaiting_suggestion"] = True
            return ASK_AMOUNT
        admin_text = (
            f"Предложение по улучшению:\n"
            f"От: {user.first_name or ''} (@{user.username or '—'}, id={user.id})\n\n"
            f"{text}"
        )
        if ADMIN_ID:
            try:
                await context.bot.send_message(chat_id=ADMIN_ID, text=admin_text)
            except Exception:
                pass
        await update.message.reply_text("Спасибо! Ваше предложение отправлено администратору. Вернёмся к заявке на профит.")
        return ASK_AMOUNT
    return await profit_receive(update, context)


async def stats_private_notice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "Статистика доступна только в группе: используйте /stats там"
    if update.message:
        await update.message.reply_text(msg)
    else:
        query = update.callback_query
        try:
            await context.bot.send_message(chat_id=query.message.chat.id, text=msg)
        except Exception:
            try:
                await query.edit_message_text(text=msg)
            except Exception:
                pass


def _period_bounds(period: str):
    now = datetime.utcnow()
    if period == "week":
        start = now - timedelta(days=7)
        return start.isoformat(), now.isoformat()
    elif period == "month":
        start = now - timedelta(days=30)
        return start.isoformat(), now.isoformat()
    else:
        return None, None


def build_my_text(user_id: int, period: str) -> str:
    start_iso, end_iso = _period_bounds(period)
    rows = get_profits_by_user(user_id)
    # Информация о присоединении
    first_seen_iso = get_user_first_seen(user_id)
    join_line = None
    if first_seen_iso:
        try:
            dt0 = datetime.fromisoformat(first_seen_iso)
            days = (datetime.utcnow() - dt0).days
            date_str = dt0.strftime("%Y-%m-%d")
            join_line = f"Дата присоединения: {date_str} • всего в боте: {days} дн."
        except Exception:
            pass
    # Фильтруем по подтверждённым и по периоду approved_at
    approved_rows = []
    for row in rows:
        status = row[7]
        approved_at = row[9]
        final_amount = row[5]
        if status != "approved" or final_amount is None:
            continue
        if start_iso and approved_at and approved_at < start_iso:
            continue
        if end_iso and approved_at and approved_at > end_iso:
            continue
        approved_rows.append(row)
    title = (
        "Моя статистика за неделю" if period == "week"
        else ("Моя статистика за месяц" if period == "month" else "Моя статистика за всё время")
    )
    lines = [title, ""]
    if join_line:
        lines.append(join_line)
        lines.append("")
    if not approved_rows:
        lines.append("За выбранный период нет подтверждённых профитов.")
        return "\n".join(lines)
    total = 0.0
    for row in approved_rows:
        total += row[5] or 0.0
    # Топ-5 по сумме
    top = sorted(approved_rows, key=lambda r: (r[5] or 0.0), reverse=True)[:5]
    lines.append(f"Итого подтверждено: {fmt_uah(total)}")
    lines.append("Топ-5 профитов:")
    for idx, r in enumerate(top, start=1):
        amt = r[5] or 0.0
        dt = r[9]
        # Форматируем дату: только дата без времени
        date_str = dt
        if dt:
            try:
                dt_obj = datetime.fromisoformat(dt.replace('Z', '+00:00'))
                date_str = dt_obj.strftime("%Y-%m-%d")
            except Exception:
                pass
        lines.append(f"{idx}. {fmt_uah(amt)} — {date_str}")
    return "\n".join(lines)


async def my_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Фиксируем пользователя в БД (дата присоединения)
    user = update.effective_user
    ensure_user_seen(user.id, user.username, user.first_name)
    user_id = update.effective_user.id
    rows = get_profits_by_user(user_id)
    if not rows:
        msg = "У вас пока нет заявок на профит."
        if update.message:
            await update.message.reply_text(msg)
        else:
            query = update.callback_query
            try:
                await context.bot.send_message(chat_id=query.message.chat.id, text=msg)
            except Exception:
                try:
                    await query.edit_message_text(text=msg)
                except Exception:
                    pass
        return
    # По умолчанию показываем за всё время и даём кнопки переключения периода
    text = build_my_text(user_id, period="all")
    keyboard = make_period_keyboard("my")
    if update.message:
        await update.message.reply_text(text, reply_markup=keyboard)
    else:
        query = update.callback_query
        try:
            await context.bot.send_message(chat_id=query.message.chat.id, text=text, reply_markup=keyboard)
        except Exception:
            try:
                await query.edit_message_text(text=text, reply_markup=keyboard)
            except Exception:
                pass


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Простой echo-ответ на любые текстовые сообщения
    await update.message.reply_text(update.message.text)


def format_time_local(iso_str: str) -> str:
    # Преобразуем ISO-дату (UTC) в локальное время и форматируем
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_tz = ZoneInfo(os.getenv("TIMEZONE", "Europe/Warsaw"))
        local_dt = dt.astimezone(local_tz)
        return local_dt.strftime("%d.%m %H:%M")
    except Exception:
        try:
            local_dt = datetime.now(ZoneInfo(os.getenv("TIMEZONE", "Europe/Warsaw")))
            return local_dt.strftime("%d.%m %H:%M")
        except Exception:
            return datetime.utcnow().strftime("%d.%m %H:%M") + " UTC"


def main() -> None:
    # Гарантируем один экземпляр через lock-файл
    lock_path = os.path.join(os.path.dirname(__file__), "bot.lock")
    with FileLock(lock_path):
        init_db()

        # Персистентность состояния и диалогов
        state_path = os.path.join(os.path.dirname(__file__), "bot_state.pkl")
        persistence = PicklePersistence(filepath=state_path)
        application = Application.builder().token(BOT_TOKEN).persistence(persistence).build()

        # Диалог /profit (persistent)
        profit_conv = ConversationHandler(
            entry_points=[
                CommandHandler("profit", profit_command, filters=filters.ChatType.PRIVATE),
                CallbackQueryHandler(profit_command, pattern="^start_profit$"),
                MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Добавить профит$"), profit_command),
            ],
            states={
                ASK_AMOUNT: [
                    MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Добавить профит$"), profit_command),
                    MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Моя статистика$"), my_command),
                    MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Помощь$"), help_command),
                    MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Статистика$"), stats_private_notice),
                    MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Предложения по улучшению$"), suggest_start_inside_profit),
                    MessageHandler(
                        filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
                        route_private_text_in_profit_dialog,
                    ),
                    CallbackQueryHandler(profit_cancel_button, pattern="^profit_cancel$"),
                    CallbackQueryHandler(profit_set_time_button, pattern="^profit_set_time$"),
                    # Удалено: CallbackQueryHandler(handle_callback, pattern="^(start:(help|stats|my|suggest)|stats:(week|month|all)|my:(week|month|all))$")
                ],
                ConversationHandler.TIMEOUT: [TypeHandler(Update, profit_timeout)],
            },
            fallbacks=[
                CommandHandler("cancel", profit_cancel, filters=filters.ChatType.PRIVATE),
                CallbackQueryHandler(profit_cancel_button, pattern="^profit_cancel$"),
            ],
            conversation_timeout=600,
            name="profit",
            persistent=True,
            per_message=True,
        )

        # Диалог предложений (persistent)
        suggest_conv = ConversationHandler(
            entry_points=[
                CommandHandler("suggest", suggest_start_conv, filters=filters.ChatType.PRIVATE),
                CallbackQueryHandler(suggest_start_conv, pattern="^start:suggest$"),
                MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Предложения по улучшению$"), suggest_start_conv),
            ],
            states={
                SUGGEST_WAIT_TEXT: [
                    MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, suggest_receive_conv),
                    CommandHandler("cancel", suggest_cancel_command),
                ]
            },
            fallbacks=[
                CommandHandler("cancel", suggest_cancel_command),
            ],
            conversation_timeout=600,
            name="suggest",
            persistent=True,
            per_message=True,
        )

        # Регистрируем обработчики команд и сообщений
        application.add_handler(CommandHandler("start", start, filters=filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("stats", stats, filters=filters.ChatType.GROUPS))
        application.add_handler(CommandHandler("reset_profits", reset_profits_command, filters=filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("reset_user_profits", reset_user_profits_command, filters=filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("my", my_command, filters=filters.ChatType.PRIVATE))
        application.add_handler(profit_conv)
        application.add_handler(suggest_conv)
        # Reply-кнопки в личке: Моя статистика, Помощь, Статистика
        application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Моя статистика$"), my_command))
        application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Помощь$"), help_command))
        application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Статистика$"), stats_private_notice))
        application.add_handler(CallbackQueryHandler(handle_callback))
        application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, admin_edit_amount))
        application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, echo))

        print("Бот запущен. Нажмите Ctrl+C для остановки.")
        application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()