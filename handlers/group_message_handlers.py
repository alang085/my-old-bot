"""群组消息管理处理器"""

# 标准库
import logging

# 第三方库
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

# 本地模块
from decorators import admin_required, error_handler, private_chat_only
from services.group_message_service import GroupMessageService

logger = logging.getLogger(__name__)


@error_handler
@admin_required
async def get_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Get current group/channel ID (use in group or channel)"""
    chat = update.effective_chat

    if chat.type == "private":
        await update.message.reply_text("❌ This command can only be used in groups or channels")
        return

    # Determine if it's a channel or group
    chat_type = "Channel" if chat.type == "channel" else "Group"

    await update.message.reply_text(
        f"📌 {chat_type} Info\n\n"
        f"{chat_type} Name: {chat.title}\n"
        f"{chat_type} ID: `{chat.id}`\n\n"
        f"Use /groupmsg_setup in this group/channel to enable automatic messages",
        parse_mode="Markdown",
    )


@error_handler
@admin_required
async def setup_group_auto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Enable automatic group/channel messages (use in group or channel)"""
    chat = update.effective_chat

    if chat.type == "private":
        await update.message.reply_text("❌ This command can only be used in groups or channels")
        return

    # Determine if it's a channel or group
    chat_type = "Channel" if chat.type == "channel" else "Group"
    chat_id = chat.id
    chat_title = chat.title or "Not set"

    try:
        # Use Service to setup group auto messages
        success, error_msg = await GroupMessageService.setup_group_auto(chat_id, chat_title)

        if success:
            existing_config = await GroupMessageService.get_config_by_chat_id(chat_id)
            if existing_config:
                message_text = (
                    f"✅ {chat_type} auto messages enabled\n\n"
                    f"{chat_type} Name: {chat_title}\n"
                    f"{chat_type} ID: {chat_id}\n\n"
                    f"💡 Messages will be randomly selected from stored messages"
                )
            else:
                message_text = (
                    f"✅ {chat_type} auto messages enabled\n\n"
                    f"{chat_type} Name: {chat_title}\n"
                    f"{chat_type} ID: {chat_id}\n\n"
                    f"💡 Messages will be randomly selected from stored messages"
                )
            await update.message.reply_text(message_text)
        else:
            await update.message.reply_text(error_msg or "❌ Setup failed")
    except Exception as e:
        logger.error(f"Setup group auto messages failed: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Setup failed: {e}")


@error_handler
@admin_required
async def test_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Test send group message (use in group)"""
    chat = update.effective_chat

    if chat.type == "private":
        await update.message.reply_text("❌ This command can only be used in groups or channels")
        return

    # Check if there are arguments (message type)
    if not context.args:
        # Show selection menu
        keyboard = [
            [
                InlineKeyboardButton("🌅 Start Work", callback_data="test_msg_start_work"),
                InlineKeyboardButton("🌙 End Work", callback_data="test_msg_end_work"),
            ],
            [
                InlineKeyboardButton("👋 Welcome", callback_data="test_msg_welcome"),
                InlineKeyboardButton("📢 Promotion", callback_data="test_msg_promotion"),
            ],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "📤 Select message type to send:\n\n"
            "• Start Work - Test start work message\n"
            "• End Work - Test end work message\n"
            "• Welcome - Test welcome message\n"
            "• Promotion - Test promotion message",
            reply_markup=reply_markup,
        )
        return

    # 如果有参数，直接发送对应类型的消息
    msg_type = context.args[0].lower()
    await _send_test_message(update, context, chat, msg_type)


async def _send_test_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE, chat, msg_type: str
) -> None:
    """发送测试消息 - 直接从数据库读取语录并播报"""
    try:
        import random

        import db_operations
        from utils.schedule_executor import (
            _combine_message_with_anti_fraud,
            _send_group_message,
            select_rotated_message,
        )

        # 获取群组配置（用于获取链接，但不检查是否开启）
        config = await db_operations.get_group_message_config_by_chat_id(chat.id)
        bot_links = config.get("bot_links") if config else None
        worker_links = config.get("worker_links") if config else None

        # 获取激活的防诈骗语录
        anti_fraud_messages = await db_operations.get_active_anti_fraud_messages()

        # Select message content based on message type - 直接从数据库读取语录
        main_message = ""
        if msg_type in ["start", "start_work"]:
            # Start work message - 直接从数据库读取
            start_work_messages = await db_operations.get_active_start_work_messages()
            if not start_work_messages:
                await update.message.reply_text("❌ No active start work messages")
                return
            message = random.choice(start_work_messages)
            main_message = select_rotated_message(message)

        elif msg_type in ["end", "end_work"]:
            # End work message - 直接从数据库读取
            end_work_messages = await db_operations.get_active_end_work_messages()
            if not end_work_messages:
                await update.message.reply_text("❌ No active end work messages")
                return
            message = random.choice(end_work_messages)
            main_message = select_rotated_message(message)

        elif msg_type == "welcome":
            # Welcome message - 从配置读取，如果没有则提示
            if config and config.get("welcome_message"):
                welcome_message = config.get("welcome_message")
                rotated_message = select_rotated_message(welcome_message)
                # Replace variables
                username = (
                    update.effective_user.username
                    or update.effective_user.first_name
                    or "Test User"
                )
                chat_title = chat.title or "Group"
                main_message = rotated_message.replace("{username}", username)
                main_message = main_message.replace("{chat_title}", chat_title)
            else:
                await update.message.reply_text("❌ No welcome message configured for this group")
                return

        elif msg_type == "promotion":
            # Promotion message - 直接从数据库读取
            promotion_messages = await db_operations.get_active_promotion_messages()
            if not promotion_messages:
                await update.message.reply_text("❌ No active promotion messages")
                return
            valid_messages = [
                msg
                for msg in promotion_messages
                if msg.get("message") and msg.get("message").strip()
            ]
            if not valid_messages:
                await update.message.reply_text("❌ No valid promotion messages")
                return
            selected_msg_dict = random.choice(valid_messages)
            main_message = selected_msg_dict.get("message", "").strip()

        else:
            await update.message.reply_text(
                "❌ Invalid message type\n\n"
                "Supported types:\n"
                "• start / start_work\n"
                "• end / end_work\n"
                "• welcome\n"
                "• promotion"
            )
            return

        if not main_message:
            await update.message.reply_text("❌ Message content is empty")
            return

        # Combine message: main message + anti-fraud message
        final_message = _combine_message_with_anti_fraud(main_message, anti_fraud_messages)

        # Send message
        if await _send_group_message(context.bot, chat.id, final_message, bot_links, worker_links):
            await update.message.reply_text("✅ Test message sent")
            logger.info(f"Test message sent to group {chat.id} (type: {msg_type})")
        else:
            await update.message.reply_text("❌ Send failed, please check logs")
    except Exception as e:
        logger.error(f"发送测试消息失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 发送失败: {e}")
