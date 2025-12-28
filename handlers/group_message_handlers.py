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
    logger.info(
        f"setup_group_auto called by user {update.effective_user.id if update.effective_user else 'unknown'}"
    )
    chat = update.effective_chat

    if not chat:
        logger.error("setup_group_auto: chat is None")
        return

    if chat.type == "private":
        logger.warning(
            f"setup_group_auto: called in private chat by user {update.effective_user.id if update.effective_user else 'unknown'}"
        )
        await update.message.reply_text("❌ This command can only be used in groups or channels")
        return

    # Determine if it's a channel or group
    chat_type = "Channel" if chat.type == "channel" else "Group"
    chat_id = chat.id
    chat_title = chat.title or "Not set"

    logger.info(f"setup_group_auto: Setting up for {chat_type} {chat_id} ({chat_title})")

    # Use Service to setup group auto messages
    try:
        success, error_msg = await GroupMessageService.setup_group_auto(chat_id, chat_title)
        logger.info(f"setup_group_auto: Service returned success={success}, error_msg={error_msg}")

        if success:
            message_text = (
                f"✅ {chat_type} auto messages enabled\n\n"
                f"{chat_type} Name: {chat_title}\n"
                f"{chat_type} ID: {chat_id}\n\n"
                f"💡 Messages will be randomly selected from database automatically"
            )
            await update.message.reply_text(message_text)
            logger.info(f"Group auto messages enabled for {chat_type} {chat_id} ({chat_title})")
        else:
            error_message = error_msg or "❌ Setup failed"
            logger.warning(
                f"Failed to setup group auto messages for {chat_type} {chat_id}: {error_message}"
            )
            await update.message.reply_text(error_message)
    except Exception as e:
        logger.error(f"setup_group_auto: Exception in service call: {e}", exc_info=True)
        raise  # Re-raise to let @error_handler catch it


@error_handler
@admin_required
async def test_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Test send group message (use in group)"""
    logger.info(
        f"test_group_message called by user {update.effective_user.id if update.effective_user else 'unknown'}"
    )
    chat = update.effective_chat

    if not chat:
        logger.error("test_group_message: chat is None")
        return

    if chat.type == "private":
        logger.warning(
            f"test_group_message: called in private chat by user {update.effective_user.id if update.effective_user else 'unknown'}"
        )
        await update.message.reply_text("❌ This command can only be used in groups or channels")
        return

    # 如果没有参数，默认发送开工消息
    # 如果有参数，发送指定类型的消息
    msg_type = context.args[0].lower() if context.args else "start_work"
    await _send_test_message(update, context, chat, msg_type)


async def _send_test_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE, chat, msg_type: str
) -> None:
    """发送测试消息 - 直接从数据库读取语录并播报"""
    import random

    import db_operations
    from utils.schedule_executor import (
        _combine_message_with_anti_fraud,
        _send_group_message,
        select_rotated_message,
    )

    # 直接从数据库获取激活的防诈骗语录
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
        # Welcome message - 从数据库读取（如果有配置的话）
        config = await db_operations.get_group_message_config_by_chat_id(chat.id)
        if config and config.get("welcome_message"):
            welcome_message = config.get("welcome_message")
            rotated_message = select_rotated_message(welcome_message)
            # Replace variables
            username = (
                update.effective_user.username or update.effective_user.first_name or "Test User"
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
            msg for msg in promotion_messages if msg.get("message") and msg.get("message").strip()
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

    # 直接从数据库读取，直接发送消息（不添加任何按钮）
    if await _send_group_message(context.bot, chat.id, final_message):
        await update.message.reply_text("✅ Test message sent")
        logger.info(f"Test message sent to group {chat.id} (type: {msg_type})")
    else:
        await update.message.reply_text("❌ Send failed, please check logs")
