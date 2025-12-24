"""群组消息管理处理器"""

# 标准库
import logging

# 第三方库
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

# 本地模块
import db_operations
from decorators import admin_required, error_handler, private_chat_only

logger = logging.getLogger(__name__)


@error_handler
@private_chat_only
@admin_required
async def manage_group_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理群组消息配置"""
    try:
        configs = await db_operations.get_group_message_configs()

        msg = "📢 群组消息管理\n\n"

        if not configs:
            msg += "❌ 当前没有配置的总群\n\n"
            msg += "使用 /groupmsg_add <chat_id> 添加总群"
        else:
            msg += "已配置的总群：\n\n"
            for config in configs:
                chat_id = config.get("chat_id")
                chat_title = config.get("chat_title", "未设置")
                is_active = config.get("is_active", 0)
                status = "✅ 启用" if is_active else "❌ 禁用"

                msg += f"📌 {chat_title} (ID: {chat_id})\n"
                msg += f"   状态: {status}\n"
                msg += (
                    f"   开工信息: {'已设置' if config.get('start_work_message') else '未设置'}\n"
                )
                msg += f"   收工信息: {'已设置' if config.get('end_work_message') else '未设置'}\n"
                msg += f"   欢迎信息: {'已设置' if config.get('welcome_message') else '未设置'}\n\n"

        keyboard = [
            [InlineKeyboardButton("➕ 添加总群", callback_data="groupmsg_add")],
            [InlineKeyboardButton("📝 设置消息", callback_data="groupmsg_set_message")],
            [InlineKeyboardButton("🔄 刷新", callback_data="groupmsg_refresh")],
        ]

        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"显示群组消息管理失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 显示失败: {e}")


@error_handler
@private_chat_only
@admin_required
async def add_group_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """添加总群/频道配置"""
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "请输入群组/频道ID\n"
            "格式: /groupmsg_add <chat_id>\n"
            "示例: /groupmsg_add -1001234567890\n\n"
            "💡 提示：\n"
            "- 在群组中使用 /groupmsg_getid 获取群组ID\n"
            "- 在频道中使用 /groupmsg_getid 获取频道ID"
        )
        return

    try:
        chat_id = int(context.args[0])

        # 尝试获取群组/频道信息
        chat_type = "群组/频道"
        try:
            chat = await context.bot.get_chat(chat_id)
            chat_title = chat.title or "未设置"
            # 判断类型
            if chat.type == "channel":
                chat_type = "频道"
            elif chat.type in ["group", "supergroup"]:
                chat_type = "群组"
        except Exception:
            chat_title = "未设置"

        # 保存配置
        success = await db_operations.save_group_message_config(
            chat_id=chat_id, chat_title=chat_title, is_active=1
        )

        if success:
            await update.message.reply_text(
                f"✅ {chat_type}配置已添加\n\n"
                f"{chat_type}ID: {chat_id}\n"
                f"{chat_type}名称: {chat_title}\n\n"
                f"请使用 /groupmsg 设置消息内容"
            )
        else:
            await update.message.reply_text("❌ 添加失败，可能已存在")
    except ValueError:
        await update.message.reply_text("❌ 群组/频道ID必须是数字")
    except Exception as e:
        logger.error(f"添加总群配置失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 添加失败: {e}")


@error_handler
@admin_required
async def get_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """获取当前群组/频道ID（在群组或频道中使用）"""
    chat = update.effective_chat

    if chat.type == "private":
        await update.message.reply_text("❌ 此命令只能在群组或频道中使用")
        return

    # 判断是群组还是频道
    chat_type = "频道" if chat.type == "channel" else "群组"

    await update.message.reply_text(
        f"📌 {chat_type}信息\n\n"
        f"{chat_type}名称: {chat.title}\n"
        f"{chat_type}ID: `{chat.id}`\n\n"
        f"复制上面的ID，在私聊中使用 /groupmsg_add {chat.id} 添加配置",
        parse_mode="Markdown",
    )


@error_handler
@admin_required
async def setup_group_auto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """一键设置群组/频道自动消息功能（在群组或频道中使用）"""
    chat = update.effective_chat

    if chat.type == "private":
        await update.message.reply_text("❌ 此命令只能在群组或频道中使用")
        return

    # 判断是群组还是频道
    chat_type = "频道" if chat.type == "channel" else "群组"
    chat_id = chat.id
    chat_title = chat.title or "未设置"

    try:
        # 检查是否已存在配置
        existing_config = await db_operations.get_group_message_config_by_chat_id(chat_id)

        if existing_config:
            # 如果已存在，更新为启用状态
            success = await db_operations.save_group_message_config(
                chat_id=chat_id, chat_title=chat_title, is_active=1
            )
            if success:
                # 自动激活公告发送计划（确保轮播功能可用）
                await db_operations.save_announcement_schedule(interval_hours=3, is_active=1)
                await update.message.reply_text(
                    f"✅ {chat_type}功能已启用\n\n"
                    f"{chat_type}名称: {chat_title}\n"
                    f"{chat_type}ID: {chat_id}\n\n"
                    f"💡 提示：在私聊中使用 /groupmsg 设置消息内容"
                )
            else:
                await update.message.reply_text("❌ 启用失败")
        else:
            # 如果不存在，创建新配置
            success = await db_operations.save_group_message_config(
                chat_id=chat_id, chat_title=chat_title, is_active=1
            )
            if success:
                # 自动激活公告发送计划（确保轮播功能可用）
                await db_operations.save_announcement_schedule(interval_hours=3, is_active=1)
                await update.message.reply_text(
                    f"✅ {chat_type}自动消息功能已开启\n\n"
                    f"{chat_type}名称: {chat_title}\n"
                    f"{chat_type}ID: {chat_id}\n\n"
                    f"💡 提示：在私聊中使用 /groupmsg 设置消息内容"
                )
            else:
                await update.message.reply_text("❌ 设置失败")
    except Exception as e:
        logger.error(f"一键设置群组自动消息功能失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 设置失败: {e}")


@error_handler
@private_chat_only
@admin_required
async def manage_announcements(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理公司公告"""
    try:
        announcements = await db_operations.get_all_company_announcements()
        schedule = await db_operations.get_announcement_schedule()

        msg = "📢 公司公告管理\n\n"

        if schedule:
            interval_hours = schedule.get("interval_hours", 3)
            is_active = schedule.get("is_active", 0)
            status = "✅ 启用" if is_active else "❌ 禁用"
            msg += f"发送间隔: {interval_hours} 小时\n"
            msg += f"状态: {status}\n\n"

        if not announcements:
            msg += "❌ 当前没有公告\n\n"
            msg += "使用 /announcement_add <消息内容> 添加公告"
        else:
            msg += f"公告列表（共 {len(announcements)} 条）：\n\n"
            active_count = sum(1 for a in announcements if a.get("is_active"))
            msg += f"激活: {active_count} 条\n\n"

            for ann in announcements[:10]:  # 只显示前10条
                ann_id = ann.get("id")
                message = ann.get("message", "")
                is_active = ann.get("is_active", 0)
                status = "✅" if is_active else "❌"

                # 截断长消息
                display_msg = message[:50] + "..." if len(message) > 50 else message
                msg += f"{status} [{ann_id}] {display_msg}\n"

            if len(announcements) > 10:
                msg += f"\n... 还有 {len(announcements) - 10} 条公告"

        keyboard = [
            [InlineKeyboardButton("➕ 添加公告", callback_data="announcement_add")],
            [InlineKeyboardButton("📋 查看全部", callback_data="announcement_list")],
            [InlineKeyboardButton("⚙️ 设置间隔", callback_data="announcement_set_interval")],
            [InlineKeyboardButton("🔄 刷新", callback_data="announcement_refresh")],
        ]

        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"显示公告管理失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 显示失败: {e}")


@error_handler
@private_chat_only
@admin_required
async def manage_anti_fraud_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理防诈骗语录"""
    try:
        messages = await db_operations.get_all_anti_fraud_messages()

        msg = "🛡️ 防诈骗语录管理\n\n"

        if not messages:
            msg += "❌ 当前没有防诈骗语录\n\n"
            msg += "使用 /antifraud_add <消息内容> 添加语录"
        else:
            msg += f"语录列表（共 {len(messages)} 条）：\n\n"
            active_count = sum(1 for m in messages if m.get("is_active"))
            msg += f"激活: {active_count} 条\n\n"

            for msg_item in messages[:10]:  # 只显示前10条
                msg_id = msg_item.get("id")
                message = msg_item.get("message", "")
                is_active = msg_item.get("is_active", 0)
                status = "✅" if is_active else "❌"

                # 截断长消息
                display_msg = message[:50] + "..." if len(message) > 50 else message
                msg += f"{status} [{msg_id}] {display_msg}\n"

            if len(messages) > 10:
                msg += f"\n... 还有 {len(messages) - 10} 条语录"

        keyboard = [
            [InlineKeyboardButton("➕ 添加语录", callback_data="antifraud_add")],
            [InlineKeyboardButton("📋 查看全部", callback_data="antifraud_list")],
            [InlineKeyboardButton("🔄 刷新", callback_data="antifraud_refresh")],
        ]

        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"显示防诈骗语录管理失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 显示失败: {e}")


@error_handler
@private_chat_only
@admin_required
async def batch_set_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """批量设置群组消息（一次性设置开工、收工、欢迎信息）"""
    try:
        configs = await db_operations.get_group_message_configs()

        if not configs:
            await update.message.reply_text(
                "❌ 当前没有配置的总群\n\n"
                "请先使用以下方式添加群组：\n"
                "1. 在群组中使用 /groupmsg_setup 一键设置\n"
                "2. 或使用 /groupmsg_add <chat_id> 添加群组"
            )
            return

        # 如果只有一个群组，直接进入设置流程
        if len(configs) == 1:
            chat_id = configs[0].get("chat_id")
            context.user_data["batch_setting_chat_id"] = chat_id
            context.user_data["batch_setting_step"] = "start_work"

            await update.message.reply_text(
                "📝 批量设置消息\n\n"
                f"群组: {configs[0].get('chat_title', '未设置')} (ID: {chat_id})\n\n"
                "步骤 1/3: 设置开工信息\n\n"
                "请输入开工信息（支持多版本，用 ⸻ 分隔）：\n\n"
                "💡 示例：\n"
                "Good morning po! 😊 Our team is now online...\n"
                "⸻\n"
                "版本二内容\n"
                "⸻\n"
                "版本三内容\n\n"
                "输入 'skip' 跳过此步骤\n"
                "输入 'cancel' 取消"
            )
            context.user_data["state"] = "BATCH_SETTING_MESSAGES"
            return

        # 多个群组，让用户选择
        keyboard = []
        for config in configs:
            chat_id = config.get("chat_id")
            chat_title = config.get("chat_title", f"ID: {chat_id}")
            keyboard.append(
                [InlineKeyboardButton(chat_title, callback_data=f"batch_set_select_{chat_id}")]
            )

        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="batch_set_cancel")])

        await update.message.reply_text(
            "📝 批量设置消息\n\n" "请选择要设置的群组：",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"批量设置消息失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 设置失败: {e}")


@error_handler
@private_chat_only
@admin_required
async def manage_promotion_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manage company promotion messages"""
    try:
        messages = await db_operations.get_all_promotion_messages()

        msg = "📢 Company Promotion Messages Management\n\n"
        msg += "Send Interval: Every 2 hours\n"
        msg += "Send Method: Sequential rotation\n\n"

        if not messages:
            msg += "❌ No promotion messages currently\n\n"
            msg += "Use /promotion_add <message> to add a message"
        else:
            msg += f"Message List (Total: {len(messages)}):\n\n"
            active_count = sum(1 for m in messages if m.get("is_active"))
            msg += f"Active: {active_count}\n\n"

            for msg_item in messages[:10]:  # 只显示前10条
                msg_id = msg_item.get("id")
                message = msg_item.get("message", "")
                is_active = msg_item.get("is_active", 0)
                status = "✅" if is_active else "❌"

                # 截断长消息
                display_msg = message[:50] + "..." if len(message) > 50 else message
                msg += f"{status} [{msg_id}] {display_msg}\n"

            if len(messages) > 10:
                msg += f"\n... {len(messages) - 10} more messages"

        keyboard = [
            [InlineKeyboardButton("➕ Add Message", callback_data="promotion_add")],
            [InlineKeyboardButton("📋 View All", callback_data="promotion_list")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="promotion_refresh")],
        ]

        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Failed to display promotion messages management: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Display failed: {e}")
