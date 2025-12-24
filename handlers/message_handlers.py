"""消息处理器（群组事件、文本输入等）"""

# 标准库
import logging
from datetime import datetime

# 第三方库
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

# 本地模块
import db_operations
from utils.date_helpers import get_daily_period_date
from utils.order_helpers import try_create_order_from_title, update_order_state_from_title
from utils.stats_helpers import update_all_stats, update_liquid_capital

logger = logging.getLogger(__name__)


async def handle_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理新成员入群（机器人入群或新成员加入）"""
    try:
        if not update.message or not update.message.new_chat_members:
            return

        chat = update.effective_chat
        if not chat:
            return

        bot_id = context.bot.id
        new_members = []
        is_bot_added = False

        # 分离新成员和机器人
        for member in update.message.new_chat_members:
            if member.id == bot_id:
                is_bot_added = True
            else:
                new_members.append(member)

        # 处理机器人被添加的情况（创建订单）
        if is_bot_added:
            if not chat.title:
                logger.warning(f"Bot added to group but no title found (chat_id: {chat.id})")
                return

            logger.info(f"Bot added to group: '{chat.title}' (chat_id: {chat.id})")

            # 检查群名是否包含完成或违约完成标记（⭕️ 或 ❌⭕️）
            if "⭕️" in chat.title or "❌⭕️" in chat.title:
                logger.info(
                    f"Group title contains completion markers, skipping order creation (chat_id: {chat.id})"
                )
                return

            # 尝试创建订单
            await try_create_order_from_title(
                update, context, chat, chat.title, manual_trigger=False
            )

        # 处理新成员加入的情况（发送欢迎信息）
        if new_members:
            # 检查该群是否配置了欢迎信息
            config = await db_operations.get_group_message_config_by_chat_id(chat.id)

            if config and config.get("is_active") and config.get("welcome_message"):
                welcome_message = config.get("welcome_message")
                chat_title = chat.title or "群组"

                # 获取激活的防诈骗语录
                from utils.schedule_executor import (
                    format_admin_mentions_from_group,
                    format_red_message,
                    select_random_anti_fraud_message,
                    select_rotated_message,
                )

                anti_fraud_messages = await db_operations.get_active_anti_fraud_messages()

                # 获取管理员@用户名（从指定群组获取）
                admin_mentions = await format_admin_mentions_from_group(context.bot)

                # 为每个新成员发送欢迎信息
                for member in new_members:
                    try:
                        username = member.username or member.first_name or "新成员"

                        # 选择轮换版本
                        rotated_message = select_rotated_message(welcome_message)

                        # 替换变量
                        personalized_message = rotated_message.replace("{username}", username)
                        personalized_message = personalized_message.replace(
                            "{chat_title}", chat_title
                        )

                        # 组合消息：主消息 + 防诈骗语录 + 管理员@用户名
                        final_message = personalized_message

                        # 添加防诈骗语录（如果存在）
                        if anti_fraud_messages:
                            random_anti_fraud = select_random_anti_fraud_message(
                                anti_fraud_messages
                            )
                            if random_anti_fraud:
                                # 处理多版本（如果语录包含 ⸻ 分隔符）
                                rotated_anti_fraud = select_rotated_message(random_anti_fraud)
                                if rotated_anti_fraud:
                                    red_anti_fraud = format_red_message(rotated_anti_fraud)
                                    final_message = f"{personalized_message}\n\n{red_anti_fraud}"

                        # 添加管理员@用户名
                        if admin_mentions:
                            final_message = f"{final_message}\n\n{admin_mentions}"

                        # 发送消息（使用HTML格式以支持红色文字）
                        await context.bot.send_message(
                            chat_id=chat.id, text=final_message, parse_mode="HTML"
                        )
                        logger.info(f"欢迎信息已发送给新成员 {member.id} 在群组 {chat.id}")
                    except Exception as e:
                        logger.error(f"发送欢迎信息失败: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error in handle_new_chat_members: {e}", exc_info=True)


async def handle_new_chat_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理群名变更"""
    try:
        if not update.message:
            return

        chat = update.effective_chat
        new_title = update.message.new_chat_title

        if not new_title:
            logger.warning(
                f"Group title changed but new_title is None (chat_id: {chat.id if chat else 'unknown'})"
            )
            return

        if not chat:
            logger.warning("Group title changed but chat is None")
            return

        logger.info(f"Group title changed to: '{new_title}' (chat_id: {chat.id})")

        # 检查是否正在等待违约完成金额输入，如果是，不处理群名变更
        # 避免在用户输入金额时，群名变更导致提示消息被删除或其他干扰
        if context.user_data.get("state") == "WAITING_BREACH_END_AMOUNT":
            chat_id = context.user_data.get("breach_end_chat_id")
            if chat_id and chat.id == chat_id:
                logger.info(
                    f"Waiting for breach end amount input, skipping title change handling (chat_id: {chat.id})"
                )
                return

        existing_order = await db_operations.get_order_by_chat_id(chat.id)
        if existing_order:
            logger.info(f"Order exists, updating state from title: '{new_title}'")
            await update_order_state_from_title(update, context, existing_order, new_title)
        else:
            # 检查群名是否包含完成或违约完成标记（⭕️ 或 ❌⭕️）
            # 如果包含，说明订单已完成，不需要创建订单
            if "⭕️" in new_title or "❌⭕️" in new_title:
                logger.info(
                    f"Group title contains completion markers (⭕️ or ❌⭕️), skipping order creation (chat_id: {chat.id})"
                )
                return
            logger.info(f"No existing order, attempting to create from title: '{new_title}'")
            await try_create_order_from_title(
                update, context, chat, new_title, manual_trigger=False
            )
    except Exception as e:
        logger.error(f"Error in handle_new_chat_title: {e}", exc_info=True)


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理文本输入（用于搜索和群发）"""
    user_state = context.user_data.get("state")

    # 1. 检查是否是快捷操作（+开头），如果是，交给 handle_amount_operation 处理
    if update.message.text.startswith("+"):
        return

    # 2. 检查状态是否需要处理群组消息
    allow_group = False
    if user_state in ["WAITING_BREACH_END_AMOUNT", "BROADCAST_PAYMENT"]:
        allow_group = True

    # 3. 检查聊天类型
    if update.effective_chat.type != "private" and not allow_group:
        return

    # 如果没有状态，忽略
    if not user_state:
        return

    text = update.message.text.strip()

    # 通用取消逻辑
    if text.lower() == "cancel":
        context.user_data["state"] = None
        msg = "✅ Operation Cancelled"
        await update.message.reply_text(msg)
        return

    if user_state == "WAITING_BREACH_END_AMOUNT":
        await _handle_breach_end_amount(update, context, text)
        return

    if user_state == "BROADCAST_PAYMENT":
        from handlers.broadcast_handlers import handle_broadcast_payment_input

        await handle_broadcast_payment_input(update, context, text)
        return

    # 以下状态仅限私聊
    if update.effective_chat.type != "private":
        return

    # 处理群发消息（必须在私聊中）
    if user_state == "BROADCASTING":
        await _handle_broadcast(update, context, text)
        return

    if user_state in ["QUERY_EXPENSE_COMPANY", "QUERY_EXPENSE_OTHER"]:
        await _handle_expense_query(update, context, text, user_state)
        return

    if user_state in ["WAITING_EXPENSE_COMPANY", "WAITING_EXPENSE_OTHER"]:
        await _handle_expense_input(update, context, text, user_state)
        return

    if user_state == "SEARCHING":
        await _handle_search_input(update, context, text)
        return

    if user_state == "SEARCHING_AMOUNT":
        await _handle_search_amount_input(update, context, text)
        return

    if user_state == "REPORT_QUERY":
        await _handle_report_query(update, context, text)
        return

    if user_state == "REPORT_SEARCHING":
        await _handle_report_search(update, context, text)
        return

    if user_state == "QUERY_INCOME":
        from handlers.income_handlers import handle_income_query_input

        await handle_income_query_input(update, context, text)
        return

    if user_state == "INCOME_QUERY_DATE":
        await _handle_income_query_date(update, context, text)
        return

    # 处理支付账户相关状态
    if user_state == "UPDATING_BALANCE_GCASH":
        await _handle_update_balance(update, context, text, "gcash")
        return

    if user_state == "UPDATING_BALANCE_PAYMAYA":
        await _handle_update_balance(update, context, text, "paymaya")
        return

    if user_state == "EDITING_ACCOUNT_GCASH":
        await _handle_edit_account(update, context, text, "gcash")
        return

    if user_state == "EDITING_ACCOUNT_PAYMAYA":
        await _handle_edit_account(update, context, text, "paymaya")
        return

    if user_state == "ADDING_ACCOUNT_GCASH":
        await _handle_add_account(update, context, text, "gcash")
        return

    if user_state == "ADDING_ACCOUNT_PAYMAYA":
        await _handle_add_account(update, context, text, "paymaya")
        return

    if user_state == "EDITING_ACCOUNT_BY_ID_GCASH":
        await _handle_edit_account_by_id(update, context, text, "gcash")
        return

    if user_state == "EDITING_ACCOUNT_BY_ID_PAYMAYA":
        await _handle_edit_account_by_id(update, context, text, "paymaya")
        return

    # 处理按ID修改余额
    if user_state and user_state.startswith("UPDATING_BALANCE_BY_ID_"):
        await _handle_update_balance_by_id(update, context, text)
        return

    # 处理定时播报输入
    if user_state and user_state.startswith("SCHEDULE_"):
        from handlers.schedule_handlers import handle_schedule_input

        handled = await handle_schedule_input(update, context)
        if handled:
            return

    # 处理群组消息配置输入
    if user_state == "ADDING_GROUP_CONFIG":
        if text.strip().lower() == "cancel":
            context.user_data["state"] = None
            await update.message.reply_text("✅ 操作已取消")
            return

        try:
            chat_id = int(text.strip())

            # 尝试获取群组信息
            try:
                chat = await context.bot.get_chat(chat_id)
                chat_title = chat.title or "未设置"
            except Exception:
                chat_title = "未设置"

            # 保存配置
            success = await db_operations.save_group_message_config(
                chat_id=chat_id, chat_title=chat_title, is_active=1
            )

            if success:
                await update.message.reply_text(
                    f"✅ 总群配置已添加\n\n"
                    f"群组ID: {chat_id}\n"
                    f"群组名称: {chat_title}\n\n"
                    f"请使用 /groupmsg 设置消息内容"
                )
            else:
                await update.message.reply_text("❌ 添加失败，可能已存在")

            context.user_data["state"] = None
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字，输入 'cancel' 取消")
        except Exception as e:
            logger.error(f"添加总群配置失败: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 添加失败: {e}")
            context.user_data["state"] = None
        return

    if user_state == "SETTING_GROUP_MESSAGE":
        if text.strip().lower() == "cancel":
            context.user_data["state"] = None
            context.user_data.pop("setting_message_chat_id", None)
            context.user_data.pop("setting_message_type", None)
            await update.message.reply_text("✅ 操作已取消")
            return

        # 如果输入 'keep'，保持当前内容不变
        if text.strip().lower() == "keep":
            chat_id = context.user_data.get("setting_message_chat_id")
            message_type = context.user_data.get("setting_message_type")

            if message_type == "start_work":
                type_name = "开工信息"
            elif message_type == "end_work":
                type_name = "收工信息"
            elif message_type == "welcome":
                type_name = "欢迎信息"
            else:
                type_name = "消息"

            await update.message.reply_text(f"✅ {type_name}保持当前内容不变")
            context.user_data["state"] = None
            context.user_data.pop("setting_message_chat_id", None)
            context.user_data.pop("setting_message_type", None)
            return

        chat_id = context.user_data.get("setting_message_chat_id")
        message_type = context.user_data.get("setting_message_type")

        if not chat_id or not message_type:
            await update.message.reply_text("❌ 错误：找不到配置信息")
            context.user_data["state"] = None
            return

        # 根据类型设置消息
        update_data = {}
        if message_type == "start_work":
            update_data["start_work_message"] = text.strip()
            type_name = "开工信息"
        elif message_type == "end_work":
            update_data["end_work_message"] = text.strip()
            type_name = "收工信息"
        elif message_type == "welcome":
            update_data["welcome_message"] = text.strip()
            type_name = "欢迎信息"
        else:
            await update.message.reply_text("❌ 错误：未知的消息类型")
            context.user_data["state"] = None
            return

        success = await db_operations.save_group_message_config(chat_id=chat_id, **update_data)

        if success:
            # 检查是否包含多版本分隔符
            has_multiple_versions = "⸻" in text.strip()
            if has_multiple_versions:
                versions = [v.strip() for v in text.strip().split("⸻") if v.strip()]
                await update.message.reply_text(
                    f"✅ {type_name}已设置\n\n" f"💡 检测到 {len(versions)} 个版本，将自动轮播"
                )
            else:
                await update.message.reply_text(f"✅ {type_name}已设置")
        else:
            await update.message.reply_text(f"❌ 设置失败")

        context.user_data["state"] = None
        context.user_data.pop("setting_message_chat_id", None)
        context.user_data.pop("setting_message_type", None)
        return

    if user_state == "BATCH_SETTING_MESSAGES":
        # 批量设置消息流程
        chat_id = context.user_data.get("batch_setting_chat_id")
        step = context.user_data.get("batch_setting_step")

        if not chat_id or not step:
            await update.message.reply_text("❌ 错误：找不到配置信息")
            context.user_data["state"] = None
            context.user_data.pop("batch_setting_chat_id", None)
            context.user_data.pop("batch_setting_step", None)
            return

        if text.strip().lower() == "cancel":
            await update.message.reply_text("❌ 已取消批量设置")
            context.user_data["state"] = None
            context.user_data.pop("batch_setting_chat_id", None)
            context.user_data.pop("batch_setting_step", None)
            return

        # 处理当前步骤
        if step == "start_work":
            if text.strip().lower() == "skip":
                await update.message.reply_text("⏭️ 已跳过开工信息设置")
            else:
                success = await db_operations.save_group_message_config(
                    chat_id=chat_id, start_work_message=text.strip()
                )
                if success:
                    versions = (
                        [v.strip() for v in text.strip().split("⸻") if v.strip()]
                        if "⸻" in text.strip()
                        else [text.strip()]
                    )
                    await update.message.reply_text(
                        f"✅ 开工信息已设置\n" f"💡 检测到 {len(versions)} 个版本"
                    )
                else:
                    await update.message.reply_text("❌ 设置失败")

            # 进入下一步
            context.user_data["batch_setting_step"] = "end_work"
            await update.message.reply_text(
                "步骤 2/3: 设置收工信息\n\n"
                "请输入收工信息（支持多版本，用 ⸻ 分隔）：\n\n"
                "输入 'skip' 跳过此步骤\n"
                "输入 'cancel' 取消"
            )
            return

        elif step == "end_work":
            if text.strip().lower() == "skip":
                await update.message.reply_text("⏭️ 已跳过收工信息设置")
            else:
                success = await db_operations.save_group_message_config(
                    chat_id=chat_id, end_work_message=text.strip()
                )
                if success:
                    versions = (
                        [v.strip() for v in text.strip().split("⸻") if v.strip()]
                        if "⸻" in text.strip()
                        else [text.strip()]
                    )
                    await update.message.reply_text(
                        f"✅ 收工信息已设置\n" f"💡 检测到 {len(versions)} 个版本"
                    )
                else:
                    await update.message.reply_text("❌ 设置失败")

            # 进入下一步
            context.user_data["batch_setting_step"] = "welcome"
            await update.message.reply_text(
                "步骤 3/3: 设置欢迎信息\n\n"
                "请输入欢迎信息（支持多版本，用 ⸻ 分隔）：\n\n"
                "💡 支持变量：\n"
                "- {username} - 新成员用户名\n"
                "- {chat_title} - 群组名称\n\n"
                "输入 'skip' 跳过此步骤\n"
                "输入 'cancel' 取消"
            )
            return

        elif step == "welcome":
            if text.strip().lower() == "skip":
                await update.message.reply_text("⏭️ 已跳过欢迎信息设置")
            else:
                success = await db_operations.save_group_message_config(
                    chat_id=chat_id, welcome_message=text.strip()
                )
                if success:
                    versions = (
                        [v.strip() for v in text.strip().split("⸻") if v.strip()]
                        if "⸻" in text.strip()
                        else [text.strip()]
                    )
                    await update.message.reply_text(
                        f"✅ 欢迎信息已设置\n" f"💡 检测到 {len(versions)} 个版本"
                    )
                else:
                    await update.message.reply_text("❌ 设置失败")

            # 完成
            config = await db_operations.get_group_message_config_by_chat_id(chat_id)
            chat_title = config.get("chat_title", f"ID: {chat_id}") if config else f"ID: {chat_id}"

            has_start = bool(config.get("start_work_message")) if config else False
            has_end = bool(config.get("end_work_message")) if config else False
            has_welcome = bool(config.get("welcome_message")) if config else False

            await update.message.reply_text(
                f"✅ 批量设置完成！\n\n"
                f"群组: {chat_title}\n"
                f"群组ID: {chat_id}\n\n"
                f"设置状态：\n"
                f"  🌅 开工信息: {'✅ 已设置' if has_start else '❌ 未设置'}\n"
                f"  🌙 收工信息: {'✅ 已设置' if has_end else '❌ 未设置'}\n"
                f"  👋 欢迎信息: {'✅ 已设置' if has_welcome else '❌ 未设置'}\n\n"
                f"💡 提示：使用 /groupmsg 查看和编辑消息内容"
            )

            context.user_data["state"] = None
            context.user_data.pop("batch_setting_chat_id", None)
            context.user_data.pop("batch_setting_step", None)
            return

    if user_state == "ADDING_ANNOUNCEMENT":
        if text.strip().lower() == "cancel":
            context.user_data["state"] = None
            await update.message.reply_text("✅ 操作已取消")
            return

        try:
            ann_id = await db_operations.save_company_announcement(text.strip())
            if ann_id:
                await update.message.reply_text(f"✅ 公告已添加 (ID: {ann_id})")
            else:
                await update.message.reply_text("❌ 添加失败")
        except Exception as e:
            logger.error(f"添加公告失败: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 添加失败: {e}")

        context.user_data["state"] = None
        return

    if user_state == "ADDING_ANTIFRAUD_MESSAGE":
        if text.strip().lower() == "cancel":
            context.user_data["state"] = None
            await update.message.reply_text("✅ 操作已取消")
            return

        try:
            msg_id = await db_operations.save_anti_fraud_message(text.strip())
            if msg_id:
                await update.message.reply_text(f"✅ 防诈骗语录已添加 (ID: {msg_id})")
            else:
                await update.message.reply_text("❌ 添加失败")
        except Exception as e:
            logger.error(f"添加防诈骗语录失败: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 添加失败: {e}")

        context.user_data["state"] = None
        return

    if user_state == "ADDING_PROMOTION_MESSAGE":
        if text.strip().lower() == "cancel":
            context.user_data["state"] = None
            await update.message.reply_text("✅ 操作已取消")
            return

        try:
            msg_id = await db_operations.save_promotion_message(text.strip())
            if msg_id:
                await update.message.reply_text(f"✅ 公司宣传轮播语录已添加 (ID: {msg_id})")
            else:
                await update.message.reply_text("❌ 添加失败")
        except Exception as e:
            logger.error(f"添加公司宣传轮播语录失败: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 添加失败: {e}")

        context.user_data["state"] = None
        return

    if user_state == "SETTING_ANNOUNCEMENT_INTERVAL":
        if text.strip().lower() == "cancel":
            context.user_data["state"] = None
            await update.message.reply_text("✅ 操作已取消")
            return

        try:
            interval_hours = int(text.strip())
            if interval_hours < 1:
                await update.message.reply_text("❌ 间隔必须大于0，输入 'cancel' 取消")
                return

            success = await db_operations.save_announcement_schedule(
                interval_hours=interval_hours, is_active=1
            )

            if success:
                await update.message.reply_text(
                    f"✅ 发送间隔已设置为 {interval_hours} 小时\n\n" f"注意：需要重启机器人才能生效"
                )
            else:
                await update.message.reply_text("❌ 设置失败")
        except ValueError:
            await update.message.reply_text("❌ 请输入有效的数字，输入 'cancel' 取消")
        except Exception as e:
            logger.error(f"设置公告间隔失败: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 设置失败: {e}")

        context.user_data["state"] = None
        return


async def _handle_income_query_date(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理高级查询的日期输入"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    from config import ADMIN_IDS

    user_id = update.effective_user.id if update.effective_user else None
    if not user_id or user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 此功能仅限管理员使用")
        context.user_data["state"] = None
        return

    try:
        dates = text.split()
        if len(dates) == 1:
            date_str = dates[0]
            # 验证日期格式
            datetime.strptime(date_str, "%Y-%m-%d")
        elif len(dates) == 2:
            start_date = dates[0]
            end_date = dates[1]
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
            date_str = f"{start_date} {end_date}"
        else:
            await update.message.reply_text(
                "❌ 格式错误。请使用：\n格式1 (单日): 2025-12-02\n格式2 (范围): 2025-12-01 2025-12-31"
            )
            return

        # 保存日期，显示类型选择界面
        context.user_data["income_query"] = context.user_data.get("income_query", {})
        context.user_data["income_query"]["date"] = date_str
        context.user_data["state"] = None

        keyboard = [
            [
                InlineKeyboardButton(
                    "订单完成", callback_data=f"income_query_type_completed_{date_str}"
                ),
                InlineKeyboardButton(
                    "违约完成", callback_data=f"income_query_type_breach_end_{date_str}"
                ),
            ],
            [
                InlineKeyboardButton(
                    "利息收入", callback_data=f"income_query_type_interest_{date_str}"
                ),
                InlineKeyboardButton(
                    "本金减少", callback_data=f"income_query_type_principal_reduction_{date_str}"
                ),
            ],
            [InlineKeyboardButton("全部类型", callback_data=f"income_query_type_all_{date_str}")],
            [InlineKeyboardButton("🔙 取消", callback_data="income_advanced_query")],
        ]

        await update.message.reply_text(
            f"📅 已选择日期: {date_str}\n\n" "🔍 请选择收入类型：",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    except ValueError:
        await update.message.reply_text("❌ 日期格式错误。请使用 YYYY-MM-DD 格式")
    except Exception as e:
        logger.error(f"处理日期输入出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ 错误: {e}")
        context.user_data["state"] = None


async def _handle_breach_end_amount(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理违约完成金额输入"""
    try:
        amount = float(text)
        if amount <= 0:
            msg = "❌ Amount must be positive"
            await update.message.reply_text(msg)
            return

        chat_id = context.user_data.get("breach_end_chat_id")
        if not chat_id:
            msg = "❌ State Error. Please retry."
            await update.message.reply_text(msg)
            context.user_data["state"] = None
            return

        order = await db_operations.get_order_by_chat_id(chat_id)
        if not order or order["state"] != "breach":
            msg = "❌ Order state changed or not found"
            await update.message.reply_text(msg)
            context.user_data["state"] = None
            return

        # 执行完成逻辑
        await db_operations.update_order_state(chat_id, "breach_end")
        group_id = order["group_id"]

        # 先记录收入明细（源数据），再更新统计数据，确保数据一致性
        from utils.date_helpers import get_daily_period_date

        user_id = update.effective_user.id if update.effective_user else None
        try:
            # 1. 先记录收入明细（如果失败，不更新统计数据）
            await db_operations.record_income(
                date=get_daily_period_date(),
                type="breach_end",
                amount=amount,
                group_id=group_id,
                order_id=order["order_id"],
                order_date=order["date"],
                customer=order["customer"],
                weekday_group=order["weekday_group"],
                note="违约完成",
                created_by=user_id,
            )
        except Exception as e:
            logger.error(f"记录违约完成收入明细失败: {e}", exc_info=True)
            msg = f"❌ 记录收入明细失败，请稍后重试或联系管理员。错误: {str(e)}"
            await update.message.reply_text(msg)
            return

        # 2. 收入明细记录成功后，再更新统计数据
        try:
            # 违约完成订单增加，金额增加
            await update_all_stats("breach_end", amount, 1, group_id)
            # 更新流动资金
            await update_liquid_capital(amount)
        except Exception as e:
            logger.error(f"更新违约完成统计数据失败: {e}", exc_info=True)
            # 统计数据更新失败，但收入明细已记录，需要手动修复或重新计算
            msg = f"❌ 更新统计失败，但收入明细已记录。请使用 /fix_statistics 修复统计数据。错误: {str(e)}"
            await update.message.reply_text(msg)
            return

        # 记录操作历史（用于撤销）
        if user_id:
            from handlers.undo_handlers import reset_undo_count

            await db_operations.record_operation(
                user_id=user_id,
                operation_type="order_breach_end",
                operation_data={
                    "chat_id": chat_id,
                    "order_id": order["order_id"],
                    "group_id": group_id,
                    "amount": amount,
                    "date": get_daily_period_date(),
                },
                chat_id=chat_id,
            )
            reset_undo_count(context, user_id)

        msg_en = f"✅ Breach Order Ended\nAmount: {amount:.2f}"

        # 删除之前的提示消息（提示消息在订单所在的群组中）
        prompt_msg_id = context.user_data.get("breach_end_prompt_msg_id")
        if prompt_msg_id:
            try:
                # 使用订单所在的chat_id删除提示消息
                await context.bot.delete_message(chat_id=chat_id, message_id=prompt_msg_id)
            except Exception as e:
                logger.debug(f"删除提示消息失败（可能已被删除）: {e}")
            context.user_data.pop("breach_end_prompt_msg_id", None)

        # 如果当前聊天不是订单所在的聊天，通知群组
        if update.effective_chat.id != chat_id:
            await context.bot.send_message(chat_id=chat_id, text=msg_en)
            await update.message.reply_text(msg_en + f"\nOrder ID: {order['order_id']}")
        else:
            await update.message.reply_text(msg_en)

        context.user_data["state"] = None

    except ValueError:
        msg = "❌ Invalid amount. Please enter a number."
        await update.message.reply_text(msg)
    except Exception as e:
        logger.error(f"处理违约完成时出错: {e}", exc_info=True)
        msg = f"⚠️ Error: {e}"
        await update.message.reply_text(msg)


async def _handle_expense_query(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, user_state: str
):
    """处理开销查询"""
    try:
        dates = text.split()
        if len(dates) == 1:
            start_date = end_date = dates[0]
        elif len(dates) == 2:
            start_date = dates[0]
            end_date = dates[1]
        else:
            await update.message.reply_text(
                "❌ Format Error. Use 'YYYY-MM-DD' or 'YYYY-MM-DD YYYY-MM-DD'"
            )
            return

        # 验证日期格式
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        expense_type = "company" if user_state == "QUERY_EXPENSE_COMPANY" else "other"
        records = await db_operations.get_expense_records(start_date, end_date, expense_type)

        title = "Company Expense" if expense_type == "company" else "Other Expense"
        msg = f"🔍 {title} Query ({start_date} to {end_date}):\n\n"

        if not records:
            msg += "No records found.\n"
        else:
            # 显示最新的20条记录（记录已按日期降序排列）
            display_records = records[:20] if len(records) > 20 else records
            real_total = sum(r["amount"] for r in records)

            for r in display_records:
                msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or 'No Note'}\n"

            if len(records) > 20:
                msg += f"\n... (Total {len(records)} records, showing latest 20)\n"
            msg += f"\nTotal: {real_total:.2f}\n"

        back_callback = (
            "report_record_company" if expense_type == "company" else "report_record_other"
        )
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=back_callback)]]
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        context.user_data["state"] = None

    except ValueError:
        await update.message.reply_text("❌ Invalid Date Format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"查询开销出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ Error: {e}")


async def _handle_expense_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, user_state: str
):
    """处理开销输入"""
    # 检查权限：只有管理员或授权员工可以录入开销
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        await update.message.reply_text("❌ 无法获取用户信息")
        context.user_data["state"] = None
        return

    from config import ADMIN_IDS

    is_admin = user_id in ADMIN_IDS
    is_authorized = await db_operations.is_user_authorized(user_id)

    if not is_admin and not is_authorized:
        await update.message.reply_text("❌ 您没有权限录入开销（仅限员工和管理员）")
        context.user_data["state"] = None
        return

    try:
        # 格式: 金额 备注
        parts = text.strip().split(maxsplit=1)
        if len(parts) < 2:
            amount_str = parts[0]
            note = "No Note"
        else:
            amount_str, note = parts

        amount = float(amount_str)
        if amount <= 0:
            await update.message.reply_text("❌ Amount must be positive")
            return

        expense_type = "company" if user_state == "WAITING_EXPENSE_COMPANY" else "other"
        date_str = get_daily_period_date()

        # 记录开销
        expense_id = await db_operations.record_expense(date_str, expense_type, amount, note)

        # 记录操作历史（用于撤销）
        from handlers.undo_handlers import reset_undo_count

        # 记录操作历史（用于撤销）- 使用当前聊天环境的 chat_id
        current_chat_id = update.effective_chat.id if update.effective_chat else None
        if current_chat_id and user_id:
            await db_operations.record_operation(
                user_id=user_id,
                operation_type="expense",
                operation_data={
                    "amount": amount,
                    "type": expense_type,
                    "note": note,
                    "date": date_str,
                    "expense_record_id": expense_id,
                },
                chat_id=current_chat_id,  # 当前操作发生的聊天环境
            )
        # 重置撤销计数
        reset_undo_count(context, user_id)

        financial_data = await db_operations.get_financial_data()
        await update.message.reply_text(
            f"✅ Expense Recorded\n"
            f"Type: {'Company' if expense_type == 'company' else 'Other'}\n"
            f"Amount: {amount:.2f}\n"
            f"Note: {note}\n"
            f"Current Balance: {financial_data['liquid_funds']:.2f}"
        )
        context.user_data["state"] = None

    except ValueError:
        await update.message.reply_text("❌ Invalid Format. Example: 100 Server Cost")
    except Exception as e:
        logger.error(f"记录开销时出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ Error: {e}")


async def _handle_search_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理搜索输入"""
    # 解析搜索条件
    criteria = {}
    try:
        # 支持 key=value 格式
        if "=" in text:
            parts = text.split()
            for part in parts:
                if "=" in part:
                    key, value = part.split("=", 1)
                    key = key.strip().lower()
                    value = value.strip()

                    # 映射别名
                    if key == "group":
                        key = "weekday_group"
                        if value.startswith("周") and len(value) == 2:
                            value = value[1]

                    if key in ["group_id", "state", "customer", "order_id", "weekday_group"]:
                        criteria[key] = value
        else:
            # 智能识别
            val = text.strip()
            # 1. 星期分组
            if val in ["一", "二", "三", "四", "五", "六", "日"]:
                criteria["weekday_group"] = val
            elif (
                val.startswith("周")
                and len(val) == 2
                and val[1] in ["一", "二", "三", "四", "五", "六", "日"]
            ):
                criteria["weekday_group"] = val[1]
            # 2. 客户类型
            elif val.upper() in ["A", "B"]:
                criteria["customer"] = val.upper()
            # 3. 状态
            elif val in [
                "normal",
                "overdue",
                "breach",
                "end",
                "breach_end",
                "正常",
                "逾期",
                "违约",
                "完成",
                "违约完成",
            ]:
                state_map = {
                    "正常": "normal",
                    "逾期": "overdue",
                    "违约": "breach",
                    "完成": "end",
                    "违约完成": "breach_end",
                }
                criteria["state"] = state_map.get(val, val)
            # 4. 归属ID
            elif len(val) == 3 and val[0].isalpha() and val[1:].isdigit():
                criteria["group_id"] = val.upper()
            # 5. 默认按订单ID
            else:
                criteria["order_id"] = val

        if not criteria:
            await update.message.reply_text(
                "❌ Cannot recognize search criteria", parse_mode="Markdown"
            )
            return

        orders = await db_operations.search_orders_advanced(criteria)

        if not orders:
            await update.message.reply_text("❌ No matching orders found")
            context.user_data["state"] = None
            return

        # 锁定群组
        locked_groups = list(set(order["chat_id"] for order in orders))
        context.user_data["locked_groups"] = locked_groups

        await update.message.reply_text(
            f"✅ Found {len(orders)} orders in {len(locked_groups)} groups.\n"
            f"Groups locked. You can now use 【Broadcast】 feature.\n"
            f"Enter 'cancel' to exit search mode (locks retained)."
        )
        # 退出输入状态，但保留 locked_groups
        context.user_data["state"] = None

    except Exception as e:
        logger.error(f"搜索出错: {e}")
        await update.message.reply_text(f"⚠️ Search Error: {e}")
        context.user_data["state"] = None


async def _handle_search_amount_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
):
    """处理按总有效金额查找输入"""

    from utils.amount_helpers import distribute_orders_evenly_by_weekday, parse_amount
    from utils.message_helpers import display_search_results_helper

    try:
        # 解析金额
        target_amount = parse_amount(text)
        if target_amount is None or target_amount <= 0:
            await update.message.reply_text(
                "❌ 无效的金额格式\n\n"
                "请输入有效的金额，例如：\n"
                "• 20万\n"
                "• 200000\n\n"
                "输入 'cancel' 取消"
            )
            return

        # 发送处理中消息
        processing_msg = await update.message.reply_text("⏳ 正在查找订单，请稍候...")

        # 获取所有有效订单（normal和overdue状态）
        criteria = {}
        all_valid_orders = await db_operations.search_orders_advanced(criteria)

        if not all_valid_orders:
            try:
                await processing_msg.delete()
            except Exception:
                pass
            await update.message.reply_text("❌ 没有找到有效订单")
            context.user_data["state"] = None
            return

        # 计算总有效金额
        total_valid_amount = sum(order.get("amount", 0) for order in all_valid_orders)

        if total_valid_amount < target_amount:
            try:
                await processing_msg.delete()
            except Exception:
                pass
            await update.message.reply_text(
                f"❌ 总有效金额不足\n\n"
                f"目标金额: {target_amount:,.2f}\n"
                f"当前总有效金额: {total_valid_amount:,.2f}\n"
                f"差额: {target_amount - total_valid_amount:,.2f}"
            )
            context.user_data["state"] = None
            return

        # 均匀分配选择订单
        try:
            selected_orders = distribute_orders_evenly_by_weekday(all_valid_orders, target_amount)
        except Exception as e:
            logger.error(f"分配订单时出错: {e}", exc_info=True)
            try:
                await processing_msg.delete()
            except Exception:
                pass
            await update.message.reply_text(f"⚠️ 处理订单时出错: {e}")
            context.user_data["state"] = None
            return

        if not selected_orders:
            try:
                await processing_msg.delete()
            except Exception:
                pass
            await update.message.reply_text("❌ 无法选择订单，请尝试调整目标金额")
            context.user_data["state"] = None
            return

        # 删除处理中消息
        try:
            await processing_msg.delete()
        except:
            pass

        # 计算选中订单的总金额
        selected_amount = sum(order.get("amount", 0) for order in selected_orders)
        selected_count = len(selected_orders)

        # 按星期分组统计
        weekday_stats = {}
        for order in selected_orders:
            weekday = order.get("weekday_group", "未知")
            if weekday not in weekday_stats:
                weekday_stats[weekday] = {"count": 0, "amount": 0.0}
            weekday_stats[weekday]["count"] += 1
            weekday_stats[weekday]["amount"] += order.get("amount", 0)

        # 计算每天的目标金额和实际金额
        daily_target = target_amount / 7
        weekday_names = ["一", "二", "三", "四", "五", "六", "日"]

        # 显示结果
        result_msg = (
            f"💰 按总有效金额查找结果\n\n"
            f"目标金额: {target_amount:,.2f}\n"
            f"选中金额: {selected_amount:,.2f}\n"
            f"差额: {target_amount - selected_amount:,.2f}\n"
            f"选中订单数: {selected_count}\n\n"
            f"按星期分组统计（目标: {daily_target:,.2f}/天）:\n"
        )

        for weekday in weekday_names:
            if weekday in weekday_stats:
                stats = weekday_stats[weekday]
                actual_amount = stats["amount"]
                diff = actual_amount - daily_target
                diff_pct = (diff / daily_target * 100) if daily_target > 0 else 0
                diff_sign = "+" if diff >= 0 else ""
                result_msg += (
                    f"周{weekday}: {stats['count']}个订单, "
                    f"{actual_amount:,.2f} "
                    f"({diff_sign}{diff:,.2f}, {diff_sign}{diff_pct:.1f}%)\n"
                )
            else:
                result_msg += f"周{weekday}: 0个订单, 0.00 (未选择)\n"

        await update.message.reply_text(result_msg)

        # 使用display_search_results_helper显示结果并锁定群组
        try:
            await display_search_results_helper(update, context, selected_orders)
        except Exception as e:
            logger.error(f"显示搜索结果时出错: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ 显示结果时出错: {e}")

        context.user_data["state"] = None

    except Exception as e:
        logger.error(f"按金额查找出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ 查找出错: {e}")
        context.user_data["state"] = None


async def _handle_report_search(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理报表查找输入"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    # 解析搜索条件
    criteria = {}
    try:
        # 支持空格分隔的多个条件
        parts = text.strip().split()

        for part in parts:
            part = part.strip()
            # 1. 星期分组（一、二、三、四、五、六、日）
            if part in ["一", "二", "三", "四", "五", "六", "日"]:
                criteria["weekday_group"] = part
            elif (
                part.startswith("周")
                and len(part) == 2
                and part[1] in ["一", "二", "三", "四", "五", "六", "日"]
            ):
                criteria["weekday_group"] = part[1]
            # 2. 状态（正常、逾期、违约、完成、违约完成）
            elif part in [
                "正常",
                "逾期",
                "违约",
                "完成",
                "违约完成",
                "normal",
                "overdue",
                "breach",
                "end",
                "breach_end",
            ]:
                state_map = {
                    "正常": "normal",
                    "逾期": "overdue",
                    "违约": "breach",
                    "完成": "end",
                    "违约完成": "breach_end",
                }
                criteria["state"] = state_map.get(part, part)
            # 3. 归属ID（S01格式）
            elif len(part) == 3 and part[0].isalpha() and part[1:].isdigit():
                criteria["group_id"] = part.upper()
            # 4. 客户类型
            elif part.upper() in ["A", "B"]:
                criteria["customer"] = part.upper()

        if not criteria:
            await update.message.reply_text(
                "❌ 无法识别查询条件\n\n示例：\n• S01\n• 三 正常\n• S01 正常"
            )
            return

        # 执行查找：如果用户指定了状态，查找所有状态的订单；否则默认只查找有效订单
        if "state" in criteria and criteria["state"]:
            # 用户指定了状态，可以查找所有状态（包括完成、违约完成等）
            orders = await db_operations.search_orders_advanced_all_states(criteria)
        else:
            # 用户未指定状态，默认只查找有效订单（normal和overdue）
            orders = await db_operations.search_orders_advanced(criteria)

        if not orders:
            await update.message.reply_text("❌ 未找到匹配的订单")
            context.user_data["state"] = None
            return

        # 计算订单数量和金额
        order_count = len(orders)
        total_amount = sum(order.get("amount", 0) for order in orders)

        # 锁定群组
        locked_groups = list(set(order["chat_id"] for order in orders))
        context.user_data["locked_groups"] = locked_groups

        # 显示结果
        result_msg = (
            f"📊 查找结果\n\n"
            f"订单数量: {order_count}\n"
            f"订单金额: {total_amount:,.2f}\n"
            f"群组数量: {len(locked_groups)}"
        )

        # 保存查找结果到context，用于后续修改归属
        context.user_data["report_search_orders"] = orders

        # 添加群发和修改归属按钮
        keyboard = [
            [
                InlineKeyboardButton("📢 群发消息", callback_data="broadcast_start"),
                InlineKeyboardButton("🔄 修改归属", callback_data="report_change_attribution"),
            ],
            [InlineKeyboardButton("🔙 返回", callback_data="report_menu_attribution")],
        ]

        await update.message.reply_text(result_msg, reply_markup=InlineKeyboardMarkup(keyboard))

        # 退出输入状态，但保留 locked_groups 和查找结果
        context.user_data["state"] = None

    except Exception as e:
        logger.error(f"报表查找出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ 查找出错: {e}")
        context.user_data["state"] = None


async def _handle_update_balance(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, account_type: str
):
    """处理更新余额输入"""
    try:
        new_balance = float(text)

        # 先检查账户是否存在
        account = await db_operations.get_payment_account(account_type)
        if not account:
            await update.message.reply_text(f"❌ 未找到{account_type.upper()}账户，请先添加账户")
            context.user_data["state"] = None
            return

        # 获取旧余额
        accounts = await db_operations.get_payment_accounts_by_type(account_type)
        old_balance = accounts[0].get("balance", 0) if accounts else 0

        success = await db_operations.update_payment_account(account_type, balance=new_balance)

        if success:
            # 记录操作历史
            user_id = update.effective_user.id if update.effective_user else None
            current_chat_id = update.effective_chat.id if update.effective_chat else None
            if current_chat_id and user_id:
                await db_operations.record_operation(
                    user_id=user_id,
                    operation_type="payment_account_balance_updated",
                    operation_data={
                        "account_type": account_type,
                        "old_balance": old_balance,
                        "new_balance": new_balance,
                    },
                    chat_id=current_chat_id,
                )

            account_name = "GCASH" if account_type == "gcash" else "PayMaya"
            # 验证更新是否成功
            updated_account = await db_operations.get_payment_account(account_type)
            if updated_account and abs(updated_account.get("balance", 0) - new_balance) < 0.01:
                await update.message.reply_text(
                    f"✅ {account_name}余额已更新为: {new_balance:,.2f}"
                )
                # 重新显示账号信息
                if account_type == "gcash":
                    from handlers.payment_handlers import show_gcash

                    await show_gcash(update, context)
                else:
                    from handlers.payment_handlers import show_paymaya

                    await show_paymaya(update, context)
            else:
                actual_balance = updated_account.get("balance", 0) if updated_account else 0
                await update.message.reply_text(
                    f"⚠️ 更新可能未生效\n"
                    f"期望值: {new_balance:,.2f}\n"
                    f"实际值: {actual_balance:,.2f}\n"
                    f"请重试或检查数据库"
                )
        else:
            await update.message.reply_text(
                "❌ 更新失败\n"
                "请检查：\n"
                "1. 账户是否存在\n"
                "2. 数据库连接是否正常\n"
                "3. 是否有权限"
            )

        context.user_data["state"] = None
    except ValueError:
        await update.message.reply_text("❌ 请输入有效的数字")
    except Exception as e:
        logger.error(f"更新余额时出错: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 更新时发生错误: {e}")


async def _handle_update_balance_by_id(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
):
    """处理按ID更新余额输入"""
    try:
        # 从user_state中提取账户ID
        user_state = context.user_data.get("state", "")
        if not user_state.startswith("UPDATING_BALANCE_BY_ID_"):
            await update.message.reply_text("❌ 错误：状态异常")
            context.user_data["state"] = None
            return

        account_id = context.user_data.get("updating_balance_account_id")
        if not account_id:
            # 尝试从state中提取
            try:
                account_id = int(user_state.replace("UPDATING_BALANCE_BY_ID_", ""))
            except ValueError:
                await update.message.reply_text("❌ 错误：找不到账户ID")
                context.user_data["state"] = None
                return

        # 解析金额
        new_balance = float(text.strip())

        # 获取账户信息
        account = await db_operations.get_payment_account_by_id(account_id)
        if not account:
            await update.message.reply_text("❌ 账户不存在")
            context.user_data["state"] = None
            context.user_data.pop("updating_balance_account_id", None)
            return

        account_type = account.get("account_type", "")
        account_name = account.get("account_name", "未设置")
        account_number = account.get("account_number", "未设置")

        # 更新余额
        success = await db_operations.update_payment_account_by_id(account_id, balance=new_balance)

        if success:
            type_name = "GCASH" if account_type == "gcash" else "PayMaya"
            display_name = (
                account_name if account_name and account_name != "未设置" else account_number
            )

            # 验证更新是否成功
            updated_account = await db_operations.get_payment_account_by_id(account_id)
            if updated_account and abs(updated_account.get("balance", 0) - new_balance) < 0.01:
                # 记录操作历史
                user_id = update.effective_user.id if update.effective_user else None
                current_chat_id = update.effective_chat.id if update.effective_chat else None
                if current_chat_id and user_id:
                    await db_operations.record_operation(
                        user_id=user_id,
                        operation_type="payment_account_balance_updated",
                        operation_data={
                            "account_id": account_id,
                            "account_type": account_type,
                            "old_balance": account.get("balance", 0),
                            "new_balance": new_balance,
                        },
                        chat_id=current_chat_id,
                    )

                await update.message.reply_text(
                    f"✅ {type_name}账户余额已更新\n\n"
                    f"账户: {display_name}\n"
                    f"账号: {account_number}\n"
                    f"新余额: {new_balance:,.2f}"
                )
                # 重新显示账户列表
                if account_type == "gcash":
                    from handlers.payment_handlers import show_gcash

                    await show_gcash(update, context)
                else:
                    from handlers.payment_handlers import show_paymaya

                    await show_paymaya(update, context)
            else:
                actual_balance = updated_account.get("balance", 0) if updated_account else 0
                await update.message.reply_text(
                    f"⚠️ 更新可能未生效\n"
                    f"期望值: {new_balance:,.2f}\n"
                    f"实际值: {actual_balance:,.2f}\n"
                    f"请重试或检查数据库"
                )
        else:
            await update.message.reply_text(
                "❌ 更新失败\n"
                "请检查：\n"
                "1. 账户是否存在\n"
                "2. 数据库连接是否正常\n"
                "3. 是否有权限"
            )

        context.user_data["state"] = None
        context.user_data.pop("updating_balance_account_id", None)

    except ValueError:
        await update.message.reply_text("❌ 请输入有效的数字")
        context.user_data["state"] = None
    except Exception as e:
        logger.error(f"按ID更新余额时出错: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 更新时发生错误: {e}")
        context.user_data["state"] = None
        context.user_data.pop("updating_balance_account_id", None)


async def _handle_add_account(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, account_type: str
):
    """处理添加账户输入"""
    parts = text.strip().split(maxsplit=1)

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ 格式错误\n" "格式: <账号号码> <账户名称>\n" "示例: 09171234567 张三"
        )
        return

    account_number = parts[0]
    account_name = parts[1]

    account_id = await db_operations.create_payment_account(
        account_type, account_number, account_name
    )

    if account_id:
        account_name_display = "GCASH" if account_type == "gcash" else "PayMaya"
        await update.message.reply_text(
            f"✅ {account_name_display}账户已添加\n\n"
            f"账号号码: {account_number}\n"
            f"账户名称: {account_name}"
        )
        # 重新显示账户列表
        if account_type == "gcash":
            from handlers.payment_handlers import show_gcash

            await show_gcash(update, context)
        else:
            from handlers.payment_handlers import show_paymaya

            await show_paymaya(update, context)
    else:
        await update.message.reply_text("❌ 添加失败")

    context.user_data["state"] = None


async def _handle_edit_account(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, account_type: str
):
    """处理编辑账号输入（兼容旧代码）"""
    parts = text.strip().split(maxsplit=1)

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ 格式错误\n" "格式: <账号号码> <账户名称>\n" "示例: 09171234567 张三"
        )
        return

    account_number = parts[0]
    account_name = parts[1]

    success = await db_operations.update_payment_account(
        account_type, account_number=account_number, account_name=account_name
    )

    if success:
        # 记录操作历史
        user_id = update.effective_user.id if update.effective_user else None
        current_chat_id = update.effective_chat.id if update.effective_chat else None
        if current_chat_id and user_id:
            await db_operations.record_operation(
                user_id=user_id,
                operation_type="payment_account_updated",
                operation_data={
                    "account_type": account_type,
                    "account_number": account_number,
                    "account_name": account_name,
                },
                chat_id=current_chat_id,
            )

        account_name_display = "GCASH" if account_type == "gcash" else "PayMaya"
        await update.message.reply_text(
            f"✅ {account_name_display}账号信息已更新\n\n"
            f"账号号码: {account_number}\n"
            f"账户名称: {account_name}"
        )
        # 重新显示账号信息
        if account_type == "gcash":
            from handlers.payment_handlers import show_gcash

            await show_gcash(update, context)
        else:
            from handlers.payment_handlers import show_paymaya

            await show_paymaya(update, context)
    else:
        await update.message.reply_text("❌ 更新失败")

    context.user_data["state"] = None


async def _handle_edit_account_by_id(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, account_type: str
):
    """处理编辑账户输入（按ID）"""
    account_id = context.user_data.get("editing_account_id")
    if not account_id:
        await update.message.reply_text("❌ 错误：找不到账户ID")
        context.user_data["state"] = None
        return

    # 检查是否要删除账户
    if text.strip().lower() == "delete":
        success = await db_operations.delete_payment_account(account_id)
        if success:
            account_name_display = "GCASH" if account_type == "gcash" else "PayMaya"
            await update.message.reply_text(f"✅ {account_name_display}账户已删除")
            # 重新显示账户列表
            if account_type == "gcash":
                from handlers.payment_handlers import show_gcash

                await show_gcash(update, context)
            else:
                from handlers.payment_handlers import show_paymaya

                await show_paymaya(update, context)
        else:
            await update.message.reply_text("❌ 删除失败")
        context.user_data["state"] = None
        context.user_data.pop("editing_account_id", None)
        return

    parts = text.strip().split(maxsplit=1)

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ 格式错误\n"
            "格式: <账号号码> <账户名称>\n"
            "示例: 09171234567 张三\n\n"
            "💡 提示：输入 'delete' 可以删除此账户"
        )
        return

    account_number = parts[0]
    account_name = parts[1]

    success = await db_operations.update_payment_account_by_id(
        account_id, account_number=account_number, account_name=account_name
    )

    if success:
        # 记录操作历史
        user_id = update.effective_user.id if update.effective_user else None
        current_chat_id = update.effective_chat.id if update.effective_chat else None
        if current_chat_id and user_id:
            await db_operations.record_operation(
                user_id=user_id,
                operation_type="payment_account_updated",
                operation_data={
                    "account_id": account_id,
                    "account_type": account_type,
                    "account_number": account_number,
                    "account_name": account_name,
                },
                chat_id=current_chat_id,
            )

        account_name_display = "GCASH" if account_type == "gcash" else "PayMaya"
        await update.message.reply_text(
            f"✅ {account_name_display}账户信息已更新\n\n"
            f"账号号码: {account_number}\n"
            f"账户名称: {account_name}"
        )
        # 重新显示账户列表
        if account_type == "gcash":
            from handlers.payment_handlers import show_gcash

            await show_gcash(update, context)
        else:
            from handlers.payment_handlers import show_paymaya

            await show_paymaya(update, context)
    else:
        await update.message.reply_text("❌ 更新失败")

    context.user_data["state"] = None
    context.user_data.pop("editing_account_id", None)


async def _handle_report_query(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理报表查询"""
    from handlers.report_handlers import generate_report_text

    user_id = update.effective_user.id if update.effective_user else None
    group_id = context.user_data.get("report_group_id")

    # 检查用户权限限制
    if user_id:
        user_group_id = await db_operations.get_user_group_id(user_id)
        if user_group_id:
            # 用户有权限限制，强制使用用户的归属ID
            group_id = user_group_id

    # 解析日期
    try:
        dates = text.split()
        if len(dates) == 1:
            start_date = end_date = dates[0]
        elif len(dates) == 2:
            start_date = dates[0]
            end_date = dates[1]
        else:
            await update.message.reply_text(
                "❌ Format Error. Use 'YYYY-MM-DD' or 'YYYY-MM-DD YYYY-MM-DD'"
            )
            return

        # 验证日期格式
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        # 检查用户权限限制，如果有权限限制，不显示开销与余额
        show_expenses = True
        if user_id:
            user_group_id = await db_operations.get_user_group_id(user_id)
            if user_group_id:
                show_expenses = False

        # 生成报表
        report_text = await generate_report_text(
            "query", start_date, end_date, group_id, show_expenses=show_expenses
        )

        # 键盘
        keyboard = [
            [
                InlineKeyboardButton(
                    "📄 今日报表",
                    callback_data=f"report_view_today_{group_id if group_id else 'ALL'}",
                ),
                InlineKeyboardButton(
                    "📅 月报", callback_data=f"report_view_month_{group_id if group_id else 'ALL'}"
                ),
            ],
            [
                InlineKeyboardButton(
                    "📆 日期查询",
                    callback_data=f"report_view_query_{group_id if group_id else 'ALL'}",
                )
            ],
        ]

        # Telegram消息最大长度限制为4096字符，如果报表太长则分段发送
        MAX_MESSAGE_LENGTH = 4096
        if len(report_text) > MAX_MESSAGE_LENGTH:
            # 分段发送
            chunks = []
            current_chunk = ""
            for line in report_text.split("\n"):
                if len(current_chunk) + len(line) + 1 > MAX_MESSAGE_LENGTH - 200:  # 留200字符余量
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = line + "\n"
                else:
                    current_chunk += line + "\n"
            if current_chunk:
                chunks.append(current_chunk)

            # 发送第一段（带按钮）
            if chunks:
                first_chunk = chunks[0]
                if len(chunks) > 1:
                    first_chunk += f"\n\n⚠️ 报表内容较长，已分段显示 ({len(chunks)}段)"
                await update.message.reply_text(
                    first_chunk, reply_markup=InlineKeyboardMarkup(keyboard)
                )

                # 发送剩余段
                for i, chunk in enumerate(chunks[1:], 2):
                    await update.message.reply_text(f"[第 {i}/{len(chunks)} 段]\n\n{chunk}")
        else:
            await update.message.reply_text(
                report_text, reply_markup=InlineKeyboardMarkup(keyboard)
            )
        context.user_data["state"] = None

    except ValueError:
        await update.message.reply_text("❌ Invalid Date Format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"查询报表出错: {e}")
        await update.message.reply_text(f"⚠️ Query Error: {e}")
        context.user_data["state"] = None


async def _handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理群发消息"""
    locked_groups = context.user_data.get("locked_groups", [])
    if not locked_groups:
        await update.message.reply_text("⚠️ No locked groups")
        context.user_data["state"] = None
        return

    success_count = 0
    fail_count = 0

    await update.message.reply_text(f"⏳ Sending message to {len(locked_groups)} groups...")

    for chat_id in locked_groups:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
            success_count += 1
        except Exception as e:
            logger.error(f"群发失败 {chat_id}: {e}")
            fail_count += 1

    await update.message.reply_text(
        f"✅ Broadcast Completed\n" f"Success: {success_count}\n" f"Failed: {fail_count}"
    )
    context.user_data["state"] = None
