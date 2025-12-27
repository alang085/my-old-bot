"""群组消息回调处理器"""

# 标准库
import logging

# 第三方库
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS

# 本地模块
from handlers.data_access import (
    delete_anti_fraud_message_for_callback,
    delete_promotion_message_for_callback,
    get_all_anti_fraud_messages_for_callback,
    get_all_promotion_messages_for_callback,
    get_group_message_config_by_chat_id_for_callback,
    get_group_message_configs_for_callback,
    toggle_anti_fraud_message_for_callback,
    toggle_promotion_message_for_callback,
)
from utils.callback_helpers import safe_edit_message_text, safe_query_reply_text

logger = logging.getLogger(__name__)


async def _refresh_group_message_list(query, configs):
    """刷新群组消息列表（辅助函数，避免递归调用）"""
    try:
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
                msg += f"   状态: {status}\n\n"

        keyboard = [
            [InlineKeyboardButton("➕ 添加总群/频道", callback_data="groupmsg_add")],
            [InlineKeyboardButton("🔄 刷新", callback_data="groupmsg_refresh")],
        ]

        # 为每个群组添加启用/禁用按钮和设置链接按钮
        for config in configs:
            chat_id = config.get("chat_id")
            chat_title = config.get("chat_title", f"ID: {chat_id}")
            is_active = config.get("is_active", 0)
            action_text = "❌ 禁用" if is_active else "✅ 启用"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{action_text} - {chat_title}", callback_data=f"groupmsg_toggle_{chat_id}"
                    ),
                    InlineKeyboardButton(
                        "🔗 设置链接", callback_data=f"groupmsg_set_links_{chat_id}"
                    ),
                ]
            )

        await safe_edit_message_text(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"刷新群组消息列表失败: {e}", exc_info=True)


# 注意：不要在函数上使用 @authorized_required，因为在 main.py 中注册时已经使用了
async def handle_group_message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理群组消息相关的回调"""
    query = update.callback_query
    if not query:
        logger.error("handle_group_message_callback: query is None")
        return

    data = query.data
    if not data:
        logger.error("handle_group_message_callback: data is None")
        return

    # 记录回调数据以便调试
    logger.info(
        f"处理群组消息回调: {data}, 用户ID: {update.effective_user.id if update.effective_user else 'None'}"
    )

    # 注意：不要在这里统一 answer，因为某些回调需要显示特定的提示信息
    # 每个回调处理函数会自己负责 answer

    if data == "groupmsg_refresh":
        logger.info("处理刷新回调")
        try:
            configs = await get_group_message_configs_for_callback()

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
                    msg += f"   状态: {status}\n\n"

            keyboard = [
                [InlineKeyboardButton("➕ 添加总群/频道", callback_data="groupmsg_add")],
                [InlineKeyboardButton("🔄 刷新", callback_data="groupmsg_refresh")],
            ]

            # 为每个群组添加启用/禁用按钮和设置链接按钮
            for config in configs:
                chat_id = config.get("chat_id")
                chat_title = config.get("chat_title", f"ID: {chat_id}")
                is_active = config.get("is_active", 0)
                action_text = "❌ 禁用" if is_active else "✅ 启用"
                keyboard.append(
                    [
                        InlineKeyboardButton(
                            f"{action_text} - {chat_title}",
                            callback_data=f"groupmsg_toggle_{chat_id}",
                        ),
                        InlineKeyboardButton(
                            "🔗 设置链接", callback_data=f"groupmsg_set_links_{chat_id}"
                        ),
                    ]
                )

            await safe_edit_message_text(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"处理刷新回调失败: {e}", exc_info=True)
            await query.answer("❌ 操作失败", show_alert=True)
        return

    elif data == "groupmsg_add":
        # 先 answer，防止客户端转圈
        try:
            await query.answer()
        except Exception:
            # Telegram API调用失败（如query已过期），忽略即可
            pass

        try:
            await safe_query_reply_text(
                query,
                "请输入群组ID：\n"
                "格式: 数字（如：-1001234567890）\n"
                "输入 'cancel' 取消\n\n"
                "💡 提示：在群组中使用 /groupmsg_getid 获取群组ID",
            )
        except Exception as e:
            logger.error(f"发送群组ID提示失败: {e}", exc_info=True)
            await query.answer("请输入群组ID", show_alert=True)
        context.user_data["state"] = "ADDING_GROUP_CONFIG"

    elif data.startswith("groupmsg_toggle_"):
        # 切换群组启用/禁用状态
        try:
            await query.answer()
        except Exception:
            pass

        try:
            chat_id = int(data.split("_")[-1])
            config = await get_group_message_config_by_chat_id_for_callback(chat_id)

            if not config:
                await query.answer("❌ 配置不存在", show_alert=True)
                return

            # 切换状态
            current_status = config.get("is_active", 0)
            new_status = 0 if current_status else 1

            import db_operations

            success = await db_operations.save_group_message_config(
                chat_id=chat_id, is_active=new_status
            )

            if success:
                status_text = "已启用" if new_status else "已禁用"
                try:
                    await query.answer(f"✅ {status_text}")
                except Exception:
                    pass  # Query 可能已过期，忽略错误

                # 刷新界面 - 使用辅助函数避免递归调用
                try:
                    configs = await get_group_message_configs_for_callback()
                    await _refresh_group_message_list(query, configs)
                except Exception as e:
                    logger.error(f"刷新界面失败: {e}", exc_info=True)
            else:
                try:
                    await query.answer("❌ 更新失败", show_alert=True)
                except Exception:
                    pass
        except (ValueError, IndexError):
            await query.answer("❌ 无效的群组ID", show_alert=True)
        except Exception as e:
            logger.error(f"切换群组状态失败: {e}", exc_info=True)
            await query.answer("❌ 操作失败", show_alert=True)

    elif data.startswith("groupmsg_set_links_"):
        # 设置群组链接（机器人链接和人工链接）
        try:
            await query.answer()
        except Exception:
            pass

        try:
            chat_id = int(data.split("_")[-1])
            config = await get_group_message_config_by_chat_id_for_callback(chat_id)

            if not config:
                await query.answer("❌ 配置不存在", show_alert=True)
                return

            # 显示设置链接菜单
            chat_title = config.get("chat_title", f"ID: {chat_id}")
            current_bot_links = config.get("bot_links", "") or "未设置"
            current_worker_links = config.get("worker_links", "") or "未设置"

            msg = f"🔗 设置链接 - {chat_title}\n\n"
            msg += f"当前机器人链接:\n{current_bot_links}\n\n"
            msg += f"当前人工链接:\n{current_worker_links}\n\n"
            msg += "请选择要设置的链接类型："

            keyboard = [
                [
                    InlineKeyboardButton(
                        "🤖 设置机器人链接", callback_data=f"groupmsg_set_bot_links_{chat_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "👤 设置人工链接", callback_data=f"groupmsg_set_worker_links_{chat_id}"
                    )
                ],
                [InlineKeyboardButton("🔙 返回", callback_data="groupmsg_refresh")],
            ]

            await safe_edit_message_text(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
        except (ValueError, IndexError):
            await query.answer("❌ 无效的群组ID", show_alert=True)
        except Exception as e:
            logger.error(f"显示设置链接菜单失败: {e}", exc_info=True)
            await query.answer("❌ 操作失败", show_alert=True)

    elif data.startswith("groupmsg_set_bot_links_"):
        # 设置机器人链接
        try:
            await query.answer()
        except Exception:
            pass

        try:
            chat_id = int(data.split("_")[-1])
            from constants import USER_STATES

            context.user_data["state"] = f"{USER_STATES['SETTING_BOT_LINKS']}_{chat_id}"
            context.user_data["setting_chat_id"] = chat_id

            await safe_query_reply_text(
                query,
                "请输入机器人链接（多个链接用换行符分隔）：\n"
                "格式: https://t.me/...\n"
                "输入 'clear' 清空链接\n"
                "输入 'cancel' 取消",
            )
        except (ValueError, IndexError):
            await query.answer("❌ 无效的群组ID", show_alert=True)
        except Exception as e:
            logger.error(f"设置机器人链接失败: {e}", exc_info=True)
            await query.answer("❌ 操作失败", show_alert=True)

    elif data.startswith("groupmsg_set_worker_links_"):
        # 设置人工链接
        try:
            await query.answer()
        except Exception:
            pass

        try:
            chat_id = int(data.split("_")[-1])
            from constants import USER_STATES

            context.user_data["state"] = f"{USER_STATES['SETTING_WORKER_LINKS']}_{chat_id}"
            context.user_data["setting_chat_id"] = chat_id

            await safe_query_reply_text(
                query,
                "请输入人工链接（多个链接用换行符分隔）：\n"
                "格式: https://t.me/...\n"
                "输入 'clear' 清空链接\n"
                "输入 'cancel' 取消",
            )
        except (ValueError, IndexError):
            await query.answer("❌ 无效的群组ID", show_alert=True)
        except Exception as e:
            logger.error(f"设置人工链接失败: {e}", exc_info=True)
            await query.answer("❌ 操作失败", show_alert=True)

    # 防诈骗语录回调
    elif data == "antifraud_refresh":
        from handlers.group_message_handlers import manage_anti_fraud_messages

        await manage_anti_fraud_messages(update, context)

    elif data == "antifraud_add":
        try:
            await safe_query_reply_text(query, "请输入防诈骗语录：\n" "输入 'cancel' 取消")
        except Exception as e:
            logger.error(f"发送防诈骗语录提示失败: {e}", exc_info=True)
            await query.answer("请输入防诈骗语录", show_alert=True)
        context.user_data["state"] = "ADDING_ANTIFRAUD_MESSAGE"
        await query.answer()

    elif data == "antifraud_list":
        messages = await get_all_anti_fraud_messages_for_callback()

        if not messages:
            await query.answer("❌ 没有防诈骗语录", show_alert=True)
            return

        msg = "🛡️ 所有防诈骗语录：\n\n"
        keyboard = []

        for msg_item in messages:
            msg_id = msg_item.get("id")
            message = msg_item.get("message", "")
            is_active = msg_item.get("is_active", 0)
            status = "✅" if is_active else "❌"

            msg += f"{status} [{msg_id}] {message}\n\n"

            action = "禁用" if is_active else "启用"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{'✅' if is_active else '❌'} [{msg_id}] {action}",
                        callback_data=f"antifraud_toggle_{msg_id}",
                    ),
                    InlineKeyboardButton("🗑️ 删除", callback_data=f"antifraud_delete_{msg_id}"),
                ]
            )

        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data="antifraud_refresh")])

        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("antifraud_toggle_"):
        try:
            msg_id = int(data.split("_")[-1])
            messages = await get_all_anti_fraud_messages_for_callback()
            current = next((m for m in messages if m.get("id") == msg_id), None)

            if not current:
                await query.answer("❌ 语录不存在", show_alert=True)
                return

            success = await toggle_anti_fraud_message_for_callback(msg_id)

            if success:
                try:
                    await query.answer("✅ 状态已更新")
                except Exception:
                    pass
                # 刷新列表 - 直接调用管理函数避免递归
                try:
                    from handlers.group_message_handlers import manage_anti_fraud_messages

                    if query.message:
                        from telegram import Update as TelegramUpdate

                        refresh_update = TelegramUpdate(
                            update_id=update.update_id, callback_query=None, message=query.message
                        )
                        await manage_anti_fraud_messages(refresh_update, context)
                except Exception as e:
                    logger.error(f"刷新界面失败: {e}", exc_info=True)
            else:
                try:
                    await query.answer("❌ 更新失败", show_alert=True)
                except Exception:
                    pass
        except (ValueError, IndexError):
            try:
                await query.answer("❌ 无效的语录ID", show_alert=True)
            except Exception:
                pass

    elif data.startswith("antifraud_delete_"):
        try:
            msg_id = int(data.split("_")[-1])
            success = await delete_anti_fraud_message_for_callback(msg_id)

            if success:
                try:
                    await query.answer("✅ 语录已删除")
                except Exception:
                    pass
                # 刷新列表 - 直接调用管理函数避免递归
                try:
                    from handlers.group_message_handlers import manage_anti_fraud_messages

                    if query.message:
                        from telegram import Update as TelegramUpdate

                        refresh_update = TelegramUpdate(
                            update_id=update.update_id, callback_query=None, message=query.message
                        )
                        await manage_anti_fraud_messages(refresh_update, context)
                except Exception as e:
                    logger.error(f"刷新界面失败: {e}", exc_info=True)
            else:
                try:
                    await query.answer("❌ 删除失败", show_alert=True)
                except Exception:
                    pass
        except (ValueError, IndexError):
            await query.answer("❌ 无效的语录ID", show_alert=True)

    # 公司宣传轮播语录回调
    elif data == "promotion_refresh":
        from handlers.group_message_handlers import manage_promotion_messages

        await manage_promotion_messages(update, context)

    elif data == "promotion_add":
        try:
            await safe_query_reply_text(
                query, "Please enter company promotion message:\n" "Type 'cancel' to cancel"
            )
        except Exception as e:
            logger.error(f"Failed to send promotion message prompt: {e}", exc_info=True)
            await query.answer("Please enter company promotion message", show_alert=True)
        context.user_data["state"] = "ADDING_PROMOTION_MESSAGE"
        await query.answer()

    elif data == "promotion_list":
        messages = await get_all_promotion_messages_for_callback()

        if not messages:
            await query.answer("❌ No promotion messages", show_alert=True)
            return

        msg = "📢 All Company Promotion Messages:\n\n"
        keyboard = []

        # 检查用户是否是管理员
        user_id = query.from_user.id if query.from_user else None
        user_id in ADMIN_IDS if user_id else False

        for msg_item in messages:
            msg_id = msg_item.get("id")
            message = msg_item.get("message", "")
            is_active = msg_item.get("is_active", 0)
            status = "✅" if is_active else "❌"

            msg += f"{status} [{msg_id}] {message}\n\n"

            action = "Disable" if is_active else "Enable"
            # 所有用户都只有删除按钮
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{'✅' if is_active else '❌'} [{msg_id}] {action}",
                        callback_data=f"promotion_toggle_{msg_id}",
                    ),
                    InlineKeyboardButton("🗑️ Delete", callback_data=f"promotion_delete_{msg_id}"),
                ]
            )

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="promotion_refresh")])

        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("promotion_toggle_"):
        try:
            msg_id = int(data.split("_")[-1])
            messages = await get_all_promotion_messages_for_callback()
            current = next((m for m in messages if m.get("id") == msg_id), None)

            if not current:
                await query.answer("❌ Message not found", show_alert=True)
                return

            success = await toggle_promotion_message_for_callback(msg_id)

            if success:
                try:
                    await query.answer("✅ Status updated")
                except Exception:
                    pass
                # 刷新列表 - 直接调用管理函数避免递归
                try:
                    from handlers.group_message_handlers import manage_promotion_messages

                    if query.message:
                        from telegram import Update as TelegramUpdate

                        refresh_update = TelegramUpdate(
                            update_id=update.update_id, callback_query=None, message=query.message
                        )
                        await manage_promotion_messages(refresh_update, context)
                except Exception as e:
                    logger.error(f"刷新界面失败: {e}", exc_info=True)
            else:
                try:
                    await query.answer("❌ Update failed", show_alert=True)
                except Exception:
                    pass
        except (ValueError, IndexError):
            try:
                await query.answer("❌ Invalid message ID", show_alert=True)
            except Exception:
                pass

    elif data.startswith("promotion_delete_"):
        try:
            msg_id = int(data.split("_")[-1])
            success = await delete_promotion_message_for_callback(msg_id)

            if success:
                try:
                    await query.answer("✅ Message deleted")
                except Exception:
                    pass
                # 刷新列表 - 直接调用管理函数避免递归
                try:
                    from handlers.group_message_handlers import manage_promotion_messages

                    if query.message:
                        from telegram import Update as TelegramUpdate

                        refresh_update = TelegramUpdate(
                            update_id=update.update_id, callback_query=None, message=query.message
                        )
                        await manage_promotion_messages(refresh_update, context)
                except Exception as e:
                    logger.error(f"刷新界面失败: {e}", exc_info=True)
            else:
                try:
                    await query.answer("❌ Delete failed", show_alert=True)
                except Exception:
                    pass
        except (ValueError, IndexError):
            await query.answer("❌ Invalid message ID", show_alert=True)

    # 测试发送语录回调
    elif data == "test_promotion":
        try:
            await query.answer("🔄 Sending promotion messages...")
            from utils.schedule_executor import send_company_promotion_messages

            bot = context.bot
            await send_company_promotion_messages(bot)
            await query.edit_message_text("✅ Promotion messages sent to all groups")
        except Exception as e:
            logger.error(f"Failed to send test promotion messages: {e}", exc_info=True)
            await query.answer(f"❌ Send failed: {str(e)[:50]}", show_alert=True)

    elif data == "test_all":
        try:
            await query.answer("🔄 Sending promotion messages...")
            from utils.schedule_executor import send_company_promotion_messages

            bot = context.bot

            # Send promotion messages
            try:
                await send_company_promotion_messages(bot)
            except Exception as e:
                logger.error(f"Failed to send promotion messages: {e}", exc_info=True)

            await safe_edit_message_text(query, "✅ Promotion messages sent to all groups")
        except Exception as e:
            logger.error(f"Failed to send all test messages: {e}", exc_info=True)
            await query.answer(f"❌ 发送失败: {str(e)[:50]}", show_alert=True)

    elif data == "test_cancel":
        await safe_edit_message_text(query, "❌ 已取消测试")

    elif data.startswith("test_msg_"):
        # 处理测试消息发送回调
        try:
            await query.answer()
        except Exception:
            pass

        try:
            import random

            import db_operations
            from utils.schedule_executor import (
                _combine_message_with_anti_fraud,
                _send_group_message,
                select_rotated_message,
            )

            chat = query.message.chat
            if chat.type == "private":
                await query.answer("❌ 此功能只能在群组中使用", show_alert=True)
                return

            msg_type_map = {
                "test_msg_start_work": "start_work",
                "test_msg_end_work": "end_work",
                "test_msg_welcome": "welcome",
                "test_msg_promotion": "promotion",
            }

            msg_type = msg_type_map.get(data)
            if not msg_type:
                await query.answer("❌ 无效的消息类型", show_alert=True)
                return

            # 获取群组配置（用于获取链接，但不检查是否开启）
            config = await db_operations.get_group_message_config_by_chat_id(chat.id)
            bot_links = config.get("bot_links") if config else None
            worker_links = config.get("worker_links") if config else None

            # 获取激活的防诈骗语录
            anti_fraud_messages = await db_operations.get_active_anti_fraud_messages()

            # 根据消息类型选择消息内容
            main_message = ""
            if msg_type == "start_work":
                # 开工消息
                start_work_messages = await db_operations.get_active_start_work_messages()
                if not start_work_messages:
                    await query.answer("❌ 没有激活的开工消息", show_alert=True)
                    return
                message = random.choice(start_work_messages)
                main_message = select_rotated_message(message)

            elif msg_type == "end_work":
                # 收工消息
                end_work_messages = await db_operations.get_active_end_work_messages()
                if not end_work_messages:
                    await query.answer("❌ 没有激活的收工消息", show_alert=True)
                    return
                message = random.choice(end_work_messages)
                main_message = select_rotated_message(message)

            elif msg_type == "welcome":
                # 欢迎消息
                welcome_message = config.get("welcome_message")
                if not welcome_message:
                    await query.answer("❌ 当前群组未配置欢迎消息", show_alert=True)
                    return
                rotated_message = select_rotated_message(welcome_message)
                # 替换变量
                username = (
                    update.effective_user.username or update.effective_user.first_name or "测试用户"
                )
                chat_title = chat.title or "群组"
                main_message = rotated_message.replace("{username}", username)
                main_message = main_message.replace("{chat_title}", chat_title)

            elif msg_type == "promotion":
                # 宣传消息
                promotion_messages = await db_operations.get_active_promotion_messages()
                if not promotion_messages:
                    await query.answer("❌ 没有激活的宣传消息", show_alert=True)
                    return
                valid_messages = [
                    msg
                    for msg in promotion_messages
                    if msg.get("message") and msg.get("message").strip()
                ]
                if not valid_messages:
                    await query.answer("❌ 没有有效的宣传消息", show_alert=True)
                    return
                selected_msg_dict = random.choice(valid_messages)
                main_message = selected_msg_dict.get("message", "").strip()

            if not main_message:
                await query.answer("❌ 消息内容为空", show_alert=True)
                return

            # 组合消息：主消息 + 防诈骗语录
            final_message = _combine_message_with_anti_fraud(main_message, anti_fraud_messages)

            # 发送消息
            bot = context.bot
            if await _send_group_message(bot, chat.id, final_message, bot_links, worker_links):
                await safe_edit_message_text(query, "✅ 测试消息已发送")
                logger.info(f"测试消息已发送到群组 {chat.id} (类型: {msg_type})")
            else:
                await query.answer("❌ 发送失败，请检查日志", show_alert=True)
        except Exception as e:
            logger.error(f"发送测试消息失败: {e}", exc_info=True)
            await query.answer(f"❌ 发送失败: {str(e)[:50]}", show_alert=True)
