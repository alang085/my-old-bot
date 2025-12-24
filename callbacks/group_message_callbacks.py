"""群组消息回调处理器"""

# 标准库
import logging

# 第三方库
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

# 本地模块
import db_operations
from config import ADMIN_IDS
from utils.callback_helpers import safe_query_reply_text

logger = logging.getLogger(__name__)


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
                    msg += f"   开工信息: {'已设置' if config.get('start_work_message') else '未设置'}\n"
                    msg += (
                        f"   收工信息: {'已设置' if config.get('end_work_message') else '未设置'}\n"
                    )
                    msg += f"   欢迎信息: {'已设置' if config.get('welcome_message') else '未设置'}\n\n"

            keyboard = [
                [InlineKeyboardButton("➕ 添加总群", callback_data="groupmsg_add")],
                [InlineKeyboardButton("📝 设置消息", callback_data="groupmsg_set_message")],
                [InlineKeyboardButton("🔄 刷新", callback_data="groupmsg_refresh")],
            ]

            try:
                await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            except Exception as e:
                logger.error(f"编辑消息失败: {e}", exc_info=True)
                # 如果编辑失败，尝试发送新消息
                try:
                    await safe_query_reply_text(
                        query, msg, reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                except Exception as e2:
                    logger.error(f"发送消息失败: {e2}", exc_info=True)
                    await query.answer("❌ 操作失败", show_alert=True)
        except Exception as e:
            logger.error(f"处理刷新回调失败: {e}", exc_info=True)
            await query.answer("❌ 操作失败", show_alert=True)
        return

    elif data == "groupmsg_add":
        # 先 answer，防止客户端转圈
        try:
            await query.answer()
        except Exception:
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

    elif data == "groupmsg_set_message":
        # 显示选择总群的界面
        logger.info("处理设置消息回调")
        # 先 answer，防止客户端转圈
        try:
            await query.answer()
        except Exception:
            pass

        try:
            configs = await db_operations.get_group_message_configs()
            logger.info(f"获取到 {len(configs)} 个群组配置")

            if not configs:
                await query.answer("❌ 没有配置的总群，请先添加", show_alert=True)
                return

            keyboard = []
            for config in configs:
                chat_id = config.get("chat_id")
                chat_title = config.get("chat_title", f"ID: {chat_id}")
                logger.info(f"添加群组按钮: {chat_title} (ID: {chat_id})")
                keyboard.append(
                    [InlineKeyboardButton(chat_title, callback_data=f"groupmsg_select_{chat_id}")]
                )

            keyboard.append([InlineKeyboardButton("🔙 返回", callback_data="groupmsg_refresh")])

            try:
                logger.info("尝试编辑消息显示群组列表")
                await query.edit_message_text(
                    "📝 选择要设置消息的总群：", reply_markup=InlineKeyboardMarkup(keyboard)
                )
                logger.info("成功编辑消息显示群组列表")
            except Exception as e:
                logger.error(f"编辑消息失败: {e}", exc_info=True)
                # 如果编辑失败，尝试发送新消息
                try:
                    logger.info("尝试发送新消息显示群组列表")
                    await safe_query_reply_text(
                        query,
                        "📝 选择要设置消息的总群：",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                    )
                    logger.info("成功发送新消息显示群组列表")
                except Exception as e2:
                    logger.error(f"发送消息失败: {e2}", exc_info=True)
                    await query.answer("❌ 操作失败，请重试", show_alert=True)
        except Exception as e:
            logger.error(f"处理设置消息失败: {e}", exc_info=True)
            try:
                await query.answer(f"❌ 操作失败: {str(e)[:50]}", show_alert=True)
            except Exception:
                pass

    elif data.startswith("groupmsg_select_"):
        logger.info(f"处理群组选择: {data}")
        # 先给用户一个反馈，表示正在处理
        # 注意：如果之前已经 answer 过，这里可能会失败，但不影响后续处理
        try:
            await query.answer("正在加载...", show_alert=False)
        except Exception as e:
            logger.debug(f"answer 失败（可能已 answer 过）: {e}")

        try:
            # 解析群组ID
            chat_id_str = data.split("_")[-1]
            logger.info(f"解析群组ID: {chat_id_str}")
            chat_id = int(chat_id_str)

            # 获取配置
            logger.info(f"查询群组配置: {chat_id}")
            config = await db_operations.get_group_message_config_by_chat_id(chat_id)

            if not config:
                logger.warning(f"群组配置不存在: {chat_id}")
                await query.answer("❌ 配置不存在", show_alert=True)
                return

            chat_title = config.get("chat_title", f"ID: {chat_id}")
            logger.info(f"找到群组配置: {chat_title} (ID: {chat_id})")

            # 检查各消息类型是否已设置
            has_start_work = bool(config.get("start_work_message"))
            has_end_work = bool(config.get("end_work_message"))
            has_welcome = bool(config.get("welcome_message"))

            # 构建按钮文本，显示是否已设置
            start_text = "🌅 设置开工信息"
            if has_start_work:
                start_text += " ✅"

            end_text = "🌙 设置收工信息"
            if has_end_work:
                end_text += " ✅"

            welcome_text = "👋 设置欢迎信息"
            if has_welcome:
                welcome_text += " ✅"

            keyboard = [
                [InlineKeyboardButton(start_text, callback_data=f"groupmsg_set_start_{chat_id}")],
                [InlineKeyboardButton(end_text, callback_data=f"groupmsg_set_end_{chat_id}")],
                [
                    InlineKeyboardButton(
                        welcome_text, callback_data=f"groupmsg_set_welcome_{chat_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "👁️ 查看所有消息内容", callback_data=f"groupmsg_view_all_{chat_id}"
                    )
                ],
                [InlineKeyboardButton("🔙 返回", callback_data="groupmsg_set_message")],
            ]

            # 构建消息文本，显示设置状态和内容预览
            message_text = (
                f"📝 设置消息内容\n\n"
                f"总群: {chat_title}\n"
                f"群组ID: {chat_id}\n\n"
                f"消息设置状态：\n"
                f"  🌅 开工信息: {'✅ 已设置' if has_start_work else '❌ 未设置'}\n"
                f"  🌙 收工信息: {'✅ 已设置' if has_end_work else '❌ 未设置'}\n"
                f"  👋 欢迎信息: {'✅ 已设置' if has_welcome else '❌ 未设置'}\n\n"
            )

            # 添加内容预览（如果有）
            if has_start_work or has_end_work or has_welcome:
                message_text += "📋 内容预览：\n"
                if has_start_work:
                    start_preview = config.get("start_work_message", "")[:100]
                    if len(config.get("start_work_message", "")) > 100:
                        start_preview += "..."
                    message_text += f"  🌅 开工: {start_preview}\n"
                if has_end_work:
                    end_preview = config.get("end_work_message", "")[:100]
                    if len(config.get("end_work_message", "")) > 100:
                        end_preview += "..."
                    message_text += f"  🌙 收工: {end_preview}\n"
                if has_welcome:
                    welcome_preview = config.get("welcome_message", "")[:100]
                    if len(config.get("welcome_message", "")) > 100:
                        welcome_preview += "..."
                    message_text += f"  👋 欢迎: {welcome_preview}\n"
                message_text += "\n"

            message_text += "请选择要设置的消息类型："

            # 尝试编辑消息
            edit_success = False
            try:
                logger.info(f"尝试编辑消息: {chat_id}")
                await query.edit_message_text(
                    message_text, reply_markup=InlineKeyboardMarkup(keyboard)
                )
                logger.info(f"成功编辑消息: {chat_id}")
                edit_success = True
            except Exception as e:
                logger.error(f"编辑消息失败: {e}", exc_info=True)
                edit_success = False

            # 如果编辑失败，尝试发送新消息
            if not edit_success:
                try:
                    logger.info(f"尝试发送新消息: {chat_id}")
                    await safe_query_reply_text(
                        query, message_text, reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    logger.info(f"成功发送新消息: {chat_id}")
                except Exception as e2:
                    logger.error(f"发送消息失败: {e2}", exc_info=True)
                    # 确保用户看到错误提示
                    try:
                        error_msg = f"❌ 操作失败: {str(e2)[:30]}"
                        await query.answer(error_msg, show_alert=True)
                    except Exception:
                        pass
        except (ValueError, IndexError) as e:
            logger.error(f"解析群组ID失败: {data}, 错误: {e}", exc_info=True)
            try:
                await query.answer("❌ 无效的群组ID", show_alert=True)
            except Exception:
                pass
        except Exception as e:
            logger.error(f"处理群组选择失败: {data}, 错误: {e}", exc_info=True)
            try:
                error_msg = f"❌ 操作失败: {str(e)[:50]}"
                await query.answer(error_msg, show_alert=True)
            except Exception:
                pass

    elif data.startswith("groupmsg_view_all_"):
        # 查看所有消息内容
        logger.info(f"查看所有消息内容: {data}")
        try:
            await query.answer()
        except Exception:
            pass

        try:
            chat_id = int(data.split("_")[-1])
            config = await db_operations.get_group_message_config_by_chat_id(chat_id)

            if not config:
                await query.answer("❌ 配置不存在", show_alert=True)
                return

            chat_title = config.get("chat_title", f"ID: {chat_id}")

            # 构建完整消息内容
            msg = f"📋 所有消息内容\n\n"
            msg += f"总群: {chat_title}\n"
            msg += f"群组ID: {chat_id}\n\n"
            msg += "=" * 40 + "\n\n"

            # 开工信息
            start_message = config.get("start_work_message")
            if start_message:
                msg += "🌅 开工信息：\n"
                msg += f"{start_message}\n\n"
                # 检查是否有多个版本
                if "⸻" in start_message:
                    versions = [v.strip() for v in start_message.split("⸻") if v.strip()]
                    msg += f"💡 检测到 {len(versions)} 个版本，将自动轮播\n\n"
            else:
                msg += "🌅 开工信息：❌ 未设置\n\n"

            msg += "=" * 40 + "\n\n"

            # 收工信息
            end_message = config.get("end_work_message")
            if end_message:
                msg += "🌙 收工信息：\n"
                msg += f"{end_message}\n\n"
                # 检查是否有多个版本
                if "⸻" in end_message:
                    versions = [v.strip() for v in end_message.split("⸻") if v.strip()]
                    msg += f"💡 检测到 {len(versions)} 个版本，将自动轮播\n\n"
            else:
                msg += "🌙 收工信息：❌ 未设置\n\n"

            msg += "=" * 40 + "\n\n"

            # 欢迎信息
            welcome_message = config.get("welcome_message")
            if welcome_message:
                msg += "👋 欢迎信息：\n"
                msg += f"{welcome_message}\n\n"
                # 检查是否有多个版本
                if "⸻" in welcome_message:
                    versions = [v.strip() for v in welcome_message.split("⸻") if v.strip()]
                    msg += f"💡 检测到 {len(versions)} 个版本，将自动轮播\n\n"
            else:
                msg += "👋 欢迎信息：❌ 未设置\n\n"

            keyboard = [
                [InlineKeyboardButton("🔙 返回", callback_data=f"groupmsg_select_{chat_id}")]
            ]

            try:
                await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            except Exception as e:
                logger.error(f"编辑消息失败: {e}", exc_info=True)
                await safe_query_reply_text(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"查看消息内容失败: {e}", exc_info=True)
            try:
                await query.answer("❌ 查看失败", show_alert=True)
            except Exception:
                pass

    elif data.startswith("batch_set_select_"):
        # 批量设置：选择群组
        try:
            await query.answer()
        except Exception:
            pass

        try:
            chat_id = int(data.split("_")[-1])
            config = await db_operations.get_group_message_config_by_chat_id(chat_id)
            chat_title = config.get("chat_title", f"ID: {chat_id}") if config else f"ID: {chat_id}"

            context.user_data["batch_setting_chat_id"] = chat_id
            context.user_data["batch_setting_step"] = "start_work"

            message_text = (
                f"📝 批量设置消息\n\n"
                f"群组: {chat_title}\n"
                f"群组ID: {chat_id}\n\n"
                f"步骤 1/3: 设置开工信息\n\n"
                f"请输入开工信息（支持多版本，用 ⸻ 分隔）：\n\n"
                f"💡 示例：\n"
                f"Good morning po! 😊 Our team is now online...\n"
                f"⸻\n"
                f"版本二内容\n"
                f"⸻\n"
                f"版本三内容\n\n"
                f"输入 'skip' 跳过此步骤\n"
                f"输入 'cancel' 取消"
            )

            try:
                await query.edit_message_text(message_text)
            except Exception:
                await safe_query_reply_text(query, message_text)

            context.user_data["state"] = "BATCH_SETTING_MESSAGES"
        except Exception as e:
            logger.error(f"批量设置选择群组失败: {e}", exc_info=True)
            try:
                await query.answer("❌ 选择失败", show_alert=True)
            except Exception:
                pass

    elif data == "batch_set_cancel":
        # 取消批量设置
        try:
            await query.answer("已取消")
            await query.edit_message_text("❌ 已取消批量设置")
        except Exception:
            pass
        context.user_data.pop("batch_setting_chat_id", None)
        context.user_data.pop("batch_setting_step", None)
        context.user_data.pop("state", None)

    elif data.startswith("groupmsg_set_start_"):
        try:
            chat_id = int(data.split("_")[-1])
            context.user_data["setting_message_chat_id"] = chat_id
            context.user_data["setting_message_type"] = "start_work"

            # 获取当前已设置的消息（如果有）
            config = await db_operations.get_group_message_config_by_chat_id(chat_id)
            current_message = config.get("start_work_message") if config else None

            try:
                if current_message:
                    # 如果已有消息，显示当前内容并提供编辑选项
                    preview = (
                        current_message[:200] + "..."
                        if len(current_message) > 200
                        else current_message
                    )
                    await safe_query_reply_text(
                        query,
                        f"📝 设置开工信息\n\n"
                        f"当前内容：\n{preview}\n\n"
                        f"💡 提示：\n"
                        f"- 输入新内容将替换当前内容\n"
                        f"- 使用 ⸻ 分隔符可以设置多个版本（自动轮播）\n"
                        f"- 输入 'cancel' 取消\n"
                        f"- 输入 'keep' 保持当前内容不变",
                    )
                else:
                    # 如果没有消息，提示输入
                    await safe_query_reply_text(
                        query,
                        "📝 设置开工信息\n\n"
                        "💡 提示：\n"
                        "- 使用 ⸻ 分隔符可以设置多个版本（自动轮播）\n"
                        "例如：\n"
                        "版本1内容\n"
                        "⸻\n"
                        "版本2内容\n"
                        "⸻\n"
                        "版本3内容\n\n"
                        "输入 'cancel' 取消",
                    )
            except Exception as e:
                logger.error(f"发送开工信息提示失败: {e}", exc_info=True)
                await query.answer("请输入开工信息", show_alert=True)
            context.user_data["state"] = "SETTING_GROUP_MESSAGE"
            await query.answer()
        except (ValueError, IndexError):
            await query.answer("❌ 无效的群组ID", show_alert=True)

    elif data.startswith("groupmsg_set_end_"):
        try:
            chat_id = int(data.split("_")[-1])
            context.user_data["setting_message_chat_id"] = chat_id
            context.user_data["setting_message_type"] = "end_work"

            # 获取当前已设置的消息（如果有）
            config = await db_operations.get_group_message_config_by_chat_id(chat_id)
            current_message = config.get("end_work_message") if config else None

            try:
                if current_message:
                    # 如果已有消息，显示当前内容并提供编辑选项
                    preview = (
                        current_message[:200] + "..."
                        if len(current_message) > 200
                        else current_message
                    )
                    await safe_query_reply_text(
                        query,
                        f"📝 设置收工信息\n\n"
                        f"当前内容：\n{preview}\n\n"
                        f"💡 提示：\n"
                        f"- 输入新内容将替换当前内容\n"
                        f"- 使用 ⸻ 分隔符可以设置多个版本（自动轮播）\n"
                        f"- 输入 'cancel' 取消\n"
                        f"- 输入 'keep' 保持当前内容不变",
                    )
                else:
                    # 如果没有消息，提示输入
                    await safe_query_reply_text(
                        query,
                        "📝 设置收工信息\n\n"
                        "💡 提示：\n"
                        "- 使用 ⸻ 分隔符可以设置多个版本（自动轮播）\n"
                        "例如：\n"
                        "版本1内容\n"
                        "⸻\n"
                        "版本2内容\n"
                        "⸻\n"
                        "版本3内容\n\n"
                        "输入 'cancel' 取消",
                    )
            except Exception as e:
                logger.error(f"发送收工信息提示失败: {e}", exc_info=True)
                await query.answer("请输入收工信息", show_alert=True)
            context.user_data["state"] = "SETTING_GROUP_MESSAGE"
            await query.answer()
        except (ValueError, IndexError):
            await query.answer("❌ 无效的群组ID", show_alert=True)

    elif data.startswith("groupmsg_set_welcome_"):
        try:
            chat_id = int(data.split("_")[-1])
            context.user_data["setting_message_chat_id"] = chat_id
            context.user_data["setting_message_type"] = "welcome"

            # 获取当前已设置的消息（如果有）
            config = await db_operations.get_group_message_config_by_chat_id(chat_id)
            current_message = config.get("welcome_message") if config else None

            try:
                if current_message:
                    # 如果已有消息，显示当前内容并提供编辑选项
                    preview = (
                        current_message[:200] + "..."
                        if len(current_message) > 200
                        else current_message
                    )
                    await safe_query_reply_text(
                        query,
                        f"📝 设置欢迎信息\n\n"
                        f"当前内容：\n{preview}\n\n"
                        f"💡 提示：\n"
                        f"- 输入新内容将替换当前内容\n"
                        f"- 使用 ⸻ 分隔符可以设置多个版本（自动轮播）\n"
                        f"- 支持变量：{{username}} 和 {{chat_title}}\n"
                        f"- 输入 'cancel' 取消\n"
                        f"- 输入 'keep' 保持当前内容不变",
                    )
                else:
                    # 如果没有消息，提示输入
                    await safe_query_reply_text(
                        query,
                        "📝 设置欢迎信息\n\n"
                        "💡 提示：\n"
                        "- 使用 ⸻ 分隔符可以设置多个版本（自动轮播）\n"
                        "- 支持变量：{username} 和 {chat_title}\n"
                        "例如：\n"
                        "版本1内容 {username}\n"
                        "⸻\n"
                        "版本2内容 {username}\n"
                        "⸻\n"
                        "版本3内容 {username}\n\n"
                        "输入 'cancel' 取消",
                    )
            except Exception as e:
                logger.error(f"发送欢迎信息提示失败: {e}", exc_info=True)
                await query.answer("请输入欢迎信息", show_alert=True)
            context.user_data["state"] = "SETTING_GROUP_MESSAGE"
            await query.answer()
        except (ValueError, IndexError):
            await query.answer("❌ 无效的群组ID", show_alert=True)

    elif data == "announcement_refresh":
        from handlers.group_message_handlers import manage_announcements

        await manage_announcements(update, context)

    elif data == "announcement_add":
        try:
            await safe_query_reply_text(query, "请输入公告内容：\n" "输入 'cancel' 取消")
        except Exception as e:
            logger.error(f"发送公告提示失败: {e}", exc_info=True)
            await query.answer("请输入公告内容", show_alert=True)
        context.user_data["state"] = "ADDING_ANNOUNCEMENT"
        await query.answer()

    elif data == "announcement_list":
        announcements = await db_operations.get_all_company_announcements()

        if not announcements:
            await query.answer("❌ 没有公告", show_alert=True)
            return

        msg = "📋 所有公告列表\n\n"
        for ann in announcements:
            ann_id = ann.get("id")
            message = ann.get("message", "")
            is_active = ann.get("is_active", 0)
            status = "✅" if is_active else "❌"

            msg += f"{status} [{ann_id}] {message}\n\n"

        keyboard = []
        for ann in announcements:
            ann_id = ann.get("id")
            is_active = ann.get("is_active", 0)
            action = "禁用" if is_active else "启用"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{'✅' if is_active else '❌'} [{ann_id}] {action}",
                        callback_data=f"announcement_toggle_{ann_id}",
                    ),
                    InlineKeyboardButton("🗑️ 删除", callback_data=f"announcement_delete_{ann_id}"),
                ]
            )

        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data="announcement_refresh")])

        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("announcement_toggle_"):
        try:
            ann_id = int(data.split("_")[-1])
            ann = await db_operations.get_all_company_announcements()
            current = next((a for a in ann if a.get("id") == ann_id), None)

            if not current:
                await query.answer("❌ 公告不存在", show_alert=True)
                return

            new_status = 0 if current.get("is_active") else 1
            success = await db_operations.toggle_company_announcement(ann_id, new_status)

            if success:
                await query.answer("✅ 状态已更新")
                # 刷新列表
                await handle_group_message_callback(update, context)
            else:
                await query.answer("❌ 更新失败", show_alert=True)
        except (ValueError, IndexError):
            await query.answer("❌ 无效的公告ID", show_alert=True)

    elif data.startswith("announcement_delete_"):
        try:
            ann_id = int(data.split("_")[-1])
            success = await db_operations.delete_company_announcement(ann_id)

            if success:
                await query.answer("✅ 公告已删除")
                # 刷新列表
                await handle_group_message_callback(update, context)
            else:
                await query.answer("❌ 删除失败", show_alert=True)
        except (ValueError, IndexError):
            await query.answer("❌ 无效的公告ID", show_alert=True)

    elif data == "announcement_set_interval":
        try:
            await safe_query_reply_text(
                query,
                "请输入发送间隔（小时）：\n"
                "格式: 数字（如：3 表示每3小时发送一次）\n"
                "输入 'cancel' 取消",
            )
        except Exception as e:
            logger.error(f"发送间隔提示失败: {e}", exc_info=True)
            await query.answer("请输入发送间隔", show_alert=True)
        context.user_data["state"] = "SETTING_ANNOUNCEMENT_INTERVAL"
        await query.answer()

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
        messages = await db_operations.get_all_anti_fraud_messages()

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
            messages = await db_operations.get_all_anti_fraud_messages()
            current = next((m for m in messages if m.get("id") == msg_id), None)

            if not current:
                await query.answer("❌ 语录不存在", show_alert=True)
                return

            success = await db_operations.toggle_anti_fraud_message(msg_id)

            if success:
                await query.answer("✅ 状态已更新")
                # 刷新列表
                await handle_group_message_callback(update, context)
            else:
                await query.answer("❌ 更新失败", show_alert=True)
        except (ValueError, IndexError):
            await query.answer("❌ 无效的语录ID", show_alert=True)

    elif data.startswith("antifraud_delete_"):
        try:
            msg_id = int(data.split("_")[-1])
            success = await db_operations.delete_anti_fraud_message(msg_id)

            if success:
                await query.answer("✅ 语录已删除")
                # 刷新列表
                await handle_group_message_callback(update, context)
            else:
                await query.answer("❌ 删除失败", show_alert=True)
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
        messages = await db_operations.get_all_promotion_messages()

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
            messages = await db_operations.get_all_promotion_messages()
            current = next((m for m in messages if m.get("id") == msg_id), None)

            if not current:
                await query.answer("❌ Message not found", show_alert=True)
                return

            success = await db_operations.toggle_promotion_message(msg_id)

            if success:
                await query.answer("✅ Status updated")
                # 刷新列表
                await handle_group_message_callback(update, context)
            else:
                await query.answer("❌ Update failed", show_alert=True)
        except (ValueError, IndexError):
            await query.answer("❌ Invalid message ID", show_alert=True)

    elif data.startswith("promotion_delete_"):
        try:
            msg_id = int(data.split("_")[-1])
            success = await db_operations.delete_promotion_message(msg_id)

            if success:
                await query.answer("✅ Message deleted")
                # 刷新列表
                await handle_group_message_callback(update, context)
            else:
                await query.answer("❌ Delete failed", show_alert=True)
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

    elif data == "test_announcement":
        try:
            await query.answer("🔄 正在发送公司公告...")
            import random

            from utils.schedule_executor import (
                format_admin_mentions_from_group,
                select_rotated_message,
            )

            bot = context.bot

            # 获取激活的公告列表
            announcements = await db_operations.get_company_announcements()

            if not announcements:
                await query.edit_message_text("❌ 没有激活的公告")
                return

            # 随机选择一条公告
            selected_announcement = random.choice(announcements)
            message = selected_announcement.get("message")

            if not message:
                await query.edit_message_text("❌ 选中的公告消息为空")
                return

            # 处理多版本消息轮播
            rotated_message = select_rotated_message(message)

            # 获取所有配置的总群
            configs = await db_operations.get_group_message_configs()

            if not configs:
                await query.edit_message_text("❌ 没有配置的总群")
                return

            # 获取管理员@用户名（从指定群组获取）
            admin_mentions = await format_admin_mentions_from_group(bot)

            # 组合消息
            final_message = rotated_message
            if admin_mentions:
                final_message = f"{rotated_message}\n\n{admin_mentions}"

            success_count = 0
            fail_count = 0

            for config in configs:
                chat_id = config.get("chat_id")
                if not chat_id:
                    continue
                try:
                    await bot.send_message(chat_id=chat_id, text=final_message, parse_mode="HTML")
                    success_count += 1
                except Exception as e:
                    fail_count += 1
                    logger.error(f"发送公司公告到群组 {chat_id} 失败: {e}", exc_info=True)

            await query.edit_message_text(
                f"✅ 公司公告已发送\n成功: {success_count}, 失败: {fail_count}"
            )
        except Exception as e:
            logger.error(f"测试发送公司公告失败: {e}", exc_info=True)
            await query.answer(f"❌ 发送失败: {str(e)[:50]}", show_alert=True)

    elif data == "test_all":
        try:
            await query.answer("🔄 Sending all types of messages...")
            from utils.schedule_executor import (
                send_company_promotion_messages,
                send_random_announcements,
            )

            bot = context.bot

            # Send promotion messages
            try:
                await send_company_promotion_messages(bot)
            except Exception as e:
                logger.error(f"Failed to send promotion messages: {e}", exc_info=True)

            # 等待1秒
            import asyncio

            await asyncio.sleep(1)

            # 发送公司公告
            try:
                await send_random_announcements(bot)
            except Exception as e:
                logger.error(f"发送公司公告失败: {e}", exc_info=True)

            await query.edit_message_text("✅ All types of messages sent to all groups")
        except Exception as e:
            logger.error(f"Failed to send all test messages: {e}", exc_info=True)
            await query.answer(f"❌ 发送失败: {str(e)[:50]}", show_alert=True)

    elif data == "test_cancel":
        await query.edit_message_text("❌ 已取消测试")
