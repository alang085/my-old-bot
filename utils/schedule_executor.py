"""定时播报执行器"""

# 标准库
import logging
import random
from datetime import datetime
from typing import Optional

# 第三方库
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# 本地模块
import db_operations

# 北京时区
BEIJING_TZ = pytz.timezone("Asia/Shanghai")

logger = logging.getLogger(__name__)

# 全局调度器
scheduler = None

# 缓存管理员用户名（只提取一次）
_cached_admin_mentions = None
_cached_group_chat_id = None

# 群组消息发送功能已优化，不再需要记录上次发送类型


def select_rotated_message(message: str) -> str:
    """简化版：直接返回消息（已移除基于日期的复杂轮换逻辑）"""
    if not message:
        return ""
    return message.strip()


def create_message_keyboard(
    bot_links: str = None, worker_links: str = None
) -> Optional[InlineKeyboardMarkup]:
    """创建消息内联键盘（自动和人工按钮）

    Args:
        bot_links: 机器人链接（多个链接用换行符分隔）
        worker_links: 人工链接（多个链接用换行符分隔）

    Returns:
        InlineKeyboardMarkup 或 None（如果没有链接）
    """
    keyboard = []

    # 解析链接（支持换行符分隔的多个链接）
    bot_link_list = []
    if bot_links:
        bot_link_list = [
            link.strip()
            for link in bot_links.split("\n")
            if link.strip()
            and (link.strip().startswith("http://") or link.strip().startswith("https://"))
        ]

    worker_link_list = []
    if worker_links:
        worker_link_list = [
            link.strip()
            for link in worker_links.split("\n")
            if link.strip()
            and (link.strip().startswith("http://") or link.strip().startswith("https://"))
        ]

    # 添加"Auto"按钮（机器人链接）
    if bot_link_list:
        # 如果只有一个链接，直接使用URL按钮
        if len(bot_link_list) == 1:
            keyboard.append([InlineKeyboardButton("🤖 Auto", url=bot_link_list[0])])
        else:
            # 多个链接：第一个链接作为主按钮
            keyboard.append([InlineKeyboardButton("🤖 Auto", url=bot_link_list[0])])
            # 可以添加更多按钮显示其他链接（如果需要）

    # 添加"Manual"按钮（个人链接）
    if worker_link_list:
        if len(worker_link_list) == 1:
            keyboard.append([InlineKeyboardButton("👤 Manual", url=worker_link_list[0])])
        else:
            # 多个链接：第一个链接作为主按钮
            keyboard.append([InlineKeyboardButton("👤 Manual", url=worker_link_list[0])])
            # 可以添加更多按钮显示其他链接（如果需要）

    if not keyboard:
        return None

    return InlineKeyboardMarkup(keyboard)


def select_random_anti_fraud_message(messages: list) -> str:
    """随机选择一个防诈骗语录"""
    if not messages:
        return ""
    return random.choice(messages)


def format_red_message(message: str) -> str:
    """将消息格式化为强调显示（HTML格式）
    注意：Telegram Bot API不支持CSS样式，使用加粗和emoji来强调
    """
    if not message:
        return ""
    # 转义 HTML 特殊字符，避免解析错误
    import html

    escaped_message = html.escape(message)
    # 使用加粗和警告emoji来强调（Telegram不支持CSS样式）
    return f"⚠️ <b>{escaped_message}</b>"


async def _send_group_message(bot, chat_id: int, message: str) -> bool:
    """统一的群组消息发送辅助函数
    机器人直接在群组中发送消息（不添加任何按钮）

    Args:
        bot: Telegram Bot 实例
        chat_id: 群组ID
        message: 消息内容

    Returns:
        bool: 发送是否成功
    """
    try:
        # 机器人直接在群组中发送消息，不添加任何按钮
        logger.info(f"机器人正在向群组 {chat_id} 发送消息（直接发送，无按钮）")
        await bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode="HTML",
        )
        logger.info(f"✅ 消息已成功发送到群组 {chat_id}")
        return True
    except Exception as e:
        logger.error(f"❌ 发送消息到群组 {chat_id} 失败: {e}", exc_info=True)
        return False


def _combine_message_with_anti_fraud(main_message: str, anti_fraud_messages: list) -> str:
    """组合主消息和防诈骗语录

    Args:
        main_message: 主消息内容
        anti_fraud_messages: 防诈骗语录列表

    Returns:
        str: 组合后的消息
    """
    final_message = main_message

    # 添加防诈骗语录（如果存在）
    if anti_fraud_messages:
        random_anti_fraud = select_random_anti_fraud_message(anti_fraud_messages)
        if random_anti_fraud:
            # 处理多版本（如果语录包含 ⸻ 分隔符）
            rotated_anti_fraud = select_rotated_message(random_anti_fraud)
            if rotated_anti_fraud:
                red_anti_fraud = format_red_message(rotated_anti_fraud)
                final_message = f"{main_message}\n\n{red_anti_fraud}"

    return final_message


async def get_group_admins_from_chat(bot, chat_id: int) -> list:
    """
    从指定群组获取所有管理员用户名
    返回用户名列表（不包含@符号）
    """
    try:
        # 获取群组管理员列表
        administrators = await bot.get_chat_administrators(chat_id)

        usernames = []
        for admin in administrators:
            user = admin.user
            # 只获取有用户名的管理员
            if user.username:
                usernames.append(user.username)

        return usernames
    except Exception as e:
        logger.error(f"获取群组 {chat_id} 管理员失败: {e}", exc_info=True)
        return []


async def format_admin_mentions_from_group(bot, group_chat_id: int = None) -> str:
    """
    从指定群组获取管理员用户名并格式化（使用缓存，只提取一次）
    如果未指定群组ID，则查找名为 "📱iPhone loan Chat(2)" 的群组
    """
    global _cached_admin_mentions, _cached_group_chat_id

    try:
        # 如果缓存存在且群组ID匹配，直接返回缓存
        if _cached_admin_mentions is not None and _cached_group_chat_id is not None:
            if group_chat_id is None or group_chat_id == _cached_group_chat_id:
                logger.debug(f"使用缓存的管理员用户名（群组ID: {_cached_group_chat_id}）")
                return _cached_admin_mentions

        # 如果没有指定群组ID，尝试查找指定名称的群组
        if group_chat_id is None:
            configs = await db_operations.get_group_message_configs()
            target_group_name = "📱iPhone loan Chat(2)"

            for config in configs:
                chat_title = config.get("chat_title", "")
                if target_group_name in chat_title or chat_title == target_group_name:
                    group_chat_id = config.get("chat_id")
                    logger.info(f"找到目标群组: {chat_title} (ID: {group_chat_id})")
                    break

            # 如果还是没找到，尝试通过群组名称查找
            if group_chat_id is None:
                try:
                    # 尝试在所有配置的群组中查找
                    for config in configs:
                        chat_id = config.get("chat_id")
                        try:
                            chat = await bot.get_chat(chat_id)
                            if chat.title == target_group_name or target_group_name in chat.title:
                                group_chat_id = chat_id
                                logger.info(
                                    f"通过API找到目标群组: {chat.title} (ID: {group_chat_id})"
                                )
                                break
                        except Exception as e:
                            logger.debug(f"检查群组 {chat_id} 失败: {e}")
                            continue
                except Exception as e:
                    logger.debug(f"查找群组失败: {e}")

        if group_chat_id is None:
            logger.warning("未找到目标群组，使用默认管理员列表")
            from config import ADMIN_IDS

            return await format_admin_mentions(bot, ADMIN_IDS)

        # 获取群组管理员用户名（只提取一次）
        admin_usernames = await get_group_admins_from_chat(bot, group_chat_id)

        if not admin_usernames:
            logger.warning(f"群组 {group_chat_id} 没有找到管理员用户名，使用默认")
            from config import ADMIN_IDS

            return await format_admin_mentions(bot, ADMIN_IDS)

        # 格式化用户名（添加@符号）
        mentions = [f"@{username}" for username in admin_usernames]
        formatted_mentions = " ".join(mentions) if mentions else ""

        # 缓存结果
        _cached_admin_mentions = formatted_mentions
        _cached_group_chat_id = group_chat_id
        logger.info(
            f"已缓存管理员用户名（群组ID: {group_chat_id}，共 {len(admin_usernames)} 个管理员）"
        )

        return formatted_mentions
    except Exception as e:
        logger.error(f"从群组获取管理员用户名失败: {e}", exc_info=True)
        # 失败时回退到默认方式
        from config import ADMIN_IDS

        return await format_admin_mentions(bot, ADMIN_IDS)


async def format_admin_mentions(bot, admin_ids: list) -> str:
    """
    格式化管理员@用户名
    固定包含 @luckyno44，然后随机选择4名其他管理员
    如果某些管理员没有用户名或获取失败，继续尝试其他管理员
    """
    if not admin_ids:
        return ""

    try:
        import random

        # 固定包含 @luckyno44
        fixed_username = "@luckyno44"
        mentions = [fixed_username]

        # 尝试获取 luckyno44 的用户ID（如果存在）
        luckyno44_id = None
        try:
            # 尝试通过用户名获取用户（需要用户已经与bot交互过）
            # 注意：这个方法可能失败，如果用户从未与bot交互
            user = await bot.get_chat("@luckyno44")
            if hasattr(user, "id"):
                luckyno44_id = user.id
        except Exception as e:
            logger.debug(f"无法获取 @luckyno44 的用户ID: {e}")

        # 从管理员列表中排除 luckyno44（如果存在）
        other_admins = [aid for aid in admin_ids if aid != luckyno44_id]

        if not other_admins:
            return fixed_username

        # 随机打乱管理员列表，然后尝试获取用户名
        # 这样可以确保即使某些管理员获取失败，也能尝试其他管理员
        shuffled_admins = other_admins.copy()
        random.shuffle(shuffled_admins)

        # 尝试获取最多4个有效的管理员用户名
        target_count = 4
        collected_count = 0

        for admin_id in shuffled_admins:
            if collected_count >= target_count:
                break

            try:
                user = await bot.get_chat(admin_id)
                username = user.username
                if username:
                    mentions.append(f"@{username}")
                    collected_count += 1
            except Exception as e:
                logger.debug(f"获取管理员 {admin_id} 用户名失败: {e}")
                # 继续尝试下一个管理员

        return " ".join(mentions) if mentions else fixed_username
    except Exception as e:
        logger.error(f"格式化管理员@用户名失败: {e}", exc_info=True)
        return "@luckyno44"  # 至少返回固定的用户名


async def send_scheduled_broadcast(bot, broadcast):
    """发送定时播报"""
    try:
        chat_id = broadcast["chat_id"]
        message = broadcast["message"]

        if not chat_id:
            logger.warning(f"播报 {broadcast['slot']} 没有设置chat_id，跳过发送")
            return

        await bot.send_message(chat_id=chat_id, text=message)
        logger.info(f"定时播报 {broadcast['slot']} 已发送到群组 {chat_id}")
    except Exception as e:
        logger.error(f"发送定时播报 {broadcast['slot']} 失败: {e}", exc_info=True)


async def setup_scheduled_broadcasts(bot):
    """设置定时播报任务"""
    global scheduler

    if scheduler is None:
        scheduler = AsyncIOScheduler()
        scheduler.start()

    # 只清除播报任务（不清除日切报表任务）
    for job in scheduler.get_jobs():
        if job.id.startswith("broadcast_"):
            scheduler.remove_job(job.id)

    # 获取所有激活的定时播报
    broadcasts = await db_operations.get_active_scheduled_broadcasts()

    for broadcast in broadcasts:
        try:
            time_str = broadcast["time"]
            # 解析时间 (HH:MM 或 HH)
            time_parts = time_str.split(":")
            hour = int(time_parts[0])
            minute = int(time_parts[1]) if len(time_parts) > 1 else 0

            # 创建定时任务（每天执行）
            job_id = f"broadcast_{broadcast['slot']}"

            scheduler.add_job(
                send_scheduled_broadcast,
                trigger=CronTrigger(hour=hour, minute=minute, timezone=BEIJING_TZ),
                args=[bot, broadcast],
                id=job_id,
                replace_existing=True,
            )

            logger.info(
                f"已设置定时播报 {broadcast['slot']}: 每天 {time_str} 发送到群组 {broadcast['chat_id']}"
            )
        except Exception as e:
            logger.error(f"设置定时播报 {broadcast['slot']} 失败: {e}", exc_info=True)


async def reload_scheduled_broadcasts(bot):
    """重新加载定时播报任务"""
    await setup_scheduled_broadcasts(bot)


async def send_daily_report(bot):
    """发送日切报表Excel文件给所有管理员和授权员工（业务员）（每天生成两个Excel：订单总表和每日变化数据）"""
    logger.info("=" * 60)
    logger.info("开始执行每日报表生成任务")
    logger.info("=" * 60)
    try:
        # 获取日切日期（使用get_daily_period_date，因为日切是在23:00后）
        # 如果当前时间在23:00之后，get_daily_period_date会返回明天的日期
        # 但我们需要统计的是今天的数据，所以需要减一天
        from datetime import datetime, timedelta

        import pytz

        import db_operations
        from config import ADMIN_IDS

        tz = pytz.timezone("Asia/Shanghai")
        now = datetime.now(tz)
        # 如果当前时间在23:00之后，统计今天的数据；否则统计昨天的数据
        if now.hour >= 23:
            # 23:00之后，统计今天的数据
            report_date = now.strftime("%Y-%m-%d")
        else:
            # 23:00之前，统计昨天的数据
            yesterday = now - timedelta(days=1)
            report_date = yesterday.strftime("%Y-%m-%d")

        logger.info(f"开始生成每日Excel报表 ({report_date})")

        # 1. 生成订单总表Excel
        try:
            from utils.excel_export import export_orders_to_excel

            # 获取所有有效订单
            valid_orders = await db_operations.get_all_valid_orders()

            # 获取当日利息总额
            daily_interest = await db_operations.get_daily_interest_total(report_date)

            # 获取当日完成的订单
            completed_orders = await db_operations.get_completed_orders_by_date(report_date)

            # 获取当日违约的订单（仅当日有变动的）
            breach_orders = await db_operations.get_breach_orders_by_date(report_date)

            # 获取当日违约完成的订单（仅当日有变动的）
            breach_end_orders = await db_operations.get_breach_end_orders_by_date(report_date)

            # 获取日切数据
            daily_summary = await db_operations.get_daily_summary(report_date)

            # 导出订单总表Excel
            orders_excel_path = await export_orders_to_excel(
                valid_orders,
                completed_orders,
                breach_orders,
                breach_end_orders,
                daily_interest,
                daily_summary,
            )
            logger.info(f"订单总表Excel已生成: {orders_excel_path}")
        except Exception as e:
            logger.error(f"生成订单总表Excel失败: {e}", exc_info=True)
            orders_excel_path = None

        # 2. 生成每日变化数据Excel
        try:
            from utils.excel_export import export_daily_changes_to_excel

            # 导出每日变化数据Excel
            changes_excel_path = await export_daily_changes_to_excel(report_date)
            logger.info(f"每日变化数据Excel已生成: {changes_excel_path}")
        except Exception as e:
            logger.error(f"生成每日变化数据Excel失败: {e}", exc_info=True)
            changes_excel_path = None

        # 获取所有授权员工（业务员）
        authorized_users = await db_operations.get_authorized_users()

        # 合并管理员和授权员工列表（去重）
        all_recipients = list(set(ADMIN_IDS + authorized_users))

        logger.info(
            f"报表接收人: {len(ADMIN_IDS)} 个管理员, {len(authorized_users)} 个业务员, 总计 {len(all_recipients)} 人"
        )

        # 发送给所有管理员和授权员工
        success_count = 0
        fail_count = 0
        for user_id in all_recipients:
            try:
                # 发送订单总表Excel
                if orders_excel_path:
                    with open(orders_excel_path, "rb") as f:
                        await bot.send_document(
                            chat_id=user_id,
                            document=f,
                            filename=f"订单总表_{report_date}.xlsx",
                            caption=f"📊 订单总表 ({report_date})\n\n包含所有有效订单及利息记录",
                        )

                # 发送每日变化数据Excel
                if changes_excel_path:
                    with open(changes_excel_path, "rb") as f:
                        await bot.send_document(
                            chat_id=user_id,
                            document=f,
                            filename=f"每日变化数据_{report_date}.xlsx",
                            caption=f"📈 每日变化数据 ({report_date})\n\n包含：\n• 新增订单\n• 完成订单\n• 违约完成订单\n• 收入明细（利息等）\n• 开销明细\n• 数据汇总",
                        )

                success_count += 1
                recipient_type = "管理员" if user_id in ADMIN_IDS else "业务员"
                logger.info(f"每日Excel报表已发送给{recipient_type} {user_id}")
            except Exception as e:
                fail_count += 1
                recipient_type = "管理员" if user_id in ADMIN_IDS else "业务员"
                logger.error(
                    f"发送每日Excel报表给{recipient_type} {user_id} 失败: {e}", exc_info=True
                )

        # 清理临时文件
        import os

        for file_path in [orders_excel_path, changes_excel_path]:
            if file_path:
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.warning(f"删除临时文件失败 {file_path}: {e}")

        logger.info(f"每日Excel报表发送完成: 成功 {success_count}, 失败 {fail_count}")
        logger.info("=" * 60)
        logger.info("每日报表生成任务执行完成")
        logger.info("=" * 60)
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"发送每日Excel报表失败: {e}", exc_info=True)
        logger.error("=" * 60)
        # 发送错误通知给管理员
        try:
            from config import ADMIN_IDS

            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        chat_id=admin_id,
                        text=f"❌ 每日报表生成失败\n\n错误: {str(e)}\n\n请检查日志获取详细信息",
                    )
                except Exception as notify_error:
                    logger.error(
                        f"发送错误通知给管理员 {admin_id} 失败: {notify_error}", exc_info=True
                    )
        except Exception as notify_error:
            logger.error(f"发送错误通知失败: {notify_error}", exc_info=True)


async def setup_daily_report(bot):
    """设置日切报表自动发送任务（每天23:05执行）"""
    global scheduler

    if scheduler is None:
        scheduler = AsyncIOScheduler()
        scheduler.start()

    # 添加日切报表任务
    try:
        scheduler.add_job(
            send_daily_report,
            trigger=CronTrigger(hour=23, minute=5, timezone=BEIJING_TZ),
            args=[bot],
            id="daily_report",
            replace_existing=True,
        )
        logger.info("已设置日切报表任务: 每天 23:05 自动发送")
    except Exception as e:
        logger.error(f"设置日切报表任务失败: {e}", exc_info=True)


async def send_start_work_messages(bot):
    """发送开工信息到所有配置的总群（随机选择）"""
    try:
        configs = await db_operations.get_group_message_configs()

        if not configs:
            logger.info("没有配置的总群，跳过发送开工信息")
            return

        # 获取所有激活的开工消息（随机选择）
        start_work_messages = await db_operations.get_active_start_work_messages()

        if not start_work_messages:
            logger.warning("没有激活的开工消息，跳过发送")
            return

        # 随机选择一条开工消息
        message = random.choice(start_work_messages)

        # 处理多版本消息轮播（如果消息包含 ⸻ 分隔符）
        rotated_message = select_rotated_message(message)

        # 获取激活的防诈骗语录
        anti_fraud_messages = await db_operations.get_active_anti_fraud_messages()

        success_count = 0
        fail_count = 0

        for config in configs:
            chat_id = config.get("chat_id")

            if not chat_id:
                continue

            try:
                # 组合消息：主消息 + 防诈骗语录（防诈骗也是随机选择）
                final_message = _combine_message_with_anti_fraud(
                    rotated_message, anti_fraud_messages
                )

                # 发送消息（直接从数据库读取，不添加按钮）
                if await _send_group_message(bot, chat_id, final_message):
                    success_count += 1
                    logger.info(f"开工信息已发送到群组 {chat_id} (随机选择)")
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(f"发送开工信息到群组 {chat_id} 失败: {e}", exc_info=True)

        logger.info(f"开工信息发送完成: 成功 {success_count}, 失败 {fail_count} (随机选择)")
    except Exception as e:
        logger.error(f"发送开工信息失败: {e}", exc_info=True)


async def setup_start_work_schedule(bot):
    """设置开工信息定时任务（每天11:00执行）"""
    global scheduler

    if scheduler is None:
        scheduler = AsyncIOScheduler()
        scheduler.start()

    try:
        scheduler.add_job(
            send_start_work_messages,
            trigger=CronTrigger(hour=11, minute=0, timezone=BEIJING_TZ),
            args=[bot],
            id="start_work_messages",
            replace_existing=True,
        )
        logger.info("已设置开工信息任务: 每天 11:00 自动发送")
    except Exception as e:
        logger.error(f"设置开工信息任务失败: {e}", exc_info=True)


async def send_end_work_messages(bot):
    """发送收工信息到所有配置的总群（随机选择）"""
    try:
        configs = await db_operations.get_group_message_configs()

        if not configs:
            logger.info("没有配置的总群，跳过发送收工信息")
            return

        # 获取所有激活的收工消息（随机选择）
        end_work_messages = await db_operations.get_active_end_work_messages()

        if not end_work_messages:
            logger.warning("没有激活的收工消息，跳过发送")
            return

        # 随机选择一条收工消息
        message = random.choice(end_work_messages)

        # 处理多版本消息轮播（如果消息包含 ⸻ 分隔符）
        rotated_message = select_rotated_message(message)

        # 获取激活的防诈骗语录
        anti_fraud_messages = await db_operations.get_active_anti_fraud_messages()

        success_count = 0
        fail_count = 0

        for config in configs:
            chat_id = config.get("chat_id")

            if not chat_id:
                continue

            try:
                # 组合消息：主消息 + 防诈骗语录（防诈骗也是随机选择）
                final_message = _combine_message_with_anti_fraud(
                    rotated_message, anti_fraud_messages
                )

                # 发送消息（直接从数据库读取，不添加按钮）
                if await _send_group_message(bot, chat_id, final_message):
                    success_count += 1
                    logger.info(f"收工信息已发送到群组 {chat_id} (随机选择)")
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(f"发送收工信息到群组 {chat_id} 失败: {e}", exc_info=True)

        logger.info(f"收工信息发送完成: 成功 {success_count}, 失败 {fail_count} (随机选择)")
    except Exception as e:
        logger.error(f"发送收工信息失败: {e}", exc_info=True)


async def setup_end_work_schedule(bot):
    """设置收工信息定时任务（每天23:00执行）"""
    global scheduler

    if scheduler is None:
        scheduler = AsyncIOScheduler()
        scheduler.start()

    try:
        scheduler.add_job(
            send_end_work_messages,
            trigger=CronTrigger(hour=23, minute=0, timezone=BEIJING_TZ),
            args=[bot],
            id="end_work_messages",
            replace_existing=True,
        )
        logger.info("已设置收工信息任务: 每天 23:00 自动发送")
    except Exception as e:
        logger.error(f"设置收工信息任务失败: {e}", exc_info=True)


async def send_daily_operations_summary(bot):
    """发送每日操作汇总报告（每天23:00执行）"""
    try:
        from config import ADMIN_IDS
        from utils.date_helpers import get_daily_period_date

        date = get_daily_period_date()
        logger.info(f"开始生成每日操作汇总报告 ({date})")

        # 获取操作汇总
        summary = await db_operations.get_daily_operations_summary(date)

        if not summary or summary.get("total_count", 0) == 0:
            # 没有操作记录，发送提示
            message = f"📊 每日操作汇总 ({date})\n\n"
            message += "✅ 今日无操作记录"

            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(chat_id=admin_id, text=message)
                except Exception as e:
                    logger.error(f"发送操作汇总给管理员 {admin_id} 失败: {e}", exc_info=True)
            return

        # 操作类型的中文名称映射
        operation_type_names = {
            "order_created": "订单创建",
            "order_state_change": "订单状态变更",
            "order_completed": "订单完成",
            "order_breach_end": "违约完成",
            "interest": "利息收入",
            "principal_reduction": "本金减少",
            "expense": "开销记录",
            "funds_adjustment": "资金调整",
            "other": "其他操作",
        }

        # 格式化汇总消息
        message = f"📊 每日操作汇总 ({date})\n"
        message += "═══════════════════════════════════════\n"
        message += f"总操作数: {summary['total_count']}\n"
        message += f"有效操作: {summary['valid_count']}\n"
        message += f"已撤销: {summary['undone_count']}\n\n"

        # 按操作类型统计
        if summary.get("by_type"):
            message += "📋 按操作类型:\n"
            for op_type, count in sorted(
                summary["by_type"].items(), key=lambda x: x[1], reverse=True
            ):
                type_name = operation_type_names.get(op_type, op_type)
                message += f"  {type_name}: {count} 次\n"
            message += "\n"

        # 按用户统计（只显示前5个）
        if summary.get("by_user"):
            message += "👥 操作最多的用户 (Top 5):\n"
            user_stats = sorted(summary["by_user"].items(), key=lambda x: x[1], reverse=True)[:5]
            for user_id, count in user_stats:
                message += f"  用户 {user_id}: {count} 次\n"

        message += "\n使用 /daily_operations 查看详细操作记录"

        # 发送给所有管理员
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(chat_id=admin_id, text=message)
            except Exception as e:
                logger.error(f"发送操作汇总给管理员 {admin_id} 失败: {e}", exc_info=True)

        logger.info(f"每日操作汇总报告发送完成 ({date})")

    except Exception as e:
        logger.error(f"发送每日操作汇总报告失败: {e}", exc_info=True)


async def setup_daily_operations_summary(bot):
    """设置每日操作汇总定时任务（已禁用自动发送，仅保留命令查询功能）"""
    # 不再设置定时任务，用户可以通过 /daily_operations 和 /daily_operations_summary 命令查询
    # 功能保留，可以随时查询，但不输出日志
    pass


async def send_company_promotion_messages(bot):
    """轮播发送公司宣传语录到所有配置的总群（每3小时）"""
    await send_promotion_messages_internal(bot)


async def send_promotion_messages_internal(bot):
    """内部函数：发送公司宣传语录"""
    try:
        # 获取激活的宣传语录列表
        promotion_messages = await db_operations.get_active_promotion_messages()

        if not promotion_messages:
            logger.info("没有激活的公司宣传语录，跳过发送")
            return

        # 过滤掉空消息（双重检查，确保没有空消息）
        valid_messages = [
            msg for msg in promotion_messages if msg.get("message") and msg.get("message").strip()
        ]

        if not valid_messages:
            logger.warning("没有有效的公司宣传语录（所有消息都为空），跳过发送")
            return

        # 随机选择一条宣传语录（简化：直接随机选择，不进行轮换）
        selected_msg_dict = random.choice(valid_messages)
        selected_message = selected_msg_dict.get("message")

        if not selected_message or not selected_message.strip():
            logger.warning("选中的宣传语录消息为空，跳过发送")
            return

        # 获取激活的防诈骗语录
        anti_fraud_messages = await db_operations.get_active_anti_fraud_messages()

        # 组合消息：主消息 + 防诈骗语录
        final_message = _combine_message_with_anti_fraud(selected_message, anti_fraud_messages)

        # 获取所有配置的总群
        configs = await db_operations.get_group_message_configs()

        if not configs:
            logger.info("没有配置的总群，跳过发送公司宣传语录")
            return

        success_count = 0
        fail_count = 0

        for config in configs:
            chat_id = config.get("chat_id")

            if not chat_id:
                continue

            try:
                # 发送消息（直接从数据库读取，不添加按钮）
                if await _send_group_message(bot, chat_id, final_message):
                    success_count += 1
                    logger.info(f"公司宣传语录已发送到群组 {chat_id}")
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(f"发送公司宣传语录到群组 {chat_id} 失败: {e}", exc_info=True)

        logger.info(f"公司宣传语录发送完成: 成功 {success_count}, 失败 {fail_count}")
    except Exception as e:
        logger.error(f"发送公司宣传语录失败: {e}", exc_info=True)


# 公司公告定时任务已删除，保留手动发送功能（用于测试）


async def setup_promotion_messages_schedule(bot):
    """设置公司宣传语录轮播任务（每3小时执行一次）"""
    global scheduler

    if scheduler is None:
        scheduler = AsyncIOScheduler()
        scheduler.start()

    try:
        # 移除旧任务（如果存在）
        try:
            scheduler.remove_job("company_promotion_messages")
            logger.info("已移除旧的宣传语录任务")
        except Exception as e:
            logger.debug(f"移除旧任务时出错（可忽略）: {e}")

        # 添加定时任务（每3小时执行一次）
        scheduler.add_job(
            send_company_promotion_messages,
            trigger=IntervalTrigger(hours=3),
            args=[bot],
            id="promotion_messages_schedule",
            replace_existing=True,
        )
        logger.info("已设置公司宣传语录轮播任务: 每 3 小时自动发送")
    except Exception as e:
        logger.error(f"设置公司宣传语录轮播任务失败: {e}", exc_info=True)


# 增量报表功能已移除


# 余额统计任务已删除，改为实时统计（在余额更新时自动保存）


async def check_data_integrity(bot):
    """数据完整性检查（定时任务）"""
    try:
        from utils.data_integrity_checker import auto_fix_common_issues, check_orders_consistency

        logger.info("开始执行数据完整性检查...")

        # 执行一致性检查
        check_result = await check_orders_consistency()

        if check_result.get("status") == "issues_found":
            issues = check_result.get("issues", [])
            logger.warning(f"发现 {len(issues)} 个数据一致性问题")
            for issue in issues:
                logger.warning(f"  - {issue.get('message', '未知问题')}")

            # 尝试自动修复
            fix_result = await auto_fix_common_issues()
            if fix_result.get("status") == "success":
                fixes = fix_result.get("fixes_applied", [])
                if fixes:
                    logger.info(f"已自动修复 {len(fixes)} 个问题")
                    for fix in fixes:
                        logger.info(f"  - {fix}")
        elif check_result.get("status") == "success":
            logger.info("数据完整性检查通过")
        else:
            logger.error(f"数据完整性检查失败: {check_result.get('error')}")

    except Exception as e:
        logger.error(f"数据完整性检查失败: {e}", exc_info=True)


async def setup_data_integrity_check_schedule(bot):
    """设置数据完整性检查定时任务（每天凌晨3点执行）"""
    global scheduler

    if scheduler is None:
        scheduler = AsyncIOScheduler()
        scheduler.start()

    try:
        # 移除旧任务（如果存在）
        try:
            scheduler.remove_job("data_integrity_check")
            logger.info("已移除旧的数据完整性检查任务")
        except Exception as e:
            logger.debug(f"移除旧任务时出错（可忽略）: {e}")

        # 添加定时任务（每天凌晨3点执行）
        scheduler.add_job(
            check_data_integrity,
            trigger=CronTrigger(hour=3, minute=0, timezone=BEIJING_TZ),
            args=[bot],
            id="data_integrity_check",
            replace_existing=True,
        )
        logger.info("已设置数据完整性检查任务: 每天 03:00 自动检查")
    except Exception as e:
        logger.error(f"设置数据完整性检查任务失败: {e}", exc_info=True)


async def create_database_backup(bot):
    """创建数据库备份（定时任务）"""
    try:
        from utils.backup_manager import cleanup_old_backups, create_backup, verify_backup

        logger.info("开始创建数据库备份...")

        # 创建备份
        backup_path = create_backup()

        # 验证备份
        if verify_backup(backup_path):
            logger.info(f"数据库备份创建成功: {backup_path}")

            # 清理旧备份（只保留最新的10个）
            deleted_count = cleanup_old_backups(keep_count=10)
            if deleted_count > 0:
                logger.info(f"已清理 {deleted_count} 个旧备份")
        else:
            logger.error("数据库备份验证失败")

    except Exception as e:
        logger.error(f"创建数据库备份失败: {e}", exc_info=True)


async def setup_database_backup_schedule(bot):
    """设置数据库备份定时任务（每天凌晨2点执行）"""
    global scheduler

    if scheduler is None:
        scheduler = AsyncIOScheduler()
        scheduler.start()

    try:
        # 移除旧任务（如果存在）
        try:
            scheduler.remove_job("database_backup")
            logger.info("已移除旧的数据库备份任务")
        except Exception as e:
            logger.debug(f"移除旧任务时出错（可忽略）: {e}")

        # 添加定时任务（每天凌晨2点执行）
        scheduler.add_job(
            create_database_backup,
            trigger=CronTrigger(hour=2, minute=0, timezone=BEIJING_TZ),
            args=[bot],
            id="database_backup",
            replace_existing=True,
        )
        logger.info("已设置数据库备份任务: 每天 02:00 自动备份")
    except Exception as e:
        logger.error(f"设置数据库备份任务失败: {e}", exc_info=True)
