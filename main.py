"""Telegram订单管理机器人主入口"""

# 标准库导入
import logging
import os
import sys
from pathlib import Path

# 第三方库导入
from telegram import error as telegram_error
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters

# 本地模块导入
import init_db
from callbacks import button_callback, handle_order_action_callback, handle_schedule_callback
from callbacks.group_message_callbacks import handle_group_message_callback
from config import ADMIN_IDS, BOT_TOKEN
from decorators import (
    admin_required,
    authorized_required,
    error_handler,
    group_chat_only,
    private_chat_only,
)
from handlers import (
    add_employee,
    adjust_funds,
    broadcast_payment,
    check_mismatch,
    create_attribution,
    create_order,
    customer_contribution,
    diagnose_data_inconsistency,
    find_tail_orders,
    fix_income_statistics,
    fix_statistics,
    handle_amount_operation,
    handle_new_chat_members,
    handle_new_chat_title,
    handle_text_input,
    list_attributions,
    list_employees,
    list_user_group_mappings,
    remove_employee,
    remove_user_group_id,
    restore_daily_data,
    search_orders,
    set_breach,
    set_breach_end,
    set_end,
    set_normal,
    set_overdue,
    set_user_group_id,
    show_all_accounts,
    show_current_order,
    show_daily_operations,
    show_daily_operations_summary,
    show_gcash,
    show_my_report,
    show_order_table,
    show_paymaya,
    show_report,
    show_schedule_menu,
    start,
    undo_last_operation,
    update_weekday_groups,
)
from handlers.group_message_handlers import (
    add_group_config,
    batch_set_messages,
    get_group_id,
    manage_announcements,
    manage_anti_fraud_messages,
    manage_group_messages,
    manage_promotion_messages,
    setup_group_auto,
)
from utils.schedule_executor import setup_scheduled_broadcasts

# 确保项目根目录在 Python 路径中（必须在所有导入之前）
# 这样无论从哪里运行，都能找到所有模块
project_root = Path(__file__).parent.absolute()
project_root_str = str(project_root)

# 添加项目根目录到 Python 路径（如果还没有）
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)

# 现在可以安全地导入所有模块

# 配置日志（必须在导入其他模块之前）
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO if os.getenv("DEBUG", "0") != "1" else logging.DEBUG,
)
logger = logging.getLogger(__name__)

# 调试信息（仅在开发环境显示）
if os.getenv("DEBUG", "0") == "1":
    try:
        logger.debug(f"Project root: {project_root_str}")
        logger.debug(f"Current working directory: {os.getcwd()}")
        logger.debug(f"Python path includes project root: {project_root_str in sys.path}")
        logger.debug(
            f"Handlers directory exists: {Path(project_root / 'handlers' / '__init__.py').exists()}"
        )
    except Exception as e:
        logger.debug(f"Error in debug output: {e}")


# 日志已在上面配置


def main() -> None:
    """启动机器人"""
    # 自动导入数据库备份（如果存在且数据库为空）
    try:
        from utils.db_helpers import import_database_backup, is_database_empty

        data_dir = os.getenv("DATA_DIR", project_root_str)
        db_path = os.path.join(data_dir, "loan_bot.db")

        # 检查备份文件位置（优先检查Volume目录，其次检查项目根目录）
        backup_file = None
        backup_file_in_data = os.path.join(data_dir, "database_backup.sql")
        backup_file_in_root = os.path.join(project_root_str, "database_backup.sql")

        if os.path.exists(backup_file_in_data):
            backup_file = backup_file_in_data
            logger.info(f"在Volume目录找到备份文件: {backup_file}")
        elif os.path.exists(backup_file_in_root):
            backup_file = backup_file_in_root
            logger.info(f"在项目根目录找到备份文件: {backup_file}")

        # 检查是否存在备份文件且数据库不存在或为空
        if backup_file:
            should_import = False
            import_reason = ""

            if not os.path.exists(db_path):
                should_import = True
                import_reason = "数据库不存在"
            elif is_database_empty(db_path):
                should_import = True
                import_reason = "数据库为空"

            if should_import:
                logger.info(
                    f"检测到数据库备份文件 ({backup_file})，开始导入（原因：{import_reason}）..."
                )

                if import_database_backup(backup_file, db_path):
                    logger.info("数据库备份导入成功")
                    # 导入成功后，可选：删除备份文件（避免重复导入）
                    # os.remove(backup_file)
                else:
                    logger.error("导入数据库备份失败")
                    # 继续执行，让 init_db 创建新数据库
    except Exception as e:
        logger.debug(f"自动导入数据库时出错: {e}")
        # 不影响正常启动

    # 验证配置
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN 未设置，无法启动机器人")
        if os.getenv("DEBUG", "0") == "1":
            print("\n❌ 错误: BOT_TOKEN 未设置")
            print("请检查 config.py 文件或环境变量")
        return

    if not ADMIN_IDS:
        logger.error("ADMIN_USER_IDS 未设置，无法启动机器人")
        if os.getenv("DEBUG", "0") == "1":
            print("\n❌ 错误: ADMIN_USER_IDS 未设置")
            print("请检查 config.py 文件或环境变量")
        return

    logger.info(f"机器人启动中... 管理员数量: {len(ADMIN_IDS)}")
    if os.getenv("DEBUG", "0") == "1":
        print("\n机器人启动中...")
        print(f"管理员数量: {len(ADMIN_IDS)}")

    # 初始化数据库（如果不存在）
    logger.info("检查数据库...")
    try:
        init_db.init_database()
        logger.info("数据库已就绪")
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}", exc_info=True)
        if os.getenv("DEBUG", "0") == "1":
            print(f"数据库初始化失败: {e}")
        return

    try:
        # 创建Application并传入bot的token
        application = Application.builder().token(BOT_TOKEN).build()
        logger.info("应用创建成功")
    except Exception as e:
        logger.error(f"创建应用时出错: {e}", exc_info=True)
        if os.getenv("DEBUG", "0") == "1":
            print(f"\n❌ 创建应用时出错: {e}")
        return

    # 添加命令处理器
    # 权限检查命令（无需权限，任何人都可以使用）
    from handlers.command_handlers import check_permission

    application.add_handler(CommandHandler("check_permission", check_permission))

    # 基础命令（私聊，需要授权）
    application.add_handler(
        CommandHandler("start", private_chat_only(authorized_required(error_handler(start))))
    )
    application.add_handler(
        CommandHandler("report", private_chat_only(authorized_required(error_handler(show_report))))
    )
    application.add_handler(
        CommandHandler("myreport", private_chat_only(error_handler(show_my_report)))
    )
    application.add_handler(
        CommandHandler(
            "search", private_chat_only(authorized_required(error_handler(search_orders)))
        )
    )
    application.add_handler(
        CommandHandler(
            "accounts", private_chat_only(authorized_required(error_handler(show_all_accounts)))
        )
    )
    application.add_handler(
        CommandHandler("gcash", private_chat_only(authorized_required(error_handler(show_gcash))))
    )
    application.add_handler(
        CommandHandler(
            "paymaya", private_chat_only(authorized_required(error_handler(show_paymaya)))
        )
    )
    application.add_handler(
        CommandHandler(
            "schedule", private_chat_only(authorized_required(error_handler(show_schedule_menu)))
        )
    )

    # 订单总表（私聊，仅管理员）- 函数内部已有装饰器和权限检查
    application.add_handler(CommandHandler("ordertable", show_order_table))

    # 群组消息管理（私聊，仅管理员）
    # /groupmsg - 管理群组消息配置（开工、收工、欢迎信息）
    # /groupmsg_add - 添加总群配置
    # /groupmsg_getid - 获取群组ID（在群组中使用）
    # /groupmsg_setup - 一键设置群组/频道自动消息功能（在群组或频道中使用）
    # /announcement - 管理公司公告
    # /antifraud - 管理防诈骗语录
    # /promotion - 管理公司宣传轮播语录
    application.add_handler(CommandHandler("groupmsg", manage_group_messages))
    application.add_handler(CommandHandler("groupmsg_add", add_group_config))
    application.add_handler(CommandHandler("groupmsg_getid", get_group_id))
    application.add_handler(CommandHandler("groupmsg_setup", setup_group_auto))
    application.add_handler(CommandHandler("groupmsg_batch", batch_set_messages))
    application.add_handler(CommandHandler("announcement", manage_announcements))
    application.add_handler(CommandHandler("antifraud", manage_anti_fraud_messages))
    application.add_handler(CommandHandler("promotion", manage_promotion_messages))

    # 初始化消息范本（私聊，仅管理员）
    # 注意：这些函数暂时未实现，如需使用请参考 scripts/init_default_templates.py
    # from handlers.command_handlers import init_templates, fill_empty_messages, test_broadcast
    # application.add_handler(CommandHandler("init_templates", init_templates))
    # application.add_handler(CommandHandler("fill_empty_messages", fill_empty_messages))
    # application.add_handler(CommandHandler("test_broadcast", test_broadcast))

    # 增量报表命令（仅管理员）
    from handlers.command_handlers import (
        merge_incremental_report_cmd,
        preview_incremental_report_cmd,
    )

    application.add_handler(CommandHandler("preview_incremental", preview_incremental_report_cmd))
    application.add_handler(CommandHandler("merge_incremental", merge_incremental_report_cmd))

    # 每日数据变更表命令（仅管理员）
    from handlers.daily_changes_handlers import show_daily_changes_table

    application.add_handler(CommandHandler("daily_changes", show_daily_changes_table))

    # 每日操作记录命令（仅管理员）
    application.add_handler(CommandHandler("daily_operations", show_daily_operations))
    application.add_handler(
        CommandHandler("daily_operations_summary", show_daily_operations_summary)
    )
    application.add_handler(CommandHandler("restore_daily_data", restore_daily_data))

    # 余额历史查询命令
    from handlers.payment_handlers import balance_history

    application.add_handler(CommandHandler("balance_history", balance_history))

    # 订单操作命令（群组，需要授权）
    application.add_handler(
        CommandHandler("create", error_handler(authorized_required(group_chat_only(create_order))))
    )
    application.add_handler(
        CommandHandler("normal", authorized_required(group_chat_only(set_normal)))
    )
    application.add_handler(
        CommandHandler("overdue", authorized_required(group_chat_only(set_overdue)))
    )
    application.add_handler(CommandHandler("end", authorized_required(group_chat_only(set_end))))
    application.add_handler(
        CommandHandler("breach", authorized_required(group_chat_only(set_breach)))
    )
    application.add_handler(
        CommandHandler("breach_end", authorized_required(group_chat_only(set_breach_end)))
    )
    application.add_handler(
        CommandHandler("order", authorized_required(group_chat_only(show_current_order)))
    )
    application.add_handler(
        CommandHandler("broadcast", authorized_required(group_chat_only(broadcast_payment)))
    )

    # 撤销操作命令（群组或私聊，需要授权）
    application.add_handler(
        CommandHandler("undo", authorized_required(error_handler(undo_last_operation)))
    )

    # 资金和归属ID管理（私聊，仅管理员）
    application.add_handler(
        CommandHandler("adjust", private_chat_only(admin_required(adjust_funds)))
    )
    application.add_handler(
        CommandHandler("create_attribution", private_chat_only(admin_required(create_attribution)))
    )
    application.add_handler(
        CommandHandler("list_attributions", private_chat_only(admin_required(list_attributions)))
    )

    # 员工管理（私聊，仅管理员）
    application.add_handler(
        CommandHandler("add_employee", private_chat_only(admin_required(add_employee)))
    )
    application.add_handler(
        CommandHandler("remove_employee", private_chat_only(admin_required(remove_employee)))
    )
    application.add_handler(
        CommandHandler("list_employees", private_chat_only(admin_required(list_employees)))
    )
    application.add_handler(
        CommandHandler(
            "update_weekday_groups", private_chat_only(admin_required(update_weekday_groups))
        )
    )
    application.add_handler(
        CommandHandler("fix_statistics", private_chat_only(admin_required(fix_statistics)))
    )
    application.add_handler(
        CommandHandler(
            "fix_income_statistics", private_chat_only(admin_required(fix_income_statistics))
        )
    )
    application.add_handler(
        CommandHandler("find_tail_orders", private_chat_only(admin_required(find_tail_orders)))
    )
    application.add_handler(
        CommandHandler("set_user_group_id", private_chat_only(admin_required(set_user_group_id)))
    )
    application.add_handler(
        CommandHandler(
            "remove_user_group_id", private_chat_only(admin_required(remove_user_group_id))
        )
    )
    application.add_handler(
        CommandHandler(
            "list_user_group_mappings", private_chat_only(admin_required(list_user_group_mappings))
        )
    )
    application.add_handler(
        CommandHandler("check_mismatch", private_chat_only(admin_required(check_mismatch)))
    )
    application.add_handler(
        CommandHandler(
            "diagnose_data", private_chat_only(admin_required(diagnose_data_inconsistency))
        )
    )
    application.add_handler(
        CommandHandler("customer", private_chat_only(admin_required(customer_contribution)))
    )

    # 自动订单创建（新成员入群监听 & 群名变更监听）
    application.add_handler(
        MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_chat_members)
    )
    application.add_handler(
        MessageHandler(filters.StatusUpdate.NEW_CHAT_TITLE, handle_new_chat_title)
    )

    # 添加消息处理器（金额操作）- 需要管理员或员工权限
    # 只处理以 + 开头的消息（快捷操作）
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.Regex(r"^\+") & filters.ChatType.GROUPS,
            handle_amount_operation,
        ),
        group=1,
    )  # 设置优先级组

    # 添加通用文本处理器（用于处理搜索和群发输入）
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(r"^\+"), handle_text_input),
        group=2,
    )

    # 添加回调查询处理器
    application.add_handler(
        CallbackQueryHandler(
            authorized_required(handle_order_action_callback), pattern="^order_action_"
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            authorized_required(handle_order_action_callback), pattern="^order_change_to_"
        )
    )
    application.add_handler(
        CallbackQueryHandler(authorized_required(handle_schedule_callback), pattern="^schedule_")
    )
    # 群组消息回调（groupmsg_*, announcement_*）
    application.add_handler(
        CallbackQueryHandler(
            authorized_required(handle_group_message_callback),
            pattern="^(groupmsg_|announcement_|antifraud_|promotion_|batch_set_|test_)",
        )
    )
    # 其他回调（报表、搜索、支付等）
    application.add_handler(CallbackQueryHandler(button_callback))

    # 启动机器人
    try:
        # 设置命令菜单
        commands = [
            ("create", "Create new order"),
            ("order", "Manage current order"),
            ("report", "View reports"),
            ("broadcast", "Broadcast payment reminder"),
            ("schedule", "Manage scheduled broadcasts"),
            ("accounts", "View all payment accounts"),
            ("gcash", "GCASH account info"),
            ("paymaya", "PayMaya account info"),
            ("start", "Start/Help"),
        ]

        async def post_init(application: Application):
            await application.bot.set_my_commands(commands)
            logger.info("命令菜单已更新")

            # 初始化定时播报任务
            await setup_scheduled_broadcasts(application.bot)
            logger.info("定时播报任务已初始化")

            # 初始化日切报表任务
            from utils.schedule_executor import setup_daily_report

            await setup_daily_report(application.bot)
            logger.info("日切报表任务已初始化")

            # 初始化群组消息定时任务
            # 开工信息：每天11:00发送
            # 收工信息：每天23:00发送
            # 公司公告：每3小时（可配置）随机发送
            # 公司宣传轮播：每2小时轮播发送
            from utils.schedule_executor import (
                setup_alternating_messages_schedule,
                setup_daily_balance_save,
                setup_daily_operations_summary,
                setup_end_work_schedule,
                setup_incremental_orders_report,
                setup_start_work_schedule,
            )

            await setup_start_work_schedule(application.bot)
            await setup_end_work_schedule(application.bot)
            # 使用统一的消息发送任务（公告和宣传语录交替发送）
            await setup_alternating_messages_schedule(application.bot)
            # 初始化增量订单报表定时任务（每天23:05发送）
            await setup_incremental_orders_report(application.bot)
            # 每日操作汇总功能（已禁用自动发送，仅保留命令查询）
            await setup_daily_operations_summary(application.bot)
            # 每日余额统计任务（每天11:00保存）
            await setup_daily_balance_save(application.bot)
            logger.info("群组消息定时任务已初始化")

        logger.info("机器人已启动，等待消息...")
        application.post_init = post_init
        # 启动机器人
        application.run_polling(drop_pending_updates=True)
    except telegram_error.Conflict:
        logger.error("机器人冲突错误：检测到多个机器人实例正在运行", exc_info=True)
        if os.getenv("DEBUG", "0") == "1":
            print("\n" + "=" * 60)
            print("⚠️ 检测到多个机器人实例正在运行！")
            print("=" * 60)
            print("\n可能的原因：")
            print("  1. 本地和部署环境（Zeabur）同时运行")
            print("  2. 多个本地实例在运行")
            print("  3. 之前的进程没有正确关闭")
            print("\n解决方法：")
            print("  1. 停止本地运行的机器人（按 Ctrl+C）")
            print("  2. 如果要在本地测试，先停止 Zeabur 部署的实例")
            print("  3. 确保只有一个实例在运行")
            print("\n当前检测到多个 Python 进程，请检查：")
            print("  - 是否有其他终端窗口在运行机器人")
            print("  - 是否有后台进程在运行")
            print("=" * 60)
        return
    except telegram_error.InvalidToken:
        logger.error("Token 无效或被拒绝")
        if os.getenv("DEBUG", "0") == "1":
            print("\n" + "=" * 60)
            print("❌ Token 无效或被拒绝！")
            print("=" * 60)
            print("\n可能的原因：")
            print("  1. Token 已过期或被撤销")
            print("  2. Token 格式不正确")
            print("  3. Token 不属于你的机器人")
            print("\n解决方法：")
            print("  1. 在 Telegram 中搜索 @BotFather")
            print("  2. 发送 /mybots 查看你的机器人列表")
            print("  3. 选择你的机器人，点击 'API Token'")
            print("  4. 复制新的 Token")
            print("  5. 更新环境变量或 config.py 文件中的 BOT_TOKEN")
            print("\n当前使用的 Token（已隐藏部分）:")
            if BOT_TOKEN:
                masked_token = (
                    BOT_TOKEN[:10] + "..." + BOT_TOKEN[-10:] if len(BOT_TOKEN) > 20 else "***"
                )
                print(f"  {masked_token}")
            print("=" * 60)
    except KeyboardInterrupt:
        logger.info("机器人被用户停止")
        if os.getenv("DEBUG", "0") == "1":
            print("\n\n👋 机器人已停止")
    except Exception as e:
        logger.error(f"运行时错误: {e}", exc_info=True)
        if os.getenv("DEBUG", "0") == "1":
            print(f"\n❌ 运行时发生错误: {e}")
            import traceback

            traceback.print_exc()
            input("\n按Enter键退出...")
        else:
            # 生产环境：记录错误后退出
            logger.critical("生产环境发生严重错误，机器人退出")


if __name__ == "__main__":
    main()
