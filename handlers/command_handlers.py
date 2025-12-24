"""命令处理器"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

import db_operations
from decorators import (
    admin_required,
    authorized_required,
    error_handler,
    group_chat_only,
    private_chat_only,
)
from utils.incremental_report_generator import get_or_create_baseline_date, prepare_incremental_data
from utils.incremental_report_merger import (
    merge_incremental_report_to_global,
    preview_incremental_report,
)
from utils.order_helpers import try_create_order_from_title
from utils.stats_helpers import update_liquid_capital

logger = logging.getLogger(__name__)


@error_handler
async def check_permission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """检查当前用户的权限状态（所有人可用）"""
    from config import ADMIN_IDS

    user_id = update.effective_user.id
    username = update.effective_user.username or "无"
    first_name = update.effective_user.first_name or "无"

    # 检查是否为管理员
    is_admin = user_id in ADMIN_IDS

    # 检查是否为授权用户
    is_authorized = await db_operations.is_user_authorized(user_id)

    # 获取用户可访问的归属ID
    user_group_ids = await db_operations.get_user_group_ids(user_id)

    # 构建权限信息
    permission_info = []
    permission_info.append("👤 用户信息:")
    permission_info.append(f"  ID: {user_id}")
    permission_info.append(f"  用户名: @{username}")
    permission_info.append(f"  姓名: {first_name}")
    permission_info.append("")
    permission_info.append("🔐 权限状态:")

    if is_admin:
        permission_info.append("  ✅ 管理员")
    else:
        permission_info.append("  ❌ 非管理员")

    if is_authorized:
        permission_info.append("  ✅ 授权用户")
    else:
        permission_info.append("  ❌ 未授权用户")

    if user_group_ids:
        permission_info.append("")
        permission_info.append("📋 可访问的归属ID:")
        for group_id in user_group_ids:
            permission_info.append(f"  - {group_id}")
    else:
        permission_info.append("")
        permission_info.append("📋 可访问的归属ID: 无")

    # 发送权限信息
    message = "\n".join(permission_info)
    await update.message.reply_text(message)


@error_handler
@private_chat_only
@authorized_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """发送欢迎消息"""
    financial_data = await db_operations.get_financial_data()

    await update.message.reply_text(
        "📋 订单管理系统\n\n"
        "💰 当前流动资金: {:.2f}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💬 群聊命令 (Group Commands)\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📝 订单操作:\n"
        "/create - 读取群名创建新订单\n"
        "/order - 管理当前订单\n\n"
        "⚡ 快捷操作:\n"
        "+<金额>b - 减少本金\n"
        "+<金额> - 利息收入\n\n"
        "🔄 状态变更:\n"
        "/normal - 设为正常\n"
        "/overdue - 设为逾期\n"
        "/end - 标记为完成\n"
        "/breach - 标记为违约\n"
        "/breach_end - 违约完成\n\n"
        "📢 播报:\n"
        "/broadcast - 播报付款提醒\n\n"
        "🔄 撤销操作:\n"
        "/undo - 撤销上一个操作（最多连续3次）\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💼 私聊命令 (Private Commands)\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📊 查询:\n"
        "/report [归属ID] - 查看报表\n"
        "/myreport - 查看我的报表（仅限有权限的归属ID）\n"
        "/ordertable - 订单总表（仅管理员）\n"
        "/search <类型> <值> - 搜索订单\n"
        "  类型: order_id/group_id/customer/state/date\n\n"
        "📢 播报:\n"
        "/schedule - 管理定时播报（最多3个）\n\n"
        "💳 支付账号:\n"
        "/accounts - 查看所有账户数据表格\n"
        "/gcash - 查看GCASH账号\n"
        "/paymaya - 查看PayMaya账号\n\n"
        "🔄 撤销操作:\n"
        "/undo - 撤销上一个操作（最多连续3次）\n\n"
        "⚙️ 管理:\n"
        "/adjust <金额> [备注] - 调整资金\n"
        "/create_attribution <ID> - 创建归属ID\n"
        "/list_attributions - 列出归属ID\n"
        "/add_employee <ID> - 添加员工\n"
        "/remove_employee <ID> - 移除员工\n"
        "/list_employees - 列出员工\n"
        "/set_user_group_id <用户ID> <归属ID> - 设置用户归属ID权限\n"
        "/remove_user_group_id <用户ID> - 移除用户归属ID权限\n"
        "/list_user_group_mappings - 列出所有用户归属ID映射\n"
        "/update_weekday_groups - 更新星期分组\n"
        "/fix_statistics - 修复统计数据\n"
        "/find_tail_orders - 查找尾数订单\n"
        "/check_mismatch [日期] - 检查收入明细和统计数据不一致\n\n"
        "⚠️ 部分操作需要管理员权限".format(financial_data["liquid_funds"])
    )


@error_handler
@authorized_required
@group_chat_only
async def create_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """创建新订单 (读取群名)"""
    try:
        chat = update.effective_chat
        if not chat:
            logger.error("Cannot get chat from update")
            return

        title = chat.title
        if not title:
            await update.message.reply_text("❌ Cannot get group title.")
            return

        logger.info(f"Creating order from title: {title} in chat {chat.id}")
        await try_create_order_from_title(update, context, chat, title, manual_trigger=True)
    except Exception as e:
        logger.error(f"Error in create_order: {e}", exc_info=True)
        if update.message:
            await update.message.reply_text(f"❌ Error creating order: {str(e)}")


@authorized_required
@group_chat_only
async def show_current_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示当前订单状态和操作菜单"""
    # 支持 CommandHandler 和 CallbackQueryHandler
    if update.message:
        chat_id = update.message.chat_id
        reply_func = update.message.reply_text
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id
        reply_func = update.callback_query.message.reply_text
    else:
        return

    order = await db_operations.get_order_by_chat_id(chat_id)
    if not order:
        await reply_func("❌ No active order in this group.\nUse /create to start a new order.")
        return

    # 查询该订单的利息总额
    interest_info = await db_operations.get_interest_by_order_id(order["order_id"])
    interest_total = interest_info.get("total_amount", 0.0) or 0.0
    interest_count = interest_info.get("count", 0) or 0

    # 构建订单信息
    msg = (
        "📋 Current Order Status:\n"
        "──────────────────\n"
        f"📝 Order ID: `{order['order_id']}`\n"
        f"🏷️ Group ID: `{order['group_id']}`\n"
        f"📅 Date: {order['date']}\n"
        f"👥 Week Group: {order['weekday_group']}\n"
        f"👤 Customer: {order['customer']}\n"
        f"💰 Amount: {order['amount']:.2f}\n"
        f"📊 State: {order['state']}\n"
    )

    # 添加利息信息
    if interest_count > 0:
        msg += (
            "──────────────────\n"
            "💵 Interest Collected:\n"
            f"   Total: {interest_total:,.2f}\n"
            f"   Times: {interest_count}\n"
        )
    else:
        msg += "──────────────────\n" "💵 Interest Collected: 0.00\n"

    msg += "──────────────────"

    # 构建操作按钮（群聊使用英文）
    keyboard = [
        [
            InlineKeyboardButton("✅ Normal", callback_data="order_action_normal"),
            InlineKeyboardButton("⚠️ Overdue", callback_data="order_action_overdue"),
        ],
        [
            InlineKeyboardButton("🏁 End", callback_data="order_action_end"),
            InlineKeyboardButton("🚫 Breach", callback_data="order_action_breach"),
        ],
        [InlineKeyboardButton("💸 Breach End", callback_data="order_action_breach_end")],
        [InlineKeyboardButton("💳 Send Account", callback_data="payment_select_account")],
        [
            InlineKeyboardButton(
                "🔄 Change Attribution", callback_data="order_action_change_attribution"
            )
        ],
    ]

    await reply_func(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


@error_handler
@admin_required
@private_chat_only
async def adjust_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """调整流动资金余额命令"""
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "❌ 用法: /adjust <金额> [备注]\n"
            "示例: /adjust +5000 收入备注\n"
            "      /adjust -3000 支出备注"
        )
        return

    amount_str = context.args[0]
    note = " ".join(context.args[1:]) if len(context.args) > 1 else "无备注"

    # 验证金额格式
    if not (amount_str.startswith("+") or amount_str.startswith("-")):
        await update.message.reply_text("❌ 金额格式错误，请使用+100或-200格式")
        return

    amount = float(amount_str)
    if amount == 0:
        await update.message.reply_text("❌ 调整金额不能为0")
        return

    # 更新财务数据
    await update_liquid_capital(amount)

    financial_data = await db_operations.get_financial_data()
    await update.message.reply_text(
        "✅ 资金调整成功\n"
        f"调整类型: {'增加' if amount > 0 else '减少'}\n"
        f"调整金额: {abs(amount):.2f}\n"
        f"调整后余额: {financial_data['liquid_funds']:.2f}\n"
        f"备注: {note}"
    )


@admin_required
@private_chat_only
async def create_attribution(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """创建新的归属ID"""
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "❌ 用法: /create_attribution <归属ID>\n示例: /create_attribution S03"
        )
        return

    group_id = context.args[0].upper()

    # 验证格式
    if len(group_id) != 3 or not group_id[0].isalpha() or not group_id[1:].isdigit():
        await update.message.reply_text("❌ 格式错误，正确格式：字母+两位数字（如S01）")
        return

    # 检查是否已存在
    existing_groups = await db_operations.get_all_group_ids()
    if group_id in existing_groups:
        await update.message.reply_text(f"⚠️ 归属ID {group_id} 已存在")
        return

    # 创建分组数据记录
    await db_operations.update_grouped_data(group_id, "valid_orders", 0)
    await update.message.reply_text(f"✅ 成功创建归属ID {group_id}")


@admin_required
@private_chat_only
async def list_attributions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """列出所有归属ID"""
    group_ids = await db_operations.get_all_group_ids()

    if not group_ids:
        await update.message.reply_text("暂无归属ID，使用 /create_attribution <ID> 创建")
        return

    message = "📋 所有归属ID:\n\n"
    for i, group_id in enumerate(sorted(group_ids), 1):
        data = await db_operations.get_grouped_data(group_id)
        message += (
            f"{i}. {group_id}\n"
            f"   有效订单: {data['valid_orders']} | "
            f"金额: {data['valid_amount']:.2f}\n"
        )

    await update.message.reply_text(message)


@admin_required
@private_chat_only
async def add_employee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """添加员工（授权用户）"""
    if not context.args:
        await update.message.reply_text("❌ 用法: /add_employee <用户ID>")
        return

    try:
        user_id = int(context.args[0])
        if await db_operations.add_authorized_user(user_id):
            await update.message.reply_text(f"✅ 已添加员工: {user_id}")
        else:
            await update.message.reply_text("⚠️ 添加失败或用户已存在")
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")


@admin_required
@private_chat_only
async def remove_employee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """移除员工（授权用户）"""
    if not context.args:
        await update.message.reply_text("❌ 用法: /remove_employee <用户ID>")
        return

    try:
        user_id = int(context.args[0])
        if await db_operations.remove_authorized_user(user_id):
            await update.message.reply_text(f"✅ 已移除员工: {user_id}")
        else:
            await update.message.reply_text("⚠️ 移除失败或用户不存在")
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")


@admin_required
@private_chat_only
async def update_weekday_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """更新所有订单的星期分组（管理员命令）"""
    try:
        msg = await update.message.reply_text("🔄 开始更新所有订单的星期分组...")

        # 直接调用更新逻辑
        from datetime import datetime

        from utils.chat_helpers import get_weekday_group_from_date

        all_orders = await db_operations.search_orders_advanced_all_states({})

        if not all_orders:
            await msg.edit_text("❌ 没有找到订单")
            return

        updated_count = 0
        error_count = 0
        skipped_count = 0

        for order in all_orders:
            order_id = order["order_id"]
            chat_id = order["chat_id"]
            order_date_str = order.get("date", "")

            try:
                # 从订单ID解析日期
                date_from_id = None
                if order_id.startswith("A"):
                    if len(order_id) >= 7 and order_id[1:7].isdigit():
                        date_part = order_id[1:7]
                        try:
                            full_date_str = f"20{date_part}"
                            date_from_id = datetime.strptime(full_date_str, "%Y%m%d").date()
                        except ValueError:
                            pass
                else:
                    if len(order_id) >= 6 and order_id[:6].isdigit():
                        date_part = order_id[:6]
                        try:
                            full_date_str = f"20{date_part}"
                            date_from_id = datetime.strptime(full_date_str, "%Y%m%d").date()
                        except ValueError:
                            pass

                # 从date字段解析日期
                date_from_db = None
                if order_date_str:
                    try:
                        date_str = (
                            order_date_str.split()[0] if " " in order_date_str else order_date_str
                        )
                        date_from_db = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        pass

                order_date = date_from_id or date_from_db

                if not order_date:
                    skipped_count += 1
                    continue

                # 计算正确的星期分组
                correct_weekday_group = get_weekday_group_from_date(order_date)

                # 更新
                success = await db_operations.update_order_weekday_group(
                    chat_id, correct_weekday_group
                )

                if success:
                    updated_count += 1
                else:
                    error_count += 1

            except Exception as e:
                logger.error(f"处理订单 {order_id} 时出错: {e}")
                error_count += 1

        result_msg = (
            "✅ 更新完成！\n\n"
            f"已更新: {updated_count} 个订单\n"
            f"跳过: {skipped_count} 个订单\n"
            f"错误: {error_count} 个订单\n"
            f"总计: {len(all_orders)} 个订单"
        )

        await msg.edit_text(result_msg)

    except Exception as e:
        logger.error(f"更新星期分组时出错: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 更新失败: {str(e)}")


@admin_required
@private_chat_only
async def fix_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """修复统计数据：根据实际订单数据重新计算所有统计数据（管理员命令）"""
    try:
        msg = await update.message.reply_text("🔄 开始修复统计数据...")

        # 直接在这里实现修复逻辑
        all_orders = await db_operations.search_orders_advanced_all_states({})
        all_group_ids = list(
            set(order.get("group_id") for order in all_orders if order.get("group_id"))
        )

        fixed_count = 0
        fixed_groups = []

        for group_id in sorted(all_group_ids):
            group_orders = [o for o in all_orders if o.get("group_id") == group_id]
            valid_orders = [o for o in group_orders if o.get("state") in ["normal", "overdue"]]

            actual_valid_count = len(valid_orders)
            actual_valid_amount = sum(o.get("amount", 0) for o in valid_orders)

            grouped_data = await db_operations.get_grouped_data(group_id)

            valid_count_diff = actual_valid_count - grouped_data["valid_orders"]
            valid_amount_diff = actual_valid_amount - grouped_data["valid_amount"]

            if abs(valid_count_diff) > 0 or abs(valid_amount_diff) > 0.01:
                if valid_count_diff != 0:
                    await db_operations.update_grouped_data(
                        group_id, "valid_orders", valid_count_diff
                    )
                if abs(valid_amount_diff) > 0.01:
                    await db_operations.update_grouped_data(
                        group_id, "valid_amount", valid_amount_diff
                    )
                fixed_count += 1
                fixed_groups.append(
                    f"{group_id} (订单数: {valid_count_diff}, 金额: {valid_amount_diff:,.2f})"
                )

        # 修复全局统计
        all_valid_orders = [o for o in all_orders if o.get("state") in ["normal", "overdue"]]
        global_valid_count = len(all_valid_orders)
        global_valid_amount = sum(o.get("amount", 0) for o in all_valid_orders)

        financial_data = await db_operations.get_financial_data()
        global_valid_count_diff = global_valid_count - financial_data["valid_orders"]
        global_valid_amount_diff = global_valid_amount - financial_data["valid_amount"]

        if abs(global_valid_count_diff) > 0 or abs(global_valid_amount_diff) > 0.01:
            if global_valid_count_diff != 0:
                await db_operations.update_financial_data("valid_orders", global_valid_count_diff)
            if abs(global_valid_amount_diff) > 0.01:
                await db_operations.update_financial_data("valid_amount", global_valid_amount_diff)
            fixed_count += 1

        if fixed_count > 0:
            result_msg = f"✅ 统计数据修复完成！\n\n已修复 {fixed_count} 个归属ID的统计数据。"
            if fixed_groups:
                result_msg += f"\n\n修复的归属ID:\n" + "\n".join(f"• {g}" for g in fixed_groups)
        else:
            result_msg = "✅ 统计数据一致，无需修复。"

        await msg.edit_text(result_msg)

    except Exception as e:
        logger.error(f"修复统计数据时出错: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 修复失败: {str(e)}")


@admin_required
@private_chat_only
@error_handler
async def fix_income_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """修复收入统计数据：根据收入明细重新计算所有收入统计数据（管理员命令）"""
    try:
        msg = await update.message.reply_text("🔄 开始修复收入统计数据...")

        # 获取所有收入明细
        income_records = await db_operations.get_income_records("1970-01-01", "2099-12-31")

        # 计算收入明细汇总
        income_summary = {
            "interest": 0.0,
            "completed_amount": 0.0,
            "breach_end_amount": 0.0,
            "completed_count": 0,
            "breach_end_count": 0,
        }

        # 按日期和归属ID分组统计
        daily_income = {}  # {date: {group_id: {type: amount}}}
        global_income = {}  # {type: amount}

        for record in income_records:
            record_type = record.get("type", "")
            amount = record.get("amount", 0.0) or 0.0
            date = record.get("date", "")
            group_id = record.get("group_id")

            if record_type == "interest":
                income_summary["interest"] += amount
                global_income["interest"] = global_income.get("interest", 0.0) + amount
                if date not in daily_income:
                    daily_income[date] = {}
                if group_id not in daily_income[date]:
                    daily_income[date][group_id] = {}
                daily_income[date][group_id]["interest"] = (
                    daily_income[date][group_id].get("interest", 0.0) + amount
                )
            elif record_type == "completed":
                income_summary["completed_amount"] += amount
                income_summary["completed_count"] += 1
                global_income["completed_amount"] = (
                    global_income.get("completed_amount", 0.0) + amount
                )
                global_income["completed_count"] = global_income.get("completed_count", 0) + 1
                if date not in daily_income:
                    daily_income[date] = {}
                if group_id not in daily_income[date]:
                    daily_income[date][group_id] = {}
                daily_income[date][group_id]["completed_amount"] = (
                    daily_income[date][group_id].get("completed_amount", 0.0) + amount
                )
                daily_income[date][group_id]["completed_count"] = (
                    daily_income[date][group_id].get("completed_count", 0) + 1
                )
            elif record_type == "breach_end":
                income_summary["breach_end_amount"] += amount
                income_summary["breach_end_count"] += 1
                global_income["breach_end_amount"] = (
                    global_income.get("breach_end_amount", 0.0) + amount
                )
                global_income["breach_end_count"] = global_income.get("breach_end_count", 0) + 1
                if date not in daily_income:
                    daily_income[date] = {}
                if group_id not in daily_income[date]:
                    daily_income[date][group_id] = {}
                daily_income[date][group_id]["breach_end_amount"] = (
                    daily_income[date][group_id].get("breach_end_amount", 0.0) + amount
                )
                daily_income[date][group_id]["breach_end_count"] = (
                    daily_income[date][group_id].get("breach_end_count", 0) + 1
                )

        # 获取当前统计数据
        financial_data = await db_operations.get_financial_data()
        await db_operations.get_stats_by_date_range("1970-01-01", "2099-12-31", None)

        fixed_items = []

        # 修复全局统计数据（financial_data表）
        interest_diff = income_summary["interest"] - financial_data.get("interest", 0.0)
        if abs(interest_diff) > 0.01:
            await db_operations.update_financial_data("interest", interest_diff)
            fixed_items.append(f"全局利息收入: {interest_diff:+,.2f}")

        completed_amount_diff = income_summary["completed_amount"] - financial_data.get(
            "completed_amount", 0.0
        )
        if abs(completed_amount_diff) > 0.01:
            await db_operations.update_financial_data("completed_amount", completed_amount_diff)
            fixed_items.append(f"全局完成订单金额: {completed_amount_diff:+,.2f}")

        completed_count_diff = income_summary["completed_count"] - financial_data.get(
            "completed_orders", 0
        )
        if abs(completed_count_diff) > 0:
            await db_operations.update_financial_data(
                "completed_orders", float(completed_count_diff)
            )
            fixed_items.append(f"全局完成订单数: {completed_count_diff:+d}")

        breach_end_amount_diff = income_summary["breach_end_amount"] - financial_data.get(
            "breach_end_amount", 0.0
        )
        if abs(breach_end_amount_diff) > 0.01:
            await db_operations.update_financial_data("breach_end_amount", breach_end_amount_diff)
            fixed_items.append(f"全局违约完成金额: {breach_end_amount_diff:+,.2f}")

        breach_end_count_diff = income_summary["breach_end_count"] - financial_data.get(
            "breach_end_orders", 0
        )
        if abs(breach_end_count_diff) > 0:
            await db_operations.update_financial_data(
                "breach_end_orders", float(breach_end_count_diff)
            )
            fixed_items.append(f"全局违约完成订单数: {breach_end_count_diff:+d}")

        # 修复日结统计数据（daily_data表）
        # 这里需要重新计算所有日期的统计数据
        # 由于daily_data表是按日期和归属ID存储的，我们需要遍历所有日期和归属ID
        daily_fixed_count = 0
        for date, groups in daily_income.items():
            for group_id, income_data in groups.items():
                # 获取当前日结数据
                current_daily = await db_operations.get_stats_by_date_range(date, date, group_id)

                # 修复利息收入
                if "interest" in income_data:
                    interest_diff = income_data["interest"] - current_daily.get("interest", 0.0)
                    if abs(interest_diff) > 0.01:
                        await db_operations.update_daily_data(
                            date, "interest", interest_diff, group_id
                        )
                        daily_fixed_count += 1

                # 修复完成订单
                if "completed_amount" in income_data:
                    completed_amount_diff = income_data["completed_amount"] - current_daily.get(
                        "completed_amount", 0.0
                    )
                    if abs(completed_amount_diff) > 0.01:
                        await db_operations.update_daily_data(
                            date, "completed_amount", completed_amount_diff, group_id
                        )
                        daily_fixed_count += 1

                if "completed_count" in income_data:
                    completed_count_diff = income_data["completed_count"] - current_daily.get(
                        "completed_orders", 0
                    )
                    if abs(completed_count_diff) > 0:
                        await db_operations.update_daily_data(
                            date, "completed_orders", float(completed_count_diff), group_id
                        )
                        daily_fixed_count += 1

                # 修复违约完成
                if "breach_end_amount" in income_data:
                    breach_end_amount_diff = income_data["breach_end_amount"] - current_daily.get(
                        "breach_end_amount", 0.0
                    )
                    if abs(breach_end_amount_diff) > 0.01:
                        await db_operations.update_daily_data(
                            date, "breach_end_amount", breach_end_amount_diff, group_id
                        )
                        daily_fixed_count += 1

                if "breach_end_count" in income_data:
                    breach_end_count_diff = income_data["breach_end_count"] - current_daily.get(
                        "breach_end_orders", 0
                    )
                    if abs(breach_end_count_diff) > 0:
                        await db_operations.update_daily_data(
                            date, "breach_end_orders", float(breach_end_count_diff), group_id
                        )
                        daily_fixed_count += 1

        # 构建结果消息
        if fixed_items or daily_fixed_count > 0:
            result_msg = "✅ 收入统计数据修复完成！\n\n"
            if fixed_items:
                result_msg += "修复的全局统计:\n"
                for item in fixed_items:
                    result_msg += f"  • {item}\n"
            if daily_fixed_count > 0:
                result_msg += f"\n修复的日结统计: {daily_fixed_count} 条记录\n"
            result_msg += f"\n📊 修复后的汇总:\n"
            result_msg += f"  利息收入: {income_summary['interest']:.2f}\n"
            result_msg += f"  完成订单: {income_summary['completed_count']} 笔, {income_summary['completed_amount']:.2f}\n"
            result_msg += f"  违约完成: {income_summary['breach_end_count']} 笔, {income_summary['breach_end_amount']:.2f}\n"
        else:
            result_msg = "✅ 收入统计数据一致，无需修复。"

        await msg.edit_text(result_msg)

    except Exception as e:
        logger.error(f"修复收入统计数据时出错: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 修复失败: {str(e)}")


@admin_required
@private_chat_only
async def find_tail_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查找导致有效金额尾数的订单（管理员命令）"""
    try:
        msg = await update.message.reply_text("🔍 正在分析有效金额尾数...")

        # 获取所有有效订单（包含所有状态，用于完整分析）
        all_valid_orders = await db_operations.search_orders_advanced({})
        await db_operations.search_orders_advanced_all_states({})

        # 计算实际有效金额（从订单表）
        actual_valid_amount = sum(order.get("amount", 0) for order in all_valid_orders)

        # 获取统计表中的有效金额
        financial_data = await db_operations.get_financial_data()
        stats_valid_amount = financial_data["valid_amount"]

        # 查找所有非整千数订单
        non_thousand_orders = []
        tail_6_orders = []
        tail_distribution = {}  # 尾数分布统计

        for order in all_valid_orders:
            amount = order.get("amount", 0)
            if amount % 1000 != 0:
                tail = int(amount % 1000)
                non_thousand_orders.append((order, tail))
                if tail not in tail_distribution:
                    tail_distribution[tail] = []
                tail_distribution[tail].append(order)
                if tail == 6:
                    tail_6_orders.append(order)

        # 按归属ID分组分析
        group_analysis = {}
        all_group_ids = list(
            set(order.get("group_id") for order in all_valid_orders if order.get("group_id"))
        )

        for group_id in sorted(all_group_ids):
            group_orders = [o for o in all_valid_orders if o.get("group_id") == group_id]
            group_amount = sum(o.get("amount", 0) for o in group_orders)
            group_tail = int(group_amount % 1000)
            group_non_thousand = [o for o in group_orders if o.get("amount", 0) % 1000 != 0]

            grouped_data = await db_operations.get_grouped_data(group_id)
            stats_group_amount = grouped_data.get("valid_amount", 0)
            stats_group_tail = int(stats_group_amount % 1000)

            group_analysis[group_id] = {
                "orders": group_orders,
                "actual_amount": group_amount,
                "actual_tail": group_tail,
                "stats_amount": stats_group_amount,
                "stats_tail": stats_group_tail,
                "non_thousand": group_non_thousand,
            }

        # 构建结果消息
        result_msg = "🔍 有效金额尾数分析报告\n\n"
        result_msg += "📊 总体统计：\n"
        result_msg += f"有效订单数: {len(all_valid_orders)}\n"
        result_msg += f"实际有效金额: {actual_valid_amount:,.2f}\n"
        result_msg += f"统计有效金额: {stats_valid_amount:,.2f}\n"
        result_msg += f"差异: {stats_valid_amount - actual_valid_amount:,.2f}\n\n"

        # 分析总金额尾数
        actual_tail = int(actual_valid_amount % 1000)
        stats_tail = int(stats_valid_amount % 1000)

        if actual_tail == 6:
            result_msg += "⚠️ 实际有效金额尾数是 6\n"
        elif stats_tail == 6:
            result_msg += f"⚠️ 统计有效金额尾数是 6（但实际尾数是 {actual_tail}）\n"
            result_msg += "   说明统计数据不一致，建议运行 /fix_statistics\n\n"
        else:
            result_msg += f"✅ 总金额尾数: 实际={actual_tail}, 统计={stats_tail}\n\n"

        # 显示尾数为6的订单
        if tail_6_orders:
            result_msg += f"⚠️ 发现 {len(tail_6_orders)} 个尾数为 6 的订单：\n\n"
            for order in tail_6_orders:
                result_msg += (
                    f"订单ID: {order.get('order_id')}\n"
                    f"金额: {order.get('amount'):,.2f}\n"
                    f"状态: {order.get('state')}\n"
                    f"归属: {order.get('group_id')}\n"
                    f"日期: {order.get('date')}\n"
                    f"客户: {order.get('customer', 'N/A')}\n\n"
                )
        else:
            result_msg += "✅ 没有找到尾数为 6 的订单\n\n"

        # 按归属ID分组显示
        result_msg += "📋 按归属ID分组分析：\n\n"
        for group_id in sorted(all_group_ids):
            analysis = group_analysis[group_id]
            result_msg += f"{group_id}:\n"
            result_msg += (
                f"  实际金额: {analysis['actual_amount']:,.2f} (尾数: {analysis['actual_tail']})\n"
            )
            result_msg += (
                f"  统计金额: {analysis['stats_amount']:,.2f} (尾数: {analysis['stats_tail']})\n"
            )

            if analysis["actual_tail"] == 6 or analysis["stats_tail"] == 6:
                result_msg += "  ⚠️ 该归属ID导致尾数6！\n"

            if analysis["non_thousand"]:
                result_msg += f"  非整千数订单: {len(analysis['non_thousand'])} 个\n"
                for order in analysis["non_thousand"][:3]:
                    amount = order.get("amount", 0)
                    tail = int(amount % 1000)
                    result_msg += f"    - {order.get('order_id')}: {amount:,.2f} (尾数: {tail})\n"
                if len(analysis["non_thousand"]) > 3:
                    result_msg += f"    ... 还有 {len(analysis['non_thousand']) - 3} 个\n"
            result_msg += "\n"

        # 尾数分布统计
        if tail_distribution:
            result_msg += f"📊 尾数分布统计：\n"
            for tail in sorted(tail_distribution.keys()):
                count = len(tail_distribution[tail])
                total = sum(o.get("amount", 0) for o in tail_distribution[tail])
                result_msg += f"  尾数 {tail}: {count} 个订单, 总金额: {total:,.2f}\n"
            result_msg += "\n"

        # 可能的原因分析
        if stats_tail == 6 and actual_tail != 6:
            result_msg += "💡 原因分析：\n"
            result_msg += "统计金额尾数为6，但实际订单金额尾数不是6\n"
            result_msg += "说明统计数据与实际订单数据不一致\n"
            result_msg += "建议：运行 /fix_statistics 修复统计数据\n"
        elif actual_tail == 6:
            result_msg += "💡 原因分析：\n"
            if tail_6_orders:
                result_msg += f"找到 {len(tail_6_orders)} 个订单金额尾数为6\n"
                result_msg += "可能原因：\n"
                result_msg += "1. 订单创建时输入了非整千数金额\n"
                result_msg += "2. 执行了本金减少操作（+<金额>b），减少的金额不是整千数\n"
                result_msg += "3. 例如：订单原金额10000，执行+9994b后，剩余金额为6\n"
            else:
                result_msg += "未找到尾数为6的订单，但总金额尾数是6\n"
                result_msg += "可能是多个订单的尾数累加导致的\n"

        # 如果消息太长，分段发送
        if len(result_msg) > 4000:
            # 发送第一部分
            await msg.edit_text(result_msg[:4000])
            # 发送剩余部分
            remaining = result_msg[4000:]
            while len(remaining) > 4000:
                await update.message.reply_text(remaining[:4000])
                remaining = remaining[4000:]
            if remaining:
                await update.message.reply_text(remaining)
        else:
            await msg.edit_text(result_msg)

    except Exception as e:
        logger.error(f"查找尾数订单时出错: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 查找失败: {str(e)}")


@admin_required
@private_chat_only
async def list_employees(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """列出所有员工"""
    users = await db_operations.get_authorized_users()
    if not users:
        await update.message.reply_text("📋 暂无授权员工")
        return

    message = "📋 授权员工列表:\n\n"
    for uid in users:
        message += f"👤 `{uid}`\n"

    await update.message.reply_text(message, parse_mode="Markdown")


@admin_required
@private_chat_only
async def set_user_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """设置用户有权限查看的归属ID（管理员命令）"""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("❌ 用法: /set_user_group_id <用户ID> <归属ID>")
        return

    try:
        user_id = int(context.args[0])
        group_id = context.args[1].upper()

        # 验证归属ID是否存在
        grouped_data = await db_operations.get_grouped_data(group_id)
        if not grouped_data:
            await update.message.reply_text(f"❌ 归属ID {group_id} 不存在")
            return

        if await db_operations.set_user_group_id(user_id, group_id):
            await update.message.reply_text(f"✅ 已设置用户 {user_id} 的归属ID权限为 {group_id}")
        else:
            await update.message.reply_text("❌ 设置失败")
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")


@admin_required
@private_chat_only
async def remove_user_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """移除用户的归属ID权限（管理员命令）"""
    if not context.args:
        await update.message.reply_text("❌ 用法: /remove_user_group_id <用户ID>")
        return

    try:
        user_id = int(context.args[0])
        if await db_operations.remove_user_group_id(user_id):
            await update.message.reply_text(f"✅ 已移除用户 {user_id} 的归属ID权限")
        else:
            await update.message.reply_text("⚠️ 移除失败或用户不存在")
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")


@admin_required
@private_chat_only
async def list_user_group_mappings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """列出所有用户归属ID映射（管理员命令）"""
    mappings = await db_operations.get_all_user_group_mappings()
    if not mappings:
        await update.message.reply_text("📋 暂无用户归属ID映射")
        return

    message = "📋 用户归属ID映射列表:\n\n"
    for mapping in mappings:
        message += f"👤 用户ID: `{mapping['user_id']}` → 归属ID: `{mapping['group_id']}`\n"

    await update.message.reply_text(message, parse_mode="Markdown")


@admin_required
@private_chat_only
@error_handler
async def check_mismatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """检查收入明细和统计数据的不一致问题（管理员命令）"""

    import db_operations

    # 获取日期参数（可选），支持日期范围
    start_date = None
    end_date = None
    if context.args and len(context.args) > 0:
        if len(context.args) == 1:
            # 单个日期
            start_date = context.args[0]
            end_date = context.args[0]
        elif len(context.args) >= 2:
            # 日期范围
            start_date = context.args[0]
            end_date = context.args[1]
    else:
        # 默认检查所有历史数据
        start_date = "1970-01-01"
        end_date = "2099-12-31"

    # 发送开始消息
    msg = await update.message.reply_text("🔍 正在检查数据不一致问题，请稍候...")

    try:
        # 获取所有收入明细统计（从最早日期到现在）
        income_records = await db_operations.get_income_records(start_date, end_date)

        # 计算收入明细汇总
        income_summary = {
            "interest": 0.0,
            "completed_amount": 0.0,
            "breach_end_amount": 0.0,
            "principal_reduction": 0.0,
            "adjustment": 0.0,
        }

        for record in income_records:
            record_type = record.get("type", "")
            amount = record.get("amount", 0.0) or 0.0
            if record_type == "interest":
                income_summary["interest"] += amount
            elif record_type == "completed":
                income_summary["completed_amount"] += amount
            elif record_type == "breach_end":
                income_summary["breach_end_amount"] += amount
            elif record_type == "principal_reduction":
                income_summary["principal_reduction"] += amount
            elif record_type == "adjustment":
                income_summary["adjustment"] += amount

        # 获取统计数据（从daily_data表汇总）
        stats = await db_operations.get_stats_by_date_range(start_date, end_date, None)

        # 获取全局统计数据（从financial_data表）
        financial_data = await db_operations.get_financial_data()

        # 比较数据
        output_lines = []
        output_lines.append(f"📊 数据一致性检查报告")
        if start_date == end_date:
            output_lines.append(f"📅 检查日期: {start_date}")
        else:
            output_lines.append(f"📅 检查日期范围: {start_date} 至 {end_date}")
        output_lines.append("=" * 50)
        output_lines.append("")

        output_lines.append("📈 收入明细汇总（从income_records表）:")
        output_lines.append(f"  利息收入: {income_summary['interest']:.2f}")
        output_lines.append(f"  完成订单金额: {income_summary['completed_amount']:.2f}")
        output_lines.append(f"  违约完成金额: {income_summary['breach_end_amount']:.2f}")
        output_lines.append(f"  本金减少: {income_summary['principal_reduction']:.2f}")
        output_lines.append("")

        output_lines.append("📊 统计数据汇总（从daily_data表）:")
        output_lines.append(f"  利息收入: {stats.get('interest', 0.0):.2f}")
        output_lines.append(f"  完成订单金额: {stats.get('completed_amount', 0.0):.2f}")
        output_lines.append(f"  违约完成金额: {stats.get('breach_end_amount', 0.0):.2f}")
        output_lines.append("")

        output_lines.append("💰 全局统计数据（从financial_data表）:")
        output_lines.append(f"  利息收入: {financial_data.get('interest', 0.0):.2f}")
        output_lines.append(f"  完成订单金额: {financial_data.get('completed_amount', 0.0):.2f}")
        output_lines.append(f"  违约完成金额: {financial_data.get('breach_end_amount', 0.0):.2f}")
        output_lines.append("")
        output_lines.append("=" * 50)
        output_lines.append("")

        mismatches = []

        # 检查利息收入（比较daily_data和income_records）
        interest_diff = abs(stats.get("interest", 0.0) - income_summary["interest"])
        if interest_diff > 0.01:  # 允许0.01的浮点误差
            mismatches.append("利息收入")
            output_lines.append(f"⚠️ 不一致! 利息收入:")
            output_lines.append(f"  统计表(daily_data): {stats.get('interest', 0.0):.2f}")
            output_lines.append(f"  明细表(income_records): {income_summary['interest']:.2f}")
            output_lines.append(f"  差异: {interest_diff:.2f}")
            output_lines.append("")

        # 检查完成订单金额
        completed_diff = abs(
            stats.get("completed_amount", 0.0) - income_summary["completed_amount"]
        )
        if completed_diff > 0.01:
            mismatches.append("完成订单金额")
            output_lines.append(f"⚠️ 不一致! 完成订单金额:")
            output_lines.append(f"  统计表(daily_data): {stats.get('completed_amount', 0.0):.2f}")
            output_lines.append(
                f"  明细表(income_records): {income_summary['completed_amount']:.2f}"
            )
            output_lines.append(f"  差异: {completed_diff:.2f}")
            output_lines.append("")

        # 检查违约完成金额
        breach_end_diff = abs(
            stats.get("breach_end_amount", 0.0) - income_summary["breach_end_amount"]
        )
        if breach_end_diff > 0.01:
            mismatches.append("违约完成金额")
            output_lines.append(f"⚠️ 不一致! 违约完成金额:")
            output_lines.append(f"  统计表(daily_data): {stats.get('breach_end_amount', 0.0):.2f}")
            output_lines.append(
                f"  明细表(income_records): {income_summary['breach_end_amount']:.2f}"
            )
            output_lines.append(f"  差异: {breach_end_diff:.2f}")
            output_lines.append("")

        # 检查全局统计数据与收入明细的一致性
        global_interest_diff = abs(financial_data.get("interest", 0.0) - income_summary["interest"])
        if global_interest_diff > 0.01:
            mismatches.append("全局利息收入")
            output_lines.append(f"⚠️ 不一致! 全局利息收入:")
            output_lines.append(
                f"  全局统计(financial_data): {financial_data.get('interest', 0.0):.2f}"
            )
            output_lines.append(f"  明细表(income_records): {income_summary['interest']:.2f}")
            output_lines.append(f"  差异: {global_interest_diff:.2f}")
            output_lines.append("")

        global_completed_diff = abs(
            financial_data.get("completed_amount", 0.0) - income_summary["completed_amount"]
        )
        if global_completed_diff > 0.01:
            mismatches.append("全局完成订单金额")
            output_lines.append(f"⚠️ 不一致! 全局完成订单金额:")
            output_lines.append(
                f"  全局统计(financial_data): {financial_data.get('completed_amount', 0.0):.2f}"
            )
            output_lines.append(
                f"  明细表(income_records): {income_summary['completed_amount']:.2f}"
            )
            output_lines.append(f"  差异: {global_completed_diff:.2f}")
            output_lines.append("")

        global_breach_end_diff = abs(
            financial_data.get("breach_end_amount", 0.0) - income_summary["breach_end_amount"]
        )
        if global_breach_end_diff > 0.01:
            mismatches.append("全局违约完成金额")
            output_lines.append(f"⚠️ 不一致! 全局违约完成金额:")
            output_lines.append(
                f"  全局统计(financial_data): {financial_data.get('breach_end_amount', 0.0):.2f}"
            )
            output_lines.append(
                f"  明细表(income_records): {income_summary['breach_end_amount']:.2f}"
            )
            output_lines.append(f"  差异: {global_breach_end_diff:.2f}")
            output_lines.append("")

        if not mismatches:
            output_lines.append("✅ 数据一致！所有统计数据与收入明细匹配。")
        else:
            output_lines.append("")
            output_lines.append(f"❌ 发现 {len(mismatches)} 项不一致:")
            for item in mismatches:
                output_lines.append(f"  - {item}")
            output_lines.append("")
            output_lines.append("💡 修复建议:")
            output_lines.append("  1. 检查收入明细是否正确记录")
            output_lines.append("  2. 使用 /fix_statistics 修复统计数据")
            output_lines.append("  3. 如果问题持续，请检查日志文件")

        output_lines.append("")
        output_lines.append("💡 提示：要查看统计收入的来源明细，请使用：")
        output_lines.append("  /report → 点击「💰 收入明细」按钮")

        output = "\n".join(output_lines)

        # 处理输出（Telegram消息有长度限制4096字符）
        if len(output) > 4096:
            # 分段发送
            chunks = []
            current_chunk = ""
            for line in output.split("\n"):
                if len(current_chunk) + len(line) + 1 > 4000:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = line + "\n"
                else:
                    current_chunk += line + "\n"
            if current_chunk:
                chunks.append(current_chunk)

            # 发送第一段
            if chunks:
                await msg.edit_text(f"```\n{chunks[0]}\n```", parse_mode="Markdown")

                # 发送剩余段
                for i, chunk in enumerate(chunks[1:], 1):
                    await update.message.reply_text(
                        f"```\n[第 {i+1} 段]\n{chunk}\n```", parse_mode="Markdown"
                    )

        else:
            # 输出不太长，直接发送
            if output:
                await msg.edit_text(f"```\n{output}\n```", parse_mode="Markdown")
            else:
                await msg.edit_text("❌ 检查完成，但没有数据")

    except Exception as e:
        logger.error(f"检查数据不一致时出错: {e}", exc_info=True)
        await msg.edit_text(f"❌ 检查失败: {str(e)}")


@admin_required
@private_chat_only
@error_handler
async def diagnose_data_inconsistency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """诊断数据不一致的详细原因（管理员命令）

    分析 income_records 与 financial_data/grouped_data 不一致的具体原因：
    1. 检查 income_records 表的完整情况（包括已撤销记录）
    2. 检查数据的时间范围
    3. 分析差异的具体来源
    4. 提供修复建议
    """
    import db_operations

    msg = await update.message.reply_text("🔍 正在诊断数据不一致原因，请稍候...")

    try:
        output_lines = []
        output_lines.append("🔬 数据不一致诊断报告")
        output_lines.append("=" * 60)
        output_lines.append("")

        # 1. 检查 income_records 表的完整情况
        output_lines.append("📋 【income_records 表分析】")
        output_lines.append("")

        # 获取所有记录（包括已撤销的）
        all_records = await db_operations.get_income_records(
            "1970-01-01", "2099-12-31", include_undone=True
        )

        # 获取未撤销的记录
        valid_records = await db_operations.get_income_records(
            "1970-01-01", "2099-12-31", include_undone=False
        )

        # 统计已撤销的记录
        undone_records = [r for r in all_records if r.get("is_undone", 0) == 1]

        output_lines.append(f"总记录数: {len(all_records)}")
        output_lines.append(f"有效记录数: {len(valid_records)}")
        output_lines.append(f"已撤销记录数: {len(undone_records)}")
        output_lines.append("")

        # 按类型统计（包括已撤销的）
        all_by_type = {
            "interest": 0.0,
            "completed": 0.0,
            "breach_end": 0.0,
            "principal_reduction": 0.0,
            "adjustment": 0.0,
        }

        valid_by_type = {
            "interest": 0.0,
            "completed": 0.0,
            "breach_end": 0.0,
            "principal_reduction": 0.0,
            "adjustment": 0.0,
        }

        undone_by_type = {
            "interest": 0.0,
            "completed": 0.0,
            "breach_end": 0.0,
            "principal_reduction": 0.0,
            "adjustment": 0.0,
        }

        for record in all_records:
            record_type = record.get("type", "")
            amount = record.get("amount", 0.0) or 0.0
            is_undone = record.get("is_undone", 0) == 1

            if record_type in all_by_type:
                all_by_type[record_type] += amount
                if not is_undone:
                    valid_by_type[record_type] += amount
                else:
                    undone_by_type[record_type] += amount

        output_lines.append("📊 按类型统计（所有记录，包括已撤销）:")
        output_lines.append(f"  利息收入: {all_by_type['interest']:.2f}")
        output_lines.append(f"  完成订单: {all_by_type['completed']:.2f}")
        output_lines.append(f"  违约完成: {all_by_type['breach_end']:.2f}")
        output_lines.append("")

        output_lines.append("✅ 按类型统计（仅有效记录，排除已撤销）:")
        output_lines.append(f"  利息收入: {valid_by_type['interest']:.2f}")
        output_lines.append(f"  完成订单: {valid_by_type['completed']:.2f}")
        output_lines.append(f"  违约完成: {valid_by_type['breach_end']:.2f}")
        output_lines.append("")

        if len(undone_records) > 0:
            output_lines.append("❌ 已撤销记录统计:")
            output_lines.append(f"  利息收入: {undone_by_type['interest']:.2f}")
            output_lines.append(f"  完成订单: {undone_by_type['completed']:.2f}")
            output_lines.append(f"  违约完成: {undone_by_type['breach_end']:.2f}")
            output_lines.append("")

        # 2. 检查数据的时间范围
        if all_records:
            dates = [r.get("date", "") for r in all_records if r.get("date")]
            if dates:
                min_date = min(dates)
                max_date = max(dates)
                output_lines.append("📅 数据时间范围:")
                output_lines.append(f"  最早记录: {min_date}")
                output_lines.append(f"  最新记录: {max_date}")
                output_lines.append("")

        # 3. 获取 financial_data 和 grouped_data 的数据
        financial_data = await db_operations.get_financial_data()
        await db_operations.get_all_group_ids()

        output_lines.append("💰 【统计数据对比】")
        output_lines.append("")

        # 对比 financial_data
        output_lines.append("🌐 全局统计数据 (financial_data):")
        output_lines.append(f"  利息收入: {financial_data.get('interest', 0.0):.2f}")
        output_lines.append(f"  完成订单: {financial_data.get('completed_amount', 0.0):.2f}")
        output_lines.append(f"  违约完成: {financial_data.get('breach_end_amount', 0.0):.2f}")
        output_lines.append("")

        output_lines.append("📈 收入明细汇总 (income_records - 仅有效记录):")
        output_lines.append(f"  利息收入: {valid_by_type['interest']:.2f}")
        output_lines.append(f"  完成订单: {valid_by_type['completed']:.2f}")
        output_lines.append(f"  违约完成: {valid_by_type['breach_end']:.2f}")
        output_lines.append("")

        # 计算差异
        interest_diff = financial_data.get("interest", 0.0) - valid_by_type["interest"]
        completed_diff = financial_data.get("completed_amount", 0.0) - valid_by_type["completed"]
        breach_end_diff = financial_data.get("breach_end_amount", 0.0) - valid_by_type["breach_end"]

        output_lines.append("🔍 差异分析:")
        output_lines.append(f"  利息收入差异: {interest_diff:+,.2f}")
        output_lines.append(f"  完成订单差异: {completed_diff:+,.2f}")
        output_lines.append(f"  违约完成差异: {breach_end_diff:+,.2f}")
        output_lines.append("")

        # 4. 分析可能的原因
        output_lines.append("💡 【可能的原因分析】")
        output_lines.append("")

        reasons = []

        if interest_diff > 1000 or completed_diff > 1000 or breach_end_diff > 1000:
            reasons.append("1. 历史数据导入时，只更新了统计表，没有创建 income_records 记录")

        if len(undone_records) > 0:
            reasons.append(f"2. 存在 {len(undone_records)} 条已撤销的记录，但统计数据可能未回滚")

        if all_records and dates:
            # 检查是否有大量历史数据缺失
            if len(all_records) < 100:  # 假设应该有更多记录
                reasons.append("3. income_records 表可能被清理过，只保留了部分记录")

        if interest_diff > 0 or completed_diff > 0 or breach_end_diff > 0:
            reasons.append("4. financial_data 包含历史累计数据，而 income_records 可能不完整")

        if reasons:
            for reason in reasons:
                output_lines.append(f"  {reason}")
        else:
            output_lines.append("  未发现明显原因，建议检查数据导入历史")

        output_lines.append("")

        # 5. 修复建议
        output_lines.append("🔧 【修复建议】")
        output_lines.append("")
        output_lines.append("1. 如果差异是历史数据导致的（正常情况）:")
        output_lines.append("   - 使用 /fix_income_statistics 命令修复统计数据")
        output_lines.append("   - 该命令会根据 income_records 重新计算统计")
        output_lines.append("")
        output_lines.append("2. 如果 income_records 数据不完整:")
        output_lines.append("   - 检查是否有历史数据备份")
        output_lines.append("   - 考虑从统计表反向生成 income_records（需谨慎）")
        output_lines.append("")
        output_lines.append("3. 如果存在已撤销记录但统计未回滚:")
        output_lines.append("   - 检查撤销操作的日志")
        output_lines.append("   - 手动修复统计数据")
        output_lines.append("")

        # 发送报告
        report = "\n".join(output_lines)
        await msg.edit_text(report)

    except Exception as e:
        logger.error(f"诊断数据不一致时出错: {e}", exc_info=True)
        await msg.edit_text(f"❌ 诊断失败: {str(e)}")


@admin_required
@private_chat_only
@error_handler
async def customer_contribution(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查询客户总贡献（跨所有订单周期）（管理员命令）"""
    if not context.args or len(context.args) == 0:
        await update.message.reply_text(
            "❌ 请指定客户类型\n\n"
            "用法: /customer <客户类型> [起始日期] [结束日期]\n\n"
            "客户类型: A (新客户) 或 B (老客户)\n"
            "日期格式: YYYY-MM-DD (可选，默认查询全部)\n\n"
            "示例:\n"
            "/customer A\n"
            "/customer B 2025-01-01 2025-12-31"
        )
        return

    customer = context.args[0].upper()
    if customer not in ["A", "B"]:
        await update.message.reply_text("❌ 客户类型必须是 A (新客户) 或 B (老客户)")
        return

    start_date = context.args[1] if len(context.args) > 1 else None
    end_date = context.args[2] if len(context.args) > 2 else None

    try:
        msg = await update.message.reply_text("🔍 正在查询客户总贡献，请稍候...")

        # 查询总贡献
        total_contribution = await db_operations.get_customer_total_contribution(
            customer, start_date, end_date
        )

        # 查询所有订单详情
        orders_summary = await db_operations.get_customer_orders_summary(
            customer, start_date, end_date
        )

        # 构建报告
        customer_name = "新客户" if customer == "A" else "老客户"
        date_range = ""
        if start_date or end_date:
            date_range = f"\n📅 查询日期范围: {start_date or '最早'} 至 {end_date or '最新'}"

        report = (
            f"📊 {customer_name} (客户类型: {customer}) 总贡献报告{date_range}\n"
            f"{'=' * 60}\n\n"
            f"💰 总贡献汇总:\n"
            f"  总贡献金额: {total_contribution['total_amount']:,.2f}\n"
            f"  其中:\n"
            f"    - 利息收入: {total_contribution['total_interest']:,.2f} ({total_contribution['interest_count']} 次)\n"
            f"    - 完成订单: {total_contribution['total_completed']:,.2f}\n"
            f"    - 违约完成: {total_contribution['total_breach_end']:,.2f}\n"
            f"    - 本金减少: {total_contribution['total_principal_reduction']:,.2f}\n\n"
            f"📋 订单统计:\n"
            f"  订单数量: {total_contribution['order_count']} 个\n"
        )

        if total_contribution["first_order_date"]:
            report += (
                f"  首次订单: {total_contribution['first_order_date']}\n"
                f"  最后订单: {total_contribution['last_order_date']}\n"
            )

        # 显示订单明细（前10个）
        if orders_summary:
            report += f"\n📝 订单明细 (显示前 {min(10, len(orders_summary))} 个):\n"
            report += f"{'-' * 60}\n"

            for i, order_info in enumerate(orders_summary[:10], 1):
                order = order_info["order"]
                report += (
                    f"\n{i}. 订单: {order['order_id']}\n"
                    f"   日期: {order['date']}\n"
                    f"   状态: {order['state']}\n"
                    f"   金额: {order['amount']:,.2f}\n"
                    f"   贡献: {order_info['total_contribution']:,.2f}\n"
                    f"      - 利息: {order_info['interest']:,.2f}\n"
                    f"      - 完成: {order_info['completed']:,.2f}\n"
                    f"      - 违约完成: {order_info['breach_end']:,.2f}\n"
                )

            if len(orders_summary) > 10:
                report += f"\n... 还有 {len(orders_summary) - 10} 个订单\n"

        await msg.edit_text(report)

    except Exception as e:
        logger.error(f"查询客户总贡献时出错: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 查询失败: {str(e)}")


@authorized_required
@private_chat_only
@error_handler
async def preview_incremental_report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """预览增量报表（员工权限）"""
    try:
        # 获取基准日期
        baseline_date = await get_or_create_baseline_date()

        # 生成预览
        preview_text = await preview_incremental_report(baseline_date)

        await update.message.reply_text(preview_text)
    except Exception as e:
        logger.error(f"预览增量报表失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 预览失败: {str(e)}")


@admin_required
@private_chat_only
@error_handler
async def merge_incremental_report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """合并增量报表到全局数据"""
    try:
        # 获取基准日期
        baseline_date = await get_or_create_baseline_date()

        # 准备增量数据
        incremental_data = await prepare_incremental_data(baseline_date)
        orders_data = incremental_data.get("orders", [])
        expense_records = incremental_data.get("expenses", [])

        if not orders_data and not expense_records:
            await update.message.reply_text("✅ 无增量数据需要合并")
            return

        # 合并到全局数据
        result = await merge_incremental_report_to_global(orders_data, expense_records)

        if result["success"]:
            stats = result["stats"]
            message = f"✅ 增量报表已合并到全局数据\n\n"
            message += (
                f"📦 订单: {stats['new_orders_count']}个, {stats['new_orders_amount']:,.2f}\n"
            )
            message += f"💰 利息: {stats['interest']:,.2f}\n"
            message += f"💸 开销: {stats['company_expenses'] + stats['other_expenses']:,.2f}\n"
            await update.message.reply_text(message)
        else:
            await update.message.reply_text(f"❌ {result['message']}")
    except Exception as e:
        logger.error(f"合并增量报表失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 合并失败: {str(e)}")
