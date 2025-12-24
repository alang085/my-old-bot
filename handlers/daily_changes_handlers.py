"""每日数据变更表处理器"""

# 标准库
import logging
from datetime import datetime

import pytz

# 第三方库
from telegram import Update
from telegram.ext import ContextTypes

# 本地模块
import db_operations
from decorators import authorized_required, error_handler, private_chat_only

logger = logging.getLogger(__name__)

BEIJING_TZ = pytz.timezone("Asia/Shanghai")


@authorized_required
@private_chat_only
@error_handler
async def show_daily_changes_table(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示每日数据变更表（员工权限）"""
    try:
        # 解析日期参数（如果有）
        date_str = None
        if context.args and len(context.args) > 0:
            date_str = context.args[0]
        else:
            # 默认使用当前日期
            date_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

        # 验证日期格式
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            await update.message.reply_text(
                "❌ 日期格式错误，请使用 YYYY-MM-DD 格式\n例如: /daily_changes 2025-12-16"
            )
            return

        # 获取每日数据变更
        changes = await get_daily_changes(date_str)

        # 生成表格文本
        table_text = generate_changes_table(date_str, changes)

        await update.message.reply_text(table_text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"显示每日数据变更表失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 查询失败: {str(e)}")


async def get_daily_changes(date: str) -> dict:
    """获取指定日期的数据变更"""
    try:
        # 获取新增订单
        new_orders = await db_operations.get_new_orders_by_date(date)

        # 获取完成的订单
        completed_orders = await db_operations.get_completed_orders_by_date(date)

        # 获取违约完成的订单
        breach_end_orders = await db_operations.get_breach_end_orders_by_date(date)

        # 获取当日利息收入
        interest_records = await db_operations.get_income_records(date, date, type="interest")

        # 获取当日本金归还
        principal_records = await db_operations.get_income_records(
            date, date, type="principal_reduction"
        )

        # 获取当日开销
        expense_records = await db_operations.get_expense_records(date, date)

        # 计算汇总
        new_orders_count = len(new_orders)
        new_orders_amount = sum(float(order.get("amount", 0) or 0) for order in new_orders)

        completed_orders_count = len(completed_orders)
        completed_orders_amount = sum(
            float(order.get("amount", 0) or 0) for order in completed_orders
        )

        breach_end_orders_count = len(breach_end_orders)
        breach_end_orders_amount = sum(
            float(order.get("amount", 0) or 0) for order in breach_end_orders
        )

        total_interest = sum(float(record.get("amount", 0) or 0) for record in interest_records)
        total_principal = sum(float(record.get("amount", 0) or 0) for record in principal_records)

        company_expenses = sum(
            float(record.get("amount", 0) or 0)
            for record in expense_records
            if record.get("type") == "company"
        )
        other_expenses = sum(
            float(record.get("amount", 0) or 0)
            for record in expense_records
            if record.get("type") == "other"
        )

        return {
            "date": date,
            "new_orders": new_orders,
            "new_orders_count": new_orders_count,
            "new_orders_amount": new_orders_amount,
            "completed_orders": completed_orders,
            "completed_orders_count": completed_orders_count,
            "completed_orders_amount": completed_orders_amount,
            "breach_end_orders": breach_end_orders,
            "breach_end_orders_count": breach_end_orders_count,
            "breach_end_orders_amount": breach_end_orders_amount,
            "interest_records": interest_records,
            "total_interest": total_interest,
            "principal_records": principal_records,
            "total_principal": total_principal,
            "expense_records": expense_records,
            "company_expenses": company_expenses,
            "other_expenses": other_expenses,
            "total_expenses": company_expenses + other_expenses,
        }
    except Exception as e:
        logger.error(f"获取每日数据变更失败: {e}", exc_info=True)
        return {
            "date": date,
            "new_orders": [],
            "new_orders_count": 0,
            "new_orders_amount": 0.0,
            "completed_orders": [],
            "completed_orders_count": 0,
            "completed_orders_amount": 0.0,
            "breach_end_orders": [],
            "breach_end_orders_count": 0,
            "breach_end_orders_amount": 0.0,
            "interest_records": [],
            "total_interest": 0.0,
            "principal_records": [],
            "total_principal": 0.0,
            "expense_records": [],
            "company_expenses": 0.0,
            "other_expenses": 0.0,
            "total_expenses": 0.0,
        }


def generate_changes_table(date: str, changes: dict) -> str:
    """生成每日数据变更表文本"""
    text = "📊 <b>每日数据变更表</b>\n"
    text += f"日期: {date}\n"
    text += "═" * 40 + "\n\n"

    # 订单变更汇总
    text += "<b>📦 订单变更汇总</b>\n"
    text += f"新增订单: {changes['new_orders_count']} 个, {changes['new_orders_amount']:,.2f}\n"
    text += f"完成订单: {changes['completed_orders_count']} 个, {changes['completed_orders_amount']:,.2f}\n"
    text += f"违约完成: {changes['breach_end_orders_count']} 个, {changes['breach_end_orders_amount']:,.2f}\n\n"

    # 新增订单明细
    if changes["new_orders"]:
        text += "<b>🆕 新增订单明细</b>\n"
        text += "─" * 40 + "\n"
        for i, order in enumerate(changes["new_orders"][:10], 1):
            order_id = order.get("order_id", "未知")
            customer = order.get("customer", "未知")
            amount = float(order.get("amount", 0) or 0)
            group_name = order.get("group_name", "未知")
            text += f"{i}. {order_id} | {customer} | {amount:,.2f} | {group_name}\n"
        if len(changes["new_orders"]) > 10:
            text += f"... 还有 {len(changes['new_orders']) - 10} 个订单\n"
        text += "\n"

    # 完成订单明细
    if changes["completed_orders"]:
        text += "<b>✅ 完成订单明细</b>\n"
        text += "─" * 40 + "\n"
        for i, order in enumerate(changes["completed_orders"][:10], 1):
            order_id = order.get("order_id", "未知")
            amount = float(order.get("amount", 0) or 0)
            group_name = order.get("group_name", "未知")
            text += f"{i}. {order_id} | {amount:,.2f} | {group_name}\n"
        if len(changes["completed_orders"]) > 10:
            text += f"... 还有 {len(changes['completed_orders']) - 10} 个订单\n"
        text += "\n"

    # 违约完成订单明细
    if changes["breach_end_orders"]:
        text += "<b>⚠️ 违约完成订单明细</b>\n"
        text += "─" * 40 + "\n"
        for i, order in enumerate(changes["breach_end_orders"][:10], 1):
            order_id = order.get("order_id", "未知")
            amount = float(order.get("amount", 0) or 0)
            group_name = order.get("group_name", "未知")
            text += f"{i}. {order_id} | {amount:,.2f} | {group_name}\n"
        if len(changes["breach_end_orders"]) > 10:
            text += f"... 还有 {len(changes['breach_end_orders']) - 10} 个订单\n"
        text += "\n"

    # 收入变更汇总
    text += "<b>💰 收入变更汇总</b>\n"
    text += f"利息收入: {changes['total_interest']:,.2f} ({len(changes['interest_records'])} 笔)\n"
    text += (
        f"归还本金: {changes['total_principal']:,.2f} ({len(changes['principal_records'])} 笔)\n\n"
    )

    # 利息收入明细（前10笔）
    if changes["interest_records"]:
        text += "<b>💵 利息收入明细（前10笔）</b>\n"
        text += "─" * 40 + "\n"
        for i, record in enumerate(changes["interest_records"][:10], 1):
            order_id = record.get("order_id", "未知")
            amount = float(record.get("amount", 0) or 0)
            record_date = record.get("date", "")[:10] if record.get("date") else "未知"
            text += f"{i}. {order_id} | {amount:,.2f} | {record_date}\n"
        if len(changes["interest_records"]) > 10:
            text += f"... 还有 {len(changes['interest_records']) - 10} 笔\n"
        text += "\n"

    # 本金归还明细（前10笔）
    if changes["principal_records"]:
        text += "<b>💸 本金归还明细（前10笔）</b>\n"
        text += "─" * 40 + "\n"
        for i, record in enumerate(changes["principal_records"][:10], 1):
            order_id = record.get("order_id", "未知")
            amount = float(record.get("amount", 0) or 0)
            record_date = record.get("date", "")[:10] if record.get("date") else "未知"
            text += f"{i}. {order_id} | {amount:,.2f} | {record_date}\n"
        if len(changes["principal_records"]) > 10:
            text += f"... 还有 {len(changes['principal_records']) - 10} 笔\n"
        text += "\n"

    # 开销变更汇总
    text += "<b>💸 开销变更汇总</b>\n"
    text += f"公司开销: {changes['company_expenses']:,.2f}\n"
    text += f"其他开销: {changes['other_expenses']:,.2f}\n"
    text += f"总开销: {changes['total_expenses']:,.2f}\n\n"

    # 开销明细（前10笔）
    if changes["expense_records"]:
        text += "<b>📝 开销明细（前10笔）</b>\n"
        text += "─" * 40 + "\n"
        for i, record in enumerate(changes["expense_records"][:10], 1):
            expense_type = "公司" if record.get("type") == "company" else "其他"
            amount = float(record.get("amount", 0) or 0)
            note = record.get("note", "无备注") or "无备注"
            record_date = record.get("date", "")[:10] if record.get("date") else "未知"
            text += f"{i}. {expense_type} | {amount:,.2f} | {note} | {record_date}\n"
        if len(changes["expense_records"]) > 10:
            text += f"... 还有 {len(changes['expense_records']) - 10} 笔\n"
        text += "\n"

    # 总计
    text += "═" * 40 + "\n"
    text += "<b>📊 当日总计</b>\n"
    net_income = changes["total_interest"] + changes["total_principal"] - changes["total_expenses"]
    text += f"净收入: {net_income:,.2f}\n"
    text += f"  (收入: {changes['total_interest'] + changes['total_principal']:,.2f} - 开销: {changes['total_expenses']:,.2f})\n"

    return text
