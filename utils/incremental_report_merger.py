"""增量报表合并器"""

# 标准库
import logging
from typing import Dict, List

import pytz

# 本地模块
from utils.stats_helpers import update_all_stats

logger = logging.getLogger(__name__)

# 北京时区
BEIJING_TZ = pytz.timezone("Asia/Shanghai")


async def calculate_incremental_stats(orders_data: List[Dict], expense_records: List[Dict]) -> Dict:
    """计算增量数据对全局统计的影响"""
    stats = {
        "new_orders_count": 0,
        "new_orders_amount": 0.0,
        "new_clients_count": 0,
        "new_clients_amount": 0.0,
        "old_clients_count": 0,
        "old_clients_amount": 0.0,
        "interest": 0.0,
        "completed_orders_count": 0,
        "completed_amount": 0.0,
        "breach_end_orders_count": 0,
        "breach_end_amount": 0.0,
        "principal_reduction": 0.0,
        "company_expenses": 0.0,
        "other_expenses": 0.0,
    }

    # 统计订单数据
    processed_orders = set()  # 避免重复统计同一订单

    for order in orders_data:
        order_id = order.get("order_id")
        if not order_id:
            continue

        # 如果是新订单（在基准日期之后创建）
        if order_id not in processed_orders:
            processed_orders.add(order_id)
            customer = order.get("customer", "")
            amount = float(order.get("amount", 0) or 0)

            # 统计新/老客户
            if customer == "A":
                stats["new_clients_count"] += 1
                stats["new_clients_amount"] += amount
            elif customer == "B":
                stats["old_clients_count"] += 1
                stats["old_clients_amount"] += amount

            # 统计新订单
            stats["new_orders_count"] += 1
            stats["new_orders_amount"] += amount

        # 统计利息
        total_interest = float(order.get("total_interest", 0) or 0)
        stats["interest"] += total_interest

        # 统计本金归还
        principal_reduction = float(order.get("principal_reduction", 0) or 0)
        stats["principal_reduction"] += principal_reduction

        # 统计订单完成
        state = order.get("state", "")
        if state == "end":
            stats["completed_orders_count"] += 1
            stats["completed_amount"] += float(order.get("amount", 0) or 0)
        elif state == "breach_end":
            stats["breach_end_orders_count"] += 1
            stats["breach_end_amount"] += float(order.get("amount", 0) or 0)

    # 统计开销
    for expense in expense_records:
        expense_type = expense.get("type", "")
        amount = float(expense.get("amount", 0) or 0)

        if expense_type == "company":
            stats["company_expenses"] += amount
        elif expense_type == "other":
            stats["other_expenses"] += amount

    return stats


async def merge_incremental_report_to_global(
    orders_data: List[Dict], expense_records: List[Dict]
) -> Dict:
    """将增量报表合并到全局数据，更新全局统计数据"""
    try:
        # 计算增量统计
        incremental_stats = await calculate_incremental_stats(orders_data, expense_records)

        # 更新全局统计数据
        # 注意：这些数据在业务操作时已经更新过了，这里主要是为了确保一致性
        # 如果数据已经更新过，这里不会重复更新（因为update_financial_data是累加的）

        # 更新新客户数据
        if incremental_stats["new_clients_count"] > 0:
            await update_all_stats(
                "new_clients",
                incremental_stats["new_clients_amount"],
                incremental_stats["new_clients_count"],
                None,
            )

        # 更新老客户数据
        if incremental_stats["old_clients_count"] > 0:
            await update_all_stats(
                "old_clients",
                incremental_stats["old_clients_amount"],
                incremental_stats["old_clients_count"],
                None,
            )

        # 更新利息收入（注意：利息收入在记录时已经更新，这里确保一致性）
        if incremental_stats["interest"] > 0:
            # 利息收入在记录时已经通过record_income更新了，这里不需要重复更新
            pass

        # 更新完成订单
        if incremental_stats["completed_orders_count"] > 0:
            await update_all_stats(
                "completed",
                incremental_stats["completed_amount"],
                incremental_stats["completed_orders_count"],
                None,
            )

        # 更新违约完成订单
        if incremental_stats["breach_end_orders_count"] > 0:
            await update_all_stats(
                "breach_end",
                incremental_stats["breach_end_amount"],
                incremental_stats["breach_end_orders_count"],
                None,
            )

        logger.info(f"增量报表已合并到全局数据: {incremental_stats}")

        return {
            "success": True,
            "stats": incremental_stats,
            "message": "增量报表已成功合并到全局数据",
        }
    except Exception as e:
        logger.error(f"合并增量报表失败: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": f"合并失败: {str(e)}"}


async def preview_incremental_report(baseline_date: str) -> str:
    """预览增量报表内容"""
    try:
        # 获取增量数据
        from utils.incremental_report_generator import prepare_incremental_data

        incremental_data = await prepare_incremental_data(baseline_date)

        orders_data = incremental_data.get("orders", [])
        expense_records = incremental_data.get("expenses", [])
        current_date = incremental_data.get("current_date", "")

        # 计算统计信息
        stats = await calculate_incremental_stats(orders_data, expense_records)

        # 生成预览文本
        preview = "📊 增量报表预览\n"
        preview += f"{'═' * 40}\n"
        preview += f"基准日期: {baseline_date}\n"
        preview += f"当前日期: {current_date}\n"
        preview += f"{'═' * 40}\n\n"

        # 订单统计
        preview += "📦 订单统计\n"
        preview += f"新增订单数: {stats['new_orders_count']}\n"
        preview += f"新增订单金额: {stats['new_orders_amount']:,.2f}\n"
        preview += f"新客户数: {stats['new_clients_count']}\n"
        preview += f"新客户金额: {stats['new_clients_amount']:,.2f}\n"
        preview += f"老客户数: {stats['old_clients_count']}\n"
        preview += f"老客户金额: {stats['old_clients_amount']:,.2f}\n\n"

        # 收入统计
        preview += "💰 收入统计\n"
        preview += f"利息收入: {stats['interest']:,.2f}\n"
        preview += f"归还本金: {stats['principal_reduction']:,.2f}\n"
        preview += f"完成订单数: {stats['completed_orders_count']}\n"
        preview += f"完成订单金额: {stats['completed_amount']:,.2f}\n"
        preview += f"违约完成订单数: {stats['breach_end_orders_count']}\n"
        preview += f"违约完成金额: {stats['breach_end_amount']:,.2f}\n\n"

        # 开销统计
        preview += "💸 开销统计\n"
        preview += f"公司开销: {stats['company_expenses']:,.2f}\n"
        preview += f"其他开销: {stats['other_expenses']:,.2f}\n"
        preview += f"总开销: {stats['company_expenses'] + stats['other_expenses']:,.2f}\n\n"

        # 订单列表（前10个）
        if orders_data:
            preview += f"📋 订单列表（显示前10个，共{len(orders_data)}个）\n"
            preview += f"{'─' * 40}\n"
            for i, order in enumerate(orders_data[:10], 1):
                order_id = order.get("order_id", "未知")
                customer = order.get("customer", "未知")
                amount = float(order.get("amount", 0) or 0)
                total_interest = float(order.get("total_interest", 0) or 0)
                principal = float(order.get("principal_reduction", 0) or 0)
                state = order.get("state", "未知")

                preview += f"{i}. {order_id} | {customer} | {amount:,.2f} | "
                preview += f"利息:{total_interest:,.2f} | 本金:{principal:,.2f} | {state}\n"

            if len(orders_data) > 10:
                preview += f"... 还有 {len(orders_data) - 10} 个订单\n"

        return preview
    except Exception as e:
        logger.error(f"预览增量报表失败: {e}", exc_info=True)
        return f"❌ 预览失败: {str(e)}"
