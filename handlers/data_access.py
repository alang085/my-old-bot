"""数据访问接口模块

为callbacks提供统一的数据访问接口，避免callbacks直接访问db_operations。
这是架构优化的一部分，确保表现层（callbacks）不直接依赖数据层（db_operations）。

功能特性：
- 统一的数据访问接口
- 缓存支持（可选）
- 性能监控
- 错误处理
"""

import logging
from typing import Dict, List, Optional

import db_operations
from utils.cache import cached
from utils.performance_monitor import monitor_performance

logger = logging.getLogger(__name__)


# ========== 用户权限相关 ==========


async def get_user_authorization_status(user_id: int) -> bool:
    """检查用户是否已授权"""
    return await db_operations.is_user_authorized(user_id)


async def get_user_group_id(user_id: int) -> Optional[str]:
    """获取用户的归属ID"""
    return await db_operations.get_user_group_id(user_id)


# ========== 订单相关 ==========


@cached(ttl=300, key_prefix="order_")  # 缓存5分钟
@monitor_performance("get_order_by_chat_id")
async def get_order_by_chat_id_for_callback(chat_id: int) -> Optional[Dict]:
    """为callbacks获取订单信息（带缓存和性能监控）"""
    return await db_operations.get_order_by_chat_id(chat_id)


# ========== 归属ID相关 ==========


@cached(ttl=300, key_prefix="group_ids_")  # 缓存5分钟
@monitor_performance("get_all_group_ids")
async def get_all_group_ids_for_callback() -> List[str]:
    """为callbacks获取所有归属ID（带缓存和性能监控）"""
    return await db_operations.get_all_group_ids()


# ========== 收入记录相关 ==========


@cached(ttl=60, key_prefix="income_")  # 缓存1分钟（带日期参数，变化频繁）
@monitor_performance("get_income_records")
async def get_income_records_for_callback(
    start_date: str, end_date: str, income_type: Optional[str] = None
) -> List[Dict]:
    """为callbacks获取收入记录（带缓存和性能监控）"""
    return await db_operations.get_income_records(start_date, end_date, type=income_type)


# ========== 支出记录相关 ==========


@cached(ttl=60, key_prefix="expense_")  # 缓存1分钟（带日期参数，变化频繁）
@monitor_performance("get_expense_records")
async def get_expense_records_for_callback(
    start_date: str, end_date: str, expense_type: Optional[str] = None
) -> List[Dict]:
    """为callbacks获取支出记录（带缓存和性能监控）"""
    return await db_operations.get_expense_records(start_date, end_date, expense_type)


# ========== 财务数据相关 ==========


@cached(ttl=60, key_prefix="financial_")  # 缓存1分钟
@monitor_performance("get_financial_data")
async def get_financial_data_for_callback() -> Dict:
    """为callbacks获取财务数据（带缓存和性能监控）"""
    return await db_operations.get_financial_data()


# ========== 操作历史相关 ==========


async def get_operations_by_date_for_callback(date: str) -> List[Dict]:
    """为callbacks获取指定日期的操作历史"""
    return await db_operations.get_operations_by_date(date)


async def record_operation_for_callback(
    user_id: int,
    operation_type: str,
    operation_data: Dict,
    chat_id: Optional[int] = None,
) -> bool:
    """为callbacks记录操作历史"""
    return await db_operations.record_operation(
        user_id=user_id,
        operation_type=operation_type,
        operation_data=operation_data,
        chat_id=chat_id,
    )


# ========== 支付账号相关 ==========


async def get_payment_accounts_by_type_for_callback(account_type: str) -> List[Dict]:
    """为callbacks获取指定类型的支付账号列表"""
    return await db_operations.get_payment_accounts_by_type(account_type)


async def get_payment_account_for_callback(account_type: str) -> Optional[Dict]:
    """为callbacks获取指定类型的支付账号（单个）"""
    return await db_operations.get_payment_account(account_type)


async def get_payment_account_by_id_for_callback(account_id: int) -> Optional[Dict]:
    """为callbacks根据ID获取支付账号"""
    return await db_operations.get_payment_account_by_id(account_id)


async def get_all_payment_accounts_for_callback() -> List[Dict]:
    """为callbacks获取所有支付账号"""
    return await db_operations.get_all_payment_accounts()


# ========== 定时播报相关 ==========


async def get_scheduled_broadcast_for_callback(broadcast_id: int) -> Optional[Dict]:
    """为callbacks获取定时播报配置"""
    return await db_operations.get_scheduled_broadcast(broadcast_id)


async def get_all_scheduled_broadcasts_for_callback() -> List[Dict]:
    """为callbacks获取所有定时播报配置"""
    return await db_operations.get_all_scheduled_broadcasts()


async def delete_scheduled_broadcast_for_callback(slot: int) -> bool:
    """为callbacks删除定时播报"""
    return await db_operations.delete_scheduled_broadcast(slot)


async def toggle_scheduled_broadcast_for_callback(slot: int, is_active: int) -> bool:
    """为callbacks切换定时播报的激活状态"""
    return await db_operations.toggle_scheduled_broadcast(slot, is_active)


# ========== 群组消息相关 ==========


async def get_group_message_config_by_chat_id_for_callback(chat_id: int) -> Optional[Dict]:
    """为callbacks获取群组消息配置"""
    return await db_operations.get_group_message_config_by_chat_id(chat_id)


async def get_group_message_configs_for_callback() -> List[Dict]:
    """为callbacks获取所有群组消息配置"""
    return await db_operations.get_group_message_configs()


async def get_all_company_announcements_for_callback() -> List[Dict]:
    """为callbacks获取所有公司公告"""
    return await db_operations.get_all_company_announcements()


async def get_all_anti_fraud_messages_for_callback() -> List[Dict]:
    """为callbacks获取所有防诈骗消息"""
    return await db_operations.get_all_anti_fraud_messages()


async def get_all_promotion_messages_for_callback() -> List[Dict]:
    """为callbacks获取所有宣传语录"""
    return await db_operations.get_all_promotion_messages()


async def get_company_announcements_for_callback() -> List[Dict]:
    """为callbacks获取所有激活的公司公告"""
    return await db_operations.get_company_announcements()


async def toggle_company_announcement_for_callback(announcement_id: int, is_active: int) -> bool:
    """为callbacks切换公司公告的激活状态"""
    return await db_operations.toggle_company_announcement(announcement_id, is_active)


async def delete_company_announcement_for_callback(announcement_id: int) -> bool:
    """为callbacks删除公司公告"""
    return await db_operations.delete_company_announcement(announcement_id)


async def toggle_anti_fraud_message_for_callback(message_id: int) -> bool:
    """为callbacks切换防诈骗消息的激活状态"""
    return await db_operations.toggle_anti_fraud_message(message_id)


async def delete_anti_fraud_message_for_callback(message_id: int) -> bool:
    """为callbacks删除防诈骗消息"""
    return await db_operations.delete_anti_fraud_message(message_id)


async def toggle_promotion_message_for_callback(message_id: int) -> bool:
    """为callbacks切换宣传语录的激活状态"""
    return await db_operations.toggle_promotion_message(message_id)


async def delete_promotion_message_for_callback(message_id: int) -> bool:
    """为callbacks删除宣传语录"""
    return await db_operations.delete_promotion_message(message_id)


# ========== 订单搜索相关 ==========


async def search_orders_advanced_for_callback(criteria: Dict) -> List[Dict]:
    """为callbacks高级搜索订单"""
    return await db_operations.search_orders_advanced(criteria)
