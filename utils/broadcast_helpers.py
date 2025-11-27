"""播报相关工具函数"""
from datetime import datetime, timedelta
from typing import Tuple


def calculate_next_payment_date() -> Tuple[datetime, str, str]:
    """
    计算下一个付款日期（下周五）
    返回: (日期对象, 日期字符串, 星期字符串)
    """
    today = datetime.now()
    days_until_friday = (4 - today.weekday()) % 7
    if days_until_friday == 0:
        days_until_friday = 7
    next_friday = today + timedelta(days=days_until_friday)
    
    # 格式化日期（格式：November 26,2025）
    date_str = next_friday.strftime("%B %d,%Y")
    weekday_str = next_friday.strftime("%A")
    
    return next_friday, date_str, weekday_str


def format_broadcast_message(
    principal: float,
    principal_12: float,
    outstanding_interest: float = 0,
    date_str: str = None,
    weekday_str: str = None
) -> str:
    """
    生成播报消息模板
    
    Args:
        principal: 本金金额
        principal_12: 本金12%金额
        outstanding_interest: 未付利息（默认0）
        date_str: 日期字符串（如果为None，自动计算）
        weekday_str: 星期字符串（如果为None，自动计算）
    
    Returns:
        格式化后的播报消息
    """
    # 如果没有提供日期，自动计算
    if date_str is None or weekday_str is None:
        _, date_str, weekday_str = calculate_next_payment_date()
    
    # 格式化金额（添加千位分隔符）
    principal_formatted = f"{principal:,.0f}"
    principal_12_formatted = f"{principal_12:,.0f}"
    
    # 构建播报消息
    message = (
        f"Your next payment is due on {date_str} ({weekday_str}) "
        f"for {principal_formatted} or {principal_12_formatted} to defer the principal payment for one week.\n\n"
        f"Your outstanding interest is {outstanding_interest}"
    )
    
    return message

