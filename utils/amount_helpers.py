"""金额处理相关工具函数"""
import re
from typing import Optional, List, Dict


def parse_amount(text: str) -> Optional[float]:
    """
    解析金额文本，支持多种格式
    例如: "20万" -> 200000, "20.5万" -> 205000, "200000" -> 200000
    """
    text = text.strip().replace(',', '')
    
    # 匹配"万"单位
    match = re.match(r'^(\d+(?:\.\d+)?)\s*万$', text)
    if match:
        return float(match.group(1)) * 10000
    
    # 匹配纯数字
    match = re.match(r'^(\d+(?:\.\d+)?)$', text)
    if match:
        return float(match.group(1))
    
    return None


def select_orders_by_amount(orders: List[Dict], target_amount: float) -> List[Dict]:
    """
    使用贪心算法从订单列表中选择订单，使得总金额尽可能接近目标金额
    返回选中的订单列表
    """
    if not orders or target_amount <= 0:
        return []
    
    # 按金额降序排序
    sorted_orders = sorted(orders, key=lambda x: x.get('amount', 0), reverse=True)
    
    selected = []
    current_total = 0.0
    
    for order in sorted_orders:
        order_amount = order.get('amount', 0)
        if order_amount <= 0:
            continue  # 跳过金额为0或负数的订单
        
        if current_total + order_amount <= target_amount:
            selected.append(order)
            current_total += order_amount
        elif current_total < target_amount and current_total + order_amount - target_amount < target_amount * 0.1:
            # 如果超过目标金额但差额小于10%，仍然选择（允许小幅超过）
            selected.append(order)
            current_total += order_amount
            break  # 达到目标后停止
    
    return selected


def distribute_orders_evenly_by_weekday(orders: List[Dict], target_total_amount: float) -> List[Dict]:
    """
    从周一到周日的有效订单中，均匀地选择订单，使得总金额接近目标金额
    并且每天的订单金额尽可能均衡
    返回选中的订单列表
    """
    from constants import WEEKDAY_GROUP
    
    if not orders or target_total_amount <= 0:
        return []
    
    # 按星期分组
    weekday_orders = {}
    for weekday_name in WEEKDAY_GROUP.values():
        weekday_orders[weekday_name] = []
    
    for order in orders:
        weekday_group = order.get('weekday_group')
        if weekday_group in weekday_orders:
            weekday_orders[weekday_group].append(order)
    
    # 计算每天的目标金额
    daily_target = target_total_amount / 7
    
    # 计算每天可用的订单总金额
    weekday_available_amounts = {}
    for weekday_name in ['一', '二', '三', '四', '五', '六', '日']:
        day_orders = weekday_orders.get(weekday_name, [])
        total_amount = sum(order.get('amount', 0) for order in day_orders)
        weekday_available_amounts[weekday_name] = total_amount
    
    # 计算总可用金额
    total_available = sum(weekday_available_amounts.values())
    
    # 如果总可用金额不足，按比例分配目标金额
    if total_available < target_total_amount:
        # 按每天可用金额的比例分配目标金额
        daily_targets = {}
        for weekday_name in ['一', '二', '三', '四', '五', '六', '日']:
            if total_available > 0:
                daily_targets[weekday_name] = (weekday_available_amounts[weekday_name] / total_available) * target_total_amount
            else:
                daily_targets[weekday_name] = 0
    else:
        # 如果总可用金额充足，使用均衡分配
        # 但需要调整：如果某天订单不足，将多余的目标分配给其他天
        daily_targets = {}
        remaining_target = target_total_amount
        remaining_days = []
        
        # 第一轮：给每天分配基础目标，但不超过可用金额
        for weekday_name in ['一', '二', '三', '四', '五', '六', '日']:
            available = weekday_available_amounts[weekday_name]
            if available >= daily_target:
                daily_targets[weekday_name] = daily_target
                remaining_target -= daily_target
            else:
                # 如果可用金额不足，使用全部可用金额
                daily_targets[weekday_name] = available
                remaining_target -= available
                remaining_days.append(weekday_name)
        
        # 第二轮：将剩余的目标金额分配给有能力的天
        if remaining_target > 0:
            capable_days = [
                name for name in ['一', '二', '三', '四', '五', '六', '日']
                if name not in remaining_days and 
                weekday_available_amounts[name] > daily_targets[name]
            ]
            if capable_days:
                # 按可用余额比例分配剩余目标
                total_capacity = sum(
                    weekday_available_amounts[name] - daily_targets[name]
                    for name in capable_days
                )
                if total_capacity > 0:
                    for name in capable_days:
                        capacity = weekday_available_amounts[name] - daily_targets[name]
                        additional = (capacity / total_capacity) * remaining_target
                        daily_targets[name] += min(additional, capacity)
    
    # 对每天使用贪心算法选择订单
    selected_orders = []
    weekday_selected_amounts = {}
    
    for weekday_name in ['一', '二', '三', '四', '五', '六', '日']:
        day_orders = weekday_orders.get(weekday_name, [])
        if day_orders and weekday_name in daily_targets:
            day_target = daily_targets[weekday_name]
            day_selected = select_orders_by_amount(day_orders, day_target)
            selected_orders.extend(day_selected)
            weekday_selected_amounts[weekday_name] = sum(
                order.get('amount', 0) for order in day_selected
            )
        else:
            weekday_selected_amounts[weekday_name] = 0.0
    
    return selected_orders

