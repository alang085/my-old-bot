#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量修复订单的星期分组
根据订单日期重新计算并更新所有订单的weekday_group字段
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

import db_operations
from utils.chat_helpers import get_weekday_group_from_date
from constants import WEEKDAY_GROUP


async def fix_weekday_groups():
    """修复所有订单的星期分组"""
    print("Starting to fix order weekday groups...")
    print("=" * 50)
    
    # 获取所有订单（包括已完成和违约完成的）
    all_orders = await db_operations.search_orders_advanced_all_states({})
    
    if not all_orders:
        print("No orders found")
        return
    
    print(f"Found {len(all_orders)} orders")
    print("-" * 50)
    
    fixed_count = 0
    error_count = 0
    unchanged_count = 0
    
    for order in all_orders:
        order_id = order['order_id']
        chat_id = order['chat_id']
        current_weekday_group = order.get('weekday_group', '')
        order_date_str = order.get('date', '')
        
        try:
            # 方法1: 从订单ID解析日期（优先）
            date_from_id = None
            if len(order_id) >= 6 and order_id[:6].isdigit():
                date_part = order_id[:6]
                try:
                    full_date_str = f"20{date_part}"
                    date_from_id = datetime.strptime(full_date_str, "%Y%m%d").date()
                except ValueError:
                    pass
            
            # 方法2: 从date字段解析日期
            date_from_db = None
            if order_date_str:
                try:
                    # 处理 "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD" 格式
                    date_str = order_date_str.split()[0] if ' ' in order_date_str else order_date_str
                    date_from_db = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    pass
            
            # 优先使用订单ID中的日期，如果没有则使用数据库中的日期
            order_date = date_from_id or date_from_db
            
            if not order_date:
                print(f"[ERROR] Order {order_id} (chat_id: {chat_id}): Cannot parse date")
                error_count += 1
                continue
            
            # 计算正确的星期分组
            correct_weekday_group = get_weekday_group_from_date(order_date)
            
            # 检查是否需要更新
            if current_weekday_group == correct_weekday_group:
                unchanged_count += 1
                continue
            
            # 更新星期分组
            success = await db_operations.update_order_weekday_group(chat_id, correct_weekday_group)
            
            if success:
                weekday_name = order_date.strftime('%A')
                print(f"[FIXED] Order {order_id}: {order_date} ({weekday_name})")
                print(f"        Update: '{current_weekday_group}' -> '{correct_weekday_group}'")
                fixed_count += 1
            else:
                print(f"[ERROR] Order {order_id}: Update failed")
                error_count += 1
                
        except Exception as e:
            print(f"[ERROR] Order {order_id}: Processing error - {e}")
            error_count += 1
    
    print("=" * 50)
    print(f"Fix completed!")
    print(f"  Fixed: {fixed_count} orders")
    print(f"  Unchanged: {unchanged_count} orders")
    print(f"  Errors: {error_count} orders")
    print(f"  Total: {len(all_orders)} orders")


if __name__ == "__main__":
    asyncio.run(fix_weekday_groups())

