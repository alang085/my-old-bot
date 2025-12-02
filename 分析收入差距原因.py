"""
分析实际收入和统计收入的差距原因，生成详细报表
"""
import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime

# 设置输出编码为UTF-8（Windows）
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.date_helpers import get_daily_period_date
import db_operations

async def analyze_income_gap():
    """分析收入差距并生成详细报表"""
    
    try:
        # 检查数据库连接
        print("正在连接数据库...", flush=True)
        test_data = await db_operations.get_financial_data()
        print(f"✅ 数据库连接成功，流动资金: {test_data.get('liquid_funds', 0):,.2f}", flush=True)
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return
    
    # 获取今天日期
    date = get_daily_period_date()
    
    print("=" * 100)
    print(f"📊 实际收入 vs 统计收入差距分析报表")
    print(f"日期: {date}")
    print("=" * 100)
    print()
    
    # 1. 获取所有收入明细记录
    print("[1] 查询收入明细记录...")
    income_records = await db_operations.get_income_records(date, date)
    print(f"✅ 共找到 {len(income_records)} 条收入明细记录")
    print()
    
    # 2. 按类型分组统计
    by_type = {}
    by_group = {}
    
    for record in income_records:
        income_type = record.get('type', 'unknown')
        group_id = record.get('group_id')
        amount = record.get('amount', 0)
        
        # 按类型统计
        if income_type not in by_type:
            by_type[income_type] = {
                'count': 0,
                'total': 0.0,
                'records': []
            }
        by_type[income_type]['count'] += 1
        by_type[income_type]['total'] += amount
        by_type[income_type]['records'].append(record)
        
        # 按归属ID统计
        group_key = group_id if group_id else 'NULL'
        if group_key not in by_group:
            by_group[group_key] = {
                'count': 0,
                'total': 0.0,
                'records': []
            }
        by_group[group_key]['count'] += 1
        by_group[group_key]['total'] += amount
        by_group[group_key]['records'].append(record)
    
    # 3. 获取统计数据
    print("[2] 查询统计数据 (daily_data)...")
    stats = await db_operations.get_stats_by_date_range(date, date, None)
    
    # 获取所有归属ID的统计数据
    all_group_ids = await db_operations.get_all_group_ids()
    group_stats = {}
    for group_id in all_group_ids:
        group_stats[group_id] = await db_operations.get_stats_by_date_range(date, date, group_id)
    
    print("✅ 统计数据查询完成")
    print()
    
    # 4. 生成对比报表
    print("=" * 100)
    print("📋 详细对比报表")
    print("=" * 100)
    print()
    
    # 4.1 按类型对比
    print("【一、按收入类型对比】")
    print("-" * 100)
    
    type_mapping = {
        'interest': ('利息收入', 'interest'),
        'completed': ('订单完成', 'completed_amount'),
        'breach_end': ('违约完成', 'breach_end_amount'),
        'principal_reduction': ('本金减少', None),
        'adjustment': ('调整', None)
    }
    
    total_actual = 0.0
    total_stats = 0.0
    
    for income_type, (type_name, stats_key) in type_mapping.items():
        if income_type not in by_type:
            continue
            
        actual_amount = by_type[income_type]['total']
        actual_count = by_type[income_type]['count']
        
        # 获取统计表中的金额
        if stats_key:
            stats_amount = stats.get(stats_key, 0)
        else:
            stats_amount = 0  # 本金减少和调整不在统计表中
        
        diff = actual_amount - stats_amount
        
        print(f"\n{type_name}:")
        print(f"  实际收入明细: {actual_count} 笔，总计 {actual_amount:,.2f} 元")
        if stats_key:
            print(f"  统计数据 (daily_data): {stats_amount:,.2f} 元")
            print(f"  差距: {diff:,.2f} 元", end="")
            if abs(diff) > 0.01:
                if diff > 0:
                    print(f" ⚠️ 收入明细比统计表多 {diff:,.2f} 元")
                else:
                    print(f" ⚠️ 统计表比收入明细多 {abs(diff):,.2f} 元")
            else:
                print(" ✅ 一致")
        else:
            print(f"  统计数据: (无此字段)")
            print(f"  说明: {type_name}不在统计表中")
        
        total_actual += actual_amount
        if stats_key:
            total_stats += stats_amount
    
    print(f"\n总计对比:")
    print(f"  实际收入明细总计: {total_actual:,.2f} 元")
    print(f"  统计数据总计: {total_stats:,.2f} 元")
    print(f"  总差距: {total_actual - total_stats:,.2f} 元")
    print()
    
    # 4.2 按归属ID对比（只对比利息收入）
    print("=" * 100)
    print("【二、按归属ID对比（利息收入）】")
    print("-" * 100)
    
    # 只统计利息收入
    interest_records = by_type.get('interest', {}).get('records', [])
    
    # 按归属ID分组利息收入
    interest_by_group = {}
    for record in interest_records:
        group_id = record.get('group_id')
        group_key = group_id if group_id else 'NULL'
        if group_key not in interest_by_group:
            interest_by_group[group_key] = {
                'count': 0,
                'total': 0.0,
                'records': []
            }
        interest_by_group[group_key]['count'] += 1
        interest_by_group[group_key]['total'] += record.get('amount', 0)
        interest_by_group[group_key]['records'].append(record)
    
    # 对比每个归属ID
    total_interest_actual = 0.0
    total_interest_stats = 0.0
    
    # 先显示全局（NULL）
    if 'NULL' in interest_by_group:
        group_key = 'NULL'
        actual = interest_by_group[group_key]['total']
        count = interest_by_group[group_key]['count']
        stats_interest = stats.get('interest', 0)
        diff = actual - stats_interest
        
        print(f"\n全局 (NULL):")
        print(f"  实际收入明细: {count} 笔，总计 {actual:,.2f} 元")
        print(f"  统计数据: {stats_interest:,.2f} 元")
        print(f"  差距: {diff:,.2f} 元", end="")
        if abs(diff) > 0.01:
            if diff > 0:
                print(f" ⚠️ 收入明细多 {diff:,.2f} 元")
            else:
                print(f" ⚠️ 统计表多 {abs(diff):,.2f} 元")
        else:
            print(" ✅ 一致")
        
        total_interest_actual += actual
        total_interest_stats += stats_interest
    
    # 显示各归属ID
    for group_id in sorted(all_group_ids):
        group_key = group_id
        if group_key not in interest_by_group:
            continue
        
        actual = interest_by_group[group_key]['total']
        count = interest_by_group[group_key]['count']
        group_stat = group_stats.get(group_id, {})
        stats_interest = group_stat.get('interest', 0)
        diff = actual - stats_interest
        
        print(f"\n{group_id}:")
        print(f"  实际收入明细: {count} 笔，总计 {actual:,.2f} 元")
        print(f"  统计数据: {stats_interest:,.2f} 元")
        print(f"  差距: {diff:,.2f} 元", end="")
        if abs(diff) > 0.01:
            if diff > 0:
                print(f" ⚠️ 收入明细多 {diff:,.2f} 元")
            else:
                print(f" ⚠️ 统计表多 {abs(diff):,.2f} 元")
        else:
            print(" ✅ 一致")
        
        total_interest_actual += actual
        total_interest_stats += stats_interest
    
    print(f"\n利息收入总计对比:")
    print(f"  实际收入明细总计: {total_interest_actual:,.2f} 元")
    print(f"  统计数据总计: {total_interest_stats:,.2f} 元")
    print(f"  总差距: {total_interest_actual - total_interest_stats:,.2f} 元")
    print()
    
    # 5. 分析差距原因
    print("=" * 100)
    print("【三、差距原因分析】")
    print("-" * 100)
    print()
    
    # 检查是否有未同步的记录
    print("可能的原因：")
    print()
    
    # 5.1 检查利息收入差距
    interest_actual = by_type.get('interest', {}).get('total', 0)
    interest_stats = stats.get('interest', 0)
    interest_diff = interest_actual - interest_stats
    
    if abs(interest_diff) > 0.01:
        print(f"1. 利息收入差距: {interest_diff:,.2f} 元")
        if interest_diff > 0:
            print(f"   → 收入明细中有 {interest_diff:,.2f} 元的利息收入未更新到统计表")
            print(f"   → 可能原因：")
            print(f"      a) record_income() 执行成功，但 update_all_stats() 执行失败")
            print(f"      b) 记录时使用了错误的日期")
            print(f"      c) 统计表被手动修改或重置")
            print(f"      d) 有归属ID的记录没有正确更新对应归属ID的统计表")
            print()
            
            # 列出所有利息收入记录
            if interest_records:
                print(f"   涉及的所有利息收入记录（共 {len(interest_records)} 条）:")
                for i, record in enumerate(sorted(interest_records, key=lambda x: x.get('created_at', '')), 1):
                    group_id = record.get('group_id')
                    group_display = group_id if group_id else 'NULL (全局)'
                    order_id = record.get('order_id') or '无'
                    amount = record.get('amount', 0)
                    created_at = record.get('created_at', 'N/A')
                    print(f"     {i}. {amount:,.2f} 元 | 订单: {order_id} | 归属ID: {group_display} | 时间: {created_at}")
        else:
            print(f"   → 统计表中的利息收入比收入明细多 {abs(interest_diff):,.2f} 元")
            print(f"   → 可能原因：")
            print(f"      a) 统计表中有历史数据或手动修改的数据")
            print(f"      b) 某些记录被删除但统计未更新")
        print()
    
    # 5.2 检查完成订单差距
    completed_actual = by_type.get('completed', {}).get('total', 0)
    completed_stats = stats.get('completed_amount', 0)
    completed_diff = completed_actual - completed_stats
    
    if abs(completed_diff) > 0.01:
        print(f"2. 订单完成金额差距: {completed_diff:,.2f} 元")
        if completed_diff > 0:
            print(f"   → 收入明细中有 {completed_diff:,.2f} 元的完成订单金额未更新到统计表")
        else:
            print(f"   → 统计表中的完成订单金额比收入明细多 {abs(completed_diff):,.2f} 元")
        print()
    
    # 5.3 检查违约完成差距
    breach_end_actual = by_type.get('breach_end', {}).get('total', 0)
    breach_end_stats = stats.get('breach_end_amount', 0)
    breach_end_diff = breach_end_actual - breach_end_stats
    
    if abs(breach_end_diff) > 0.01:
        print(f"3. 违约完成金额差距: {breach_end_diff:,.2f} 元")
        if breach_end_diff > 0:
            print(f"   → 收入明细中有 {breach_end_diff:,.2f} 元的违约完成金额未更新到统计表")
        else:
            print(f"   → 统计表中的违约完成金额比收入明细多 {abs(breach_end_diff):,.2f} 元")
        print()
    
    # 6. 生成建议
    print("=" * 100)
    print("【四、修复建议】")
    print("-" * 100)
    print()
    
    if abs(interest_diff) > 0.01 or abs(completed_diff) > 0.01 or abs(breach_end_diff) > 0.01:
        print("发现数据不一致，建议采取以下措施：")
        print()
        print("1. 使用 /fix_statistics 命令修复统计数据")
        print("   （此命令会根据收入明细重新计算并更新统计表）")
        print()
        print("2. 检查是否有错误删除的记录")
        print("   （如果有记录被删除，统计表可能还保留了旧数据）")
        print()
        print("3. 检查记录创建时间")
        print("   （确认收入记录的日期是否正确）")
        print()
        print("4. 检查归属ID是否正确")
        print("   （确认每条记录是否属于正确的归属ID）")
    else:
        print("✅ 数据一致，无需修复")
    
    print()
    print("=" * 100)
    print("报表生成完成")
    print("=" * 100)

if __name__ == "__main__":
    print("脚本开始运行...", flush=True)
    try:
        asyncio.run(analyze_income_gap())
        print("\n脚本执行完成", flush=True)
    except KeyboardInterrupt:
        print("\n已取消", flush=True)
        sys.exit(0)
    except Exception as e:
        print(f"❌ 运行时错误: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

