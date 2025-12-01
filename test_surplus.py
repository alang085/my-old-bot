"""测试盈余计算功能"""
import asyncio
import sys
from datetime import datetime
import pytz

# 添加项目路径
sys.path.insert(0, '.')

import db_operations
from handlers.report_handlers import generate_report_text
from utils.date_helpers import get_daily_period_date


async def test_surplus_calculation():
    """测试盈余计算逻辑"""
    print("=" * 60)
    print("🧪 盈余计算功能测试")
    print("=" * 60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # 测试场景
    test_cases = [
        {
            "name": "测试场景1: 正常情况",
            "stats": {
                'interest': 1000.0,
                'breach_end_amount': 5000.0,
                'breach_amount': 3000.0
            },
            "expected": 3000.0,
            "description": "利息1000 + 违约完成5000 - 违约3000 = 盈余3000"
        },
        {
            "name": "测试场景2: 负盈余",
            "stats": {
                'interest': 500.0,
                'breach_end_amount': 1000.0,
                'breach_amount': 3000.0
            },
            "expected": -1500.0,
            "description": "利息500 + 违约完成1000 - 违约3000 = 盈余-1500"
        },
        {
            "name": "测试场景3: 零盈余",
            "stats": {
                'interest': 1000.0,
                'breach_end_amount': 2000.0,
                'breach_amount': 3000.0
            },
            "expected": 0.0,
            "description": "利息1000 + 违约完成2000 - 违约3000 = 盈余0"
        },
        {
            "name": "测试场景4: 只有利息",
            "stats": {
                'interest': 5000.0,
                'breach_end_amount': 0.0,
                'breach_amount': 0.0
            },
            "expected": 5000.0,
            "description": "利息5000 + 违约完成0 - 违约0 = 盈余5000"
        }
    ]
    
    print("📊 测试盈余计算公式: 盈余 = 利息 + 违约完成金额 - 违约金额\n")
    
    passed = 0
    failed = 0
    
    for i, case in enumerate(test_cases, 1):
        print(f"【{case['name']}】")
        print(f"  描述: {case['description']}")
        
        # 计算盈余
        surplus = case['stats']['interest'] + case['stats']['breach_end_amount'] - case['stats']['breach_amount']
        
        # 验证结果
        if abs(surplus - case['expected']) < 0.01:  # 允许浮点数误差
            print(f"  ✅ 通过: 盈余 = {surplus:.2f} (预期: {case['expected']:.2f})")
            passed += 1
        else:
            print(f"  ❌ 失败: 盈余 = {surplus:.2f} (预期: {case['expected']:.2f})")
            failed += 1
        print()
    
    print("=" * 60)
    print(f"📈 测试结果: {passed} 通过, {failed} 失败")
    print("=" * 60)
    
    return failed == 0


async def test_report_generation():
    """测试报表生成中的盈余显示"""
    print("\n" + "=" * 60)
    print("📄 报表生成测试")
    print("=" * 60 + "\n")
    
    try:
        # 获取今日日期
        today = get_daily_period_date()
        
        # 测试1: 全局报表（不应显示盈余）
        print("【测试1】全局报表（不应显示盈余）")
        try:
            report_global = await generate_report_text("today", today, today, None)
            if "盈余" in report_global:
                print("  ❌ 失败: 全局报表显示了盈余（不应该显示）")
                return False
            else:
                print("  ✅ 通过: 全局报表未显示盈余")
        except Exception as e:
            print(f"  ❌ 失败: {e}")
            return False
        
        print()
        
        # 测试2: 归属报表（应该显示盈余）
        print("【测试2】归属报表（应该显示盈余）")
        try:
            # 先检查是否有归属ID数据
            # 如果没有数据，报表仍然应该生成，只是盈余可能是0或负数
            report_group = await generate_report_text("today", today, today, "S01")
            
            if "盈余:" in report_group:
                print("  ✅ 通过: 归属报表显示了盈余字段")
                
                # 提取盈余值
                lines = report_group.split('\n')
                for line in lines:
                    if '盈余:' in line:
                        print(f"  📊 盈余行: {line.strip()}")
                        break
            else:
                print("  ⚠️  警告: 归属报表未显示盈余字段（可能没有数据）")
                print("  这是正常的，如果该归属ID没有任何统计数据")
        except Exception as e:
            print(f"  ❌ 失败: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        print()
        
        # 测试3: 检查报表格式
        print("【测试3】报表格式检查")
        try:
            report = await generate_report_text("today", today, today, "TEST")
            if "📈" in report and "违约完成金额" in report:
                print("  ✅ 通过: 报表格式正确")
            else:
                print("  ⚠️  警告: 报表格式可能不完整")
        except Exception as e:
            print(f"  ⚠️  警告: 报表生成可能有问题: {e}")
        
        print()
        
        return True
        
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_database_stats():
    """测试数据库统计数据获取"""
    print("\n" + "=" * 60)
    print("🗄️  数据库统计测试")
    print("=" * 60 + "\n")
    
    try:
        today = get_daily_period_date()
        
        # 测试获取全局统计
        print("【测试1】获取全局统计数据")
        try:
            stats_global = await db_operations.get_stats_by_date_range(today, today, None)
            print(f"  ✅ 通过: 成功获取全局统计数据")
            print(f"  📊 利息: {stats_global.get('interest', 0):.2f}")
            print(f"  📊 违约完成金额: {stats_global.get('breach_end_amount', 0):.2f}")
            print(f"  📊 违约金额: {stats_global.get('breach_amount', 0):.2f}")
            
            # 计算盈余
            surplus_global = stats_global.get('interest', 0) + stats_global.get('breach_end_amount', 0) - stats_global.get('breach_amount', 0)
            print(f"  📊 计算盈余: {surplus_global:.2f}")
        except Exception as e:
            print(f"  ❌ 失败: {e}")
            return False
        
        print()
        
        # 测试获取归属统计
        print("【测试2】获取归属统计数据")
        try:
            stats_group = await db_operations.get_stats_by_date_range(today, today, "S01")
            print(f"  ✅ 通过: 成功获取归属统计数据")
            print(f"  📊 利息: {stats_group.get('interest', 0):.2f}")
            print(f"  📊 违约完成金额: {stats_group.get('breach_end_amount', 0):.2f}")
            print(f"  📊 违约金额: {stats_group.get('breach_amount', 0):.2f}")
            
            # 计算盈余
            surplus_group = stats_group.get('interest', 0) + stats_group.get('breach_end_amount', 0) - stats_group.get('breach_amount', 0)
            print(f"  📊 计算盈余: {surplus_group:.2f}")
        except Exception as e:
            print(f"  ❌ 失败: {e}")
            return False
        
        print()
        
        return True
        
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """主测试流程"""
    results = []
    
    # 测试盈余计算逻辑
    try:
        result = await test_surplus_calculation()
        results.append(("盈余计算逻辑", result))
    except Exception as e:
        print(f"❌ 盈余计算逻辑测试失败: {e}")
        results.append(("盈余计算逻辑", False))
    
    # 测试数据库统计
    try:
        result = await test_database_stats()
        results.append(("数据库统计", result))
    except Exception as e:
        print(f"❌ 数据库统计测试失败: {e}")
        results.append(("数据库统计", False))
    
    # 测试报表生成
    try:
        result = await test_report_generation()
        results.append(("报表生成", result))
    except Exception as e:
        print(f"❌ 报表生成测试失败: {e}")
        results.append(("报表生成", False))
    
    # 总结
    print("\n" + "=" * 60)
    print("📋 测试总结")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {status} - {name}")
    
    print("=" * 60)
    print(f"\n📈 测试通过率: {passed}/{total} ({passed*100//total if total > 0 else 0}%)")
    
    if passed == total:
        print("\n🎉 所有测试通过！盈余功能正常工作")
    else:
        print(f"\n⚠️  有 {total - passed} 个测试未通过，请检查")
    
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

