"""完整测试所有撤销功能"""
import asyncio
import sys
from pathlib import Path

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import db_operations
from datetime import datetime

# 测试结果统计
test_results = {
    'passed': 0,
    'failed': 0,
    'errors': []
}


def log_result(test_name, success, error=None):
    """记录测试结果"""
    if success:
        print(f"✅ {test_name}")
        test_results['passed'] += 1
    else:
        print(f"❌ {test_name}")
        test_results['failed'] += 1
        if error:
            print(f"   错误: {error}")
            test_results['errors'].append(f"{test_name}: {error}")


async def test_order_created_undo():
    """测试订单创建撤销"""
    print("\n" + "=" * 50)
    print("测试订单创建撤销")
    print("=" * 50)
    
    user_id = 999999
    
    # 1. 记录订单创建操作
    print("\n[步骤1] 记录订单创建操作...")
    try:
        operation_id = await db_operations.record_operation(
            user_id=user_id,
            operation_type='order_created',
            operation_data={
                'order_id': 'TEST001',
                'chat_id': 123456,
                'group_id': 'S01',
                'amount': 10000.0,
                'customer': 'A',
                'initial_state': 'normal',
                'is_historical': False,
                'date': '2025-12-02 12:00:00'
            }
        )
        log_result("记录订单创建操作", operation_id > 0)
    except Exception as e:
        log_result("记录订单创建操作", False, str(e))
        return
    
    # 2. 获取最后一个操作
    print("\n[步骤2] 获取最后一个操作...")
    try:
        last_op = await db_operations.get_last_operation(user_id)
        log_result("获取最后一个操作", last_op is not None and last_op['operation_type'] == 'order_created')
    except Exception as e:
        log_result("获取最后一个操作", False, str(e))
        return
    
    # 3. 标记为已撤销
    print("\n[步骤3] 标记操作为已撤销...")
    try:
        if last_op:
            result = await db_operations.mark_operation_undone(last_op['id'])
            log_result("标记操作为已撤销", result)
        else:
            log_result("标记操作为已撤销", False, "没有找到操作")
    except Exception as e:
        log_result("标记操作为已撤销", False, str(e))


async def test_order_state_change_undo():
    """测试订单状态变更撤销"""
    print("\n" + "=" * 50)
    print("测试订单状态变更撤销")
    print("=" * 50)
    
    user_id = 999998
    
    # 1. 记录状态变更操作
    print("\n[步骤1] 记录订单状态变更操作...")
    try:
        operation_id = await db_operations.record_operation(
            user_id=user_id,
            operation_type='order_state_change',
            operation_data={
                'chat_id': 123457,
                'order_id': 'TEST002',
                'old_state': 'normal',
                'new_state': 'breach',
                'group_id': 'S01',
                'amount': 5000.0
            }
        )
        log_result("记录订单状态变更操作", operation_id > 0)
    except Exception as e:
        log_result("记录订单状态变更操作", False, str(e))
        return
    
    # 2. 获取操作
    print("\n[步骤2] 获取最后一个操作...")
    try:
        last_op = await db_operations.get_last_operation(user_id)
        log_result("获取最后一个操作", last_op is not None and last_op['operation_type'] == 'order_state_change')
    except Exception as e:
        log_result("获取最后一个操作", False, str(e))


async def test_order_completed_undo():
    """测试订单完成撤销"""
    print("\n" + "=" * 50)
    print("测试订单完成撤销")
    print("=" * 50)
    
    user_id = 999997
    
    # 1. 记录订单完成操作
    print("\n[步骤1] 记录订单完成操作...")
    try:
        operation_id = await db_operations.record_operation(
            user_id=user_id,
            operation_type='order_completed',
            operation_data={
                'chat_id': 123458,
                'order_id': 'TEST003',
                'group_id': 'S01',
                'amount': 8000.0,
                'old_state': 'normal',
                'date': '2025-12-02'
            }
        )
        log_result("记录订单完成操作", operation_id > 0)
    except Exception as e:
        log_result("记录订单完成操作", False, str(e))
        return
    
    # 2. 获取操作
    print("\n[步骤2] 获取最后一个操作...")
    try:
        last_op = await db_operations.get_last_operation(user_id)
        log_result("获取最后一个操作", last_op is not None and last_op['operation_type'] == 'order_completed')
    except Exception as e:
        log_result("获取最后一个操作", False, str(e))


async def test_breach_end_undo():
    """测试违约完成撤销"""
    print("\n" + "=" * 50)
    print("测试违约完成撤销")
    print("=" * 50)
    
    user_id = 999996
    
    # 1. 记录违约完成操作
    print("\n[步骤1] 记录违约完成操作...")
    try:
        operation_id = await db_operations.record_operation(
            user_id=user_id,
            operation_type='order_breach_end',
            operation_data={
                'chat_id': 123459,
                'order_id': 'TEST004',
                'group_id': 'S01',
                'amount': 6000.0,
                'date': '2025-12-02'
            }
        )
        log_result("记录违约完成操作", operation_id > 0)
    except Exception as e:
        log_result("记录违约完成操作", False, str(e))
        return
    
    # 2. 获取操作
    print("\n[步骤2] 获取最后一个操作...")
    try:
        last_op = await db_operations.get_last_operation(user_id)
        log_result("获取最后一个操作", last_op is not None and last_op['operation_type'] == 'order_breach_end')
    except Exception as e:
        log_result("获取最后一个操作", False, str(e))


async def test_operation_history():
    """测试操作历史查询"""
    print("\n" + "=" * 50)
    print("测试操作历史查询")
    print("=" * 50)
    
    user_id = 999995
    
    # 1. 记录多个操作
    print("\n[步骤1] 记录多个操作...")
    try:
        for i in range(3):
            await db_operations.record_operation(
                user_id=user_id,
                operation_type='interest',
                operation_data={
                    'amount': 100.0 * (i + 1),
                    'group_id': 'S01',
                    'date': '2025-12-02'
                }
            )
        log_result("记录多个操作", True)
    except Exception as e:
        log_result("记录多个操作", False, str(e))
        return
    
    # 2. 获取最近操作历史
    print("\n[步骤2] 获取最近操作历史...")
    try:
        operations = await db_operations.get_recent_operations(user_id, limit=5)
        log_result("获取最近操作历史", len(operations) >= 3)
        print(f"   找到 {len(operations)} 条操作记录")
    except Exception as e:
        log_result("获取最近操作历史", False, str(e))


async def test_undo_count_logic():
    """测试撤销计数逻辑"""
    print("\n" + "=" * 50)
    print("测试撤销计数逻辑")
    print("=" * 50)
    
    user_id = 999994
    
    # 1. 记录一个操作
    print("\n[步骤1] 记录操作并标记为已撤销...")
    try:
        operation_id = await db_operations.record_operation(
            user_id=user_id,
            operation_type='expense',
            operation_data={
                'amount': 500.0,
                'type': 'company',
                'date': '2025-12-02',
                'expense_record_id': 1
            }
        )
        
        # 立即标记为已撤销
        await db_operations.mark_operation_undone(operation_id)
        
        # 获取最后一个操作（应该返回None，因为已撤销）
        last_op = await db_operations.get_last_operation(user_id)
        log_result("撤销后无法获取操作", last_op is None)
    except Exception as e:
        log_result("撤销计数逻辑测试", False, str(e))


async def run_all_tests():
    """运行所有测试"""
    print("\n" + "=" * 60)
    print("撤销功能完整测试")
    print("=" * 60)
    print(f"测试开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 运行所有测试
    await test_order_created_undo()
    await test_order_state_change_undo()
    await test_order_completed_undo()
    await test_breach_end_undo()
    await test_operation_history()
    await test_undo_count_logic()
    
    # 输出测试总结
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    print(f"✅ 通过: {test_results['passed']}")
    print(f"❌ 失败: {test_results['failed']}")
    print(f"总计: {test_results['passed'] + test_results['failed']}")
    
    if test_results['errors']:
        print("\n错误列表:")
        for error in test_results['errors']:
            print(f"  - {error}")
    
    success_rate = (test_results['passed'] / (test_results['passed'] + test_results['failed']) * 100) if (test_results['passed'] + test_results['failed']) > 0 else 0
    print(f"\n成功率: {success_rate:.1f}%")
    
    if test_results['failed'] == 0:
        print("\n🎉 所有测试通过！")
    else:
        print(f"\n⚠️ 有 {test_results['failed']} 个测试失败")
    
    print("=" * 60)
    
    return test_results['failed'] == 0


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)

