"""测试撤销功能"""
import asyncio
import sys
from pathlib import Path

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import db_operations


async def test_undo_functions():
    """测试撤销相关的数据库函数"""
    print("=" * 50)
    print("测试撤销功能")
    print("=" * 50)

    # 测试1: 记录操作
    print("\n[测试1] 记录操作历史...")
    try:
        operation_id = await db_operations.record_operation(
            user_id=123456,
            operation_type='interest',
            operation_data={
                'amount': 1000.0,
                'group_id': 'S01',
                'date': '2025-12-02'
            }
        )
        print(f"✅ 操作记录成功，操作ID: {operation_id}")
    except Exception as e:
        print(f"❌ 操作记录失败: {e}")
        return

    # 测试2: 获取最后一个操作
    print("\n[测试2] 获取最后一个操作...")
    try:
        last_op = await db_operations.get_last_operation(123456)
        if last_op:
            print(f"✅ 获取成功:")
            print(f"   - 操作ID: {last_op['id']}")
            print(f"   - 操作类型: {last_op['operation_type']}")
            print(f"   - 操作数据: {last_op['operation_data']}")
        else:
            print("❌ 没有找到操作记录")
    except Exception as e:
        print(f"❌ 获取失败: {e}")

    # 测试3: 标记操作为已撤销
    print("\n[测试3] 标记操作为已撤销...")
    try:
        if last_op:
            result = await db_operations.mark_operation_undone(last_op['id'])
            if result:
                print(f"✅ 标记成功")
            else:
                print("❌ 标记失败")
        else:
            print("⚠️ 跳过（没有操作记录）")
    except Exception as e:
        print(f"❌ 标记失败: {e}")

    # 测试4: 再次获取最后一个操作（应该返回None，因为已经撤销）
    print("\n[测试4] 再次获取最后一个操作（应该返回None）...")
    try:
        last_op2 = await db_operations.get_last_operation(123456)
        if last_op2:
            print(f"⚠️ 仍然找到操作记录: {last_op2['id']}")
        else:
            print("✅ 正确：没有找到未撤销的操作")
    except Exception as e:
        print(f"❌ 获取失败: {e}")

    # 测试5: 获取最近操作历史
    print("\n[测试5] 获取最近操作历史...")
    try:
        operations = await db_operations.get_recent_operations(123456, limit=5)
        print(f"✅ 获取成功，共 {len(operations)} 条记录")
        for op in operations:
            status = "已撤销" if op['is_undone'] else "未撤销"
            print(f"   - ID: {op['id']}, 类型: {op['operation_type']}, 状态: {status}")
    except Exception as e:
        print(f"❌ 获取失败: {e}")

    print("\n" + "=" * 50)
    print("测试完成")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(test_undo_functions())

