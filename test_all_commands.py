"""测试所有命令功能 - 完整测试脚本"""
import sys
import asyncio
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
project_root = Path(__file__).parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 测试结果记录
test_results = []


def log_test(name, result, details=""):
    """记录测试结果"""
    status = "✅ 通过" if result else "❌ 失败"
    test_results.append({
        'name': name,
        'status': status,
        'result': result,
        'details': details,
        'time': datetime.now().strftime("%H:%M:%S")
    })
    print(f"   {status} - {name}")
    if details:
        print(f"      {details}")


async def test_command_imports():
    """测试命令处理器导入"""
    print("\n" + "=" * 60)
    print("🔍 测试命令处理器导入")
    print("=" * 60)
    
    commands_to_test = [
        # 基础命令
        ('start', 'handlers.command_handlers', 'start'),
        ('create_order', 'handlers.command_handlers', 'create_order'),
        ('show_current_order', 'handlers.command_handlers', 'show_current_order'),
        ('adjust_funds', 'handlers.command_handlers', 'adjust_funds'),
        ('create_attribution', 'handlers.command_handlers', 'create_attribution'),
        ('list_attributions', 'handlers.command_handlers', 'list_attributions'),
        
        # 员工管理
        ('add_employee', 'handlers.command_handlers', 'add_employee'),
        ('remove_employee', 'handlers.command_handlers', 'remove_employee'),
        ('list_employees', 'handlers.command_handlers', 'list_employees'),
        
        # 订单状态
        ('set_normal', 'handlers.order_handlers', 'set_normal'),
        ('set_overdue', 'handlers.order_handlers', 'set_overdue'),
        ('set_end', 'handlers.order_handlers', 'set_end'),
        ('set_breach', 'handlers.order_handlers', 'set_breach'),
        ('set_breach_end', 'handlers.order_handlers', 'set_breach_end'),
        
        # 报表
        ('show_report', 'handlers.report_handlers', 'show_report'),
        ('show_my_report', 'handlers.report_handlers', 'show_my_report'),
        
        # 收入明细
        ('show_income_detail', 'handlers.income_handlers', 'show_income_detail'),
        
        # 搜索
        ('search_orders', 'handlers.search_handlers', 'search_orders'),
        
        # 支付账户
        ('show_gcash', 'handlers.payment_handlers', 'show_gcash'),
        ('show_paymaya', 'handlers.payment_handlers', 'show_paymaya'),
        ('show_all_accounts', 'handlers.payment_handlers', 'show_all_accounts'),
        
        # 定时播报
        ('show_schedule_menu', 'handlers.schedule_handlers', 'show_schedule_menu'),
        
        # 播报
        ('broadcast_payment', 'handlers.broadcast_handlers', 'broadcast_payment'),
    ]
    
    all_passed = True
    for name, module_path, func_name in commands_to_test:
        try:
            module = __import__(module_path, fromlist=[func_name])
            func = getattr(module, func_name)
            log_test(f"导入 {name}", True, f"模块: {module_path}")
        except Exception as e:
            log_test(f"导入 {name}", False, f"错误: {e}")
            all_passed = False
    
    return all_passed


async def test_database_operations():
    """测试数据库操作"""
    print("\n" + "=" * 60)
    print("🔍 测试数据库操作")
    print("=" * 60)
    
    import db_operations
    from utils.date_helpers import get_daily_period_date
    
    tests = [
        ("获取财务数据", lambda: db_operations.get_financial_data()),
        ("获取所有归属ID", lambda: db_operations.get_all_group_ids()),
        ("获取日结数据", lambda: db_operations.get_daily_data(get_daily_period_date())),
        ("获取收入记录", lambda: db_operations.get_income_records(get_daily_period_date(), get_daily_period_date())),
        ("获取开销记录", lambda: db_operations.get_expense_records(get_daily_period_date(), get_daily_period_date())),
    ]
    
    all_passed = True
    for name, test_func in tests:
        try:
            result = await test_func()
            if result is not None:
                log_test(name, True, f"返回类型: {type(result).__name__}")
            else:
                log_test(name, False, "返回 None")
                all_passed = False
        except Exception as e:
            log_test(name, False, f"错误: {str(e)[:50]}")
            all_passed = False
    
    return all_passed


async def test_command_handlers():
    """测试命令处理器函数签名"""
    print("\n" + "=" * 60)
    print("🔍 测试命令处理器函数")
    print("=" * 60)
    
    from handlers import (
        start, create_order, show_current_order, adjust_funds,
        create_attribution, list_attributions,
        add_employee, remove_employee, list_employees,
        set_normal, set_overdue, set_end, set_breach, set_breach_end,
        show_report, show_my_report,
        search_orders,
        show_gcash, show_paymaya, show_all_accounts,
        show_schedule_menu,
        broadcast_payment
    )
    
    commands = [
        ("start", start),
        ("create_order", create_order),
        ("show_current_order", show_current_order),
        ("adjust_funds", adjust_funds),
        ("create_attribution", create_attribution),
        ("list_attributions", list_attributions),
        ("add_employee", add_employee),
        ("remove_employee", remove_employee),
        ("list_employees", list_employees),
        ("set_normal", set_normal),
        ("set_overdue", set_overdue),
        ("set_end", set_end),
        ("set_breach", set_breach),
        ("set_breach_end", set_breach_end),
        ("show_report", show_report),
        ("show_my_report", show_my_report),
        ("search_orders", search_orders),
        ("show_gcash", show_gcash),
        ("show_paymaya", show_paymaya),
        ("show_all_accounts", show_all_accounts),
        ("show_schedule_menu", show_schedule_menu),
        ("broadcast_payment", broadcast_payment),
    ]
    
    all_passed = True
    for name, func in commands:
        try:
            # 检查函数是否存在且可调用
            if callable(func):
                # 检查函数签名
                import inspect
                sig = inspect.signature(func)
                params = list(sig.parameters.keys())
                log_test(f"{name} 函数", True, f"参数: {', '.join(params[:2])}")
            else:
                log_test(f"{name} 函数", False, "不是可调用函数")
                all_passed = False
        except Exception as e:
            log_test(f"{name} 函数", False, f"错误: {str(e)[:50]}")
            all_passed = False
    
    return all_passed


async def test_callbacks():
    """测试回调处理器"""
    print("\n" + "=" * 60)
    print("🔍 测试回调处理器")
    print("=" * 60)
    
    from callbacks import (
        button_callback,
        handle_report_callback,
        handle_search_callback,
        handle_order_action_callback,
        handle_payment_callback,
        handle_schedule_callback
    )
    
    callbacks = [
        ("button_callback", button_callback),
        ("handle_report_callback", handle_report_callback),
        ("handle_search_callback", handle_search_callback),
        ("handle_order_action_callback", handle_order_action_callback),
        ("handle_payment_callback", handle_payment_callback),
        ("handle_schedule_callback", handle_schedule_callback),
    ]
    
    all_passed = True
    for name, func in callbacks:
        try:
            if callable(func):
                log_test(f"{name} 回调", True)
            else:
                log_test(f"{name} 回调", False, "不是可调用函数")
                all_passed = False
        except Exception as e:
            log_test(f"{name} 回调", False, f"错误: {str(e)[:50]}")
            all_passed = False
    
    return all_passed


async def test_utils_functions():
    """测试工具函数"""
    print("\n" + "=" * 60)
    print("🔍 测试工具函数")
    print("=" * 60)
    
    tests = [
        ("get_daily_period_date", "utils.date_helpers"),
        ("update_liquid_capital", "utils.stats_helpers"),
        ("update_all_stats", "utils.stats_helpers"),
        ("is_group_chat", "utils.chat_helpers"),
        ("parse_order_from_title", "utils.order_helpers"),
    ]
    
    all_passed = True
    for func_name, module_path in tests:
        try:
            module = __import__(module_path, fromlist=[func_name])
            func = getattr(module, func_name)
            if callable(func):
                log_test(func_name, True, f"模块: {module_path}")
            else:
                log_test(func_name, False, "不是函数")
                all_passed = False
        except Exception as e:
            log_test(func_name, False, f"错误: {str(e)[:50]}")
            all_passed = False
    
    return all_passed


async def generate_test_report():
    """生成测试报告"""
    print("\n" + "=" * 60)
    print("📊 测试结果总结")
    print("=" * 60)
    
    total = len(test_results)
    passed = sum(1 for r in test_results if r['result'])
    failed = total - passed
    
    print(f"\n总测试数: {total}")
    print(f"✅ 通过: {passed}")
    print(f"❌ 失败: {failed}")
    print(f"📈 通过率: {passed*100//total if total > 0 else 0}%")
    
    if failed > 0:
        print("\n❌ 失败的测试:")
        for result in test_results:
            if not result['result']:
                print(f"   - {result['name']}: {result['details']}")
    
    print("\n" + "=" * 60)
    
    # 保存测试报告
    report_file = "命令测试报告.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# 命令测试报告\n\n")
        f.write(f"**测试时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"## 测试结果统计\n\n")
        f.write(f"- 总测试数: {total}\n")
        f.write(f"- ✅ 通过: {passed}\n")
        f.write(f"- ❌ 失败: {failed}\n")
        f.write(f"- 📈 通过率: {passed*100//total if total > 0 else 0}%\n\n")
        f.write("## 详细测试结果\n\n")
        
        # 按类别分组
        categories = {}
        for result in test_results:
            category = result['name'].split()[0] if ' ' in result['name'] else '其他'
            if category not in categories:
                categories[category] = []
            categories[category].append(result)
        
        for category, results in categories.items():
            f.write(f"### {category}\n\n")
            for result in results:
                status_icon = "✅" if result['result'] else "❌"
                f.write(f"{status_icon} **{result['name']}**\n")
                if result['details']:
                    f.write(f"   - {result['details']}\n")
                f.write(f"   - 测试时间: {result['time']}\n\n")
        
        f.write("## 测试结论\n\n")
        if passed == total:
            f.write("🎉 **所有测试通过！**\n")
        else:
            f.write(f"⚠️ **有 {failed} 个测试未通过，请检查。**\n")
    
    print(f"\n📝 测试报告已保存到: {report_file}")


async def main():
    """主测试流程"""
    print("=" * 60)
    print("🧪 完整命令功能测试")
    print("=" * 60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 运行所有测试
    results = []
    
    try:
        result = await test_command_imports()
        results.append(("命令导入", result))
    except Exception as e:
        print(f"❌ 命令导入测试失败: {e}")
        results.append(("命令导入", False))
    
    try:
        import db_operations
        from utils.date_helpers import get_daily_period_date
        result = await test_database_operations()
        results.append(("数据库操作", result))
    except Exception as e:
        print(f"❌ 数据库操作测试失败: {e}")
        results.append(("数据库操作", False))
    
    try:
        result = await test_command_handlers()
        results.append(("命令处理器", result))
    except Exception as e:
        print(f"❌ 命令处理器测试失败: {e}")
        results.append(("命令处理器", False))
    
    try:
        result = await test_callbacks()
        results.append(("回调处理器", result))
    except Exception as e:
        print(f"❌ 回调处理器测试失败: {e}")
        results.append(("回调处理器", False))
    
    try:
        result = await test_utils_functions()
        results.append(("工具函数", result))
    except Exception as e:
        print(f"❌ 工具函数测试失败: {e}")
        results.append(("工具函数", False))
    
    # 生成报告
    await generate_test_report()
    
    print("\n" + "=" * 60)
    print("📋 测试完成")
    print("=" * 60)
    
    all_passed = all(r[1] for r in results)
    if all_passed:
        print("\n🎉 所有测试类别通过！")
    else:
        print("\n⚠️ 部分测试类别未通过，请查看详细报告")


if __name__ == "__main__":
    asyncio.run(main())

