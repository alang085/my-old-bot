#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接修改运行服务上的报表数据工具

功能：
1. 修改全局财务数据 (financial_data)
2. 修改分组财务数据 (grouped_data)
3. 修改日结数据 (daily_data)

使用方法：
    python scripts/modify_report_data.py --type financial --field liquid_funds --value 100000
    python scripts/modify_report_data.py --type grouped --group_id S01 --field interest --value 5000
    python scripts/modify_report_data.py --type daily --date 2025-01-15 --field interest --value 1000
"""

import argparse
import os
import sqlite3
import sys
from typing import Optional

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 数据库文件路径
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_NAME = os.path.join(DATA_DIR, "loan_bot.db")


def get_connection():
    """获取数据库连接"""
    if not os.path.exists(DB_NAME):
        print(f"❌ 数据库文件不存在: {DB_NAME}")
        print("   请检查 DATA_DIR 环境变量或数据库文件路径")
        sys.exit(1)

    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def show_current_data(
    conn: sqlite3.Connection,
    table_type: str,
    group_id: Optional[str] = None,
    date: Optional[str] = None,
):
    """显示当前数据"""
    cursor = conn.cursor()

    if table_type == "financial":
        cursor.execute("SELECT * FROM financial_data ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            print("\n📊 当前全局财务数据:")
            print(f"  有效订单数: {row['valid_orders']}")
            print(f"  有效订单金额: {row['valid_amount']:.2f}")
            print(f"  活动资金: {row['liquid_funds']:.2f}")
            print(f"  新客户数: {row['new_clients']}")
            print(f"  新客户金额: {row['new_clients_amount']:.2f}")
            print(f"  老客户数: {row['old_clients']}")
            print(f"  老客户金额: {row['old_clients_amount']:.2f}")
            print(f"  利息收入: {row['interest']:.2f}")
            print(f"  完成订单数: {row['completed_orders']}")
            print(f"  完成订单金额: {row['completed_amount']:.2f}")
            print(f"  违约订单数: {row['breach_orders']}")
            print(f"  违约金额: {row['breach_amount']:.2f}")
            print(f"  违约完成订单数: {row['breach_end_orders']}")
            print(f"  违约完成金额: {row['breach_end_amount']:.2f}")
        else:
            print("❌ 未找到全局财务数据")

    elif table_type == "grouped":
        if not group_id:
            print("❌ 修改分组数据需要指定 --group_id")
            return

        cursor.execute("SELECT * FROM grouped_data WHERE group_id = ?", (group_id,))
        row = cursor.fetchone()
        if row:
            print(f"\n📊 当前归属ID {group_id} 的财务数据:")
            print(f"  有效订单数: {row['valid_orders']}")
            print(f"  有效订单金额: {row['valid_amount']:.2f}")
            print(f"  活动资金: {row['liquid_funds']:.2f}")
            print(f"  新客户数: {row['new_clients']}")
            print(f"  新客户金额: {row['new_clients_amount']:.2f}")
            print(f"  老客户数: {row['old_clients']}")
            print(f"  老客户金额: {row['old_clients_amount']:.2f}")
            print(f"  利息收入: {row['interest']:.2f}")
            print(f"  完成订单数: {row['completed_orders']}")
            print(f"  完成订单金额: {row['completed_amount']:.2f}")
            print(f"  违约订单数: {row['breach_orders']}")
            print(f"  违约金额: {row['breach_amount']:.2f}")
            print(f"  违约完成订单数: {row['breach_end_orders']}")
            print(f"  违约完成金额: {row['breach_end_amount']:.2f}")
        else:
            print(f"❌ 未找到归属ID {group_id} 的数据")

    elif table_type == "daily":
        if not date:
            print("❌ 修改日结数据需要指定 --date")
            return

        if group_id:
            cursor.execute(
                "SELECT * FROM daily_data WHERE date = ? AND group_id = ?", (date, group_id)
            )
        else:
            cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL", (date,))

        row = cursor.fetchone()
        if row:
            group_info = f"归属ID {group_id}" if group_id else "全局"
            print(f"\n📊 当前 {date} {group_info} 的日结数据:")
            print(f"  新客户数: {row['new_clients']}")
            print(f"  新客户金额: {row['new_clients_amount']:.2f}")
            print(f"  老客户数: {row['old_clients']}")
            print(f"  老客户金额: {row['old_clients_amount']:.2f}")
            print(f"  利息收入: {row['interest']:.2f}")
            print(f"  完成订单数: {row['completed_orders']}")
            print(f"  完成订单金额: {row['completed_amount']:.2f}")
            print(f"  违约订单数: {row['breach_orders']}")
            print(f"  违约金额: {row['breach_amount']:.2f}")
            print(f"  违约完成订单数: {row['breach_end_orders']}")
            print(f"  违约完成金额: {row['breach_end_amount']:.2f}")
            print(f"  资金流量: {row['liquid_flow']:.2f}")
            print(f"  公司开销: {row['company_expenses']:.2f}")
            print(f"  其他开销: {row['other_expenses']:.2f}")
        else:
            print(f"❌ 未找到 {date} 的数据")


def modify_financial_data(conn: sqlite3.Connection, field: str, value: float, mode: str = "set"):
    """修改全局财务数据"""
    cursor = conn.cursor()

    # 获取当前值
    cursor.execute("SELECT * FROM financial_data ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()

    if not row:
        # 如果不存在，创建新记录
        cursor.execute(
            """
        INSERT INTO financial_data (
            valid_orders, valid_amount, liquid_funds,
            new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount
        ) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        """
        )
        current_value = 0
    else:
        current_value = row[field] if field in row.keys() else 0

    # 计算新值
    if mode == "set":
        new_value = value
    elif mode == "add":
        new_value = current_value + value
    elif mode == "subtract":
        new_value = current_value - value
    else:
        print(f"❌ 不支持的模式: {mode}")
        return False

    # 更新数据
    cursor.execute(
        f"""
    UPDATE financial_data 
    SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = (SELECT id FROM financial_data ORDER BY id DESC LIMIT 1)
    """,
        (new_value,),
    )

    conn.commit()
    print(f"✅ 已更新全局财务数据: {field}")
    print(f"   旧值: {current_value:.2f}")
    print(f"   新值: {new_value:.2f}")
    print(f"   变化: {new_value - current_value:+.2f}")

    return True


def modify_grouped_data(
    conn: sqlite3.Connection, group_id: str, field: str, value: float, mode: str = "set"
):
    """修改分组财务数据"""
    cursor = conn.cursor()

    # 获取当前值
    cursor.execute("SELECT * FROM grouped_data WHERE group_id = ?", (group_id,))
    row = cursor.fetchone()

    if not row:
        # 如果不存在，创建新记录
        cursor.execute(
            """
        INSERT INTO grouped_data (
            group_id, valid_orders, valid_amount, liquid_funds,
            new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount
        ) VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        """,
            (group_id,),
        )
        current_value = 0
    else:
        current_value = row[field] if field in row.keys() else 0

    # 计算新值
    if mode == "set":
        new_value = value
    elif mode == "add":
        new_value = current_value + value
    elif mode == "subtract":
        new_value = current_value - value
    else:
        print(f"❌ 不支持的模式: {mode}")
        return False

    # 更新数据
    cursor.execute(
        f"""
    UPDATE grouped_data 
    SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
    WHERE group_id = ?
    """,
        (new_value, group_id),
    )

    conn.commit()
    print(f"✅ 已更新归属ID {group_id} 的财务数据: {field}")
    print(f"   旧值: {current_value:.2f}")
    print(f"   新值: {new_value:.2f}")
    print(f"   变化: {new_value - current_value:+.2f}")

    return True


def modify_daily_data(
    conn: sqlite3.Connection,
    date: str,
    field: str,
    value: float,
    group_id: Optional[str] = None,
    mode: str = "set",
):
    """修改日结数据"""
    cursor = conn.cursor()

    # 获取当前值
    if group_id:
        cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id = ?", (date, group_id))
    else:
        cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL", (date,))

    row = cursor.fetchone()

    if not row:
        # 如果不存在，创建新记录
        cursor.execute(
            """
        INSERT INTO daily_data (
            date, group_id, new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount,
            liquid_flow, company_expenses, other_expenses
        ) VALUES (?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        """,
            (date, group_id),
        )
        current_value = 0
    else:
        current_value = row[field] if field in row.keys() else 0

    # 计算新值
    if mode == "set":
        new_value = value
    elif mode == "add":
        new_value = current_value + value
    elif mode == "subtract":
        new_value = current_value - value
    else:
        print(f"❌ 不支持的模式: {mode}")
        return False

    # 更新数据
    if group_id:
        cursor.execute(
            f"""
        UPDATE daily_data 
        SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
        WHERE date = ? AND group_id = ?
        """,
            (new_value, date, group_id),
        )
    else:
        cursor.execute(
            f"""
        UPDATE daily_data 
        SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
        WHERE date = ? AND group_id IS NULL
        """,
            (new_value, date),
        )

    conn.commit()
    group_info = f"归属ID {group_id}" if group_id else "全局"
    print(f"✅ 已更新 {date} {group_info} 的日结数据: {field}")
    print(f"   旧值: {current_value:.2f}")
    print(f"   新值: {new_value:.2f}")
    print(f"   变化: {new_value - current_value:+.2f}")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="直接修改运行服务上的报表数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 设置全局活动资金为 100000
  python scripts/modify_report_data.py --type financial --field liquid_funds --value 100000 --mode set

  # 增加归属ID S01 的利息收入 5000
  python scripts/modify_report_data.py --type grouped --group_id S01 --field interest --value 5000 --mode add

  # 设置 2025-01-15 的全局利息收入为 1000
  python scripts/modify_report_data.py --type daily --date 2025-01-15 --field interest --value 1000 --mode set

  # 查看当前数据
  python scripts/modify_report_data.py --type financial --show
  python scripts/modify_report_data.py --type grouped --group_id S01 --show
  python scripts/modify_report_data.py --type daily --date 2025-01-15 --show
        """,
    )

    parser.add_argument(
        "--type",
        required=True,
        choices=["financial", "grouped", "daily"],
        help="数据类型: financial(全局财务), grouped(分组财务), daily(日结数据)",
    )
    parser.add_argument("--field", help="要修改的字段名")
    parser.add_argument("--value", type=float, help="新值或增量值")
    parser.add_argument(
        "--mode",
        choices=["set", "add", "subtract"],
        default="set",
        help="修改模式: set(设置), add(增加), subtract(减少)",
    )
    parser.add_argument("--group_id", help="归属ID (用于grouped和daily类型)")
    parser.add_argument("--date", help="日期 (用于daily类型, 格式: YYYY-MM-DD)")
    parser.add_argument("--show", action="store_true", help="仅显示当前数据，不修改")
    parser.add_argument("--db_path", help="数据库文件路径 (可选，默认使用环境变量DATA_DIR)")

    args = parser.parse_args()

    # 如果指定了数据库路径，使用指定的路径
    global DB_NAME
    if args.db_path:
        DB_NAME = args.db_path

    # 连接数据库
    conn = get_connection()

    try:
        # 如果只是查看数据
        if args.show:
            show_current_data(conn, args.type, args.group_id, args.date)
            return

        # 验证必需参数
        if not args.field:
            print("❌ 必须指定 --field 参数")
            return

        if args.value is None:
            print("❌ 必须指定 --value 参数")
            return

        # 根据类型执行修改
        if args.type == "financial":
            if args.group_id or args.date:
                print("⚠️  警告: financial 类型不需要 --group_id 和 --date 参数")
            modify_financial_data(conn, args.field, args.value, args.mode)
            # 显示修改后的数据
            show_current_data(conn, args.type)

        elif args.type == "grouped":
            if not args.group_id:
                print("❌ grouped 类型必须指定 --group_id 参数")
                return
            if args.date:
                print("⚠️  警告: grouped 类型不需要 --date 参数")
            modify_grouped_data(conn, args.group_id, args.field, args.value, args.mode)
            # 显示修改后的数据
            show_current_data(conn, args.type, args.group_id)

        elif args.type == "daily":
            if not args.date:
                print("❌ daily 类型必须指定 --date 参数")
                return
            modify_daily_data(conn, args.date, args.field, args.value, args.group_id, args.mode)
            # 显示修改后的数据
            show_current_data(conn, args.type, args.group_id, args.date)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
