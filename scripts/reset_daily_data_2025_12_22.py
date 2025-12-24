"""
一次性脚本：将2025-12-22的统计数据（除了延续性数据）全部归零

延续性数据（保留）：
- valid_orders (有效订单数)
- valid_amount (有效金额)
- liquid_funds (资金余额)

需要归零的数据：
1. financial_data 表：除了延续性数据外的所有字段
2. grouped_data 表：除了延续性数据外的所有字段
3. daily_data 表：2025-12-22的所有数据
4. income_records 表：2025-12-22的记录（标记为已撤销）
5. expense_records 表：2025-12-22的记录（删除）

执行方式：
    python scripts/reset_daily_data_2025_12_22.py
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import sqlite3

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 数据库路径
DATA_DIR = os.getenv("DATA_DIR", project_root)
DB_NAME = os.path.join(DATA_DIR, "loan_bot.db")

# 目标日期
TARGET_DATE = "2025-12-22"


def get_db_connection():
    """获取数据库连接"""
    return sqlite3.connect(DB_NAME)


def reset_financial_data(conn):
    """重置 financial_data 表（保留延续性数据）"""
    cursor = conn.cursor()

    logger.info("开始重置 financial_data 表...")

    # 获取当前延续性数据
    cursor.execute(
        "SELECT valid_orders, valid_amount, liquid_funds FROM financial_data ORDER BY id DESC LIMIT 1"
    )
    row = cursor.fetchone()

    if row:
        valid_orders, valid_amount, liquid_funds = row
        logger.info(
            f"保留延续性数据: valid_orders={valid_orders}, valid_amount={valid_amount}, liquid_funds={liquid_funds}"
        )
    else:
        # 如果没有数据，创建默认值
        valid_orders = 0
        valid_amount = 0
        liquid_funds = 0
        logger.info("未找到现有数据，使用默认值")

    # 更新 financial_data，只保留延续性数据，其他字段归零
    cursor.execute(
        """
    UPDATE financial_data 
    SET 
        new_clients = 0,
        new_clients_amount = 0,
        old_clients = 0,
        old_clients_amount = 0,
        interest = 0,
        completed_orders = 0,
        completed_amount = 0,
        breach_orders = 0,
        breach_amount = 0,
        breach_end_orders = 0,
        breach_end_amount = 0,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = (SELECT id FROM financial_data ORDER BY id DESC LIMIT 1)
    """
    )

    # 如果没有记录，创建一条
    cursor.execute("SELECT COUNT(*) FROM financial_data")
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            """
        INSERT INTO financial_data (
            valid_orders, valid_amount, liquid_funds,
            new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount
        ) VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        """,
            (valid_orders, valid_amount, liquid_funds),
        )

    logger.info("✅ financial_data 表重置完成")


def reset_grouped_data(conn):
    """重置 grouped_data 表（保留延续性数据）"""
    cursor = conn.cursor()

    logger.info("开始重置 grouped_data 表...")

    # 获取所有分组ID
    cursor.execute("SELECT group_id FROM grouped_data")
    group_ids = [row[0] for row in cursor.fetchall()]

    logger.info(f"找到 {len(group_ids)} 个分组")

    # 更新每个分组的数据，只保留延续性数据，其他字段归零
    for group_id in group_ids:
        cursor.execute(
            """
        UPDATE grouped_data 
        SET 
            new_clients = 0,
            new_clients_amount = 0,
            old_clients = 0,
            old_clients_amount = 0,
            interest = 0,
            completed_orders = 0,
            completed_amount = 0,
            breach_orders = 0,
            breach_amount = 0,
            breach_end_orders = 0,
            breach_end_amount = 0,
            updated_at = CURRENT_TIMESTAMP
        WHERE group_id = ?
        """,
            (group_id,),
        )
        logger.info(f"  重置分组 {group_id}")

    logger.info("✅ grouped_data 表重置完成")


def reset_daily_data(conn, date):
    """重置 daily_data 表中指定日期的所有数据"""
    cursor = conn.cursor()

    logger.info(f"开始重置 daily_data 表中 {date} 的数据...")

    # 删除指定日期的所有 daily_data 记录
    cursor.execute("DELETE FROM daily_data WHERE date = ?", (date,))
    deleted_count = cursor.rowcount

    logger.info(f"  删除了 {deleted_count} 条 daily_data 记录")
    logger.info("✅ daily_data 表重置完成")


def reset_income_records(conn, date):
    """将指定日期的收入记录标记为已撤销"""
    cursor = conn.cursor()

    logger.info(f"开始处理 income_records 表中 {date} 的记录...")

    # 检查是否有该日期的收入记录
    cursor.execute("SELECT COUNT(*) FROM income_records WHERE date = ?", (date,))
    count = cursor.fetchone()[0]

    if count > 0:
        # 标记为已撤销
        cursor.execute(
            """
        UPDATE income_records 
        SET is_undone = 1, updated_at = CURRENT_TIMESTAMP
        WHERE date = ? AND (is_undone IS NULL OR is_undone = 0)
        """,
            (date,),
        )
        updated_count = cursor.rowcount
        logger.info(f"  标记了 {updated_count} 条收入记录为已撤销")
    else:
        logger.info("  未找到该日期的收入记录")

    logger.info("✅ income_records 表处理完成")


def reset_expense_records(conn, date):
    """删除指定日期的支出记录"""
    cursor = conn.cursor()

    logger.info(f"开始删除 expense_records 表中 {date} 的记录...")

    # 删除指定日期的支出记录
    cursor.execute("DELETE FROM expense_records WHERE date = ?", (date,))
    deleted_count = cursor.rowcount

    logger.info(f"  删除了 {deleted_count} 条支出记录")
    logger.info("✅ expense_records 表处理完成")


def generate_report(conn):
    """生成重置后的数据报告"""
    cursor = conn.cursor()

    logger.info("\n" + "=" * 60)
    logger.info("生成重置后的数据报告...")
    logger.info("=" * 60)

    # 1. financial_data
    cursor.execute("SELECT * FROM financial_data ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        row_dict = dict(zip([col[0] for col in cursor.description], row))
        logger.info("\n【financial_data】")
        logger.info(f"  有效订单数: {row_dict.get('valid_orders', 0)}")
        logger.info(f"  有效金额: {row_dict.get('valid_amount', 0):.2f}")
        logger.info(f"  流动资金: {row_dict.get('liquid_funds', 0):.2f}")
        logger.info(f"  新客户数: {row_dict.get('new_clients', 0)}")
        logger.info(f"  新客户金额: {row_dict.get('new_clients_amount', 0):.2f}")
        logger.info(f"  老客户数: {row_dict.get('old_clients', 0)}")
        logger.info(f"  老客户金额: {row_dict.get('old_clients_amount', 0):.2f}")
        logger.info(f"  利息: {row_dict.get('interest', 0):.2f}")
        logger.info(f"  完成订单数: {row_dict.get('completed_orders', 0)}")
        logger.info(f"  完成金额: {row_dict.get('completed_amount', 0):.2f}")
        logger.info(f"  违约订单数: {row_dict.get('breach_orders', 0)}")
        logger.info(f"  违约金额: {row_dict.get('breach_amount', 0):.2f}")
        logger.info(f"  违约完成订单数: {row_dict.get('breach_end_orders', 0)}")
        logger.info(f"  违约完成金额: {row_dict.get('breach_end_amount', 0):.2f}")

    # 2. grouped_data 统计
    cursor.execute("SELECT COUNT(*) FROM grouped_data")
    group_count = cursor.fetchone()[0]
    logger.info("\n【grouped_data】")
    logger.info(f"  分组数量: {group_count}")

    # 3. daily_data 统计
    cursor.execute("SELECT COUNT(*) FROM daily_data WHERE date = ?", (TARGET_DATE,))
    daily_count = cursor.fetchone()[0]
    logger.info("\n【daily_data】")
    logger.info(f"  {TARGET_DATE} 的记录数: {daily_count}")

    # 4. income_records 统计
    cursor.execute("SELECT COUNT(*) FROM income_records WHERE date = ?", (TARGET_DATE,))
    income_count = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM income_records WHERE date = ? AND is_undone = 1", (TARGET_DATE,)
    )
    income_undone_count = cursor.fetchone()[0]
    logger.info("\n【income_records】")
    logger.info(f"  {TARGET_DATE} 的记录数: {income_count}")
    logger.info(f"  已撤销记录数: {income_undone_count}")

    # 5. expense_records 统计
    cursor.execute("SELECT COUNT(*) FROM expense_records WHERE date = ?", (TARGET_DATE,))
    expense_count = cursor.fetchone()[0]
    logger.info("\n【expense_records】")
    logger.info(f"  {TARGET_DATE} 的记录数: {expense_count}")

    logger.info("\n" + "=" * 60)


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始执行数据重置脚本")
    logger.info(f"目标日期: {TARGET_DATE}")
    logger.info("=" * 60)

    # 确认执行
    print(f"\n⚠️  警告：此脚本将重置 {TARGET_DATE} 的所有统计数据（除了延续性数据）")
    print("延续性数据（保留）：valid_orders, valid_amount, liquid_funds")
    print("\n需要归零的数据：")
    print("  - financial_data 表中的非延续性字段")
    print("  - grouped_data 表中的非延续性字段")
    print(f"  - daily_data 表中 {TARGET_DATE} 的所有数据")
    print(f"  - income_records 表中 {TARGET_DATE} 的记录（标记为已撤销）")
    print(f"  - expense_records 表中 {TARGET_DATE} 的记录（删除）")

    confirm = input("\n确认执行？(yes/no): ").strip().lower()
    if confirm != "yes":
        logger.info("用户取消执行")
        return

    conn = None
    try:
        conn = get_db_connection()

        # 开始事务
        conn.execute("BEGIN TRANSACTION")

        # 1. 重置 financial_data
        reset_financial_data(conn)

        # 2. 重置 grouped_data
        reset_grouped_data(conn)

        # 3. 重置 daily_data
        reset_daily_data(conn, TARGET_DATE)

        # 4. 处理 income_records
        reset_income_records(conn, TARGET_DATE)

        # 5. 处理 expense_records
        reset_expense_records(conn, TARGET_DATE)

        # 提交事务
        conn.commit()

        logger.info("\n✅ 所有操作已成功完成并提交")

        # 生成报告
        generate_report(conn)

    except Exception as e:
        logger.error(f"❌ 执行过程中出错: {e}", exc_info=True)
        if conn:
            conn.rollback()
            logger.error("已回滚所有更改")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭")


if __name__ == "__main__":
    main()
