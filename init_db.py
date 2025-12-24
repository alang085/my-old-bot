import logging
import os
import sqlite3

logger = logging.getLogger(__name__)

# 数据库文件路径 - 支持持久化存储
# 如果设置了 DATA_DIR 环境变量，使用该目录；否则使用当前目录
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
DB_NAME = os.path.join(DATA_DIR, "loan_bot.db")


def init_database():
    """初始化数据库，创建所有必要的表"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # 创建订单表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT UNIQUE NOT NULL,
        group_id TEXT NOT NULL,
        chat_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        weekday_group TEXT NOT NULL,
        customer TEXT NOT NULL,
        amount REAL NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建财务数据表（全局统计）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS financial_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        valid_orders INTEGER DEFAULT 0,
        valid_amount REAL DEFAULT 0,
        liquid_funds REAL DEFAULT 0,
        new_clients INTEGER DEFAULT 0,
        new_clients_amount REAL DEFAULT 0,
        old_clients INTEGER DEFAULT 0,
        old_clients_amount REAL DEFAULT 0,
        interest REAL DEFAULT 0,
        completed_orders INTEGER DEFAULT 0,
        completed_amount REAL DEFAULT 0,
        breach_orders INTEGER DEFAULT 0,
        breach_amount REAL DEFAULT 0,
        breach_end_orders INTEGER DEFAULT 0,
        breach_end_amount REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建分组数据表（按归属ID分组）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS grouped_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT UNIQUE NOT NULL,
        valid_orders INTEGER DEFAULT 0,
        valid_amount REAL DEFAULT 0,
        liquid_funds REAL DEFAULT 0,
        new_clients INTEGER DEFAULT 0,
        new_clients_amount REAL DEFAULT 0,
        old_clients INTEGER DEFAULT 0,
        old_clients_amount REAL DEFAULT 0,
        interest REAL DEFAULT 0,
        completed_orders INTEGER DEFAULT 0,
        completed_amount REAL DEFAULT 0,
        breach_orders INTEGER DEFAULT 0,
        breach_amount REAL DEFAULT 0,
        breach_end_orders INTEGER DEFAULT 0,
        breach_end_amount REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建日结数据表（按日期和归属ID存储）
    # 先创建表（如果不存在）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS daily_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        group_id TEXT,
        new_clients INTEGER DEFAULT 0,
        new_clients_amount REAL DEFAULT 0,
        old_clients INTEGER DEFAULT 0,
        old_clients_amount REAL DEFAULT 0,
        interest REAL DEFAULT 0,
        completed_orders INTEGER DEFAULT 0,
        completed_amount REAL DEFAULT 0,
        breach_orders INTEGER DEFAULT 0,
        breach_amount REAL DEFAULT 0,
        breach_end_orders INTEGER DEFAULT 0,
        breach_end_amount REAL DEFAULT 0,
        liquid_flow REAL DEFAULT 0,
        company_expenses REAL DEFAULT 0,
        other_expenses REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date, group_id)
    )
    """
    )

    # 检查表是否存在，如果存在需要检查列是否存在并添加缺失的列
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_data'")
    table_exists = cursor.fetchone()

    if table_exists:
        # 检查表结构，添加缺失的列
        cursor.execute("PRAGMA table_info(daily_data)")
        columns = [row[1] for row in cursor.fetchall()]

        # 添加缺失的列（如果不存在）
        if "liquid_flow" not in columns:
            try:
                cursor.execute("ALTER TABLE daily_data ADD COLUMN liquid_flow REAL DEFAULT 0")
                conn.commit()
                print("已添加列: liquid_flow")
            except sqlite3.OperationalError as e:
                print(f"添加列 liquid_flow 时出错（可能已存在）: {e}")

        if "company_expenses" not in columns:
            try:
                cursor.execute("ALTER TABLE daily_data ADD COLUMN company_expenses REAL DEFAULT 0")
                conn.commit()
                print("已添加列: company_expenses")
            except sqlite3.OperationalError as e:
                print(f"添加列 company_expenses 时出错（可能已存在）: {e}")

        if "other_expenses" not in columns:
            try:
                cursor.execute("ALTER TABLE daily_data ADD COLUMN other_expenses REAL DEFAULT 0")
                conn.commit()
                print("已添加列: other_expenses")
            except sqlite3.OperationalError as e:
                print(f"添加列 other_expenses 时出错（可能已存在）: {e}")

    # 初始化财务数据（如果不存在）
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
        ) VALUES (0, 0, 100000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        """
        )

    # 创建授权用户表（员工）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS authorized_users (
        user_id INTEGER PRIMARY KEY,
        added_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建支付账号表（GCASH和PayMaya）
    # 检查表是否存在，如果存在需要迁移（移除UNIQUE约束）
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='payment_accounts'")
    table_exists = cursor.fetchone()

    if table_exists:
        # 检查是否有UNIQUE约束（通过检查索引）
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='payment_accounts'"
        )
        table_sql = cursor.fetchone()
        if table_sql and "UNIQUE" in table_sql[0]:
            # 需要迁移：保存数据，重建表
            cursor.execute("SELECT * FROM payment_accounts")
            old_data = cursor.fetchall()
            cursor.execute("DROP TABLE payment_accounts")
            # 创建新表（无UNIQUE约束）
            cursor.execute(
                """
            CREATE TABLE payment_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_type TEXT NOT NULL,
                account_number TEXT NOT NULL,
                account_name TEXT,
                balance REAL DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
            )
            # 恢复数据
            if old_data:
                cursor.executemany(
                    """
                INSERT INTO payment_accounts (id, account_type, account_number, account_name, balance, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                    old_data,
                )

    # 创建表（如果不存在）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS payment_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_type TEXT NOT NULL,
        account_number TEXT NOT NULL,
        account_name TEXT,
        balance REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 初始化支付账号（如果不存在）
    cursor.execute("SELECT COUNT(*) FROM payment_accounts")
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            """
        INSERT INTO payment_accounts (account_type, account_number, account_name, balance)
        VALUES ('gcash', '', '', 0)
        """
        )
        cursor.execute(
            """
        INSERT INTO payment_accounts (account_type, account_number, account_name, balance)
        VALUES ('paymaya', '', '', 0)
        """
        )

    # 创建支付账号余额历史表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS payment_balance_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id INTEGER,
        account_type TEXT NOT NULL,
        balance REAL NOT NULL,
        date TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (account_id) REFERENCES payment_accounts(id)
    )
    """
    )

    # 创建索引以提高查询性能
    cursor.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_payment_balance_history_date 
    ON payment_balance_history(date)
    """
    )
    cursor.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_payment_balance_history_account_id 
    ON payment_balance_history(account_id)
    """
    )

    # 创建定时播报表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        slot INTEGER NOT NULL CHECK(slot >= 1 AND slot <= 3),
        time TEXT NOT NULL,
        chat_id INTEGER,
        chat_title TEXT,
        message TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(slot)
    )
    """
    )

    # 创建用户归属ID映射表（用于限制用户只能查看特定归属ID的报表）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS user_group_mapping (
        user_id INTEGER PRIMARY KEY,
        group_id TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建群组消息配置表（总群/总频道配置）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS group_message_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL UNIQUE,
        chat_title TEXT,
        start_work_message TEXT,
        end_work_message TEXT,
        welcome_message TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建公司公告表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS company_announcements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建公告发送计划表（全局配置，只有一条记录）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS announcement_schedule (
        id INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),
        interval_hours INTEGER DEFAULT 3,
        is_active INTEGER DEFAULT 1,
        last_sent_at TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 初始化公告发送计划表（如果不存在记录）
    cursor.execute("SELECT COUNT(*) FROM announcement_schedule")
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            """
        INSERT INTO announcement_schedule (id, interval_hours, is_active)
        VALUES (1, 3, 1)
        """
        )

    # 创建防诈骗语录表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS anti_fraud_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建公司宣传轮播语录表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS company_promotion_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建开销记录表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS expense_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        type TEXT NOT NULL,
        amount REAL NOT NULL,
        note TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建收入明细表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS income_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        type TEXT NOT NULL,
        amount REAL NOT NULL,
        group_id TEXT,
        order_id TEXT,
        order_date TEXT,
        customer TEXT,
        weekday_group TEXT,
        note TEXT,
        created_by INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        is_undone INTEGER DEFAULT 0
    )
    """
    )

    # 检查并添加 is_undone 字段（迁移旧表结构）
    cursor.execute("PRAGMA table_info(income_records)")
    columns = [col[1] for col in cursor.fetchall()]
    if "is_undone" not in columns:
        try:
            logger.info("添加 is_undone 字段到 income_records 表...")
            cursor.execute(
                """
            ALTER TABLE income_records 
            ADD COLUMN is_undone INTEGER DEFAULT 0
            """
            )
        except sqlite3.OperationalError as e:
            logger.warning(f"添加 is_undone 字段失败（可能已存在）: {e}")

    # 创建操作历史表（用于撤销功能）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS operation_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        operation_type TEXT NOT NULL,
        operation_data TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        is_undone INTEGER DEFAULT 0
    )
    """
    )

    # 创建日切数据汇总表
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS daily_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL UNIQUE,
        new_orders_count INTEGER DEFAULT 0,
        new_orders_amount REAL DEFAULT 0,
        completed_orders_count INTEGER DEFAULT 0,
        completed_orders_amount REAL DEFAULT 0,
        breach_end_orders_count INTEGER DEFAULT 0,
        breach_end_orders_amount REAL DEFAULT 0,
        daily_interest REAL DEFAULT 0,
        company_expenses REAL DEFAULT 0,
        other_expenses REAL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建基准报表表（用于11:05增量报表）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS baseline_report (
        id INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),
        baseline_date TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 创建增量报表合并记录表（记录已合并的日期）
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS incremental_merge_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        merge_date TEXT NOT NULL UNIQUE,
        baseline_date TEXT NOT NULL,
        orders_count INTEGER DEFAULT 0,
        total_amount REAL DEFAULT 0,
        total_interest REAL DEFAULT 0,
        total_expenses REAL DEFAULT 0,
        merged_by INTEGER,
        merged_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
    )

    # 检查并添加 chat_id 字段（如果不存在）- 迁移旧表结构
    cursor.execute("PRAGMA table_info(operation_history)")
    columns = [col[1] for col in cursor.fetchall()]
    if "chat_id" not in columns:
        try:
            logger.info("添加 chat_id 字段到 operation_history 表...")
            cursor.execute(
                """
            ALTER TABLE operation_history 
            ADD COLUMN chat_id INTEGER NOT NULL DEFAULT 0
            """
            )
        except sqlite3.OperationalError as e:
            logger.warning(f"添加 chat_id 字段失败（可能已存在）: {e}")

    # 为操作历史表创建索引
    cursor.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_operation_user_time ON operation_history(user_id, created_at DESC)
    """
    )
    cursor.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_operation_undone ON operation_history(is_undone, created_at DESC)
    """
    )
    cursor.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_operation_chat_user ON operation_history(chat_id, user_id, created_at DESC)
    """
    )
    # 添加按日期查询的索引
    cursor.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_operation_date ON operation_history(DATE(created_at))
    """
    )

    # 创建索引以优化查询性能
    try:
        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_income_date ON income_records(date)
        """
        )
        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_income_type ON income_records(type)
        """
        )
        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_income_customer ON income_records(customer)
        """
        )
        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_income_group ON income_records(group_id)
        """
        )
        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_income_order ON income_records(order_id)
        """
        )
        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_income_date_type ON income_records(date, type)
        """
        )
        cursor.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_income_group_type ON income_records(group_id, type)
        """
        )
    except sqlite3.OperationalError:
        # 索引可能已存在，忽略错误
        pass

    conn.commit()
    conn.close()
    print(f"数据库 {DB_NAME} 初始化完成！")


if __name__ == "__main__":
    init_database()
