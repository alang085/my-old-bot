"""
数据库操作模块

职责：
    提供所有数据库操作的统一接口，包括订单、财务、用户、支付账号等数据的增删改查。

主要功能分区：
    1. 订单操作 - 订单的创建、查询、更新、状态变更
    2. 财务数据操作 - 全局和分组财务统计数据的查询和更新
    3. 日结数据操作 - 按日期和归属ID的日结流量数据
    4. 收入明细操作 - 收入记录的创建和查询
    5. 支出明细操作 - 支出记录的创建和查询
    6. 日切汇总操作 - 每日日切数据的计算和存储
    7. 用户权限操作 - 授权用户和用户归属映射的管理
    8. 支付账号操作 - GCASH和PayMaya账号的管理
    9. 定时播报操作 - 定时播报任务的配置和管理
    10. 操作历史操作 - 用户操作历史的记录和查询

对外接口（主要函数）：
    订单操作：
        - create_order() - 创建订单
        - get_order_by_chat_id() - 根据chat_id获取订单
        - update_order_state() - 更新订单状态
        - get_all_valid_orders() - 获取所有有效订单
        - get_new_orders_by_date() - 获取指定日期的新增订单
        - get_completed_orders_by_date() - 获取指定日期的完成订单
        - get_breach_end_orders_by_date() - 获取指定日期的违约完成订单

    财务数据操作：
        - get_financial_data() - 获取全局财务数据
        - update_financial_data() - 更新全局财务数据
        - get_grouped_data() - 获取分组财务数据
        - update_grouped_data() - 更新分组财务数据
        - get_stats_by_date_range() - 按日期范围获取统计数据

    收入明细操作：
        - record_income() - 记录收入
        - get_income_by_date_range() - 按日期范围查询收入
        - get_all_interest_by_order_id() - 获取订单的所有利息记录

    日切汇总操作：
        - calculate_daily_summary() - 计算日切数据
        - save_daily_summary() - 保存日切数据
        - get_daily_summary() - 获取日切数据
        - get_daily_interest_total() - 获取指定日期的利息总额
        - get_daily_expenses() - 获取指定日期的开销

    用户权限操作：
        - is_user_authorized() - 检查用户是否授权
        - get_authorized_users() - 获取所有授权用户
        - add_authorized_user() - 添加授权用户
        - remove_authorized_user() - 移除授权用户

    支付账号操作：
        - get_payment_accounts() - 获取支付账号列表
        - update_payment_account() - 更新支付账号

    定时播报操作：
        - get_active_scheduled_broadcasts() - 获取激活的定时播报
        - save_scheduled_broadcast() - 保存定时播报配置

    操作历史操作：
        - record_operation() - 记录操作历史
        - get_last_operation() - 获取最后一次操作
        - mark_operation_undone() - 标记操作为已撤销

数据库文件：
    - 路径：由环境变量 DATA_DIR 指定，默认为模块所在目录
    - 文件名：loan_bot.db
    - 初始化：通过 init_db.py 脚本初始化表结构

注意事项：
    - 所有数据库操作都是异步的，使用装饰器 @db_transaction 或 @db_query
    - 事务操作会自动提交，查询操作只读不提交
    - 所有函数都返回字典或列表，便于后续处理
"""

# 标准库
import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime
from functools import wraps
from typing import Dict, List, Optional

# 第三方库
import pytz

# 本地模块
from utils.date_helpers import get_date_range_for_query

# 日志
logger = logging.getLogger(__name__)

# 数据库文件路径
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
os.makedirs(DATA_DIR, exist_ok=True)
DB_NAME = os.path.join(DATA_DIR, "loan_bot.db")


def get_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_transaction(func):
    """数据库事务装饰器"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_running_loop()

        def sync_work():
            conn = get_connection()
            cursor = conn.cursor()
            try:
                result = func(conn, cursor, *args, **kwargs)
                if result is not False:
                    conn.commit()
                return result
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error in {func.__name__}: {e}", exc_info=True)
                return False
            finally:
                conn.close()

        return await loop.run_in_executor(None, sync_work)

    return wrapper


def db_query(func):
    """数据库查询装饰器"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_running_loop()

        def sync_work():
            conn = get_connection()
            cursor = conn.cursor()
            try:
                return func(conn, cursor, *args, **kwargs)
            except Exception as e:
                logger.error(f"Database query error in {func.__name__}: {e}", exc_info=True)
                raise e
            finally:
                conn.close()

        return await loop.run_in_executor(None, sync_work)

    return wrapper


# ========== 订单操作 ==========


@db_transaction
def create_order(conn, cursor, order_data: Dict) -> bool:
    """创建新订单"""
    try:
        cursor.execute(
            """
        INSERT INTO orders (
            order_id, group_id, chat_id, date, weekday_group,
            customer, amount, state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                order_data["order_id"],
                order_data["group_id"],
                order_data["chat_id"],
                order_data["date"],
                order_data["group"],
                order_data["customer"],
                order_data["amount"],
                order_data["state"],
            ),
        )
        return True
    except sqlite3.IntegrityError as e:
        logger.warning(f"订单创建失败（重复）: {e}")
        return False


@db_query
def get_order_by_chat_id(conn, cursor, chat_id: int) -> Optional[Dict]:
    """根据chat_id获取订单"""
    cursor.execute(
        "SELECT * FROM orders WHERE chat_id = ? AND state NOT IN (?, ?)",
        (chat_id, "end", "breach_end"),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


@db_query
def get_order_by_order_id(conn, cursor, order_id: str) -> Optional[Dict]:
    """根据order_id获取订单"""
    cursor.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


@db_transaction
def update_order_amount(conn, cursor, chat_id: int, new_amount: float) -> bool:
    """更新订单金额"""
    cursor.execute(
        """
    UPDATE orders 
    SET amount = ?, updated_at = CURRENT_TIMESTAMP
    WHERE chat_id = ? AND state NOT IN (?, ?)
    """,
        (new_amount, chat_id, "end", "breach_end"),
    )
    return cursor.rowcount > 0


@db_transaction
def update_order_state(conn, cursor, chat_id: int, new_state: str) -> bool:
    """更新订单状态"""
    cursor.execute(
        """
    UPDATE orders 
    SET state = ?, updated_at = CURRENT_TIMESTAMP
    WHERE chat_id = ? AND state NOT IN (?, ?)
    """,
        (new_state, chat_id, "end", "breach_end"),
    )
    return cursor.rowcount > 0


@db_transaction
def update_order_group_id(conn, cursor, chat_id: int, new_group_id: str) -> bool:
    """更新订单归属ID"""
    cursor.execute(
        """
    UPDATE orders 
    SET group_id = ?, updated_at = CURRENT_TIMESTAMP
    WHERE chat_id = ?
    """,
        (new_group_id, chat_id),
    )
    return cursor.rowcount > 0


@db_transaction
def update_order_weekday_group(conn, cursor, chat_id: int, new_weekday_group: str) -> bool:
    """更新订单星期分组

    Args:
        conn: 数据库连接对象
        cursor: 数据库游标对象
        chat_id: 订单的聊天ID
        new_weekday_group: 新的星期分组（一、二、三、四、五、六、日）

    Returns:
        bool: 如果成功更新至少一行，返回 True；否则返回 False

    Note:
        此函数会自动提交事务（通过 @db_transaction 装饰器）
        如果返回 False，说明没有行被更新（可能 chat_id 不存在）
    """
    cursor.execute(
        """
    UPDATE orders 
    SET weekday_group = ?, updated_at = CURRENT_TIMESTAMP
    WHERE chat_id = ?
    """,
        (new_weekday_group, chat_id),
    )
    rowcount = cursor.rowcount
    if rowcount > 0:
        # 确保数据已写入（虽然装饰器会提交，但这里可以添加额外验证）
        logger.debug(
            f"更新订单星期分组: chat_id={chat_id}, weekday_group={new_weekday_group}, rowcount={rowcount}"
        )
    return rowcount > 0


@db_transaction
def update_order_date(conn, cursor, chat_id: int, new_date: str) -> bool:
    """更新订单日期

    Args:
        conn: 数据库连接对象
        cursor: 数据库游标对象
        chat_id: 订单的聊天ID
        new_date: 新的日期字符串，格式为 'YYYY-MM-DD HH:MM:SS'

    Returns:
        bool: 如果成功更新至少一行，返回 True；否则返回 False

    Note:
        此函数会自动更新 updated_at 字段为当前时间戳
    """
    cursor.execute(
        """
    UPDATE orders 
    SET date = ?, updated_at = CURRENT_TIMESTAMP
    WHERE chat_id = ?
    """,
        (new_date, chat_id),
    )
    return cursor.rowcount > 0


@db_transaction
def delete_order_by_chat_id(conn, cursor, chat_id: int) -> bool:
    """删除订单（用于撤销订单创建）"""
    cursor.execute("DELETE FROM orders WHERE chat_id = ?", (chat_id,))
    return cursor.rowcount > 0


@db_transaction
def delete_order_by_order_id(conn, cursor, order_id: str) -> bool:
    """根据订单ID删除订单"""
    cursor.execute("DELETE FROM orders WHERE order_id = ?", (order_id,))
    return cursor.rowcount > 0


@db_transaction
# ========== 查找功能 ==========


@db_query
def search_orders_by_group_id(
    conn, cursor, group_id: str, state: Optional[str] = None
) -> List[Dict]:
    """根据归属ID查找订单"""
    if state:
        cursor.execute(
            "SELECT * FROM orders WHERE group_id = ? AND state = ? ORDER BY date DESC",
            (group_id, state),
        )
    else:
        # 默认排除完成和违约完成的订单
        cursor.execute(
            "SELECT * FROM orders WHERE group_id = ? AND state NOT IN ('end', 'breach_end') ORDER BY date DESC",
            (group_id,),
        )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_by_date_range(conn, cursor, start_date: str, end_date: str) -> List[Dict]:
    """根据日期范围查找订单"""
    cursor.execute(
        """
    SELECT * FROM orders 
    WHERE date >= ? AND date <= ?
    ORDER BY date DESC
    """,
        (start_date, end_date),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_by_customer(conn, cursor, customer: str) -> List[Dict]:
    """根据客户类型查找订单"""
    cursor.execute(
        "SELECT * FROM orders WHERE customer = ? ORDER BY date DESC", (customer.upper(),)
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_by_state(conn, cursor, state: str) -> List[Dict]:
    """根据状态查找订单"""
    cursor.execute("SELECT * FROM orders WHERE state = ? ORDER BY date DESC", (state,))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_all(conn, cursor) -> List[Dict]:
    """查找所有订单"""
    cursor.execute("SELECT * FROM orders ORDER BY date DESC")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_advanced(conn, cursor, criteria: Dict) -> List[Dict]:
    """
    高级查找订单（支持混合条件）
    """
    query = "SELECT * FROM orders WHERE 1=1"
    params = []

    if "group_id" in criteria and criteria["group_id"]:
        query += " AND group_id = ?"
        params.append(criteria["group_id"])

    if "state" in criteria and criteria["state"]:
        query += " AND state = ?"
        params.append(criteria["state"])
    else:
        # 默认只查找有效订单（normal和overdue状态）
        query += " AND state IN ('normal', 'overdue')"

    if "customer" in criteria and criteria["customer"]:
        query += " AND customer = ?"
        params.append(criteria["customer"])

    if "order_id" in criteria and criteria["order_id"]:
        query += " AND order_id = ?"
        params.append(criteria["order_id"])

    if "date_range" in criteria and criteria["date_range"]:
        start_date, end_date = criteria["date_range"]
        query += " AND date >= ? AND date <= ?"
        params.extend([start_date, end_date])

    if "weekday_group" in criteria and criteria["weekday_group"]:
        query += " AND weekday_group = ?"
        params.append(criteria["weekday_group"])

    query += " ORDER BY date DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_advanced_all_states(conn, cursor, criteria: Dict) -> List[Dict]:
    """
    高级查找订单（支持混合条件，包含所有状态的订单）
    用于报表查找功能
    """
    query = "SELECT * FROM orders WHERE 1=1"
    params = []

    if "group_id" in criteria and criteria["group_id"]:
        query += " AND group_id = ?"
        params.append(criteria["group_id"])

    if "state" in criteria and criteria["state"]:
        query += " AND state = ?"
        params.append(criteria["state"])

    if "customer" in criteria and criteria["customer"]:
        query += " AND customer = ?"
        params.append(criteria["customer"])

    if "order_id" in criteria and criteria["order_id"]:
        query += " AND order_id = ?"
        params.append(criteria["order_id"])

    if "date_range" in criteria and criteria["date_range"]:
        start_date, end_date = criteria["date_range"]
        query += " AND date >= ? AND date <= ?"
        params.extend([start_date, end_date])

    if "weekday_group" in criteria and criteria["weekday_group"]:
        query += " AND weekday_group = ?"
        params.append(criteria["weekday_group"])

    query += " ORDER BY date DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


# ========== 财务数据操作 ==========


@db_query
def get_financial_data(conn, cursor) -> Dict:
    """获取全局财务数据"""
    cursor.execute("SELECT * FROM financial_data ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        return dict(row)
    return {
        "valid_orders": 0,
        "valid_amount": 0,
        "liquid_funds": 0,
        "new_clients": 0,
        "new_clients_amount": 0,
        "old_clients": 0,
        "old_clients_amount": 0,
        "interest": 0,
        "completed_orders": 0,
        "completed_amount": 0,
        "breach_orders": 0,
        "breach_amount": 0,
        "breach_end_orders": 0,
        "breach_end_amount": 0,
    }


@db_transaction
def update_financial_data(conn, cursor, field: str, amount: float) -> bool:
    """更新财务数据字段

    Args:
        conn: 数据库连接对象
        cursor: 数据库游标对象
        field: 要更新的字段名
        amount: 要增加/减少的金额（正数表示增加，负数表示减少）

    Returns:
        bool: 如果成功更新，返回 True；否则返回 False

    Note:
        - 此函数会自动提交事务（通过 @db_transaction 装饰器）
        - 如果字段不存在，会使用默认值 0
        - 使用增量更新（current_value + amount）
    """
    # 验证字段名，防止SQL注入
    valid_fields = [
        "valid_orders",
        "valid_amount",
        "liquid_funds",
        "new_clients",
        "new_clients_amount",
        "old_clients",
        "old_clients_amount",
        "interest",
        "completed_orders",
        "completed_amount",
        "breach_orders",
        "breach_amount",
        "breach_end_orders",
        "breach_end_amount",
    ]
    if field not in valid_fields:
        logger.error(f"无效的财务数据字段名: {field}")
        return False

    cursor.execute("SELECT * FROM financial_data ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    if not row:
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
        row_dict = dict(row)
        current_value = row_dict.get(field, 0)

    new_value = current_value + amount
    # 使用参数化查询，避免SQL注入
    cursor.execute(
        f"""
    UPDATE financial_data 
    SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = (SELECT id FROM financial_data ORDER BY id DESC LIMIT 1)
    """,
        (new_value,),
    )

    if cursor.rowcount == 0:
        logger.warning(f"更新财务数据失败: field={field}, amount={amount}, rowcount=0")
        return False

    logger.debug(f"财务数据已更新: {field} = {current_value} + {amount} = {new_value}")
    return True


# ========== 分组数据操作 ==========


@db_query
def get_grouped_data(conn, cursor, group_id: Optional[str] = None) -> Dict:
    """获取分组数据"""
    if group_id:
        cursor.execute("SELECT * FROM grouped_data WHERE group_id = ?", (group_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return {
            "group_id": group_id,
            "valid_orders": 0,
            "valid_amount": 0,
            "liquid_funds": 0,
            "new_clients": 0,
            "new_clients_amount": 0,
            "old_clients": 0,
            "old_clients_amount": 0,
            "interest": 0,
            "completed_orders": 0,
            "completed_amount": 0,
            "breach_orders": 0,
            "breach_amount": 0,
            "breach_end_orders": 0,
            "breach_end_amount": 0,
        }
    else:
        # 获取所有分组数据
        cursor.execute("SELECT * FROM grouped_data")
        rows = cursor.fetchall()
        result = {}
        for row in rows:
            result[row["group_id"]] = dict(row)
        return result


@db_transaction
def update_grouped_data(conn, cursor, group_id: str, field: str, amount: float) -> bool:
    """更新分组数据字段

    Args:
        conn: 数据库连接对象
        cursor: 数据库游标对象
        group_id: 归属ID
        field: 要更新的字段名
        amount: 要增加/减少的金额（正数表示增加，负数表示减少）

    Returns:
        bool: 如果成功更新，返回 True；否则返回 False

    Note:
        - 此函数会自动提交事务（通过 @db_transaction 装饰器）
        - 如果分组不存在，会自动创建
        - 使用增量更新（current_value + amount）
    """
    # 验证字段名，防止SQL注入
    valid_fields = [
        "valid_orders",
        "valid_amount",
        "liquid_funds",
        "new_clients",
        "new_clients_amount",
        "old_clients",
        "old_clients_amount",
        "interest",
        "completed_orders",
        "completed_amount",
        "breach_orders",
        "breach_amount",
        "breach_end_orders",
        "breach_end_amount",
    ]
    if field not in valid_fields:
        logger.error(f"无效的分组数据字段名: {field}")
        return False

    if not group_id:
        logger.error("group_id 不能为空")
        return False

    cursor.execute("SELECT * FROM grouped_data WHERE group_id = ?", (group_id,))
    row = cursor.fetchone()

    if not row:
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
        row_dict = dict(row)
        current_value = row_dict.get(field, 0)

    new_value = current_value + amount
    cursor.execute(
        f"""
    UPDATE grouped_data 
    SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
    WHERE group_id = ?
    """,
        (new_value, group_id),
    )

    if cursor.rowcount == 0:
        logger.warning(
            f"更新分组数据失败: group_id={group_id}, field={field}, amount={amount}, rowcount=0"
        )
        return False

    logger.debug(f"分组数据已更新: {group_id} {field} = {current_value} + {amount} = {new_value}")
    return True


@db_query
def get_all_group_ids(conn, cursor) -> List[str]:
    """获取所有归属ID列表"""
    cursor.execute("SELECT DISTINCT group_id FROM grouped_data ORDER BY group_id")
    rows = cursor.fetchall()
    return [row[0] for row in rows]


# ========== 日结数据操作 ==========


@db_query
def get_daily_data(conn, cursor, date: str, group_id: Optional[str] = None) -> Dict:
    """获取日结数据"""
    if group_id:
        cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id = ?", (date, group_id))
    else:
        # 全局日结数据（group_id为NULL）
        cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL", (date,))

    row = cursor.fetchone()
    if row:
        return dict(row)

    return {
        "new_clients": 0,
        "new_clients_amount": 0,
        "old_clients": 0,
        "old_clients_amount": 0,
        "interest": 0,
        "completed_orders": 0,
        "completed_amount": 0,
        "breach_orders": 0,
        "breach_amount": 0,
        "breach_end_orders": 0,
        "breach_end_amount": 0,
        "liquid_flow": 0,
        "company_expenses": 0,
        "other_expenses": 0,
    }


@db_transaction
def update_daily_data(
    conn, cursor, date: str, field: str, amount: float, group_id: Optional[str] = None
) -> bool:
    """更新日结数据字段

    Args:
        conn: 数据库连接对象
        cursor: 数据库游标对象
        date: 日期字符串，格式 'YYYY-MM-DD'
        field: 要更新的字段名
        amount: 要增加/减少的金额（正数表示增加，负数表示减少）
        group_id: 归属ID，如果为 None 则更新全局日结数据

    Returns:
        bool: 如果成功更新，返回 True；否则返回 False

    Note:
        - 此函数会自动提交事务（通过 @db_transaction 装饰器）
        - 如果日结数据不存在，会自动创建
        - 使用增量更新（current_value + amount）
    """
    # 验证字段名，防止SQL注入
    valid_fields = [
        "new_clients",
        "new_clients_amount",
        "old_clients",
        "old_clients_amount",
        "interest",
        "completed_orders",
        "completed_amount",
        "breach_orders",
        "breach_amount",
        "breach_end_orders",
        "breach_end_amount",
        "liquid_flow",
        "company_expenses",
        "other_expenses",
    ]
    if field not in valid_fields:
        logger.error(f"无效的日结数据字段名: {field}")
        return False

    # 验证日期格式
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        logger.error(f"无效的日期格式: {date}")
        return False

    if group_id:
        cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id = ?", (date, group_id))
    else:
        cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL", (date,))

    row = cursor.fetchone()

    if not row:
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
        row_dict = dict(row)
        current_value = row_dict.get(field, 0)

    new_value = current_value + amount
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

    if cursor.rowcount == 0:
        logger.warning(
            f"更新日结数据失败: date={date}, group_id={group_id}, field={field}, amount={amount}, rowcount=0"
        )
        return False

    logger.debug(
        f"日结数据已更新: {date} {group_id or '全局'} {field} = {current_value} + {amount} = {new_value}"
    )
    return True


@db_query
def get_stats_by_date_range(
    conn, cursor, start_date: str, end_date: str, group_id: Optional[str] = None
) -> Dict:
    """根据日期范围聚合统计数据"""
    where_clause = "date >= ? AND date <= ?"
    params = [start_date, end_date]

    if group_id:
        where_clause += " AND group_id = ?"
        params.append(group_id)
    else:
        where_clause += " AND group_id IS NULL"

    cursor.execute(
        f"""
    SELECT 
        SUM(new_clients) as new_clients,
        SUM(new_clients_amount) as new_clients_amount,
        SUM(old_clients) as old_clients,
        SUM(old_clients_amount) as old_clients_amount,
        SUM(interest) as interest,
        SUM(completed_orders) as completed_orders,
        SUM(completed_amount) as completed_amount,
        SUM(breach_orders) as breach_orders,
        SUM(breach_amount) as breach_amount,
        SUM(breach_end_orders) as breach_end_orders,
        SUM(breach_end_amount) as breach_end_amount,
        SUM(liquid_flow) as liquid_flow,
        SUM(company_expenses) as company_expenses,
        SUM(other_expenses) as other_expenses
    FROM daily_data 
    WHERE {where_clause}
    """,
        params,
    )

    row = cursor.fetchone()

    result = {}
    keys = [
        "new_clients",
        "new_clients_amount",
        "old_clients",
        "old_clients_amount",
        "interest",
        "completed_orders",
        "completed_amount",
        "breach_orders",
        "breach_amount",
        "breach_end_orders",
        "breach_end_amount",
        "liquid_flow",
        "company_expenses",
        "other_expenses",
    ]

    for i, key in enumerate(keys):
        result[key] = row[i] if row[i] is not None else 0

    return result


# ========== 授权用户操作 ==========


@db_transaction
def add_authorized_user(conn, cursor, user_id: int) -> bool:
    """添加授权用户"""
    cursor.execute("INSERT OR IGNORE INTO authorized_users (user_id) VALUES (?)", (user_id,))
    return True


@db_transaction
def remove_authorized_user(conn, cursor, user_id: int) -> bool:
    """移除授权用户"""
    cursor.execute("DELETE FROM authorized_users WHERE user_id = ?", (user_id,))
    return True


@db_query
def get_authorized_users(conn, cursor) -> List[int]:
    """获取所有授权用户ID"""
    cursor.execute("SELECT user_id FROM authorized_users")
    rows = cursor.fetchall()
    return [row[0] for row in rows]


@db_query
def is_user_authorized(conn, cursor, user_id: int) -> bool:
    """检查用户是否授权"""
    cursor.execute("SELECT 1 FROM authorized_users WHERE user_id = ?", (user_id,))
    return cursor.fetchone() is not None


# ========== 用户归属ID映射操作 ==========


@db_query
def get_user_group_id(conn, cursor, user_id: int) -> Optional[str]:
    """获取用户有权限查看的归属ID"""
    cursor.execute("SELECT group_id FROM user_group_mapping WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    return row[0] if row else None


@db_transaction
def set_user_group_id(conn, cursor, user_id: int, group_id: str) -> bool:
    """设置用户有权限查看的归属ID"""
    cursor.execute(
        """
    INSERT OR REPLACE INTO user_group_mapping (user_id, group_id, updated_at)
    VALUES (?, ?, CURRENT_TIMESTAMP)
    """,
        (user_id, group_id),
    )
    return True


@db_transaction
def remove_user_group_id(conn, cursor, user_id: int) -> bool:
    """移除用户的归属ID映射"""
    cursor.execute("DELETE FROM user_group_mapping WHERE user_id = ?", (user_id,))
    return cursor.rowcount > 0


@db_query
def get_all_user_group_mappings(conn, cursor) -> List[Dict]:
    """获取所有用户归属ID映射"""
    cursor.execute(
        """
    SELECT user_id, group_id, created_at, updated_at
    FROM user_group_mapping
    ORDER BY user_id
    """
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


# ========== 支付账号操作 ==========


@db_query
def get_payment_account(conn, cursor, account_type: str) -> Optional[Dict]:
    """获取支付账号信息"""
    cursor.execute("SELECT * FROM payment_accounts WHERE account_type = ?", (account_type,))
    row = cursor.fetchone()
    if row:
        return dict(row)
    return None


@db_query
def get_all_payment_accounts(conn, cursor) -> List[Dict]:
    """获取所有支付账号信息"""
    cursor.execute("SELECT * FROM payment_accounts ORDER BY account_type, account_name")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_payment_accounts_by_type(conn, cursor, account_type: str) -> List[Dict]:
    """获取指定类型的所有支付账号信息"""
    cursor.execute(
        "SELECT * FROM payment_accounts WHERE account_type = ? ORDER BY account_name",
        (account_type,),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_payment_account_by_id(conn, cursor, account_id: int) -> Optional[Dict]:
    """根据ID获取支付账号信息"""
    cursor.execute("SELECT * FROM payment_accounts WHERE id = ?", (account_id,))
    row = cursor.fetchone()
    if row:
        return dict(row)
    return None


@db_transaction
def create_payment_account(
    conn, cursor, account_type: str, account_number: str, account_name: str = "", balance: float = 0
) -> int:
    """创建新的支付账号，返回账户ID"""
    cursor.execute(
        """
    INSERT INTO payment_accounts (account_type, account_number, account_name, balance)
    VALUES (?, ?, ?, ?)
    """,
        (account_type, account_number, account_name or "", balance or 0),
    )
    return cursor.lastrowid


@db_transaction
def update_payment_account_by_id(
    conn,
    cursor,
    account_id: int,
    account_number: str = None,
    account_name: str = None,
    balance: float = None,
) -> bool:
    """根据ID更新支付账号信息"""
    updates = []
    params = []

    if account_number is not None:
        updates.append("account_number = ?")
        params.append(account_number)

    if account_name is not None:
        updates.append("account_name = ?")
        params.append(account_name)

    if balance is not None:
        updates.append("balance = ?")
        params.append(balance)

    if not updates:
        return False

    # 验证所有字段名都在白名单中（虽然这里字段名是硬编码的，但为了安全起见）
    valid_field_names = ["account_number", "account_name", "balance", "updated_at"]
    for update_clause in updates:
        field_name = update_clause.split(" = ")[0]
        if field_name not in valid_field_names:
            logger.error(f"无效的支付账户字段名: {field_name}")
        return False

    updates.append("updated_at = CURRENT_TIMESTAMP")
    params.append(account_id)

    set_clause = ", ".join(updates)
    query = f"UPDATE payment_accounts SET {set_clause} WHERE id = ?"
    try:
        cursor.execute(query, params)
        # 事务的commit由@db_transaction装饰器处理
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"更新支付账号时出错: {e}", exc_info=True)
        return False


@db_transaction
def delete_payment_account(conn, cursor, account_id: int) -> bool:
    """删除支付账号"""
    cursor.execute("DELETE FROM payment_accounts WHERE id = ?", (account_id,))
    return cursor.rowcount > 0


@db_transaction
def update_payment_account(
    conn,
    cursor,
    account_type: str,
    account_number: str = None,
    account_name: str = None,
    balance: float = None,
) -> bool:
    """更新支付账号信息（兼容旧代码，更新该类型的第一个账户）"""
    cursor.execute("SELECT * FROM payment_accounts WHERE account_type = ? LIMIT 1", (account_type,))
    row = cursor.fetchone()

    if row:
        # 更新现有记录
        account_id = row["id"]
        return update_payment_account_by_id(
            conn, cursor, account_id, account_number, account_name, balance
        )
    else:
        # 创建新记录
        if account_number:
            create_payment_account(
                conn, cursor, account_type, account_number, account_name or "", balance or 0
            )
            return True
        return False


@db_transaction
def record_expense(conn, cursor, date: str, type: str, amount: float, note: str) -> int:
    """记录开销，返回开销记录ID"""
    # 验证开销类型
    valid_expense_types = ["company", "other"]
    if type not in valid_expense_types:
        logger.error(f"无效的开销类型: {type}")
        raise ValueError(f"无效的开销类型: {type}，必须是 {valid_expense_types} 之一")

    cursor.execute(
        """
    INSERT INTO expense_records (date, type, amount, note)
    VALUES (?, ?, ?, ?)
    """,
        (date, type, amount, note),
    )
    expense_id = cursor.lastrowid

    # 验证字段名，防止SQL注入
    field = "company_expenses" if type == "company" else "other_expenses"
    valid_expense_fields = ["company_expenses", "other_expenses"]
    if field not in valid_expense_fields:
        logger.error(f"无效的开销字段名: {field}")
        raise ValueError(f"无效的开销字段名: {field}")

    cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL", (date,))
    row = cursor.fetchone()

    if not row:
        cursor.execute(
            """
        INSERT INTO daily_data (
            date, group_id, new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount,
            liquid_flow, company_expenses, other_expenses
        ) VALUES (?, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?)
        """,
            (
                date,
                amount if field == "company_expenses" else 0,
                amount if field == "other_expenses" else 0,
            ),
        )
    else:
        cursor.execute(
            f"""
        UPDATE daily_data 
        SET "{field}" = "{field}" + ?, updated_at = CURRENT_TIMESTAMP
        WHERE date = ? AND group_id IS NULL
        """,
            (amount, date),
        )

    cursor.execute("SELECT * FROM financial_data ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    if not row:
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
        row_dict = dict(row)
        current_value = row_dict.get("liquid_funds", 0)

    new_value = current_value - amount

    cursor.execute(
        """
    UPDATE financial_data 
    SET "liquid_funds" = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = (SELECT id FROM financial_data ORDER BY id DESC LIMIT 1)
    """,
        (new_value,),
    )

    cursor.execute("SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL", (date,))
    daily_row = cursor.fetchone()

    if not daily_row:
        cursor.execute(
            """
        INSERT INTO daily_data (
            date, group_id, new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount,
            liquid_flow, company_expenses, other_expenses
        ) VALUES (?, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?)
        """,
            (
                date,
                -amount,
                amount if field == "company_expenses" else 0,
                amount if field == "other_expenses" else 0,
            ),
        )
    else:
        cursor.execute(
            """
        UPDATE daily_data 
        SET "liquid_flow" = "liquid_flow" - ?, updated_at = CURRENT_TIMESTAMP
        WHERE date = ? AND group_id IS NULL
        """,
            (amount, date),
        )

    return expense_id


@db_query
def get_expense_records(
    conn, cursor, start_date: str, end_date: str = None, type: Optional[str] = None
) -> List[Dict]:
    """获取开销记录（支持日期范围）"""
    query = "SELECT * FROM expense_records WHERE date >= ?"
    params = [start_date]

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    else:
        query += " AND date <= ?"
        params.append(start_date)

    if type:
        query += " AND type = ?"
        params.append(type)

    query += " ORDER BY date DESC, created_at ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_transaction
def delete_expense_record(conn, cursor, expense_id: int) -> bool:
    """删除开销记录"""
    cursor.execute("DELETE FROM expense_records WHERE id = ?", (expense_id,))
    return cursor.rowcount > 0


@db_transaction
def delete_income_record(conn, cursor, income_id: int) -> bool:
    """强制删除收入记录（不可恢复）"""
    cursor.execute("DELETE FROM income_records WHERE id = ?", (income_id,))
    return cursor.rowcount > 0


# ========== 定时播报操作 ==========


@db_query
def get_scheduled_broadcast(conn, cursor, slot: int) -> Optional[Dict]:
    """获取指定槽位的定时播报"""
    cursor.execute("SELECT * FROM scheduled_broadcasts WHERE slot = ?", (slot,))
    row = cursor.fetchone()
    return dict(row) if row else None


@db_query
def get_all_scheduled_broadcasts(conn, cursor) -> List[Dict]:
    """获取所有定时播报"""
    cursor.execute("SELECT * FROM scheduled_broadcasts ORDER BY slot")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_active_scheduled_broadcasts(conn, cursor) -> List[Dict]:
    """获取所有激活的定时播报"""
    cursor.execute("SELECT * FROM scheduled_broadcasts WHERE is_active = 1 ORDER BY slot")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_transaction
def create_or_update_scheduled_broadcast(
    conn,
    cursor,
    slot: int,
    time: str,
    chat_id: Optional[int],
    chat_title: Optional[str],
    message: str,
    is_active: int = 1,
) -> bool:
    """创建或更新定时播报"""
    cursor.execute("SELECT * FROM scheduled_broadcasts WHERE slot = ?", (slot,))
    row = cursor.fetchone()

    if row:
        # 更新现有记录
        cursor.execute(
            """
        UPDATE scheduled_broadcasts 
        SET time = ?, chat_id = ?, chat_title = ?, message = ?, 
            is_active = ?, updated_at = CURRENT_TIMESTAMP
        WHERE slot = ?
        """,
            (time, chat_id, chat_title, message, is_active, slot),
        )
    else:
        # 创建新记录
        cursor.execute(
            """
        INSERT INTO scheduled_broadcasts (slot, time, chat_id, chat_title, message, is_active)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
            (slot, time, chat_id, chat_title, message, is_active),
        )

    return True


@db_transaction
def delete_scheduled_broadcast(conn, cursor, slot: int) -> bool:
    """删除定时播报"""
    cursor.execute("DELETE FROM scheduled_broadcasts WHERE slot = ?", (slot,))
    return cursor.rowcount > 0


@db_transaction
def toggle_scheduled_broadcast(conn, cursor, slot: int, is_active: int) -> bool:
    """切换定时播报的激活状态"""
    cursor.execute(
        """
    UPDATE scheduled_broadcasts 
    SET is_active = ?, updated_at = CURRENT_TIMESTAMP
    WHERE slot = ?
    """,
        (is_active, slot),
    )
    return cursor.rowcount > 0


# ========== 群组消息配置操作 ==========


@db_query
def get_group_message_configs(conn, cursor) -> List[Dict]:
    """获取所有激活的群组消息配置"""
    cursor.execute("SELECT * FROM group_message_config WHERE is_active = 1 ORDER BY chat_id")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_group_message_config_by_chat_id(conn, cursor, chat_id: int) -> Optional[Dict]:
    """根据chat_id获取群组消息配置"""
    cursor.execute("SELECT * FROM group_message_config WHERE chat_id = ?", (chat_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


@db_transaction
def save_group_message_config(
    conn,
    cursor,
    chat_id: int,
    chat_title: Optional[str] = None,
    start_work_message: Optional[str] = None,
    end_work_message: Optional[str] = None,
    welcome_message: Optional[str] = None,
    is_active: int = 1,
) -> bool:
    """保存或更新群组消息配置"""
    # 检查是否已存在
    cursor.execute("SELECT * FROM group_message_config WHERE chat_id = ?", (chat_id,))
    row = cursor.fetchone()

    if row:
        # 更新现有记录
        updates = []
        params = []

        if chat_title is not None:
            updates.append("chat_title = ?")
            params.append(chat_title)

        if start_work_message is not None:
            updates.append("start_work_message = ?")
            params.append(start_work_message)

        if end_work_message is not None:
            updates.append("end_work_message = ?")
            params.append(end_work_message)

        if welcome_message is not None:
            updates.append("welcome_message = ?")
            params.append(welcome_message)

        if is_active is not None:
            updates.append("is_active = ?")
            params.append(is_active)

        if not updates:
            return False

        # 验证所有字段名都在白名单中，防止SQL注入
        valid_field_names = [
            "chat_title",
            "start_work_message",
            "end_work_message",
            "welcome_message",
            "is_active",
            "updated_at",
        ]
        for update_clause in updates:
            field_name = update_clause.split(" = ")[0]
            if field_name not in valid_field_names:
                logger.error(f"无效的群组消息配置字段名: {field_name}")
            return False

        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(chat_id)

        cursor.execute(
            f"""
        UPDATE group_message_config 
        SET {', '.join(updates)}
        WHERE chat_id = ?
        """,
            params,
        )
        return cursor.rowcount > 0
    else:
        # 创建新记录
        cursor.execute(
            """
        INSERT INTO group_message_config (
            chat_id, chat_title, start_work_message, end_work_message, 
            welcome_message, is_active
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                chat_id,
                chat_title or "",
                start_work_message or "",
                end_work_message or "",
                welcome_message or "",
                is_active,
            ),
        )
        return True


@db_transaction
def delete_group_message_config(conn, cursor, chat_id: int) -> bool:
    """删除群组消息配置"""
    cursor.execute("DELETE FROM group_message_config WHERE chat_id = ?", (chat_id,))
    return cursor.rowcount > 0


# ========== 公司公告操作 ==========


@db_query
def get_company_announcements(conn, cursor) -> List[Dict]:
    """获取所有激活的公司公告（过滤空消息）"""
    cursor.execute("SELECT * FROM company_announcements WHERE is_active = 1 ORDER BY id")
    rows = cursor.fetchall()
    # 过滤掉空消息或只有空白字符的消息
    result = []
    for row in rows:
        message = row["message"]
        if message and message.strip():  # 确保消息不为空且不只是空白字符
            result.append(dict(row))
    return result


@db_query
def get_all_company_announcements(conn, cursor) -> List[Dict]:
    """获取所有公司公告（包括未激活的）"""
    cursor.execute("SELECT * FROM company_announcements ORDER BY id")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_transaction
def save_company_announcement(conn, cursor, message: str, is_active: int = 1) -> int:
    """保存公司公告，返回公告ID"""
    cursor.execute(
        """
    INSERT INTO company_announcements (message, is_active)
    VALUES (?, ?)
    """,
        (message, is_active),
    )
    return cursor.lastrowid


@db_transaction
def delete_company_announcement(conn, cursor, announcement_id: int) -> bool:
    """删除公司公告"""
    cursor.execute("DELETE FROM company_announcements WHERE id = ?", (announcement_id,))
    return cursor.rowcount > 0


@db_transaction
def toggle_company_announcement(conn, cursor, announcement_id: int, is_active: int) -> bool:
    """切换公司公告的激活状态"""
    cursor.execute(
        """
    UPDATE company_announcements 
    SET is_active = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
    """,
        (is_active, announcement_id),
    )
    return cursor.rowcount > 0


# ========== 公告发送计划操作 ==========


@db_query
def get_announcement_schedule(conn, cursor) -> Optional[Dict]:
    """获取公告发送计划配置"""
    cursor.execute("SELECT * FROM announcement_schedule WHERE id = 1")
    row = cursor.fetchone()
    return dict(row) if row else None


@db_transaction
def save_announcement_schedule(conn, cursor, interval_hours: int = 3, is_active: int = 1) -> bool:
    """保存公告发送计划配置"""
    cursor.execute("SELECT * FROM announcement_schedule WHERE id = 1")
    row = cursor.fetchone()

    if row:
        # 更新
        cursor.execute(
            """
        UPDATE announcement_schedule 
        SET interval_hours = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """,
            (interval_hours, is_active),
        )
        return cursor.rowcount > 0
    else:
        # 创建
        cursor.execute(
            """
        INSERT INTO announcement_schedule (id, interval_hours, is_active)
        VALUES (1, ?, ?)
        """,
            (interval_hours, is_active),
        )
        return True


@db_transaction
def update_announcement_last_sent(conn, cursor) -> bool:
    """更新公告最后发送时间"""
    from datetime import datetime

    import pytz

    tz = pytz.timezone("Asia/Shanghai")
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        """
    UPDATE announcement_schedule 
    SET last_sent_at = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = 1
    """,
        (now,),
    )
    return cursor.rowcount > 0


# ========== 防诈骗语录操作 ==========


@db_query
def get_active_anti_fraud_messages(conn, cursor) -> List[str]:
    """获取所有激活的防诈骗语录"""
    cursor.execute("SELECT message FROM anti_fraud_messages WHERE is_active = 1")
    rows = cursor.fetchall()
    return [row[0] for row in rows]


@db_query
def get_all_anti_fraud_messages(conn, cursor) -> List[Dict]:
    """获取所有防诈骗语录（包括未激活的）"""
    cursor.execute("SELECT * FROM anti_fraud_messages ORDER BY id")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_transaction
def save_anti_fraud_message(conn, cursor, message: str) -> int:
    """保存防诈骗语录，返回语录ID"""
    cursor.execute(
        """
    INSERT INTO anti_fraud_messages (message, is_active)
    VALUES (?, 1)
    """,
        (message,),
    )
    return cursor.lastrowid


@db_transaction
def delete_anti_fraud_message(conn, cursor, message_id: int) -> bool:
    """删除防诈骗语录"""
    cursor.execute("DELETE FROM anti_fraud_messages WHERE id = ?", (message_id,))
    return cursor.rowcount > 0


@db_transaction
def toggle_anti_fraud_message(conn, cursor, message_id: int) -> bool:
    """切换防诈骗语录的激活状态"""
    cursor.execute(
        """
    UPDATE anti_fraud_messages 
    SET is_active = NOT is_active, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
    """,
        (message_id,),
    )
    return cursor.rowcount > 0


# ========== 公司宣传轮播语录操作 ==========


@db_query
def get_active_promotion_messages(conn, cursor) -> List[Dict]:
    """获取所有激活的公司宣传轮播语录（过滤空消息）"""
    cursor.execute("SELECT * FROM company_promotion_messages WHERE is_active = 1 ORDER BY id")
    rows = cursor.fetchall()
    # 过滤掉空消息或只有空白字符的消息
    result = []
    for row in rows:
        message = row["message"]
        if message and message.strip():  # 确保消息不为空且不只是空白字符
            result.append(dict(row))
    return result


@db_query
def get_all_promotion_messages(conn, cursor) -> List[Dict]:
    """获取所有公司宣传轮播语录（包括未激活的）"""
    cursor.execute("SELECT * FROM company_promotion_messages ORDER BY id")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_transaction
def save_promotion_message(conn, cursor, message: str) -> int:
    """保存公司宣传轮播语录，返回语录ID"""
    cursor.execute(
        """
    INSERT INTO company_promotion_messages (message, is_active)
    VALUES (?, 1)
    """,
        (message,),
    )
    return cursor.lastrowid


@db_transaction
def delete_promotion_message(conn, cursor, message_id: int) -> bool:
    """删除公司宣传轮播语录"""
    cursor.execute("DELETE FROM company_promotion_messages WHERE id = ?", (message_id,))
    return cursor.rowcount > 0


@db_transaction
def toggle_promotion_message(conn, cursor, message_id: int) -> bool:
    """切换公司宣传轮播语录的激活状态"""
    cursor.execute(
        """
    UPDATE company_promotion_messages 
    SET is_active = NOT is_active, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
    """,
        (message_id,),
    )
    return cursor.rowcount > 0


@db_query
def get_promotion_schedule(conn, cursor) -> Optional[Dict]:
    """获取公司宣传轮播发送计划（复用公告计划表结构）"""
    # 使用公告计划表存储，但可以扩展为独立表
    cursor.execute("SELECT * FROM announcement_schedule WHERE id = 1")
    row = cursor.fetchone()
    return dict(row) if row else None


# ========== 收入明细操作 ==========


@db_transaction
def record_income(
    conn,
    cursor,
    date: str,
    type: str,
    amount: float,
    group_id: Optional[str] = None,
    order_id: Optional[str] = None,
    order_date: Optional[str] = None,
    customer: Optional[str] = None,
    weekday_group: Optional[str] = None,
    note: Optional[str] = None,
    created_by: Optional[int] = None,
) -> int:
    """记录收入明细，返回收入记录ID"""
    # 使用北京时间作为 created_at
    tz_beijing = pytz.timezone("Asia/Shanghai")
    created_at = datetime.now(tz_beijing).strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        """
    INSERT INTO income_records (
        date, type, amount, group_id, order_id, order_date,
        customer, weekday_group, note, created_by, created_at, is_undone
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    """,
        (
            date,
            type,
            amount,
            group_id,
            order_id,
            order_date,
            customer,
            weekday_group,
            note,
            created_by,
            created_at,
        ),
    )
    return cursor.lastrowid


@db_query
def get_income_records(
    conn,
    cursor,
    start_date: str,
    end_date: str = None,
    type: Optional[str] = None,
    customer: Optional[str] = None,
    group_id: Optional[str] = None,
    order_id: Optional[str] = None,
    include_undone: bool = False,
) -> List[Dict]:
    """获取收入明细（支持多维度过滤）"""
    query = "SELECT * FROM income_records WHERE date >= ?"
    params = [start_date]

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    else:
        query += " AND date <= ?"
        params.append(start_date)

    if type:
        query += " AND type = ?"
        params.append(type)

    if customer:
        query += " AND customer = ?"
        params.append(customer)

    if group_id:
        query += " AND group_id = ?"
        params.append(group_id)

    if order_id:
        query += " AND order_id = ?"
        params.append(order_id)

    # 默认排除已撤销的记录，除非明确指定包含
    if not include_undone:
        query += " AND (is_undone IS NULL OR is_undone = 0)"

    query += " ORDER BY date DESC, created_at DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_interest_by_order_id(conn, cursor, order_id: str) -> Dict:
    """获取指定订单的所有利息收入汇总（排除已撤销的记录）"""
    cursor.execute(
        """
    SELECT 
        COUNT(*) as count,
        SUM(amount) as total_amount,
        MIN(date) as first_date,
        MAX(date) as last_date
    FROM income_records 
    WHERE order_id = ? AND type = 'interest' AND (is_undone IS NULL OR is_undone = 0)
    """,
        (order_id,),
    )

    row = cursor.fetchone()
    if row and row[0] > 0:
        return {
            "count": row[0],
            "total_amount": row[1] or 0.0,
            "first_date": row[2],
            "last_date": row[3],
        }
    return {"count": 0, "total_amount": 0.0, "first_date": None, "last_date": None}


@db_query
def get_all_interest_by_order_id(conn, cursor, order_id: str) -> List[Dict]:
    """获取指定订单的所有利息收入明细（排除已撤销的记录）"""
    cursor.execute(
        """
    SELECT * FROM income_records 
    WHERE order_id = ? AND type = 'interest' AND (is_undone IS NULL OR is_undone = 0)
    ORDER BY date ASC, created_at ASC
    """,
        (order_id,),
    )

    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_interests_by_order_ids(conn, cursor, order_ids: List[str]) -> Dict[str, List[Dict]]:
    """批量获取多个订单的利息收入明细（优化N+1查询）

    Args:
        order_ids: 订单ID列表

    Returns:
        字典，key为order_id，value为该订单的利息记录列表
    """
    if not order_ids:
        return {}

    # 使用IN查询批量获取（排除已撤销的记录）
    placeholders = ",".join(["?"] * len(order_ids))
    cursor.execute(
        f"""
    SELECT * FROM income_records 
    WHERE order_id IN ({placeholders}) AND type = 'interest' AND (is_undone IS NULL OR is_undone = 0)
    ORDER BY order_id, date ASC, created_at ASC
    """,
        order_ids,
    )

    rows = cursor.fetchall()

    # 按order_id分组
    result = {}
    for row in rows:
        order_id = row["order_id"]
        if order_id not in result:
            result[order_id] = []
        result[order_id].append(dict(row))

    # 确保所有order_id都有条目（即使没有利息记录）
    for order_id in order_ids:
        if order_id not in result:
            result[order_id] = []

    return result


@db_query
def get_all_valid_orders(conn, cursor) -> List[Dict]:
    """获取所有有效订单（normal和overdue状态）"""
    cursor.execute(
        """
    SELECT * FROM orders 
    WHERE state IN ('normal', 'overdue')
    ORDER BY date DESC, order_id DESC
    """
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_completed_orders_by_date(conn, cursor, date: str) -> List[Dict]:
    """获取指定日期完成的订单（通过updated_at判断，使用北京时间范围）"""
    start_time, end_time = get_date_range_for_query(date)
    # 注意：使用 <= 来包含23:59:59的数据
    cursor.execute(
        """
    SELECT * FROM orders 
    WHERE state = 'end' 
    AND updated_at >= ? AND updated_at <= ?
    ORDER BY updated_at DESC
    """,
        (start_time, end_time),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_breach_end_orders_by_date(conn, cursor, date: str) -> List[Dict]:
    """获取指定日期违约完成且有变动的订单（通过updated_at判断，使用北京时间范围）"""
    start_time, end_time = get_date_range_for_query(date)
    # 注意：使用 <= 来包含23:59:59的数据
    cursor.execute(
        """
    SELECT * FROM orders 
    WHERE state = 'breach_end' 
    AND updated_at >= ? AND updated_at <= ?
    ORDER BY updated_at DESC
    """,
        (start_time, end_time),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_new_orders_by_date(conn, cursor, date: str) -> List[Dict]:
    """获取指定日期新增的订单（通过created_at判断，使用北京时间范围）"""
    start_time, end_time = get_date_range_for_query(date)
    # 注意：使用 <= 来包含23:59:59的数据
    cursor.execute(
        """
    SELECT * FROM orders 
    WHERE created_at >= ? AND created_at <= ?
    ORDER BY created_at DESC
    """,
        (start_time, end_time),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_daily_interest_total(conn, cursor, date: str) -> float:
    """获取指定日期的利息收入总额（排除已撤销的记录）"""
    cursor.execute(
        """
    SELECT COALESCE(SUM(amount), 0) as total
    FROM income_records 
    WHERE date = ? AND type = 'interest' AND (is_undone IS NULL OR is_undone = 0)
    """,
        (date,),
    )
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] else 0.0


@db_query
def get_daily_expenses(conn, cursor, date: str) -> Dict:
    """获取指定日期的开销（公司开销+其他开销）"""
    cursor.execute(
        """
    SELECT 
        type,
        COALESCE(SUM(amount), 0) as total
    FROM expense_records 
    WHERE date = ?
    GROUP BY type
    """,
        (date,),
    )
    rows = cursor.fetchall()

    result = {"company_expenses": 0.0, "other_expenses": 0.0, "total": 0.0}

    for row in rows:
        expense_type = row[0]
        amount = float(row[1]) if row[1] else 0.0
        if expense_type == "company":
            result["company_expenses"] = amount
        elif expense_type == "other":
            result["other_expenses"] = amount
        result["total"] += amount

    return result


@db_query
def get_daily_summary(conn, cursor, date: str) -> Optional[Dict]:
    """获取指定日期的日切数据"""
    cursor.execute(
        """
    SELECT * FROM daily_summary 
    WHERE date = ?
    """,
        (date,),
    )
    row = cursor.fetchone()
    if row:
        return dict(row)
    return None


@db_transaction
def save_daily_summary(conn, cursor, date: str, data: Dict) -> bool:
    """保存日切数据"""
    try:
        cursor.execute(
            """
        INSERT OR REPLACE INTO daily_summary (
            date, new_orders_count, new_orders_amount,
            completed_orders_count, completed_orders_amount,
            breach_end_orders_count, breach_end_orders_amount,
            daily_interest, company_expenses, other_expenses,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                date,
                data.get("new_orders_count", 0),
                data.get("new_orders_amount", 0.0),
                data.get("completed_orders_count", 0),
                data.get("completed_orders_amount", 0.0),
                data.get("breach_end_orders_count", 0),
                data.get("breach_end_orders_amount", 0.0),
                data.get("daily_interest", 0.0),
                data.get("company_expenses", 0.0),
                data.get("other_expenses", 0.0),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e


@db_query
def get_customer_total_contribution(
    conn, cursor, customer: str, start_date: str = None, end_date: str = None
) -> Dict:
    """获取指定客户的总贡献（跨所有订单周期）

    参数:
        customer: 客户类型（'A'=新客户，'B'=老客户）
        start_date: 起始日期（可选，如果提供则只统计该日期之后的数据）
        end_date: 结束日期（可选，如果提供则只统计该日期之前的数据）

    返回:
        {
            'total_interest': 总利息收入,
            'total_completed': 总完成订单金额,
            'total_breach_end': 总违约完成金额,
            'total_principal_reduction': 总本金减少,
            'total_amount': 总贡献金额,
            'order_count': 订单数量,
            'interest_count': 利息收取次数,
            'first_order_date': 首次订单日期,
            'last_order_date': 最后订单日期
        }
    """
    # 构建基础查询条件
    income_conditions = ["customer = ?"]
    income_params = [customer.upper()]

    order_conditions = ["customer = ?"]
    order_params = [customer.upper()]

    if start_date:
        income_conditions.append("date >= ?")
        income_params.append(start_date)
        order_conditions.append("date >= ?")
        order_params.append(start_date)

    if end_date:
        income_conditions.append("date <= ?")
        income_params.append(end_date)
        order_conditions.append("date <= ?")
        order_params.append(end_date)

    income_where = " AND ".join(income_conditions)
    order_where = " AND ".join(order_conditions)

    # 查询收入汇总
    cursor.execute(
        f"""
    SELECT 
        type,
        COUNT(*) as count,
        SUM(amount) as total_amount
    FROM income_records 
    WHERE {income_where}
    GROUP BY type
    """,
        income_params,
    )

    income_rows = cursor.fetchall()

    # 初始化结果
    result = {
        "total_interest": 0.0,
        "total_completed": 0.0,
        "total_breach_end": 0.0,
        "total_principal_reduction": 0.0,
        "total_amount": 0.0,
        "interest_count": 0,
        "order_count": 0,
        "first_order_date": None,
        "last_order_date": None,
    }

    # 处理收入数据
    for row in income_rows:
        income_type = row[0]
        count = row[1]
        amount = row[2] or 0.0

        if income_type == "interest":
            result["total_interest"] = amount
            result["interest_count"] = count
        elif income_type == "completed":
            result["total_completed"] = amount
        elif income_type == "breach_end":
            result["total_breach_end"] = amount
        elif income_type == "principal_reduction":
            result["total_principal_reduction"] = amount

        result["total_amount"] += amount

    # 查询订单统计
    cursor.execute(
        f"""
    SELECT 
        COUNT(*) as order_count,
        MIN(date) as first_date,
        MAX(date) as last_date
    FROM orders 
    WHERE {order_where}
    """,
        order_params,
    )

    order_row = cursor.fetchone()
    if order_row:
        result["order_count"] = order_row[0] or 0
        result["first_order_date"] = order_row[1]
        result["last_order_date"] = order_row[2]

    return result


@db_query
def get_customer_orders_summary(
    conn, cursor, customer: str, start_date: str = None, end_date: str = None
) -> List[Dict]:
    """获取指定客户的所有订单及每笔订单的贡献汇总

    返回每个订单的详细信息，包括：
    - 订单基本信息
    - 该订单的利息总额
    - 该订单的完成金额
    - 该订单的总贡献
    """
    # 构建查询条件
    conditions = ["customer = ?"]
    params = [customer.upper()]

    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)

    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)

    where_clause = " AND ".join(conditions)

    # 查询所有订单
    cursor.execute(
        f"""
    SELECT * FROM orders 
    WHERE {where_clause}
    ORDER BY date DESC
    """,
        params,
    )

    order_rows = cursor.fetchall()
    orders = [dict(row) for row in order_rows]

    # 为每个订单查询收入汇总
    result = []
    for order in orders:
        order_id = order["order_id"]

        # 查询该订单的收入汇总
        cursor.execute(
            """
        SELECT 
            type,
            COUNT(*) as count,
            SUM(amount) as total_amount
        FROM income_records 
        WHERE order_id = ?
        GROUP BY type
        """,
            (order_id,),
        )

        income_rows = cursor.fetchall()

        order_interest = 0.0
        order_completed = 0.0
        order_breach_end = 0.0
        order_principal_reduction = 0.0
        order_total = 0.0

        for row in income_rows:
            income_type = row[0]
            amount = row[2] or 0.0

            if income_type == "interest":
                order_interest = amount
            elif income_type == "completed":
                order_completed = amount
            elif income_type == "breach_end":
                order_breach_end = amount
            elif income_type == "principal_reduction":
                order_principal_reduction = amount

            order_total += amount

        result.append(
            {
                "order": order,
                "interest": order_interest,
                "completed": order_completed,
                "breach_end": order_breach_end,
                "principal_reduction": order_principal_reduction,
                "total_contribution": order_total,
            }
        )

    return result


@db_query
def get_income_summary_by_type(
    conn, cursor, start_date: str, end_date: str = None, group_id: Optional[str] = None
) -> Dict:
    """按收入类型和客户类型汇总"""
    query = """
    SELECT 
        type,
        customer,
        COUNT(*) as count,
        SUM(amount) as total_amount
    FROM income_records 
    WHERE date >= ? AND date <= ?
    """
    params = [start_date, end_date or start_date]

    if group_id:
        query += " AND group_id = ?"
        params.append(group_id)

    query += " GROUP BY type, customer ORDER BY type, customer"

    cursor.execute(query, params)
    rows = cursor.fetchall()

    # 构建汇总字典
    summary = {}
    for row in rows:
        type_name = row[0]
        customer_type = row[1] or "None"
        count = row[2]
        total = row[3]

        if type_name not in summary:
            summary[type_name] = {}
        summary[type_name][customer_type] = {"count": count, "total": total}

    return summary


@db_query
def get_income_summary_by_group(conn, cursor, start_date: str, end_date: str = None) -> Dict:
    """按归属ID汇总收入"""
    query = """
    SELECT 
        group_id,
        COUNT(*) as count,
        SUM(amount) as total_amount
    FROM income_records 
    WHERE date >= ? AND date <= ?
    GROUP BY group_id
    ORDER BY total_amount DESC
    """
    params = [start_date, end_date or start_date]

    cursor.execute(query, params)
    rows = cursor.fetchall()

    summary = {}
    for row in rows:
        group_id = row[0] or "NULL"
        count = row[1]
        total = row[2]
        summary[group_id] = {"count": count, "total": total}

    return summary


# ========== 操作历史（撤销功能） ==========


@db_transaction
def record_operation(
    conn, cursor, user_id: int, operation_type: str, operation_data: Dict, chat_id: int
) -> int:
    """记录操作历史，返回操作ID（使用北京时间）"""
    # 使用北京时间作为 created_at
    import pytz

    tz_beijing = pytz.timezone("Asia/Shanghai")
    created_at = datetime.now(tz_beijing).strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        """
    INSERT INTO operation_history (user_id, chat_id, operation_type, operation_data, is_undone, created_at)
    VALUES (?, ?, ?, ?, 0, ?)
    """,
        (
            user_id,
            chat_id,
            operation_type,
            json.dumps(operation_data, ensure_ascii=False),
            created_at,
        ),
    )
    return cursor.lastrowid


@db_query
def get_last_operation(
    conn, cursor, user_id: int, chat_id: int, date: Optional[str] = None
) -> Optional[Dict]:
    """获取用户在指定聊天环境中的最后一个未撤销的操作

    Args:
        user_id: 用户ID
        chat_id: 聊天环境ID
        date: 日期字符串（YYYY-MM-DD），如果提供则只返回该日期的操作，如果为None则返回当天的操作
    """
    from utils.date_helpers import get_daily_period_date

    # 如果没有提供日期，使用当天日期
    if date is None:
        date = get_daily_period_date()

    cursor.execute(
        """
    SELECT * FROM operation_history 
    WHERE user_id = ? AND chat_id = ? AND is_undone = 0 AND DATE(created_at) = ?
    ORDER BY created_at DESC, id DESC
    LIMIT 1
    """,
        (user_id, chat_id, date),
    )
    row = cursor.fetchone()
    if row:
        result = dict(row)
        result["operation_data"] = json.loads(result["operation_data"])
        return result
    return None


@db_transaction
def mark_operation_undone(conn, cursor, operation_id: int) -> bool:
    """标记操作为已撤销"""
    cursor.execute(
        """
    UPDATE operation_history 
    SET is_undone = 1
    WHERE id = ?
    """,
        (operation_id,),
    )
    return cursor.rowcount > 0


@db_transaction
def delete_operation(conn, cursor, operation_id: int) -> bool:
    """强制删除操作记录（不可恢复）"""
    cursor.execute("DELETE FROM operation_history WHERE id = ?", (operation_id,))
    return cursor.rowcount > 0


@db_query
def get_operation_by_id(conn, cursor, operation_id: int) -> Optional[Dict]:
    """根据ID获取操作记录"""
    cursor.execute("SELECT * FROM operation_history WHERE id = ?", (operation_id,))
    row = cursor.fetchone()
    if row:
        result = dict(row)
        result["operation_data"] = json.loads(result["operation_data"])
        return result
    return None


@db_query
def get_recent_operations(conn, cursor, user_id: int, limit: int = 10) -> List[Dict]:
    """获取用户最近的操作历史"""
    cursor.execute(
        """
    SELECT * FROM operation_history 
    WHERE user_id = ?
    ORDER BY created_at DESC, id DESC
    LIMIT ?
    """,
        (user_id, limit),
    )
    rows = cursor.fetchall()
    result = []
    for row in rows:
        op = dict(row)
        op["operation_data"] = json.loads(op["operation_data"])
        result.append(op)
    return result


@db_query
def get_operations_by_date(conn, cursor, date: str, user_id: Optional[int] = None) -> List[Dict]:
    """获取指定日期的操作历史

    Args:
        date: 日期字符串，格式 'YYYY-MM-DD'
        user_id: 可选的用户ID，如果提供则只返回该用户的操作

    Returns:
        操作历史列表，每个操作包含完整信息
    """
    if user_id:
        cursor.execute(
            """
        SELECT * FROM operation_history 
        WHERE DATE(created_at) = ? AND user_id = ?
        ORDER BY created_at ASC, id ASC
        """,
            (date, user_id),
        )
    else:
        cursor.execute(
            """
        SELECT * FROM operation_history 
        WHERE DATE(created_at) = ?
        ORDER BY created_at ASC, id ASC
        """,
            (date,),
        )

    rows = cursor.fetchall()
    result = []
    for row in rows:
        op = dict(row)
        try:
            op["operation_data"] = json.loads(op["operation_data"])
        except (json.JSONDecodeError, TypeError):
            op["operation_data"] = {}
        result.append(op)
    return result


@db_query
def get_daily_operations_summary(conn, cursor, date: str) -> Dict:
    """获取指定日期的操作汇总统计

    Args:
        date: 日期字符串，格式 'YYYY-MM-DD'

    Returns:
        包含统计信息的字典：
        - total_count: 总操作数
        - by_type: 按操作类型统计
        - by_user: 按用户统计
        - undone_count: 已撤销的操作数
    """
    # 获取总数
    cursor.execute(
        """
    SELECT COUNT(*) FROM operation_history 
    WHERE DATE(created_at) = ?
    """,
        (date,),
    )
    total_count = cursor.fetchone()[0] or 0

    # 按操作类型统计
    cursor.execute(
        """
    SELECT operation_type, COUNT(*) as count
    FROM operation_history 
    WHERE DATE(created_at) = ?
    GROUP BY operation_type
    ORDER BY count DESC
    """,
        (date,),
    )
    by_type = {row[0]: row[1] for row in cursor.fetchall()}

    # 按用户统计
    cursor.execute(
        """
    SELECT user_id, COUNT(*) as count
    FROM operation_history 
    WHERE DATE(created_at) = ?
    GROUP BY user_id
    ORDER BY count DESC
    """,
        (date,),
    )
    by_user = {row[0]: row[1] for row in cursor.fetchall()}

    # 已撤销的操作数
    cursor.execute(
        """
    SELECT COUNT(*) FROM operation_history 
    WHERE DATE(created_at) = ? AND is_undone = 1
    """,
        (date,),
    )
    undone_count = cursor.fetchone()[0] or 0

    return {
        "date": date,
        "total_count": total_count,
        "by_type": by_type,
        "by_user": by_user,
        "undone_count": undone_count,
    }


# ========== 支付账号余额历史操作 ==========


@db_transaction
def record_payment_balance_history(
    conn, cursor, account_id: int, account_type: str, balance: float, date: str
) -> int:
    """记录支付账号余额历史

    Args:
        account_id: 账号ID
        account_type: 账号类型（gcash/paymaya）
        balance: 余额
        date: 日期字符串，格式 'YYYY-MM-DD'

    Returns:
        记录ID
    """
    # 检查当天是否已有记录，如果有则更新，否则插入
    cursor.execute(
        """
    SELECT id FROM payment_balance_history 
    WHERE account_id = ? AND date = ?
    """,
        (account_id, date),
    )
    existing = cursor.fetchone()

    if existing:
        # 更新现有记录
        cursor.execute(
            """
        UPDATE payment_balance_history 
        SET balance = ?, created_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
            (balance, existing[0]),
        )
        return existing[0]
    else:
        # 插入新记录
        cursor.execute(
            """
        INSERT INTO payment_balance_history (account_id, account_type, balance, date)
        VALUES (?, ?, ?, ?)
        """,
            (account_id, account_type, balance, date),
        )
        return cursor.lastrowid


@db_query
def get_balance_history_by_date(conn, cursor, date: str) -> List[Dict]:
    """获取指定日期的所有账号余额历史

    Args:
        date: 日期字符串，格式 'YYYY-MM-DD'

    Returns:
        余额历史记录列表
    """
    cursor.execute(
        """
    SELECT * FROM payment_balance_history 
    WHERE date = ?
    ORDER BY account_type, account_id
    """,
        (date,),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_balance_summary_by_date(conn, cursor, date: str) -> Dict:
    """获取指定日期的余额汇总统计

    Args:
        date: 日期字符串，格式 'YYYY-MM-DD'

    Returns:
        包含总余额和每个账号余额的字典
    """
    cursor.execute(
        """
    SELECT account_type, SUM(balance) as total_balance, COUNT(*) as account_count
    FROM payment_balance_history 
    WHERE date = ?
    GROUP BY account_type
    """,
        (date,),
    )

    rows = cursor.fetchall()
    result = {"date": date, "gcash_total": 0.0, "paymaya_total": 0.0, "total": 0.0, "accounts": []}

    for row in rows:
        account_type = row[0]
        total_balance = row[1] or 0.0
        account_count = row[2] or 0

        if account_type == "gcash":
            result["gcash_total"] = total_balance
        elif account_type == "paymaya":
            result["paymaya_total"] = total_balance

        result["accounts"].append(
            {
                "account_type": account_type,
                "total_balance": total_balance,
                "account_count": account_count,
            }
        )

    result["total"] = result["gcash_total"] + result["paymaya_total"]

    # 获取每个账号的详细信息
    cursor.execute(
        """
    SELECT pb.*, pa.account_number, pa.account_name
    FROM payment_balance_history pb
    LEFT JOIN payment_accounts pa ON pb.account_id = pa.id
    WHERE pb.date = ?
    ORDER BY pb.account_type, pb.account_id
    """,
        (date,),
    )

    rows = cursor.fetchall()
    result["account_details"] = [dict(row) for row in rows]

    return result


@db_query
def get_operations_by_filters(
    conn,
    cursor,
    date: Optional[str] = None,
    user_id: Optional[int] = None,
    operation_type: Optional[str] = None,
    limit: int = 100,
) -> List[Dict]:
    """根据多个条件筛选操作历史

    Args:
        date: 日期字符串，格式 'YYYY-MM-DD'，可选
        user_id: 用户ID，可选
        operation_type: 操作类型，可选
        limit: 返回的最大记录数

    Returns:
        操作历史列表
    """
    conditions = []
    params = []

    if date:
        conditions.append("DATE(created_at) = ?")
        params.append(date)

    if user_id:
        conditions.append("user_id = ?")
        params.append(user_id)

    if operation_type:
        conditions.append("operation_type = ?")
        params.append(operation_type)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    params.append(limit)

    query = f"""
    SELECT * FROM operation_history 
    WHERE {where_clause}
    ORDER BY created_at DESC, id DESC
    LIMIT ?
    """

    cursor.execute(query, params)
    rows = cursor.fetchall()
    result = []
    for row in rows:
        op = dict(row)
        try:
            op["operation_data"] = json.loads(op["operation_data"])
        except (json.JSONDecodeError, TypeError):
            op["operation_data"] = {}
        result.append(op)
    return result


@db_transaction
def update_operation_data(conn, cursor, operation_id: int, new_operation_data: Dict) -> bool:
    """更新操作记录的数据（用于管理员修正）

    Args:
        operation_id: 操作记录ID
        new_operation_data: 新的操作数据字典

    Returns:
        是否更新成功
    """
    cursor.execute(
        """
    UPDATE operation_history 
    SET operation_data = ?
    WHERE id = ?
    """,
        (json.dumps(new_operation_data, ensure_ascii=False), operation_id),
    )
    return cursor.rowcount > 0


# ========== 基准报表操作 ==========


@db_query
def check_baseline_exists(conn, cursor) -> bool:
    """检查基准日期是否存在"""
    cursor.execute("SELECT COUNT(*) FROM baseline_report WHERE id = 1")
    count = cursor.fetchone()[0]
    return count > 0


@db_query
def get_baseline_date(conn, cursor) -> Optional[str]:
    """获取基准日期"""
    cursor.execute("SELECT baseline_date FROM baseline_report WHERE id = 1")
    row = cursor.fetchone()
    return row[0] if row else None


@db_transaction
def save_baseline_date(conn, cursor, date: str) -> bool:
    """保存基准日期（第一次执行时）"""
    try:
        # 检查是否已存在
        cursor.execute("SELECT COUNT(*) FROM baseline_report WHERE id = 1")
        exists = cursor.fetchone()[0] > 0

        if exists:
            # 更新
            cursor.execute(
                """
            UPDATE baseline_report 
            SET baseline_date = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
                (date,),
            )
        else:
            # 插入
            cursor.execute(
                """
            INSERT INTO baseline_report (id, baseline_date)
            VALUES (1, ?)
            """,
                (date,),
            )
        return True
    except Exception as e:
        logger.error(f"保存基准日期失败: {e}", exc_info=True)
        return False


@db_query
def get_incremental_orders(conn, cursor, baseline_date: str) -> List[Dict]:
    """获取基准日期之后的所有订单（创建或更新）"""
    cursor.execute(
        """
    SELECT * FROM orders 
    WHERE date >= ? OR updated_at >= ?
    ORDER BY date ASC, order_id ASC
    """,
        (baseline_date, f"{baseline_date} 00:00:00"),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_incremental_orders_with_details(conn, cursor, baseline_date: str) -> List[Dict]:
    """获取增量订单及其详细信息（优化批量查询）"""
    # 获取增量订单
    cursor.execute(
        """
    SELECT * FROM orders 
    WHERE date >= ? OR updated_at >= ?
    ORDER BY date ASC, order_id ASC
    """,
        (baseline_date, f"{baseline_date} 00:00:00"),
    )
    order_rows = cursor.fetchall()
    orders = [dict(row) for row in order_rows]

    if not orders:
        return []

    # 批量获取所有订单的利息和本金归还记录（优化N+1查询）
    order_ids = [order["order_id"] for order in orders]
    placeholders = ",".join(["?"] * len(order_ids))

    # 批量获取利息记录（排除已撤销的记录）
    cursor.execute(
        f"""
    SELECT * FROM income_records 
    WHERE order_id IN ({placeholders}) AND type = 'interest' AND date >= ? 
    AND (is_undone IS NULL OR is_undone = 0)
    ORDER BY order_id, date ASC, created_at ASC
    """,
        order_ids + [baseline_date],
    )
    interest_rows = cursor.fetchall()

    # 批量获取本金归还记录（排除已撤销的记录）
    cursor.execute(
        f"""
    SELECT order_id, SUM(amount) as total_principal_reduction
    FROM income_records 
    WHERE order_id IN ({placeholders}) AND type = 'principal_reduction' AND date >= ?
    AND (is_undone IS NULL OR is_undone = 0)
    GROUP BY order_id
    """,
        order_ids + [baseline_date],
    )
    principal_rows = cursor.fetchall()

    # 构建映射表
    interests_map = {}
    for row in interest_rows:
        order_id = row["order_id"]
        if order_id not in interests_map:
            interests_map[order_id] = []
        interests_map[order_id].append(dict(row))

    principal_map = {row[0]: (row[1] if row[1] else 0.0) for row in principal_rows}

    # 组装结果
    result = []
    for order in orders:
        order_id = order["order_id"]

        # 获取该订单的利息记录
        interests = interests_map.get(order_id, [])

        # 获取该订单的本金归还总额
        principal_reduction = principal_map.get(order_id, 0.0)

        # 计算利息总数
        total_interest = sum(i["amount"] for i in interests)

        # 生成备注
        note_parts = []
        if order.get("created_at", "")[:10] >= baseline_date:
            note_parts.append("新订单")
        if principal_reduction > 0:
            note_parts.append(f"归还本金 {principal_reduction:.2f}元")
        if order["state"] == "end":
            note_parts.append("订单完成")
        elif order["state"] == "breach_end":
            note_parts.append("违约完成")
        note = "→".join(note_parts) if note_parts else ""

        result.append(
            {
                **order,
                "interests": interests,
                "total_interest": total_interest,
                "principal_reduction": principal_reduction,
                "note": note,
            }
        )

    return result


# ========== 增量报表合并记录操作 ==========


@db_query
def check_merge_record_exists(conn, cursor, merge_date: str) -> bool:
    """检查指定日期的合并记录是否存在"""
    cursor.execute(
        "SELECT COUNT(*) FROM incremental_merge_records WHERE merge_date = ?", (merge_date,)
    )
    count = cursor.fetchone()[0]
    return count > 0


@db_query
def get_merge_record(conn, cursor, merge_date: str) -> Optional[Dict]:
    """获取指定日期的合并记录"""
    cursor.execute(
        """
    SELECT * FROM incremental_merge_records 
    WHERE merge_date = ?
    """,
        (merge_date,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


@db_query
def get_all_merge_records(conn, cursor) -> List[Dict]:
    """获取所有合并记录"""
    cursor.execute(
        """
    SELECT * FROM incremental_merge_records 
    ORDER BY merged_at DESC
    """
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_transaction
def save_merge_record(
    conn,
    cursor,
    merge_date: str,
    baseline_date: str,
    orders_count: int,
    total_amount: float,
    total_interest: float,
    total_expenses: float,
    merged_by: Optional[int] = None,
) -> bool:
    """保存合并记录"""
    try:
        cursor.execute(
            """
        INSERT INTO incremental_merge_records 
        (merge_date, baseline_date, orders_count, total_amount, total_interest, total_expenses, merged_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                merge_date,
                baseline_date,
                orders_count,
                total_amount,
                total_interest,
                total_expenses,
                merged_by,
            ),
        )
        return True
    except sqlite3.IntegrityError:
        # 如果记录已存在，更新记录
        cursor.execute(
            """
        UPDATE incremental_merge_records 
        SET baseline_date = ?, orders_count = ?, total_amount = ?, 
            total_interest = ?, total_expenses = ?, merged_by = ?, merged_at = CURRENT_TIMESTAMP
        WHERE merge_date = ?
        """,
            (
                baseline_date,
                orders_count,
                total_amount,
                total_interest,
                total_expenses,
                merged_by,
                merge_date,
            ),
        )
        return True
    except Exception as e:
        logger.error(f"保存合并记录失败: {e}", exc_info=True)
        return False
