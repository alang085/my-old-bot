"""日期相关工具函数"""

from datetime import datetime, timedelta
from typing import Optional

import pytz

from constants import DAILY_CUTOFF_HOUR

# 北京时区
BEIJING_TZ = pytz.timezone("Asia/Shanghai")
UTC_TZ = pytz.UTC

# 时区修复基准日期：2024-12-02
# 在此之前的数据可能是UTC时间，之后的数据是北京时间
# 注意：这是一个保守的阈值，实际应用中可能需要根据具体情况调整
TIMEZONE_FIX_DATE = datetime(2024, 12, 2, 0, 0, 0, tzinfo=BEIJING_TZ).date()


def get_daily_period_date() -> str:
    """获取当前日结周期对应的日期（每天23:00日切）"""
    tz = pytz.timezone("Asia/Shanghai")
    now = datetime.now(tz)
    current_hour = now.hour

    # 如果当前时间 >= 23:00，算作明天
    if current_hour >= DAILY_CUTOFF_HOUR:
        period_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        period_date = now.strftime("%Y-%m-%d")

    return period_date


def parse_datetime_str(datetime_str: str) -> Optional[datetime]:
    """
    解析时间字符串，返回datetime对象（时区感知）

    Args:
        datetime_str: 时间字符串，可能是ISO格式或SQLite格式

    Returns:
        datetime对象（带时区信息），如果解析失败返回None
    """
    if not datetime_str or datetime_str == "未知":
        return None

    try:
        dt = None

        # ISO格式（可能包含时区信息）
        if "T" in datetime_str:
            try:
                # 尝试标准ISO格式解析
                dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
            except Exception:
                # 清理字符串后解析
                datetime_str_clean = datetime_str.split(".")[0].split("+")[0].split("Z")[0]
                dt = datetime.strptime(datetime_str_clean, "%Y-%m-%dT%H:%M:%S")
        else:
            # SQLite格式 (2024-12-02 15:00:00)
            if "." in datetime_str:
                dt = datetime.strptime(datetime_str.split(".")[0], "%Y-%m-%d %H:%M:%S")
            else:
                # 只包含日期部分
                if len(datetime_str) == 10:
                    dt = datetime.strptime(datetime_str, "%Y-%m-%d")
                else:
                    dt = datetime.strptime(datetime_str[:19], "%Y-%m-%d %H:%M:%S")

        # 如果没有时区信息，根据日期判断时区
        if dt.tzinfo is None:
            record_date = dt.date()
            # 判断是新数据（北京时间）还是旧数据（可能是UTC）
            if record_date >= TIMEZONE_FIX_DATE:
                # 新数据：假设是北京时间
                dt = BEIJING_TZ.localize(dt)
            else:
                # 旧数据：可能是UTC时间，但为了安全，我们假设是北京时间
                # 如果需要更精确的处理，可以查看实际数据的分布
                dt = BEIJING_TZ.localize(dt)

        return dt

    except Exception:
        return None


def datetime_to_beijing_str(dt: datetime) -> str:
    """
    将datetime对象转换为北京时间字符串

    Args:
        dt: datetime对象（可以是任何时区）

    Returns:
        北京时间字符串 (YYYY-MM-DD HH:MM:SS)
    """
    if dt is None:
        return ""

    # 如果有时区信息，转换为北京时间
    if dt.tzinfo is not None:
        dt_beijing = dt.astimezone(BEIJING_TZ)
    else:
        # 如果没有时区信息，假设已经是北京时间
        dt_beijing = BEIJING_TZ.localize(dt)

    return dt_beijing.strftime("%Y-%m-%d %H:%M:%S")


def datetime_str_to_beijing_str(datetime_str: str) -> str:
    """
    将时间字符串转换为北京时间字符串（统一的时间显示函数）

    Args:
        datetime_str: 时间字符串，可能是ISO格式或SQLite格式

    Returns:
        北京时间字符串 (YYYY-MM-DD HH:MM:SS)，如果解析失败返回原字符串
    """
    if not datetime_str or datetime_str == "未知":
        return datetime_str

    dt = parse_datetime_str(datetime_str)
    if dt is None:
        return datetime_str

    return datetime_to_beijing_str(dt)


def get_date_range_for_query(date: str) -> tuple[str, str]:
    """
    获取日期查询的起始和结束时间（北京时间）

    用于在数据库中查询指定日期的数据
    返回的是该日期在北京时间下的完整时间范围

    Args:
        date: 日期字符串 (YYYY-MM-DD)

    Returns:
        (start_datetime, end_datetime) 北京时间字符串元组
        start_datetime: YYYY-MM-DD 00:00:00 (北京时间)
        end_datetime: YYYY-MM-DD 23:59:59 (北京时间)
    """
    # 解析日期并设置为北京时区的开始时间
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    start_dt = BEIJING_TZ.localize(date_obj.replace(hour=0, minute=0, second=0))
    end_dt = BEIJING_TZ.localize(date_obj.replace(hour=23, minute=59, second=59))

    # 返回SQLite格式的字符串（不带时区信息，因为数据库存储的是本地时间）
    # 注意：这里假设数据库中存储的时间已经是北京时间的字符串表示
    return start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")


def get_now_beijing() -> datetime:
    """
    获取当前北京时间（时区感知）

    Returns:
        当前北京时间的datetime对象
    """
    return datetime.now(BEIJING_TZ)


def get_today_beijing() -> str:
    """
    获取今天在北京时区下的日期字符串

    Returns:
        日期字符串 (YYYY-MM-DD)
    """
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
