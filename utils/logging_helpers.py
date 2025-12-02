"""日志工具函数 - 支持北京时区的日志格式化"""
import logging
from datetime import datetime
import pytz
import time


class BeijingTimeFormatter(logging.Formatter):
    """使用北京时间的日志格式化器"""
    
    def __init__(self, fmt=None, datefmt=None):
        super().__init__(fmt, datefmt)
        self.tz_beijing = pytz.timezone('Asia/Shanghai')
    
    def converter(self, timestamp):
        """时间戳转换器 - 将UTC时间戳转换为北京时区的元组"""
        # 将时间戳转换为UTC时间的datetime
        dt_utc = datetime.utcfromtimestamp(timestamp)
        dt_utc = pytz.utc.localize(dt_utc)
        # 转换为北京时间
        dt_beijing = dt_utc.astimezone(self.tz_beijing)
        # 返回时间元组
        return dt_beijing.timetuple()
    
    def formatTime(self, record, datefmt=None):
        """格式化时间戳为北京时间"""
        # 使用converter转换时区
        ct = self.converter(record.created)
        t = time.struct_time(ct)
        
        if datefmt:
            s = time.strftime(datefmt, t)
        else:
            # 默认格式：YYYY-MM-DD HH:MM:SS,mmm
            s = time.strftime('%Y-%m-%d %H:%M:%S', t)
            s = '%s,%03d' % (s, record.msecs)
        return s


def setup_beijing_logging(level=logging.INFO):
    """
    配置使用北京时间的日志系统
    
    Args:
        level: 日志级别，默认为 INFO
    """
    # 创建格式化器
    formatter = BeijingTimeFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 清除现有的处理器
    root_logger.handlers.clear()
    
    # 添加新的处理器
    root_logger.addHandler(console_handler)
    
    return root_logger

