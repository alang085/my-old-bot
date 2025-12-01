"""回调处理器模块 - 统一导出所有回调处理函数"""
import os
import sys
from pathlib import Path

# 确保项目根目录在 Python 路径中
# 这样子模块在导入时能找到 handlers, decorators, utils 等模块
# ⚠️ 必须在所有导入语句之前执行！否则会导致 ModuleNotFoundError
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 导入所有回调处理器
# 主回调（路由）
from .main_callback import button_callback

# 报表回调
from .report_callbacks import handle_report_callback

# 搜索回调
from .search_callbacks import handle_search_callback

# 订单回调
from .order_callbacks import handle_order_action_callback

# 支付账户回调
from .payment_callbacks import handle_payment_callback

# 定时播报回调
from .schedule_callbacks import handle_schedule_callback


__all__ = [
    # 主回调（路由）
    'button_callback',
    # 业务回调
    'handle_report_callback',
    'handle_search_callback',
    'handle_order_action_callback',
    'handle_payment_callback',
    'handle_schedule_callback'
]
