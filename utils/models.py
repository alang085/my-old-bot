"""Pydantic数据模型定义

用于数据验证和类型安全，简化重复的验证逻辑
"""

from datetime import date, datetime
from typing import Literal, Optional

try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    # 简单的替代实现
    class BaseModel:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
        
        def to_dict(self):
            return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
        
        @classmethod
        def model_validate(cls, data):
            return cls(**data)
    
    def Field(*args, **kwargs):
        return None
    
    def field_validator(*args, **kwargs):
        def decorator(func):
            return func
        return decorator

# ========== 订单相关模型 ==========


class OrderModel(BaseModel):
    """订单数据模型"""

    order_id: str = Field(..., description="订单ID")
    group_id: str = Field(..., description="归属ID")
    chat_id: int = Field(..., description="聊天ID")
    date: str = Field(..., description="订单日期，格式：YYYY-MM-DD HH:MM:SS")
    weekday_group: str = Field(..., description="星期分组（一、二、三、四、五、六、日）")
    customer: Literal["A", "B"] = Field(..., description="客户类型：A=新客户，B=老客户")
    amount: float = Field(..., gt=0, description="订单金额，必须大于0")
    state: Literal["normal", "overdue", "breach", "end", "breach_end"] = Field(
        ..., description="订单状态"
    )

    @field_validator("weekday_group")
    @classmethod
    def validate_weekday_group(cls, v: str) -> str:  # noqa: ARG002
        """验证星期分组"""
        valid_weekdays = ["一", "二", "三", "四", "五", "六", "日"]
        if v not in valid_weekdays:
            raise ValueError(f"星期分组必须是 {valid_weekdays} 之一，当前值: {v}")
        return v

    @field_validator("date")
    @classmethod
    def validate_date_format(cls, v: str) -> str:  # noqa: ARG002
        """验证日期格式"""
        try:
            # 尝试解析日期部分
            date_part = v.split()[0] if " " in v else v
            datetime.strptime(date_part, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError(f"日期格式错误，期望格式：YYYY-MM-DD HH:MM:SS，当前值: {v}")

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:  # noqa: ARG002
        """验证金额"""
        if v <= 0:
            raise ValueError(f"订单金额必须大于0，当前值: {v}")
        return v

    class Config:
        """Pydantic配置"""

        # 允许从字典创建
        from_attributes = True
        # 允许额外字段（向后兼容）
        extra = "allow"


class OrderStateModel(BaseModel):
    """订单状态验证模型"""

    state: Literal["normal", "overdue", "breach", "end", "breach_end"]

    def can_transition_to(self, target_state: str) -> bool:
        """检查是否可以转换到目标状态"""
        valid_transitions = {
            "normal": ["overdue", "breach", "end"],
            "overdue": ["normal", "breach", "end"],
            "breach": ["breach_end"],
            "end": [],  # 已完成，不能转换
            "breach_end": [],  # 已完成，不能转换
        }
        return target_state in valid_transitions.get(self.state, [])

    def can_complete(self) -> bool:
        """检查是否可以完成（normal或overdue可以完成）"""
        return self.state in ("normal", "overdue")

    def can_breach_end(self) -> bool:
        """检查是否可以违约完成（只有breach可以违约完成）"""
        return self.state == "breach"


class OrderCreateModel(BaseModel):
    """创建订单的数据模型"""

    order_id: str
    group_id: str = "S01"  # 默认归属
    chat_id: int
    date: str  # 格式：YYYY-MM-DD HH:MM:SS
    weekday_group: str
    customer: Literal["A", "B"]
    amount: float = Field(..., gt=0)
    state: Literal["normal", "overdue", "breach", "end", "breach_end"] = "normal"

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:  # noqa: ARG002
        """验证金额"""
        if v <= 0:
            raise ValueError(f"订单金额必须大于0，当前值: {v}")
        return round(v, 2)  # 保留2位小数

    def to_dict(self) -> dict:
        """转换为字典（用于数据库操作）"""
        return {
            "order_id": getattr(self, 'order_id', ''),
            "group_id": getattr(self, 'group_id', 'S01'),
            "chat_id": getattr(self, 'chat_id', 0),
            "date": getattr(self, 'date', ''),
            "weekday_group": getattr(self, 'weekday_group', ''),
            "customer": getattr(self, 'customer', 'A'),
            "amount": round(getattr(self, 'amount', 0.0), 2),
            "state": getattr(self, 'state', 'normal'),
        }


# ========== 金额相关模型 ==========


class AmountModel(BaseModel):
    """金额验证模型"""

    amount: float = Field(..., gt=0, description="金额，必须大于0")

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:  # noqa: ARG002
        """验证金额"""
        if v <= 0:
            raise ValueError(f"金额必须大于0，当前值: {v}")
        return round(v, 2)  # 保留2位小数

    def __float__(self) -> float:
        """转换为float"""
        return self.amount


# ========== 日期相关模型 ==========


class DateModel(BaseModel):
    """日期验证模型"""

    date: str = Field(..., description="日期字符串，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")

    @field_validator("date")
    @classmethod
    def validate_date(cls, v: str) -> str:  # noqa: ARG002
        """验证日期格式"""
        try:
            # 尝试解析日期部分
            date_part = v.split()[0] if " " in v else v
            datetime.strptime(date_part, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError(
                f"日期格式错误，期望格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS，当前值: {v}"
            )

    def to_date(self) -> date:
        """转换为date对象"""
        date_part = self.date.split()[0] if " " in self.date else self.date
        return datetime.strptime(date_part, "%Y-%m-%d").date()


# ========== 辅助函数 ==========


def validate_order(order_dict: dict) -> OrderModel:
    """验证订单字典，返回OrderModel（兼容模式）

    Args:
        order_dict: 订单字典

    Returns:
        OrderModel: 验证后的订单模型

    Raises:
        ValueError: 如果验证失败
    """
    try:
        if PYDANTIC_AVAILABLE:
            return OrderModel.model_validate(order_dict)
        else:
            return OrderModel(**order_dict)
    except Exception as e:
        raise ValueError(f"订单数据验证失败: {e}")


def validate_order_state(
    order_dict: dict, allowed_states: Optional[tuple] = None
) -> OrderStateModel:
    """验证订单状态

    Args:
        order_dict: 订单字典
        allowed_states: 允许的状态列表，如果提供则检查状态是否在列表中

    Returns:
        OrderStateModel: 验证后的状态模型

    Raises:
        ValueError: 如果验证失败
    """
    if not order_dict:
        raise ValueError("订单不存在")

    state = order_dict.get("state")
    if not state:
        raise ValueError("订单缺少状态信息")

    state_model = OrderStateModel(state=state)

    if allowed_states and state not in allowed_states:
        raise ValueError(f"订单状态必须是 {allowed_states} 之一，当前状态: {state}")

    return state_model


def validate_amount(amount: float, min_value: float = 0.01) -> float:
    """验证金额（兼容模式：不依赖 pydantic）

    Args:
        amount: 金额
        min_value: 最小金额，默认0.01

    Returns:
        float: 验证后的金额（保留2位小数）

    Raises:
        ValueError: 如果验证失败
    """
    if amount <= 0:
        raise ValueError(f"金额必须大于0，当前值: {amount}")
    if amount < min_value:
        raise ValueError(f"金额必须大于等于 {min_value}，当前值: {amount}")
    return round(amount, 2)
