"""配置管理模块

使用 Pydantic Settings 统一管理配置，提供环境变量验证和默认值。
"""

import os
from typing import List, Optional

try:
    from pydantic import Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    def Field(*args, **kwargs):
        return None
    
    def field_validator(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    class BaseSettings:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
    
    class SettingsConfigDict:
        def __init__(self, **kwargs):
            pass


class BotSettings(BaseSettings):
    """机器人配置设置"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Telegram Bot 配置
    bot_token: str = Field(..., description="Telegram Bot Token")
    admin_user_ids: str = Field(..., description="管理员用户ID列表（逗号分隔）")

    # 数据目录
    data_dir: Optional[str] = Field(
        default=None, description="数据目录路径（可选，默认为项目根目录）"
    )

    # 调试模式
    debug: bool = Field(default=False, description="调试模式（0=关闭，1=开启）")

    # 速率限制配置
    rate_limit_enabled: bool = Field(default=True, description="是否启用速率限制")
    rate_limit_window: int = Field(default=60, description="速率限制时间窗口（秒）")
    rate_limit_max_requests: int = Field(default=30, description="速率限制最大请求数")

    # Zeabur 环境标识
    zeabur: Optional[str] = Field(default=None, description="Zeabur 环境标识")

    @field_validator("admin_user_ids")
    @classmethod
    def parse_admin_ids(cls, v: str) -> str:  # noqa: ARG002
        """验证并解析管理员ID列表"""
        if not v:
            raise ValueError("ADMIN_USER_IDS 不能为空")
        # 验证格式（逗号分隔的数字）
        ids = [id_str.strip() for id_str in v.split(",") if id_str.strip()]
        for id_str in ids:
            try:
                int(id_str)
            except ValueError:
                raise ValueError(f"无效的管理员ID: {id_str}")
        return v

    @property
    def admin_ids(self) -> List[int]:
        """获取管理员ID列表（整数列表）"""
        if not self.admin_user_ids:
            return []
        return [int(id_str.strip()) for id_str in self.admin_user_ids.split(",") if id_str.strip()]

    @property
    def is_production(self) -> bool:
        """判断是否为生产环境"""
        return bool(self.data_dir) or bool(self.zeabur)


# 全局配置实例
_settings: Optional[BotSettings] = None


def get_settings() -> BotSettings:
    """获取配置实例（单例模式）

    Returns:
        BotSettings: 配置实例

    Raises:
        ValueError: 如果配置验证失败
    """
    global _settings

    if _settings is None:
        try:
            if PYDANTIC_AVAILABLE:
                _settings = BotSettings()
            else:
                raise ImportError("Pydantic not available, use fallback")
        except (Exception, ImportError) as e:
            # 如果 Pydantic Settings 加载失败，尝试从环境变量或 user_config.py 加载
            # 保持向后兼容
            token = os.getenv("BOT_TOKEN")
            admin_ids_str = os.getenv("ADMIN_USER_IDS", "")

            # 检查是否为生产环境
            is_production = bool(os.getenv("DATA_DIR")) or bool(os.getenv("ZEABUR"))

            # 如果不是生产环境，且环境变量没有，尝试从user_config.py读取
            if not is_production and (not token or not admin_ids_str):
                import importlib.util
                from pathlib import Path

                user_config_path = Path(__file__).parent.parent / "user_config.py"
                if user_config_path.exists():
                    try:
                        spec = importlib.util.spec_from_file_location(
                            "user_config", user_config_path
                        )
                        if spec is None or spec.loader is None:
                            raise ValueError("无法加载user_config模块")
                        user_config = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(user_config)

                        token = token or getattr(user_config, "BOT_TOKEN", None)
                        admin_ids_str = admin_ids_str or getattr(user_config, "ADMIN_USER_IDS", "")
                    except Exception as config_error:
                        import logging

                        logger = logging.getLogger(__name__)
                        logger.debug(f"加载user_config.py失败: {config_error}")

            if not token:
                raise ValueError("BOT_TOKEN 未设置！请设置环境变量 BOT_TOKEN 或创建 user_config.py")

            if not admin_ids_str:
                raise ValueError(
                    "ADMIN_USER_IDS 未设置！请设置环境变量 ADMIN_USER_IDS 或创建 user_config.py"
                )

            # 创建配置实例（使用环境变量）
            _settings = BotSettings(
                bot_token=token,
                admin_user_ids=admin_ids_str,
                data_dir=os.getenv("DATA_DIR"),
                debug=os.getenv("DEBUG", "0") == "1",
                rate_limit_enabled=os.getenv("RATE_LIMIT_ENABLED", "1") == "1",
                rate_limit_window=int(os.getenv("RATE_LIMIT_WINDOW", "60")),
                rate_limit_max_requests=int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "30")),
                zeabur=os.getenv("ZEABUR"),
            )

    return _settings


def reload_settings() -> BotSettings:
    """重新加载配置

    Returns:
        BotSettings: 新的配置实例
    """
    global _settings
    _settings = None
    return get_settings()


def validate_settings() -> bool:
    """验证配置是否有效

    Returns:
        bool: 如果配置有效返回 True，否则返回 False
    """
    try:
        settings = get_settings()
        # 基本验证
        if not settings.bot_token:
            return False
        if not settings.admin_ids:
            return False
        return True
    except Exception:
        return False
