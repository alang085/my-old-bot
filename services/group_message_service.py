"""群组消息服务 - 封装群组消息相关的业务逻辑"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import db_operations
from constants import DEFAULT_ANNOUNCEMENT_INTERVAL
from db.repositories import MessageRepository

logger = logging.getLogger(__name__)

# 创建 Repository 实例
_message_repository = MessageRepository()


class GroupMessageService:
    """群组消息业务服务"""

    @staticmethod
    async def get_all_configs() -> List[Dict[str, Any]]:
        """获取所有群组消息配置

        Returns:
            配置列表
        """
        return await db_operations.get_group_message_configs()

    @staticmethod
    async def get_config_by_chat_id(chat_id: int) -> Optional[Dict[str, Any]]:
        """根据 chat_id 获取配置

        Args:
            chat_id: 群组/频道ID

        Returns:
            配置字典，如果不存在则返回 None
        """
        return await db_operations.get_group_message_config_by_chat_id(chat_id)

    @staticmethod
    async def save_config(
        chat_id: int,
        chat_title: Optional[str] = None,
        is_active: Optional[int] = None,
        bot_links: Optional[str] = None,
        worker_links: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """保存群组消息配置

        Args:
            chat_id: 群组/频道ID
            chat_title: 群组/频道标题（可选）
            is_active: 是否启用（可选）
            bot_links: 机器人链接（可选）
            worker_links: 人工链接（可选）

        Returns:
            Tuple[success, error_message]:
                - success: 是否成功
                - error_message: 错误消息（如果失败）
        """
        try:
            success = await db_operations.save_group_message_config(
                chat_id=chat_id,
                chat_title=chat_title,
                is_active=is_active,
                bot_links=bot_links,
                worker_links=worker_links,
            )
            if success:
                return True, None
            else:
                return False, "❌ 保存失败"
        except Exception as e:
            logger.error(f"保存群组消息配置失败: {e}", exc_info=True)
            return False, f"❌ 保存失败: {str(e)}"

    @staticmethod
    async def setup_group_auto(chat_id: int, chat_title: str) -> Tuple[bool, Optional[str]]:
        """一键设置群组自动消息功能
        自动从数据库读取语录并配置到群组

        Args:
            chat_id: 群组/频道ID
            chat_title: 群组/频道标题

        Returns:
            Tuple[success, error_message]:
                - success: 是否成功
                - error_message: 错误消息（如果失败）
        """
        try:
            import random

            # 从数据库读取激活的语录
            start_work_messages = await db_operations.get_active_start_work_messages()
            end_work_messages = await db_operations.get_active_end_work_messages()

            # 随机选择一条开工消息和收工消息
            start_work_message = random.choice(start_work_messages) if start_work_messages else ""
            end_work_message = random.choice(end_work_messages) if end_work_messages else ""

            # 检查是否已存在配置
            existing_config = await db_operations.get_group_message_config_by_chat_id(chat_id)

            if existing_config:
                # 更新现有配置，自动配置文案
                success = await db_operations.save_group_message_config(
                    chat_id=chat_id,
                    chat_title=chat_title,
                    start_work_message=start_work_message,
                    end_work_message=end_work_message,
                    is_active=1,
                )
                if not success:
                    return False, "❌ 更新配置失败"
            else:
                # 创建新配置，自动配置文案
                success = await db_operations.save_group_message_config(
                    chat_id=chat_id,
                    chat_title=chat_title,
                    start_work_message=start_work_message,
                    end_work_message=end_work_message,
                    is_active=1,
                )
                if not success:
                    return False, "❌ 创建配置失败"

                # 设置公告定时任务（如果不存在）
                await db_operations.save_announcement_schedule(
                    interval_hours=DEFAULT_ANNOUNCEMENT_INTERVAL, is_active=1
                )

            return True, None
        except Exception as e:
            logger.error(f"设置群组自动消息失败: {e}", exc_info=True)
            return False, f"❌ 设置失败: {str(e)}"

    @staticmethod
    async def get_all_announcements() -> List[Dict[str, Any]]:
        """获取所有公司公告

        Returns:
            公告列表
        """
        return await db_operations.get_all_company_announcements()

    @staticmethod
    async def get_announcement_schedule() -> Optional[Dict[str, Any]]:
        """获取公告定时任务配置

        Returns:
            定时任务配置字典
        """
        return await db_operations.get_announcement_schedule()

    @staticmethod
    async def save_announcement_schedule(interval_hours: int, is_active: int) -> bool:
        """保存公告定时任务配置

        Args:
            interval_hours: 间隔小时数
            is_active: 是否启用

        Returns:
            是否成功
        """
        return await db_operations.save_announcement_schedule(interval_hours, is_active)

    @staticmethod
    async def get_all_anti_fraud_messages() -> List[Dict[str, Any]]:
        """获取所有防诈骗消息

        Returns:
            防诈骗消息列表
        """
        return await db_operations.get_all_anti_fraud_messages()

    @staticmethod
    async def get_all_promotion_messages() -> List[Dict[str, Any]]:
        """获取所有宣传消息

        Returns:
            宣传消息列表
        """
        return await db_operations.get_all_promotion_messages()
