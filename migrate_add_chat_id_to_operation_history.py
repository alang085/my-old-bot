"""为 operation_history 表添加 chat_id 字段的迁移脚本"""
import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

# 数据库文件路径 - 支持持久化存储
DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(DATA_DIR, 'loan_bot.db')

def migrate_add_chat_id():
    """执行迁移：为 operation_history 表添加 chat_id 字段"""
    if not os.path.exists(DB_PATH):
        logger.warning(f"数据库文件 {DB_PATH} 不存在，跳过迁移")
        return True
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='operation_history'")
        if not cursor.fetchone():
            logger.info("operation_history 表不存在，跳过迁移（将在初始化时创建）")
            conn.close()
            return True
        
        # 检查 chat_id 字段是否已存在
        cursor.execute("PRAGMA table_info(operation_history)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'chat_id' in columns:
            logger.debug("chat_id 字段已存在，跳过迁移")
            conn.close()
            return True
        
        logger.info("开始迁移：为 operation_history 表添加 chat_id 字段...")
        
        # 添加 chat_id 字段（使用默认值 0，表示历史数据）
        cursor.execute('''
        ALTER TABLE operation_history 
        ADD COLUMN chat_id INTEGER NOT NULL DEFAULT 0
        ''')
        
        # 创建新索引
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_operation_chat_user 
        ON operation_history(chat_id, user_id, created_at DESC)
        ''')
        
        conn.commit()
        logger.info("迁移完成：已添加 chat_id 字段和索引")
        conn.close()
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"迁移失败: {e}", exc_info=True)
        conn.close()
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    success = migrate_add_chat_id()
    if success:
        print("[PASS] 迁移成功")
    else:
        print("[FAIL] 迁移失败")
        exit(1)

