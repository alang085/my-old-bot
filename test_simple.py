"""简单功能测试"""
import sys
import sqlite3
from pathlib import Path

print("=" * 60)
print("🧪 简单功能测试")
print("=" * 60)

# 1. 检查数据库文件
print("\n🔍 检查数据库文件...")
db_path = Path("loan_bot.db")
if db_path.exists():
    print(f"   ✅ 数据库文件存在: {db_path}")
    size = db_path.stat().st_size
    print(f"   📊 数据库大小: {size / 1024:.2f} KB")
else:
    print(f"   ❌ 数据库文件不存在")
    sys.exit(1)

# 2. 检查收入明细表
print("\n🔍 检查收入明细表...")
try:
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # 检查表是否存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='income_records'")
    table_exists = cursor.fetchone()
    
    if table_exists:
        print("   ✅ 收入明细表存在")
        
        # 检查表结构
        cursor.execute("PRAGMA table_info(income_records)")
        columns = cursor.fetchall()
        print(f"   📊 表字段数: {len(columns)}")
        
        # 检查记录数
        cursor.execute("SELECT COUNT(*) FROM income_records")
        count = cursor.fetchone()[0]
        print(f"   📊 收入记录数: {count}")
        
        if count > 0:
            # 显示最新5条记录
            cursor.execute("SELECT * FROM income_records ORDER BY created_at DESC LIMIT 5")
            records = cursor.fetchall()
            print(f"\n   📋 最新5条收入记录:")
            for i, record in enumerate(records, 1):
                print(f"      {i}. 类型: {record[2]}, 金额: {record[3]:.2f}, 日期: {record[1]}")
    else:
        print("   ❌ 收入明细表不存在")
    
    conn.close()
except Exception as e:
    print(f"   ❌ 检查失败: {e}")
    import traceback
    traceback.print_exc()

# 3. 检查常量定义
print("\n🔍 检查常量定义...")
try:
    from constants import INCOME_TYPES, CUSTOMER_TYPES
    print(f"   ✅ 收入类型: {len(INCOME_TYPES)} 种")
    for key, value in INCOME_TYPES.items():
        print(f"      - {key}: {value}")
    print(f"   ✅ 客户类型: {len(CUSTOMER_TYPES)} 种")
    for key, value in CUSTOMER_TYPES.items():
        print(f"      - {key}: {value}")
except Exception as e:
    print(f"   ❌ 常量定义检查失败: {e}")

# 4. 检查模块导入
print("\n🔍 检查模块导入...")
modules_to_check = [
    'db_operations',
    'handlers.income_handlers',
    'utils.date_helpers',
    'constants'
]

for module_name in modules_to_check:
    try:
        __import__(module_name)
        print(f"   ✅ {module_name}")
    except Exception as e:
        print(f"   ❌ {module_name}: {e}")

# 5. 检查配置文件
print("\n🔍 检查配置文件...")
try:
    from config import BOT_TOKEN, ADMIN_IDS
    if BOT_TOKEN and len(BOT_TOKEN) > 10:
        print(f"   ✅ BOT_TOKEN 已配置: {BOT_TOKEN[:10]}...")
    else:
        print(f"   ❌ BOT_TOKEN 未配置")
    
    if ADMIN_IDS and len(ADMIN_IDS) > 0:
        print(f"   ✅ ADMIN_IDS 已配置: {len(ADMIN_IDS)} 个管理员")
    else:
        print(f"   ❌ ADMIN_IDS 未配置")
except Exception as e:
    print(f"   ❌ 配置检查失败: {e}")

print("\n" + "=" * 60)
print("✅ 基础测试完成")
print("=" * 60)

