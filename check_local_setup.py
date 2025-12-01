"""本地环境检查脚本"""
import sys
import os
from pathlib import Path

def check_python_version():
    """检查 Python 版本"""
    print("🔍 检查 Python 版本...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"   ✅ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"   ❌ Python 版本过低: {version.major}.{version.minor}.{version.micro}")
        print("   ⚠️  需要 Python 3.8 或更高版本")
        return False

def check_dependencies():
    """检查依赖包"""
    print("\n🔍 检查依赖包...")
    required_packages = {
        'telegram': 'python-telegram-bot',
        'pytz': 'pytz',
        'scheduler': 'APScheduler'
    }
    
    all_ok = True
    for module, package in required_packages.items():
        try:
            if module == 'telegram':
                import telegram
            elif module == 'pytz':
                import pytz
            elif module == 'scheduler':
                from apscheduler.schedulers.asyncio import AsyncIOScheduler
            print(f"   ✅ {package}")
        except ImportError:
            print(f"   ❌ {package} 未安装")
            print(f"   💡 运行: pip install {package}")
            all_ok = False
    
    return all_ok

def check_config():
    """检查配置文件"""
    print("\n🔍 检查配置文件...")
    config_path = Path('user_config.py')
    
    if config_path.exists():
        print("   ✅ user_config.py 文件存在")
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("user_config", config_path)
            user_config = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(user_config)
            
            # 检查 Token
            token = getattr(user_config, 'BOT_TOKEN', None)
            if token and token != '你的机器人Token':
                print(f"   ✅ BOT_TOKEN 已配置: {token[:10]}...")
            else:
                print("   ❌ BOT_TOKEN 未配置或使用默认值")
                return False
            
            # 检查管理员 ID
            admin_ids = getattr(user_config, 'ADMIN_USER_IDS', None)
            if admin_ids and admin_ids != '你的用户ID1,你的用户ID2':
                print(f"   ✅ ADMIN_USER_IDS 已配置: {admin_ids}")
            else:
                print("   ❌ ADMIN_USER_IDS 未配置或使用默认值")
                return False
            
            return True
        except Exception as e:
            print(f"   ❌ 加载配置文件失败: {e}")
            return False
    else:
        # 检查环境变量
        print("   ⚠️  user_config.py 不存在，检查环境变量...")
        token = os.getenv('BOT_TOKEN')
        admin_ids = os.getenv('ADMIN_USER_IDS')
        
        if token:
            print(f"   ✅ BOT_TOKEN 环境变量已设置: {token[:10]}...")
        else:
            print("   ❌ BOT_TOKEN 未设置（环境变量或配置文件）")
            return False
        
        if admin_ids:
            print(f"   ✅ ADMIN_USER_IDS 环境变量已设置: {admin_ids}")
        else:
            print("   ❌ ADMIN_USER_IDS 未设置（环境变量或配置文件）")
            return False
        
        return True

def check_database():
    """检查数据库"""
    print("\n🔍 检查数据库...")
    db_path = Path('loan_bot.db')
    
    if db_path.exists():
        print(f"   ✅ 数据库文件存在: {db_path}")
        size = db_path.stat().st_size
        print(f"   📊 数据库大小: {size / 1024:.2f} KB")
        return True
    else:
        print("   ⚠️  数据库文件不存在（首次运行时会自动创建）")
        return True  # 首次运行是可以的

def check_project_structure():
    """检查项目结构"""
    print("\n🔍 检查项目结构...")
    required_dirs = ['handlers', 'callbacks', 'utils']
    required_files = ['main.py', 'config.py', 'db_operations.py', 'init_db.py']
    
    all_ok = True
    for dir_name in required_dirs:
        if Path(dir_name).exists():
            print(f"   ✅ {dir_name}/ 目录存在")
        else:
            print(f"   ❌ {dir_name}/ 目录不存在")
            all_ok = False
    
    for file_name in required_files:
        if Path(file_name).exists():
            print(f"   ✅ {file_name} 文件存在")
        else:
            print(f"   ❌ {file_name} 文件不存在")
            all_ok = False
    
    return all_ok

def main():
    """主检查流程"""
    print("=" * 60)
    print("📋 本地环境检查")
    print("=" * 60)
    
    checks = [
        ("Python 版本", check_python_version),
        ("依赖包", check_dependencies),
        ("配置文件", check_config),
        ("数据库", check_database),
        ("项目结构", check_project_structure)
    ]
    
    results = {}
    for name, check_func in checks:
        try:
            results[name] = check_func()
        except Exception as e:
            print(f"   ❌ 检查失败: {e}")
            results[name] = False
    
    print("\n" + "=" * 60)
    print("📊 检查结果总结")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results.items():
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"   {status} - {name}")
        if not passed:
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("\n🎉 所有检查通过！可以运行项目了")
        print("\n💡 运行命令:")
        print("   python main.py")
    else:
        print("\n⚠️  部分检查未通过，请修复后重试")
        print("\n💡 常见问题解决:")
        print("   1. 安装依赖: pip install -r requirements.txt")
        print("   2. 配置 user_config.py 文件")
        print("   3. 初始化数据库: python init_db.py")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()

