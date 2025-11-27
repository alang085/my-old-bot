# 配置说明

## 配置方式

机器人支持两种配置方式，按优先级排序：

### 方式1：环境变量（推荐用于生产环境）

#### Windows PowerShell
```powershell
$env:BOT_TOKEN="你的机器人Token"
$env:ADMIN_USER_IDS="用户ID1,用户ID2"
```

#### Windows CMD
```cmd
set BOT_TOKEN=你的机器人Token
set ADMIN_USER_IDS=用户ID1,用户ID2
```

#### Linux/Mac
```bash
export BOT_TOKEN="你的机器人Token"
export ADMIN_USER_IDS="用户ID1,用户ID2"
```

### 方式2：配置文件（推荐用于开发环境）

1. 复制 `user_config.example.py` 为 `user_config.py`
2. 编辑 `user_config.py`，填入你的配置：

```python
BOT_TOKEN = '你的机器人Token'
ADMIN_USER_IDS = '你的用户ID1,你的用户ID2'
```

## 获取配置信息

### 获取 Bot Token

1. 在 Telegram 中搜索 `@BotFather`
2. 发送 `/newbot` 创建新机器人，或发送 `/mybots` 查看已有机器人
3. 选择你的机器人，点击 `API Token`
4. 复制 Token

### 获取用户ID

1. 在 Telegram 中搜索 `@userinfobot`
2. 发送任意消息
3. 机器人会回复你的用户ID（数字）

## 注意事项

1. **不要将 `user_config.py` 提交到版本控制系统**
   - 该文件包含敏感信息
   - 已在 `.gitignore` 中排除（如果使用git）

2. **环境变量优先级高于配置文件**
   - 如果同时设置了环境变量和配置文件，优先使用环境变量

3. **管理员ID格式**
   - 多个ID用逗号分隔，不要有空格
   - 示例：`'123456789,987654321'`

4. **Token格式**
   - 通常格式为：`数字:字母数字组合`
   - 示例：`1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`

## 故障排除

### 错误：BOT_TOKEN 未设置

**解决方案：**
1. 检查环境变量是否设置正确
2. 检查 `user_config.py` 文件是否存在且格式正确
3. 确保 Token 值在引号内（字符串格式）

### 错误：ADMIN_USER_IDS 未设置

**解决方案：**
1. 检查环境变量或配置文件中的 `ADMIN_USER_IDS`
2. 确保ID格式正确（数字，逗号分隔）
3. 确保至少有一个管理员ID

### 错误：Token 无效

**解决方案：**
1. 检查 Token 是否完整（没有遗漏字符）
2. 在 @BotFather 中重新获取 Token
3. 确保 Token 没有被撤销
