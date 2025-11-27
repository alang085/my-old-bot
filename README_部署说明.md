# Zeabur 部署说明

## 部署步骤

### 1. 准备代码
确保所有文件都已提交到 Git 仓库（GitHub/GitLab）

### 2. 在 Zeabur 创建项目
1. 登录 [Zeabur](https://zeabur.com)
2. 点击 "New Project"
3. 选择 "Import from Git"
4. 连接你的 Git 仓库
5. 选择 `loan005.bot` 目录

### 3. 配置环境变量
在 Zeabur 项目设置中添加以下环境变量：

#### 必需的环境变量：
```
BOT_TOKEN=你的机器人Token
ADMIN_USER_IDS=你的用户ID（多个用逗号分隔）
```

#### 如何获取：
- **BOT_TOKEN**: 在 Telegram 中搜索 @BotFather，发送 `/token` 获取
- **ADMIN_USER_IDS**: 在 Telegram 中搜索 @userinfobot，发送消息获取你的用户ID

### 4. 配置启动命令
Zeabur 会自动识别 `Procfile` 或使用 `zeabur.json` 中的配置

### 5. 部署
点击 "Deploy" 按钮开始部署

## 注意事项

1. **数据库文件**: 数据库文件 `loan_bot.db` 会存储在容器中，重启可能会丢失数据
   - 建议：使用 Zeabur 的持久化存储或外部数据库（如 PostgreSQL）

2. **配置文件**: 不要将 `config.py` 提交到 Git（已在 .gitignore 中）
   - 使用环境变量代替

3. **日志**: 可以在 Zeabur 的日志面板查看运行日志

4. **重启策略**: 已配置自动重启，如果机器人崩溃会自动重启

## 环境变量说明

| 变量名 | 说明 | 示例 |
|--------|------|------|
| BOT_TOKEN | Telegram Bot Token | 123456789:ABCdef... |
| ADMIN_USER_IDS | 管理员用户ID列表 | 123456789,987654321 |

## 故障排查

1. **机器人无法启动**
   - 检查环境变量是否正确设置
   - 查看日志中的错误信息

2. **Token 无效**
   - 确认 BOT_TOKEN 格式正确
   - 确认 Token 未被撤销

3. **权限错误**
   - 确认 ADMIN_USER_IDS 包含你的用户ID
   - 用户ID必须是数字，多个用逗号分隔

## 更新部署

每次推送代码到 Git 仓库，Zeabur 会自动重新部署

