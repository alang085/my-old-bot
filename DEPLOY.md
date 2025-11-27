# Zeabur 部署指南

## 📋 部署前检查清单

- [x] `requirements.txt` - Python 依赖已配置
- [x] `Procfile` - 启动命令已配置
- [x] `.gitignore` - 敏感文件已排除
- [x] `zeabur.json` - Zeabur 配置已创建
- [ ] 环境变量已准备（BOT_TOKEN, ADMIN_USER_IDS）
- [ ] Git 仓库已创建并推送代码

## 🚀 快速部署步骤

### 1. 准备 Git 仓库

```bash
# 初始化 Git（如果还没有）
git init
git add .
git commit -m "Initial commit for Zeabur deployment"

# 推送到 GitHub/GitLab
git remote add origin <你的仓库地址>
git push -u origin main
```

### 2. 在 Zeabur 部署

1. 访问 [Zeabur Dashboard](https://dash.zeabur.com)
2. 点击 **"New Project"**
3. 选择 **"Import from Git"**
4. 连接你的 Git 仓库（GitHub/GitLab）
5. 选择仓库和分支
6. 选择根目录为项目根目录

### 3. 配置持久化存储（重要！）

**添加 Volume：**
1. 在 Zeabur 项目设置中找到 "Volumes" 或 "Storage"
2. 添加 Volume：
   - **Mount Path**: `/data`
   - **Size**: 1GB（可根据需要调整）

### 4. 配置环境变量

在 Zeabur 项目设置中添加：

```
BOT_TOKEN=你的机器人Token
ADMIN_USER_IDS=你的用户ID
DATA_DIR=/data
```

**说明：**
- `DATA_DIR=/data` 将数据库存储在持久化 Volume 中
- 容器重启不会丢失数据

**获取方式：**
- BOT_TOKEN: 在 Telegram 搜索 @BotFather → `/token`
- ADMIN_USER_IDS: 在 Telegram 搜索 @userinfobot → 发送消息获取ID

### 4. 部署

点击 **"Deploy"** 按钮，等待部署完成

## 📝 文件说明

| 文件 | 说明 |
|------|------|
| `requirements.txt` | Python 依赖包列表 |
| `Procfile` | 启动命令配置 |
| `zeabur.json` | Zeabur 平台配置 |
| `.gitignore` | Git 忽略文件列表 |
| `runtime.txt` | Python 版本指定 |

## ⚠️ 重要提示

1. **不要提交敏感文件**
   - `config.py` 已在 `.gitignore` 中
   - 使用环境变量代替配置文件

2. **数据库持久化** ✅ 已解决
   - 代码已配置支持持久化存储
   - 设置 `DATA_DIR=/data` 环境变量
   - 在 Zeabur 添加 Volume（Mount Path: `/data`）
   - 数据库文件将存储在 Volume 中，容器重启不会丢失数据

3. **日志查看**
   - 在 Zeabur Dashboard 的 "Logs" 标签页查看运行日志

4. **自动重启**
   - 已配置自动重启策略
   - 机器人崩溃会自动重启（最多10次）

## 🔧 故障排查

### 问题：部署失败
- 检查 `requirements.txt` 是否正确
- 查看构建日志中的错误信息

### 问题：机器人无法启动
- 检查环境变量是否正确设置
- 查看运行日志中的错误信息
- 确认 BOT_TOKEN 格式正确

### 问题：权限错误
- 确认 ADMIN_USER_IDS 包含你的用户ID
- 用户ID 必须是数字，多个用逗号分隔（无空格）

## 📞 测试部署

部署成功后，在 Telegram 中：
1. 向机器人发送 `/start`（私聊）
2. 检查是否收到欢迎消息
3. 测试创建订单功能

## 🔄 更新部署

每次推送代码到 Git 仓库，Zeabur 会自动检测并重新部署。

```bash
git add .
git commit -m "Update code"
git push
```

