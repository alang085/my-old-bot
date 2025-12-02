# 订单管理机器人

这是一个基于 Telegram Bot 的订单管理系统，使用 SQLite 数据库存储数据。

## 功能特性

- ✅ 订单创建和管理
- ✅ 财务数据统计
- ✅ 按归属ID分组统计
- ✅ 订单状态管理（正常、逾期、违约、完成等）
- ✅ 本金减少、利息收入、违约协商还款
- ✅ 完善的数据库存储
- ✅ 强大的订单查找功能
- ✅ **收入明细系统**（仅管理员）- 自动记录每笔收入，支持多维度查询
- ✅ **盈余计算**（归属报表）- 自动计算归属ID的盈利情况

## 安装和配置

### 1. 安装依赖

```bash
pip install python-telegram-bot
```

### 2. 初始化数据库

运行以下命令初始化数据库：

```bash
python init_db.py
```

这将创建 `loan_bot.db` 数据库文件，包含以下表：
- `orders` - 订单表
- `financial_data` - 全局财务数据表
- `grouped_data` - 按归属ID分组的财务数据表
- `daily_data` - 日结数据表
- `income_records` - 收入明细表（新增）
- `expense_records` - 支出明细表
- `order_counter` - 订单ID计数器表

### 3. 配置环境变量

设置以下环境变量：

```bash
# Windows PowerShell
$env:BOT_TOKEN="你的机器人Token"
$env:ADMIN_USER_IDS="管理员用户ID1,管理员用户ID2"

# Linux/Mac
export BOT_TOKEN="你的机器人Token"
export ADMIN_USER_IDS="管理员用户ID1,管理员用户ID2"
```

## 使用方法

### 基本命令

- `/start` - 显示帮助信息（仅私聊）
- `/create <归属ID> <客户A/B> <金额>` - 创建新订单
  - 示例: `/create S01 A 10000`
- `/order` - 查看当前群组的订单状态

### 金额操作

- `+<金额>` - 记录利息收入
  - 示例: `+500`
- `+<金额>b` - 本金减少
  - 示例: `+1000b`
- `+<金额>c` - 违约协商还款
  - 示例: `+2000c`

### 状态变更

- `/normal` - 转为正常状态
- `/overdue` - 转为逾期状态
- `/end` - 标记订单为完成
- `/breach` - 标记为违约
- `/breach_end` - 违约订单完成

### 查询功能

- `/report` - 查看全局报表（仅私聊）
- `/report <归属ID>` - 查看指定归属ID的报表（显示盈余）
- `/search <类型> <值> [值2]` - 查找订单（仅私聊）

#### 报表功能

**全局报表** (`/report`)：
- 显示全局财务数据统计
- 支持当日、当月、按日期查询
- 包含收入明细查询入口（仅管理员）

**归属报表** (`/report S01`)：
- 显示指定归属ID的财务数据
- **新增：盈余计算**
  - 计算公式：盈余 = 利息收入 + 违约完成金额 - 违约金额
  - 支持当日、当月、按日期查询
  - 帮助分析归属ID的盈利情况

**收入明细查询**（仅管理员）：
- 从报表界面点击"💰 收入明细"按钮
- 支持今日、本月、日期范围查询
- 支持按类型分类查询
- 显示详细的收入记录和汇总

#### 查找类型

1. **按订单ID查找**
   ```
   /search order_id 0001
   ```

2. **按归属ID查找**
   ```
   /search group_id S01
   ```

3. **按客户类型查找**
   ```
   /search customer A
   /search customer B
   ```

4. **按状态查找**
   ```
   /search state normal
   /search state overdue
   /search state breach
   /search state end
   /search state breach_end
   ```

5. **按日期范围查找**
   ```
   /search date 2024-01-01 2024-01-31
   ```

## 数据库结构

### orders 表
- `id` - 主键
- `order_id` - 订单ID（唯一）
- `group_id` - 归属ID
- `chat_id` - 聊天ID
- `date` - 创建日期
- `weekday_group` - 星期分组（一、二、三...）
- `customer` - 客户类型（A/B）
- `amount` - 金额
- `state` - 状态（normal/overdue/breach/end/breach_end）
- `created_at` - 创建时间戳
- `updated_at` - 更新时间戳

### financial_data 表
存储全局财务统计数据，包括：
- 有效订单数和金额
- 活动资金
- 新/老客户统计
- 利息收入
- 完成订单统计
- 违约订单统计

### grouped_data 表
按归属ID分组的财务数据，结构与 financial_data 相同。

### daily_data 表
按日期和归属ID存储的日结流量数据，用于生成周期报表。

### income_records 表（新增）
收入明细记录表，自动记录每笔收入的详细信息：
- 收入类型（订单完成、违约完成、利息、本金减少、调整）
- 关联订单信息
- 客户类型（新客户/老客户）
- 归属ID
- 支持多维度查询和汇总

### expense_records 表
支出明细记录表，记录每笔支出的详细信息。

## 运行

```bash
python main.py
```

## 注意事项

1. 大部分命令需要授权用户或管理员权限
2. `/start`、`/report` 和 `/search` 命令只能在私聊中使用
3. 每个群组只能有一个活跃订单（状态不是 end 或 breach_end）
4. 订单完成后可以从该群组创建新订单
5. **收入明细查询仅限管理员使用**
6. **盈余计算仅在归属报表中显示**，全局报表不显示

## 文件说明

- `main.py` - 主程序文件
- `db_operations.py` - 数据库操作模块
- `init_db.py` - 数据库初始化脚本
- `loan_bot.db` - SQLite 数据库文件（运行后自动创建）

