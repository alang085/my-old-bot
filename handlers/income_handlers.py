"""收入明细查询处理器（仅管理员权限）"""
import logging
from datetime import datetime
from typing import Optional
import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.date_helpers import get_daily_period_date
from decorators import error_handler, private_chat_only
from config import ADMIN_IDS
from constants import INCOME_TYPES, CUSTOMER_TYPES

logger = logging.getLogger(__name__)


def _is_admin(user_id: Optional[int]) -> bool:
    """检查用户是否为管理员"""
    return user_id is not None and user_id in ADMIN_IDS


async def format_income_detail(record: dict) -> str:
    """格式化单条收入明细"""
    type_name = INCOME_TYPES.get(record['type'], record['type'])
    customer_name = CUSTOMER_TYPES.get(record['customer'], record['customer'] or '无关联')
    
    time_str = ""
    if record.get('created_at'):
        try:
            dt = datetime.fromisoformat(record['created_at'].replace('Z', '+00:00'))
            time_str = dt.strftime("%H:%M:%S")
        except:
            pass
    
    detail = f"💰 {record['amount']:,.2f}"
    if record.get('order_id'):
        detail += f" - 订单: {record['order_id']}"
    if record.get('group_id'):
        detail += f" - 归属: {record['group_id']}"
    if record.get('customer'):
        detail += f" - {customer_name}"
    if time_str:
        detail += f" - [{time_str}]"
    if record.get('note'):
        detail += f"\n  📝 {record['note']}"
    
    return detail


async def generate_income_report(records: list, start_date: str, end_date: str,
                                  title: str = "收入明细") -> str:
    """生成收入明细报表"""
    if not records:
        return f"💰 {title}\n\n{start_date} 至 {end_date}\n\n❌ 无记录"
    
    # 按类型和客户类型分组
    by_type = {}
    for record in records:
        type_name = record['type']
        customer = record['customer'] or 'None'
        
        if type_name not in by_type:
            by_type[type_name] = {}
        if customer not in by_type[type_name]:
            by_type[type_name][customer] = []
        by_type[type_name][customer].append(record)
    
    # 计算总计
    total_amount = sum(r['amount'] for r in records)
    
    # 生成报表文本
    report = f"💰 {title}\n"
    report += f"{'═' * 30}\n"
    report += f"📅 {start_date} 至 {end_date}\n"
    report += f"{'═' * 30}\n\n"
    
    # 按类型显示
    type_order = ['completed', 'breach_end', 'interest', 'principal_reduction', 'adjustment']
    
    for type_key in type_order:
        if type_key not in by_type:
            continue
        
        type_name = INCOME_TYPES.get(type_key, type_key)
        type_records = []
        for customer_list in by_type[type_key].values():
            type_records.extend(customer_list)
        
        type_total = sum(r['amount'] for r in type_records)
        type_count = len(type_records)
        
        report += f"【{type_name}】总计: {type_total:,.2f} ({type_count}笔)\n"
        report += f"{'─' * 30}\n"
        
        # 按客户类型分组显示
        for customer_key, customer_records in sorted(by_type[type_key].items()):
            customer_name = CUSTOMER_TYPES.get(customer_key, customer_key) if customer_key != 'None' else '无关联'
            customer_total = sum(r['amount'] for r in customer_records)
            customer_count = len(customer_records)
            
            report += f"  {customer_name} - {customer_total:,.2f} ({customer_count}笔)\n"
            
            # 显示明细（最多显示前10条）
            display_records = customer_records[:10]
            for i, record in enumerate(display_records, 1):
                detail = await format_income_detail(record)
                report += f"    {i}. {detail}\n"
            
            if len(customer_records) > 10:
                report += f"    ... (还有 {len(customer_records) - 10} 条记录)\n"
            
            report += "\n"
        
        report += "\n"
    
    report += f"{'═' * 30}\n"
    report += f"💰 总收入: {total_amount:,.2f}\n"
    
    # 按归属ID汇总
    by_group = {}
    for record in records:
        group_id = record.get('group_id') or '全局'
        if group_id not in by_group:
            by_group[group_id] = 0
        by_group[group_id] += record['amount']
    
    if by_group:
        report += f"\n【按归属ID汇总】\n"
        for group_id, amount in sorted(by_group.items(), key=lambda x: x[1], reverse=True):
            report += f"  • {group_id}: {amount:,.2f}\n"
    
    return report


@error_handler
@private_chat_only
async def show_income_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示今日收入明细（仅管理员）"""
    user_id = update.effective_user.id if update.effective_user else None
    
    if not _is_admin(user_id):
        await update.message.reply_text("❌ 此功能仅限管理员使用")
        return
    
    date = get_daily_period_date()
    records = await db_operations.get_income_records(date, date)
    
    report = await generate_income_report(records, date, date, f"今日收入明细 ({date})")
    
    keyboard = [
        [
            InlineKeyboardButton("📅 本月收入", callback_data="income_view_month"),
            InlineKeyboardButton("📆 日期查询", callback_data="income_view_query")
        ],
        [
            InlineKeyboardButton("🔍 分类查询", callback_data="income_view_by_type")
        ],
        [
            InlineKeyboardButton("🔙 返回报表", callback_data="report_view_today_ALL")
        ]
    ]
    
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(report, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text(report, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"显示收入明细失败: {e}", exc_info=True)
        if update.callback_query:
            await update.callback_query.message.reply_text(report, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text(report, reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_income_query_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理收入明细查询输入"""
    user_id = update.effective_user.id if update.effective_user else None
    
    if not _is_admin(user_id):
        await update.message.reply_text("❌ 此功能仅限管理员使用")
        context.user_data['state'] = None
        return
    
    try:
        dates = text.split()
        if len(dates) == 1:
            start_date = end_date = dates[0]
        elif len(dates) == 2:
            start_date = dates[0]
            end_date = dates[1]
        else:
            await update.message.reply_text("❌ 格式错误。请使用：\n格式1 (单日): 2024-01-01\n格式2 (范围): 2024-01-01 2024-01-31")
            return
        
        # 验证日期格式
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
        
        records = await db_operations.get_income_records(start_date, end_date)
        
        report = await generate_income_report(records, start_date, end_date, 
                                               f"收入明细 ({start_date} 至 {end_date})")
        
        keyboard = [[InlineKeyboardButton("🔙 返回", callback_data="income_view_today")]]
        await update.message.reply_text(report, reply_markup=InlineKeyboardMarkup(keyboard))
        context.user_data['state'] = None
        
    except ValueError:
        await update.message.reply_text("❌ 日期格式错误。请使用 YYYY-MM-DD 格式")
    except Exception as e:
        logger.error(f"查询收入明细出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ 错误: {e}")
        context.user_data['state'] = None

