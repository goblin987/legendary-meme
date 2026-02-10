"""
Worker UI - Permission-based interfaces for workers
Provides simplified access to add products, check stock, and marketing tools.
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

# Import worker management
from worker_management import get_worker_by_user_id, is_worker, check_worker_permission

# Import utils
from utils import CITIES, DISTRICTS

logger = logging.getLogger(__name__)

# ============= WORKER MENU =============

async def handle_worker_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main menu for workers based on their permissions"""
    query = update.callback_query if hasattr(update, 'callback_query') and update.callback_query else None
    user_id = update.effective_user.id
    
    # Check if user is a worker
    worker = get_worker_by_user_id(user_id)
    if not worker:
        if query:
            await query.answer("You don't have worker permissions", show_alert=True)
        return
    
    if query:
        await query.answer()
    
    permissions = worker.get('permissions', [])
    username = worker.get('username', f"ID: {user_id}")
    
    msg = f"👷 **Worker Dashboard**\n\n"
    msg += f"Welcome @{username}!\n\n"
    msg += "**Your Permissions:**\n"
    
    keyboard = []
    
    # Show available actions based on permissions
    if "add_products" in permissions:
        msg += "• ➕ Add Products\n"
        keyboard.append([InlineKeyboardButton("➕ Add Single Product", callback_data="worker_add_single")])
        keyboard.append([InlineKeyboardButton("📦 Add Bulk Products", callback_data="worker_add_bulk")])
    
    if "check_stock" in permissions:
        msg += "• 📦 Check Stock\n"
        keyboard.append([InlineKeyboardButton("📦 Check Stock", callback_data="worker_check_stock")])
    
    if "marketing" in permissions:
        msg += "• 🎁 Marketing Tools\n"
        keyboard.append([InlineKeyboardButton("🎁 Marketing Tools", callback_data="worker_marketing")])
    
    keyboard.append([InlineKeyboardButton("🏠 Main Menu", callback_data="start")])
    
    if query:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

# ============= WORKER ADD PRODUCTS =============

async def handle_worker_add_single(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Redirect to admin add products flow with worker tracking"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permission
    if not check_worker_permission(user_id, "add_products"):
        await query.answer("You don't have permission to add products", show_alert=True)
        return
    
    await query.answer()
    
    # Set worker flag in context for tracking
    context.user_data['is_worker'] = True
    worker = get_worker_by_user_id(user_id)
    if worker:
        context.user_data['worker_id'] = worker['id']
    
    # Redirect to single product city selection
    from admin import handle_adm_city
    await handle_adm_city(update, context, params)

async def handle_worker_add_bulk(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Redirect to admin bulk add flow with worker tracking"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permission
    if not check_worker_permission(user_id, "add_products"):
        await query.answer("You don't have permission to add products", show_alert=True)
        return
    
    await query.answer()
    
    # Set worker flag in context for tracking
    context.user_data['is_worker'] = True
    worker = get_worker_by_user_id(user_id)
    if worker:
        context.user_data['worker_id'] = worker['id']
    
    # Redirect to bulk add city selection
    from admin import handle_adm_bulk_city
    await handle_adm_bulk_city(update, context, params)

# ============= WORKER CHECK STOCK =============

async def handle_worker_check_stock(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show stock information (read-only)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permission
    if not check_worker_permission(user_id, "check_stock"):
        await query.answer("You don't have permission to check stock", show_alert=True)
        return
    
    await query.answer()
    
    # Redirect to view stock handler
    from stock import handle_view_stock
    await handle_view_stock(update, context, params)

# ============= WORKER MARKETING =============

async def handle_worker_marketing(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show marketing tools menu"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permission
    if not check_worker_permission(user_id, "marketing"):
        await query.answer("You don't have permission to access marketing tools", show_alert=True)
        return
    
    await query.answer()
    
    msg = "🎁 **Marketing Tools**\n\n"
    msg += "Choose a marketing feature:"
    
    keyboard = [
        [InlineKeyboardButton("🚀 Auto Ads System", callback_data="auto_ads_menu")],
        [InlineKeyboardButton("🔍 Scout Userbots", callback_data="scout_menu")],
        [InlineKeyboardButton("📢 Broadcast Message", callback_data="adm_broadcast_start")],
        [InlineKeyboardButton("🔙 Back to Worker Dashboard", callback_data="worker_dashboard")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

