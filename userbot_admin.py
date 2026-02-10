"""
Userbot Admin Interface
Handles all admin interface for userbot configuration and management
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime

from userbot_config import userbot_config
from userbot_manager import userbot_manager
from userbot_database import (
    get_delivery_stats,
    get_connection_status,
    reset_userbot_config,
    init_userbot_tables
)
from product_delivery import test_userbot_delivery
from utils import is_primary_admin, send_message_with_retry

logger = logging.getLogger(__name__)

# Helper function for permission checks
def check_userbot_access(user_id):
    """Check if user has access to userbot features (admin or worker with marketing permission)"""
    try:
        from worker_management import is_worker, check_worker_permission
        is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    except ImportError:
        is_auth_worker = False
    
    is_admin = is_primary_admin(user_id)
    return is_admin or is_auth_worker

# ==================== MAIN USERBOT CONTROL PANEL ====================

async def handle_userbot_control(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main userbot control panel - shows list of all userbots"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    # Always show userbot list/dashboard
    await _show_userbot_dashboard(query, context)

async def _show_userbot_dashboard(query, context):
    """Show minimalistic dashboard with list of all userbots"""
    import time
    from userbot_database import get_db_connection
    
    update_time = time.strftime("%H:%M:%S")
    
    msg = f"🔐 <b>Secret Chat Userbots</b> <i>(Updated: {update_time})</i>\n\n"
    msg += "⚠️ <b>PURPOSE:</b> These userbots deliver products via TRUE encrypted Telegram secret chats ONLY.\n\n"
    
    # Get all userbots from database with usage info
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT id, name, phone_number, is_enabled, is_connected, 
                   status_message, last_connected_at, session_string,
                   scout_mode_enabled
            FROM userbots 
            ORDER BY priority DESC, id ASC
        """)
        userbots = c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching userbots: {e}")
        userbots = []
    finally:
        conn.close()
    
    if not userbots:
        msg += "📭 <b>No userbots configured yet.</b>\n\n"
        msg += "Click <b>➕ Add Userbot</b> to add your first account!"
    else:
        msg += f"📊 <b>Total Userbots:</b> {len(userbots)}\n\n"
        
        for ub in userbots:
            userbot_id = ub['id']
            name = ub['name']
            phone = ub['phone_number']
            enabled = ub['is_enabled']
            connected = ub['is_connected']
            has_session = bool(ub['session_string'])
            scout_enabled = ub.get('scout_mode_enabled', False)
            
            # Status icon
            if enabled and connected and has_session:
                status_icon = "✅"
                status_text = "Connected & Ready"
            elif has_session and not connected:
                status_icon = "🟡"
                status_text = "Session exists, not connected"
            elif not has_session:
                status_icon = "🔴"
                status_text = "No session (needs setup)"
            elif not enabled:
                status_icon = "⏸️"
                status_text = "Disabled"
            else:
                status_icon = "⚠️"
                status_text = "Unknown status"
            
            # Build usage tags
            usage_tags = []
            if scout_enabled:
                usage_tags.append("🔍 Scout")
            # Add more usage checks here as features are added
            # if auto_ads_enabled: usage_tags.append("📢 Ads")
            
            if not usage_tags:
                usage_tags.append("💬 Delivery Only")
            
            msg += f"{status_icon} <b>{name}</b>\n"
            msg += f"   📱 {phone}\n"
            msg += f"   {status_text}\n"
            msg += f"   Used for: {' | '.join(usage_tags)}\n\n"
    
    # Keyboard - minimalistic design
    keyboard = []
    
    if userbots:
        # Show userbot buttons with clear status indicators
        for ub in userbots[:5]:  # Show max 5 userbots in quick access
            userbot_id = ub['id']
            name = ub['name']
            connected = ub['is_connected']
            icon = "🟢" if connected else "🔴"
            keyboard.append([InlineKeyboardButton(f"{icon} {name}", callback_data=f"userbot_manage:{userbot_id}")])
    
    keyboard.append([InlineKeyboardButton("➕ Add New Userbot", callback_data="userbot_add_new")])
    
    if userbots:
        keyboard.append([
            InlineKeyboardButton("🔄 Reconnect All", callback_data="userbot_reconnect_all"),
            InlineKeyboardButton("📊 Statistics", callback_data="userbot_stats_all")
        ])
    
    # Dynamic back button for workers
    try:
        from worker_management import is_worker, check_worker_permission
        is_auth_worker = is_worker(query.from_user.id) and check_worker_permission(query.from_user.id, 'marketing')
    except:
        is_auth_worker = False
    
    is_admin = is_primary_admin(query.from_user.id)
    back_callback = "admin_menu" if not is_auth_worker or is_admin else "worker_marketing"
    
    keyboard.append([InlineKeyboardButton("🔍 Scout System", callback_data="scout_menu")])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=back_callback)])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_add_new(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start add new userbot wizard"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    msg = "➕ <b>Add New Secret Chat Userbot</b>\n\n"
    msg += "⚠️ <b>PURPOSE:</b> This account will ONLY be used for delivering products via TRUE encrypted Telegram secret chats.\n\n"
    msg += "<b>What you need:</b>\n"
    msg += "1️⃣ A separate Telegram account (phone number)\n"
    msg += "2️⃣ API credentials from https://my.telegram.org/apps\n\n"
    msg += "<b>Setup Steps:</b>\n"
    msg += "• Enter account name (e.g., 'Userbot 1')\n"
    msg += "• Enter API ID\n"
    msg += "• Enter API Hash\n"
    msg += "• Enter phone number\n"
    msg += "• Verify with Telegram code\n"
    msg += "• Done! Userbot ready for secret chat delivery!\n\n"
    msg += "Ready to add a new userbot?"
    
    keyboard = [
        [InlineKeyboardButton("🚀 Start", callback_data="userbot_add_start_name")],
        [InlineKeyboardButton("❌ Cancel", callback_data="userbot_control")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_add_start_name(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Step 1: Ask for userbot name"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    msg = "➕ <b>Step 1/5: Userbot Name</b>\n\n"
    msg += "Give this userbot a name (for identification).\n\n"
    msg += "📝 <b>Examples:</b>\n"
    msg += "• Userbot 1\n"
    msg += "• Secret Chat Account\n"
    msg += "• Delivery Bot\n\n"
    msg += "Please enter the name:"
    
    context.user_data['state'] = 'awaiting_new_userbot_name'
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="userbot_control")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_stats_all(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show statistics for all userbots"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    msg = "📊 <b>Userbot Statistics</b>\n\n"
    msg += "Coming soon! This will show:\n"
    msg += "• Total deliveries per userbot\n"
    msg += "• Success rates\n"
    msg += "• Uptime statistics\n"
    msg += "• Load distribution\n"
    
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="userbot_control")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_reconnect_all(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Reconnect all userbots in the pool"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer("🔄 Reconnecting all userbots...", show_alert=False)
    
    try:
        from userbot_pool import userbot_pool
        
        # Disconnect all first
        await userbot_pool.disconnect_all()
        
        # Re-initialize pool
        await userbot_pool.initialize()
        
        connected_count = len(userbot_pool.clients)
        
        await query.answer(f"✅ Reconnected {connected_count} userbot(s)!", show_alert=True)
        
        # Refresh dashboard
        await handle_userbot_control(update, context)
        
    except Exception as e:
        logger.error(f"Error reconnecting userbots: {e}", exc_info=True)
        await query.answer(f"❌ Error: {str(e)}", show_alert=True)

async def _show_setup_wizard(query, context):
    """Show initial setup wizard"""
    msg = "🤖 <b>Userbot Setup Wizard</b>\n\n"
    msg += "⚠️ <b>PURPOSE:</b> This userbot is used ONLY for delivering products via TRUE encrypted Telegram secret chats.\n\n"
    msg += "<b>What is a userbot?</b>\n"
    msg += "A Telegram user account that acts as a bot. It can:\n"
    msg += "• Create TRUE secret chats (end-to-end encrypted)\n"
    msg += "• Send self-destructing messages\n"
    msg += "• Deliver media securely with no server storage\n\n"
    msg += "<b>Requirements:</b>\n"
    msg += "• A separate Telegram account (NOT your main bot account)\n"
    msg += "• API ID and API Hash from https://my.telegram.org/apps\n"
    msg += "• Phone number for verification\n\n"
    msg += "<b>Two-Step Setup:</b>\n"
    msg += "1️⃣ First: Configure userbot credentials (Pyrogram)\n"
    msg += "2️⃣ Then: Enable secret chats (Telethon)\n\n"
    msg += "Click <b>Start Setup</b> to begin!"
    
    keyboard = [
        [InlineKeyboardButton("🚀 Start Setup", callback_data="userbot_setup_start")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def _show_status_dashboard(query, context):
    """Show userbot status dashboard"""
    # 🚀  Force FRESH read from DB, bypass cache completely
    config = userbot_config.get_dict(force_fresh=True)
    status = get_connection_status()
    stats = get_delivery_stats()
    
    # 🚀  Add timestamp to force message update
    import time
    update_time = time.strftime("%H:%M:%S")
    
    msg = f"🤖 <b>Userbot Control Panel</b> <i>(Updated: {update_time})</i>\n\n"
    
    # Configuration status
    config_status = "✅ Configured" if userbot_config.is_configured() else "❌ Not Configured"
    msg += f"<b>Configuration Status:</b> {config_status}\n"
    
    # Connection status
    is_connected = status.get('is_connected', False)
    conn_status = "✅ Connected" if is_connected else "❌ Disconnected"
    status_msg = status.get('status_message', 'Unknown')
    msg += f"<b>Connection Status:</b> {conn_status}\n"
    msg += f"*{status_msg}*\n"
    
    # Last updated
    last_updated = status.get('last_updated')
    if last_updated:
        msg += f"<b>Last Updated:</b> {last_updated.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
    
    # 🔐 Telethon Secret Chat Status
    msg += "\n<b>🔐 Secret Chat (Telethon):</b>\n"
    try:
        from userbot_telethon_secret import telethon_secret_chat
        if telethon_secret_chat.is_connected:
            msg += "✅ Telethon Connected - TRUE SECRET CHATS ENABLED!\n"
        else:
            msg += "⚠️ Telethon Not Connected - Regular delivery only\n"
            msg += "<i>Set up Telethon for encrypted secret chats</i>\n"
    except Exception as e:
        msg += "❌ Telethon Not Available\n"
    
    msg += "\n<b>Settings:</b>\n"
    
    # Enabled status
    enabled = config.get('enabled', False)
    enabled_status = "✅ Enabled" if enabled else "❌ Disabled"
    msg += f"• Delivery: {enabled_status}\n"
    
    # Auto-reconnect
    auto_reconnect = config.get('auto_reconnect', True)
    reconnect_status = "✅ Enabled" if auto_reconnect else "❌ Disabled"
    msg += f"• Auto-Reconnect: {reconnect_status}\n"
    
    # Notifications
    notifications = config.get('send_notifications', True)
    notif_status = "✅ Enabled" if notifications else "❌ Disabled"
    msg += f"• Admin Notifications: {notif_status}\n"
    
    # TTL
    ttl = config.get('secret_chat_ttl', 86400)
    ttl_hours = ttl // 3600
    msg += f"• Message TTL: {ttl_hours} hours\n"
    
    # Max retries
    max_retries = config.get('max_retries', 3)
    msg += f"• Max Retries: {max_retries}\n"
    
    msg += "\n<b>Statistics:</b>\n"
    msg += f"• Total Deliveries: {stats['total']}\n"
    msg += f"• Success Rate: {stats['success_rate']}%\n"
    msg += f"• Failed Deliveries: {stats['failed']}\n"
    
    # Build keyboard based on connection status
    keyboard = []
    
    if is_connected:
        keyboard.append([
            InlineKeyboardButton("🔌 Disconnect", callback_data="userbot_disconnect"),
            InlineKeyboardButton("🧪 Test", callback_data="userbot_test")
        ])
    else:
        keyboard.append([InlineKeyboardButton("🔌 Connect", callback_data="userbot_connect")])
    
    keyboard.extend([
        [InlineKeyboardButton("⚙️ Settings", callback_data="userbot_settings"),
         InlineKeyboardButton("📊 Stats", callback_data="userbot_stats")],
        [InlineKeyboardButton("🔐 Setup Secret Chat", callback_data="telethon_setup")],
        [InlineKeyboardButton("🗑️ Reset Config", callback_data="userbot_reset_confirm")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

# ==================== SETUP WIZARD ====================

async def handle_userbot_setup_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start setup wizard - ask for API ID"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    msg = "🔧 <b>Step 1/3: API ID</b>\n\n"
    msg += "Get your API ID from: https://my.telegram.org\n\n"
    msg += "1. Log in with your phone number\n"
    msg += "2. Go to 'API development tools'\n"
    msg += "3. Create an application if you haven't\n"
    msg += "4. Copy your <b>API ID</b>\n\n"
    msg += "📝 <b>Please send your API ID now:</b>"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="userbot_control")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    # Set state
    context.user_data['state'] = 'awaiting_userbot_api_id'

async def handle_userbot_api_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle API ID input"""
    if context.user_data.get('state') != 'awaiting_userbot_api_id':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    api_id = update.message.text.strip()
    
    # Validate API ID (should be numeric)
    if not api_id.isdigit():
        await update.message.reply_text(
            "❌ <b>Invalid API ID</b>\n\nAPI ID should be a number. Please try again:",
            parse_mode='HTML'
        )
        return
    
    # Store in context
    context.user_data['userbot_api_id'] = api_id
    context.user_data['state'] = 'awaiting_userbot_api_hash'
    
    msg = "✅ <b>API ID Saved!</b>\n\n"
    msg += "🔧 <b>Step 2/3: API Hash</b>\n\n"
    msg += "From the same page (https://my.telegram.org), copy your <b>API Hash</b>.\n\n"
    msg += "📝 <b>Please send your API Hash now:</b>"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="userbot_control")]]
    
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_api_hash_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle API Hash input"""
    if context.user_data.get('state') != 'awaiting_userbot_api_hash':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    api_hash = update.message.text.strip()
    
    # Validate API Hash (should be alphanumeric, 32 chars)
    if len(api_hash) < 20:
        await update.message.reply_text(
            "❌ <b>Invalid API Hash</b>\n\nAPI Hash seems too short. Please check and try again:",
            parse_mode='HTML'
        )
        return
    
    # Store in context
    context.user_data['userbot_api_hash'] = api_hash
    context.user_data['state'] = 'awaiting_userbot_phone'
    
    msg = "✅ <b>API Hash Saved!</b>\n\n"
    msg += "🔧 <b>Step 3/3: Phone Number</b>\n\n"
    msg += "Enter the phone number for your userbot account.\n\n"
    msg += "<b>Format:</b> +1234567890 (include country code)\n\n"
    msg += "📝 <b>Please send your phone number now:</b>"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="userbot_control")]]
    
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_phone_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle phone number input and start authentication"""
    if context.user_data.get('state') != 'awaiting_userbot_phone':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    phone_number = update.message.text.strip()
    
    # Validate phone number (should start with +)
    if not phone_number.startswith('+'):
        await update.message.reply_text(
            "❌ <b>Invalid Phone Number</b>\n\nPhone number must start with + and include country code.\n\nExample: +1234567890\n\nPlease try again:",
            parse_mode='HTML'
        )
        return
    
    # Get API credentials from context
    api_id = context.user_data.get('userbot_api_id')
    api_hash = context.user_data.get('userbot_api_hash')
    
    if not api_id or not api_hash:
        await update.message.reply_text("❌ <b>Error:</b> Setup data lost. Please start again.")
        context.user_data.pop('state', None)
        return
    
    # Save config to database
    userbot_config.save(api_id, api_hash, phone_number)
    
    # Start phone authentication
    await update.message.reply_text("⏳ <b>Sending verification code...</b>", parse_mode='HTML')
    
    result = await userbot_manager.start_phone_auth(phone_number)
    
    if not result['success']:
        error_msg = result.get('error', 'Unknown error')
        await update.message.reply_text(
            f"❌ <b>Authentication Failed</b>\n\n{error_msg}\n\nPlease try again or contact support.",
            parse_mode='HTML'
        )
        context.user_data.pop('state', None)
        return
    
    # Store phone for verification step
    context.user_data['userbot_phone'] = phone_number
    context.user_data['state'] = 'awaiting_userbot_verification_code'
    
    msg = "✅ <b>Verification Code Sent!</b>\n\n"
    msg += f"A verification code has been sent to <b>{phone_number}</b>.\n\n"
    msg += "📝 <b>Please send the verification code now:</b>"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="userbot_control")]]
    
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_verification_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle verification code input"""
    if context.user_data.get('state') != 'awaiting_userbot_verification_code':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    code = update.message.text.strip()
    phone_number = context.user_data.get('userbot_phone')
    
    if not phone_number:
        await update.message.reply_text("❌ <b>Error:</b> Phone number not found. Please start again.")
        context.user_data.pop('state', None)
        return
    
    await update.message.reply_text("⏳ <b>Verifying code...</b>", parse_mode='HTML')
    
    result = await userbot_manager.verify_phone_code(phone_number, code)
    
    if not result['success']:
        error_msg = result.get('error', 'Unknown error')
        await update.message.reply_text(
            f"❌ <b>Verification Failed</b>\n\n{error_msg}\n\nPlease try again:",
            parse_mode='HTML'
        )
        return
    
    # Clear state
    context.user_data.pop('state', None)
    context.user_data.pop('userbot_api_id', None)
    context.user_data.pop('userbot_api_hash', None)
    context.user_data.pop('userbot_phone', None)
    
    # Success message
    username = result.get('username', 'User')
    msg = "🎉 <b>Setup Complete!</b>\n\n"
    msg += f"Userbot authenticated as <b>@{username}</b>!\n\n"
    msg += "✅ Configuration saved\n"
    msg += "✅ Session stored securely\n\n"
    msg += "Now connecting to Telegram..."
    
    await update.message.reply_text(msg, parse_mode='HTML')
    
    # Initialize userbot
    await asyncio.sleep(1)
    success = await userbot_manager.initialize()
    
    if success:
        await update.message.reply_text(
            "✅ <b>Userbot Connected!</b>\n\nYour userbot is now ready to deliver products via secret chats!",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            "⚠️ <b>Connection Issue</b>\n\nSetup complete but failed to connect. Try reconnecting from the control panel.",
            parse_mode='HTML'
        )

# ==================== CONNECTION MANAGEMENT ====================

async def handle_userbot_connect(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Connect userbot"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer("Connecting...", show_alert=False)
    
    success = await userbot_manager.initialize()
    
    if success:
        await query.answer("✅ Connected successfully!", show_alert=True)
    else:
        await query.answer("❌ Connection failed. Check logs.", show_alert=True)
    
    # Refresh dashboard
    await _show_userbot_dashboard(query, context)

async def handle_userbot_disconnect(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Disconnect userbot"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer("Disconnecting...", show_alert=False)
    
    success = await userbot_manager.disconnect()
    
    if success:
        await query.answer("✅ Disconnected successfully!", show_alert=True)
    else:
        await query.answer("❌ Disconnect failed. Check logs.", show_alert=True)
    
    # Refresh dashboard
    await _show_userbot_dashboard(query, context)

async def handle_userbot_test(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Test userbot delivery"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer("Sending test message...", show_alert=False)
    
    result = await test_userbot_delivery(user_id)
    
    if result['success']:
        await query.answer("✅ Test message sent! Check your messages.", show_alert=True)
    else:
        error_msg = result.get('error', 'Unknown error')
        await query.answer(f"❌ Test failed: {error_msg}", show_alert=True)

# ==================== SETTINGS PANEL ====================

async def handle_userbot_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show settings panel"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    # 🚀  Force FRESH read from DB, bypass cache completely
    config = userbot_config.get_dict(force_fresh=True)
    
    # 🚀  Add microsecond timestamp to force message text change
    import time
    update_time = time.strftime("%H:%M:%S")
    
    msg = f"⚙️ <b>Userbot Settings</b> <i>(Updated: {update_time})</i>\n\n"
    msg += "Configure userbot behavior:\n\n"
    
    # Current settings
    enabled = config.get('enabled', False)
    auto_reconnect = config.get('auto_reconnect', True)
    notifications = config.get('send_notifications', True)
    ttl = config.get('secret_chat_ttl', 86400)
    ttl_hours = ttl // 3600
    max_retries = config.get('max_retries', 3)
    retry_delay = config.get('retry_delay', 5)
    
    msg += f"<b>Delivery:</b> {'✅ Enabled' if enabled else '❌ Disabled'}\n"
    msg += f"<b>Auto-Reconnect:</b> {'✅ Enabled' if auto_reconnect else '❌ Disabled'}\n"
    msg += f"<b>Notifications:</b> {'✅ Enabled' if notifications else '❌ Disabled'}\n"
    msg += f"<b>Message TTL:</b> {ttl_hours} hours\n"
    msg += f"<b>Max Retries:</b> {max_retries}\n"
    msg += f"<b>Retry Delay:</b> {retry_delay} seconds\n"
    
    keyboard = [
        [InlineKeyboardButton(
            f"{'🔴 Disable' if enabled else '🟢 Enable'} Delivery",
            callback_data=f"userbot_toggle_enabled|{not enabled}"
        )],
        [InlineKeyboardButton(
            f"{'🔴 Disable' if auto_reconnect else '🟢 Enable'} Auto-Reconnect",
            callback_data=f"userbot_toggle_reconnect|{not auto_reconnect}"
        )],
        [InlineKeyboardButton(
            f"{'🔴 Disable' if notifications else '🟢 Enable'} Notifications",
            callback_data=f"userbot_toggle_notifications|{not notifications}"
        )],
        [InlineKeyboardButton("⏰ Change TTL", callback_data="userbot_change_ttl"),
         InlineKeyboardButton("🔄 Change Retries", callback_data="userbot_change_retries")],
        [InlineKeyboardButton("⬅️ Back", callback_data="userbot_control")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_toggle_enabled(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle userbot enabled/disabled"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        return
    
    enabled = params[0] == 'True'
    userbot_config.set_enabled(enabled)
    
    # 🚀  Add timestamp to force message change (Telegram won't reject "unchanged" message)
    import time
    status = "enabled" if enabled else "disabled"
    timestamp = time.strftime("%H:%M:%S")
    await query.answer(f"✅ Delivery {status}! ({timestamp})", show_alert=True)
    
    # Refresh settings (force_fresh=True will bypass cache)
    await asyncio.sleep(0.5)
    try:
        await handle_userbot_settings(update, context)
    except Exception as e:
        # If refresh fails, just ignore (likely unchanged message)
        logger.warning(f"Could not refresh settings after toggle: {e}")

async def handle_userbot_toggle_reconnect(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle auto-reconnect"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        return
    
    auto_reconnect = params[0] == 'True'
    userbot_config.set_auto_reconnect(auto_reconnect)
    
    # 🚀  Add timestamp to force UI update
    import time
    status = "enabled" if auto_reconnect else "disabled"
    timestamp = time.strftime("%H:%M:%S")
    await query.answer(f"✅ Auto-reconnect {status}! ({timestamp})", show_alert=True)
    
    # Refresh settings (with a small delay to avoid BadRequest)
    await asyncio.sleep(0.5)
    try:
        await handle_userbot_settings(update, context)
    except Exception as e:
        logger.warning(f"Could not refresh settings after toggle: {e}")

async def handle_userbot_toggle_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle notifications"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        return
    
    notifications = params[0] == 'True'
    userbot_config.set_notifications(notifications)
    
    # 🚀  Add timestamp to force UI update
    import time
    status = "enabled" if notifications else "disabled"
    timestamp = time.strftime("%H:%M:%S")
    await query.answer(f"✅ Notifications {status}! ({timestamp})", show_alert=True)
    
    # Refresh settings (with a small delay to avoid BadRequest)
    await asyncio.sleep(0.5)
    try:
        await handle_userbot_settings(update, context)
    except Exception as e:
        logger.warning(f"Could not refresh settings after toggle: {e}")

# ==================== STATISTICS PANEL ====================

async def handle_userbot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show delivery statistics"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    stats = get_delivery_stats()
    
    msg = "📊 <b>Delivery Statistics</b>\n\n"
    msg += f"<b>Total Deliveries:</b> {stats['total']}\n"
    msg += f"<b>Successful:</b> {stats['success']} ✅\n"
    msg += f"<b>Failed:</b> {stats['failed']} ❌\n"
    msg += f"<b>Success Rate:</b> {stats['success_rate']}%\n\n"
    
    # Recent deliveries
    recent = stats.get('recent_deliveries', [])
    if recent:
        msg += "<b>Recent Deliveries:</b>\n\n"
        for delivery in recent[:5]:
            status_emoji = "✅" if delivery['delivery_status'] == 'success' else "❌"
            delivered_at = delivery.get('delivered_at')
            time_str = delivered_at.strftime('%Y-%m-%d %H:%M') if delivered_at else 'N/A'
            msg += f"{status_emoji} User {delivery['user_id']} - {time_str}\n"
            if delivery.get('error_message'):
                msg += f"   *Error: {delivery['error_message'][:50]}*\n"
    else:
        msg += "No deliveries yet."
    
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="userbot_control")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

# ==================== RESET CONFIRMATION ====================

async def handle_userbot_reset_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm reset configuration"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    msg = "⚠️ <b>Reset Userbot Configuration</b>\n\n"
    msg += "This will:\n"
    msg += "• Delete all configuration\n"
    msg += "• Remove saved session\n"
    msg += "• Disconnect userbot\n"
    msg += "• Keep delivery statistics\n\n"
    msg += "<b>Are you sure you want to reset?</b>"
    
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Reset", callback_data="userbot_reset_confirmed"),
         InlineKeyboardButton("❌ Cancel", callback_data="userbot_control")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_reset_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Reset userbot configuration"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    # Disconnect first
    await userbot_manager.disconnect()
    
    # Reset config
    success = reset_userbot_config()
    
    if success:
        await query.answer("✅ Configuration reset!", show_alert=True)
        msg = "✅ <b>Configuration Reset</b>\n\nUserbot configuration has been reset. You can set it up again anytime."
        keyboard = [
            [InlineKeyboardButton("🚀 Setup Again", callback_data="userbot_setup_start")],
            [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    else:
        await query.answer("❌ Reset failed. Check logs.", show_alert=True)

# Import asyncio for the phone verification handler
import asyncio

# ==================== TELETHON SECRET CHAT SETUP ====================

async def handle_telethon_setup(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show Telethon secret chat setup wizard"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    # Check if Telethon is already connected
    try:
        from userbot_telethon_secret import telethon_secret_chat
        if telethon_secret_chat.is_connected:
            msg = "🔐 <b>Telethon Secret Chat Status</b>\n\n"
            msg += "✅ <b>Already Connected!</b>\n\n"
            msg += "TRUE SECRET CHATS are enabled.\n"
            msg += "All deliveries will use end-to-end encrypted secret chats.\n\n"
            msg += "<b>Features:</b>\n"
            msg += "• End-to-end encryption\n"
            msg += "• Self-destructing messages\n"
            msg += "• No server storage\n"
            msg += "• Perfect forward secrecy\n"
            
            keyboard = [
                [InlineKeyboardButton("🔌 Disconnect Telethon", callback_data="telethon_disconnect")],
                [InlineKeyboardButton("⬅️ Back", callback_data="userbot_control")]
            ]
            
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return
    except Exception as e:
        logger.error(f"Error checking Telethon status: {e}")
    
    # Check if userbot is configured
    if not userbot_config.is_configured():
        await query.answer("❌ Please set up userbot first!", show_alert=True)
        await _show_setup_wizard(query, context)
        return
    
    # Show setup wizard
    msg = "🔐 <b>Telethon Secret Chat Setup</b>\n\n"
    msg += "⚠️ <b>IMPORTANT:</b> This userbot is ONLY for delivering products via TRUE encrypted Telegram secret chats.\n\n"
    msg += "<b>What are SECRET CHATS?</b>\n"
    msg += "• End-to-end encrypted (not stored on Telegram servers)\n"
    msg += "• Self-destructing messages\n"
    msg += "• Perfect forward secrecy\n"
    msg += "• Cannot be forwarded or screenshotted without notification\n\n"
    msg += "<b>Why Telethon?</b>\n"
    msg += "Pyrogram doesn't support creating secret chats. We use Telethon for TRUE secret chat delivery.\n\n"
    msg += "<b>Setup Process:</b>\n"
    msg += "1. We'll use your SAME userbot credentials\n"
    msg += "2. Send verification code to your phone\n"
    msg += "3. Enter the code QUICKLY (codes expire in ~2 minutes!)\n"
    msg += "4. Done! Secret chats enabled.\n\n"
    msg += "<i>Note: This is a one-time setup. Your Telethon session will be saved securely in PostgreSQL.</i>\n\n"
    msg += "Ready to enable TRUE secret chats?"
    
    keyboard = [
        [InlineKeyboardButton("🚀 Start Setup", callback_data="telethon_start_auth")],
        [InlineKeyboardButton("❌ Cancel", callback_data="userbot_control")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_telethon_start_auth(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start Telethon authentication process"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer("⏳ Sending code...", show_alert=False)
    
    try:
        from userbot_telethon_secret import telethon_secret_chat
        
        # Get userbot config
        config = userbot_config.get_dict()
        api_id = config.get('api_id')
        api_hash = config.get('api_hash')
        phone = config.get('phone_number')
        
        if not all([api_id, api_hash, phone]):
            await query.answer("❌ Missing userbot config!", show_alert=True)
            return
        
        # Start authentication
        success, message, needs_code = await telethon_secret_chat.authenticate_telethon(
            int(api_id),
            api_hash,
            phone
        )
        
        if not success:
            await query.answer(f"❌ {message}", show_alert=True)
            return
        
        # Store auth data in context for verification step
        context.user_data['telethon_api_id'] = api_id
        context.user_data['telethon_api_hash'] = api_hash
        context.user_data['telethon_phone'] = phone
        context.user_data['state'] = 'awaiting_telethon_code'
        
        msg = "🔐 <b>Verification Code Sent!</b>\n\n"
        msg += f"A verification code has been sent to <b>{phone}</b>.\n\n"
        msg += "⏰ <b>IMPORTANT:</b> Enter the code within 2 minutes!\n\n"
        msg += "📱 Please enter the code you received:\n\n"
        msg += "<i>Example: 12345</i>"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Request New Code", callback_data="telethon_start_auth")],
            [InlineKeyboardButton("❌ Cancel", callback_data="telethon_cancel_auth")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error starting Telethon auth: {e}", exc_info=True)
        await query.answer(f"❌ Error: {str(e)}", show_alert=True)

async def handle_telethon_verification_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Telethon verification code input"""
    if context.user_data.get('state') != 'awaiting_telethon_code':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    code = update.message.text.strip()
    api_id = context.user_data.get('telethon_api_id')
    api_hash = context.user_data.get('telethon_api_hash')
    phone = context.user_data.get('telethon_phone')
    
    if not all([api_id, api_hash, phone]):
        await update.message.reply_text("❌ <b>Error:</b> Setup data lost. Please start again.", parse_mode='HTML')
        context.user_data.pop('state', None)
        return
    
    await update.message.reply_text("⏳ <b>Verifying code...</b>", parse_mode='HTML')
    
    try:
        from userbot_telethon_secret import telethon_secret_chat
        
        success, message, session_string = await telethon_secret_chat.complete_telethon_auth(
            int(api_id),
            api_hash,
            phone,
            code
        )
        
        if not success:
            # Check if code expired
            if "expired" in message.lower():
                await update.message.reply_text(
                    f"❌ <b>Code Expired!</b>\n\n{message}\n\n"
                    "⚠️ Telegram codes expire in ~2 minutes.\n\n"
                    "Please go back to Admin → Userbot Control → Setup Secret Chat and try again with a NEW code.",
                    parse_mode='HTML'
                )
                # Clear state
                context.user_data.pop('state', None)
                context.user_data.pop('telethon_api_id', None)
                context.user_data.pop('telethon_api_hash', None)
                context.user_data.pop('telethon_phone', None)
            else:
                await update.message.reply_text(
                    f"❌ <b>Verification Failed</b>\n\n{message}\n\nPlease try again:",
                    parse_mode='HTML'
                )
            return
        
        # Clear state
        context.user_data.pop('state', None)
        context.user_data.pop('telethon_api_id', None)
        context.user_data.pop('telethon_api_hash', None)
        context.user_data.pop('telethon_phone', None)
        
        # Success message
        msg = "🎉 <b>Telethon Setup Complete!</b>\n\n"
        msg += f"✅ {message}\n\n"
        msg += "🔐 <b>TRUE SECRET CHATS ENABLED!</b>\n\n"
        msg += "All product deliveries will now use:\n"
        msg += "• End-to-end encryption\n"
        msg += "• Self-destructing messages\n"
        msg += "• No server storage\n"
        msg += "• Perfect forward secrecy\n\n"
        msg += "Your buyers will receive products in encrypted secret chats! 🎯"
        
        await update.message.reply_text(msg, parse_mode='HTML')
        
        # Now re-initialize Telethon to connect it
        await asyncio.sleep(1)
        await update.message.reply_text("⏳ <b>Connecting Telethon...</b>", parse_mode='HTML')
        
        telethon_initialized = await telethon_secret_chat.initialize(
            int(api_id),
            api_hash,
            phone,
            use_existing_pyrogram=False
        )
        
        if telethon_initialized:
            await update.message.reply_text(
                "✅ <b>Telethon Connected!</b>\n\nSecret chat delivery is now active! 🔐",
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                "⚠️ <b>Telethon saved but not connected</b>\n\nIt will connect on next restart.",
                parse_mode='HTML'
            )
        
    except Exception as e:
        logger.error(f"Error completing Telethon auth: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ <b>Error:</b> {str(e)}\n\nPlease try again or contact support.",
            parse_mode='HTML'
        )

async def handle_telethon_cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Cancel Telethon authentication"""
    query = update.callback_query
    
    # Clear state
    context.user_data.pop('state', None)
    context.user_data.pop('telethon_api_id', None)
    context.user_data.pop('telethon_api_hash', None)
    context.user_data.pop('telethon_phone', None)
    
    await query.answer("❌ Setup cancelled", show_alert=False)
    await handle_userbot_control(update, context)

async def handle_telethon_disconnect(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Disconnect Telethon"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    try:
        from userbot_telethon_secret import telethon_secret_chat
        await telethon_secret_chat.disconnect()
        
        # Also remove session from database
        from userbot_database import get_db_connection
        conn = get_db_connection()
        c = conn.cursor()
        try:
            c.execute("DELETE FROM system_settings WHERE setting_key = 'telethon_session_string'")
            conn.commit()
        except Exception as e:
            logger.warning(f"Could not remove Telethon session from DB: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        await query.answer("✅ Telethon disconnected!", show_alert=True)
        await handle_userbot_control(update, context)
        
    except Exception as e:
        logger.error(f"Error disconnecting Telethon: {e}")
        await query.answer(f"❌ Error: {str(e)}", show_alert=True)

# ==================== NEW USERBOT ADD FLOW (Message Handlers) ====================

async def handle_new_userbot_name_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new userbot name input"""
    if context.user_data.get('state') != 'awaiting_new_userbot_name':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    name = update.message.text.strip()
    
    if len(name) < 3:
        await update.message.reply_text(
            "❌ Name too short. Please enter at least 3 characters:",
            parse_mode='HTML'
        )
        return
    
    # Check if name already exists
    from userbot_database import get_db_connection
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT id FROM userbots WHERE name = %s", (name,))
        if c.fetchone():
            await update.message.reply_text(
                f"❌ A userbot with name '<b>{name}</b>' already exists.\n\nPlease choose a different name:",
                parse_mode='HTML'
            )
            return
    finally:
        conn.close()
    
    # Store name and move to next step
    context.user_data['new_userbot_name'] = name
    context.user_data['state'] = 'awaiting_new_userbot_api_id'
    
    msg = f"➕ <b>Step 2/5: API ID</b>\n\n"
    msg += f"Userbot Name: <b>{name}</b>\n\n"
    msg += "Now enter your <b>API ID</b>.\n\n"
    msg += "🔗 Get it from: https://my.telegram.org/apps\n\n"
    msg += "📝 Example: 12345678"
    
    await update.message.reply_text(msg, parse_mode='HTML')

async def handle_new_userbot_api_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new userbot API ID input"""
    if context.user_data.get('state') != 'awaiting_new_userbot_api_id':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    api_id = update.message.text.strip()
    
    if not api_id.isdigit():
        await update.message.reply_text(
            "❌ API ID must be a number.\n\nPlease try again:",
            parse_mode='HTML'
        )
        return
    
    context.user_data['new_userbot_api_id'] = api_id
    context.user_data['state'] = 'awaiting_new_userbot_api_hash'
    
    name = context.user_data.get('new_userbot_name', 'Userbot')
    
    msg = f"➕ <b>Step 3/5: API Hash</b>\n\n"
    msg += f"Userbot Name: <b>{name}</b>\n"
    msg += f"API ID: <code>{api_id}</code>\n\n"
    msg += "Now enter your <b>API Hash</b>.\n\n"
    msg += "🔗 Get it from: https://my.telegram.org/apps\n\n"
    msg += "📝 Example: 1234567890abcdef1234567890abcdef"
    
    await update.message.reply_text(msg, parse_mode='HTML')

async def handle_new_userbot_api_hash_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new userbot API Hash input"""
    if context.user_data.get('state') != 'awaiting_new_userbot_api_hash':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    api_hash = update.message.text.strip()
    
    if len(api_hash) < 32:
        await update.message.reply_text(
            "❌ API Hash seems too short (should be 32 characters).\n\nPlease try again:",
            parse_mode='HTML'
        )
        return
    
    context.user_data['new_userbot_api_hash'] = api_hash
    context.user_data['state'] = 'awaiting_new_userbot_phone'
    
    name = context.user_data.get('new_userbot_name', 'Userbot')
    api_id = context.user_data.get('new_userbot_api_id', 'N/A')
    
    msg = f"➕ <b>Step 4/5: Phone Number</b>\n\n"
    msg += f"Userbot Name: <b>{name}</b>\n"
    msg += f"API ID: <code>{api_id}</code>\n"
    msg += f"API Hash: <code>{api_hash[:8]}...{api_hash[-8:]}</code>\n\n"
    msg += "Now enter your <b>Phone Number</b> (with country code).\n\n"
    msg += "📱 <b>Format:</b> +1234567890\n\n"
    msg += "⚠️ Must start with +"
    
    await update.message.reply_text(msg, parse_mode='HTML')

async def handle_new_userbot_phone_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new userbot phone number input and send verification code"""
    if context.user_data.get('state') != 'awaiting_new_userbot_phone':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    phone = update.message.text.strip()
    
    if not phone.startswith('+'):
        await update.message.reply_text(
            "❌ Phone number must start with + and include country code.\n\nExample: +1234567890\n\nPlease try again:",
            parse_mode='HTML'
        )
        return
    
    # Check if phone already exists
    from userbot_database import get_db_connection
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT id FROM userbots WHERE phone_number = %s", (phone,))
        if c.fetchone():
            await update.message.reply_text(
                f"❌ A userbot with phone <b>{phone}</b> already exists.\n\nPlease use a different phone number:",
                parse_mode='HTML'
            )
            return
    finally:
        conn.close()
    
    context.user_data['new_userbot_phone'] = phone
    
    # Send verification code via Telethon
    await update.message.reply_text("⏳ <b>Sending verification code...</b>", parse_mode='HTML')
    
    api_id = context.user_data.get('new_userbot_api_id')
    api_hash = context.user_data.get('new_userbot_api_hash')
    
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        
        # Create Telethon client that will stay alive
        temp_client = TelegramClient(StringSession(), int(api_id), api_hash)
        await temp_client.connect()
        
        # Send code and get phone_code_hash
        sent_code = await temp_client.send_code_request(phone)
        phone_code_hash = sent_code.phone_code_hash
        
        # DON'T disconnect! Keep the client alive in context
        # Store EVERYTHING in context for the next step
        context.user_data['new_userbot_temp_client'] = temp_client
        context.user_data['new_userbot_phone_code_hash'] = phone_code_hash
        context.user_data['state'] = 'awaiting_new_userbot_code'
        
        logger.info(f"✅ Code sent to {phone}, client kept alive for verification")
        
        name = context.user_data.get('new_userbot_name', 'Userbot')
        
        msg = f"➕ <b>Step 5/5: Verification Code</b>\n\n"
        msg += f"Userbot Name: <b>{name}</b>\n"
        msg += f"Phone: <b>{phone}</b>\n\n"
        msg += f"✅ Verification code sent to <b>{phone}</b>!\n\n"
        msg += "⏰ <b>IMPORTANT:</b> Enter the code within 2 minutes!\n\n"
        msg += "📱 Please enter the code you received:"
        
        await update.message.reply_text(msg, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error sending code for new userbot: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ <b>Error:</b> {str(e)}\n\nPlease start over from Admin → Userbot Control.",
            parse_mode='HTML'
        )
        context.user_data.pop('state', None)

async def handle_new_userbot_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new userbot verification code and complete setup"""
    if context.user_data.get('state') != 'awaiting_new_userbot_code':
        return
    
    user_id = update.effective_user.id
    if not check_userbot_access(user_id):
        return
    
    code = update.message.text.strip()
    
    await update.message.reply_text("⏳ <b>Verifying code and creating userbot...</b>", parse_mode='HTML')
    
    # Get all stored data INCLUDING the temp client
    name = context.user_data.get('new_userbot_name')
    api_id = context.user_data.get('new_userbot_api_id')
    api_hash = context.user_data.get('new_userbot_api_hash')
    phone = context.user_data.get('new_userbot_phone')
    phone_code_hash = context.user_data.get('new_userbot_phone_code_hash')
    temp_client = context.user_data.get('new_userbot_temp_client')
    
    if not all([name, api_id, api_hash, phone, phone_code_hash, temp_client]):
        await update.message.reply_text(
            "❌ <b>Error:</b> Setup data lost. Please start over from Admin → Userbot Control.",
            parse_mode='HTML'
        )
        context.user_data.pop('state', None)
        return
    
    try:
        from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError
        
        # Use the EXISTING client that sent the code (DON'T create a new one!)
        logger.info(f"Using existing client to verify code for {phone}")
        
        try:
            await temp_client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        except PhoneCodeExpiredError:
            await temp_client.disconnect()
            context.user_data.pop('new_userbot_temp_client', None)
            await update.message.reply_text(
                "❌ <b>Code Expired!</b>\n\n"
                "⚠️ Telegram codes expire in ~2 minutes.\n\n"
                "Please go back to Admin → Userbot Control → Add New Userbot and try again.",
                parse_mode='HTML'
            )
            context.user_data.pop('state', None)
            return
        except PhoneCodeInvalidError:
            # DON'T disconnect on invalid code - let user retry
            await update.message.reply_text(
                "❌ <b>Invalid Code!</b>\n\nPlease try again:",
                parse_mode='HTML'
            )
            return
        except SessionPasswordNeededError:
            await temp_client.disconnect()
            context.user_data.pop('new_userbot_temp_client', None)
            await update.message.reply_text(
                "❌ <b>2FA Enabled</b>\n\n"
                "This account has Two-Factor Authentication enabled.\n"
                "Please disable it temporarily and try again.",
                parse_mode='HTML'
            )
            context.user_data.pop('state', None)
            return
        
        # Get user info
        me = await temp_client.get_me()
        username = me.username or me.first_name
        
        # Get session string
        session_string = temp_client.session.save()
        
        await temp_client.disconnect()
        
        # Save to database
        from userbot_database import get_db_connection
        conn = get_db_connection()
        c = conn.cursor()
        try:
            c.execute("""
                INSERT INTO userbots (name, api_id, api_hash, phone_number, session_string, is_enabled, is_connected)
                VALUES (%s, %s, %s, %s, %s, TRUE, FALSE)
                RETURNING id
            """, (name, api_id, api_hash, phone, session_string))
            
            new_userbot_id = c.fetchone()['id']
            conn.commit()
            
            logger.info(f"✅ New userbot created: ID={new_userbot_id}, Name={name}, Phone={phone}")
            
        except Exception as db_err:
            logger.error(f"Error saving userbot to database: {db_err}")
            conn.rollback()
            await update.message.reply_text(
                f"❌ <b>Database Error:</b> {str(db_err)}\n\nPlease try again.",
                parse_mode='HTML'
            )
            context.user_data.pop('state', None)
            return
        finally:
            conn.close()
        
        # Clear state and temp client
        context.user_data.pop('state', None)
        context.user_data.pop('new_userbot_name', None)
        context.user_data.pop('new_userbot_api_id', None)
        context.user_data.pop('new_userbot_api_hash', None)
        context.user_data.pop('new_userbot_phone', None)
        context.user_data.pop('new_userbot_phone_code_hash', None)
        context.user_data.pop('new_userbot_temp_client', None)  # Clear the temp client too
        
        # Auto-connect the userbot
        logger.info(f"🔄 Auto-connecting newly created userbot #{new_userbot_id}...")
        try:
            from userbot_pool import userbot_pool
            connect_success = await userbot_pool.connect_single_userbot(new_userbot_id)
            
            if connect_success:
                connection_status = "✅ Connected & Ready"
            else:
                connection_status = "⚠️ Created but connection failed (check logs)"
        except Exception as e:
            logger.error(f"Error auto-connecting userbot: {e}")
            connection_status = "⚠️ Created but connection failed"
        
        # Success message
        msg = f"🎉 <b>Userbot Created Successfully!</b>\n\n"
        msg += f"✅ Name: <b>{name}</b>\n"
        msg += f"📱 Phone: <b>{phone}</b>\n"
        msg += f"👤 Logged in as: <b>@{username}</b>\n\n"
        msg += f"🔐 Status: {connection_status}\n\n"
        msg += f"This userbot is now ready for TRUE SECRET CHAT delivery!"
        
        await update.message.reply_text(msg, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error completing new userbot setup: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ <b>Error:</b> {str(e)}\n\nPlease try again or contact support.",
            parse_mode='HTML'
        )
        context.user_data.pop('state', None)

