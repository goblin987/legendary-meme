"""
Scout System Admin Interface
Admin panel for managing scout keywords and monitoring triggers
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from utils import is_primary_admin, get_db_connection
from userbot_scout import add_keyword, toggle_keyword, delete_keyword, toggle_scout_mode

# Import worker permissions
try:
    from worker_management import is_worker, check_worker_permission
except ImportError:
    def is_worker(uid): return False
    def check_worker_permission(uid, perm): return False

logger = logging.getLogger(__name__)

# ==================== MAIN SCOUT MENU ====================

async def handle_scout_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main scout system menu"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    # Get stats
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) as count FROM scout_keywords WHERE is_active = TRUE")
    active_keywords = c.fetchone()['count']
    
    c.execute("SELECT COUNT(*) as count FROM userbots")
    total_userbots = c.fetchone()['count']
    
    c.execute("SELECT COUNT(*) as count FROM userbots WHERE is_connected = TRUE")
    connected_bots = c.fetchone()['count']
    
    c.execute("SELECT COUNT(*) as count FROM userbots WHERE scout_mode_enabled = TRUE")
    scout_bots = c.fetchone()['count']
    
    c.execute("SELECT COUNT(*) as count FROM scout_triggers WHERE triggered_at > NOW() - INTERVAL '24 hours'")
    triggers_24h = c.fetchone()['count']
    
    c.execute("SELECT COUNT(*) as count FROM scout_triggers WHERE response_sent = TRUE AND triggered_at > NOW() - INTERVAL '24 hours'")
    responses_24h = c.fetchone()['count']
    
    conn.close()
    
    # Determine system status
    if total_userbots == 0:
        status = "⚠️ **No Userbots Configured**"
        status_msg = "\n\n❗ You need to add userbots first!\nGo to: Marketing > Userbot Control > Add Userbot\n"
    elif connected_bots == 0:
        status = "🔴 **All Userbots Offline**"
        status_msg = "\n\n⚠️ Connect your userbots to enable scout mode.\n"
    elif scout_bots == 0:
        status = "⚠️ **Scout Mode Disabled**"
        status_msg = "\n\n💡 Enable scout mode on at least one userbot to start monitoring.\n"
    elif active_keywords == 0:
        status = "⚠️ **No Active Keywords**"
        status_msg = "\n\n💡 Add keywords to detect and auto-reply to messages.\n"
    else:
        status = "✅ **Scout System Active**"
        status_msg = "\n\n🎯 Scout mode is running and monitoring for keywords!\n"
    
    msg = (
        f"🔍 **Scout System Control Panel**\n\n"
        f"{status}{status_msg}\n"
        f"📊 **Statistics (Last 24h):**\n"
        f"🔑 Active Keywords: **{active_keywords}**\n"
        f"🤖 Userbots: **{connected_bots}/{total_userbots}** online | **{scout_bots}** with scout enabled\n"
        f"🎯 Triggers Detected: **{triggers_24h}**\n"
        f"✅ Responses Sent: **{responses_24h}**\n\n"
        f"_Scout userbots automatically reply when they detect keywords in groups._"
    )
    
    # Dynamic back button based on user role
    back_callback = "userbot_control"  # Default for admin
    if is_auth_worker and not is_admin:
        back_callback = "worker_marketing"  # For workers
    
    keyboard = [
        [InlineKeyboardButton("🔑 Manage Keywords", callback_data="scout_keywords|0")],
        [InlineKeyboardButton("🤖 Configure Userbots", callback_data="scout_userbots")],
        [InlineKeyboardButton("📊 View Triggers Log", callback_data="scout_triggers|0")],
        [InlineKeyboardButton("🧪 Test Scout System", callback_data="scout_test_system")],
        [InlineKeyboardButton("📖 Quick Start Guide", callback_data="scout_quick_start")],
        [InlineKeyboardButton("⬅️ Back", callback_data=back_callback)]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


# ==================== KEYWORDS MANAGEMENT ====================

async def handle_scout_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """List all keywords with pagination"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    page = int(params[0]) if params else 0
    per_page = 10
    offset = page * per_page
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Get total count
    c.execute("SELECT COUNT(*) as count FROM scout_keywords")
    total = c.fetchone()['count']
    
    # Get keywords for this page with creator info
    c.execute("""
        SELECT sk.id, sk.keyword, sk.match_type, sk.response_text, sk.is_active, sk.uses_count, 
               sk.created_by, COALESCE(w.username, 'Admin') as added_by_name
        FROM scout_keywords sk
        LEFT JOIN workers w ON sk.created_by = w.user_id
        ORDER BY sk.is_active DESC, sk.uses_count DESC, sk.id DESC
        LIMIT %s OFFSET %s
    """, (per_page, offset))
    keywords = c.fetchall()
    conn.close()
    
    total_pages = (total + per_page - 1) // per_page
    
    msg = f"🔑 **Scout Keywords** (Page {page + 1}/{max(total_pages, 1)})\n\n"
    
    keyboard = []
    
    if not keywords:
        msg += "No keywords configured yet.\n\nAdd your first keyword to start scout mode!"
    else:
        for kw in keywords:
            status = "✅" if kw['is_active'] else "❌"
            response_preview = kw['response_text'][:40] + "..." if len(kw['response_text']) > 40 else kw['response_text']
            msg += f"{status} **{kw['keyword']}** ({kw['match_type']})\n"
            msg += f"   Uses: {kw['uses_count']} | Response: {response_preview}\n"
            msg += f"   Added by: {kw['added_by_name']}\n\n"
            
            # Add action buttons for each keyword
            toggle_text = "🔴 Disable" if kw['is_active'] else "🟢 Enable"
            keyboard.append([
                InlineKeyboardButton("✏️ Edit", callback_data=f"scout_edit_keyword|{kw['id']}"),
                InlineKeyboardButton(toggle_text, callback_data=f"scout_toggle_keyword|{kw['id']}"),
                InlineKeyboardButton("🗑️ Delete", callback_data=f"scout_delete_keyword|{kw['id']}")
            ])
    
    keyboard.append([InlineKeyboardButton("➕ Add Keyword", callback_data="scout_add_keyword_start")])
    
    # Bulk actions (only show if there are keywords)
    if keywords:
        keyboard.append([
            InlineKeyboardButton("✅ Enable All", callback_data="scout_bulk_enable"),
            InlineKeyboardButton("❌ Disable All", callback_data="scout_bulk_disable")
        ])
    
    # Pagination
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"scout_keywords|{page-1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"scout_keywords|{page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([
        InlineKeyboardButton("🔄 Refresh", callback_data=f"scout_keywords|{page}"),
        InlineKeyboardButton("⬅️ Back", callback_data="scout_menu")
    ])
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Already up to date", show_alert=False)
        else:
            raise


async def handle_scout_add_keyword_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start adding a new keyword"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    msg = (
        "➕ **Add Scout Keyword**\n\n"
        "**Step 1:** Enter the keyword to detect\n\n"
        "Examples:\n"
        "• `discount code` - detect when someone asks for discounts\n"
        "• `steam games` - detect steam game mentions\n"
        "• `looking for shop` - detect people looking for shops\n\n"
        "Type your keyword:"
    )
    
    context.user_data['state'] = 'awaiting_scout_keyword'
    context.user_data['scout_keyword_data'] = {}
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="scout_keywords|0")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


async def handle_scout_keyword_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin messages for keyword creation"""
    user_id = update.effective_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        return
    
    state = context.user_data.get('state')
    text = update.message.text.strip()
    
    if state == 'awaiting_scout_keyword':
        # Save keyword and ask for response
        context.user_data['scout_keyword_data']['keyword'] = text
        context.user_data['state'] = 'awaiting_scout_response'
        
        msg = (
            f"✅ Keyword: **{text}**\n\n"
            f"**Step 2:** Enter the auto-reply message\n\n"
            f"This message will be sent when the keyword is detected.\n\n"
            f"Example:\n"
            f"`🎮 Check out our shop! Get 10% off with code WELCOME10\n@YourBotName`\n\n"
            f"Type your response:"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="scout_keywords|0")]]
        
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    elif state == 'awaiting_scout_response':
        # Save response and show confirmation
        context.user_data['scout_keyword_data']['response_text'] = text
        context.user_data['state'] = None
        
        keyword_data = context.user_data['scout_keyword_data']
        
        # Add to database
        keyword_id = add_keyword(
            keyword=keyword_data['keyword'],
            response_text=keyword_data['response_text'],
            match_type='contains',  # Default
            case_sensitive=False,  # Default
            response_delay=3,  # Default
            created_by=user_id
        )
        
        if keyword_id:
            msg = (
                f"✅ **Keyword Added Successfully!**\n\n"
                f"**Keyword:** {keyword_data['keyword']}\n"
                f"**Response:** {keyword_data['response_text'][:100]}...\n"
                f"**Match Type:** Contains (case-insensitive)\n"
                f"**Delay:** 3 seconds\n\n"
                f"Scout userbots will now reply when this keyword is detected in groups!"
            )
        else:
            msg = "❌ Error adding keyword. Please try again."
        
        keyboard = [
            [InlineKeyboardButton("🔑 Back to Keywords", callback_data="scout_keywords|0")],
            [InlineKeyboardButton("🔍 Scout Menu", callback_data="scout_menu")]
        ]
        
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
        # Clear context
        context.user_data.pop('scout_keyword_data', None)


async def handle_scout_toggle_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle keyword active status"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    keyword_id = int(params[0]) if params else None
    
    if not keyword_id:
        await query.answer("Error: Invalid keyword ID", show_alert=True)
        return
    
    # Get current status
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT is_active FROM scout_keywords WHERE id = %s", (keyword_id,))
    result = c.fetchone()
    conn.close()
    
    if not result:
        await query.answer("Error: Keyword not found", show_alert=True)
        return
    
    new_status = not result['is_active']
    toggle_keyword(keyword_id, new_status)
    
    await query.answer(f"✅ Keyword {'enabled' if new_status else 'disabled'}", show_alert=True)
    
    # Refresh the list
    await handle_scout_keywords(update, context, ['0'])


async def handle_scout_delete_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Delete a keyword"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    keyword_id = int(params[0]) if params else None
    
    if not keyword_id:
        await query.answer("Error: Invalid keyword ID", show_alert=True)
        return
    
    delete_keyword(keyword_id)
    
    await query.answer("✅ Keyword deleted", show_alert=True)
    
    # Refresh the list
    await handle_scout_keywords(update, context, ['0'])


# ==================== EDIT KEYWORD HANDLERS ====================

async def handle_scout_edit_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show edit menu for a keyword"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    keyword_id = int(params[0]) if params else None
    if not keyword_id:
        await query.answer("❌ Invalid keyword ID", show_alert=True)
        return
    
    # Get keyword details
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        SELECT id, keyword, match_type, response_text, is_active, 
               case_sensitive, response_delay
        FROM scout_keywords 
        WHERE id = %s
    """, (keyword_id,))
    kw = c.fetchone()
    conn.close()
    
    if not kw:
        await query.answer("❌ Keyword not found", show_alert=True)
        return
    
    msg = (
        f"✏️ **Edit Keyword**\n\n"
        f"**Current Settings:**\n"
        f"🔑 Keyword: `{kw['keyword']}`\n"
        f"💬 Response: {kw['response_text'][:100]}{'...' if len(kw['response_text']) > 100 else ''}\n"
        f"🎯 Match Type: {kw['match_type']}\n"
        f"⏱️ Delay: {kw['response_delay']}s\n"
        f"{'✅' if kw['is_active'] else '❌'} Status: {'Active' if kw['is_active'] else 'Disabled'}\n\n"
        f"What would you like to change?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✏️ Edit Keyword Text", callback_data=f"scout_edit_kw_text|{keyword_id}")],
        [InlineKeyboardButton("💬 Edit Response", callback_data=f"scout_edit_kw_response|{keyword_id}")],
        [InlineKeyboardButton("🎯 Change Match Type", callback_data=f"scout_edit_kw_match|{keyword_id}")],
        [InlineKeyboardButton("⏱️ Change Delay", callback_data=f"scout_edit_kw_delay|{keyword_id}")],
        [InlineKeyboardButton(f"{'🔴 Disable' if kw['is_active'] else '🟢 Enable'}", 
                             callback_data=f"scout_toggle_keyword|{keyword_id}")],
        [InlineKeyboardButton("⬅️ Back to Keywords", callback_data="scout_keywords|0")]
    ]
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Already up to date")
        else:
            logger.error(f"Error editing keyword menu: {e}")
            raise


async def handle_scout_edit_kw_text(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start editing keyword text"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    keyword_id = int(params[0]) if params else None
    if not keyword_id:
        await query.answer("❌ Invalid keyword ID", show_alert=True)
        return
    
    # Get current keyword
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT keyword FROM scout_keywords WHERE id = %s", (keyword_id,))
    result = c.fetchone()
    conn.close()
    
    if not result:
        await query.answer("❌ Keyword not found", show_alert=True)
        return
    
    msg = (
        f"✏️ **Edit Keyword Text**\n\n"
        f"Current keyword: `{result['keyword']}`\n\n"
        f"Type the new keyword text:"
    )
    
    context.user_data['state'] = 'awaiting_scout_edit_keyword_text'
    context.user_data['edit_keyword_id'] = keyword_id
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"scout_edit_keyword|{keyword_id}")]]
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Already up to date")
        else:
            logger.error(f"Error starting keyword text edit: {e}")
            raise


async def handle_scout_edit_kw_response(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start editing keyword response"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    keyword_id = int(params[0]) if params else None
    if not keyword_id:
        await query.answer("❌ Invalid keyword ID", show_alert=True)
        return
    
    # Get current response
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT response_text FROM scout_keywords WHERE id = %s", (keyword_id,))
    result = c.fetchone()
    conn.close()
    
    if not result:
        await query.answer("❌ Keyword not found", show_alert=True)
        return
    
    msg = (
        f"💬 **Edit Response Message**\n\n"
        f"Current response:\n{result['response_text'][:200]}{'...' if len(result['response_text']) > 200 else ''}\n\n"
        f"Type the new response message:"
    )
    
    context.user_data['state'] = 'awaiting_scout_edit_response'
    context.user_data['edit_keyword_id'] = keyword_id
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"scout_edit_keyword|{keyword_id}")]]
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Already up to date")
        else:
            logger.error(f"Error starting response edit: {e}")
            raise


async def handle_scout_edit_kw_match(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Change keyword match type"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    keyword_id = int(params[0]) if params else None
    if not keyword_id:
        await query.answer("❌ Invalid keyword ID", show_alert=True)
        return
    
    # Get current match type
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT match_type FROM scout_keywords WHERE id = %s", (keyword_id,))
    result = c.fetchone()
    conn.close()
    
    if not result:
        await query.answer("❌ Keyword not found", show_alert=True)
        return
    
    current = result['match_type']
    
    msg = (
        f"🎯 **Change Match Type**\n\n"
        f"Current: **{current}**\n\n"
        f"**Match Types:**\n"
        f"• **contains** - Matches if keyword appears anywhere\n"
        f"• **exact** - Matches only exact phrase\n"
        f"• **starts_with** - Matches if message starts with keyword\n"
        f"• **regex** - Advanced pattern matching\n\n"
        f"Select new match type:"
    )
    
    keyboard = [
        [InlineKeyboardButton(f"{'✅' if current == 'contains' else '◻️'} Contains", 
                             callback_data=f"scout_set_match|{keyword_id}|contains")],
        [InlineKeyboardButton(f"{'✅' if current == 'exact' else '◻️'} Exact", 
                             callback_data=f"scout_set_match|{keyword_id}|exact")],
        [InlineKeyboardButton(f"{'✅' if current == 'starts_with' else '◻️'} Starts With", 
                             callback_data=f"scout_set_match|{keyword_id}|starts_with")],
        [InlineKeyboardButton(f"{'✅' if current == 'regex' else '◻️'} Regex", 
                             callback_data=f"scout_set_match|{keyword_id}|regex")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"scout_edit_keyword|{keyword_id}")]
    ]
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Already up to date")
        else:
            logger.error(f"Error showing match type options: {e}")
            raise


async def handle_scout_set_match(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set new match type for keyword"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("❌ Invalid parameters", show_alert=True)
        return
    
    keyword_id = int(params[0])
    new_match_type = params[1]
    
    # Update match type
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE scout_keywords SET match_type = %s WHERE id = %s", (new_match_type, keyword_id))
    conn.commit()
    conn.close()
    
    await query.answer(f"✅ Match type changed to {new_match_type}", show_alert=False)
    
    # Go back to edit menu
    await handle_scout_edit_keyword(update, context, [str(keyword_id)])


async def handle_scout_edit_kw_delay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start editing keyword response delay"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    keyword_id = int(params[0]) if params else None
    if not keyword_id:
        await query.answer("❌ Invalid keyword ID", show_alert=True)
        return
    
    # Get current delay
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT response_delay FROM scout_keywords WHERE id = %s", (keyword_id,))
    result = c.fetchone()
    conn.close()
    
    if not result:
        await query.answer("❌ Keyword not found", show_alert=True)
        return
    
    msg = (
        f"⏱️ **Edit Response Delay**\n\n"
        f"Current delay: **{result['response_delay']} seconds**\n\n"
        f"Enter new delay in seconds (1-60):\n"
        f"_Recommended: 3-10 seconds to look more human_"
    )
    
    context.user_data['state'] = 'awaiting_scout_edit_delay'
    context.user_data['edit_keyword_id'] = keyword_id
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"scout_edit_keyword|{keyword_id}")]]
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Already up to date")
        else:
            logger.error(f"Error starting delay edit: {e}")
            raise


# Message handlers for edit operations
async def handle_scout_edit_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle messages during keyword editing"""
    user_id = update.effective_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        return
    
    state = context.user_data.get('state')
    keyword_id = context.user_data.get('edit_keyword_id')
    text = update.message.text.strip()
    
    if not state or not keyword_id:
        return
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        if state == 'awaiting_scout_edit_keyword_text':
            # Update keyword text
            c.execute("UPDATE scout_keywords SET keyword = %s WHERE id = %s", (text, keyword_id))
            conn.commit()
            
            msg = f"✅ **Keyword updated to:** `{text}`"
            keyboard = [[InlineKeyboardButton("⬅️ Back to Edit Menu", callback_data=f"scout_edit_keyword|{keyword_id}")]]
            
            await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            
        elif state == 'awaiting_scout_edit_response':
            # Update response text
            c.execute("UPDATE scout_keywords SET response_text = %s WHERE id = %s", (text, keyword_id))
            conn.commit()
            
            msg = f"✅ **Response updated!**\n\nNew response:\n{text[:200]}{'...' if len(text) > 200 else ''}"
            keyboard = [[InlineKeyboardButton("⬅️ Back to Edit Menu", callback_data=f"scout_edit_keyword|{keyword_id}")]]
            
            await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            
        elif state == 'awaiting_scout_edit_delay':
            # Update delay
            try:
                delay = int(text)
                if delay < 1 or delay > 60:
                    await update.message.reply_text("❌ Delay must be between 1 and 60 seconds. Please try again.")
                    return
                
                c.execute("UPDATE scout_keywords SET response_delay = %s WHERE id = %s", (delay, keyword_id))
                conn.commit()
                
                msg = f"✅ **Delay updated to {delay} seconds!**"
                keyboard = [[InlineKeyboardButton("⬅️ Back to Edit Menu", callback_data=f"scout_edit_keyword|{keyword_id}")]]
                
                await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                
            except ValueError:
                await update.message.reply_text("❌ Please enter a valid number between 1 and 60.")
                return
        
        # Clear state
        context.user_data.pop('state', None)
        context.user_data.pop('edit_keyword_id', None)
        
    except Exception as e:
        logger.error(f"Error updating keyword: {e}")
        await update.message.reply_text(f"❌ Error updating keyword: {str(e)}")
    finally:
        conn.close()


# ==================== USERBOT CONFIGURATION ====================

async def handle_scout_userbots(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Configure scout mode for userbots"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    # Get all userbots
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        SELECT id, name, is_connected, scout_mode_enabled
        FROM userbots
        ORDER BY id
    """)
    userbots = c.fetchall()
    conn.close()
    
    msg = "🤖 **Configure Scout Userbots**\n\n"
    
    if not userbots:
        msg += "No userbots available. Add userbots first in Userbot Control Panel."
    else:
        msg += "Enable scout mode for userbots that should monitor groups:\n\n"
        for ub in userbots:
            status = "✅" if ub['scout_mode_enabled'] else "❌"
            connected = "🟢" if ub['is_connected'] else "🔴"
            msg += f"{status} {connected} **{ub['name']}**\n"
    
    keyboard = []
    for ub in userbots:
        action = "disable" if ub['scout_mode_enabled'] else "enable"
        toggle_label = f"{'✅' if ub['scout_mode_enabled'] else '❌'} {ub['name']}"
        manage_label = "⚙️ Manage"
        
        # Add two buttons per userbot: toggle scout mode + manage userbot
        keyboard.append([
            InlineKeyboardButton(toggle_label, callback_data=f"scout_toggle_bot|{ub['id']}"),
            InlineKeyboardButton(manage_label, callback_data=f"userbot_manage:{ub['id']}")
        ])
    
    keyboard.append([InlineKeyboardButton("➕ Add New Userbot", callback_data="userbot_add_new")])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="scout_menu")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


async def handle_scout_toggle_bot(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle scout mode for a userbot"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    userbot_id = int(params[0]) if params else None
    
    if not userbot_id:
        await query.answer("Error: Invalid userbot ID", show_alert=True)
        return
    
    # Get current status
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT scout_mode_enabled FROM userbots WHERE id = %s", (userbot_id,))
    result = c.fetchone()
    conn.close()
    
    if not result:
        await query.answer("Error: Userbot not found", show_alert=True)
        return
    
    new_status = not result['scout_mode_enabled']
    toggle_scout_mode(userbot_id, new_status)
    
    await query.answer(f"✅ Scout mode {'enabled' if new_status else 'disabled'}", show_alert=True)
    
    # Refresh the list
    await handle_scout_userbots(update, context)


# ==================== TRIGGERS LOG ====================

async def handle_scout_triggers(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """View scout triggers log"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    page = int(params[0]) if params else 0
    per_page = 15
    offset = page * per_page
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Get total count
    c.execute("SELECT COUNT(*) as count FROM scout_triggers")
    total = c.fetchone()['count']
    
    # Get triggers for this page
    c.execute("""
        SELECT st.*, sk.keyword, ub.name as userbot_name
        FROM scout_triggers st
        LEFT JOIN scout_keywords sk ON st.keyword_id = sk.id
        LEFT JOIN userbots ub ON st.userbot_id = ub.id
        ORDER BY st.triggered_at DESC
        LIMIT %s OFFSET %s
    """, (per_page, offset))
    triggers = c.fetchall()
    conn.close()
    
    total_pages = (total + per_page - 1) // per_page
    
    msg = f"📊 **Scout Triggers Log** (Page {page + 1}/{max(total_pages, 1)})\n\n"
    
    if not triggers:
        msg += "No triggers logged yet."
    else:
        for t in triggers:
            status = "✅" if t['response_sent'] else "❌"
            chat_name = t['chat_title'] or f"Chat {t['chat_id']}"
            user_display = f"@{t['user_username']}" if t['user_username'] else f"User {t['user_id']}"
            
            msg += f"{status} **{t['keyword']}** by {t['userbot_name']}\n"
            msg += f"   {chat_name} | {user_display}\n"
            msg += f"   {t['triggered_at'].strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    keyboard = []
    
    # Pagination
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"scout_triggers|{page-1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"scout_triggers|{page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([
        InlineKeyboardButton("🔄 Refresh", callback_data=f"scout_triggers|{page}"),
        InlineKeyboardButton("⬅️ Back", callback_data="scout_menu")
    ])
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Already up to date")
        else:
            raise


# ==================== HELPER FUNCTIONS ====================

async def handle_scout_quick_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show quick start guide for scout system"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    msg = (
        "📖 **Scout System Quick Start Guide**\n\n"
        "**What is Scout Mode?**\n"
        "Scout userbots monitor group chats and automatically reply when specific keywords are detected.\n\n"
        "**Setup Steps:**\n\n"
        "1️⃣ **Add a Userbot**\n"
        "   Go to: Marketing > Userbot Control > Add Userbot\n"
        "   Enter API credentials and connect\n\n"
        "2️⃣ **Join Groups**\n"
        "   Make sure your userbot is a member of target groups\n\n"
        "3️⃣ **Add Keywords**\n"
        "   Click 'Manage Keywords' and add keywords to detect\n"
        "   Example: 'discount code', 'steam games', 'shop'\n\n"
        "4️⃣ **Enable Scout Mode**\n"
        "   Click 'Configure Userbots' and enable scout for your userbot\n\n"
        "5️⃣ **Test It**\n"
        "   Send a message with your keyword in a group\n"
        "   The userbot should reply automatically!\n\n"
        "**Match Types:**\n"
        "• **Contains:** Matches if keyword appears anywhere\n"
        "• **Exact:** Matches only exact phrase\n"
        "• **Starts With:** Matches if message starts with keyword\n"
        "• **Regex:** Advanced pattern matching\n\n"
        "**Tips:**\n"
        "✅ Use natural keywords people actually type\n"
        "✅ Add delay (3+ seconds) to look more human\n"
        "✅ Keep responses friendly and helpful\n"
        "⚠️ Don't spam - use scout mode responsibly!"
    )
    
    keyboard = [
        [InlineKeyboardButton("🤖 Go to Userbot Control", callback_data="userbot_control")],
        [InlineKeyboardButton("🔑 Add Keywords", callback_data="scout_keywords|0")],
        [InlineKeyboardButton("⬅️ Back to Scout Menu", callback_data="scout_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


async def handle_scout_bulk_enable(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Enable all keywords"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE scout_keywords SET is_active = TRUE")
    affected = c.rowcount
    conn.commit()
    conn.close()
    
    await query.answer(f"✅ Enabled {affected} keyword(s)", show_alert=True)
    await handle_scout_keywords(update, context, ['0'])


async def handle_scout_bulk_disable(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Disable all keywords"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE scout_keywords SET is_active = FALSE")
    affected = c.rowcount
    conn.commit()
    conn.close()
    
    await query.answer(f"✅ Disabled {affected} keyword(s)", show_alert=True)
    await handle_scout_keywords(update, context, ['0'])


async def handle_scout_test_system(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Test scout system configuration and status"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer("🧪 Testing scout system...")
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Get detailed stats
    c.execute("SELECT COUNT(*) as count FROM scout_keywords WHERE is_active = TRUE")
    active_keywords_count = c.fetchone()['count']
    
    c.execute("SELECT COUNT(*) as count FROM scout_keywords")
    total_keywords = c.fetchone()['count']
    
    c.execute("SELECT COUNT(*) as count FROM userbots WHERE scout_mode_enabled = TRUE")
    scout_enabled_count = c.fetchone()['count']
    
    c.execute("SELECT COUNT(*) as count FROM userbots WHERE scout_mode_enabled = TRUE AND is_connected = TRUE")
    scout_connected_count = c.fetchone()['count']
    
    # Get list of keywords
    c.execute("""
        SELECT keyword, match_type, is_active 
        FROM scout_keywords 
        ORDER BY is_active DESC, id DESC 
        LIMIT 10
    """)
    keywords = c.fetchall()
    
    # Get list of userbots
    c.execute("""
        SELECT name, scout_mode_enabled, is_connected 
        FROM userbots 
        ORDER BY scout_mode_enabled DESC, is_connected DESC
    """)
    userbots = c.fetchall()
    
    conn.close()
    
    # Build diagnostic message
    msg = "🧪 **Scout System Diagnostics**\n\n"
    
    # Overall status
    if scout_connected_count > 0 and active_keywords_count > 0:
        msg += "✅ **Status:** System is operational\n\n"
    elif scout_enabled_count == 0:
        msg += "⚠️ **Status:** No userbots have scout mode enabled\n\n"
    elif scout_connected_count == 0:
        msg += "⚠️ **Status:** Scout userbots are not connected\n\n"
    elif active_keywords_count == 0:
        msg += "⚠️ **Status:** No active keywords configured\n\n"
    else:
        msg += "❌ **Status:** System has issues\n\n"
    
    # Keywords
    msg += f"📊 **Keywords:** {active_keywords_count} active / {total_keywords} total\n"
    if keywords:
        msg += "```\n"
        for kw in keywords[:5]:
            status = "✅" if kw['is_active'] else "❌"
            msg += f"{status} {kw['keyword']} ({kw['match_type']})\n"
        if len(keywords) > 5:
            msg += f"... and {len(keywords) - 5} more\n"
        msg += "```\n"
    msg += "\n"
    
    # Userbots
    msg += f"🤖 **Userbots:** {scout_connected_count} active / {scout_enabled_count} enabled\n"
    if userbots:
        msg += "```\n"
        for ub in userbots:
            scout = "🔍" if ub['scout_mode_enabled'] else "  "
            conn_status = "🟢" if ub['is_connected'] else "🔴"
            msg += f"{scout} {conn_status} {ub['name']}\n"
        msg += "```\n"
    msg += "\n"
    
    # Troubleshooting tips
    if scout_connected_count == 0:
        msg += "**⚠️ Issue:** Scout userbots not connected\n"
        msg += "**Solution:** Restart the bot or check userbot credentials\n\n"
    
    if active_keywords_count == 0:
        msg += "**⚠️ Issue:** No active keywords\n"
        msg += "**Solution:** Add keywords via 'Manage Keywords'\n\n"
    
    if scout_connected_count > 0 and active_keywords_count > 0:
        msg += "**✅ System Ready!**\n"
        msg += "**Test:** Type one of your keywords in a group where your scout userbot is a member.\n"
        msg += "**Expected:** Userbot will reply after 3 seconds\n\n"
        msg += "**Debug:** Check server logs for messages like:\n"
        msg += "`🔍 Scout checking message...`\n"
        msg += "`✅ KEYWORD MATCHED!`\n"
    
    keyboard = [[InlineKeyboardButton("⬅️ Back to Scout Menu", callback_data="scout_menu")]]
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Already showing test results")
        else:
            logger.error(f"Error showing test results: {e}")
            raise

