"""
Worker Admin Interface
Handles all admin-side worker management: add, edit, remove, view, and analytics.
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from datetime import datetime, timedelta

# Import worker management functions
from worker_management import (
    add_worker, remove_worker, get_worker_by_user_id, get_worker_by_id,
    get_all_workers, update_worker_permissions, update_worker_locations,
    get_worker_stats, get_all_workers_stats
)

# Import utils
from utils import (
    CITIES, DISTRICTS, is_primary_admin, format_currency, PRODUCT_TYPES
)

logger = logging.getLogger(__name__)

# ============= MAIN WORKERS MENU =============

async def handle_workers_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main workers management menu"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    # Get worker count
    workers = get_all_workers()
    worker_count = len(workers)
    
    msg = "👷 **Worker Management**\n\n"
    msg += f"Active Workers: **{worker_count}**\n\n"
    msg += "Manage your team members and track their performance."
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Worker", callback_data="add_worker_start")],
        [InlineKeyboardButton("👥 View Workers", callback_data="view_workers|0")],
        [InlineKeyboardButton("📊 Worker Analytics", callback_data="worker_analytics_menu")],
        [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

# ============= ADD WORKER FLOW =============

async def handle_add_worker_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Step 1: Prompt for worker username or user ID"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    # Initialize session
    context.user_data['worker_session'] = {
        'step': 'awaiting_username',
        'permissions': [],
        'locations': {}
    }
    
    msg = "👷 **Add New Worker**\n\n"
    msg += "You can add a worker in two ways:\n\n"
    msg += "**Option 1:** Send their Telegram username\n"
    msg += "Example: `@worker_username` or `worker_username`\n\n"
    msg += "**Option 2:** Send their Telegram User ID\n"
    msg += "Example: `123456789`\n\n"
    msg += "💡 **Tip:** If username doesn't work, ask the worker to send /start to your bot, then use their User ID."
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="workers_menu")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def handle_add_worker_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 2: Process username/user_id and ask for permissions"""
    if not is_primary_admin(update.message.from_user.id):
        return
    
    session = context.user_data.get('worker_session', {})
    if session.get('step') != 'awaiting_username':
        return
    
    input_text = update.message.text.strip()
    
    if not input_text:
        await update.message.reply_text("❌ Invalid input. Please try again.")
        return
    
    user_id = None
    username = None
    
    # Check if input is a numeric user ID
    if input_text.isdigit():
        user_id = int(input_text)
        # Try to get username from user_id
        try:
            chat = await context.bot.get_chat(user_id)
            username = chat.username if hasattr(chat, 'username') and chat.username else f"ID_{user_id}"
            logger.info(f"Resolved user ID {user_id} to username: {username}")
        except Exception as e:
            logger.warning(f"Could not get username for user_id {user_id}: {e}")
            username = f"ID_{user_id}"
    else:
        # Input is a username - try multiple methods
        username = input_text
        # Remove @ if present
        if username.startswith('@'):
            username = username[1:]
        
        # Method 1: Try Bot API
        try:
            chat = await context.bot.get_chat(f"@{username}")
            user_id = chat.id
            logger.info(f"Resolved username @{username} to user_id via Bot API: {user_id}")
        except Exception as e:
            logger.warning(f"Bot API failed for @{username}: {e}")
            
            # Method 2: Search in database (users who have used the bot)
            try:
                from utils import get_db_connection
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT user_id, username FROM users WHERE LOWER(username) = LOWER(%s) LIMIT 1", (username,))
                result = c.fetchone()
                conn.close()
                
                if result:
                    user_id = result['user_id']
                    username = result['username'] or username
                    logger.info(f"✅ Found @{username} in database with user_id: {user_id}")
                else:
                    logger.error(f"Username @{username} not found in database either")
                    
                    # Show helpful error with recent users from database
                    try:
                        conn = get_db_connection()
                        c = conn.cursor()
                        c.execute("""
                            SELECT user_id, username FROM users 
                            WHERE username IS NOT NULL AND username != ''
                            ORDER BY user_id DESC LIMIT 5
                        """)
                        recent_users = c.fetchall()
                        conn.close()
                    except Exception:
                        recent_users = []
                    
                    msg = f"❌ Could not find user @{username}\n\n"
                    
                    if recent_users:
                        msg += "**Recent users who used the bot:**\n"
                        for u in recent_users:
                            msg += f"• @{u['username']} (ID: `{u['user_id']}`)\n"
                        msg += "\n💡 Copy and send the User ID number"
                    else:
                        msg += "**Solutions:**\n"
                        msg += "1. Ask worker to send /start\n"
                        msg += "2. Ask worker to check their User ID in Profile\n"
                        msg += "3. Send the numeric User ID directly"
                    
                    await update.message.reply_text(
                        msg,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="workers_menu")]]),
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
            except Exception as db_error:
                logger.error(f"Database search failed: {db_error}")
                await update.message.reply_text(
                    f"❌ Could not find user @{username}\n\n"
                    "**Send their User ID instead:**\n"
                    "Ask the worker to:\n"
                    "1. Send /start to this bot\n"
                    "2. Tap Profile\n"
                    "3. Send you the User ID number shown there",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="workers_menu")]])
                )
                return
    
    # Check if worker already exists
    existing_worker = get_worker_by_user_id(user_id)
    if existing_worker:
        display_name = f"@{username}" if not username.startswith("ID_") else f"User ID: {user_id}"
        await update.message.reply_text(
            f"❌ Worker {display_name} is already registered!\n\n"
            "Use 'View Workers' to manage existing workers.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="workers_menu")]])
        )
        context.user_data.pop('worker_session', None)
        return
    
    # Store in session
    session['username'] = username
    session['user_id'] = user_id
    session['step'] = 'selecting_permissions'
    context.user_data['worker_session'] = session
    
    # Show permissions selection
    await show_permissions_selection(update, context)

async def show_permissions_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show permissions selection interface"""
    session = context.user_data.get('worker_session', {})
    permissions = session.get('permissions', [])
    username = session.get('username', 'Unknown')
    
    msg = f"👷 **Add Worker: @{username}**\n\n"
    msg += "Select permissions for this worker:\n\n"
    
    # Show current selections with checkboxes
    has_add_products = "add_products" in permissions
    has_check_stock = "check_stock" in permissions
    has_marketing = "marketing" in permissions
    
    msg += f"{'✅' if has_add_products else '☐'} Add Products\n"
    msg += f"{'✅' if has_check_stock else '☐'} Check Stock\n"
    msg += f"{'✅' if has_marketing else '☐'} Marketing Tools\n"
    
    keyboard = [
        [InlineKeyboardButton(
            f"{'✅' if has_add_products else '☐'} Add Products",
            callback_data="worker_toggle_perm|add_products"
        )],
        [InlineKeyboardButton(
            f"{'✅' if has_check_stock else '☐'} Check Stock",
            callback_data="worker_toggle_perm|check_stock"
        )],
        [InlineKeyboardButton(
            f"{'✅' if has_marketing else '☐'} Marketing",
            callback_data="worker_toggle_perm|marketing"
        )],
        [InlineKeyboardButton("✅ Continue", callback_data="worker_confirm_permissions")],
        [InlineKeyboardButton("❌ Cancel", callback_data="workers_menu")]
    ]
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
        )

async def handle_worker_toggle_permission(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle a permission on/off"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    if not params or len(params) < 1:
        return await query.answer("Invalid request", show_alert=True)
    
    permission = params[0]
    session = context.user_data.get('worker_session', {})
    permissions = session.get('permissions', [])
    
    # Toggle permission
    if permission in permissions:
        permissions.remove(permission)
    else:
        permissions.append(permission)
    
    session['permissions'] = permissions
    context.user_data['worker_session'] = session
    
    # Refresh display
    await show_permissions_selection(update, context)

async def handle_worker_confirm_permissions(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm permissions and proceed to location selection or finalize"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    session = context.user_data.get('worker_session', {})
    permissions = session.get('permissions', [])
    
    if not permissions:
        await query.answer("⚠️ Please select at least one permission", show_alert=True)
        return
    
    # If "add_products" is selected, show location selector
    if "add_products" in permissions:
        session['step'] = 'selecting_locations'
        context.user_data['worker_session'] = session
        await show_city_selection(update, context)
    else:
        # No location restrictions needed, finalize
        await finalize_add_worker(update, context)

async def show_city_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show city selection for location restrictions"""
    session = context.user_data.get('worker_session', {})
    username = session.get('username', 'Unknown')
    selected_cities = list(session.get('locations', {}).keys())
    
    msg = f"👷 **Add Worker: @{username}**\n\n"
    msg += "Select cities this worker can add products to:\n\n"
    
    if selected_cities:
        msg += "**Selected:**\n"
        for city_id in selected_cities:
            city_name = CITIES.get(city_id, city_id)
            msg += f"• {city_name}\n"
        msg += "\n"
    
    keyboard = []
    for city_id, city_name in CITIES.items():
        is_selected = city_id in selected_cities
        keyboard.append([InlineKeyboardButton(
            f"{'✅' if is_selected else '☐'} {city_name}",
            callback_data=f"worker_toggle_city|{city_id}"
        )])
    
    keyboard.append([InlineKeyboardButton("➡️ Configure Districts", callback_data="worker_configure_districts")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="workers_menu")])
    
    await update.callback_query.edit_message_text(
        msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
    )

async def handle_worker_toggle_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle city selection"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    if not params or len(params) < 1:
        return await query.answer("Invalid request", show_alert=True)
    
    city_id = params[0]
    session = context.user_data.get('worker_session', {})
    locations = session.get('locations', {})
    
    # Toggle city
    if city_id in locations:
        del locations[city_id]
    else:
        locations[city_id] = "all"  # Default to all districts
    
    session['locations'] = locations
    context.user_data['worker_session'] = session
    
    # Refresh display
    await show_city_selection(update, context)

async def handle_worker_configure_districts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show district configuration for selected cities"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    session = context.user_data.get('worker_session', {})
    locations = session.get('locations', {})
    
    if not locations:
        await query.answer("⚠️ Please select at least one city first", show_alert=True)
        return
    
    # Show first city for district selection
    city_ids = list(locations.keys())
    session['district_config_cities'] = city_ids
    session['district_config_index'] = 0
    context.user_data['worker_session'] = session
    
    await show_district_selection_for_city(update, context)

async def show_district_selection_for_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show district selection for current city"""
    session = context.user_data.get('worker_session', {})
    username = session.get('username', 'Unknown')
    cities = session.get('district_config_cities', [])
    index = session.get('district_config_index', 0)
    
    if index >= len(cities):
        # Done configuring districts
        await finalize_add_worker(update, context)
        return
    
    city_id = cities[index]
    city_name = CITIES.get(city_id, city_id)
    locations = session.get('locations', {})
    current_districts = locations.get(city_id, "all")
    
    msg = f"👷 **Add Worker: @{username}**\n\n"
    msg += f"Configure districts for **{city_name}**:\n\n"
    
    keyboard = []
    
    # "All Districts" option
    all_selected = current_districts == "all"
    keyboard.append([InlineKeyboardButton(
        f"{'✅' if all_selected else '☐'} All Districts",
        callback_data=f"worker_district_all|{city_id}"
    )])
    
    # Individual districts
    if not all_selected:
        if not isinstance(current_districts, list):
            current_districts = []
        
        districts = DISTRICTS.get(city_id, {})
        for dist_id, dist_name in districts.items():
            is_selected = dist_id in current_districts
            keyboard.append([InlineKeyboardButton(
                f"{'✅' if is_selected else '☐'} {dist_name}",
                callback_data=f"worker_toggle_district|{city_id}|{dist_id}"
            )])
    
    keyboard.append([InlineKeyboardButton("➡️ Next City" if index < len(cities) - 1 else "✅ Finish", 
                                          callback_data="worker_next_city")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="workers_menu")])
    
    try:
        await update.callback_query.edit_message_text(
            msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        if "message is not modified" in str(e).lower():
            await update.callback_query.answer()
        else:
            raise

async def handle_worker_district_all(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set all districts for a city"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    if not params or len(params) < 1:
        return
    
    city_id = params[0]
    session = context.user_data.get('worker_session', {})
    locations = session.get('locations', {})
    
    locations[city_id] = "all"
    session['locations'] = locations
    context.user_data['worker_session'] = session
    
    await show_district_selection_for_city(update, context)

async def handle_worker_toggle_district(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle district selection"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    if not params or len(params) < 2:
        return
    
    city_id, dist_id = params[0], params[1]
    session = context.user_data.get('worker_session', {})
    locations = session.get('locations', {})
    
    current = locations.get(city_id, "all")
    if current == "all":
        current = []
    
    if not isinstance(current, list):
        current = []
    
    # Toggle district
    if dist_id in current:
        current.remove(dist_id)
    else:
        current.append(dist_id)
    
    locations[city_id] = current if current else "all"
    session['locations'] = locations
    context.user_data['worker_session'] = session
    
    await show_district_selection_for_city(update, context)

async def handle_worker_next_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Move to next city or finish"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    session = context.user_data.get('worker_session', {})
    index = session.get('district_config_index', 0)
    session['district_config_index'] = index + 1
    context.user_data['worker_session'] = session
    
    await show_district_selection_for_city(update, context)

async def finalize_add_worker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Finalize worker addition"""
    session = context.user_data.get('worker_session', {})
    username = session.get('username')
    user_id = session.get('user_id')
    permissions = session.get('permissions', [])
    locations = session.get('locations', {})
    admin_id = update.callback_query.from_user.id if update.callback_query else update.message.from_user.id
    
    # Add worker to database
    worker_id = add_worker(username, user_id, admin_id, permissions, locations)
    
    if worker_id:
        msg = f"✅ **Worker Added Successfully!**\n\n"
        msg += f"**Username:** @{username}\n"
        msg += f"**Permissions:**\n"
        for perm in permissions:
            perm_name = perm.replace('_', ' ').title()
            msg += f"• {perm_name}\n"
        
        if locations:
            msg += f"\n**Allowed Locations:**\n"
            for city_id, districts in locations.items():
                city_name = CITIES.get(city_id, city_id)
                if districts == "all":
                    msg += f"• {city_name}: All districts\n"
                else:
                    dist_names = [DISTRICTS.get(city_id, {}).get(d, d) for d in districts]
                    msg += f"• {city_name}: {', '.join(dist_names)}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Back to Workers", callback_data="workers_menu")]]
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
            )
    else:
        msg = "❌ Failed to add worker. Please try again."
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="workers_menu")]]
        
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # Clear session
    context.user_data.pop('worker_session', None)

# ============= VIEW WORKERS =============

async def handle_view_workers(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """View all workers with pagination"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    page = int(params[0]) if params else 0
    workers = get_all_workers()
    
    if not workers:
        msg = "👷 **Workers**\n\nNo workers registered yet."
        keyboard = [[InlineKeyboardButton("➕ Add Worker", callback_data="add_worker_start")],
                   [InlineKeyboardButton("🔙 Back", callback_data="workers_menu")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        return
    
    # Pagination
    per_page = 5
    total_pages = (len(workers) + per_page - 1) // per_page
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    end = start + per_page
    page_workers = workers[start:end]
    
    msg = f"👷 **Workers** (Page {page + 1}/{total_pages})\n\n"
    
    keyboard = []
    for worker in page_workers:
        username = worker['username'] or f"ID: {worker['user_id']}"
        perms = len(worker['permissions']) if isinstance(worker['permissions'], list) else 0
        keyboard.append([InlineKeyboardButton(
            f"@{username} ({perms} permissions)",
            callback_data=f"view_worker_details|{worker['id']}"
        )])
    
    # Pagination buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Previous", callback_data=f"view_workers|{page-1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"view_workers|{page+1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="workers_menu")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def handle_view_worker_details(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """View detailed information about a specific worker"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    if not params or len(params) < 1:
        return await query.answer("Invalid request", show_alert=True)
    
    worker_id = int(params[0])
    worker = get_worker_by_id(worker_id)
    
    if not worker:
        await query.answer("Worker not found", show_alert=True)
        return
    
    username = worker['username'] or f"ID: {worker['user_id']}"
    permissions = worker['permissions'] if isinstance(worker['permissions'], list) else []
    locations = worker.get('allowed_locations', {}) if isinstance(worker.get('allowed_locations'), dict) else {}
    
    msg = f"👷 **Worker Details**\n\n"
    msg += f"**Username:** @{username}\n"
    msg += f"**User ID:** `{worker['user_id']}`\n"
    msg += f"**Added:** {worker['added_date'].strftime('%Y-%m-%d %H:%M')}\n"
    msg += f"**Status:** {'✅ Active' if worker['is_active'] else '❌ Inactive'}\n\n"
    
    msg += "**Permissions:**\n"
    if permissions:
        for perm in permissions:
            perm_name = perm.replace('_', ' ').title()
            msg += f"• {perm_name}\n"
    else:
        msg += "• None\n"
    
    if locations:
        msg += "\n**Allowed Locations:**\n"
        for city_id, districts in locations.items():
            city_name = CITIES.get(city_id, city_id)
            if districts == "all":
                msg += f"• {city_name}: All districts\n"
            elif isinstance(districts, list):
                dist_names = [DISTRICTS.get(city_id, {}).get(d, d) for d in districts]
                msg += f"• {city_name}: {', '.join(dist_names)}\n"
    
    keyboard = [
        [InlineKeyboardButton("✏️ Edit Permissions", callback_data=f"edit_worker_permissions|{worker_id}")],
        [InlineKeyboardButton("🗺️ Edit Locations", callback_data=f"edit_worker_locations|{worker_id}")],
        [InlineKeyboardButton("📊 View Statistics", callback_data=f"worker_stats_single|{worker_id}")],
        [InlineKeyboardButton("🗑️ Remove Worker", callback_data=f"confirm_remove_worker|{worker_id}")],
        [InlineKeyboardButton("🔙 Back", callback_data="view_workers|0")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

# ============= REMOVE WORKER =============

async def handle_confirm_remove_worker(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm worker removal"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    if not params or len(params) < 1:
        return
    
    worker_id = int(params[0])
    worker = get_worker_by_id(worker_id)
    
    if not worker:
        await query.answer("Worker not found", show_alert=True)
        return
    
    username = worker['username'] or f"ID: {worker['user_id']}"
    
    msg = f"⚠️ **Confirm Worker Removal**\n\n"
    msg += f"Are you sure you want to remove worker @{username}?\n\n"
    msg += "They will no longer have access to worker features."
    
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Remove", callback_data=f"execute_remove_worker|{worker_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"view_worker_details|{worker_id}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def handle_execute_remove_worker(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Execute worker removal"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    if not params or len(params) < 1:
        return
    
    worker_id = int(params[0])
    success = remove_worker(worker_id)
    
    if success:
        msg = "✅ Worker removed successfully!"
    else:
        msg = "❌ Failed to remove worker. Please try again."
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Workers", callback_data="view_workers|0")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

# ============= WORKER ANALYTICS =============

async def handle_worker_analytics_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Worker analytics menu"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    msg = "📊 **Worker Analytics**\n\n"
    msg += "View performance statistics for your workers."
    
    keyboard = [
        [InlineKeyboardButton("👥 All Workers", callback_data="worker_stats_all|30")],
        [InlineKeyboardButton("👤 Select Worker", callback_data="worker_stats_select")],
        [InlineKeyboardButton("🔙 Back", callback_data="workers_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def handle_worker_stats_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Select a worker to view stats"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    workers = get_all_workers()
    if not workers:
        return await query.answer("No workers found.", show_alert=True)
        
    await query.answer()
    
    msg = "📊 **Select Worker for Analytics**\n\n"
    
    keyboard = []
    for worker in workers:
        status = "🟢" if worker['is_active'] else "🔴"
        keyboard.append([InlineKeyboardButton(f"{status} {worker['username']}", callback_data=f"worker_stats_single|{worker['id']}")])
        
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="worker_analytics_menu")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def handle_worker_stats_all(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show statistics for all workers"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    days = int(params[0]) if params else 30
    date_from = datetime.now() - timedelta(days=days)
    date_to = datetime.now()
    
    all_stats = get_all_workers_stats(date_from, date_to)
    
    if not all_stats:
        msg = "📊 **Worker Analytics**\n\nNo data available."
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="worker_analytics_menu")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        return
    
    msg = f"📊 **All Workers Performance**\n"
    msg += f"📅 Last {days} days\n\n"
    
    # Sort by products added
    all_stats.sort(key=lambda x: x.get('total_added', 0), reverse=True)
    
    for stats in all_stats[:10]:  # Show top 10
        worker = stats.get('worker', {})
        username = worker.get('username', f"ID: {worker.get('user_id')}")
        total_added = stats.get('total_added', 0)
        total_sold = stats.get('total_sold', 0)
        revenue = stats.get('revenue', 0)
        
        msg += f"👷 @{username}\n"
        msg += f"├─ Added: {total_added} products\n"
        msg += f"├─ Sold: {total_sold} ({format_conversion_rate(total_sold, total_added)})\n"
        msg += f"└─ Revenue: {format_currency(revenue)}\n\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="worker_analytics_menu")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def handle_worker_stats_single(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show detailed statistics for a single worker"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    if not params or len(params) < 1:
        return
    
    worker_id = int(params[0])
    days = 30
    date_from = datetime.now() - timedelta(days=days)
    date_to = datetime.now()
    
    stats = get_worker_stats(worker_id, date_from, date_to)
    
    if not stats:
        await query.answer("No data available", show_alert=True)
        return
    
    worker = stats.get('worker', {})
    username = worker.get('username', f"ID: {worker.get('user_id')}")
    total_added = stats.get('total_added', 0)
    by_type = stats.get('by_type', {})
    by_location = stats.get('by_location', [])
    total_sold = stats.get('total_sold', 0)
    revenue = stats.get('revenue', 0)
    activity = stats.get('activity', {})
    
    msg = f"📊 **Worker Performance**\n\n"
    msg += f"👷 @{username}\n"
    msg += f"📅 Last {days} days\n\n"
    
    msg += f"📦 **Products Added:** {total_added}\n"
    
    if by_type:
        msg += "\n**By Type:**\n"
        for ptype, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True)[:5]:
            emoji = PRODUCT_TYPES.get(ptype, '📦')
            percentage = (count / total_added * 100) if total_added > 0 else 0
            msg += f"  • {emoji} {ptype}: {count} ({percentage:.0f}%)\n"
    
    if by_location:
        msg += "\n**By Location:**\n"
        for loc in by_location[:5]:
            count = loc['count']
            percentage = (count / total_added * 100) if total_added > 0 else 0
            msg += f"  • {loc['city']}/{loc['district']}: {count} ({percentage:.0f}%)\n"
    
    msg += f"\n💰 **Products Sold:** {total_sold}\n"
    msg += f"└─ Revenue: {format_currency(revenue)}\n"
    msg += f"└─ Conversion: {format_conversion_rate(total_sold, total_added)}\n"
    
    if activity:
        msg += "\n📊 **Activity:**\n"
        for action, count in activity.items():
            action_name = action.replace('_', ' ').title()
            msg += f"  • {action_name}: {count}\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=f"view_worker_details|{worker_id}")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

# ============= HELPERS =============

def format_conversion_rate(sold: int, added: int) -> str:
    """Format conversion rate percentage"""
    if added == 0:
        return "0%"
    rate = (sold / added) * 100
    return f"{rate:.1f}%"

