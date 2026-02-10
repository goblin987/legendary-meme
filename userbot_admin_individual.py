# Individual userbot management handlers
# This file contains handlers for managing individual userbots in the multi-userbot system

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from utils import is_primary_admin

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

async def handle_userbot_manage(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show management options for a specific userbot"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    # Extract userbot ID from callback data
    if params:
        userbot_id = params[0]
    else:
        # Parse from callback_data (format: "userbot_manage:123")
        try:
            userbot_id = int(query.data.split(':')[1])
        except:
            await query.answer("❌ Invalid userbot ID", show_alert=True)
            return
    
    # Get userbot details from database
    from userbot_database import get_db_connection
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT id, name, api_id, api_hash, phone_number, session_string,
                   is_enabled, is_connected, status_message, last_connected_at,
                   priority, max_deliveries_per_hour
            FROM userbots
            WHERE id = %s
        """, (userbot_id,))
        
        userbot = c.fetchone()
        
        if not userbot:
            await query.answer("❌ Userbot not found", show_alert=True)
            return
    finally:
        conn.close()
    
    # Build status message
    import time
    update_time = time.strftime("%H:%M:%S")
    
    msg = f"⚙️ <b>Manage Userbot</b> <i>(Updated: {update_time})</i>\n\n"
    msg += f"<b>Name:</b> {userbot['name']}\n"
    msg += f"<b>Phone:</b> {userbot['phone_number']}\n"
    msg += f"<b>API ID:</b> <code>{userbot['api_id']}</code>\n\n"
    
    # Status
    enabled = userbot['is_enabled']
    connected = userbot['is_connected']
    has_session = bool(userbot['session_string'])
    
    if enabled and connected and has_session:
        msg += "🟢 <b>Status:</b> Active & Connected\n"
    elif enabled and has_session:
        msg += "🟡 <b>Status:</b> Enabled (not connected)\n"
    elif not enabled:
        msg += "🔴 <b>Status:</b> Disabled\n"
    elif not has_session:
        msg += "⚪ <b>Status:</b> No session (needs auth)\n"
    
    msg += f"<b>Priority:</b> {userbot['priority']}\n"
    msg += f"<b>Max Deliveries/Hour:</b> {userbot['max_deliveries_per_hour']}\n"
    
    if userbot['last_connected_at']:
        msg += f"<b>Last Connected:</b> {userbot['last_connected_at'].strftime('%Y-%m-%d %H:%M UTC')}\n"
    
    # Keyboard
    keyboard = []
    
    # Enable/Disable toggle
    if enabled:
        keyboard.append([InlineKeyboardButton("⏸️ Disable", callback_data=f"userbot_toggle_enable:{userbot_id}")])
    else:
        keyboard.append([InlineKeyboardButton("▶️ Enable", callback_data=f"userbot_toggle_enable:{userbot_id}")])
    
    # Connect/Disconnect (only if has session)
    if has_session:
        if connected:
            keyboard.append([InlineKeyboardButton("🔌 Disconnect", callback_data=f"userbot_disconnect_single:{userbot_id}")])
        else:
            keyboard.append([InlineKeyboardButton("🔌 Connect", callback_data=f"userbot_connect_single:{userbot_id}")])
    
    keyboard.extend([
        [InlineKeyboardButton("🗑️ Delete Userbot", callback_data=f"userbot_delete_confirm:{userbot_id}")],
        [InlineKeyboardButton("⬅️ Back to List", callback_data="userbot_control")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_toggle_enable_single(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle enable/disable for a single userbot"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if params:
        userbot_id = params[0]
    else:
        try:
            userbot_id = int(query.data.split(':')[1])
        except:
            await query.answer("❌ Invalid userbot ID", show_alert=True)
            return
    
    # Toggle in database
    from userbot_database import get_db_connection
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT is_enabled FROM userbots WHERE id = %s", (userbot_id,))
        result = c.fetchone()
        if not result:
            await query.answer("❌ Userbot not found", show_alert=True)
            return
        
        current_status = result['is_enabled']
        new_status = not current_status
        
        c.execute("UPDATE userbots SET is_enabled = %s WHERE id = %s", (new_status, userbot_id))
        conn.commit()
        
        status_text = "enabled" if new_status else "disabled"
        await query.answer(f"✅ Userbot {status_text}!", show_alert=False)
        
    finally:
        conn.close()
    
    # Refresh the management view
    await handle_userbot_manage(update, context, params=[userbot_id])

async def handle_userbot_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm userbot deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if params:
        userbot_id = params[0]
    else:
        try:
            userbot_id = int(query.data.split(':')[1])
        except:
            await query.answer("❌ Invalid userbot ID", show_alert=True)
            return
    
    # Get userbot name
    from userbot_database import get_db_connection
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT name FROM userbots WHERE id = %s", (userbot_id,))
        result = c.fetchone()
        if not result:
            await query.answer("❌ Userbot not found", show_alert=True)
            return
        
        name = result['name']
    finally:
        conn.close()
    
    msg = f"⚠️ <b>Delete Userbot?</b>\n\n"
    msg += f"Are you sure you want to delete userbot '<b>{name}</b>'?\n\n"
    msg += "⚠️ This action CANNOT be undone!\n\n"
    msg += "The session will be terminated and all data will be removed."
    
    keyboard = [
        [InlineKeyboardButton("❌ Yes, Delete", callback_data=f"userbot_delete_confirmed:{userbot_id}")],
        [InlineKeyboardButton("⬅️ Cancel", callback_data=f"userbot_manage:{userbot_id}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def handle_userbot_delete_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Actually delete the userbot"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if params:
        userbot_id = params[0]
    else:
        try:
            userbot_id = int(query.data.split(':')[1])
        except:
            await query.answer("❌ Invalid userbot ID", show_alert=True)
            return
    
    # Delete from database
    from userbot_database import get_db_connection
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("DELETE FROM userbots WHERE id = %s RETURNING name", (userbot_id,))
        result = c.fetchone()
        
        if not result:
            await query.answer("❌ Userbot not found", show_alert=True)
            return
        
        name = result['name']
        conn.commit()
        
        logger.info(f"✅ Userbot deleted: ID={userbot_id}, Name={name}")
        await query.answer(f"✅ {name} deleted!", show_alert=True)
        
    except Exception as e:
        logger.error(f"Error deleting userbot: {e}")
        conn.rollback()
        await query.answer(f"❌ Error: {str(e)}", show_alert=True)
    finally:
        conn.close()
    
    # Go back to main dashboard
    from userbot_admin import handle_userbot_control
    await handle_userbot_control(update, context)

async def handle_userbot_connect_single(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Connect a single userbot"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if params:
        userbot_id = params[0]
    else:
        try:
            userbot_id = int(query.data.split(':')[1])
        except:
            await query.answer("❌ Invalid userbot ID", show_alert=True)
            return
    
    # Connect the userbot via userbot pool
    from userbot_pool import userbot_pool
    
    try:
        success = await userbot_pool.connect_single_userbot(userbot_id)
        if success:
            await query.answer("✅ Userbot connected!", show_alert=False)
        else:
            await query.answer("❌ Failed to connect userbot", show_alert=True)
    except Exception as e:
        logger.error(f"Error connecting userbot {userbot_id}: {e}")
        await query.answer(f"❌ Error: {str(e)}", show_alert=True)
    
    # Refresh the management view
    await handle_userbot_manage(update, context, params=[userbot_id])

async def handle_userbot_disconnect_single(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Disconnect a single userbot"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not check_userbot_access(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if params:
        userbot_id = params[0]
    else:
        try:
            userbot_id = int(query.data.split(':')[1])
        except:
            await query.answer("❌ Invalid userbot ID", show_alert=True)
            return
    
    # Disconnect the userbot via userbot pool
    from userbot_pool import userbot_pool
    
    try:
        success = await userbot_pool.disconnect_single_userbot(userbot_id)
        if success:
            await query.answer("✅ Userbot disconnected!", show_alert=False)
        else:
            await query.answer("❌ Failed to disconnect userbot", show_alert=True)
    except Exception as e:
        logger.error(f"Error disconnecting userbot {userbot_id}: {e}")
        await query.answer(f"❌ Error: {str(e)}", show_alert=True)
    
    # Refresh the management view
    await handle_userbot_manage(update, context, params=[userbot_id])

