"""
Running Ads Admin Handlers
Allows admins to configure animated running text for buttons
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from admin import is_primary_admin
from marquee_text_system import (
    get_marquee_settings,
    update_marquee_text,
    update_marquee_enabled,
    update_marquee_speed,
    get_current_marquee_frame
)

logger = logging.getLogger(__name__)

async def handle_admin_marquee_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main running ads settings menu"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    settings = get_marquee_settings()
    
    if not settings:
        msg = "❌ Running Ads system not initialized"
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    msg = "📢 RUNNING ADS MANAGEMENT\n\n"
    msg += f"Current Button Text:\n{settings['text']}\n\n"
    msg += f"Status: {'✅ ENABLED' if settings['enabled'] else '❌ DISABLED'}\n\n"
    msg += "This text will appear on the Running Ads button.\n"
    msg += "The button is static (no animation, no action when clicked).\n\n"
    msg += "Add the button via:\n"
    msg += "Bot UI Management → UI Theme Designer → Edit Bot Look"
    
    keyboard = [
        [InlineKeyboardButton("✏️ Change Button Text", callback_data="admin_marquee_change_text")],
        [
            InlineKeyboardButton(
                "✅ Disable Button" if settings['enabled'] else "▶️ Enable Button",
                callback_data="admin_marquee_toggle"
            )
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_admin_marquee_change_text(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Prompt admin to enter new button text"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    msg = "✏️ CHANGE BUTTON TEXT\n\n"
    msg += "Send me the text you want to display on the Running Ads button.\n\n"
    msg += "Tips:\n"
    msg += "• Use emojis for visual appeal 🎉\n"
    msg += "• Keep it catchy and promotional\n"
    msg += "• This will be static text (no animation)\n\n"
    msg += "Examples:\n"
    msg += "🔥 HOT DEALS - 50% OFF!\n"
    msg += "💎 NEW PRODUCTS AVAILABLE!\n"
    msg += "⚡ LIMITED TIME OFFER!"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="admin_marquee_settings")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # Set state for text input
    context.user_data['awaiting_marquee_text'] = True

async def handle_marquee_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process new button text input"""
    user_id = update.message.from_user.id
    
    if not is_primary_admin(user_id):
        return
    
    if not context.user_data.get('awaiting_marquee_text'):
        return
    
    new_text = update.message.text.strip()
    
    if not new_text:
        await update.message.reply_text("❌ Text cannot be empty!")
        return
    
    if len(new_text) > 200:
        await update.message.reply_text("❌ Text too long! Max 200 characters.")
        return
    
    # Update text
    success = update_marquee_text(new_text)
    
    if success:
        msg = f"✅ BUTTON TEXT UPDATED!\n\n"
        msg += f"New button text:\n{new_text}\n\n"
        msg += "The Running Ads button will now display this text.\n"
        msg += "Users can see it in any menu where you've added the button!"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Settings", callback_data="admin_marquee_settings")]]
        
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text("❌ Failed to update text. Please try again.")
    
    # Clear state
    context.user_data['awaiting_marquee_text'] = False

async def handle_admin_marquee_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle running ads button on/off"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    settings = get_marquee_settings()
    
    if not settings:
        await query.answer("❌ Running Ads not initialized", show_alert=True)
        return
    
    # Toggle
    new_state = not settings['enabled']
    success = update_marquee_enabled(new_state)
    
    if success:
        await query.answer(
            f"✅ Running Ads button {'enabled' if new_state else 'disabled'}!",
            show_alert=True
        )
    else:
        await query.answer("❌ Failed to update", show_alert=True)
    
    # Refresh menu
    await handle_admin_marquee_settings(update, context)

async def handle_admin_marquee_speed(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show speed selection menu"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    settings = get_marquee_settings()
    current_speed = settings['speed'] if settings else 'medium'
    
    msg = "⚡ MARQUEE SPEED\n\n"
    msg += "Select animation speed:\n\n"
    msg += f"{'✅' if current_speed == 'slow' else '⚪'} SLOW - 1.0s per character\n"
    msg += f"{'✅' if current_speed == 'medium' else '⚪'} MEDIUM - 0.5s per character\n"
    msg += f"{'✅' if current_speed == 'fast' else '⚪'} FAST - 0.2s per character\n\n"
    msg += "Faster = more eye-catching\n"
    msg += "Slower = easier to read"
    
    keyboard = [
        [InlineKeyboardButton("🐌 Slow", callback_data="admin_marquee_set_speed|slow")],
        [InlineKeyboardButton("🚶 Medium", callback_data="admin_marquee_set_speed|medium")],
        [InlineKeyboardButton("🚀 Fast", callback_data="admin_marquee_set_speed|fast")],
        [InlineKeyboardButton("⬅️ Back", callback_data="admin_marquee_settings")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_admin_marquee_set_speed(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set marquee speed"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 1:
        await query.answer("Invalid speed", show_alert=True)
        return
    
    speed = params[0]
    
    success = update_marquee_speed(speed)
    
    if success:
        await query.answer(f"✅ Speed set to {speed.upper()}!", show_alert=True)
    else:
        await query.answer("❌ Failed to update speed", show_alert=True)
    
    # Refresh menu
    await handle_admin_marquee_speed(update, context)

async def handle_admin_marquee_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show animated preview of running ads"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    settings = get_marquee_settings()
    
    if not settings:
        await query.answer("❌ Running Ads not initialized", show_alert=True)
        return
    
    msg = "👁️ RUNNING ADS PREVIEW\n\n"
    msg += "Animation frames (how it scrolls):\n\n"
    
    # Show 5 frames - CLEAN, no symbols
    for i in range(5):
        frame = get_current_marquee_frame(i * 3)
        msg += f"Frame {i+1}: {frame}\n"
    
    msg += "\nThis text will scroll smoothly on the button!"
    
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="admin_marquee_settings")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

