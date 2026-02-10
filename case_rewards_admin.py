"""
Case Rewards Admin Interface - CS:GO Style
Manage product pools for cases (product types, not individual products)
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from utils import get_db_connection, is_primary_admin
from daily_rewards_system import get_all_cases
from case_rewards_system import (
    get_all_product_types,
    get_case_reward_pool,
    add_product_to_case_pool,
    remove_product_from_case_pool,
    set_case_lose_emoji
)

logger = logging.getLogger(__name__)

# ============================================================================
# PRODUCT POOL MANAGER (NEW SYSTEM)
# ============================================================================

async def handle_admin_product_pool_v2(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """New product pool manager - Step 1: Select case type"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    # Get cases from database
    cases = get_all_cases()
    
    msg = "🎁 PRODUCT POOL MANAGER\n\n"
    msg += "Step 1: Select a case to configure\n\n"
    msg += "Each case can have multiple product types with different win chances.\n"
    msg += "Users win PRODUCTS (not points) or NOTHING.\n\n"
    
    if not cases:
        msg += "❌ No cases created yet.\n\n"
        msg += "Go to 'Manage Cases' to create your first case!"
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_main")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    msg += "Select a case:"
    
    keyboard = []
    
    for case_type, config in cases.items():
        keyboard.append([InlineKeyboardButton(
            f"{config['emoji']} {config['name']}",
            callback_data=f"admin_case_pool|{case_type}"
        )])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_main")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_case_pool(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Step 2: Manage specific case pool"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    cases = get_all_cases()
    config = cases.get(case_type)
    
    if not config:
        await query.answer("Case not found", show_alert=True)
        return
    
    await query.answer()
    
    # Get current reward pool
    rewards = get_case_reward_pool(case_type)
    
    # Get show_percentages setting from database
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT setting_value FROM bot_settings WHERE setting_key = %s', ('show_case_win_percentages',))
    result = c.fetchone()
    show_percentages = result['setting_value'] == 'true' if result else True
    conn.close()
    
    msg = f"{config['emoji']} {config['name'].upper()} - REWARD POOL\n\n"
    msg += f"Cost: {config['cost']} points\n\n"
    msg += f"{'👁️ Percentages Visible to Users' if show_percentages else '🙈 Percentages Hidden from Users'}\n\n"
    
    if rewards:
        msg += "Current Rewards:\n"
        total_chance = 0
        for reward in rewards:
            emoji = reward['reward_emoji'] or '🎁'
            msg += f"{emoji} {reward['product_type_name']} {reward['product_size']}\n"
            msg += f"   Win Chance: {reward['win_chance_percent']}%\n\n"
            total_chance += reward['win_chance_percent']
        
        lose_chance = 100 - total_chance
        msg += f"💸 Lose (Nothing): {lose_chance:.1f}%\n\n"
    else:
        msg += "❌ No rewards configured yet!\n\n"
    
    msg += "What would you like to do?"
    
    # Toggle button for showing percentages
    toggle_text = "🙈 Hide Percentages" if show_percentages else "👁️ Show Percentages"
    
    keyboard = [
        [InlineKeyboardButton(toggle_text, callback_data=f"admin_toggle_show_percentages|{case_type}")],
        [InlineKeyboardButton("➕ Add Product Type", callback_data=f"admin_add_product_to_case|{case_type}")],
        [InlineKeyboardButton("🗑️ Remove Product", callback_data=f"admin_remove_from_case|{case_type}")],
        [InlineKeyboardButton("💸 Set Lose Emoji", callback_data=f"admin_set_lose_emoji|{case_type}")],
    ]
    
    # Add Save button if there are rewards configured
    if rewards:
        keyboard.append([InlineKeyboardButton("💾 Save & Activate Case", callback_data=f"admin_save_case_config|{case_type}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Cases", callback_data="admin_product_pool_v2")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_add_product_to_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Step 3: Select product type to add"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    await query.answer()
    
    # Get all available product types
    product_types = get_all_product_types()
    
    msg = "➕ ADD PRODUCT TO CASE\n\n"
    msg += "Select a product type:\n\n"
    
    if product_types:
        msg += "Available Product Types:\n"
        for pt in product_types[:10]:  # Show first 10
            msg += f"• {pt['name']} {pt['size']} - {pt['min_price']}€ (Stock: {pt['total_available']})\n"
    else:
        msg += "❌ No products available\n"
    
    keyboard = []
    
    # Create buttons for product types (2 per row)
    for i in range(0, min(len(product_types), 10), 2):
        row = []
        for j in range(2):
            if i + j < len(product_types):
                pt = product_types[i + j]
                row.append(InlineKeyboardButton(
                    f"{pt['name']} {pt['size']}",
                    callback_data=f"admin_select_product|{case_type}|{pt['name']}|{pt['size']}"
                ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_case_pool|{case_type}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_select_product(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Step 4: Set win chance for selected product"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 3:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_type = params[0]
    product_type = params[1]
    size = params[2]
    
    await query.answer()
    
    msg = f"➕ ADD: {product_type} {size}\n\n"
    msg += "Select win chance percentage:\n\n"
    msg += "💡 Tips:\n"
    msg += "• Lower % = More rare = More exciting\n"
    msg += "• Total of all products should be < 100%\n"
    msg += "• Remaining % = Lose chance\n\n"
    msg += "Example: If total products = 30%, lose chance = 70%"
    
    # Win chance presets
    chances = [0.5, 1, 2, 5, 10, 15, 20, 25]
    
    keyboard = []
    row = []
    for i, chance in enumerate(chances):
        row.append(InlineKeyboardButton(
            f"{chance}%",
            callback_data=f"admin_set_product_chance|{case_type}|{product_type}|{size}|{chance}"
        ))
        if (i + 1) % 4 == 0:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    # Add custom % input button
    keyboard.append([InlineKeyboardButton("✏️ Enter Custom %", callback_data=f"admin_custom_chance|{case_type}|{product_type}|{size}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_add_product_to_case|{case_type}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_set_product_chance(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Step 5: Set emoji for product"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 4:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_type = params[0]
    product_type = params[1]
    size = params[2]
    chance = float(params[3])
    
    # Store in context for next step
    context.user_data['pending_product'] = {
        'case_type': case_type,
        'product_type': product_type,
        'size': size,
        'chance': chance
    }
    
    await query.answer()
    
    msg = f"🎨 SET EMOJI\n\n"
    msg += f"Product: {product_type} {size}\n"
    msg += f"Win Chance: {chance}%\n\n"
    msg += "Select an emoji for this reward:"
    
    # Emoji picker
    emojis = {
        "Food": ["☕", "🍕", "🍔", "🌮", "🍜", "🍱"],
        "Rewards": ["🎁", "💎", "🏆", "⭐", "💰", "🔥"],
        "Gaming": ["🎮", "🕹️", "👾", "🎯", "🎲", "🃏"],
        "Fun": ["✨", "🎉", "🎊", "🎈", "🎆", "🎇"]
    }
    
    keyboard = []
    
    for category, emoji_list in emojis.items():
        row = []
        for emoji in emoji_list:
            row.append(InlineKeyboardButton(
                emoji,
                callback_data=f"admin_save_product_reward|{emoji}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_select_product|{case_type}|{product_type}|{size}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_save_product_reward(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Step 6: Save product to case pool"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid emoji", show_alert=True)
        return
    
    emoji = params[0]
    
    # Get pending product from context
    pending = context.user_data.get('pending_product')
    if not pending:
        await query.answer("Session expired, please try again", show_alert=True)
        return
    
    # Save to database
    success = add_product_to_case_pool(
        pending['case_type'],
        pending['product_type'],
        pending['size'],
        pending['chance'],
        emoji
    )
    
    if success:
        await query.answer(f"✅ Added {emoji} {pending['product_type']} {pending['size']} ({pending['chance']}%)", show_alert=True)
        # Clear context
        context.user_data.pop('pending_product', None)
        # Return to case pool view
        await handle_admin_case_pool(update, context, [pending['case_type']])
    else:
        await query.answer("❌ Error saving product", show_alert=True)

async def handle_admin_remove_from_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Remove product from case pool"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    await query.answer()
    
    # Get current rewards
    rewards = get_case_reward_pool(case_type)
    
    msg = "🗑️ REMOVE PRODUCT\n\n"
    msg += "Select a product to remove:"
    
    keyboard = []
    
    for reward in rewards:
        emoji = reward['reward_emoji'] or '🎁'
        keyboard.append([InlineKeyboardButton(
            f"{emoji} {reward['product_type_name']} {reward['product_size']} ({reward['win_chance_percent']}%)",
            callback_data=f"admin_confirm_remove|{case_type}|{reward['id']}"
        )])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_case_pool|{case_type}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_confirm_remove(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm removal"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_type = params[0]
    pool_id = int(params[1])
    
    success = remove_product_from_case_pool(pool_id)
    
    if success:
        await query.answer("✅ Product removed", show_alert=True)
        await handle_admin_case_pool(update, context, [case_type])
    else:
        await query.answer("❌ Error removing product", show_alert=True)

async def handle_admin_set_lose_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set lose emoji for case"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    await query.answer()
    
    msg = "💸 SET LOSE EMOJI\n\n"
    msg += "Select an emoji that shows when user wins NOTHING:"
    
    lose_emojis = ["💸", "😢", "💔", "😭", "💨", "👎", "❌", "🚫"]
    
    keyboard = []
    row = []
    for i, emoji in enumerate(lose_emojis):
        row.append(InlineKeyboardButton(
            emoji,
            callback_data=f"admin_save_lose_emoji|{case_type}|{emoji}"
        ))
        if (i + 1) % 4 == 0:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_case_pool|{case_type}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_save_lose_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save lose emoji"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_type = params[0]
    emoji = params[1]
    
    success = set_case_lose_emoji(case_type, emoji, "Better luck next time!")
    
    if success:
        await query.answer(f"✅ Lose emoji set to {emoji}", show_alert=True)
        await handle_admin_case_pool(update, context, [case_type])
    else:
        await query.answer("❌ Error saving emoji", show_alert=True)

async def handle_admin_save_case_config(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Sync case_reward_pools to rewards_config in case_settings"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    await query.answer("Saving configuration...", show_alert=False)
    
    from utils import get_db_connection
    import json
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get reward pool from case_reward_pools table
        c.execute('''
            SELECT win_chance_percent, product_type_name, product_size
            FROM case_reward_pools
            WHERE case_type = %s AND is_active = TRUE
        ''', (case_type,))
        
        rewards_data = c.fetchall()
        
        if not rewards_data:
            await query.answer("❌ No rewards configured!", show_alert=True)
            return
        
        # Build rewards_config dict (outcome_type: percentage)
        rewards_config = {}
        total_chance = 0
        
        for reward in rewards_data:
            # Use product type + size as key for now
            # In the actual opening, we'll map this to 'win_product'
            key = f"win_product_{reward['product_type_name']}_{reward['product_size']}"
            rewards_config[key] = float(reward['win_chance_percent'])
            total_chance += float(reward['win_chance_percent'])
        
        # Add lose chance
        lose_chance = 100 - total_chance
        if lose_chance > 0:
            rewards_config['lose_all'] = lose_chance
        
        # Update case_settings with rewards_config
        c.execute('''
            UPDATE case_settings
            SET rewards_config = %s::jsonb
            WHERE case_type = %s
        ''', (json.dumps(rewards_config), case_type))
        
        conn.commit()
        
        await query.answer(f"✅ Case '{case_type}' saved and activated!", show_alert=True)
        
        # Show success message
        msg = f"✅ CASE CONFIGURATION SAVED!\n\n"
        msg += f"Case: {case_type.upper()}\n"
        msg += f"Total Win Chance: {total_chance:.1f}%\n"
        msg += f"Lose Chance: {lose_chance:.1f}%\n\n"
        msg += f"The case is now ready for users to open!\n\n"
        msg += f"🎮 Users can now:\n"
        msg += f"• See '{case_type}' in case opening menu\n"
        msg += f"• Open the case and win products\n"
        msg += f"• View their stats and leaderboard"
        
        keyboard = [
            [InlineKeyboardButton("⬅️ Back to Pool", callback_data=f"admin_case_pool|{case_type}")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="admin_daily_rewards_main")]
        ]
        
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    finally:
        conn.close()

# ============================================================================
# NEW HANDLERS: TOGGLE PERCENTAGES & CUSTOM %
# ============================================================================

async def handle_admin_toggle_show_percentages(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle showing win percentages to users"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_type = params[0]
    
    # Toggle the setting
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get current value
        c.execute('SELECT setting_value FROM bot_settings WHERE setting_key = %s', ('show_case_win_percentages',))
        result = c.fetchone()
        current = result['setting_value'] == 'true' if result else True
        
        # Toggle it
        new_value = 'false' if current else 'true'
        
        # Update or insert
        c.execute('''
            INSERT INTO bot_settings (setting_key, setting_value)
            VALUES (%s, %s)
            ON CONFLICT (setting_key)
            DO UPDATE SET setting_value = EXCLUDED.setting_value
        ''', ('show_case_win_percentages', new_value))
        
        conn.commit()
        
        status = "visible" if new_value == 'true' else "hidden"
        await query.answer(f"✅ Win percentages now {status} to users!", show_alert=True)
        
        # Refresh the pool view
        await handle_admin_case_pool(update, context, [case_type])
    finally:
        conn.close()

async def handle_admin_custom_chance(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Prompt admin to enter custom win chance %"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 3:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_type = params[0]
    product_type = params[1]
    size = params[2]
    
    await query.answer()
    
    # Store in context for text input handler
    context.user_data['state'] = 'awaiting_custom_win_chance'
    context.user_data['custom_chance_case'] = case_type
    context.user_data['custom_chance_product'] = product_type
    context.user_data['custom_chance_size'] = size
    
    msg = f"✏️ CUSTOM WIN CHANCE\n\n"
    msg += f"Product: {product_type} {size}\n\n"
    msg += f"Please enter the win chance percentage as a number.\n\n"
    msg += f"Examples:\n"
    msg += f"• 0.1 = 0.1%\n"
    msg += f"• 3.5 = 3.5%\n"
    msg += f"• 12 = 12%\n"
    msg += f"• 50 = 50%\n\n"
    msg += f"💡 Make sure total doesn't exceed 100%!"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"admin_select_product_chance|{case_type}|{product_type}|{size}")]]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_custom_chance_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text input for custom win chance"""
    user_id = update.effective_user.id
    
    if not is_primary_admin(user_id):
        return
    
    if context.user_data.get('state') != 'awaiting_custom_win_chance':
        return
    
    text = update.message.text.strip()
    
    try:
        chance = float(text)
        
        if chance <= 0 or chance > 100:
            await update.message.reply_text(
                f"❌ Invalid percentage! Must be between 0 and 100.\n\nPlease try again:"
            )
            return
        
        # Get stored data
        case_type = context.user_data.get('custom_chance_case')
        product_type = context.user_data.get('custom_chance_product')
        size = context.user_data.get('custom_chance_size')
        
        # Clear state
        context.user_data['state'] = None
        
        # Store chance in context for next step (emoji selection)
        context.user_data['product_win_chance'] = chance
        context.user_data['product_case_type'] = case_type
        context.user_data['product_type_name'] = product_type
        context.user_data['product_size'] = size
        
        # Show emoji selection
        msg = f"✅ Win chance set to {chance}%\n\n"
        msg += f"Now select an emoji for {product_type} {size}:"
        
        emojis = ['🍺', '🌿', '💊', '💉', '🧪', '💎', '⭐', '🎁', '💸', '🏆', '🔥', '⚡']
        
        keyboard = []
        row = []
        for i, emoji in enumerate(emojis):
            row.append(InlineKeyboardButton(
                emoji,
                callback_data=f"admin_save_product_emoji|{case_type}|{emoji}"
            ))
            if (i + 1) % 4 == 0:
                keyboard.append(row)
                row = []
        
        if row:
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_select_product_chance|{case_type}|{product_type}|{size}")])
        
        await update.message.reply_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except ValueError:
        await update.message.reply_text(
            f"❌ Invalid number format! Please enter a valid number (e.g., 5 or 3.5):"
        )

async def handle_admin_save_product_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save product with custom emoji after custom % input"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_type = params[0]
    emoji = params[1]
    
    # Get stored data from context
    product_type = context.user_data.get('product_type_name')
    size = context.user_data.get('product_size')
    chance = context.user_data.get('product_win_chance')
    
    if not all([product_type, size, chance]):
        await query.answer("Session expired. Please start again.", show_alert=True)
        return
    
    await query.answer()
    
    # Save to database
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Add product to case reward pool
        c.execute('''
            INSERT INTO case_reward_pools 
            (case_type, product_type_name, product_size, win_chance_percent, reward_emoji, is_active)
            VALUES (%s, %s, %s, %s, %s, TRUE)
        ''', (case_type, product_type, size, chance, emoji))
        
        conn.commit()
        
        # Clear context
        context.user_data.pop('product_type_name', None)
        context.user_data.pop('product_size', None)
        context.user_data.pop('product_win_chance', None)
        context.user_data.pop('product_case_type', None)
        
        msg = f"✅ Product added successfully!\n\n"
        msg += f"{emoji} {product_type} {size}\n"
        msg += f"Win Chance: {chance}%\n\n"
        msg += f"Don't forget to click 'Save & Activate Case' when you're done configuring!"
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Another Product", callback_data=f"admin_add_product_to_case|{case_type}")],
            [InlineKeyboardButton("⬅️ Back to Pool", callback_data=f"admin_case_pool|{case_type}")]
        ]
        
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error saving product: {e}")
        await query.answer(f"❌ Error: {e}", show_alert=True)
        conn.rollback()
    finally:
        conn.close()

