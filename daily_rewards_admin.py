"""
Daily Rewards Admin Interface - Clean & Robust
Dummy-proof admin panel for managing cases and rewards
"""

import logging
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from utils import get_db_connection, is_primary_admin
from daily_rewards_system import (
    get_all_cases, 
    get_reward_schedule, 
    update_reward_for_day,
    get_reward_for_day
)

logger = logging.getLogger(__name__)

# ============================================================================
# MAIN ADMIN MENU
# ============================================================================

async def handle_admin_daily_rewards_main(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Clean, simple admin main menu"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    # Get quick stats
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('SELECT COUNT(*) as count FROM user_points')
        total_users = c.fetchone()['count']
        
        c.execute('SELECT SUM(points) as total FROM user_points')
        result = c.fetchone()
        total_points = result['total'] if result and result['total'] else 0
        
        c.execute('SELECT COUNT(*) as count FROM case_openings')
        total_cases = c.fetchone()['count']
        
        msg = "🎁 DAILY REWARDS ADMIN\n\n"
        msg += f"👥 Active Users: {total_users}\n"
        msg += f"💰 Points in Circulation: {total_points}\n"
        msg += f"📦 Cases Opened: {total_cases}\n\n"
        msg += "What would you like to manage?"
        
    except Exception as e:
        logger.error(f"Error loading admin stats: {e}")
        msg = "🎁 DAILY REWARDS ADMIN\n\n❌ Error loading stats"
    finally:
        conn.close()
    
    keyboard = [
        [InlineKeyboardButton("📅 Manage Reward Schedule", callback_data="admin_reward_schedule")],
        [InlineKeyboardButton("🎁 Manage Product Pool", callback_data="admin_product_pool")],
        [InlineKeyboardButton("📦 Manage Cases", callback_data="admin_manage_cases")],
        [InlineKeyboardButton("📊 View Statistics", callback_data="admin_case_stats")],
        [InlineKeyboardButton("🎯 Give Me Test Points", callback_data="admin_give_test_points")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

# ============================================================================
# PRODUCT POOL MANAGER (Robust UI like Edit Bot Look)
# ============================================================================

async def handle_admin_product_pool(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Product pool manager - Step 1: Select product"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get all products with available stock
        c.execute('''
            SELECT id, name, product_emoji, available, price
            FROM products
            WHERE available > 0
            ORDER BY price DESC
            LIMIT 20
        ''')
        products = c.fetchall()
        
        msg = "🎁 PRODUCT POOL MANAGER\n\n"
        msg += "Step 1: Select a product to configure\n\n"
        
        if products:
            msg += "Available Products:\n"
            for product in products:
                emoji = product['product_emoji'] or '🎁'
                msg += f"{emoji} {product['name']} - {product['price']}€ (Stock: {product['available']})\n"
            msg += "\n💡 Click a product below to set its win chance and emoji"
        else:
            msg += "❌ No products available\n\n"
            msg += "Add products with available stock first!"
        
        keyboard = []
        
        # Create buttons for each product (2 per row)
        for i in range(0, len(products), 2):
            row = []
            for j in range(2):
                if i + j < len(products):
                    product = products[i + j]
                    emoji = product['product_emoji'] or '🎁'
                    row.append(InlineKeyboardButton(
                        f"{emoji} {product['name'][:15]}",
                        callback_data=f"admin_edit_product_pool|{product['id']}"
                    ))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("📦 Add More Products", callback_data="adm_products")])
        keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_main")])
        
    except Exception as e:
        logger.error(f"Error loading product pool: {e}")
        msg = f"❌ Error: {e}"
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_main")]]
    finally:
        conn.close()
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def handle_admin_edit_product_pool(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit specific product in pool - Step 2: Configure"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid product", show_alert=True)
        return
    
    product_id = int(params[0])
    await query.answer()
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get product details
        c.execute('''
            SELECT id, name, product_emoji, available, price
            FROM products
            WHERE id = %s
        ''', (product_id,))
        product = c.fetchone()
        
        if not product:
            await query.answer("Product not found", show_alert=True)
            return
        
        emoji = product['product_emoji'] or '🎁'
        
        msg = f"{emoji} CONFIGURE PRODUCT\n\n"
        msg += f"Product: {product['name']}\n"
        msg += f"Value: {product['price']}€\n"
        msg += f"Stock: {product['available']}\n"
        msg += f"Current Emoji: {emoji}\n\n"
        msg += "What would you like to do?"
        
        keyboard = [
            [InlineKeyboardButton("🎨 Change Emoji", callback_data=f"admin_set_emoji|{product_id}")],
            [InlineKeyboardButton("📊 Set Win Chance %", callback_data=f"admin_set_chance|{product_id}")],
            [InlineKeyboardButton("📦 Edit Product Details", callback_data=f"edit_product|{product_id}")],
            [InlineKeyboardButton("⬅️ Back to Pool", callback_data="admin_product_pool")]
        ]
        
    except Exception as e:
        logger.error(f"Error loading product: {e}")
        msg = f"❌ Error: {e}"
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="admin_product_pool")]]
    finally:
        conn.close()
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def handle_admin_set_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Emoji picker for product"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid product", show_alert=True)
        return
    
    product_id = int(params[0])
    await query.answer()
    
    msg = "🎨 EMOJI PICKER\n\n"
    msg += "Popular Emojis for Rewards:\n\n"
    msg += "Click an emoji to set it for this product\n"
    
    # Emoji categories
    emojis = {
        "Gaming": ["🎮", "🕹️", "👾", "🎯", "🎲", "🃏"],
        "Tech": ["💻", "📱", "⌚", "🎧", "🎤", "📷"],
        "Rewards": ["🎁", "💎", "🏆", "⭐", "💰", "🔥"],
        "Fun": ["✨", "🎉", "🎊", "🎈", "🎆", "🎇"]
    }
    
    keyboard = []
    
    for category, emoji_list in emojis.items():
        msg += f"\n{category}:\n"
        row = []
        for emoji in emoji_list:
            msg += f"{emoji} "
            row.append(InlineKeyboardButton(
                emoji,
                callback_data=f"admin_save_emoji|{product_id}|{emoji}"
            ))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_edit_product_pool|{product_id}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def handle_admin_save_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save selected emoji"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    product_id = int(params[0])
    emoji = params[1]
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            UPDATE products
            SET product_emoji = %s
            WHERE id = %s
        ''', (emoji, product_id))
        conn.commit()
        
        await query.answer(f"✅ Emoji set to {emoji}!", show_alert=True)
    except Exception as e:
        logger.error(f"Error saving emoji: {e}")
        await query.answer(f"❌ Error: {e}", show_alert=True)
        if conn and conn.status == 1:
            conn.rollback()
    finally:
        conn.close()
    
    # Return to product config
    await handle_admin_edit_product_pool(update, context, [str(product_id)])

async def handle_admin_set_chance(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set win chance percentage"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid product", show_alert=True)
        return
    
    product_id = int(params[0])
    await query.answer()
    
    msg = "📊 SET WIN CHANCE\n\n"
    msg += "How rare should this product be?\n\n"
    msg += "Select a win chance percentage:\n"
    msg += "• Lower % = More rare = More exciting!\n"
    msg += "• Higher % = More common = More wins!\n\n"
    msg += "💡 Recommended ranges:\n"
    msg += "• Cheap items: 10-20%\n"
    msg += "• Mid-tier: 5-10%\n"
    msg += "• Expensive: 1-5%\n"
    msg += "• Ultra rare: 0.1-1%"
    
    # Preset percentages
    percentages = [0.1, 0.5, 1, 2, 5, 10, 15, 20]
    
    keyboard = []
    row = []
    for i, pct in enumerate(percentages):
        row.append(InlineKeyboardButton(
            f"{pct}%",
            callback_data=f"admin_save_chance|{product_id}|{pct}"
        ))
        if (i + 1) % 4 == 0:  # 4 buttons per row
            keyboard.append(row)
            row = []
    
    if row:  # Add remaining buttons
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_edit_product_pool|{product_id}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def handle_admin_save_chance(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save win chance (placeholder - needs database schema)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    product_id = int(params[0])
    chance = float(params[1])
    
    # TODO: Save to product_pool_config table
    # For now, just show success
    await query.answer(f"✅ Win chance set to {chance}%! (Feature coming soon)", show_alert=True)
    
    # Return to product config
    await handle_admin_edit_product_pool(update, context, [str(product_id)])

# ============================================================================
# CASE MANAGER
# ============================================================================

async def handle_admin_manage_cases(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Manage cases - list all cases from database"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    # Get cases from database
    cases = get_all_cases()
    
    msg = "📦 CASE MANAGER\n\n"
    
    if cases:
        msg += "Current Cases:\n\n"
        for case_type, config in cases.items():
            msg += f"🎁 {case_type.title()}\n"
            msg += f"   💰 Cost: {config['cost']} points\n\n"
    else:
        msg += "❌ No cases created yet.\n\n"
    
    msg += "💡 Create, edit, or delete cases"
    
    keyboard = []
    
    # Existing cases
    for case_type in cases.keys():
        keyboard.append([InlineKeyboardButton(
            f"✏️ Edit {case_type.title()}",
            callback_data=f"admin_edit_case|{case_type}"
        )])
    
    keyboard.append([InlineKeyboardButton("➕ Create New Case", callback_data="admin_create_case")])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_main")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def handle_admin_edit_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit specific case from database"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    
    # Get case from database
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''
            SELECT case_type, enabled, cost, rewards_config
            FROM case_settings
            WHERE case_type = %s
        ''', (case_type,))
        
        case = c.fetchone()
        
        if not case:
            await query.answer("Case not found", show_alert=True)
            return
        
        await query.answer()
        
        msg = f"✏️ EDIT CASE: {case_type.upper()}\n\n"
        msg += f"💰 Cost: {case['cost']} points\n"
        msg += f"✅ Enabled: {'Yes' if case['enabled'] else 'No'}\n\n"
        msg += "What would you like to do?"
        
        keyboard = [
            [InlineKeyboardButton("💰 Change Cost", callback_data=f"admin_case_cost|{case_type}")],
            [InlineKeyboardButton("🗑️ Delete Case", callback_data=f"admin_delete_case|{case_type}")],
            [InlineKeyboardButton("⬅️ Back to Cases", callback_data="admin_manage_cases")]
        ]
        
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    finally:
        conn.close()

async def handle_admin_case_cost(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Change case cost"""
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
    
    msg = "💰 SET CASE COST\n\n"
    msg += "Select a new cost for this case:\n\n"
    msg += "💡 Recommended pricing:\n"
    msg += "• Basic: 10-30 points\n"
    msg += "• Premium: 40-70 points\n"
    msg += "• Legendary: 80-150 points"
    
    costs = [10, 20, 30, 50, 75, 100, 150, 200]
    
    keyboard = []
    row = []
    for i, cost in enumerate(costs):
        row.append(InlineKeyboardButton(
            f"{cost} pts",
            callback_data=f"admin_save_case_cost|{case_type}|{cost}"
        ))
        if (i + 1) % 4 == 0:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"admin_edit_case|{case_type}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def handle_admin_save_case_cost(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save case cost to database"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_type = params[0]
    cost = int(params[1])
    
    # Save to database
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''
            UPDATE case_settings
            SET cost = %s
            WHERE case_type = %s
        ''', (cost, case_type))
        conn.commit()
        await query.answer(f"✅ Cost set to {cost} points!", show_alert=True)
    except Exception as e:
        logger.error(f"Error saving case cost: {e}")
        await query.answer("❌ Error saving cost", show_alert=True)
        conn.rollback()
    finally:
        conn.close()
    
    # Return to case editor
    await handle_admin_edit_case(update, context, [case_type])

async def handle_admin_create_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Create new case - Step 1: Enter name"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    msg = "➕ CREATE NEW CASE\n\n"
    msg += "Step 1: Choose a name or type custom\n\n"
    msg += "Quick suggestions:\n\n"
    msg += "OR type your own case name (lowercase, no spaces)"
    
    # Quick name suggestions + custom option
    keyboard = [
        [InlineKeyboardButton("🥉 bronze", callback_data="admin_create_case_name|bronze")],
        [InlineKeyboardButton("🥈 silver", callback_data="admin_create_case_name|silver")],
        [InlineKeyboardButton("🥇 gold", callback_data="admin_create_case_name|gold")],
        [InlineKeyboardButton("💎 diamond", callback_data="admin_create_case_name|diamond")],
        [InlineKeyboardButton("✏️ Type Custom Name", callback_data="admin_create_case_custom_name")],
        [InlineKeyboardButton("⬅️ Back", callback_data="admin_manage_cases")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_create_case_custom_name(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Prompt admin to type custom case name"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    # Set state to wait for custom name
    context.user_data['state'] = 'awaiting_case_name'
    
    msg = "✏️ TYPE CUSTOM CASE NAME\n\n"
    msg += "Send me the name for your case\n\n"
    msg += "Rules:\n"
    msg += "• Use lowercase\n"
    msg += "• No spaces (use underscore if needed)\n"
    msg += "• Example: mystery_box\n\n"
    msg += "Type the name now:"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="admin_create_case")]]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_create_case_name(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Create case - Step 2: Set cost"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid name", show_alert=True)
        return
    
    case_name = params[0]
    
    # Check if case already exists
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('SELECT case_type FROM case_settings WHERE case_type = %s', (case_name,))
        if c.fetchone():
            await query.answer(f"❌ Case '{case_name}' already exists!", show_alert=True)
            await handle_admin_create_case(update, context)
            return
    finally:
        conn.close()
    
    await query.answer()
    
    msg = f"➕ CREATE CASE: {case_name.upper()}\n\n"
    msg += "Step 2: Set the cost in points\n\n"
    msg += "💡 Recommended pricing:\n"
    msg += "• Starter: 10-20 points\n"
    msg += "• Mid-tier: 30-60 points\n"
    msg += "• High-tier: 70-150 points"
    
    costs = [10, 20, 30, 50, 75, 100, 150, 200]
    
    keyboard = []
    row = []
    for i, cost in enumerate(costs):
        row.append(InlineKeyboardButton(
            f"{cost} pts",
            callback_data=f"admin_set_case_cost|{case_name}|{cost}"
        ))
        if (i + 1) % 4 == 0:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✏️ Custom Cost", callback_data=f"admin_case_custom_cost|{case_name}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_create_case")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_case_custom_cost(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Prompt admin to type custom cost"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_name = params[0]
    await query.answer()
    
    # Set state to wait for custom cost
    context.user_data['state'] = 'awaiting_case_cost'
    context.user_data['pending_case_name'] = case_name
    
    msg = "💰 TYPE CUSTOM COST\n\n"
    msg += "Send me the cost in points\n\n"
    msg += "Example: 35\n\n"
    msg += "Type the cost now:"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"admin_create_case_name|{case_name}")]]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_set_case_cost(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show case preview with options to add products or save"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_name = params[0]
    cost = int(params[1])
    
    await query.answer()
    
    # Store in context for final save
    context.user_data['pending_case'] = {
        'name': case_name,
        'cost': cost
    }
    
    msg = f"📦 CASE PREVIEW\n\n"
    msg += f"Name: {case_name.title()}\n"
    msg += f"Cost: {cost} points\n"
    msg += f"Products: 0 (not added yet)\n\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += "What would you like to do?\n\n"
    msg += "💡 Tip: Add products first, then save"
    
    keyboard = [
        [InlineKeyboardButton("🎁 Add Products", callback_data=f"admin_add_products_to_new_case|{case_name}")],
        [InlineKeyboardButton("💾 Save Case (No Products)", callback_data=f"admin_save_empty_case|{case_name}|{cost}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="admin_create_case")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_add_products_to_new_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save case and redirect to product pool manager"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_name = params[0]
    
    # Get pending case from context
    pending_case = context.user_data.get('pending_case')
    if not pending_case:
        await query.answer("Session expired, please try again", show_alert=True)
        await handle_admin_create_case(update, context)
        return
    
    cost = pending_case['cost']
    
    # Create case in database
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO case_settings (case_type, enabled, cost, rewards_config)
            VALUES (%s, TRUE, %s, %s)
        ''', (case_name, cost, json.dumps({})))
        conn.commit()
        
        # Clear context
        context.user_data.pop('pending_case', None)
        
        await query.answer(f"✅ Case '{case_name}' created! Now add products", show_alert=True)
        
        # Redirect to product pool manager to add products
        from case_rewards_admin import handle_admin_case_pool
        await handle_admin_case_pool(update, context, [case_name])
        
    except Exception as e:
        logger.error(f"Error creating case: {e}")
        await query.answer("❌ Error creating case", show_alert=True)
        conn.rollback()
    finally:
        conn.close()

async def handle_admin_save_empty_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save case without products (final save button)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    case_name = params[0]
    cost = int(params[1])
    
    # Create case in database
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO case_settings (case_type, enabled, cost, rewards_config)
            VALUES (%s, TRUE, %s, %s)
        ''', (case_name, cost, json.dumps({})))
        conn.commit()
        
        # Clear context
        context.user_data.pop('pending_case', None)
        
        await query.answer(f"✅ Case '{case_name}' saved!", show_alert=True)
        
        msg = f"✅ CASE SAVED!\n\n"
        msg += f"Name: {case_name.title()}\n"
        msg += f"Cost: {cost} points\n"
        msg += f"Products: 0\n\n"
        msg += "⚠️ Remember to add products later!\n\n"
        msg += "Users can now see this case, but it has no rewards yet."
        
        keyboard = [
            [InlineKeyboardButton("🎁 Add Products Now", callback_data="admin_product_pool")],
            [InlineKeyboardButton("📦 Back to Cases", callback_data="admin_manage_cases")]
        ]
        
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error saving case: {e}")
        await query.answer("❌ Error saving case", show_alert=True)
        conn.rollback()
        await handle_admin_manage_cases(update, context)
    finally:
        conn.close()

async def handle_admin_delete_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Delete case from database"""
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
    
    msg = f"🗑️ DELETE CASE: {case_type.upper()}\n\n"
    msg += "⚠️ Are you sure you want to delete this case?\n\n"
    msg += "This will:\n"
    msg += "• Remove the case from the database\n"
    msg += "• Remove all product rewards\n"
    msg += "• Users won't be able to open it\n\n"
    msg += "This action cannot be undone!"
    
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Delete", callback_data=f"admin_confirm_delete_case|{case_type}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_edit_case|{case_type}")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_confirm_delete_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm and delete case"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    
    # Delete from database
    conn = get_db_connection()
    c = conn.cursor()
    try:
        # Delete case rewards
        c.execute('DELETE FROM case_reward_pools WHERE case_type = %s', (case_type,))
        # Delete lose emoji
        c.execute('DELETE FROM case_lose_emojis WHERE case_type = %s', (case_type,))
        # Delete case
        c.execute('DELETE FROM case_settings WHERE case_type = %s', (case_type,))
        conn.commit()
        await query.answer(f"✅ Case '{case_type}' deleted!", show_alert=True)
    except Exception as e:
        logger.error(f"Error deleting case: {e}")
        await query.answer("❌ Error deleting case", show_alert=True)
        conn.rollback()
    finally:
        conn.close()
    
    # Return to case manager
    await handle_admin_manage_cases(update, context)

async def handle_admin_case_desc(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Change case description (not used in new system)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    await query.answer("Description not needed in new system", show_alert=True)
    await handle_admin_edit_case(update, context, [case_type])

async def handle_admin_case_rewards(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit case rewards (placeholder)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid case", show_alert=True)
        return
    
    case_type = params[0]
    await query.answer("Feature coming soon! Rewards are in daily_rewards_system.py", show_alert=True)
    await handle_admin_edit_case(update, context, [case_type])

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

async def handle_admin_give_test_points(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Give admin 200 test points"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            INSERT INTO user_points (user_id, points)
            VALUES (%s, 200)
            ON CONFLICT (user_id) DO UPDATE
            SET points = user_points.points + 200
        ''', (user_id,))
        conn.commit()
        
        c.execute('SELECT points FROM user_points WHERE user_id = %s', (user_id,))
        result = c.fetchone()
        new_total = result['points'] if result else 200
        
        await query.answer(f"✅ Added 200 points! Total: {new_total}", show_alert=True)
    except Exception as e:
        logger.error(f"Error giving test points: {e}")
        await query.answer(f"❌ Error: {e}", show_alert=True)
        if conn and conn.status == 1:
            conn.rollback()
    finally:
        conn.close()
    
    await handle_admin_daily_rewards_main(update, context)

async def handle_admin_case_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """View statistics"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        msg = "📊 STATISTICS\n\n"
        
        # Case opening breakdown
        c.execute('''
            SELECT case_type, COUNT(*) as opens, SUM(points_spent) as spent
            FROM case_openings
            GROUP BY case_type
        ''')
        case_stats = c.fetchall()
        
        if case_stats:
            msg += "Cases Opened:\n"
            for stat in case_stats:
                # Replace underscores for display
                case_display = stat['case_type'].replace('_', ' ').title()
                msg += f"   {case_display}: {stat['opens']} opens ({stat['spent']} pts)\n"
        else:
            msg += "No cases opened yet\n"
        
        msg += "\nOutcome Distribution:\n"
        c.execute('''
            SELECT outcome_type, COUNT(*) as count
            FROM case_openings
            GROUP BY outcome_type
            ORDER BY count DESC
        ''')
        outcomes = c.fetchall()
        
        if outcomes:
            for outcome in outcomes:
                # Replace underscores for display
                outcome_display = outcome['outcome_type'].replace('_', ' ').title()
                msg += f"   {outcome_display}: {outcome['count']}\n"
        else:
            msg += "No outcomes yet\n"
        
    except Exception as e:
        logger.error(f"Error loading stats: {e}")
        msg = f"❌ Error: {e}"
    finally:
        conn.close()
    
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_main")]]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ============================================================================
# TEXT INPUT HANDLERS (for custom name/cost)
# ============================================================================

async def handle_case_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle custom case name text input"""
    user_id = update.effective_user.id
    
    if not is_primary_admin(user_id):
        return
    
    case_name = update.message.text.strip().lower()
    
    # Validate name
    if not case_name or len(case_name) > 30:
        await update.message.reply_text(
            "❌ Invalid name. Please use 1-30 characters.\nTry again:"
        )
        return
    
    if ' ' in case_name:
        await update.message.reply_text(
            "❌ No spaces allowed. Use underscore instead.\nTry again:"
        )
        return
    
    # Clear state
    context.user_data.pop('state', None)
    
    # Continue to cost selection
    from telegram import Update as TgUpdate
    from telegram import CallbackQuery
    
    # Simulate callback query to reuse existing handler
    context.user_data['custom_case_name'] = case_name
    
    await update.message.reply_text(f"✅ Name set to: {case_name}\n\nNow setting cost...")
    
    # Redirect to cost selection by calling the handler directly
    # We need to create a fake callback query
    class FakeCallbackQuery:
        def __init__(self, user_id, message):
            self.from_user = type('obj', (object,), {'id': user_id})
            self.message = message
        
        async def answer(self, *args, **kwargs):
            pass
        
        async def edit_message_text(self, text, **kwargs):
            await self.message.reply_text(text, reply_markup=kwargs.get('reply_markup'))
    
    fake_update = type('obj', (object,), {
        'callback_query': FakeCallbackQuery(user_id, update.message),
        'effective_user': update.effective_user
    })()
    
    await handle_admin_create_case_name(fake_update, context, [case_name])

async def handle_case_cost_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle custom case cost text input"""
    user_id = update.effective_user.id
    
    if not is_primary_admin(user_id):
        return
    
    try:
        cost = int(update.message.text.strip())
        
        if cost < 1 or cost > 10000:
            await update.message.reply_text(
                "❌ Cost must be between 1-10000 points.\nTry again:"
            )
            return
        
        # Get pending case name
        case_name = context.user_data.get('pending_case_name')
        if not case_name:
            await update.message.reply_text("❌ Session expired. Please start over.")
            context.user_data.pop('state', None)
            return
        
        # Clear state
        context.user_data.pop('state', None)
        
        await update.message.reply_text(f"✅ Cost set to: {cost} points\n\nShowing preview...")
        
        # Simulate callback query
        class FakeCallbackQuery:
            def __init__(self, user_id, message):
                self.from_user = type('obj', (object,), {'id': user_id})
                self.message = message
            
            async def answer(self, *args, **kwargs):
                pass
            
            async def edit_message_text(self, text, **kwargs):
                await self.message.reply_text(text, reply_markup=kwargs.get('reply_markup'))
        
        fake_update = type('obj', (object,), {
            'callback_query': FakeCallbackQuery(user_id, update.message),
            'effective_user': update.effective_user
        })()
        
        await handle_admin_set_case_cost(fake_update, context, [case_name, str(cost)])
        
    except ValueError:
        await update.message.reply_text(
            "❌ Please enter a valid number.\nTry again:"
        )

# ============================================================================
# REWARD SCHEDULE MANAGER (NEW!)
# ============================================================================

async def handle_admin_reward_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Manage daily reward schedule - seller can customize everything!"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    # Get current schedule
    schedule = get_reward_schedule()
    max_day = max(schedule.keys()) if schedule else 7
    
    # Detect pattern type
    all_same = len(set(schedule[d]['points'] for d in schedule.keys())) == 1
    is_progressive = all([schedule[i+1]['points'] == schedule[i]['points'] + 1 
                          for i in range(1, max_day) if i in schedule and i+1 in schedule])
    
    msg = "📅 DAILY REWARD SCHEDULE\n\n"
    
    # Show current settings
    if all_same:
        msg += f"📊 Current: FIXED ({schedule[1]['points']} pts/day)\n"
    elif is_progressive:
        msg += f"📈 Current: PROGRESSIVE (starts at {schedule[1]['points']} pts)\n"
    else:
        msg += f"🎨 Current: CUSTOM pattern\n"
    
    msg += f"📆 Total days: {max_day}\n"
    msg += f"♾️ Unlimited: YES (repeats pattern)\n\n"
    
    msg += "Current Schedule:\n"
    for day in sorted(schedule.keys()):
        points = schedule[day]['points']
        msg += f"Day {day}: {points} pts\n"
    
    msg += "\n💡 Click a day to edit or apply a pattern"
    
    keyboard = []
    row = []
    for day in sorted(schedule.keys()):
        row.append(InlineKeyboardButton(
            f"Day {day}",
            callback_data=f"admin_edit_reward_day|{day}"
        ))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    # Simple pattern buttons
    keyboard.append([
        InlineKeyboardButton("📊 Fixed", callback_data="admin_pattern_fixed"),
        InlineKeyboardButton("📈 Progressive", callback_data="admin_pattern_progressive")
    ])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_main")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_edit_reward_day(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit reward for a specific day"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid day", show_alert=True)
        return
    
    day_number = int(params[0])
    current_points = get_reward_for_day(day_number)
    
    await query.answer()
    
    msg = f"✏️ EDIT DAY {day_number} REWARD\n\n"
    msg += f"Current: {current_points} points\n\n"
    msg += "Select new reward amount:"
    
    # Preset amounts
    presets = [10, 15, 20, 25, 30, 40, 50, 60, 75, 90, 100, 150, 200, 250, 300, 500]
    
    keyboard = []
    row = []
    for i, points in enumerate(presets):
        row.append(InlineKeyboardButton(
            f"{points} pts",
            callback_data=f"admin_save_reward_day|{day_number}|{points}"
        ))
        if (i + 1) % 4 == 0:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("✏️ Enter Custom Amount", callback_data=f"admin_custom_reward_day|{day_number}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_reward_schedule")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_save_reward_day(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save new reward amount for a day"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params or len(params) < 2:
        await query.answer("Invalid data", show_alert=True)
        return
    
    day_number = int(params[0])
    points = int(params[1])
    
    # Update in database
    success = update_reward_for_day(day_number, points)
    
    if success:
        await query.answer(f"✅ Day {day_number} now awards {points} points!", show_alert=True)
    else:
        await query.answer("❌ Error updating reward", show_alert=True)
    
    # Refresh schedule view
    await handle_admin_reward_schedule(update, context)

async def handle_admin_custom_reward_day(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Prompt for custom reward amount"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid day", show_alert=True)
        return
    
    day_number = int(params[0])
    
    await query.answer()
    
    # Store in context
    context.user_data['state'] = 'awaiting_custom_reward_amount'
    context.user_data['reward_day_number'] = day_number
    
    msg = f"✏️ CUSTOM REWARD FOR DAY {day_number}\n\n"
    msg += "Enter the reward amount (points):\n\n"
    msg += "Examples:\n"
    msg += "• 10\n"
    msg += "• 50\n"
    msg += "• 100\n"
    msg += "• 500\n\n"
    msg += "Just type a number and send it!"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"admin_edit_reward_day|{day_number}")]]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_custom_reward_amount_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text input for custom reward amount"""
    user_id = update.effective_user.id
    
    if not is_primary_admin(user_id):
        return
    
    if context.user_data.get('state') != 'awaiting_custom_reward_amount':
        return
    
    text = update.message.text.strip()
    
    try:
        points = int(text)
        
        if points <= 0:
            await update.message.reply_text(
                "❌ Points must be greater than 0.\nTry again:"
            )
            return
        
        day_number = context.user_data.get('reward_day_number')
        
        # Clear state
        context.user_data['state'] = None
        
        # Update reward
        success = update_reward_for_day(day_number, points)
        
        if success:
            await update.message.reply_text(
                f"✅ Day {day_number} now awards {points} points!\n\nReturning to schedule..."
            )
        else:
            await update.message.reply_text(
                "❌ Error updating reward. Please try again."
            )
        
    except ValueError:
        await update.message.reply_text(
            "❌ Please enter a valid number.\nTry again:"
        )

async def handle_admin_add_reward_days(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Add more days to the reward schedule"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    schedule = get_reward_schedule()
    max_day = max(schedule.keys()) if schedule else 7
    
    msg = f"➕ ADD MORE REWARD DAYS\n\n"
    msg += f"Current schedule goes up to Day {max_day}\n\n"
    msg += "How many more days would you like to add?"
    
    keyboard = []
    for days_to_add in [7, 14, 30]:
        keyboard.append([InlineKeyboardButton(
            f"Add {days_to_add} more days",
            callback_data=f"admin_confirm_add_days|{days_to_add}"
        )])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_reward_schedule")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_confirm_add_days(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm and add more days to schedule"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid data", show_alert=True)
        return
    
    days_to_add = int(params[0])
    
    schedule = get_reward_schedule()
    max_day = max(schedule.keys()) if schedule else 7
    
    # Add new days with progressive rewards
    for i in range(1, days_to_add + 1):
        new_day = max_day + i
        # Calculate progressive reward (50% more than previous cycle)
        base_day = ((new_day - 1) % 7) + 1
        base_reward = schedule.get(base_day, {}).get('points', 10)
        cycle_number = (new_day - 1) // 7
        multiplier = 1 + (cycle_number * 0.5)
        new_reward = int(base_reward * multiplier)
        
        update_reward_for_day(new_day, new_reward, f'Day {new_day} reward')
    
    await query.answer(f"✅ Added {days_to_add} more days!", show_alert=True)
    
    # Refresh schedule view
    await handle_admin_reward_schedule(update, context)

# ============================================================================
# REWARD PATTERN HANDLERS
# ============================================================================

async def handle_admin_pattern_fixed(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply fixed reward pattern (same points every day)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    msg = "📊 FIXED REWARD PATTERN\n\n"
    msg += "Every day users get the SAME amount of points.\n\n"
    msg += "Example:\n"
    msg += "• Day 1: 1 point\n"
    msg += "• Day 2: 1 point\n"
    msg += "• Day 3: 1 point\n"
    msg += "• Day 4: 1 point\n"
    msg += "• ...\n\n"
    msg += "Enter the fixed amount (points per day):"
    
    # Preset amounts
    presets = [1, 2, 5, 10, 15, 20, 25, 50, 100]
    
    keyboard = []
    row = []
    for i, points in enumerate(presets):
        row.append(InlineKeyboardButton(
            f"{points} pts",
            callback_data=f"admin_apply_fixed|{points}"
        ))
        if (i + 1) % 3 == 0:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_reward_schedule")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_apply_fixed(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply fixed reward pattern to all days"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid amount", show_alert=True)
        return
    
    fixed_amount = int(params[0])
    
    # Get current schedule
    schedule = get_reward_schedule()
    max_day = max(schedule.keys()) if schedule else 7
    
    # Apply fixed pattern to all days
    for day in range(1, max_day + 1):
        update_reward_for_day(day, fixed_amount, f'Fixed reward')
    
    await query.answer(f"✅ Applied fixed pattern: {fixed_amount} pts/day for {max_day} days!", show_alert=True)
    
    # Refresh schedule view
    await handle_admin_reward_schedule(update, context)

async def handle_admin_pattern_progressive(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply progressive reward pattern (+1 more each day)"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    msg = "📈 PROGRESSIVE REWARD PATTERN\n\n"
    msg += "Every day users get +1 MORE point than the previous day.\n\n"
    msg += "Example:\n"
    msg += "• Day 1: 1 point\n"
    msg += "• Day 2: 2 points (+1)\n"
    msg += "• Day 3: 3 points (+1)\n"
    msg += "• Day 4: 4 points (+1)\n"
    msg += "• Day 5: 5 points (+1)\n"
    msg += "• ...\n\n"
    msg += "Select starting amount:"
    
    # Starting amounts
    starts = [1, 2, 5, 10]
    
    keyboard = []
    for start in starts:
        keyboard.append([InlineKeyboardButton(
            f"Start at {start} pts (Day 1={start}, Day 2={start+1}, Day 3={start+2}...)",
            callback_data=f"admin_apply_progressive|{start}"
        )])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_reward_schedule")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_admin_apply_progressive(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply progressive reward pattern to all days"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    if not params:
        await query.answer("Invalid amount", show_alert=True)
        return
    
    start_amount = int(params[0])
    
    # Get current schedule
    schedule = get_reward_schedule()
    max_day = max(schedule.keys()) if schedule else 7
    
    # Apply progressive pattern to all days
    for day in range(1, max_day + 1):
        points = start_amount + (day - 1)  # Day 1 = start, Day 2 = start+1, etc.
        update_reward_for_day(day, points, f'Progressive reward')
    
    await query.answer(f"✅ Applied progressive pattern starting at {start_amount} pts!", show_alert=True)
    
    # Refresh schedule view
    await handle_admin_reward_schedule(update, context)
