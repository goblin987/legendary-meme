"""
Daily Rewards & Case Opening Handlers
Telegram bot handlers for the gamification system
"""

import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from daily_rewards_system import (
    check_daily_login,
    claim_daily_reward,
    get_user_points,
    open_case,
    get_user_stats,
    get_leaderboard,
    get_rolling_calendar,
    get_reward_for_day
)
from utils import get_db_connection, is_primary_admin

logger = logging.getLogger(__name__)

# ============================================================================
# USER HANDLERS
# ============================================================================

async def handle_daily_rewards_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main daily rewards menu"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    
    # Check daily login status
    login_info = check_daily_login(user_id)
    user_points = get_user_points(user_id)
    
    # Build message
    msg = "🎁 **DAILY REWARDS** 🎁\n\n"
    
    if login_info.get('is_first_time'):
        msg += "👋 Welcome! This is your first login!\n\n"
    elif login_info.get('streak_broken'):
        msg += "😢 Your streak was broken. Starting fresh!\n\n"
    
    msg += f"🔥 **Current Streak:** {login_info['streak']} day(s)\n"
    msg += f"💰 **Your Points:** {user_points}\n\n"
    
    # Show rolling calendar (last 6 days + next day)
    calendar = get_rolling_calendar(user_id, login_info['streak'])
    
    if login_info['streak'] <= 7:
        msg += "📅 **7-Day Streak Calendar:**\n"
    else:
        # Show which days are visible in rolling view
        first_day = calendar[0]['day_number']
        last_day = calendar[-1]['day_number']
        msg += f"📅 **Streak Calendar (Days {first_day}-{last_day}):**\n"
    
    for day_info in calendar:
        day_num = day_info['day_number']
        points = day_info['points']
        claimed = day_info['claimed']
        is_next = day_info['is_next']
        
        if claimed:
            msg += f"✅ Day {day_num}: {points} pts\n"
        elif is_next and login_info['can_claim']:
            msg += f"🎯 **Day {day_num}: {points} pts** ⬅️ Claim Now!\n"
        elif is_next:
            msg += f"✅ Day {day_num}: {points} pts (claimed)\n"
        else:
            msg += f"⬜ Day {day_num}: {points} pts\n"
    
    msg += f"\n🎁 **Next Reward:** {login_info.get('next_reward', '—')} points"
    
    # Build keyboard
    keyboard = []
    
    if login_info['can_claim']:
        keyboard.append([InlineKeyboardButton(
            f"🎁 Claim {login_info['points_to_award']} Points",
            callback_data="claim_daily_reward"
        )])
    
    keyboard.extend([
        [InlineKeyboardButton("💎 Open Cases", callback_data="case_opening_menu")],
        [InlineKeyboardButton("📊 My Stats", callback_data="my_case_stats")],
        [InlineKeyboardButton("🏆 Leaderboard", callback_data="case_leaderboard")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_start")]
    ])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_claim_daily_reward(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Claim daily reward with celebration animation"""
    query = update.callback_query
    user_id = query.from_user.id
    
    result = claim_daily_reward(user_id)
    
    if result['success']:
        # Success animation
        msg = "🎉 **REWARD CLAIMED!** 🎉\n\n"
        msg += f"✨ +{result['points_awarded']} Points\n"
        msg += f"🔥 Streak: Day {result['new_streak']}\n"
        msg += f"💰 Total Points: {result['total_points']}\n\n"
        msg += "🎰 Ready to test your luck?\n"
        msg += "Open cases to win products or multiply your points!"
        
        keyboard = [
            [InlineKeyboardButton("💎 Open Cases Now", callback_data="case_opening_menu")],
            [InlineKeyboardButton("⬅️ Back", callback_data="daily_rewards_menu")]
        ]
        
        await query.answer("🎁 Reward claimed!", show_alert=True)
    else:
        msg = result['message']
        keyboard = [
            [InlineKeyboardButton("⬅️ Back", callback_data="daily_rewards_menu")]
        ]
        
        await query.answer(result['message'], show_alert=True)
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_case_opening_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Case opening selection menu"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    
    user_points = get_user_points(user_id)
    
    # Get cases from database
    from daily_rewards_system import get_all_cases
    CASE_TYPES = get_all_cases()
    
    msg = "💎 **CASE OPENING** 💎\n\n"
    msg += f"💰 **Your Points:** {user_points}\n\n"
    
    if not CASE_TYPES:
        msg += "❌ No cases available yet\n\n"
        msg += "Admin needs to create cases first!"
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="daily_rewards_menu")]]
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    msg += "**Available Cases:**\n\n"
    
    keyboard = []
    
    for case_type, config in CASE_TYPES.items():
        emoji = config.get('emoji', '🎁')
        name = config.get('name', case_type.title())
        cost = config.get('cost', 10)
        rewards = config.get('rewards', {})
        
        msg += f"{emoji} **{name}**\n"
        msg += f"   💰 Cost: {cost} points\n"
        
        # Calculate win chances if rewards exist
        if rewards:
            win_product = rewards.get('win_product', 0)
            if win_product > 0:
                msg += f"   🎁 Product Win: {win_product}%\n"
            
            win_chance = sum(v for k, v in rewards.items() if 'win' in k and isinstance(v, (int, float)))
            if win_chance > 0:
                msg += f"   📊 Win Chance: {win_chance}%\n"
        
        msg += "\n"
        
        # Add button
        if user_points >= cost:
            keyboard.append([InlineKeyboardButton(
                f"{emoji} Open {name} ({cost} pts)",
                callback_data=f"open_case|{case_type}"
            )])
        else:
            keyboard.append([InlineKeyboardButton(
                f"{emoji} {name} - Need {cost - user_points} more pts",
                callback_data="noop"
            )])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="daily_rewards_menu")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_open_case(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Open a case with premium CS:GO-style animation"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not params:
        await query.answer("Invalid case selection", show_alert=True)
        return
    
    case_type = params[0]
    
    # Process case opening
    result = open_case(user_id, case_type)
    
    if not result['success']:
        await query.answer(result['message'], show_alert=True)
        return
    
    await query.answer("🎰 Opening case...", show_alert=False)
    
    # === PREMIUM ANIMATION SEQUENCE ===
    animation_data = result['animation_data']
    case_emoji = animation_data['case_emoji']
    
    # Step 1: Case intro (0.5s)
    msg = f"{case_emoji} **OPENING CASE** {case_emoji}\n\n"
    msg += f"**{case_emoji}  READY  {case_emoji}**\n\n"
    msg += "🎰 Spinning the reel..."
    
    await query.edit_message_text(msg, parse_mode='Markdown')
    await asyncio.sleep(0.5)
    
    # Step 2: CS:GO-Style Horizontal Scrolling Reel
    reel_items = animation_data['reel_items']
    
    # Show horizontal scrolling window (5 items visible, middle one is selected)
    for i in range(len(reel_items) - 4):
        visible_window = reel_items[i:i+5]
        
        msg = f"{case_emoji} **SPINNING...** {case_emoji}\n\n"
        
        # Build horizontal reel with center indicator (clean, no boxes)
        reel_line = ""
        for idx, item in enumerate(visible_window):
            if idx == 2:  # Center item (target)
                reel_line += f"**[{item['emoji']}]**  "
            else:
                reel_line += f" {item['emoji']}   "
        
        msg += reel_line + "\n\n"
        msg += "           ▼ ▼ ▼\n\n"
        
        # Progress bar
        progress = int((i / (len(reel_items) - 4)) * 20)
        msg += "🎰 " + "▓" * progress + "░" * (20 - progress)
        
        await query.edit_message_text(msg, parse_mode='Markdown')
        
        # Dynamic speed: start fast, slow down near end (CS:GO style)
        if i < 15:
            await asyncio.sleep(0.08)  # Fast
        elif i < 23:
            await asyncio.sleep(0.15)  # Medium
        else:
            await asyncio.sleep(0.35)  # Slow dramatic reveal
    
    # Step 3: Dramatic pause
    await asyncio.sleep(0.5)
    
    # Step 4: REVEAL with particles (clean, no boxes)
    outcome_emoji = animation_data['final_outcome']['emoji']
    outcome_msg = animation_data['final_outcome']['message']
    outcome_value = animation_data['final_outcome']['value']
    glow = animation_data['final_outcome']['glow_color']
    particles = animation_data['particles'][:6]
    
    msg = f"{case_emoji} **CASE OPENED!** {case_emoji}\n\n"
    msg += f"{' '.join(particles)}\n\n"
    msg += f"**{outcome_emoji}  {outcome_emoji}  {outcome_emoji}**\n\n"
    msg += f"{glow} **{outcome_msg}** {glow}\n"
    msg += f"🎁 **{outcome_value}**\n\n"
    msg += f"💰 New Balance: **{result['new_balance']} points**"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Open Another", callback_data="case_opening_menu")],
        [InlineKeyboardButton("📊 My Stats", callback_data="my_case_stats")],
        [InlineKeyboardButton("⬅️ Back", callback_data="daily_rewards_menu")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_my_case_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show user's case opening statistics"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    
    stats = get_user_stats(user_id)
    
    msg = "📊 **YOUR STATISTICS** 📊\n\n"
    msg += f"💰 **Current Points:** {stats['current_points']}\n"
    msg += f"🎁 **Lifetime Points:** {stats['lifetime_points']}\n\n"
    msg += f"📦 **Cases Opened:** {stats['total_cases_opened']}\n"
    msg += f"🏆 **Products Won:** {stats['total_products_won']}\n\n"
    msg += f"🔥 **Current Streak:** {stats['current_streak']} days\n"
    msg += f"⭐ **Longest Streak:** {stats['longest_streak']} days\n\n"
    msg += "Keep opening cases to climb the leaderboard!"
    
    keyboard = [
        [InlineKeyboardButton("🏆 Leaderboard", callback_data="case_leaderboard")],
        [InlineKeyboardButton("⬅️ Back", callback_data="case_opening_menu")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_case_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show case opening leaderboard"""
    query = update.callback_query
    await query.answer()
    
    leaderboard = get_leaderboard(limit=10)
    
    msg = "🏆 **LEADERBOARD** 🏆\n\n"
    msg += "**Top Players by Cases Opened:**\n\n"
    
    medals = ['🥇', '🥈', '🥉']
    
    for idx, player in enumerate(leaderboard, 1):
        medal = medals[idx-1] if idx <= 3 else f"{idx}."
        msg += f"{medal} **User {player['user_id']}**\n"
        msg += f"   📦 Cases: {player['total_cases_opened']}\n"
        msg += f"   🏆 Products: {player['total_products_won']}\n"
        msg += f"   💰 Points: {player['points']}\n\n"
    
    keyboard = [
        [InlineKeyboardButton("📊 My Stats", callback_data="my_case_stats")],
        [InlineKeyboardButton("⬅️ Back", callback_data="case_opening_menu")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

# ============================================================================
# ADMIN HANDLERS
# ============================================================================

async def handle_admin_daily_rewards_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin panel for daily rewards system"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    msg = "⚙️ **DAILY REWARDS ADMIN** ⚙️\n\n"
    msg += "**System Overview:**\n\n"
    
    # Get system stats
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
        
        msg += f"👥 **Active Users:** {total_users}\n"
        msg += f"💰 **Total Points in Circulation:** {total_points}\n"
        msg += f"📦 **Total Cases Opened:** {total_cases}\n\n"
        
        msg += "**Management Options:**\n"
        msg += "• View detailed statistics\n"
        msg += "• Manage rewards pool\n"
        msg += "• Edit case settings\n"
        msg += "• Give test points\n"
        
    except Exception as e:
        logger.error(f"Error loading admin stats: {e}")
        msg += f"❌ Error loading stats: {e}\n"
    finally:
        conn.close()
    
    keyboard = [
        [InlineKeyboardButton("📊 View Statistics", callback_data="admin_case_stats")],
        [InlineKeyboardButton("🎁 Manage Rewards Pool", callback_data="admin_manage_rewards")],
        [InlineKeyboardButton("⚙️ Edit Case Settings", callback_data="admin_edit_cases")],
        [InlineKeyboardButton("🎯 Give Me 200 Test Points", callback_data="admin_give_test_points")],
        [InlineKeyboardButton("🎨 Product Emojis", callback_data="admin_product_emojis")],
        [InlineKeyboardButton("👥 Top Players", callback_data="case_leaderboard")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_admin_case_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Detailed statistics for admin"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        msg = "📊 **DETAILED STATISTICS** 📊\n\n"
        
        # Case opening breakdown
        c.execute('''
            SELECT case_type, COUNT(*) as opens, SUM(points_spent) as spent
            FROM case_openings
            GROUP BY case_type
        ''')
        case_stats = c.fetchall()
        
        msg += "**Cases Opened by Type:**\n"
        for stat in case_stats:
            msg += f"   {stat['case_type']}: {stat['opens']} opens ({stat['spent']} pts spent)\n"
        
        msg += "\n**Outcome Distribution:**\n"
        c.execute('''
            SELECT outcome_type, COUNT(*) as count
            FROM case_openings
            GROUP BY outcome_type
            ORDER BY count DESC
        ''')
        outcomes = c.fetchall()
        
        for outcome in outcomes:
            msg += f"   {outcome['outcome_type']}: {outcome['count']}\n"
        
    except Exception as e:
        logger.error(f"Error loading stats: {e}")
        msg = f"❌ Error: {e}"
    finally:
        conn.close()
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_settings")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_admin_manage_rewards(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Manage products in rewards pool"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get products with stock
        c.execute('''
            SELECT id, name, price, stock as available, product_emoji
            FROM products
            WHERE stock > 0
            ORDER BY price DESC
            LIMIT 20
        ''')
        products = c.fetchall()
        
        msg = "🎁 **REWARDS POOL MANAGER** 🎁\n\n"
        msg += "Products available for case opening wins:\n\n"
        
        if products:
            for product in products:
                emoji = product['product_emoji'] or '🎁'
                msg += f"{emoji} **{product['name']}**\n"
                msg += f"   💰 Value: {product['price']}€\n"
                msg += f"   📦 Stock: {product['available']}\n\n"
        else:
            msg += "❌ No products available\n"
            msg += "Add products with stock to enable product wins!\n\n"
        
        msg += "💡 Products are randomly selected when users win.\n"
        msg += "Make sure to keep stock updated!\n"
        
    except Exception as e:
        logger.error(f"Error loading rewards pool: {e}")
        msg = f"❌ Error: {e}"
    finally:
        conn.close()
    
    keyboard = [
        [InlineKeyboardButton("📦 Manage Products", callback_data="adm_products")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="admin_manage_rewards")],
        [InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_settings")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

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
        # Add 200 points
        c.execute('''
            INSERT INTO user_points (user_id, points)
            VALUES (%s, 200)
            ON CONFLICT (user_id) DO UPDATE
            SET points = user_points.points + 200
        ''', (user_id,))
        conn.commit()
        
        # Get new total
        c.execute('SELECT points FROM user_points WHERE user_id = %s', (user_id,))
        result = c.fetchone()
        new_total = result['points'] if result else 200
        
        await query.answer(f"✅ Added 200 test points! New total: {new_total}", show_alert=True)
    except Exception as e:
        logger.error(f"Error giving test points: {e}")
        await query.answer(f"❌ Error: {e}", show_alert=True)
        if conn and conn.status == 1:
            conn.rollback()
    finally:
        conn.close()
    
    # Refresh the admin menu
    await handle_admin_daily_rewards_settings(update, context)

async def handle_admin_edit_cases(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin interface to edit case settings"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    msg = "⚙️ **CASE SETTINGS EDITOR** ⚙️\n\n"
    msg += "**Current Case Configuration:**\n\n"
    
    for case_type, config in CASE_TYPES.items():
        msg += f"{config['emoji']} **{config['name']}**\n"
        msg += f"   💰 Cost: {config['cost']} points\n"
        msg += f"   🎁 Win Product: {config['rewards']['win_product']}%\n"
        
        # Calculate total win points percentage
        win_points_total = sum(v for k, v in config['rewards'].items() if 'win_points' in k)
        msg += f"   💎 Win Points: {win_points_total}%\n"
        
        # Calculate total lose percentage
        lose_total = sum(v for k, v in config['rewards'].items() if 'lose' in k)
        msg += f"   ❌ Lose: {lose_total}%\n"
        msg += f"   🎰 Animation: {config['animation_speed']}\n\n"
    
    msg += "**Daily Streak Rewards:**\n"
    for day, points in DAILY_REWARDS.items():
        msg += f"   Day {day}: {points} points\n"
    
    msg += "\n💡 **Note:** To modify these values, edit the configuration in `daily_rewards_system.py`\n"
    msg += "Restart the bot after making changes.\n"
    
    keyboard = [
        [InlineKeyboardButton("📊 View Statistics", callback_data="admin_case_stats")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="admin_edit_cases")],
        [InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_settings")]
    ]
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_admin_product_emojis(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin interface to set custom emojis for products in case opening"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied", show_alert=True)
        return
    
    await query.answer()
    
    # Get all products with their current emojis
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            SELECT id, name, product_emoji, stock
            FROM products
            WHERE stock > 0
            ORDER BY name
            LIMIT 20
        ''')
        products = c.fetchall()
        
        msg = "🎨 **PRODUCT EMOJI MANAGER** 🎨\n\n"
        msg += "Set custom emojis for products that appear in case openings!\n\n"
        msg += "**Current Products:**\n\n"
        
        if products:
            for product in products:
                emoji = product['product_emoji'] or '🎁'
                msg += f"{emoji} **{product['name']}**\n"
                msg += f"   📦 Stock: {product['stock']}\n"
                msg += f"   🎨 Emoji: `{emoji}`\n\n"
        else:
            msg += "❌ No products with stock available\n\n"
        
        msg += "💡 **How to set emojis:**\n"
        msg += "1. Go to **📦 Product Management**\n"
        msg += "2. Edit a product\n"
        msg += "3. Set the emoji field\n\n"
        msg += "Popular emojis for cases:\n"
        msg += "🎁 💎 🏆 ⭐ 💰 🔥 ✨ 🎉 🎊 🎈\n"
        msg += "🎮 🕹️ 💻 📱 ⌚ 🎧 🎤 🎸 🎹 🎺\n"
        
        keyboard = [
            [InlineKeyboardButton("📦 Manage Products", callback_data="adm_products")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="admin_product_emojis")],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin_daily_rewards_settings")]
        ]
        
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error in admin_product_emojis: {e}")
        await query.answer(f"❌ Error: {e}", show_alert=True)
    finally:
        conn.close()
