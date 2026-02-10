# --- START OF FILE referral_system.py ---

import logging
import psycopg2
import random
import string
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Optional, Dict, List, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from utils import (
    get_db_connection, send_message_with_retry, format_currency,
    is_primary_admin, LANGUAGES
)

logger = logging.getLogger(__name__)

def init_referral_tables():
    """Initialize referral system database tables."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # referral_codes table - for referral program
        c.execute('''CREATE TABLE IF NOT EXISTS referral_codes (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            referral_code TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            total_referrals INTEGER DEFAULT 0,
            total_rewards_earned REAL DEFAULT 0.0,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )''')
        
        # referrals table - tracks successful referrals
        c.execute('''CREATE TABLE IF NOT EXISTS referrals (
            id SERIAL PRIMARY KEY,
            referrer_user_id BIGINT NOT NULL,
            referred_user_id BIGINT NOT NULL,
            referral_code TEXT NOT NULL,
            created_at TEXT NOT NULL,
            first_purchase_at TEXT,
            referred_reward REAL DEFAULT 0.0,
            referrer_reward REAL DEFAULT 0.0,
            status TEXT DEFAULT 'active',
            FOREIGN KEY (referrer_user_id) REFERENCES users(user_id) ON DELETE CASCADE,
            FOREIGN KEY (referred_user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )''')
        
        # 🚀 MODE: ADD MISSING COLUMNS TO USERS TABLE!
        try:
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS total_referrals INTEGER DEFAULT 0")
            logger.info("✅ Added total_referrals column to users table")
        except Exception as e:
            logger.debug(f"total_referrals column might already exist: {e}")
            
        try:
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS total_earnings REAL DEFAULT 0.0")
            logger.info("✅ Added total_earnings column to users table")
        except Exception as e:
            logger.debug(f"total_earnings column might already exist: {e}")
        
        conn.commit()
        logger.info("Referral system database tables initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing referral tables: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# Referral program settings - NOW ADMIN CONFIGURABLE! 🚀
def get_referral_settings():
    """Get referral settings from database with fallback to defaults"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get settings from bot_settings table
        c.execute("SELECT setting_key, setting_value FROM bot_settings WHERE setting_key LIKE 'referral_%'")
        settings = {row['setting_key']: row['setting_value'] for row in c.fetchall()}
        
        return {
            'referrer_percentage': Decimal(settings.get('referral_referrer_percentage', '5.0')),
            'referred_bonus': Decimal(settings.get('referral_referred_bonus', '2.0')),
            'min_purchase': Decimal(settings.get('referral_min_purchase', '10.0')),
            'program_enabled': settings.get('referral_program_enabled', 'true') == 'true'
        }
    except Exception as e:
        logger.error(f"Error getting referral settings: {e}")
        # Fallback to defaults
        return {
            'referrer_percentage': Decimal('5.0'),
            'referred_bonus': Decimal('2.0'),
            'min_purchase': Decimal('10.0'),
            'program_enabled': True
        }
    finally:
        if conn:
            conn.close()

def set_referral_setting(key: str, value: str):
    """Set a referral setting in the database"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            INSERT INTO bot_settings (setting_key, setting_value) 
            VALUES (%s, %s)
            ON CONFLICT (setting_key) 
            DO UPDATE SET setting_value = EXCLUDED.setting_value
        """, (f'referral_{key}', value))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error setting referral setting {key}: {e}")
        return False
    finally:
        if conn:
            conn.close()

# Legacy constants for backward compatibility
REFERRER_REWARD_PERCENTAGE = Decimal('5.0')  # Will be overridden by get_referral_settings()
REFERRED_USER_BONUS = Decimal('2.0')  # Will be overridden by get_referral_settings()
MIN_PURCHASE_FOR_REFERRAL = Decimal('10.0')  # Will be overridden by get_referral_settings()
REFERRAL_CODE_LENGTH = 8

# --- Core Referral Functions ---

def generate_referral_code(user_id: int) -> str:
    """Generates a unique referral code for a user."""
    # Create a code based on user ID and random string for uniqueness
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    code = f"REF{user_id % 10000:04d}{random_part}"
    return code

async def create_referral_code(user_id: int) -> Optional[str]:
    """Creates a referral code for a user if they don't have one."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check if user already has a referral code
        c.execute("""
            SELECT referral_code FROM referral_codes 
            WHERE user_id = %s AND is_active = TRUE
        """, (user_id,))
        
        existing = c.fetchone()
        if existing:
            return existing['referral_code']
        
        # Generate new code and ensure it's unique
        max_attempts = 10
        for _ in range(max_attempts):
            code = generate_referral_code(user_id)
            
            # Check if code already exists
            c.execute("SELECT id FROM referral_codes WHERE referral_code = %s", (code,))
            if not c.fetchone():
                # Code is unique, create it
                c.execute("""
                    INSERT INTO referral_codes 
                    (user_id, referral_code, created_at, is_active)
                    VALUES (%s, %s, %s, TRUE)
                """, (user_id, code, datetime.now(timezone.utc).isoformat()))
                
                conn.commit()
                logger.info(f"Created referral code {code} for user {user_id}")
                return code
        
        logger.error(f"Failed to generate unique referral code for user {user_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error creating referral code: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

async def apply_referral_code(referred_user_id: int, referral_code: str) -> Dict[str, any]:
    """
    Applies a referral code for a new user.
    Returns result dictionary with success status and details.
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check if user already used a referral code
        c.execute("""
            SELECT id FROM referrals WHERE referred_user_id = %s
        """, (referred_user_id,))
        
        if c.fetchone():
            return {
                'success': False,
                'error': 'referral_already_used',
                'message': 'You have already used a referral code.'
            }
        
        # Find the referral code and its owner
        c.execute("""
            SELECT rc.user_id, rc.referral_code, u.username
            FROM referral_codes rc
            LEFT JOIN users u ON rc.user_id = u.user_id
            WHERE rc.referral_code = %s AND rc.is_active = TRUE
        """, (referral_code.upper(),))
        
        referrer = c.fetchone()
        if not referrer:
            return {
                'success': False,
                'error': 'invalid_code',
                'message': 'Invalid or inactive referral code.'
            }
        
        referrer_user_id = referrer['user_id']
        
        # Can't refer yourself
        if referrer_user_id == referred_user_id:
            return {
                'success': False,
                'error': 'self_referral',
                'message': 'You cannot use your own referral code.'
            }
        
        # Create the referral relationship
        c.execute("""
            INSERT INTO referrals 
            (referrer_user_id, referred_user_id, referral_code, created_at, 
             referred_reward, status)
            VALUES (%s, %s, %s, %s, %s, 'active')
        """, (
            referrer_user_id, referred_user_id, referral_code,
            datetime.now(timezone.utc).isoformat(),
            float(REFERRED_USER_BONUS)
        ))
        
        # Give immediate bonus to referred user using dynamic settings
        settings = get_referral_settings()
        c.execute("""
            UPDATE users SET balance = balance + %s WHERE user_id = %s
        """, (float(settings['referred_bonus']), referred_user_id))
        
        # Update referrer's total referrals count
        c.execute("""
            UPDATE referral_codes 
            SET total_referrals = total_referrals + 1
            WHERE user_id = %s
        """, (referrer_user_id,))
        
        conn.commit()
        
        logger.info(f"Applied referral code {referral_code}: {referrer_user_id} -> {referred_user_id}")
        
        return {
            'success': True,
            'referrer_username': referrer.get('username', 'Unknown'),
            'bonus_amount': settings['referred_bonus'],
            'referrer_user_id': referrer_user_id
        }
        
    except Exception as e:
        logger.error(f"Error applying referral code: {e}")
        if conn:
            conn.rollback()
        return {
            'success': False,
            'error': 'system_error',
            'message': 'System error occurred. Please try again.'
        }
    finally:
        if conn:
            conn.close()

async def process_referral_purchase(user_id: int, purchase_amount: Decimal) -> bool:
    """
    Processes a purchase for referral rewards.
    Gives reward to referrer if this is the referred user's first qualifying purchase.
    """
    conn = None
    try:
        # Get current referral settings
        settings = get_referral_settings()
        
        # Check if referral program is enabled
        if not settings['program_enabled']:
            return False
        
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check if this user was referred and hasn't had their first purchase processed
        c.execute("""
            SELECT referrer_user_id, referral_code, first_purchase_at
            FROM referrals 
            WHERE referred_user_id = %s AND status = 'active'
        """, (user_id,))
        
        referral = c.fetchone()
        if not referral or referral['first_purchase_at']:
            return False  # No referral or already processed
        
        # Check if purchase meets minimum requirement using dynamic settings
        if purchase_amount < settings['min_purchase']:
            logger.debug(f"Purchase {purchase_amount} below minimum {settings['min_purchase']} for referral")
            return False
        
        referrer_user_id = referral['referrer_user_id']
        referral_code = referral['referral_code']
        
        # Calculate referrer reward using dynamic percentage
        referrer_reward = (purchase_amount * settings['referrer_percentage'] / 100).quantize(
            Decimal('0.01'), rounding=ROUND_DOWN
        )
        
        # Update referral record
        c.execute("""
            UPDATE referrals 
            SET first_purchase_at = %s, referrer_reward = %s, status = 'completed'
            WHERE referred_user_id = %s
        """, (datetime.now(timezone.utc).isoformat(), float(referrer_reward), user_id))
        
        # Give reward to referrer
        c.execute("""
            UPDATE users SET balance = balance + %s WHERE user_id = %s
        """, (float(referrer_reward), referrer_user_id))
        
        # Update referrer's total rewards earned
        c.execute("""
            UPDATE referral_codes 
            SET total_rewards_earned = total_rewards_earned + %s
            WHERE user_id = %s
        """, (float(referrer_reward), referrer_user_id))
        
        conn.commit()
        
        logger.info(f"Processed referral reward: {referrer_reward} EUR to user {referrer_user_id} for referring {user_id}")
        
        # Store notification for later sending
        try:
            reward_msg = (
                f"🎉 **Referral Reward!**\n\n"
                f"Your referral made their first purchase!\n"
                f"💰 You earned: {format_currency(referrer_reward)}\n"
                f"🔗 Referral code: {referral_code}\n\n"
                f"Keep sharing your code to earn more rewards!"
            )
            
            # Store notification in context for main to send
            conn.execute("""
                INSERT INTO user_notifications (user_id, message, notification_type, created_at)
                VALUES (%s, %s, 'referral_reward', %s)
            """, (referrer_user_id, reward_msg, datetime.now(timezone.utc).isoformat()))
            
        except Exception as e:
            logger.error(f"Error storing referral notification: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing referral purchase: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def get_referral_stats(user_id: int) -> Dict[str, any]:
    """Gets referral statistics for a user."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get referral code info
        c.execute("""
            SELECT referral_code, total_referrals, total_rewards_earned, created_at
            FROM referral_codes 
            WHERE user_id = %s AND is_active = 1
        """, (user_id,))
        
        code_info = c.fetchone()
        if not code_info:
            return {'has_code': False}
        
        # Get successful referrals
        c.execute("""
            SELECT COUNT(*) as completed_referrals
            FROM referrals 
            WHERE referrer_user_id = %s AND status = 'completed'
        """, (user_id,))
        
        completed = c.fetchone()['completed_referrals']
        
        # Get pending referrals
        c.execute("""
            SELECT COUNT(*) as pending_referrals
            FROM referrals 
            WHERE referrer_user_id = %s AND status = 'active' AND first_purchase_at IS NULL
        """, (user_id,))
        
        pending = c.fetchone()['pending_referrals']
        
        # Get recent referrals
        c.execute("""
            SELECT r.created_at, r.status, r.referrer_reward, u.username
            FROM referrals r
            LEFT JOIN users u ON r.referred_user_id = u.user_id
            WHERE r.referrer_user_id = %s
            ORDER BY r.created_at DESC
            LIMIT 10
        """, (user_id,))
        
        recent_referrals = c.fetchall()
        
        return {
            'has_code': True,
            'referral_code': code_info['referral_code'],
            'total_referrals': code_info['total_referrals'],
            'completed_referrals': completed,
            'pending_referrals': pending,
            'total_earned': Decimal(str(code_info['total_rewards_earned'])),
            'recent_referrals': recent_referrals,
            'code_created': code_info['created_at']
        }
        
    except Exception as e:
        logger.error(f"Error getting referral stats: {e}")
        return {'has_code': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()

# --- User Interface Handlers ---

async def handle_referral_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows the referral program menu to users."""
    query = update.callback_query
    user_id = query.from_user.id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    
    # Get current referral settings
    settings = get_referral_settings()
    
    # Check if referral program is enabled
    if not settings['program_enabled']:
        await query.edit_message_text(
            "🚫 **Referral Program Disabled**\n\nThe referral program is currently disabled by the administrator.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Profile", callback_data="profile")]]),
            parse_mode='Markdown'
        )
        return
    
    stats = get_referral_stats(user_id)
    
    if not stats.get('has_code'):
        # User doesn't have a referral code yet
        msg = "🎁 **Referral Program**\n\n"
        msg += "Invite friends and earn rewards!\n\n"
        msg += f"💰 **How it works:**\n"
        msg += f"• Get your unique referral code\n"
        msg += f"• Share it with friends\n"
        msg += f"• They get {format_currency(settings['referred_bonus'])} welcome bonus\n"
        msg += f"• You get {settings['referrer_percentage']}% of their first purchase\n\n"
        msg += "Ready to start earning?"
        
        keyboard = [
            [InlineKeyboardButton("🎯 Get My Referral Code", callback_data="referral_create_code")],
            [InlineKeyboardButton("❓ How It Works", callback_data="referral_how_it_works")],
            [InlineKeyboardButton("⬅️ Back to Profile", callback_data="profile")]
        ]
    else:
        # User has a referral code
        msg = "🎁 **Your Referral Dashboard**\n\n"
        msg += f"🔗 **Your Code:** `{stats['referral_code']}`\n\n"
        msg += f"📊 **Statistics:**\n"
        msg += f"• Total Referrals: {stats['total_referrals']}\n"
        msg += f"• Completed: {stats['completed_referrals']}\n"
        msg += f"• Pending: {stats['pending_referrals']}\n"
        msg += f"• Total Earned: {format_currency(stats['total_earned'])}\n\n"
        msg += "Share your code to earn more rewards!"
        
        keyboard = [
            [InlineKeyboardButton("📤 Share Code", callback_data="referral_share_code")],
            [InlineKeyboardButton("📊 View Details", callback_data="referral_view_details")],
            [InlineKeyboardButton("💡 Tips & Tricks", callback_data="referral_tips")],
            [InlineKeyboardButton("⬅️ Back to Profile", callback_data="profile")]
        ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_create_code(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Creates a referral code for the user."""
    query = update.callback_query
    user_id = query.from_user.id
    
    await query.answer("Creating your referral code...", show_alert=False)
    
    code = await create_referral_code(user_id)
    
    if code:
        msg = f"🎉 **Your Referral Code Created!**\n\n"
        msg += f"🔗 **Your Code:** `{code}`\n\n"
        msg += f"💡 **How to use:**\n"
        msg += f"1. Share this code with friends\n"
        msg += f"2. They enter it when they first use the bot\n"
        msg += f"3. They get {format_currency(REFERRED_USER_BONUS)} welcome bonus\n"
        msg += f"4. You get {REFERRER_REWARD_PERCENTAGE}% of their first purchase\n\n"
        msg += f"Start sharing and earning now!"
        
        keyboard = [
            [InlineKeyboardButton("📤 Share Code", callback_data="referral_share_code")],
            [InlineKeyboardButton("⬅️ Back to Referrals", callback_data="referral_menu")]
        ]
    else:
        msg = "❌ **Error Creating Code**\n\nSorry, we couldn't create your referral code right now. Please try again later."
        keyboard = [[InlineKeyboardButton("🔄 Try Again", callback_data="referral_create_code")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_share_code(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows sharing options for the referral code."""
    query = update.callback_query
    user_id = query.from_user.id
    
    stats = get_referral_stats(user_id)
    
    if not stats.get('has_code'):
        await query.answer("You don't have a referral code yet!", show_alert=True)
        return
    
    code = stats['referral_code']
    bot_username = context.bot.username
    
    # Create sharing message
    share_text = (
        f"🎁 Join this amazing bot and get {format_currency(REFERRED_USER_BONUS)} welcome bonus!\n\n"
        f"Use my referral code: {code}\n\n"
        f"Start here: @{bot_username}"
    )
    
    # URL encode the share text
    import urllib.parse
    encoded_text = urllib.parse.quote(share_text)
    
    msg = f"📤 **Share Your Referral Code**\n\n"
    msg += f"🔗 **Your Code:** `{code}`\n\n"
    msg += f"**Share Message:**\n{share_text}\n\n"
    msg += "Choose how you want to share:"
    
    keyboard = [
        [InlineKeyboardButton("📱 Share on Telegram", 
                            url=f"https://t.me/share/url?url={encoded_text}")],
        [InlineKeyboardButton("📋 Copy Code Only", callback_data=f"referral_copy_code|{code}")],
        [InlineKeyboardButton("⬅️ Back to Referrals", callback_data="referral_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_copy_code(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows the code for copying."""
    query = update.callback_query
    
    if not params:
        await query.answer("Error: No code provided", show_alert=True)
        return
    
    code = params[0]
    
    msg = f"📋 **Copy Your Referral Code**\n\n"
    msg += f"`{code}`\n\n"
    msg += "Tap the code above to copy it, then share it with your friends!"
    
    keyboard = [[InlineKeyboardButton("⬅️ Back to Share Options", callback_data="referral_share_code")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- Admin Handlers ---

async def handle_referral_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows referral program admin menu."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied.", show_alert=True)
        return
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get referral program statistics from users table
        c.execute("SELECT COUNT(*) as total_users FROM users WHERE referral_code IS NOT NULL")
        users_with_codes = c.fetchone()['total_users']
        
        c.execute("SELECT COUNT(*) as total_referrals, SUM(total_earnings) as total_earnings FROM users WHERE total_referrals > 0")
        referral_stats = c.fetchone()
        
        c.execute("SELECT COUNT(*) as active_referrers FROM users WHERE total_referrals > 0")
        active_referrers = c.fetchone()['active_referrers']
        
        total_referrals = referral_stats['total_referrals'] or 0
        total_earnings = referral_stats['total_earnings'] or 0
        
        msg = "🎁 **Referral Program Admin**\n\n"
        msg += f"📊 **Program Statistics:**\n"
        msg += f"• Users with Referral Codes: {users_with_codes}\n"
        msg += f"• Active Referrers: {active_referrers}\n"
        msg += f"• Total Referrals Made: {total_referrals}\n"
        msg += f"• Total Earnings Paid: ${total_earnings:.2f}\n\n"
        
        if active_referrers > 0:
            avg_referrals = total_referrals / active_referrers
            msg += f"• Average Referrals per User: {avg_referrals:.1f}\n"
        
        msg += f"\n🎯 **Referral Program Features:**\n"
        msg += f"• 10% commission for referrers\n"
        msg += f"• 5% discount for new users\n"
        msg += f"• Automatic tracking and rewards\n"
        msg += f"• Real-time statistics"
        
        keyboard = [
            [InlineKeyboardButton("📊 Detailed Stats", callback_data="referral_admin_stats")],
            [InlineKeyboardButton("👥 Top Referrers", callback_data="referral_admin_top_referrers")],
            [InlineKeyboardButton("⚙️ Program Settings", callback_data="referral_admin_settings")],
            [InlineKeyboardButton("🔄 Reset Program", callback_data="referral_admin_reset")],
            [InlineKeyboardButton("⬅️ Back to User Management", callback_data="admin_users_menu")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error showing referral admin menu: {e}")
        # Fallback to simple menu if database queries fail
        msg = "🎁 **Referral Program Admin**\n\n"
        msg += "📊 **Referral System Management**\n\n"
        msg += "Manage your referral program settings and view statistics.\n\n"
        msg += "**Features:**\n"
        msg += "• View referral statistics\n"
        msg += "• Manage top referrers\n"
        msg += "• Configure program settings\n"
        msg += "• Reset program data"
        
        keyboard = [
            [InlineKeyboardButton("📊 View Stats", callback_data="referral_admin_stats")],
            [InlineKeyboardButton("👥 Top Referrers", callback_data="referral_admin_top_referrers")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="referral_admin_settings")],
            [InlineKeyboardButton("⬅️ Back to User Management", callback_data="admin_users_menu")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    finally:
        if conn:
            conn.close()

# --- Missing Referral Handlers ---

async def handle_referral_how_it_works(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show how referral program works"""
    query = update.callback_query
    
    msg = "❓ **How Referrals Work**\n\n"
    msg += "1️⃣ Get your unique referral code\n"
    msg += "2️⃣ Share it with friends\n" 
    msg += "3️⃣ They use your code when making purchases\n"
    msg += "4️⃣ You both get rewards!\n\n"
    msg += "💰 **Rewards:**\n"
    msg += "• You: 10% commission on their purchases\n"
    msg += "• Friend: 5% discount on first purchase"
    
    keyboard = [[InlineKeyboardButton("⬅️ Back to Referrals", callback_data="referral_menu")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_view_details(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """View referral details and stats"""
    query = update.callback_query
    user_id = query.from_user.id
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get user's referral stats
        c.execute("""
            SELECT referral_code, total_referrals, total_earnings
            FROM users 
            WHERE user_id = %s
        """, (user_id,))
        user_stats = c.fetchone()
        
        if not user_stats or not user_stats['referral_code']:
            await query.answer("No referral code found", show_alert=True)
            return
        
        # Get detailed referral activity
        c.execute("""
            SELECT referred_user_id, created_at, commission_earned, status
            FROM referrals 
            WHERE referrer_user_id = %s
            ORDER BY created_at DESC
            LIMIT 10
        """, (user_id,))
        referral_activity = c.fetchall()
        
        # Get referral performance (last 30 days)
        c.execute("""
            SELECT 
                COUNT(*) as recent_referrals,
                SUM(commission_earned) as recent_earnings
            FROM referrals 
            WHERE referrer_user_id = %s 
            AND created_at >= datetime('now', '-30 days')
        """, (user_id,))
        recent_stats = c.fetchone()
        
    except Exception as e:
        logger.error(f"Error getting referral details: {e}")
        await query.answer("Error loading details", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    msg = f"📊 **Your Referral Statistics**\n\n"
    msg += f"🔗 **Your Code:** `{user_stats['referral_code']}`\n\n"
    
    # Overall stats
    msg += f"📈 **Overall Performance:**\n"
    msg += f"• Total Referrals: {user_stats['total_referrals']}\n"
    msg += f"• Total Earnings: ${user_stats['total_earnings']:.2f}\n"
    msg += f"• Average per Referral: ${(user_stats['total_earnings'] / user_stats['total_referrals']):.2f if user_stats['total_referrals'] > 0 else 0}\n\n"
    
    # Recent performance
    msg += f"📅 **Last 30 Days:**\n"
    msg += f"• New Referrals: {recent_stats['recent_referrals']}\n"
    msg += f"• Recent Earnings: ${recent_stats['recent_earnings'] or 0:.2f}\n\n"
    
    # Recent activity
    if referral_activity:
        msg += f"🎯 **Recent Activity:**\n"
        for activity in referral_activity[:5]:
            date_str = activity['created_at'][:10] if activity['created_at'] else "Unknown"
            status_emoji = "✅" if activity['status'] == 'active' else "⏳"
            msg += f"{status_emoji} User {activity['referred_user_id']} - ${activity['commission_earned']:.2f} ({date_str})\n"
    else:
        msg += f"📝 **No referral activity yet.**\n"
        msg += f"Share your code to start earning commissions!"
    
    keyboard = [
        [InlineKeyboardButton("📤 Share Code", callback_data="referral_share_code")],
        [InlineKeyboardButton("💡 Tips & Tricks", callback_data="referral_tips")],
        [InlineKeyboardButton("⬅️ Back to Referrals", callback_data="referral_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_tips(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show referral tips and tricks"""
    query = update.callback_query
    
    msg = "💡 **Referral Tips & Tricks**\n\n"
    msg += "🎯 **Best Practices:**\n"
    msg += "• Share in relevant groups/chats\n"
    msg += "• Explain the benefits to friends\n"
    msg += "• Be genuine, not spammy\n"
    msg += "• Follow up but don't be pushy\n\n"
    msg += "📈 **Maximize Earnings:**\n"
    msg += "• Target active buyers\n"
    msg += "• Share during sales events\n"
    msg += "• Use personal messages\n"
    msg += "• Build trust first"
    
    keyboard = [[InlineKeyboardButton("⬅️ Back to Referrals", callback_data="referral_menu")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show admin referral statistics"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get overall referral statistics from users table
        c.execute("""
            SELECT 
                SUM(total_referrals) as total_referrals,
                COUNT(CASE WHEN total_referrals > 0 THEN 1 END) as active_referrers,
                SUM(total_earnings) as total_commissions,
                AVG(total_earnings) as avg_commission
            FROM users
        """)
        overall_stats = c.fetchone()
        
        # Get recent referral activity (users who joined recently)
        c.execute("""
            SELECT 
                COUNT(*) as recent_users,
                SUM(total_referrals) as recent_referrals
            FROM users 
            WHERE first_interaction >= datetime('now', '-30 days')
            AND total_referrals > 0
        """)
        recent_stats = c.fetchone()
        
        # Get top referrers from users table
        c.execute("""
            SELECT user_id, referral_code, total_referrals, total_earnings
            FROM users
            WHERE referral_code IS NOT NULL AND total_referrals > 0
            ORDER BY total_referrals DESC, total_earnings DESC
            LIMIT 10
        """)
        top_referrers = c.fetchall()
        
        # Get conversion stats (users with purchases)
        c.execute("""
            SELECT 
                COUNT(CASE WHEN total_purchases > 0 THEN 1 END) as users_with_purchases,
                COUNT(*) as total_users
            FROM users
        """)
        conversion_stats = c.fetchone()
        
    except Exception as e:
        logger.error(f"Error getting referral admin stats: {e}")
        await query.answer("Error loading statistics", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    msg = "📊 **Referral Program Statistics**\n\n"
    
    # Overall performance
    total_referrals = overall_stats['total_referrals'] or 0
    active_referrers = overall_stats['active_referrers'] or 0
    total_commissions = overall_stats['total_commissions'] or 0
    avg_commission = overall_stats['avg_commission'] or 0
    
    msg += f"🎯 **Overall Performance:**\n"
    msg += f"• Total Referrals: {total_referrals:,}\n"
    msg += f"• Active Referrers: {active_referrers:,}\n"
    msg += f"• Total Commissions Paid: ${total_commissions:.2f}\n"
    msg += f"• Average Commission: ${avg_commission:.2f}\n\n"
    
    # Recent activity
    recent_referrals = recent_stats['recent_referrals'] or 0
    recent_users = recent_stats['recent_users'] or 0
    msg += f"📅 **Last 30 Days:**\n"
    msg += f"• New Active Referrers: {recent_users:,}\n"
    msg += f"• New Referrals Made: {recent_referrals:,}\n\n"
    
    # Conversion metrics
    if conversion_stats['total_users'] > 0:
        conversion_rate = (conversion_stats['users_with_purchases'] / conversion_stats['total_users']) * 100
        msg += f"📈 **User Conversion Metrics:**\n"
        msg += f"• Total Users: {conversion_stats['total_users']:,}\n"
        msg += f"• Users with Purchases: {conversion_stats['users_with_purchases']:,}\n"
        msg += f"• Overall Conversion Rate: {conversion_rate:.1f}%\n\n"
    
    # Top performers
    if top_referrers:
        msg += f"🏆 **Top Referrers:**\n"
        for i, referrer in enumerate(top_referrers[:5], 1):
            msg += f"{i}. User {referrer['user_id']}: {referrer['total_referrals']} referrals (${referrer['total_earnings']:.2f})\n"
    
    keyboard = [
        [InlineKeyboardButton("👥 Top Referrers", callback_data="referral_admin_top_referrers")],
        [InlineKeyboardButton("⚙️ Program Settings", callback_data="referral_admin_settings")],
        [InlineKeyboardButton("📋 Export Report", callback_data="referral_admin_export")],
        [InlineKeyboardButton("⬅️ Back to Referral Admin", callback_data="referral_admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_admin_top_referrers(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show top referrers"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer("Top referrers coming soon!", show_alert=False)
    await query.edit_message_text("👥 Top referrers list coming soon!", 
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="referral_admin_menu")]]))

async def handle_referral_admin_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show referral program settings - FULLY FUNCTIONAL! 🚀"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get current settings
    settings = get_referral_settings()
    
    msg = "⚙️ **Referral Program Settings**\n\n"
    msg += f"🎯 **Current Configuration:**\n\n"
    msg += f"📊 **Program Status:** {'✅ ENABLED' if settings['program_enabled'] else '❌ DISABLED'}\n"
    msg += f"💰 **Referrer Percentage:** {settings['referrer_percentage']}%\n"
    msg += f"🎁 **New User Bonus:** {format_currency(settings['referred_bonus'])}\n"
    msg += f"🛒 **Minimum Purchase:** {format_currency(settings['min_purchase'])}\n\n"
    msg += f"💡 **How it works:**\n"
    msg += f"• New users get {format_currency(settings['referred_bonus'])} when using a referral code\n"
    msg += f"• Referrers get {settings['referrer_percentage']}% commission on first purchase\n"
    msg += f"• Minimum purchase of {format_currency(settings['min_purchase'])} required for commission"
    
    keyboard = [
        [InlineKeyboardButton(f"{'🔴 Disable' if settings['program_enabled'] else '🟢 Enable'} Program", 
                            callback_data=f"referral_admin_toggle|{'disable' if settings['program_enabled'] else 'enable'}")],
        [InlineKeyboardButton("📊 Set Referrer %", callback_data="referral_admin_set_percentage"),
         InlineKeyboardButton("🎁 Set User Bonus", callback_data="referral_admin_set_bonus")],
        [InlineKeyboardButton("🛒 Set Min Purchase", callback_data="referral_admin_set_min_purchase")],
        [InlineKeyboardButton("📈 View Statistics", callback_data="referral_admin_stats"),
         InlineKeyboardButton("🔄 Reset All Data", callback_data="referral_admin_reset_confirm")],
        [InlineKeyboardButton("⬅️ Back to Referral Admin", callback_data="referral_admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# 🚀 MODE: COMPLETE ALL ADMIN HANDLERS!

async def handle_referral_admin_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle referral program on/off"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        return await query.answer("Invalid action", show_alert=True)
    
    action = params[0]  # 'enable' or 'disable'
    new_status = 'true' if action == 'enable' else 'false'
    
    if set_referral_setting('program_enabled', new_status):
        status_text = "ENABLED" if action == 'enable' else "DISABLED"
        await query.answer(f"✅ Referral program {status_text}!", show_alert=True)
        # Refresh the settings menu
        await handle_referral_admin_settings(update, context)
    else:
        await query.answer("❌ Failed to update setting", show_alert=True)

async def handle_referral_admin_set_percentage(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set referrer percentage"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    context.user_data['state'] = 'awaiting_referral_percentage'
    
    msg = "📊 **Set Referrer Percentage**\n\n"
    msg += "Enter the percentage that referrers should earn from their referrals' first purchase.\n\n"
    msg += "**Examples:**\n"
    msg += "• `5` = 5%\n"
    msg += "• `10` = 10%\n"
    msg += "• `2.5` = 2.5%\n\n"
    msg += "**Current:** " + str(get_referral_settings()['referrer_percentage']) + "%\n\n"
    msg += "Type the new percentage (0-50):"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="referral_admin_settings")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_admin_set_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set new user bonus"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    context.user_data['state'] = 'awaiting_referral_bonus'
    
    msg = "🎁 **Set New User Bonus**\n\n"
    msg += "Enter the bonus amount that new users get when they use a referral code.\n\n"
    msg += "**Examples:**\n"
    msg += "• `2` = 2.00 EUR\n"
    msg += "• `5.50` = 5.50 EUR\n"
    msg += "• `10` = 10.00 EUR\n\n"
    msg += "**Current:** " + format_currency(get_referral_settings()['referred_bonus']) + "\n\n"
    msg += "Type the new bonus amount:"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="referral_admin_settings")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_admin_set_min_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set minimum purchase amount"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    context.user_data['state'] = 'awaiting_referral_min_purchase'
    
    msg = "🛒 **Set Minimum Purchase**\n\n"
    msg += "Enter the minimum purchase amount required for referrers to earn commission.\n\n"
    msg += "**Examples:**\n"
    msg += "• `10` = 10.00 EUR\n"
    msg += "• `25.50` = 25.50 EUR\n"
    msg += "• `5` = 5.00 EUR\n\n"
    msg += "**Current:** " + format_currency(get_referral_settings()['min_purchase']) + "\n\n"
    msg += "Type the new minimum purchase amount:"
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="referral_admin_settings")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_admin_reset_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm referral program reset"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "🔄 **Reset Referral Program**\n\n"
    msg += "⚠️ **WARNING:** This will permanently delete:\n"
    msg += "• All referral codes\n"
    msg += "• All referral relationships\n"
    msg += "• All referral statistics\n"
    msg += "• All earned commissions data\n\n"
    msg += "**This action CANNOT be undone!**\n\n"
    msg += "Are you absolutely sure you want to proceed?"
    
    keyboard = [
        [InlineKeyboardButton("🔴 YES, RESET EVERYTHING", callback_data="referral_admin_reset_confirmed")],
        [InlineKeyboardButton("❌ Cancel", callback_data="referral_admin_settings")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_admin_reset(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Actually reset the referral program"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Delete all referral data
        c.execute("DELETE FROM referrals")
        c.execute("DELETE FROM referral_codes")
        c.execute("UPDATE users SET referral_code = NULL, total_referrals = 0, total_earnings = 0")
        
        conn.commit()
        
        msg = "✅ **Referral Program Reset Complete**\n\n"
        msg += "All referral data has been permanently deleted:\n"
        msg += "• Referral codes: Cleared\n"
        msg += "• Referral relationships: Deleted\n"
        msg += "• Statistics: Reset to zero\n"
        msg += "• User earnings: Reset to zero\n\n"
        msg += "The referral program is ready for a fresh start!"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Settings", callback_data="referral_admin_settings")]]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        logger.info(f"Admin {query.from_user.id} reset the entire referral program")
        
    except Exception as e:
        logger.error(f"Error resetting referral program: {e}")
        await query.answer("❌ Error resetting program", show_alert=True)
    finally:
        if conn:
            conn.close()

# 🚀 MESSAGE HANDLERS FOR ADMIN SETTINGS

async def handle_referral_percentage_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle referrer percentage input"""
    if context.user_data.get('state') != 'awaiting_referral_percentage':
        return
    
    user_id = update.effective_user.id
    if not is_primary_admin(user_id):
        return
    
    try:
        percentage = float(update.message.text.strip())
        if percentage < 0 or percentage > 50:
            await update.message.reply_text(
                "❌ **Invalid Percentage**\n\nPlease enter a percentage between 0 and 50.",
                parse_mode='Markdown'
            )
            return
        
        if set_referral_setting('referrer_percentage', str(percentage)):
            context.user_data.pop('state', None)
            
            # 🚀 MODE: DUMMY-PROOF NAVIGATION WITH BACK BUTTON!
            msg = f"✅ **Referrer Percentage Updated**\n\n"
            msg += f"Referrers will now earn **{percentage}%** commission on their referrals' first purchase.\n\n"
            msg += f"💡 **What's Next?**\n"
            msg += f"Continue setting up other referral settings or go back to manage more options."
            
            keyboard = [
                [InlineKeyboardButton("⚙️ Back to Referral Settings", callback_data="referral_admin_settings")],
                [InlineKeyboardButton("📊 View Statistics", callback_data="referral_admin_stats")],
                [InlineKeyboardButton("⬅️ Back to User Management", callback_data="admin_users_menu")]
            ]
            
            await update.message.reply_text(
                msg, 
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("❌ **Error**\n\nFailed to update percentage. Please try again.", parse_mode='Markdown')
    
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid Input**\n\nPlease enter a valid number (e.g., 5, 10, 2.5).",
            parse_mode='Markdown'
        )

async def handle_referral_bonus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new user bonus input"""
    if context.user_data.get('state') != 'awaiting_referral_bonus':
        return
    
    user_id = update.effective_user.id
    if not is_primary_admin(user_id):
        return
    
    try:
        bonus = float(update.message.text.strip())
        if bonus < 0 or bonus > 100:
            await update.message.reply_text(
                "❌ **Invalid Amount**\n\nPlease enter an amount between 0 and 100 EUR.",
                parse_mode='Markdown'
            )
            return
        
        if set_referral_setting('referred_bonus', str(bonus)):
            context.user_data.pop('state', None)
            
            # 🚀 MODE: DUMMY-PROOF NAVIGATION WITH BACK BUTTON!
            msg = f"✅ **New User Bonus Updated**\n\n"
            msg += f"New users will now receive **{format_currency(Decimal(str(bonus)))}** when using a referral code.\n\n"
            msg += f"💡 **What's Next?**\n"
            msg += f"Continue setting up other referral settings or go back to manage more options."
            
            keyboard = [
                [InlineKeyboardButton("⚙️ Back to Referral Settings", callback_data="referral_admin_settings")],
                [InlineKeyboardButton("📊 View Statistics", callback_data="referral_admin_stats")],
                [InlineKeyboardButton("⬅️ Back to User Management", callback_data="admin_users_menu")]
            ]
            
            await update.message.reply_text(
                msg, 
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("❌ **Error**\n\nFailed to update bonus. Please try again.", parse_mode='Markdown')
    
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid Input**\n\nPlease enter a valid amount (e.g., 2, 5.50, 10).",
            parse_mode='Markdown'
        )

async def handle_referral_min_purchase_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle minimum purchase input"""
    if context.user_data.get('state') != 'awaiting_referral_min_purchase':
        return
    
    user_id = update.effective_user.id
    if not is_primary_admin(user_id):
        return
    
    try:
        min_purchase = float(update.message.text.strip())
        if min_purchase < 0 or min_purchase > 1000:
            await update.message.reply_text(
                "❌ **Invalid Amount**\n\nPlease enter an amount between 0 and 1000 EUR.",
                parse_mode='Markdown'
            )
            return
        
        if set_referral_setting('min_purchase', str(min_purchase)):
            context.user_data.pop('state', None)
            
            # 🚀 MODE: DUMMY-PROOF NAVIGATION WITH BACK BUTTON!
            msg = f"✅ **Minimum Purchase Updated**\n\n"
            msg += f"Referrers will now earn commission only when their referrals spend at least **{format_currency(Decimal(str(min_purchase)))}**.\n\n"
            msg += f"💡 **What's Next?**\n"
            msg += f"Continue setting up other referral settings or go back to manage more options."
            
            keyboard = [
                [InlineKeyboardButton("⚙️ Back to Referral Settings", callback_data="referral_admin_settings")],
                [InlineKeyboardButton("📊 View Statistics", callback_data="referral_admin_stats")],
                [InlineKeyboardButton("⬅️ Back to User Management", callback_data="admin_users_menu")]
            ]
            
            await update.message.reply_text(
                msg, 
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("❌ **Error**\n\nFailed to update minimum purchase. Please try again.", parse_mode='Markdown')
    
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid Input**\n\nPlease enter a valid amount (e.g., 10, 25.50, 5).",
            parse_mode='Markdown'
        )

async def handle_referral_code_payment(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle referral code entry from payment menu"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check if referral program is enabled
    settings = get_referral_settings()
    if not settings['program_enabled']:
        await query.answer("❌ Referral program is currently disabled.", show_alert=True)
        return
    
    # Check if user already used a referral code
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT id FROM referrals WHERE referred_user_id = %s", (user_id,))
        if c.fetchone():
            await query.answer("❌ You have already used a referral code.", show_alert=True)
            return
    except Exception as e:
        logger.error(f"Error checking referral usage: {e}")
        await query.answer("❌ Error checking referral status.", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    # Set state to await referral code input
    context.user_data['state'] = 'awaiting_referral_code_payment'
    
    msg = "🎁 **Enter Referral Code** 🎁\n\n"
    msg += "💡 **How it works:**\n"
    msg += f"• Enter a valid referral code to get **{format_currency(settings['referred_bonus'])}** bonus\n"
    msg += f"• The code must be from an existing user\n"
    msg += f"• You can only use one referral code per account\n\n"
    msg += "📝 **Please type the referral code:**"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_referral_code")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_referral_code_payment_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle referral code input message from payment menu"""
    if context.user_data.get('state') != 'awaiting_referral_code_payment':
        return
    
    user_id = update.effective_user.id
    referral_code = update.message.text.strip().upper()
    
    if len(referral_code) < 3 or len(referral_code) > 10:
        await update.message.reply_text(
            "❌ **Invalid Code Format**\n\nReferral codes are usually 3-10 characters long.",
            parse_mode='Markdown'
        )
        return
    
    # Apply the referral code
    result = await apply_referral_code(user_id, referral_code)
    
    context.user_data.pop('state', None)
    
    if result['success']:
        msg = f"🎉 **Referral Code Applied Successfully!** 🎉\n\n"
        msg += f"✅ **Bonus Added:** {format_currency(result['bonus_amount'])}\n"
        msg += f"💰 **Your New Balance:** {format_currency(result['new_balance'])}\n\n"
        msg += f"🙏 **Thank you for using referral code:** `{referral_code}`\n"
        msg += f"👤 **Referrer:** {result.get('referrer_name', 'Unknown')}"
        
        keyboard = [
            [InlineKeyboardButton("💳 Continue to Payment", callback_data="back_to_payment")],
            [InlineKeyboardButton("👤 View Profile", callback_data="profile")]
        ]
        
        await update.message.reply_text(
            msg, 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        error_messages = {
            'referral_already_used': "❌ You have already used a referral code.",
            'code_not_found': f"❌ Referral code `{referral_code}` not found or inactive.",
            'self_referral': "❌ You cannot use your own referral code.",
            'database_error': "❌ Database error occurred. Please try again."
        }
        
        error_msg = error_messages.get(result.get('error'), result.get('message', 'Unknown error'))
        
        keyboard = [
            [InlineKeyboardButton("🔄 Try Again", callback_data="referral_code")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_referral_code")]
        ]
        
        await update.message.reply_text(
            f"**Referral Code Error**\n\n{error_msg}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def handle_cancel_referral_code(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle canceling referral code entry"""
    query = update.callback_query
    context.user_data.pop('state', None)
    
    await query.edit_message_text(
        "❌ **Referral Code Cancelled**\n\nYou can try again later from the payment menu.",
        parse_mode='Markdown'
    )

# --- END OF FILE referral_system.py ---
