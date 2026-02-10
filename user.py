# Telegram Bot User Module
# User interaction handlers
# !!!! DEPLOY MARKER: 2025-12-08 00:05 UTC - FINAL FIX !!!!
print("=" * 80)
# Module loaded
print("=" * 80)
import sqlite3
import time
import logging
import asyncio
import os # Import os for path joining
from datetime import datetime, timezone
from collections import defaultdict, Counter
from decimal import Decimal, ROUND_DOWN # <<< Added ROUND_DOWN

# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram import helpers
import telegram.error as telegram_error
# -------------------------

# Import from utils
from utils import (
    CITIES, DISTRICTS, PRODUCT_TYPES, THEMES, LANGUAGES, BOT_MEDIA, ADMIN_ID, BASKET_TIMEOUT, MIN_DEPOSIT_EUR,
    WEBHOOK_URL, # <<< Added WEBHOOK_URL
    format_currency, get_progress_bar, send_message_with_retry, format_discount_value,
    clear_expired_basket, fetch_last_purchases, get_user_status, fetch_reviews,
    # SOL-only payment system
    get_db_connection, MEDIA_DIR, # Import helper and MEDIA_DIR
    DEFAULT_PRODUCT_EMOJI, # Import default emoji
    load_active_welcome_message, # <<< Import welcome message loader (though we'll modify its usage)
    DEFAULT_WELCOME_MESSAGE, # <<< Import default welcome message fallback
    _get_lang_data, # <<< IMPORT THE HELPER FROM UTILS >>>
    _unreserve_basket_items, # <<< IMPORT UNRESERVE HELPER >>>
    is_primary_admin, is_secondary_admin, is_any_admin, # Admin helper functions
    SECONDARY_ADMIN_IDS, is_user_banned, # Import ban check helper
    update_user_broadcast_status, # Import broadcast tracking helper
    is_human_verification_enabled, is_user_verified, set_user_verified, generate_verification_code, generate_verification_image, # Human verification
    get_verification_attempt_limit, get_user_verification_attempts, increment_verification_attempts, 
    reset_verification_attempts, block_user_for_failed_verification, # Verification attempts
    is_language_selection_enabled, get_language_prompt_placement, VERIFICATION_TEXTS, # Language selection
    get_translation # Interface translations
)
import json # <<< Make sure json is imported
import payment # <<< Make sure payment module is imported

# Import worker management
try:
    from worker_management import is_worker
    from worker_ui import handle_worker_dashboard
except ImportError:
    def is_worker(user_id): return False
    async def handle_worker_dashboard(update, context): pass

# --- Import Reseller Helper ---
try:
    from reseller_management import get_reseller_discount
except ImportError:
    logger_dummy_reseller = logging.getLogger(__name__ + "_dummy_reseller")
    logger_dummy_reseller.error("Could not import get_reseller_discount from reseller_management.py. Reseller discounts will not work.")
    # Define a dummy function that always returns zero discount
    def get_reseller_discount(user_id: int, product_type: str) -> Decimal:
        return Decimal('0.0')
# -----------------------------


# Logging setup
logger = logging.getLogger(__name__)

# Emojis (Defaults/Placeholders)
EMOJI_CITY = "🏙️"
EMOJI_DISTRICT = "🏘️"
# EMOJI_PRODUCT = "💎" # No longer primary source
EMOJI_HERB = "🌿" # Keep for potential specific logic if needed
EMOJI_PRICE = "💰"
EMOJI_QUANTITY = "🔢"
EMOJI_BASKET = "🛒"
EMOJI_PROFILE = "👤"
EMOJI_REFILL = "💸"
EMOJI_REVIEW = "📝"
EMOJI_PRICELIST = "📋"
EMOJI_LANG = "🌐"
EMOJI_BACK = "⬅️"
EMOJI_HOME = "🏠"
EMOJI_SHOP = "🛍️"
EMOJI_DISCOUNT = "🏷️"
EMOJI_PAY_NOW = "💳" # <<< ADDED Emoji for Pay Now

# --- NEW: Define Supported Crypto Assets with Network Specificity ---
# Key: NOWPayments API currency code (lowercase)
# Value: User-friendly display name for the button
# SOL-only payment system
SUPPORTED_CRYPTO = {
    'btc': 'BTC',
    'ltc': 'LTC',
    'eth': 'ETH',
    'sol': 'SOL',
    'usdttrc20': 'USDT (TRC20)', # TRC20 USDT - commonly supported
    'usdterc20': 'USDT (ERC20)', # ERC20 USDT - commonly supported
    'ton': 'TON',
    # Removed potentially problematic currencies:
    # 'usdtbsc': 'USDT (BEP20)' - May not be supported or has different code
    # 'usdtsol': 'USDT (SOL)' - May not be supported or has different code  
    # 'usdctrc20': 'USDC (TRC20)' - May not be supported or has different code
    # 'usdcerc20': 'USDC (ERC20)' - May not be supported or has different code
    # 'usdcsol': 'USDC (SOL)' - May not be supported or has different code
    # Add other verified currencies here as needed
    # SOL-only payment system
}
# --------------------------------------------------------------------


# --- Helper Function to Build Start Menu ---
def _build_start_menu_content(user_id: int, username: str, lang_data: dict, context: ContextTypes.DEFAULT_TYPE, user_obj=None) -> tuple[str, InlineKeyboardMarkup]:
    """Builds the text and keyboard for the start menu using provided lang_data."""
    logger.info(f"=" * 80)
    logger.info(f"Building start menu content for user {user_id}")
    logger.info(f"=" * 80)
    logger.debug(f"_build_start_menu_content: Building menu for user {user_id} with lang_data.")

    balance, purchases, basket_count = Decimal('0.0'), 0, 0
    conn = None
    active_template_name_from_db = None # Variable to store DB setting

    # --- Initial Data Fetch ---
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Get user stats
        c.execute("SELECT balance, total_purchases FROM users WHERE user_id = %s", (user_id,))
        result = c.fetchone()
        if result:
            balance = Decimal(str(result['balance']))
            purchases = result['total_purchases']

        # Get active welcome template name setting
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = %s", ("active_welcome_message_name",)) # LINE 89
        setting_row = c.fetchone()
        if setting_row and setting_row['setting_value']: # Check if value is not None/empty
            active_template_name_from_db = setting_row['setting_value']
            logger.info(f"Active welcome template name from settings: '{active_template_name_from_db}'")
        else:
            active_template_name_from_db = "default" # Fallback to 'default' if setting is missing/empty
            logger.info("Active welcome message name not found in settings, falling back to 'default'.")

        # Call synchronous clear_expired_basket (no await needed)
        clear_expired_basket(context, user_id) # Assuming clear_expired_basket is synchronous
        basket = context.user_data.get("basket", [])
        basket_count = len(basket)
        if not basket: context.user_data.pop('applied_discount', None)

    except sqlite3.Error as e:
        logger.error(f"Database error fetching initial data for start menu build (user {user_id}): {e}", exc_info=True)
        active_template_name_from_db = "default" # Fallback on DB error fetching setting
    finally:
        if conn: conn.close()

    # --- Determine which template text to use ---
    welcome_template_to_use = None # Start with None

    if active_template_name_from_db: # Only try if we have a name (even if it's 'default')
        conn_load = None
        try:
            conn_load = get_db_connection()
            c_load = conn_load.cursor()
            c_load.execute("SELECT template_text FROM welcome_messages WHERE name = %s", (active_template_name_from_db,))
            template_row = c_load.fetchone()
            if template_row:
                welcome_template_to_use = template_row['template_text']
                logger.info(f"Using welcome message template from DB: '{active_template_name_from_db}'")
            else:
                logger.warning(f"Active template '{active_template_name_from_db}' set in DB but not found in templates table. Will fall back.")
                # welcome_template_to_use remains None
        except sqlite3.Error as e:
            logger.error(f"DB error loading specific welcome template '{active_template_name_from_db}': {e}")
            # welcome_template_to_use remains None
        finally:
            if conn_load: conn_load.close()

    # Fallback logic if DB load failed or no active name was determined initially
    if welcome_template_to_use is None:
        logger.warning("Falling back to default welcome message defined in LANGUAGES.")
        welcome_template_to_use = lang_data.get('welcome', DEFAULT_WELCOME_MESSAGE) # Use language file default OR hardcoded default

    # --- Check for Custom Mini App Welcome Text ---
    # If in Mini App Only mode and custom text exists, use it instead
    ui_mode_for_welcome = get_bot_setting("ui_mode", "bot")
    custom_miniapp_welcome = get_bot_setting("miniapp_welcome_text", None)
    
    if ui_mode_for_welcome == "miniapp" and custom_miniapp_welcome:
        logger.info(f"📱 Using custom Mini App welcome text for user {user_id}")
        welcome_template_to_use = custom_miniapp_welcome

    # --- Format the chosen template ---
    status = get_user_status(purchases)
    balance_str = format_currency(balance)
    progress_bar_str = get_progress_bar(purchases)

    try:
        # Format using the raw username and placeholders
        full_welcome = welcome_template_to_use.format(
            username=username,
            user_name=username,  # Add user_name for template compatibility
            user_id=user_id,     # Add user_id for template compatibility
            bot_name="Bot",      # Add bot_name for template compatibility
            status=status,
            progress_bar=progress_bar_str,
            balance_str=balance_str,
            purchases=purchases,
            basket_count=basket_count
        )
    except KeyError as e:
        logger.error(f"Placeholder error formatting welcome message template. Missing key: {e}. Template: '{welcome_template_to_use[:100]}...' Using fallback.")
        full_welcome = f"👋 Welcome, {username}!\n\n💰 Balance: {balance_str} EUR"
    except Exception as format_e:
        logger.error(f"Unexpected error formatting welcome message: {format_e}. Template: '{welcome_template_to_use[:100]}...' Using fallback.")
        full_welcome = f"👋 Welcome, {username}!\n\n💰 Balance: {balance_str} EUR"

    # --- Build Keyboard ---
    shop_button_text = lang_data.get("shop_button", "Shop")
    profile_button_text = lang_data.get("profile_button", "Profile")
    top_up_button_text = lang_data.get("top_up_button", "Top Up")
    reviews_button_text = lang_data.get("reviews_button", "Reviews")
    price_list_button_text = lang_data.get("price_list_button", "Price List")
    language_button_text = lang_data.get("language_button", "Language")
    admin_button_text = lang_data.get("admin_button", "🔧 Admin Panel")
    
    # Check if Daily Rewards should be shown
    from utils import is_daily_rewards_enabled, get_bot_setting
    show_daily_rewards = is_daily_rewards_enabled()
    logger.info(f"🎁 Daily Rewards enabled check: {show_daily_rewards} for user {user_id}")
    
    # Check UI mode setting (bot or miniapp)
    ui_mode = get_bot_setting("ui_mode", "bot")
    logger.info(f"📱 UI Mode: {ui_mode} for user {user_id}")
    
    # Default keyboard layout
    # Add timestamp for cache busting
    import time
    webapp_url = f"{WEBHOOK_URL.rstrip('/')}/webapp_fresh/app.html?v=3.0&t={int(time.time())}"
    default_keyboard = [
        [InlineKeyboardButton(text="🌐 Open Shop App", web_app=WebAppInfo(url=webapp_url))],
    ]
    
    # Only add old Shop button if NOT in Mini App Only mode
    if ui_mode == "bot":
        default_keyboard.append([InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop")])
    
    # Conditionally add Daily Rewards button
    if show_daily_rewards:
        logger.info(f"🎁 Adding Daily Rewards button to default keyboard for user {user_id}")
        default_keyboard.append([InlineKeyboardButton("🎁 Daily Rewards", callback_data="daily_rewards_menu")])
    
    default_keyboard.extend([
        [InlineKeyboardButton(f"{EMOJI_PROFILE} {profile_button_text}", callback_data="profile"),
         InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill")],
        [InlineKeyboardButton(f"{EMOJI_REVIEW} {reviews_button_text}", callback_data="reviews"),
         InlineKeyboardButton(f"{EMOJI_PRICELIST} {price_list_button_text}", callback_data="price_list"),
         InlineKeyboardButton(f"{EMOJI_LANG} {language_button_text}", callback_data="language")]
    ])
    
    # Only apply custom layout if NO preset theme is active
    custom_header_message = None
    try:
        from marketing_promotions import apply_custom_layout_to_keyboard, get_custom_layout, process_dynamic_variables, get_active_ui_theme
        
        # Check if preset theme is active
        active_theme = get_active_ui_theme()
        if active_theme and active_theme.get('theme_name') != 'custom':
            # Preset theme is active - use default layout for that theme
            logger.info(f"Preset theme '{active_theme.get('theme_name')}' is active - using default layout")
            # For classic theme, use the 6-button layout we defined
            if active_theme.get('theme_name') == 'classic':
                logger.info(f"🎨 CLASSIC THEME: Building keyboard for user {user_id}, ui_mode={ui_mode}")
                import time
                webapp_url = f"{WEBHOOK_URL.rstrip('/')}/webapp_fresh/app.html?v=3.0&t={int(time.time())}"
                keyboard = [
                    [InlineKeyboardButton(text="🌐 Open Shop App", web_app=WebAppInfo(url=webapp_url))],
                ]
                
                # Only add old Shop button if NOT in Mini App Only mode
                if ui_mode == "bot":
                    logger.info(f"🎨 CLASSIC: ui_mode is 'bot', adding Shop button")
                    keyboard.append([InlineKeyboardButton("🛍️ Shop", callback_data="shop")])
                else:
                    logger.info(f"🎨 CLASSIC: ui_mode is '{ui_mode}', SKIPPING Shop button")
                
                # Conditionally add Daily Rewards button for classic theme
                if show_daily_rewards:
                    keyboard.append([InlineKeyboardButton("🎁 Daily Rewards", callback_data="daily_rewards_menu")])
                
                # Add Worker Dashboard button for classic theme if user is a worker
                try:
                    from worker_management import is_worker
                    if is_worker(user_id):
                        keyboard.append([InlineKeyboardButton("👷 Worker Dashboard", callback_data="worker_menu")])
                except:
                    pass
                
                keyboard.extend([
                    [InlineKeyboardButton("👤 Profile", callback_data="profile"), 
                     InlineKeyboardButton("💳 Top Up", callback_data="refill")],
                    [InlineKeyboardButton("📝 Reviews", callback_data="reviews"),
                     InlineKeyboardButton("📋 Price List", callback_data="price_list"),
                     InlineKeyboardButton("🌐 Language", callback_data="language")]
                ])
        else:
            # No preset theme - apply custom layout
            # Get user language from context or database
            user_language = 'en'  # Default
            try:
                # Try to get language from database
                c.execute("SELECT language FROM users WHERE user_id = %s", (user_id,))
                lang_result = c.fetchone()
                if lang_result and lang_result['language']:
                    user_language = lang_result['language']
            except Exception as lang_e:
                logger.debug(f"Could not get user language: {lang_e}")
            
            keyboard = apply_custom_layout_to_keyboard('start_menu', default_keyboard, user_language)
            logger.info(f"Applied custom start menu layout for user {user_id} (language: {user_language})")
        
        # Get custom header message only if using custom layout
        custom_layout = None
        if active_theme and active_theme.get('theme_name') == 'custom':
            custom_layout = get_custom_layout('start_menu')
            
        if custom_layout:
            # Get user data for variable processing
            user_data = {
                'user_mention': username,
                'user_first_name': user_obj.first_name if user_obj else username,
                'user_id': str(user_id),
                'balance': f"{balance:.2f}",
                'total_purchases': str(purchases),
                'vip_level': 'Standard',  # Default, can be enhanced later
                'basket_count': str(basket_count)
            }
            
            # Get header message from database
            try:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT header_message FROM bot_menu_layouts WHERE menu_name = 'start_menu' AND is_active = TRUE LIMIT 1")
                result = c.fetchone()
                if result and result['header_message']:
                    custom_header_message = process_dynamic_variables(result['header_message'], user_data)
                    logger.info(f"Loaded custom header message for user {user_id}: {result['header_message'][:50]}...")
                else:
                    logger.info(f"No custom header message found for user {user_id}")
                conn.close()
            except Exception as header_error:
                logger.error(f"Error loading custom header message for user {user_id}: {header_error}")
                if conn:
                    conn.close()
            
    except ImportError:
        keyboard = default_keyboard
        logger.debug("Marketing promotions module not available, using default keyboard")
    except Exception as e:
        keyboard = default_keyboard
        logger.warning(f"Error applying custom layout for user {user_id}: {e}")
    
    # Add Worker Dashboard button AFTER custom layout is applied
    try:
        from worker_management import is_worker
        if is_worker(user_id):
            logger.info(f"👷 User {user_id} is a worker, adding Worker Dashboard button to final keyboard")
            # Insert worker button after Daily Rewards (if exists) or after Shop button
            insert_position = 2 if show_daily_rewards else 1
            keyboard.insert(insert_position, [InlineKeyboardButton("👷 Worker Dashboard", callback_data="worker_menu")])
    except ImportError:
        pass  # Worker system not available
    except Exception as e:
        logger.error(f"Error adding worker button for user {user_id}: {e}")
    
    # Add admin button if user is admin
    if is_primary_admin(user_id):
        keyboard.insert(0, [InlineKeyboardButton(admin_button_text, callback_data="admin_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    # Use custom header message if available, otherwise use default
    if custom_header_message:
        logger.info(f"Using custom header message for user {user_id}")
        return custom_header_message, reply_markup
    else:
        logger.info(f"Using default welcome message for user {user_id}")
    return full_welcome, reply_markup


# --- User Command Handlers ---
# --- Human Verification System ---
async def handle_human_verification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show human verification challenge"""
    user_id = update.effective_user.id
    
    # Check if user has exceeded attempt limit
    current_attempts = get_user_verification_attempts(user_id)
    max_attempts = get_verification_attempt_limit()

    if current_attempts >= max_attempts:
        # User has exceeded attempts, block them
        block_user_for_failed_verification(user_id)
        
        msg = f"🚫 **Access Blocked**\n\n"
        msg += f"You have exceeded the maximum number of verification attempts ({max_attempts}).\n\n"
        msg += f"Your access to this bot has been blocked for security reasons.\n\n"
        msg += f"Contact support if you believe this is an error."
        
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode='Markdown')
        else:
            await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode='Markdown')
        return
    
    # Generate verification code
    verification_code = generate_verification_code()
    context.user_data['verification_code'] = verification_code
    context.user_data['state'] = 'awaiting_verification'
    
    # Show attempt info
    attempts_left = max_attempts - current_attempts
    
    # Try to generate professional verification image
    try:
        verification_image = generate_verification_image(verification_code)
        
        # Get user's language preference
        user_language = context.user_data.get('lang', 'en')
        
        # Show verification text in all 3 languages if placement is AFTER
        placement = get_language_prompt_placement()
        if placement == 'after':
            # User hasn't selected language yet, show all 3 languages with proper spacing
            msg = "🤖 **Prove you're human: reply with the text in the image.**\n\n"
            msg += "🤖 **Įrodykite, kad esate žmogus: atsakykite tekstu pavaizduotame paveikslėlyje.**\n\n"
            msg += "🤖 **Докажите, что вы человек: ответьте текстом на изображении.**"
        else:
            # Language already selected, use user's language
            msg = VERIFICATION_TEXTS.get(user_language, VERIFICATION_TEXTS['en'])
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="verification_cancel")]]
        
        if update.callback_query:
            # For callback queries, we need to send a new message with photo
            await update.callback_query.message.reply_photo(
                photo=verification_image,
                caption=msg,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            # Delete the old message
            try:
                await update.callback_query.message.delete()
            except:
                pass
        else:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=verification_image,
                caption=msg,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            
    except Exception as e:
        logger.error(f"Failed to generate/send verification image: {e}")
        # Fallback to text-based verification
        user_language = context.user_data.get('lang', 'en')
        
        # Show fallback text in all 3 languages if placement is AFTER
        placement = get_language_prompt_placement()
        if placement == 'after':
            # User hasn't selected language yet, show all 3 languages with proper spacing
            msg = f"🤖 **Prove you're human: reply with the text below.**\n\n"
            msg += f"🤖 **Įrodykite, kad esate žmogus: atsakykite žemiau esančiu tekstu.**\n\n"
            msg += f"🤖 **Докажите, что вы человек: ответьте текстом ниже.**\n\n"
            msg += f"**Code / Kodas / Код:** `{verification_code}`"
        else:
            # Language already selected, use user's language
            fallback_texts = {
                'en': f"🤖 **Prove you're human: reply with the text below.**\n\n**Code:** `{verification_code}`",
                'lt': f"🤖 **Įrodykite, kad esate žmogus: atsakykite žemiau esančiu tekstu.**\n\n**Kodas:** `{verification_code}`",
                'ru': f"🤖 **Докажите, что вы человек: ответьте текстом ниже.**\n\n**Код:** `{verification_code}`"
            }
            msg = fallback_texts.get(user_language, fallback_texts['en'])
        
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="verification_cancel")]]
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                msg, 
                reply_markup=InlineKeyboardMarkup(keyboard), 
                parse_mode='Markdown'
            )
        else:
            await send_message_with_retry(
                context.bot, 
                update.effective_chat.id, 
                msg, 
                reply_markup=InlineKeyboardMarkup(keyboard), 
                parse_mode='Markdown'
            )

async def handle_verification_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user's verification code input"""
    if context.user_data.get('state') != 'awaiting_verification':
        return
    
    user_id = update.effective_user.id
    user_input = update.message.text.strip().upper()
    correct_code = context.user_data.get('verification_code', '').upper()
    
    if user_input == correct_code:
        # Verification successful
        logger.info(f"✅ User {user_id} entered correct verification code: {correct_code}")
        
        # Set verification status directly
        logger.info(f"Setting verification status for user {user_id}")
        
        # Also use the utility function as backup
        verification_result = set_user_verified(user_id, True)
        reset_verification_attempts(user_id)  # Reset attempts on success
        
        logger.info(f"🔍 Verification status set for user {user_id}: {verification_result}")
        
        # Double-check verification status
        is_verified = is_user_verified(user_id)
        logger.info(f"🔍 Double-check: User {user_id} verification status: {is_verified}")
        
        context.user_data.pop('verification_code', None)
        context.user_data.pop('state', None)
        
        # Get user's language and show success message in their language
        user_language = context.user_data.get('lang', 'en')
        success_message = get_translation('verification_successful', user_language)
        
        await update.message.reply_text(
            success_message,
            parse_mode='Markdown'
        )
        
        # Wait a moment for database consistency
        import asyncio
        await asyncio.sleep(0.5)
        
        # Now show the start menu
        await start(update, context)
    else:
        # Verification failed - increment attempts
        new_attempt_count = increment_verification_attempts(user_id)
        max_attempts = get_verification_attempt_limit()
        attempts_left = max_attempts - new_attempt_count
        
        if attempts_left <= 0:
            # User has exceeded attempts, block them
            block_user_for_failed_verification(user_id)
            context.user_data.pop('verification_code', None)
            context.user_data.pop('state', None)
            
            await update.message.reply_text(
                f"🚫 **Access Blocked**\n\n"
                f"You have exceeded the maximum number of verification attempts ({max_attempts}).\n\n"
                f"Your access to this bot has been blocked for security reasons.\n\n"
                f"Contact support if you believe this is an error.",
                parse_mode='Markdown'
            )
        else:
            # Still have attempts left
            await update.message.reply_text(
                f"❌ **Incorrect code!**\n\n"
                f"🔢 **Attempts remaining:** {attempts_left}/{max_attempts}\n"
                f"⚠️ **Warning:** You will be blocked after {max_attempts} failed attempts.\n\n"
                f"Type `/start` to get a new code or try again.",
                parse_mode='Markdown'
            )

async def handle_verification_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle verification cancellation"""
    query = update.callback_query
    context.user_data.pop('verification_code', None)
    context.user_data.pop('state', None)
    
    # Delete the verification message (which might be a photo) and send a new text message
    try:
        await query.message.delete()
    except:
        pass  # Ignore if deletion fails
    
    # Send a new message instead of editing
    await query.message.reply_text(
        "❌ **Verification Cancelled**\n\nType /start to try again.",
        parse_mode='Markdown'
    )
    
    # Answer the callback query to remove the loading state
    await query.answer()

async def handle_language_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show language selection prompt"""
    msg = "🌍 **Choose language / Выберите язык / Pasirinkite kalbą**"
    
    keyboard = [
        [InlineKeyboardButton("English 🇬🇧", callback_data="select_language|en")],
        [InlineKeyboardButton("Русский 🇷🇺", callback_data="select_language|ru")],
        [InlineKeyboardButton("Lietuvių 🇱🇹", callback_data="select_language|lt")]
    ]
    
    # Handle both callback queries and regular messages
    if update.callback_query:
        await update.callback_query.edit_message_text(
            msg, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode='Markdown'
        )
    else:
        await send_message_with_retry(
            context.bot, 
            update.effective_chat.id, 
            msg, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode='Markdown'
        )

async def handle_select_language(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle language selection"""
    query = update.callback_query
    if not params:
        await query.answer("Invalid language selection", show_alert=True)
        return
    
    language = params[0]  # 'en', 'ru', or 'lt'
    user_id = update.effective_user.id
    
    # Save language preference
    context.user_data['lang'] = language
    context.user_data['language_selection_completed'] = True  # Mark as completed
    
    # Update user's language in database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE users SET language = %s WHERE user_id = %s", (language, user_id))
        conn.commit()
        logger.info(f"✅ User {user_id} language set to {language}")
    except Exception as e:
        logger.error(f"❌ Error setting user language: {e}")
    finally:
        if conn:
            conn.close()
    
    # Show confirmation
    confirmations = {
        'en': "✅ **Language set to English**",
        'ru': "✅ **Язык установлен на русский**",
        'lt': "✅ **Kalba nustatyta į lietuvių**"
    }
    
    await query.answer(confirmations.get(language, "✅ Language set"), show_alert=False)
    
    # Continue to next step based on placement and verification status
    placement = get_language_prompt_placement()
    
    if placement == 'before':
        # Language was shown before verification, now check if verification is needed
        if is_human_verification_enabled():
            if not (is_primary_admin(user_id) or is_secondary_admin(user_id)):
                if not is_user_verified(user_id):
                    logger.info(f"🔍 Language selected, now showing verification for user {user_id}")
                    return await handle_human_verification(update, context)
    elif placement == 'after':
        # Language was shown after verification, verification should already be done
        # Just continue to main menu
        pass
    
    # Show main menu
    logger.info(f"🌍 Language selection complete, showing main menu for user {user_id}")
    await start(update, context)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command and the initial welcome message."""
    logger.info("🚀 Processing /start command")
    user = update.effective_user
    chat_id = update.effective_chat.id
    is_callback = update.callback_query is not None
    user_id = user.id
    
    # PROPER USERNAME FETCHING - Priority: username > first_name > last_name > fallback
    display_name = None
    db_username = None  # Actual Telegram @username for database
    
    # Extract user info from update
    logger.info(f"User info: {user.first_name}, @{user.username}")
    
    # First, get from Update object as baseline
    db_username = user.username  # Actual @username from Telegram
    display_name = user.first_name or user.username or user.last_name or f"User_{user_id}"
    
    # Filter out weird characters (dots, underscores, etc.) from display name
    # Strategy: Remove all common punctuation. If empty or too short, fallback.
    import re
    if display_name:
        # Remove dots, underscores, hyphens, spaces
        clean_name = re.sub(r'[._\-\s]', '', display_name)
        if len(clean_name) < 1: # If name was ONLY punctuation
            logger.warning(f"⚠️ Display name '{display_name}' is mostly punctuation, using fallback")
            display_name = user.username or f"User_{user_id}"
        elif len(display_name) > 3 and all(c in '._-' for c in display_name): # Double check specific chars
             logger.warning(f"⚠️ Display name '{display_name}' is just dots/dashes, using fallback")
             display_name = user.username or f"User_{user_id}"
    
    # Then try to get more complete info from bot.get_chat
    try:
        chat_info = await context.bot.get_chat(user_id)
        if chat_info:
            if chat_info.username:  # Only override if we got a username
                db_username = chat_info.username
            if chat_info.first_name and chat_info.first_name != display_name:  # Only override if different
                # Also filter here
                new_name = chat_info.first_name
                clean_new_name = re.sub(r'[._\-\s]', '', new_name)
                if len(clean_new_name) >= 1: # Valid name
                    display_name = new_name
            logger.info(f"✅ Got user info from bot.get_chat: @{db_username} / {display_name} for user {user_id}")
    except Exception as e:
        logger.warning(f"Could not fetch chat info for {user_id}: {e}, using Update object data")
    
    logger.info(f"👤 User {user_id} - DB username: @{db_username}, Display: {display_name}")

    # IMMEDIATELY save username to database (before any early returns!)
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Try to update with first_name, fallback if column doesn't exist
        try:
            first_name_to_store = display_name if display_name else f"User_{user_id}"
            c.execute("""
                INSERT INTO users (user_id, username, first_name, language, is_reseller) 
                VALUES (%s, %s, %s, 'en', FALSE)
                ON CONFLICT(user_id) DO UPDATE SET 
                    username=COALESCE(excluded.username, users.username),
                    first_name=COALESCE(excluded.first_name, users.first_name)
            """, (user_id, db_username, first_name_to_store))
            conn.commit()
            logger.info(f"✅ SAVED username to DB: user={user_id}, username={db_username}, first_name={first_name_to_store}")
        except Exception as insert_err:
            if "first_name" in str(insert_err).lower() or "column" in str(insert_err).lower():
                logger.warning(f"first_name column doesn't exist, saving username only")
                conn.rollback()
                c.execute("""
                    INSERT INTO users (user_id, username, language, is_reseller) 
                    VALUES (%s, %s, 'en', FALSE)
                    ON CONFLICT(user_id) DO UPDATE SET username=COALESCE(excluded.username, users.username)
                """, (user_id, db_username))
                conn.commit()
                logger.info(f"✅ SAVED username to DB (no first_name): user={user_id}, username={db_username}")
            else:
                logger.error(f"❌ Failed to save username: {insert_err}")
    except Exception as e:
        logger.error(f"❌ DB error saving username for {user_id}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

    # WORKER REDIRECT
    if is_worker(user_id) and not is_primary_admin(user_id):
        return await handle_worker_dashboard(update, context)
    
    # LANGUAGE SELECTION CHECK - Before verification if enabled
    if is_language_selection_enabled() and get_language_prompt_placement() == 'before':
        # Skip language selection for admins
        if not (is_primary_admin(user_id) or is_secondary_admin(user_id)):
            # Show language selection if not completed in this session (before verification)
            if not context.user_data.get('language_selection_completed'):
                logger.info(f"🌍 Showing language selection for user {user_id} (before verification)")
                return await handle_language_selection(update, context)
            
            # Load language from context or database
            user_language = context.user_data.get('lang')
            if not user_language:
                # Check database for saved language
                conn = None
                try:
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute("SELECT language FROM users WHERE user_id = %s", (user_id,))
                    result = c.fetchone()
                    if result and result['language']:
                        user_language = result['language']
                        context.user_data['lang'] = user_language
                        logger.info(f"🌍 User {user_id} language loaded from DB: {user_language}")
                except Exception as e:
                    logger.error(f"Error checking user language: {e}")
                finally:
                    if conn:
                        conn.close()
            
            logger.info(f"🌍 User {user_id} has language: {user_language}")
        else:
            logger.info(f"👑 User {user_id} is admin, skipping language selection")
    
    # HUMAN VERIFICATION CHECK
    if is_human_verification_enabled():
        logger.info(f"🔍 Human verification is enabled, checking user {user_id}")
        # Skip verification for admins
        if not (is_primary_admin(user_id) or is_secondary_admin(user_id)):
            is_verified = is_user_verified(user_id)
            logger.info(f"🔍 User {user_id} verification status: {is_verified}")
            
            # Check verification status normally (no auto-reset)
            
            # Admin debug commands
            if update.message and update.message.text:
                message_text = update.message.text.lower()
                
                # Reset verification command
                if "reset verification" in message_text:
                    logger.info(f"🔄 Resetting verification for user {user_id}")
                    conn = None
                    try:
                        conn = get_db_connection()
                        c = conn.cursor()
                        c.execute("UPDATE users SET is_human_verified = FALSE, verification_attempts = 0 WHERE user_id = %s", (user_id,))
                        conn.commit()
                        logger.info(f"✅ Verification reset for user {user_id}")
                        is_verified = False
                        await update.message.reply_text("🔄 Verification status reset! Type /start again.")
                        return
                    except Exception as e:
                        logger.error(f"Error resetting verification: {e}")
                    finally:
                        if conn:
                            conn.close()
                
                # Debug settings command
                if "debug settings" in message_text:
                    human_verification = is_human_verification_enabled()
                    language_selection = is_language_selection_enabled() 
                    placement = get_language_prompt_placement()
                    debug_msg = f"🔍 **Debug Info:**\n\n"
                    debug_msg += f"🤖 Human Verification: {'✅ ENABLED' if human_verification else '❌ DISABLED'}\n"
                    debug_msg += f"🌍 Language Selection: {'✅ ENABLED' if language_selection else '❌ DISABLED'}\n"
                    debug_msg += f"📍 Language Placement: {placement.upper()}\n"
                    debug_msg += f"✅ User Verified: {is_verified}\n"
                    debug_msg += f"🌍 User Language: {context.user_data.get('lang', 'None')}"
                    await update.message.reply_text(debug_msg, parse_mode='Markdown')
                    return
            
            if not is_verified:
                logger.info(f"User {user_id} needs human verification")
                return await handle_human_verification(update, context)
            else:
                logger.info(f"✅ User {user_id} is already verified, proceeding to main menu")
        else:
            logger.info(f"👑 User {user_id} is admin, skipping verification")
    else:
        logger.info(f"🔍 Human verification is disabled")
    
    # LANGUAGE SELECTION CHECK - After verification if enabled
    if is_language_selection_enabled() and get_language_prompt_placement() == 'after':
        # Skip language selection for admins
        if not (is_primary_admin(user_id) or is_secondary_admin(user_id)):
            # Only show language selection if user is verified AND hasn't completed language selection
            is_user_already_verified = is_user_verified(user_id)
            
            if is_user_already_verified and not context.user_data.get('language_selection_completed'):
                logger.info(f"🌍 User {user_id} is verified, showing language selection (after verification)")
                return await handle_language_selection(update, context)
            elif not is_user_already_verified:
                logger.info(f"🔍 User {user_id} not verified yet, skipping language selection (placement=after)")
                # Don't show language selection - verification should come first
            
            # Load language from context or database
            user_language = context.user_data.get('lang')
            if not user_language:
                # Check database for saved language
                conn = None
                try:
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute("SELECT language FROM users WHERE user_id = %s", (user_id,))
                    result = c.fetchone()
                    if result and result['language']:
                        user_language = result['language']
                        context.user_data['lang'] = user_language
                        logger.info(f"🌍 User {user_id} language loaded from DB: {user_language}")
                except Exception as e:
                    logger.error(f"Error checking user language: {e}")
                finally:
                    if conn:
                        conn.close()
            
            logger.info(f"🌍 User {user_id} has language: {user_language}")
        else:
            logger.info(f"👑 User {user_id} is admin, skipping language selection")
    
    # Check if admin has activated custom UI theme
    try:
        from marketing_promotions import get_active_ui_theme, handle_minimalist_welcome, handle_modern_welcome, handle_classic_welcome, get_custom_layout
        active_theme = get_active_ui_theme()
        
        if active_theme and active_theme.get('theme_name') == 'minimalist':
            logger.info(f"Using admin-configured minimalist UI theme for user {user_id}")
            return await handle_minimalist_welcome(update, context)
        elif active_theme and active_theme.get('theme_name') == 'modern':
            logger.info(f"Using admin-configured modern UI theme for user {user_id}")
            return await handle_modern_welcome(update, context)
        elif active_theme and active_theme.get('theme_name') == 'classic':
            logger.info(f"Using admin-configured classic UI theme for user {user_id}")
            # For classic theme, use hardcoded 6-button layout directly
            return await handle_classic_welcome(update, context)
        elif active_theme and active_theme.get('theme_name') == 'custom':
            logger.info(f"Using custom layout for user {user_id}")
            # Custom layouts are handled by the custom layout system in _build_start_menu_content
            # Continue with regular start flow but with custom layout applied
        else:
            logger.info(f"Using default interface for user {user_id} (active theme: {active_theme.get('theme_name', 'default')})")
    except ImportError:
        logger.warning("Marketing promotions module not available, using default UI")
    except Exception as e:
        logger.error(f"Error checking admin-configured UI theme: {e}")
        # Continue with regular start flow if theme check fails
    
    # Ban check is now handled in main.py start_command_wrapper
    # This check is kept for callback queries that might bypass the wrapper
    if is_callback and await is_user_banned(user_id):
        logger.info(f"Banned user {user_id} attempted to use /start command via callback.")
        ban_message = "❌ Your access to this bot has been restricted. If you believe this is an error, please contact support."
        try:
            await update.callback_query.edit_message_text(ban_message, parse_mode=None)
        except Exception as e:
            logger.error(f"Error editing message for banned user {user_id}: {e}")
            try:
                await update.callback_query.answer(ban_message, show_alert=True)
            except Exception as e2:
                logger.error(f"Error answering callback for banned user {user_id}: {e2}")
        return

    # Send Bot Media (Only on direct /start, not callbacks)
    if not is_callback and BOT_MEDIA.get("type") and BOT_MEDIA.get("path"):
        media_path = BOT_MEDIA["path"]
        media_type = BOT_MEDIA["type"]
        logger.info(f"Attempting to send BOT_MEDIA: type={media_type}, path={media_path}")

        # Check if file exists using asyncio.to_thread
        if await asyncio.to_thread(os.path.exists, media_path):
            try:
                # Pass the file path directly to the send_* methods with rate limiting
                from utils import send_media_with_retry
                if media_type == "photo":
                    await send_media_with_retry(context.bot, chat_id, media_path, media_type='photo')
                elif media_type == "video":
                    await send_media_with_retry(context.bot, chat_id, media_path, media_type='video')
                elif media_type == "gif":
                    await send_media_with_retry(context.bot, chat_id, media_path, media_type='animation')
                else:
                    logger.warning(f"Unsupported BOT_MEDIA type for sending: {media_type}")

            except telegram_error.TelegramError as e:
                logger.error(f"Error sending BOT_MEDIA ({media_path}): {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Unexpected error sending BOT_MEDIA ({media_path}): {e}", exc_info=True)
        else:
            logger.warning(f"BOT_MEDIA path {media_path} not found on disk when trying to send.")


    # ALWAYS update username and first_name in database on every /start (not just first time)
    logger.info(f"🔧 ATTEMPTING to update username for user {user_id}: db_username={db_username}, display_name={display_name}")
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Ensure user exists and update username AND first_name
        try:
            # Use display_name which is ALWAYS set (never None)
            first_name_to_store = display_name if display_name else f"User_{user_id}"
            logger.info(f"🔧 Executing INSERT/UPDATE for user {user_id}")
            c.execute("""
                INSERT INTO users (user_id, username, first_name, language, is_reseller) 
                VALUES (%s, %s, %s, 'en', FALSE)
                ON CONFLICT(user_id) DO UPDATE SET 
                    username=excluded.username,
                    first_name=excluded.first_name
            """, (user_id, db_username, first_name_to_store))
            conn.commit()
            logger.info(f"✅ Updated user {user_id} - username: {db_username}, first_name: {first_name_to_store}")
        except Exception as insert_err:
            # If first_name column doesn't exist yet, try without it
            if "first_name" in str(insert_err).lower() or "column" in str(insert_err).lower():
                logger.warning(f"first_name column doesn't exist yet, inserting without it")
                conn.rollback()  # Rollback failed transaction
                c.execute("""
                    INSERT INTO users (user_id, username, language, is_reseller) 
                    VALUES (%s, %s, 'en', FALSE)
                    ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
                """, (user_id, db_username))
                conn.commit()
                logger.info(f"✅ Updated user {user_id} - username: {db_username} (without first_name)")
            else:
                logger.error(f"Error inserting user: {insert_err}")
                raise  # Re-raise if it's a different error
    except Exception as e:
        logger.error(f"DB error updating username for {user_id}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    
    # Ensure language context is set
    lang = context.user_data.get("lang", None)
    if lang is None:
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            # Get language
            c.execute("SELECT language FROM users WHERE user_id = %s", (user_id,))
            result = c.fetchone()
            db_lang = result['language'] if result else 'en'
            try: from utils import LANGUAGES as UTILS_LANGUAGES_START
            except ImportError: UTILS_LANGUAGES_START = {'en': {}}
            lang = db_lang if db_lang and db_lang in UTILS_LANGUAGES_START else 'en'
            context.user_data["lang"] = lang
            logger.info(f"start: Set language for user {user_id} to '{lang}' from DB/default.")
            
            # Update user activity status (they successfully interacted with the bot)
            update_user_broadcast_status(user_id, success=True)
        except Exception as e:
            logger.error(f"DB error ensuring user/language in start for {user_id}: {e}")
            lang = 'en'
            context.user_data["lang"] = lang
            logger.warning(f"start: Defaulted language to 'en' for user {user_id} due to DB error.")
        finally:
            if conn: conn.close()
    else:
        logger.info(f"start: Using existing language '{lang}' from context for user {user_id}.")

    # Build and Send/Edit Menu
    logger.info(f"🔍 ABOUT TO CALL _get_lang_data for user {user_id}")
    try:
        lang, lang_data = _get_lang_data(context)
        logger.info(f"✅ _get_lang_data SUCCESS for user {user_id}, lang={lang}")
    except Exception as e:
        logger.error(f"❌ _get_lang_data FAILED for user {user_id}: {e}", exc_info=True)
        raise
    
    logger.info(f"🔍 ABOUT TO CALL _build_start_menu_content for user {user_id}")
    try:
        full_welcome, reply_markup = _build_start_menu_content(user_id, display_name, lang_data, context, user)
        logger.info(f"✅ _build_start_menu_content SUCCESS for user {user_id}")
    except Exception as e:
        logger.error(f"❌ _build_start_menu_content FAILED for user {user_id}: {e}", exc_info=True)
        raise

    if is_callback:
        query = update.callback_query
        try:
             if query.message and (query.message.text != full_welcome or query.message.reply_markup != reply_markup):
                  await query.edit_message_text(full_welcome, reply_markup=reply_markup, parse_mode=None)
             elif query: await query.answer()
        except telegram_error.BadRequest as e:
            if "message is not modified" in str(e).lower():
                try:
                    await query.answer()
                except telegram_error.BadRequest:
                    pass
            else:
                logger.warning(f"Failed to edit start message (callback): {e}. Sending new.")
                await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)
        except Exception as e:
             logger.error(f"Unexpected error editing start message (callback): {e}", exc_info=True)
             await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)
    else:
        await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)


# --- Other handlers ---
async def handle_back_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Back' button presses that should return to the main start menu."""
    await start(update, context)

async def refresh_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force refresh the start menu with new buttons (clears Telegram cache)."""
    user = update.effective_user
    logger.info(f"🔄 REFRESH COMMAND CALLED by user {user.id} - Generating FRESH v3.0 buttons")
    await start(update, context)

async def check_version_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command to check running version."""
    await update.message.reply_text("✅ BOT IS RUNNING VERSION 3.1 - GRID FIXED - PINS FIXED")
    await start(update, context)

# --- Shopping Handlers ---
async def handle_shop(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    logger.info(f"handle_shop triggered by user {user_id} (lang: {lang}).")

    no_cities_available_msg = lang_data.get("no_cities_available", "No cities available at the moment. Please check back later.")
    choose_city_title = lang_data.get("choose_city_title", "Choose a City")
    select_location_prompt = lang_data.get("select_location_prompt", "Select your location:")
    home_button_text = lang_data.get("home_button", "Home")

    if not CITIES:
        keyboard = [[InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        await query.edit_message_text(f"{EMOJI_CITY} {no_cities_available_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        return

    try:
        sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
        keyboard = []
        for c_id in sorted_city_ids:
             city_name = CITIES.get(c_id)
             if city_name: keyboard.append([InlineKeyboardButton(f"{EMOJI_CITY} {city_name}", callback_data=f"city|{c_id}")])
             else: logger.warning(f"handle_shop: City name missing for ID {c_id}.")
        keyboard.append([InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = f"{EMOJI_CITY} {choose_city_title}\n\n{select_location_prompt}"
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode=None)
        logger.info(f"handle_shop: Sent city list to user {user_id}.")
    except telegram_error.BadRequest as e:
         if "message is not modified" not in str(e).lower(): logger.error(f"Error editing shop message: {e}"); await query.answer("Error displaying cities.", show_alert=True)
         else: await query.answer()
    except Exception as e:
        logger.error(f"Error in handle_shop for user {user_id}: {e}", exc_info=True)
        try: keyboard = [[InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]; await query.edit_message_text("❌ An error occurred.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except Exception as inner_e: logger.error(f"Failed fallback in handle_shop: {inner_e}")


# --- Modified handle_city_selection (Corrected Formatting FINAL) ---
async def handle_city_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id # Added for logging
    lang, lang_data = _get_lang_data(context)

    if not params:
        logger.warning(f"handle_city_selection called without city_id for user {user_id}.")
        await query.answer("Error: City ID missing.", show_alert=True)
        return
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        error_city_not_found = lang_data.get("error_city_not_found", "Error: City not found.")
        logger.warning(f"City ID {city_id} not found in CITIES for user {user_id}.")
        await query.edit_message_text(f"❌ {error_city_not_found}", parse_mode=None)
        return await handle_shop(update, context) # Go back to city selection

    districts_in_city = DISTRICTS.get(city_id, {})
    back_cities_button = lang_data.get("back_cities_button", "Back to Cities")
    home_button = lang_data.get("home_button", "Home")
    no_districts_msg = lang_data.get("no_districts_available", "No districts available yet for this city.")
    no_products_in_districts_msg = lang_data.get("no_products_in_city_districts", "No products currently available in any district of this city.")
    choose_district_prompt = lang_data.get("choose_district_prompt", "Choose a district:")
    error_loading_districts = lang_data.get("error_loading_districts", "Error loading districts. Please try again.")
    available_label_short = lang_data.get("available_label_short", "Av") # Get short available label

    keyboard = []
    message_text_parts = [f"{EMOJI_CITY} {city_name}\n\n"] # Start message
    districts_with_products_info = [] # Store tuples: (d_id, dist_name)

    if not districts_in_city:
        # If no districts are configured AT ALL for the city
        keyboard_nav = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
        await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n{no_districts_msg}", reply_markup=InlineKeyboardMarkup(keyboard_nav), parse_mode=None)
        return
    else:
        # If districts are configured, check each one for products
        sorted_district_ids = sorted(districts_in_city.keys(), key=lambda dist_id: districts_in_city.get(dist_id, ''))
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()

            for d_id in sorted_district_ids:
                dist_name = districts_in_city.get(d_id)
                if dist_name:
                    # NEW Query for detailed product summary in this district
                    c.execute("""
                        SELECT product_type, size, price, COUNT(*) as quantity
                        FROM products
                        WHERE city = %s AND district = %s AND available > reserved
                        GROUP BY product_type, size, price
                        ORDER BY product_type, price, size
                    """, (city_name, dist_name))
                    products_in_district = c.fetchall()

                    if products_in_district:
                        # Add district header to message text (using Markdown for bold)
                        escaped_dist_name = helpers.escape_markdown(dist_name, version=2)
                        message_text_parts.append(f"{EMOJI_DISTRICT} *{escaped_dist_name}*:\n") # Keep newline after district name

                        # --- Build product list string for this district ---
                        for prod in products_in_district:
                            prod_emoji = PRODUCT_TYPES.get(prod['product_type'], DEFAULT_PRODUCT_EMOJI)
                            price_str = format_currency(prod['price'])
                            # Escape parts individually
                            escaped_type = helpers.escape_markdown(prod['product_type'], version=2)
                            escaped_size = helpers.escape_markdown(prod['size'], version=2)
                            escaped_price = helpers.escape_markdown(price_str, version=2)
                            escaped_qty = helpers.escape_markdown(str(prod['quantity']), version=2)
                            # Create the formatted line WITH a standard Python newline \n
                            # Removed the quantity display per admin request
                            message_text_parts.append(f"    • {prod_emoji} {escaped_type} {escaped_size} \\({escaped_price}€\\)\n")

                        # Add a blank line for spacing after the district's products
                        message_text_parts.append("\n")
                        # --- End building product list string ---

                        # Add district to list for button creation
                        districts_with_products_info.append((d_id, dist_name))
                    # else: District has no products, do nothing (it's skipped)
                else:
                    logger.warning(f"District name missing for ID {d_id} in city {city_id} (handle_city_selection)")

        except sqlite3.Error as e:
            logger.error(f"DB error checking product availability for districts in city {city_name} (ID: {city_id}) for user {user_id}: {e}")
            keyboard_error = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n❌ {error_loading_districts}", reply_markup=InlineKeyboardMarkup(keyboard_error), parse_mode=None)
            if conn: conn.close()
            return # Stop processing on DB error
        finally:
            if conn:
                conn.close()

        # After checking all districts:
        if not districts_with_products_info:
            # If we looped through all configured districts but none had products
            keyboard_nav = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n{no_products_in_districts_msg}", reply_markup=InlineKeyboardMarkup(keyboard_nav), parse_mode=None)
        else:
            # Add prompt below details ONLY if there are districts with products
            message_text_parts.append(f"\n{choose_district_prompt}")
            final_message = "".join(message_text_parts)

            # Create buttons ONLY for districts with products
            for d_id, dist_name in districts_with_products_info:
                 keyboard.append([InlineKeyboardButton(f"{EMOJI_DISTRICT} {dist_name}", callback_data=f"dist|{city_id}|{d_id}")])

            keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])

            # Check length and edit message
            try:
                if len(final_message) > 4000:
                    # Find a good place to truncate (e.g., before the last district's details)
                    trunc_point = final_message.rfind(f"\n{EMOJI_DISTRICT}", 0, 3900)
                    if trunc_point != -1:
                        final_message = final_message[:trunc_point] + "\n\n\\[\\.\\.\\. Message truncated \\.\\.\\.\\]"
                    else: # Fallback if no good split point found
                        final_message = final_message[:4000] + "\n\n\\[\\.\\.\\. Message truncated \\.\\.\\.\\]"
                    logger.warning(f"District selection message for user {user_id} city {city_name} truncated.")

                await query.edit_message_text(
                    final_message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN_V2 # Use Markdown
                )
            except telegram_error.BadRequest as e:
                if "message is not modified" not in str(e).lower():
                    logger.error(f"Error editing district selection message (Markdown): {e}")
                    # Fallback to plain text if Markdown fails
                    try:
                         plain_text_message = "".join(message_text_parts).replace('*','').replace('\\','') # Basic removal of bold and escapes
                         if len(plain_text_message) > 4000: plain_text_message = plain_text_message[:4000] + "\n\n[... Message truncated ...]"
                         await query.edit_message_text(plain_text_message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
                    except Exception as fallback_e:
                         logger.error(f"Failed fallback edit for district selection: {fallback_e}")
                         await query.answer("Error displaying districts.", show_alert=True)
                else:
                    await query.answer() # Acknowledge if not modified
# --- END handle_city_selection ---


async def handle_district_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 2: logger.warning("handle_district_selection missing params."); await query.answer("Error: City/District ID missing.", show_alert=True); return
    city_id, dist_id = params[0], params[1]
    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)

    if not city or not district: error_district_city_not_found = lang_data.get("error_district_city_not_found", "Error: District or city not found."); await query.edit_message_text(f"❌ {error_district_city_not_found}", parse_mode=None); return await handle_shop(update, context)

    back_districts_button = lang_data.get("back_districts_button", "Back to Districts"); home_button = lang_data.get("home_button", "Home")
    no_types_msg = lang_data.get("no_types_available", "No product types currently available here."); select_type_prompt = lang_data.get("select_type_prompt", "Select product type:")
    error_loading_types = lang_data.get("error_loading_types", "Error: Failed to Load Product Types"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    max_retries = 3
    retry_delay = 0.1  # 100ms
    
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT DISTINCT product_type FROM products WHERE city = %s AND district = %s AND available > reserved ORDER BY product_type", (city, district))
            available_types = [row['product_type'] for row in c.fetchall()]
            break  # Success, exit retry loop
        except sqlite3.Error as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                logger.warning(f"Database locked for district selection (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                if conn:
                    conn.close()
                    conn = None
                continue
            else:
                logger.error(f"DB error fetching product types {city}/{district}: {e}", exc_info=True)
                await query.edit_message_text(f"❌ {error_loading_types}", parse_mode=None)
                return
        except Exception as e:
            logger.error(f"Unexpected error in handle_district_selection: {e}", exc_info=True)
            await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
            return
        finally:
            if conn:
                conn.close()
                conn = None

    if not available_types:
        keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_districts_button}", callback_data=f"city|{city_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
        await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n\n{no_types_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        keyboard = []
        for pt in available_types:
            emoji = PRODUCT_TYPES.get(pt, DEFAULT_PRODUCT_EMOJI)
            keyboard.append([InlineKeyboardButton(f"{emoji} {pt}", callback_data=f"type|{city_id}|{dist_id}|{pt}")])
        # Go back to city selection (which now shows the product list)
        keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_districts_button}", callback_data=f"city|{city_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])
        try:
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n\n{select_type_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except Exception as e:
            # Handle "Message is not modified" error gracefully
            if "Message is not modified" in str(e):
                await query.answer("Already showing this view")
            else:
                logger.error(f"Error editing message in handle_district_selection: {e}")
                await query.answer("Error updating view")


# <<< MODIFIED: Incorporate Reseller Discount Display >>>
async def handle_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id # <<< GET USER ID
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 3: logger.warning("handle_type_selection missing params."); await query.answer("Error: City/District/Type missing.", show_alert=True); return
    city_id, dist_id, p_type = params
    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)

    if not city or not district: error_district_city_not_found = lang_data.get("error_district_city_not_found", "Error: District or city not found."); await query.edit_message_text(f"❌ {error_district_city_not_found}", parse_mode=None); return await handle_shop(update, context)

    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    back_types_button = lang_data.get("back_types_button", "Back to Types"); home_button = lang_data.get("home_button", "Home")
    no_items_of_type = lang_data.get("no_items_of_type", "No items of this type currently available here.")
    available_options_prompt = lang_data.get("available_options_prompt", "Available options:")
    error_loading_products = lang_data.get("error_loading_products", "Error: Failed to Load Products"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT size, price, COUNT(*) as count_available FROM products WHERE city = %s AND district = %s AND product_type = %s AND available > reserved GROUP BY size, price ORDER BY price", (city, district, p_type))
        products = c.fetchall()

        if not products:
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_types_button}", callback_data=f"dist|{city_id}|{dist_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n{product_emoji} {p_type}\n\n{no_items_of_type}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            keyboard = []
            available_label_short = lang_data.get("available_label_short", "Av")
            # <<< Fetch reseller discount ONCE >>>
            reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, p_type)
            # <<< End Fetch >>>

            for row in products:
                size, original_price_decimal, count = row['size'], Decimal(str(row['price'])), row['count_available']
                original_price_str = format_currency(original_price_decimal)
                original_price_callback_str = f"{original_price_decimal:.2f}" # Use original price for callback

                # <<< Apply Reseller Discount for Display >>>
                discounted_price_str = original_price_str # Default to original
                if reseller_discount_percent > Decimal('0.0'):
                    discount_amount = (original_price_decimal * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                    discounted_price_decimal = original_price_decimal - discount_amount
                    discounted_price_str = format_currency(discounted_price_decimal)
                    # Use simple plain text for original price notation
                    button_text = f"{product_emoji} {size} ({discounted_price_str}€ / Orig: {original_price_str}€) - {available_label_short}: {count}"
                else:
                    # No discount, show original price only
                    button_text = f"{product_emoji} {size} ({original_price_str}€) - {available_label_short}: {count}"
                # <<< End Apply >>>

                # Callback still uses original price
                callback_data = f"product|{city_id}|{dist_id}|{p_type}|{size}|{original_price_callback_str}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

            keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_types_button}", callback_data=f"dist|{city_id}|{dist_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n{product_emoji} {p_type}\n\n{available_options_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e: logger.error(f"DB error fetching products {city}/{district}/{p_type}: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_loading_products}", parse_mode=None)
    except Exception as e: logger.error(f"Unexpected error in handle_type_selection: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()

# --- END OF handle_type_selection ---

# <<< MODIFIED: Add Pay Now Button & Logic for Single Item Discount Flow >>>
async def handle_product_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 5: logger.warning("handle_product_selection missing params."); await query.answer("Error: Incomplete product data.", show_alert=True); return
    city_id, dist_id, p_type, size, price_str = params # price_str is ORIGINAL price

    try: original_price = Decimal(price_str)
    except ValueError: logger.warning(f"Invalid price format: {price_str}"); await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None); return

    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district: error_location_mismatch = lang_data.get("error_location_mismatch", "Error: Location data mismatch."); await query.edit_message_text(f"❌ {error_location_mismatch}", parse_mode=None); return await handle_shop(update, context)

    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    theme_name = context.user_data.get("theme", "default")
    theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)

    price_label = lang_data.get("price_label", "Price"); available_label_long = lang_data.get("available_label_long", "Available")
    back_options_button = lang_data.get("back_options_button", "Back to Options"); home_button = lang_data.get("home_button", "Home")
    drop_unavailable_msg = lang_data.get("drop_unavailable", "Drop Unavailable! This option just sold out or was reserved.")
    add_to_basket_button = lang_data.get("add_to_basket_button", "Add to Basket")
    pay_now_button_text = lang_data.get("pay_now_button", "Pay Now")
    error_loading_details = lang_data.get("error_loading_details", "Error: Failed to Load Product Details"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # FIXED: Query current price from DB instead of using cached price
        c.execute("SELECT price, COUNT(*) as count FROM products WHERE city = %s AND district = %s AND product_type = %s AND size = %s AND available > reserved GROUP BY price ORDER BY price LIMIT 1", (city, district, p_type, size))
        result = c.fetchone()
        
        if not result or result['count'] <= 0:
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"❌ {drop_unavailable_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            # Use CURRENT price from database, not cached price
            current_price = Decimal(str(result['price']))
            available_count = result['count']
            current_price_str = f"{current_price:.2f}"  # For callback data
            
            current_price_formatted = format_currency(current_price)
            reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, p_type)
            display_price_str = current_price_formatted
            if reseller_discount_percent > Decimal('0.0'):
                discount_amount = (current_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                discounted_price = current_price - discount_amount
                display_price_str = f"{format_currency(discounted_price)} (Orig: {current_price_formatted}€)"

            msg = (f"{EMOJI_CITY} {city} | {EMOJI_DISTRICT} {district}\n"
                   f"{product_emoji} {p_type} - {size}\n"
                   f"{EMOJI_PRICE} {price_label}: {display_price_str} EUR\n"
                   f"{EMOJI_QUANTITY} {available_label_long}: {available_count}")

            # Use CURRENT price in callback data
            add_callback = f"add|{city_id}|{dist_id}|{p_type}|{size}|{current_price_str}"
            back_callback = f"type|{city_id}|{dist_id}|{p_type}"
            pay_now_callback = f"pay_single_item|{city_id}|{dist_id}|{p_type}|{size}|{current_price_str}"
            
            # New callbacks for product view
            apply_discount_callback = f"apply_discount_product|{city_id}|{dist_id}|{p_type}|{size}|{current_price_str}"
            apply_referral_callback = f"apply_referral_product|{city_id}|{dist_id}|{p_type}|{size}|{current_price_str}"
            
            apply_discount_text = lang_data.get("apply_discount_pay_button", "🏷️ Apply Discount")
            apply_referral_text = lang_data.get("apply_referral_button", "🎁 Apply Referral")

            keyboard = [
                [
                    InlineKeyboardButton(f"{basket_emoji} {add_to_basket_button}", callback_data=add_callback),
                    InlineKeyboardButton(f"{EMOJI_PAY_NOW} {pay_now_button_text}", callback_data=pay_now_callback)
                ]
            ]
            
            # Add discount/referral buttons row
            dr_row = [InlineKeyboardButton(apply_discount_text, callback_data=apply_discount_callback)]
            
            from referral_system import get_referral_settings
            if get_referral_settings().get('program_enabled', False):
                dr_row.append(InlineKeyboardButton(apply_referral_text, callback_data=apply_referral_callback))
            
            keyboard.append(dr_row)
            
            keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=back_callback), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])

            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.Error as e: logger.error(f"DB error checking availability {city}/{district}/{p_type}/{size}: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_loading_details}", parse_mode=None)
    except Exception as e: logger.error(f"Unexpected error in handle_product_selection: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()

# --- END handle_product_selection ---

# <<< MODIFIED: Incorporate Reseller Discount Calculation & Display >>>
async def handle_add_to_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id # <<< GET USER ID
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 5: logger.warning("handle_add_to_basket missing params."); await query.answer("Error: Incomplete product data.", show_alert=True); return
    city_id, dist_id, p_type, size, price_str = params # price_str is ORIGINAL price

    try: original_price = Decimal(price_str) # <<< Store original price
    except ValueError: logger.warning(f"Invalid price format add_to_basket: {price_str}"); await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None); return

    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district: error_location_mismatch = lang_data.get("error_location_mismatch", "Error: Location data mismatch."); await query.edit_message_text(f"❌ {error_location_mismatch}", parse_mode=None); return await handle_shop(update, context)

    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    theme_name = context.user_data.get("theme", "default"); theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)
    product_id_reserved = None; conn = None

    back_options_button = lang_data.get("back_options_button", "Back to Options"); home_button = lang_data.get("home_button", "Home")
    out_of_stock_msg = lang_data.get("out_of_stock", "Out of Stock! Sorry, the last one was taken or reserved.")
    pay_now_button_text = lang_data.get("pay_now_button", "Pay Now"); top_up_button_text = lang_data.get("top_up_button", "Top Up")
    view_basket_button_text = lang_data.get("view_basket_button", "View Basket"); clear_basket_button_text = lang_data.get("clear_basket_button", "Clear Basket")
    shop_more_button_text = lang_data.get("shop_more_button", "Shop More"); expires_label = lang_data.get("expires_label", "Expires in")
    error_adding_db = lang_data.get("error_adding_db", "Error: Database issue adding item."); error_adding_unexpected = lang_data.get("error_adding_unexpected", "Error: An unexpected issue occurred.")
    added_msg_template = lang_data.get("added_to_basket", "✅ Item Reserved!\n\n{item} is in your basket for {timeout} minutes! ⏳")
    pay_msg_template = lang_data.get("pay", "💳 Total to Pay: {amount} EUR")
    apply_discount_button_text = lang_data.get("apply_discount_button", "Apply Discount Code")
    reseller_discount_label = lang_data.get("reseller_discount_label", "Reseller Discount") # <<< NEW

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        
        # Step 1: Find an available product (FIXED: removed price from WHERE to use current price)
        c.execute("SELECT id, price FROM products WHERE city = %s AND district = %s AND product_type = %s AND size = %s AND available > reserved ORDER BY id LIMIT 1", (city, district, p_type, size))
        product_row = c.fetchone()

        if not product_row:
            conn.rollback()
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"❌ {out_of_stock_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
            return

        product_id_reserved = product_row['id']
        # Use CURRENT price from database, not cached price
        current_price = Decimal(str(product_row['price']))
        original_price = current_price  # Update to use current price for all subsequent calculations
        
        # Step 2: Atomically reserve the specific product with availability check
        # This prevents race conditions by ensuring reserved never exceeds available
        c.execute("UPDATE products SET reserved = reserved + 1 WHERE id = %s AND available > reserved", (product_id_reserved,))
        
        if c.rowcount == 0:
            # Race condition: product was taken between SELECT and UPDATE
            conn.rollback()
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text("❌ Sorry, this item was just taken by another user! Please try again.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
            return
        c.execute("SELECT basket FROM users WHERE user_id = %s", (user_id,))
        user_basket_row = c.fetchone(); current_basket_str = user_basket_row['basket'] if user_basket_row else ''
        timestamp = time.time(); new_item_str = f"{product_id_reserved}:{timestamp}"
        new_basket_str = f"{current_basket_str},{new_item_str}" if current_basket_str else new_item_str
        c.execute("UPDATE users SET basket = %s WHERE user_id = %s", (new_basket_str, user_id))
        conn.commit()

        if "basket" not in context.user_data or not isinstance(context.user_data["basket"], list): context.user_data["basket"] = []
        # <<< Store product_type along with original price >>>
        context.user_data["basket"].append({
            "product_id": product_id_reserved,
            "price": original_price, # Store original price
            "product_type": p_type, # Store product type
            "timestamp": timestamp
        })
        # <<< End store >>>
        logger.info(f"User {user_id} added product {product_id_reserved} (type: {p_type}) to basket.")

        timeout_minutes = BASKET_TIMEOUT // 60
        current_basket_list = context.user_data["basket"]

        # --- Calculate Totals with Reseller Discount ---
        basket_original_total = Decimal('0.0')
        total_reseller_discount_amount = Decimal('0.0')
        total_after_reseller = Decimal('0.0')

        for item in current_basket_list:
            item_original_price = item.get('price', Decimal('0.0')) # Ensure it's Decimal
            item_type = item.get('product_type', '') # Ensure it exists
            basket_original_total += item_original_price

            item_reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, item_type)
            item_reseller_discount = (item_original_price * item_reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            total_reseller_discount_amount += item_reseller_discount
            total_after_reseller += (item_original_price - item_reseller_discount)
        # --- End Calculate ---

        # --- Apply General Discount (if any) ---
        final_total = total_after_reseller # Start with reseller-discounted total
        general_discount_amount = Decimal('0.0')
        applied_discount_info = context.user_data.get('applied_discount')
        pay_msg_str = ""

        if applied_discount_info:
            # Validate general code against the total *after* reseller discount
            # Note: For existing applied discounts, we don't re-validate atomically to avoid double-charging
            code_valid, _, discount_details = validate_discount_code(applied_discount_info['code'], float(total_after_reseller))
            if code_valid and discount_details:
                general_discount_amount = Decimal(str(discount_details['discount_amount']))
                final_total = Decimal(str(discount_details['final_total'])) # validate_discount_code returns final total after THIS code
                # Update context with amounts based on the reseller-adjusted total
                context.user_data['applied_discount']['amount'] = float(general_discount_amount)
                context.user_data['applied_discount']['final_total'] = float(final_total)
            else:
                # General discount became invalid (maybe due to reseller discount changing total)
                context.user_data.pop('applied_discount', None)
                await query.answer("General discount removed (basket changed).", show_alert=False)
        # --- End Apply General Discount ---


        # --- Build Message ---
        item_price_str = format_currency(original_price)
        item_desc = f"{product_emoji} {p_type} {size} ({item_price_str}€)"
        expiry_dt = datetime.fromtimestamp(timestamp + BASKET_TIMEOUT); expiry_time_str = expiry_dt.strftime('%H:%M:%S')
        reserved_msg = (added_msg_template.format(timeout=timeout_minutes, item=item_desc) + "\n\n" + f"⏳ {expires_label}: {expiry_time_str}\n\n")

        # Display breakdown
        basket_original_total_str = format_currency(basket_original_total)
        reserved_msg += f"{lang_data.get('subtotal_label', 'Subtotal')}: {basket_original_total_str} EUR\n"
        if total_reseller_discount_amount > Decimal('0.0'):
            reseller_discount_str = format_currency(total_reseller_discount_amount)
            reserved_msg += f"{EMOJI_DISCOUNT} {reseller_discount_label}: -{reseller_discount_str} EUR\n"
        if general_discount_amount > Decimal('0.0'):
            general_discount_str = format_currency(general_discount_amount)
            general_code = applied_discount_info.get('code', 'Discount')
            reserved_msg += f"{EMOJI_DISCOUNT} {lang_data.get('discount_applied_label', 'Discount Applied')} ({general_code}): -{general_discount_str} EUR\n"

        final_total_str = format_currency(final_total)
        reserved_msg += pay_msg_template.format(amount=final_total_str) # Total to pay

        district_btn_text = district[:15]

        keyboard = [
            [InlineKeyboardButton(f"💳 {pay_now_button_text}", callback_data="confirm_pay"), InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill")],
            [InlineKeyboardButton(f"{basket_emoji} {view_basket_button_text} ({len(current_basket_list)})", callback_data="view_basket"), InlineKeyboardButton(f"{basket_emoji} {clear_basket_button_text}", callback_data="clear_basket")],
            [InlineKeyboardButton(f"{EMOJI_DISCOUNT} {apply_discount_button_text}", callback_data="apply_discount_start")],
            [InlineKeyboardButton(f"➕ {shop_more_button_text} ({district_btn_text})", callback_data=f"dist|{city_id}|{dist_id}")],
            [InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]
        ]
        await query.edit_message_text(reserved_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"DB error adding product {product_id_reserved if product_id_reserved else 'N/A'} user {user_id}: {e}", exc_info=True)
        await query.edit_message_text(f"❌ {error_adding_db}", parse_mode=None)
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"Unexpected error adding item user {user_id}: {e}", exc_info=True)
        await query.edit_message_text(f"❌ {error_adding_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()

# --- END handle_add_to_basket ---


# --- Profile Handlers ---
async def handle_profile(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    theme_name = context.user_data.get("theme", "default")
    theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT balance, total_purchases FROM users WHERE user_id = %s", (user_id,))
        result = c.fetchone()
        if not result: logger.error(f"User {user_id} not found in DB for profile."); await query.edit_message_text("❌ Error: Could not load profile.", parse_mode=None); return
        balance, purchases = Decimal(str(result['balance'])), result['total_purchases']

        # Call synchronous clear_expired_basket (no await needed)
        clear_expired_basket(context, user_id) # Assuming clear_expired_basket is synchronous
        basket_count = len(context.user_data.get("basket", []))
        status = get_user_status(purchases); progress_bar = get_progress_bar(purchases); balance_str = format_currency(balance)
        status_label = lang_data.get("status_label", "Status"); balance_label = lang_data.get("balance_label", "Balance")
        purchases_label = lang_data.get("purchases_label", "Total Purchases"); basket_label = lang_data.get("basket_label", "Basket Items")
        profile_title = lang_data.get("profile_title", "Your Profile")
        profile_msg = (f"🎉 {profile_title}\n\n" f"👤 {status_label}: {status} {progress_bar}\n" f"💰 {balance_label}: {balance_str} EUR\n"
                       f"📦 {purchases_label}: {purchases}\n" f"🛒 {basket_label}: {basket_count}")

        top_up_button_text = lang_data.get("top_up_button", "Top Up"); view_basket_button_text = lang_data.get("view_basket_button", "View Basket")
        purchase_history_button_text = lang_data.get("purchase_history_button", "Purchase History"); home_button_text = lang_data.get("home_button", "Home")
        keyboard = [
            [InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill"), InlineKeyboardButton(f"{basket_emoji} {view_basket_button_text} ({basket_count})", callback_data="view_basket")],
            [InlineKeyboardButton(f"📜 {purchase_history_button_text}", callback_data="view_history")],
            [InlineKeyboardButton("👑 VIP Status", callback_data="vip_status_menu"), InlineKeyboardButton("🎁 Referral Program", callback_data="referral_menu")],
            [InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]
        ]
        await query.edit_message_text(profile_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e: logger.error(f"DB error loading profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Failed to Load Profile.", parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Unexpected BadRequest handle_profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
        else: await query.answer()
    except Exception as e: logger.error(f"Unexpected error handle_profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
    finally:
        if conn: conn.close()

# --- Discount Validation (Synchronous - Adjusted for base total) ---
def validate_discount_code(code_text: str, base_total_float: float) -> tuple[bool, str, dict | None]:
    """ Validates a general discount code against a base total (which should be after reseller discounts). """
    lang_data = LANGUAGES.get('en', {}) # Use English for internal messages
    no_code_msg = lang_data.get("no_code_provided", "No code provided.")
    not_found_msg = lang_data.get("discount_code_not_found", "Discount code not found.")
    inactive_msg = lang_data.get("discount_code_inactive", "This discount code is inactive.")
    expired_msg = lang_data.get("discount_code_expired", "This discount code has expired.")
    invalid_expiry_msg = lang_data.get("invalid_code_expiry_data", "Invalid code expiry data.")
    limit_reached_msg = lang_data.get("code_limit_reached", "Code reached usage limit.")
    internal_error_type_msg = lang_data.get("internal_error_discount_type", "Internal error processing discount type.")
    db_error_msg = lang_data.get("db_error_validating_code", "Database error validating code.")
    unexpected_error_msg = lang_data.get("unexpected_error_validating_code", "An unexpected error occurred.")
    code_applied_msg_template = lang_data.get("code_applied_message", "Code '{code}' ({value}) applied. Discount: -{amount} EUR")

    if not code_text: return False, no_code_msg, None
    
    # Normalize the code: strip whitespace and convert to uppercase for case-insensitive lookup
    normalized_code = code_text.strip().upper()
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Use case-insensitive search by converting both to uppercase
        c.execute("SELECT * FROM discount_codes WHERE UPPER(code) = %s", (normalized_code,))
        code_data = c.fetchone()

        if not code_data: return False, not_found_msg, None
        if not code_data['is_active']: return False, inactive_msg, None
        # Enhanced expiry date validation
        if code_data['expiry_date']:
            try:
                expiry_date_str = code_data['expiry_date']
                # Handle different date formats and ensure UTC
                if 'T' in expiry_date_str:
                    # ISO format
                    if expiry_date_str.endswith('Z'):
                        expiry_dt = datetime.fromisoformat(expiry_date_str.replace('Z', '+00:00'))
                    elif '+' in expiry_date_str or expiry_date_str.count('-') > 2:
                        expiry_dt = datetime.fromisoformat(expiry_date_str)
                    else:
                        # No timezone info, assume UTC
                        expiry_dt = datetime.fromisoformat(expiry_date_str).replace(tzinfo=timezone.utc)
                else:
                    # Date only format, set to end of day UTC
                    date_only = datetime.strptime(expiry_date_str, '%Y-%m-%d')
                    expiry_dt = date_only.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                
                if datetime.now(timezone.utc) > expiry_dt: 
                    logger.info(f"Discount code '{code_data['code']}' expired on {expiry_dt}")
                    return False, expired_msg, None
            except (ValueError, TypeError) as e: 
                logger.warning(f"Invalid expiry_date format DB code {code_data['code']}: {e}")
                return False, invalid_expiry_msg, None
        if code_data['max_uses'] is not None and code_data['uses_count'] >= code_data['max_uses']: 
            logger.info(f"Discount code '{code_data['code']}' reached usage limit: {code_data['uses_count']}/{code_data['max_uses']}")
            return False, limit_reached_msg, None

        # Check minimum order amount if specified (add this column to DB if needed)
        try:
            min_order_amount = code_data['min_order_amount']
        except (KeyError, IndexError):
            min_order_amount = None
        if min_order_amount is not None and base_total_float < float(min_order_amount):
            logger.info(f"Discount code '{code_data['code']}' minimum order not met: {base_total_float} < {min_order_amount}")
            return False, lang_data.get("discount_min_order_not_met", "Minimum order amount not met for this discount code."), None

        discount_amount = Decimal('0.0')
        dtype = code_data['discount_type']; value = Decimal(str(code_data['value']))
        base_total_decimal = Decimal(str(base_total_float)) # Use the passed base total

        if dtype == 'percentage': discount_amount = (base_total_decimal * value) / Decimal('100.0')
        elif dtype == 'fixed': discount_amount = value
        else: logger.error(f"Unknown discount type '{dtype}' code {code_data['code']}"); return False, internal_error_type_msg, None

        # Ensure discount doesn't exceed the (potentially already reseller-discounted) base total
        discount_amount = min(discount_amount, base_total_decimal).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        final_total_decimal = (base_total_decimal - discount_amount).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        # Ensure final total is not negative
        final_total_decimal = max(Decimal('0.0'), final_total_decimal)

        discount_amount_float = float(discount_amount)
        final_total_float = float(final_total_decimal)

        details = {'code': code_data['code'], 'type': dtype, 'value': float(value), 'discount_amount': discount_amount_float, 'final_total': final_total_float}
        code_display = code_data['code']; value_str_display = format_discount_value(dtype, float(value))
        amount_str_display = format_currency(discount_amount_float)
        message = code_applied_msg_template.format(code=code_display, value=value_str_display, amount=amount_str_display)
        return True, message, details

    except sqlite3.Error as e: logger.error(f"DB error validating discount code '{code_text}': {e}", exc_info=True); return False, db_error_msg, None
    except Exception as e: logger.error(f"Unexpected error validating code '{code_text}': {e}", exc_info=True); return False, unexpected_error_msg, None
    finally:
        if conn: conn.close()

# --- END Discount Validation ---

# --- ATOMIC DISCOUNT CODE VALIDATION AND APPLICATION ---
def validate_and_apply_discount_atomic(code_text: str, base_total_float: float, user_id: int) -> tuple[bool, str, dict | None]:
    """
    ATOMIC: Validates AND applies discount code in a single database transaction.
    This prevents race conditions and multiple applications of the same code.
    """
    lang_data = LANGUAGES.get('en', {})
    no_code_msg = lang_data.get("no_code_provided", "No code provided.")
    not_found_msg = lang_data.get("discount_code_not_found", "Discount code not found.")
    inactive_msg = lang_data.get("discount_code_inactive", "This discount code is inactive.")
    expired_msg = lang_data.get("discount_code_expired", "This discount code has expired.")
    limit_reached_msg = lang_data.get("code_limit_reached", "Code reached usage limit.")
    already_used_msg = lang_data.get("code_already_used", "Discount code is not available. Please check the code or try again later.")
    internal_error_msg = lang_data.get("internal_error_discount_type", "Internal error processing discount type.")
    db_error_msg = lang_data.get("db_error_validating_code", "Database error validating code.")
    unexpected_error_msg = lang_data.get("unexpected_error_validating_code", "An unexpected error occurred.")
    code_applied_msg_template = lang_data.get("code_applied_message", "Code '{code}' ({value}) applied. Discount: -{amount} EUR")

    if not code_text: 
        return False, no_code_msg, None
    
    # Normalize the code
    normalized_code = code_text.strip().upper()
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # ATOMIC TRANSACTION: Validate and apply in one operation
        c.execute("BEGIN IMMEDIATE")
        
        # Note: Users can reuse discount codes multiple times until admin disables/deletes the code
        # No need to check for previous usage - allow unlimited reuse per user
        
        # Get and validate discount code
        c.execute("""
            SELECT * FROM discount_codes 
            WHERE UPPER(code) = %s
        """, (normalized_code,))
        code_data = c.fetchone()

        if not code_data: 
            conn.rollback()
            return False, not_found_msg, None
            
        if not code_data['is_active']: 
            conn.rollback()
            return False, inactive_msg, None
            
        # Check expiry
        if code_data['expiry_date']:
            try:
                expiry_date_str = code_data['expiry_date']
                if 'T' in expiry_date_str:
                    if expiry_date_str.endswith('Z'):
                        expiry_dt = datetime.fromisoformat(expiry_date_str.replace('Z', '+00:00'))
                    elif '+' in expiry_date_str or expiry_date_str.count('-') > 2:
                        expiry_dt = datetime.fromisoformat(expiry_date_str)
                    else:
                        expiry_dt = datetime.fromisoformat(expiry_date_str).replace(tzinfo=timezone.utc)
                else:
                    date_only = datetime.strptime(expiry_date_str, '%Y-%m-%d')
                    expiry_dt = date_only.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                
                if datetime.now(timezone.utc) > expiry_dt: 
                    logger.info(f"Discount code '{code_data['code']}' expired on {expiry_dt}")
                    conn.rollback()
                    return False, expired_msg, None
            except (ValueError, TypeError) as e: 
                logger.warning(f"Invalid expiry_date format DB code {code_data['code']}: {e}")
                conn.rollback()
                return False, "Invalid code expiry data.", None
                
        # Check usage limit
        if code_data['max_uses'] is not None and code_data['uses_count'] >= code_data['max_uses']: 
            logger.info(f"Discount code '{code_data['code']}' reached usage limit: {code_data['uses_count']}/{code_data['max_uses']}")
            conn.rollback()
            return False, limit_reached_msg, None

        # Check minimum order amount
        try:
            min_order_amount = code_data['min_order_amount']
        except (KeyError, IndexError):
            min_order_amount = None
        if min_order_amount is not None and base_total_float < float(min_order_amount):
            logger.info(f"Discount code '{code_data['code']}' minimum order not met: {base_total_float} < {min_order_amount}")
            conn.rollback()
            return False, "Minimum order amount not met for this discount code.", None

        # Calculate discount
        discount_amount = Decimal('0.0')
        dtype = code_data['discount_type']
        value = Decimal(str(code_data['value']))
        base_total_decimal = Decimal(str(base_total_float))

        if dtype == 'percentage': 
            discount_amount = (base_total_decimal * value) / Decimal('100.0')
        elif dtype == 'fixed': 
            discount_amount = value
        else: 
            logger.error(f"Unknown discount type '{dtype}' code {code_data['code']}")
            conn.rollback()
            return False, internal_error_msg, None

        # Ensure discount doesn't exceed base total
        discount_amount = min(discount_amount, base_total_decimal).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        final_total_decimal = (base_total_decimal - discount_amount).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        final_total_decimal = max(Decimal('0.0'), final_total_decimal)

        # ATOMIC: Record usage and increment usage count
        c.execute("""
            INSERT INTO discount_code_usage (user_id, code, used_at, discount_amount)
            VALUES (%s, %s, %s, %s)
        """, (user_id, normalized_code, datetime.now(timezone.utc).isoformat(), float(discount_amount)))
        
        c.execute("""
            UPDATE discount_codes 
            SET uses_count = uses_count + 1 
            WHERE UPPER(code) = %s
        """, (normalized_code,))
        
        # Commit the transaction
        conn.commit()
        
        # Prepare response
        discount_amount_float = float(discount_amount)
        final_total_float = float(final_total_decimal)
        details = {
            'code': code_data['code'], 
            'type': dtype, 
            'value': float(value), 
            'discount_amount': discount_amount_float, 
            'final_total': final_total_float
        }
        
        code_display = code_data['code']
        value_str_display = format_discount_value(dtype, float(value))
        amount_str_display = format_currency(discount_amount_float)
        message = code_applied_msg_template.format(code=code_display, value=value_str_display, amount=amount_str_display)
        
        logger.info(f"ATOMIC: User {user_id} successfully applied discount code '{normalized_code}'. Discount: {discount_amount_float:.2f} EUR")
        return True, message, details

    except sqlite3.Error as e: 
        logger.error(f"DB error in atomic discount validation '{code_text}': {e}", exc_info=True)
        if conn: conn.rollback()
        return False, db_error_msg, None
    except Exception as e: 
        logger.error(f"Unexpected error in atomic discount validation '{code_text}': {e}", exc_info=True)
        if conn: conn.rollback()
        return False, unexpected_error_msg, None
    finally:
        if conn: conn.close()

# --- END ATOMIC DISCOUNT VALIDATION ---

# --- Basket Handlers ---
# <<< MODIFIED: Incorporate Reseller Discount Calculation & Display >>>
async def handle_view_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    theme_name = context.user_data.get("theme", "default"); theme = THEMES.get(theme_name, THEMES["default"]); basket_emoji = theme.get('basket', EMOJI_BASKET)
    reseller_discount_label = lang_data.get("reseller_discount_label", "Reseller Discount")

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])

    if not basket:
        context.user_data.pop('applied_discount', None)
        basket_empty_msg = lang_data.get("basket_empty", "🛒 Your Basket is Empty!")
        add_items_prompt = lang_data.get("add_items_prompt", "Add items to start shopping!")
        shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
        full_empty_msg = basket_empty_msg + "\n\n" + add_items_prompt + " 😊"
        keyboard = [[InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        try: await query.edit_message_text(full_empty_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower(): logger.error(f"Error editing empty basket msg: {e}")
             else: await query.answer()
        return

    msg = f"{basket_emoji} {lang_data.get('your_basket_title', 'Your Basket')}\n\n"
    keyboard_items = []
    product_db_details = {}
    conn = None

    basket_original_total = Decimal('0.0')
    total_reseller_discount_amount = Decimal('0.0')
    total_after_reseller = Decimal('0.0')
    basket_items_with_details = []

    product_ids_in_basket = list(set(item.get('product_id') for item in basket if item.get('product_id')))
    if product_ids_in_basket:
        try:
            conn = get_db_connection()
            c = conn.cursor()
            placeholders = ','.join('%s' for _ in product_ids_in_basket)
            c.execute(f"SELECT id, name, size FROM products WHERE id IN ({placeholders})", product_ids_in_basket)
            product_db_details = {row['id']: {'name': row['name'], 'size': row['size']} for row in c.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"DB error fetching product names/sizes for basket view user {user_id}: {e}")
        finally:
            if conn: conn.close(); conn = None

    items_to_process_count = 0
    for item in basket:
        prod_id = item.get('product_id')
        original_price = item.get('price')
        product_type = item.get('product_type')
        timestamp = item.get('timestamp')

        if prod_id is None or original_price is None or product_type is None or timestamp is None:
            logger.warning(f"Skipping malformed item in basket context user {user_id}: {item}")
            continue

        items_to_process_count += 1
        basket_original_total += original_price
        item_reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, product_type)
        item_reseller_discount = (original_price * item_reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        item_price_after_reseller = original_price - item_reseller_discount
        total_reseller_discount_amount += item_reseller_discount
        total_after_reseller += item_price_after_reseller
        db_info = product_db_details.get(prod_id, {})
        basket_items_with_details.append({
            'id': prod_id, 'type': product_type, 'name': db_info.get('name', f'P{prod_id}'),
            'size': db_info.get('size', '%s'), 'original_price': original_price,
            'discounted_price': item_price_after_reseller, 'timestamp': timestamp,
            'has_reseller_discount': item_reseller_discount > Decimal('0.0')
        })

    if items_to_process_count == 0:
        context.user_data['basket'] = []
        context.user_data.pop('applied_discount', None);
        basket_empty_msg = lang_data.get("basket_empty", "🛒 Your Basket is Empty!"); items_expired_note = lang_data.get("items_expired_note", "Items may have expired or were removed.")
        shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
        full_empty_msg = basket_empty_msg + "\n\n" + items_expired_note
        keyboard = [[InlineKeyboardButton(f"🛍️ {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"🏠 {home_button_text}", callback_data="back_start")]]; await query.edit_message_text(full_empty_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return


    final_total = total_after_reseller
    general_discount_amount = Decimal('0.0')
    applied_discount_info = context.user_data.get('applied_discount')
    discount_code_to_revalidate = applied_discount_info.get('code') if applied_discount_info else None
    discount_applied_str = ""
    discount_removed_note_template = lang_data.get("discount_removed_note", "Discount code {code} removed: {reason}")

    if discount_code_to_revalidate:
        # Note: For revalidation of existing discounts, we don't use atomic to avoid double-charging
        code_valid, validation_message, discount_details = validate_discount_code(discount_code_to_revalidate, float(total_after_reseller))
        if code_valid and discount_details:
            general_discount_amount = Decimal(str(discount_details['discount_amount']))
            final_total = Decimal(str(discount_details['final_total']))
            discount_code = discount_code_to_revalidate
            discount_value_str = format_discount_value(discount_details['type'], discount_details['value'])
            discount_amount_str = format_currency(general_discount_amount)
            discount_applied_str = (f"\n{EMOJI_DISCOUNT} {lang_data.get('discount_applied_label', 'Discount Applied')} ({discount_code}: {discount_value_str}): -{discount_amount_str} EUR")
            context.user_data['applied_discount'] = {'code': discount_code_to_revalidate, 'amount': float(general_discount_amount), 'final_total': float(final_total)}
        else:
            context.user_data.pop('applied_discount', None)
            logger.info(f"General Discount '{discount_code_to_revalidate}' invalidated for user {user_id} in basket view. Reason: {validation_message}")
            discount_applied_str = f"\n{discount_removed_note_template.format(code=discount_code_to_revalidate, reason=validation_message)}"
            await query.answer("Applied discount code removed (basket changed).", show_alert=False)

    expires_in_label = lang_data.get("expires_in_label", "Expires in"); remove_button_label = lang_data.get("remove_button_label", "Remove")
    current_time = time.time()

    for index, item_detail in enumerate(basket_items_with_details):
        prod_id = item_detail['id']
        product_emoji = PRODUCT_TYPES.get(item_detail['type'], DEFAULT_PRODUCT_EMOJI)
        item_desc_base = f"{product_emoji} {item_detail['name']} {item_detail['size']}"
        price_display = format_currency(item_detail['discounted_price'])
        if item_detail['has_reseller_discount']:
            original_price_formatted = format_currency(item_detail['original_price'])
            price_display += f" (Orig: {original_price_formatted}€)"
        timestamp = item_detail['timestamp']
        remaining_time = max(0, int(BASKET_TIMEOUT - (current_time - timestamp)))
        time_str = f"{remaining_time // 60} min {remaining_time % 60} sec"
        msg += (f"{index + 1}. {item_desc_base} ({price_display})\n"
                f"   ⏳ {expires_in_label}: {time_str}\n")
        remove_button_text = f"🗑️ {remove_button_label} {item_desc_base}"[:60]
        keyboard_items.append([InlineKeyboardButton(remove_button_text, callback_data=f"remove|{prod_id}")])

    subtotal_label = lang_data.get("subtotal_label", "Subtotal"); total_label = lang_data.get("total_label", "Total")
    basket_original_total_str = format_currency(basket_original_total)
    final_total_str = format_currency(final_total)
    msg += f"\n{subtotal_label}: {basket_original_total_str} EUR"
    if total_reseller_discount_amount > Decimal('0.0'):
        reseller_discount_str = format_currency(total_reseller_discount_amount)
        msg += f"\n{EMOJI_DISCOUNT} {reseller_discount_label}: -{reseller_discount_str} EUR"
    msg += discount_applied_str
    msg += f"\n💳 **{total_label}: {final_total_str} EUR**"

    pay_now_button_text = lang_data.get("pay_now_button", "Pay Now"); clear_all_button_text = lang_data.get("clear_all_button", "Clear All")
    remove_discount_button_text = lang_data.get("remove_discount_button", "Remove Discount"); apply_discount_button_text = lang_data.get("apply_discount_button", "Apply Discount Code")
    shop_more_button_text = lang_data.get("shop_more_button", "Shop More"); home_button_text = lang_data.get("home_button", "Home")

    action_buttons = [
        [InlineKeyboardButton(f"💳 {pay_now_button_text}", callback_data="confirm_pay"), InlineKeyboardButton(f"{basket_emoji} {clear_all_button_text}", callback_data="clear_basket")],
        *([[InlineKeyboardButton(f"❌ {remove_discount_button_text}", callback_data="remove_discount")]] if context.user_data.get('applied_discount') else []),
        [InlineKeyboardButton(f"{EMOJI_DISCOUNT} {apply_discount_button_text}", callback_data="apply_discount_start")],
        [InlineKeyboardButton(f"{EMOJI_SHOP} {shop_more_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]
    ]
    final_keyboard = keyboard_items + action_buttons

    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(final_keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
         if "message is not modified" not in str(e).lower():
             logger.error(f"Error editing basket view message: {e}")
         else:
             try: await query.answer()
             except telegram_error.BadRequest: logger.debug(f"Query answer failed after 'message not modified' for user {user_id} (likely too old).")
             except Exception as ans_e: logger.warning(f"Error answering query after 'message not modified' for user {user_id}: {ans_e}")
    except Exception as e:
         logger.error(f"Unexpected error viewing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)

# --- END handle_view_basket ---


# --- Discount Application Handlers ---
async def apply_discount_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    if not basket: no_items_message = lang_data.get("discount_no_items", "Your basket is empty."); await query.answer(no_items_message, show_alert=True); return await handle_view_basket(update, context)

    context.user_data['state'] = 'awaiting_user_discount_code'
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="view_basket")]]
    enter_code_prompt = lang_data.get("enter_discount_code_prompt", "Please enter your discount code:")
    await query.edit_message_text(f"{EMOJI_DISCOUNT} {enter_code_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer(lang_data.get("enter_code_answer", "Enter code in chat."))

async def remove_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Removes a *general* discount code."""
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    if 'applied_discount' in context.user_data:
        removed_code = context.user_data.pop('applied_discount')['code']
        logger.info(f"User {user_id} removed general discount code '{removed_code}'.")
        discount_removed_answer = lang_data.get("discount_removed_answer", "Discount removed.")
        await query.answer(discount_removed_answer)
    else: no_discount_answer = lang_data.get("no_discount_answer", "No discount applied."); await query.answer(no_discount_answer, show_alert=False)
    await handle_view_basket(update, context) # Refresh basket view

# <<< MODIFIED: Calculate base total AFTER reseller discounts >>>
async def handle_user_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles user entering a general discount code."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_user_discount_code": return
    if not update.message or not update.message.text: send_text_please = lang_data.get("send_text_please", "Please send the code as text."); await send_message_with_retry(context.bot, chat_id, send_text_please, parse_mode=None); return

    entered_code = update.message.text.strip()
    context.user_data.pop('state', None)
    view_basket_button_text = lang_data.get("view_basket_button", "View Basket"); returning_to_basket_msg = lang_data.get("returning_to_basket", "Returning to basket.")

    if not entered_code: no_code_entered_msg = lang_data.get("no_code_entered", "No code entered."); await send_message_with_retry(context.bot, chat_id, no_code_entered_msg, parse_mode=None); keyboard = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    total_after_reseller_decimal = Decimal('0.0') # <<< Base total for validation

    if basket:
         try:
            for item in basket:
                original_price = item.get('price', Decimal('0.0'))
                product_type = item.get('product_type', '')
                reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, product_type)
                item_reseller_discount = (original_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                total_after_reseller_decimal += (original_price - item_reseller_discount)
         except Exception as e:
             logger.error(f"Error recalculating reseller-adjusted total user {user_id}: {e}"); error_calc_total = lang_data.get("error_calculating_total", "Error calculating total."); await send_message_with_retry(context.bot, chat_id, f"❌ {error_calc_total}", parse_mode=None); kb = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None); return
    else:
        basket_empty_no_discount = lang_data.get("basket_empty_no_discount", "Basket empty. Cannot apply code."); await send_message_with_retry(context.bot, chat_id, basket_empty_no_discount, parse_mode=None); kb = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None); return

    # SECURITY: Use atomic validation to prevent race conditions and multiple uses
    code_valid, validation_message, discount_details = validate_and_apply_discount_atomic(entered_code, float(total_after_reseller_decimal), user_id)

    if code_valid and discount_details:
        context.user_data['applied_discount'] = {'code': entered_code, 'amount': discount_details['discount_amount'], 'final_total': discount_details['final_total']}
        logger.info(f"User {user_id} applied general discount code '{entered_code}'.")
        success_label = lang_data.get("success_label", "Success!")
        feedback_msg = f"✅ {success_label} {validation_message}"
    else:
        context.user_data.pop('applied_discount', None)
        logger.warning(f"User {user_id} failed to apply general code '{entered_code}': {validation_message}")
        feedback_msg = f"❌ {validation_message}"

    keyboard = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]
    await send_message_with_retry(context.bot, chat_id, feedback_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

# --- END handle_user_discount_code_message ---


# --- Remove From Basket ---
async def handle_remove_from_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    if not params: logger.warning(f"handle_remove_from_basket no product_id user {user_id}."); await query.answer("Error: Product ID missing.", show_alert=True); return
    try: product_id_to_remove = int(params[0])
    except ValueError: logger.warning(f"Invalid product_id format user {user_id}: {params[0]}"); await query.answer("Error: Invalid product data.", show_alert=True); return

    logger.info(f"Attempting remove product {product_id_to_remove} user {user_id}.")
    item_removed_from_context = False; item_to_remove_str = None; conn = None
    current_basket_context = context.user_data.get("basket", []); new_basket_context = []
    found_item_index = -1

    for index, item in enumerate(current_basket_context):
        if item.get('product_id') == product_id_to_remove:
            found_item_index = index
            try: timestamp_float = float(item['timestamp']); item_to_remove_str = f"{item['product_id']}:{timestamp_float}"
            except (ValueError, TypeError, KeyError) as e: logger.error(f"Invalid format in context item {item}: {e}"); item_to_remove_str = None
            break

    if found_item_index != -1:
        item_removed_from_context = True
        new_basket_context = current_basket_context[:found_item_index] + current_basket_context[found_item_index+1:]
        logger.debug(f"Found item {product_id_to_remove} in context user {user_id}. DB String: {item_to_remove_str}")
    else: logger.warning(f"Product {product_id_to_remove} not in user_data basket user {user_id}."); new_basket_context = list(current_basket_context)

    try:
        conn = get_db_connection()
        c = conn.cursor(); c.execute("BEGIN")
        if item_removed_from_context:
             c.execute("UPDATE products SET reserved = GREATEST(0, reserved - 1) WHERE id = %s", (product_id_to_remove,))
             if c.rowcount > 0: logger.debug(f"Decremented reservation P{product_id_to_remove}.")
             else: logger.warning(f"Could not find P{product_id_to_remove} to decrement reservation (maybe already cleared%s).")
        c.execute("SELECT basket FROM users WHERE user_id = %s", (user_id,))
        db_basket_result = c.fetchone(); db_basket_str = db_basket_result['basket'] if db_basket_result else ''
        if db_basket_str and item_to_remove_str:
            items_list = db_basket_str.split(',')
            if item_to_remove_str in items_list:
                items_list.remove(item_to_remove_str); new_db_basket_str = ','.join(items_list)
                c.execute("UPDATE users SET basket = %s WHERE user_id = %s", (new_db_basket_str, user_id)); logger.debug(f"Updated DB basket user {user_id} to: {new_db_basket_str}")
            else: logger.warning(f"Item string '{item_to_remove_str}' not found in DB basket '{db_basket_str}' user {user_id}.")
        elif item_removed_from_context and not item_to_remove_str: logger.warning(f"Could not construct item string for DB removal P{product_id_to_remove}.")
        elif not item_removed_from_context: logger.debug(f"Item {product_id_to_remove} not in context, DB basket not modified.")
        conn.commit()
        logger.info(f"DB ops complete remove P{product_id_to_remove} user {user_id}.")
        context.user_data['basket'] = new_basket_context

        if not context.user_data['basket']:
            context.user_data.pop('applied_discount', None)
        elif context.user_data.get('applied_discount'):
            applied_discount_info = context.user_data['applied_discount']
            total_after_reseller_decimal = Decimal('0.0')
            for item in context.user_data['basket']:
                original_price = item.get('price', Decimal('0.0'))
                product_type = item.get('product_type', '')
                reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, product_type)
                item_reseller_discount = (original_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                total_after_reseller_decimal += (original_price - item_reseller_discount)
            code_valid, validation_message, _ = validate_discount_code(applied_discount_info['code'], float(total_after_reseller_decimal))
            if not code_valid:
                reason_removed = lang_data.get("discount_removed_invalid_basket", "Discount removed (basket changed).")
                logger.info(f"Removing invalid general discount '{applied_discount_info['code']}' for user {user_id} after item removal.")
                context.user_data.pop('applied_discount', None);
                await query.answer(reason_removed, show_alert=False)

    except sqlite3.Error as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"DB error removing item {product_id_to_remove} user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Failed to remove item (DB).", parse_mode=None); return
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"Unexpected error removing item {product_id_to_remove} user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue removing item.", parse_mode=None); return
    finally:
        if conn: conn.close()
    await handle_view_basket(update, context)
# --- END handle_remove_from_basket ---


async def handle_clear_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    conn = None

    current_basket_context = context.user_data.get("basket", [])
    if not current_basket_context: already_empty_msg = lang_data.get("basket_already_empty", "Basket already empty."); await query.answer(already_empty_msg, show_alert=False); return await handle_view_basket(update, context)

    product_ids_to_release_counts = Counter(item['product_id'] for item in current_basket_context)

    try:
        conn = get_db_connection()
        c = conn.cursor(); c.execute("BEGIN"); c.execute("UPDATE users SET basket = '' WHERE user_id = %s", (user_id,))
        if product_ids_to_release_counts:
             decrement_data = [(count, pid) for pid, count in product_ids_to_release_counts.items()]
             c.executemany("UPDATE products SET reserved = GREATEST(0, reserved - %s) WHERE id = %s", decrement_data)
             total_items_released = sum(product_ids_to_release_counts.values()); logger.info(f"Released {total_items_released} reservations user {user_id} clear.")
        conn.commit()
        context.user_data["basket"] = []; context.user_data.pop('applied_discount', None)
        logger.info(f"Cleared basket/discount user {user_id}.")
        shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
        cleared_msg = lang_data.get("basket_cleared", "🗑️ Basket Cleared!")
        keyboard = [[InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        await query.edit_message_text(cleared_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"DB error clearing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: DB issue clearing basket.", parse_mode=None)
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"Unexpected error clearing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
    finally:
        if conn: conn.close()


# --- Confirm Pay Handler (Modified for Reseller Discount) ---
async def handle_confirm_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Pay Now' button press from the basket."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang, lang_data = _get_lang_data(context)

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    applied_discount_info = context.user_data.get('applied_discount')

    if not basket:
        await query.answer("Your basket is empty!", show_alert=True)
        await handle_view_basket(update, context)
        return

    conn = None
    original_total = Decimal('0.0')
    total_after_reseller = Decimal('0.0')
    final_total = Decimal('0.0')
    valid_basket_items_snapshot = []
    discount_code_to_use = None
    user_balance = Decimal('0.0')
    error_occurred = False

    try:
        conn = get_db_connection()
        c = conn.cursor()

        product_ids_in_basket = list(set(item['product_id'] for item in basket))
        if not product_ids_in_basket:
             logger.warning(f"Basket context had items, but no product IDs found for user {user_id}.")
             await query.answer("Basket empty after validation.", show_alert=True)
             await handle_view_basket(update, context)
             return

        placeholders = ','.join('%s' for _ in product_ids_in_basket)
        # MODIFIED: Fetch city, district, original_text
        c.execute(f"SELECT id, price, name, size, product_type, city, district, original_text FROM products WHERE id IN ({placeholders})", product_ids_in_basket)
        product_db_details = {row['id']: dict(row) for row in c.fetchall()}

        for item_context in basket:
             prod_id = item_context.get('product_id')
             if prod_id in product_db_details:
                 details = product_db_details[prod_id]
                 item_original_price = Decimal(str(details['price']))
                 item_product_type = details['product_type']
                 original_total += item_original_price
                 item_reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, item_product_type)
                 item_reseller_discount = (item_original_price * item_reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                 item_price_after_reseller = item_original_price - item_reseller_discount
                 total_after_reseller += item_price_after_reseller
                 # MODIFIED: Enrich item_snapshot
                 item_snapshot = {
                     "product_id": prod_id, "price": float(item_original_price),
                     "name": details['name'], "size": details['size'],
                     "product_type": item_product_type,
                     "city": details['city'],
                     "district": details['district'],
                     "original_text": details.get('original_text')
                 }
                 valid_basket_items_snapshot.append(item_snapshot)
             else: logger.warning(f"Product {prod_id} missing during payment confirm user {user_id} (DB fetch).")

        if not valid_basket_items_snapshot:
             context.user_data['basket'] = []
             context.user_data.pop('applied_discount', None)
             logger.warning(f"All items unavailable user {user_id} payment confirm.")
             keyboard_back = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
             try: await query.edit_message_text("❌ Error: All items unavailable.", reply_markup=InlineKeyboardMarkup(keyboard_back), parse_mode=None)
             except telegram_error.BadRequest: await send_message_with_retry(context.bot, chat_id, "❌ Error: All items unavailable.", reply_markup=InlineKeyboardMarkup(keyboard_back), parse_mode=None)
             return

        final_total = total_after_reseller
        if applied_discount_info:
            # Note: For existing applied discounts, we don't re-validate atomically to avoid double-charging
            code_valid, _, discount_details = validate_discount_code(applied_discount_info['code'], float(total_after_reseller))
            if code_valid and discount_details:
                final_total = Decimal(str(discount_details['final_total']))
                discount_code_to_use = applied_discount_info.get('code')
                context.user_data['applied_discount']['final_total'] = float(final_total)
                context.user_data['applied_discount']['amount'] = discount_details['discount_amount']
            else:
                final_total = total_after_reseller
                discount_code_to_use = None
                context.user_data.pop('applied_discount', None)
                await query.answer("Applied discount code became invalid.", show_alert=True)

        if final_total < Decimal('0.0'): final_total = Decimal('0.0')
        c.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
        balance_result = c.fetchone()
        user_balance = Decimal(str(balance_result['balance'])) if balance_result else Decimal('0.0')
    except (sqlite3.Error, Exception) as e:
        logger.error(f"Error during payment confirm data processing user {user_id}: {e}", exc_info=True)
        error_occurred = True
        kb = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
        try: await query.edit_message_text("❌ Error preparing payment.", reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        except Exception as edit_err: logger.error(f"Failed to edit message in error handler: {edit_err}")
    finally:
        if conn: conn.close(); logger.debug("DB connection closed in handle_confirm_pay.")

    if error_occurred: return

    logger.info(f"Payment confirm user {user_id}. Final Total (after all discounts): {final_total:.2f}, Balance: {user_balance:.2f}. Basket Snapshot: {valid_basket_items_snapshot}")

    if user_balance >= final_total:
        logger.info(f"User {user_id} has sufficient balance ({user_balance:.2f} EUR) for purchase ({final_total:.2f} EUR). Processing balance payment.")
        success = await payment.process_purchase_with_balance(user_id, final_total, valid_basket_items_snapshot, discount_code_to_use, context)
        if success:
            try: pass # User notification handled in process_purchase_with_balance
            except telegram_error.BadRequest: pass
    else:
        logger.info(f"Insufficient balance user {user_id}. Prompting for crypto payment or discount.")
        context.user_data['basket_pay_snapshot'] = valid_basket_items_snapshot
        context.user_data['basket_pay_total_eur'] = float(final_total)
        context.user_data['basket_pay_discount_code'] = discount_code_to_use
        
        # Track reservation for abandonment cleanup  
        from utils import track_reservation
        track_reservation(user_id, valid_basket_items_snapshot, "basket")
        insufficient_msg_template = lang_data.get("insufficient_balance_pay_option", "⚠️ Insufficient Balance! ({balance} / {required} EUR)")
        insufficient_msg = insufficient_msg_template.format(balance=format_currency(user_balance), required=format_currency(final_total))
        prompt_msg = lang_data.get("prompt_discount_or_pay", "Do you have a discount code to apply before paying with crypto%s")
        pay_crypto_button = lang_data.get("pay_crypto_button", "💳 Pay with Crypto")
        apply_discount_button = lang_data.get("apply_discount_pay_button", "🏷️ Apply Discount Code")
        back_basket_button = lang_data.get("back_basket_button", "Back to Basket")
        keyboard = [
             [InlineKeyboardButton(pay_crypto_button, callback_data="skip_discount_basket_pay")],
             [InlineKeyboardButton(apply_discount_button, callback_data="apply_discount_basket_pay")],
             [InlineKeyboardButton(f"⬅️ {back_basket_button}", callback_data="view_basket")]
        ]
        await query.edit_message_text(f"{insufficient_msg}\n\n{prompt_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer()
# --- END handle_confirm_pay ---


# --- NEW: Handler to Ask for Discount Code in Basket Pay Flow ---
async def handle_apply_discount_basket_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    if 'basket_pay_snapshot' not in context.user_data or 'basket_pay_total_eur' not in context.user_data:
        logger.error(f"User {user_id} clicked apply_discount_basket_pay but context is missing.")
        await query.answer("Error: Context lost. Please go back to basket.", show_alert=True)
        return await handle_view_basket(update, context)

    context.user_data['state'] = 'awaiting_basket_discount_code'
    prompt_msg = lang_data.get("basket_pay_enter_discount", "Please enter discount code for this purchase:")
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="skip_discount_basket_pay")]]

    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter discount code in chat.")


# --- NEW: Message Handler for Basket Pay Discount Code ---
async def handle_basket_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_basket_discount_code": return
    if not update.message or not update.message.text: return

    entered_code = update.message.text.strip()
    context.user_data.pop('state', None)

    basket_snapshot = context.user_data.get('basket_pay_snapshot')
    total_after_reseller_float = context.user_data.get('basket_pay_total_eur')

    if basket_snapshot is None or total_after_reseller_float is None:
        logger.error(f"User {user_id} sent basket discount code but snapshot/total context is missing.")
        await send_message_with_retry(context.bot, chat_id, "Error: Context lost. Returning to basket.", parse_mode=None)
        context.user_data.pop('basket_pay_snapshot', None); context.user_data.pop('basket_pay_total_eur', None); context.user_data.pop('basket_pay_discount_code', None)
        return await handle_view_basket(update, context)

    if not entered_code:
        await send_message_with_retry(context.bot, chat_id, lang_data.get("no_code_entered", "No code entered."), parse_mode=None)
        await _show_crypto_choices_for_basket(update, context)
        return

    # SECURITY: Use atomic validation to prevent race conditions and multiple uses
    code_valid, validation_message, discount_details = validate_and_apply_discount_atomic(entered_code, total_after_reseller_float, user_id)
    feedback_msg_template = ""
    if code_valid and discount_details:
        new_final_total_float = discount_details['final_total']
        context.user_data['basket_pay_total_eur'] = new_final_total_float
        context.user_data['basket_pay_discount_code'] = entered_code
        logger.info(f"User {user_id} applied valid basket discount '{entered_code}'. New FINAL total for crypto: {new_final_total_float:.2f} EUR")
        feedback_msg_template = lang_data.get("basket_pay_code_applied", "✅ Code '{code}' applied. New total: {total} EUR. Choose crypto:")
        feedback_msg = feedback_msg_template.format(code=entered_code, total=format_currency(new_final_total_float))
    else:
        context.user_data['basket_pay_discount_code'] = None
        logger.warning(f"User {user_id} entered invalid basket discount '{entered_code}': {validation_message}")
        total_to_pay_str = format_currency(total_after_reseller_float)
        feedback_msg_template = lang_data.get("basket_pay_code_invalid", "❌ Code invalid: {reason}. Choose crypto to pay {total} EUR:")
        feedback_msg = feedback_msg_template.format(reason=validation_message, total=total_to_pay_str)

    try: await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception as e: logger.warning(f"Could not delete user's discount code message: {e}")

    await send_message_with_retry(context.bot, chat_id, feedback_msg, parse_mode=None)
    await _show_crypto_choices_for_basket(update, context)


# --- NEW: Handler to Skip Discount in Basket Pay Flow ---
async def handle_skip_discount_basket_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id

    if 'basket_pay_snapshot' not in context.user_data or 'basket_pay_total_eur' not in context.user_data:
        logger.error(f"User {user_id} clicked skip_discount_basket_pay but context is missing.")
        await query.answer("Error: Context lost. Please go back to basket.", show_alert=True)
        return await handle_view_basket(update, context)

    context.user_data['basket_pay_discount_code'] = None
    await query.answer("Proceeding to payment...")
    await _show_crypto_choices_for_basket(update, context, edit_message=True)


# --- NEW: Helper to Show Crypto Choices for Basket Payment ---
# <<< MODIFIED: Use SUPPORTED_CRYPTO dictionary >>>
async def _show_crypto_choices_for_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, edit_message: bool = False):
    """Auto-selects SOL for basket payment (skipping selection)."""
    
    # 1. Ensure context is correctly mapped for single-item flows
    is_single_item_flow = 'single_item_pay_final_eur' in context.user_data and 'single_item_pay_snapshot' in context.user_data
    
    if is_single_item_flow:
        # Map single item context to basket_pay context so handle_select_basket_crypto can use it
        context.user_data['basket_pay_snapshot'] = context.user_data['single_item_pay_snapshot']
        context.user_data['basket_pay_total_eur'] = context.user_data['single_item_pay_final_eur']
        context.user_data['basket_pay_discount_code'] = context.user_data.get('single_item_pay_discount_code')
        # Store single item flag if needed, or rely on handle_select_basket_crypto handling
    
    # 2. Auto-select SOL
    from payment import handle_select_basket_crypto
    await handle_select_basket_crypto(update, context, params=['sol'])

# --- END _show_crypto_choices_for_basket ---

# --- CORRECTED Handler for Pay Single Item Button ---
async def handle_pay_single_item(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Pay Now' button directly from product selection."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang, lang_data = _get_lang_data(context)

    if not params or len(params) < 5:
        logger.warning("handle_pay_single_item missing params."); await query.answer("Error: Incomplete product data.", show_alert=True); return
    city_id, dist_id, p_type, size, price_str = params # price_str is ORIGINAL price

    try: original_price = Decimal(price_str)
    except ValueError: logger.warning(f"Invalid price format pay_single_item: {price_str}"); await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None); return

    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district: error_location_mismatch = lang_data.get("error_location_mismatch", "Error: Location data mismatch."); await query.edit_message_text(f"❌ {error_location_mismatch}", parse_mode=None); return await handle_shop(update, context)

    await query.answer("⏳ Reserving & preparing payment options...")

    reserved_id = None
    conn = None
    product_details_for_snapshot = None
    error_occurred_reservation = False

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        # FIXED: Removed price from WHERE to use current price from database
        c.execute("SELECT id, name, price, size, product_type, city, district, original_text FROM products WHERE city = %s AND district = %s AND product_type = %s AND size = %s AND available > reserved ORDER BY id LIMIT 1", (city, district, p_type, size))
        product_to_reserve = c.fetchone()

        if not product_to_reserve:
            conn.rollback()
            logger.warning(f"Item {p_type} {size} in {city}/{district} taken before pay_single user {user_id}.")
            try: await query.edit_message_text("❌ Sorry, this item was just taken!", parse_mode=None)
            except Exception: pass
            error_occurred_reservation = True
        else:
            reserved_id = product_to_reserve['id']
            # Use CURRENT price from database
            current_price = Decimal(str(product_to_reserve['price']))
            original_price = current_price  # Update to use current price for payment
            product_details_for_snapshot = dict(product_to_reserve) # Now contains enriched data
            c.execute("UPDATE products SET reserved = reserved + 1 WHERE id = %s AND available > reserved", (reserved_id,))
            if c.rowcount == 1:
                conn.commit()
                logger.info(f"Successfully reserved product {reserved_id} for single item payment by user {user_id}.")
            else:
                conn.rollback()
                logger.warning(f"Failed to reserve product {reserved_id} (race condition%s) for single item payment user {user_id}.")
                try: await query.edit_message_text("❌ Sorry, this item was just taken!", parse_mode=None)
                except Exception: pass
                error_occurred_reservation = True
    except sqlite3.Error as e:
        logger.error(f"DB error reserving single item {p_type} {size} user {user_id}: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        try: await query.edit_message_text("❌ Database error during reservation.", parse_mode=None)
        except Exception: pass
        error_occurred_reservation = True
    finally:
        if conn: conn.close()

    if error_occurred_reservation:
        return

    if reserved_id and product_details_for_snapshot:
        # MODIFIED: Enrich single_item_snapshot
        single_item_snapshot = [{
            "product_id": reserved_id,
            "price": float(original_price),
            "name": product_details_for_snapshot['name'],
            "size": product_details_for_snapshot['size'],
            "product_type": product_details_for_snapshot['product_type'],
            "city": product_details_for_snapshot['city'],
            "district": product_details_for_snapshot['district'],
            "original_text": product_details_for_snapshot.get('original_text')
        }]

        reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, p_type)
        reseller_discount_amount = (original_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        price_after_reseller = original_price - reseller_discount_amount

        context.user_data['single_item_pay_snapshot'] = single_item_snapshot
        context.user_data['single_item_pay_final_eur'] = float(price_after_reseller)
        context.user_data['single_item_pay_discount_code'] = None
        context.user_data['single_item_pay_back_params'] = params
        
        # Track reservation for abandonment cleanup
        from utils import track_reservation
        track_reservation(user_id, single_item_snapshot, "single")

        # --- NEW: Check pre-applied discount ---
        pre_applied_code = context.user_data.pop('pre_applied_discount_code', None)
        if pre_applied_code:
            code_valid, validation_message, discount_details = validate_and_apply_discount_atomic(pre_applied_code, float(price_after_reseller), user_id)
            if code_valid and discount_details:
                new_final = discount_details['final_total']
                context.user_data['single_item_pay_final_eur'] = new_final
                context.user_data['single_item_pay_discount_code'] = pre_applied_code
                logger.info(f"Applied pre-entered discount code '{pre_applied_code}' for user {user_id}")
                
                # Check balance
                conn_bal = None
                try:
                    conn_bal = get_db_connection()
                    c_bal = conn_bal.cursor()
                    c_bal.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
                    bal_res = c_bal.fetchone()
                    user_balance = Decimal(str(bal_res['balance'])) if bal_res else Decimal('0.0')
                except: user_balance = Decimal('0.0')
                finally: 
                    if conn_bal: conn_bal.close()
                
                if user_balance >= Decimal(str(new_final)):
                    # Use balance
                    await payment.process_purchase_with_balance(user_id, Decimal(str(new_final)), single_item_snapshot, pre_applied_code, context)
                    # Clear context
                    context.user_data.pop('single_item_pay_snapshot', None)
                    context.user_data.pop('single_item_pay_final_eur', None)
                    context.user_data.pop('single_item_pay_discount_code', None)
                    context.user_data.pop('single_item_pay_back_params', None)
                    return
                else:
                    # Use crypto
                    await _show_crypto_choices_for_basket(update, context, edit_message=True)
                    return
        # ---------------------------------------

        # Skip intermediate menu since buttons are now on product view
        # Directly proceed to payment options (which handle_skip_discount_single_pay handles)
        await handle_skip_discount_single_pay(update, context)
    else:
        logger.error(f"Reached end of handle_pay_single_item without valid reservation for user {user_id}")
        try: await query.edit_message_text("❌ An internal error occurred during payment initiation.", parse_mode=None)
        except Exception: pass
# --- END handle_pay_single_item ---


# --- Other User Handlers ---
async def handle_view_history(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    history = fetch_last_purchases(user_id, limit=10)

    history_title = lang_data.get("purchase_history_title", "Purchase History"); no_history_msg = lang_data.get("no_purchases_yet", "No purchases yet.")
    recent_purchases_title = lang_data.get("recent_purchases_title", "Recent Purchases"); back_profile_button = lang_data.get("back_profile_button", "Back to Profile")
    home_button = lang_data.get("home_button", "Home"); unknown_date_label = lang_data.get("unknown_date_label", "Unknown Date")

    if not history: msg = f"📜 {history_title}\n\n{no_history_msg}"; keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_profile_button}", callback_data="profile"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
    else:
        msg = f"📜 {recent_purchases_title}\n\n"
        for i, purchase in enumerate(history):
            try:
                # Ensure purchase_date is treated as UTC if no timezone info
                dt_obj = datetime.fromisoformat(purchase['purchase_date'].replace('Z', '+00:00'))
                if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                # Convert to local time if needed, or keep as UTC/formatted
                date_str = dt_obj.strftime('%y-%m-%d %H:%M') # Shorter date format
            except (ValueError, TypeError):
                date_str = "%s%s%s"
            p_type = purchase.get('product_type', 'Product') # Use get with fallback
            p_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
            p_name = purchase.get('product_name', 'N/A') # Use name from purchase record if available
            p_size = purchase.get('product_size', 'N/A')
            p_price = format_currency(purchase.get('price_paid', 0))
            msg += f"  - {date_str}: {p_emoji} {p_size} ({p_price}€)\n" # Simplified item display
        keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_profile_button}", callback_data="profile"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]

    try: await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing history msg: {e}")
        else: await query.answer()


# --- Price List ---
async def handle_price_list(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)

    if not CITIES: no_cities_msg = lang_data.get("no_cities_for_prices", "No cities available."); keyboard = [[InlineKeyboardButton(f"{EMOJI_HOME} {lang_data.get('home_button', 'Home')}", callback_data="back_start")]]; await query.edit_message_text(f"{EMOJI_CITY} {no_cities_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return

    sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
    home_button_text = lang_data.get("home_button", "Home")
    keyboard = [[InlineKeyboardButton(f"{EMOJI_CITY} {CITIES.get(c, 'N/A')}", callback_data=f"price_list_city|{c}")] for c in sorted_city_ids if CITIES.get(c)]
    keyboard.append([InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")])
    price_list_title = lang_data.get("price_list_title", "Price List"); select_city_prompt = lang_data.get("select_city_prices_prompt", "Select a city:")
    
    try:
        await query.edit_message_text(f"{EMOJI_PRICELIST} {price_list_title}\n\n{select_city_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" in str(e).lower():
            # Message is already the same, just answer the callback
            await query.answer()
        else:
            logger.error(f"Error editing price list message: {e}")
            await query.answer("Error displaying price list.", show_alert=True)

async def handle_price_list_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    if not params: logger.warning("handle_price_list_city no city_id."); await query.answer("Error: City ID missing.", show_alert=True); return

    city_id = params[0]; city_name = CITIES.get(city_id)
    if not city_name: error_city_not_found = lang_data.get("error_city_not_found", "Error: City not found."); await query.edit_message_text(f"❌ {error_city_not_found}", parse_mode=None); return await handle_price_list(update, context)

    price_list_title_city_template = lang_data.get("price_list_title_city", "Price List: {city_name}"); msg = f"{EMOJI_PRICELIST} {price_list_title_city_template.format(city_name=city_name)}\n\n"
    found_products = False; conn = None

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT product_type, size, price, district, COUNT(*) as quantity FROM products WHERE city = %s AND available > reserved GROUP BY product_type, size, price, district ORDER BY product_type, price, size, district", (city_name,))
        results = c.fetchall()
        no_products_in_city = lang_data.get("no_products_in_city", "No products available here."); available_label = lang_data.get("available_label", "available")

        if not results: msg += no_products_in_city
        else:
            found_products = True
            grouped_data = defaultdict(lambda: defaultdict(list))
            for row in results: price_size_key = (Decimal(str(row['price'])), row['size']); grouped_data[row['product_type']][price_size_key].append((row['district'], row['quantity']))

            for p_type in sorted(grouped_data.keys()):
                type_data = grouped_data[p_type]; sorted_price_size = sorted(type_data.keys(), key=lambda x: (x[0], x[1]))
                prod_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
                for price, size in sorted_price_size:
                    districts_list = type_data[(price, size)]; price_str = format_currency(price)
                    msg += f"\n{prod_emoji} {p_type} {size} ({price_str}€)\n"
                    districts_list.sort(key=lambda x: x[0])
                    for district, quantity in districts_list: msg += f"  • {EMOJI_DISTRICT} {district}\n"

        back_city_list_button = lang_data.get("back_city_list_button", "Back to City List"); home_button = lang_data.get("home_button", "Home")
        keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_city_list_button}", callback_data="price_list"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]

        try:
            if len(msg) > 4000: truncated_note = lang_data.get("message_truncated_note", "Message truncated."); msg = msg[:4000] + f"\n\n✂️ ... {truncated_note}"; logger.warning(f"Price list message truncated {city_name}.")
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower():
                 logger.error(f"Error editing price list: {e}. Snippet: {msg[:200]}")
                 error_displaying_prices = lang_data.get("error_displaying_prices", "Error displaying prices.")
                 await query.answer(error_displaying_prices, show_alert=True)
             else:
                 await query.answer()

    except sqlite3.Error as e:
        logger.error(f"DB error fetching price list city {city_name}: {e}", exc_info=True)
        error_loading_prices_db_template = lang_data.get("error_loading_prices_db", "Error: DB Load Error {city_name}")
        await query.edit_message_text(f"❌ {error_loading_prices_db_template.format(city_name=city_name)}", parse_mode=None)
    except Exception as e:
        logger.error(f"Unexpected error price list city {city_name}: {e}", exc_info=True)
        error_unexpected_prices = lang_data.get("error_unexpected_prices", "Error: Unexpected issue.")
        await query.edit_message_text(f"❌ {error_unexpected_prices}", parse_mode=None)
    finally:
         if conn: conn.close()


# --- Review Handlers ---
async def handle_reviews_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    review_prompt = lang_data.get("reviews", "📝 Reviews Menu")
    view_reviews_button = lang_data.get("view_reviews_button", "View Reviews")
    leave_review_button = lang_data.get("leave_review_button", "Leave a Review")
    home_button = lang_data.get("home_button", "Home")
    keyboard = [
        [InlineKeyboardButton(f"👀 {view_reviews_button}", callback_data="view_reviews|0")],
        [InlineKeyboardButton(f"✍️ {leave_review_button}", callback_data="leave_review")],
        [InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(review_prompt, reply_markup=reply_markup, parse_mode=None)


async def handle_leave_review(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    context.user_data["state"] = "awaiting_review"
    enter_review_prompt = lang_data.get("enter_review_prompt", "Please type your review message and send it."); cancel_button_text = lang_data.get("cancel_button", "Cancel"); prompt_msg = f"✍️ {enter_review_prompt}"
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="reviews")]]
    try:
        await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        enter_review_answer = lang_data.get("enter_review_answer", "Enter your review in the chat.")
        await query.answer(enter_review_answer)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing leave review prompt: {e}"); await send_message_with_retry(context.bot, update.effective_chat.id, prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); await query.answer()
        else: await query.answer()
    except Exception as e: logger.error(f"Unexpected error handle_leave_review: {e}", exc_info=True); await query.answer("Error occurred.", show_alert=True)


async def handle_leave_review_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_review": return

    send_text_review_please = lang_data.get("send_text_review_please", "Please send text only for your review.")
    review_not_empty = lang_data.get("review_not_empty", "Review cannot be empty. Please try again or cancel.")
    review_too_long = lang_data.get("review_too_long", "Review is too long (max 1000 characters). Please shorten it.")
    review_thanks = lang_data.get("review_thanks", "Thank you for your review! Your feedback helps us improve.")
    error_saving_review_db = lang_data.get("error_saving_review_db", "Error: Could not save your review due to a database issue.")
    error_saving_review_unexpected = lang_data.get("error_saving_review_unexpected", "Error: An unexpected issue occurred while saving your review.")
    view_reviews_button = lang_data.get("view_reviews_button", "View Reviews")
    home_button = lang_data.get("home_button", "Home")

    if not update.message or not update.message.text:
        await send_message_with_retry(context.bot, chat_id, send_text_review_please, parse_mode=None)
        return

    review_text = update.message.text.strip()
    if not review_text:
        await send_message_with_retry(context.bot, chat_id, review_not_empty, parse_mode=None)
        return

    if len(review_text) > 1000:
         await send_message_with_retry(context.bot, chat_id, review_too_long, parse_mode=None)
         return

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO reviews (user_id, review_text, review_date) VALUES (%s, %s, %s)",
            (user_id, review_text, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        logger.info(f"User {user_id} left a review.")
        context.user_data.pop("state", None)

        success_msg = f"✅ {review_thanks}"
        keyboard = [[InlineKeyboardButton(f"👀 {view_reviews_button}", callback_data="view_reviews|0"),
                     InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
        await send_message_with_retry(context.bot, chat_id, success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        logger.error(f"DB error saving review user {user_id}: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
            except:
                pass
        context.user_data.pop("state", None)
        await send_message_with_retry(context.bot, chat_id, f"❌ {error_saving_review_db}", parse_mode=None)

    except Exception as e:
        logger.error(f"Unexpected error saving review user {user_id}: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
            except:
                pass
        context.user_data.pop("state", None)
        await send_message_with_retry(context.bot, chat_id, f"❌ {error_saving_review_unexpected}", parse_mode=None)

    finally:
        if conn: conn.close()

async def handle_view_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    offset = 0; reviews_per_page = 5
    if params and len(params) > 0 and params[0].isdigit(): offset = int(params[0])
    reviews_data = fetch_reviews(offset=offset, limit=reviews_per_page + 1)
    user_reviews_title = lang_data.get("user_reviews_title", "User Reviews"); no_reviews_yet = lang_data.get("no_reviews_yet", "No reviews yet."); no_more_reviews = lang_data.get("no_more_reviews", "No more reviews."); prev_button = lang_data.get("prev_button", "Prev"); next_button = lang_data.get("next_button", "Next"); back_review_menu_button = lang_data.get("back_review_menu_button", "Back to Reviews"); unknown_date_label = lang_data.get("unknown_date_label", "Unknown Date"); error_displaying_review = lang_data.get("error_displaying_review", "Error display"); error_updating_review_list = lang_data.get("error_updating_review_list", "Error updating list.")
    msg = f"{EMOJI_REVIEW} {user_reviews_title}\n\n"; keyboard = []
    if not reviews_data:
        if offset == 0: msg += no_reviews_yet; keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_review_menu_button}", callback_data="reviews")]]
        else: msg += no_more_reviews; keyboard = [[InlineKeyboardButton(f"⬅️ {prev_button}", callback_data=f"view_reviews|{max(0, offset - reviews_per_page)}")], [InlineKeyboardButton(f"{EMOJI_BACK} {back_review_menu_button}", callback_data="reviews")]]
    else:
        has_more = len(reviews_data) > reviews_per_page; reviews_to_show = reviews_data[:reviews_per_page]
        for review in reviews_to_show:
            try:
                date_str = review.get('review_date', '')
                formatted_date = unknown_date_label
                if date_str:
                    try: formatted_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')).strftime("%Y-%m-%d")
                    except ValueError: pass
                username = review.get('username', 'anonymous'); username_display = f"@{username}" if username and username != 'anonymous' else username
                review_text = review.get('review_text', ''); msg += f"{EMOJI_PROFILE} {username_display} ({formatted_date}):\n{review_text}\n\n"
            except Exception as e: logger.error(f"Error formatting review: {review}, Error: {e}"); msg += f"({error_displaying_review})\n\n"
        nav_buttons = []
        if offset > 0: nav_buttons.append(InlineKeyboardButton(f"⬅️ {prev_button}", callback_data=f"view_reviews|{max(0, offset - reviews_per_page)}"))
        if has_more: nav_buttons.append(InlineKeyboardButton(f"➡️ {next_button}", callback_data=f"view_reviews|{offset + reviews_per_page}"))
        if nav_buttons: keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_review_menu_button}", callback_data="reviews")])
    try: await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.warning(f"Failed edit view_reviews: {e}"); await query.answer(error_updating_review_list, show_alert=True)
        else: await query.answer()

async def handle_leave_review_now(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Callback handler specifically for the 'Leave Review Now' button after purchase."""
    await handle_leave_review(update, context, params)

# --- Refill Handlers ---
async def handle_refill(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang, lang_data = _get_lang_data(context)

    # SOL payment system is always available (no API keys needed)

    context.user_data['state'] = 'awaiting_refill_amount'
    logger.info(f"User {user_id} initiated refill process. State -> awaiting_refill_amount.")

    top_up_title = lang_data.get("top_up_title", "Top Up Balance")
    enter_refill_amount_prompt = lang_data.get("enter_refill_amount_prompt", "Please reply with the amount in EUR you wish to add to your balance (e.g., 10 or 25.50).")
    min_top_up_note_template = lang_data.get("min_top_up_note", "Minimum top up: {amount} EUR")
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    enter_amount_answer = lang_data.get("enter_amount_answer", "Enter the top-up amount.")

    min_amount_str = format_currency(MIN_DEPOSIT_EUR)
    min_top_up_note = min_top_up_note_template.format(amount=min_amount_str)
    prompt_msg = (f"{EMOJI_REFILL} {top_up_title}\n\n{enter_refill_amount_prompt}\n\n{min_top_up_note}")
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="profile")]]

    try:
        await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer(enter_amount_answer)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing refill prompt: {e}"); await send_message_with_retry(context.bot, chat_id, prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); await query.answer()
        else: await query.answer(enter_amount_answer)
    except Exception as e: logger.error(f"Unexpected error handle_refill: {e}", exc_info=True); error_occurred_answer = lang_data.get("error_occurred_answer", "An error occurred."); await query.answer(error_occurred_answer, show_alert=True)

# <<< MODIFIED: Use SUPPORTED_CRYPTO dictionary >>>
async def handle_refill_amount_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_refill_amount": logger.debug(f"Ignore msg user {user_id}, state: {state}"); return

    send_amount_as_text = lang_data.get("send_amount_as_text", "Send amount as text (e.g., 10).")
    amount_too_low_msg_template = lang_data.get("amount_too_low_msg", "Amount too low. Min: {amount} EUR.")
    amount_too_high_msg = lang_data.get("amount_too_high_msg", "Amount too high. Max: 10000 EUR.")
    invalid_amount_format_msg = lang_data.get("invalid_amount_format_msg", "Invalid amount format (e.g., 10.50).")
    unexpected_error_msg = lang_data.get("unexpected_error_msg", "Unexpected error. Try again.")
    choose_crypto_prompt_template = lang_data.get("choose_crypto_prompt", "Top up {amount} EUR. Choose crypto:")
    cancel_top_up_button = lang_data.get("cancel_top_up_button", "Cancel Top Up")

    if not update.message or not update.message.text:
        await send_message_with_retry(context.bot, chat_id, f"❌ {send_amount_as_text}", parse_mode=None)
        return

    amount_text = update.message.text.strip().replace(',', '.')

    try:
        refill_amount_decimal = Decimal(amount_text)
        if refill_amount_decimal < MIN_DEPOSIT_EUR:
            min_amount_str = format_currency(MIN_DEPOSIT_EUR)
            amount_too_low_msg = amount_too_low_msg_template.format(amount=min_amount_str)
            await send_message_with_retry(context.bot, chat_id, f"❌ {amount_too_low_msg}", parse_mode=None)
            return
        if refill_amount_decimal > Decimal('10000.00'):
            await send_message_with_retry(context.bot, chat_id, f"❌ {amount_too_high_msg}", parse_mode=None)
            return

        context.user_data['refill_eur_amount'] = float(refill_amount_decimal)
        # Skip selection state and go straight to processing
        # context.user_data['state'] = 'awaiting_refill_crypto_choice' 
        logger.info(f"User {user_id} entered refill EUR: {refill_amount_decimal:.2f}. Auto-selecting SOL.")

        # Call payment handler directly
        from payment import handle_select_refill_crypto
        # We need to simulate the callback query structure if possible, but the handler takes (update, context, params)
        # Since handle_select_refill_crypto usually expects a callback query in 'update', we might need to be careful.
        # However, `handle_select_refill_crypto` uses `query = update.callback_query`.
        # If we call it from a message handler, `update.callback_query` is None.
        
        # We need to patch the update or refactor handle_select_refill_crypto to handle message updates or create a mock query.
        # A simpler way is to adapt handle_select_refill_crypto or create a wrapper.
        
        # BETTER APPROACH: Since we are in a message handler, we can't easily use a callback handler that relies on `query.edit_message_text`.
        # We should replicate the logic of handle_select_refill_crypto but for message context.
        
        # ... OR ... we create a new state `awaiting_refill_confirmation`? No, user just wants to pay.
        
        # Let's call create_nowpayments_payment directly here, similar to handle_select_refill_crypto logic.
        
        preparing_invoice_msg = lang_data.get("preparing_invoice", "⏳ Preparing your payment invoice...")
        await send_message_with_retry(context.bot, chat_id, preparing_invoice_msg, parse_mode=None)
        
        from payment import create_sol_payment, display_sol_invoice
        
        # Call SOL payment creation
        payment_result = await create_sol_payment(
            user_id, refill_eur_amount_decimal,
            is_purchase=False # Explicitly False for refill
        )
        
        if 'error' in payment_result:
            # Error handling logic (simplified from payment.py)
            error_code = payment_result['error']
            failed_invoice_creation_msg = lang_data.get("failed_invoice_creation", "❌ Failed to create payment invoice. Please try again later or contact support.")
            await send_message_with_retry(context.bot, chat_id, failed_invoice_creation_msg, parse_mode=None)
            context.user_data.pop('refill_eur_amount', None)
        else:
            logger.info(f"SOL refill invoice created successfully for user {user_id}. Payment ID: {payment_result.get('payment_id')}")
            context.user_data.pop('refill_eur_amount', None)
            context.user_data.pop('state', None)
            
            # We need to mock a query object for display_nowpayments_invoice because it uses query.message.chat_id
            # But wait, display_nowpayments_invoice uses query.message.chat_id. 
            # If update.callback_query is None, it might fail.
            
            # Let's modify display_nowpayments_invoice to be robust or handle it here.
            # Actually display_nowpayments_invoice expects a callback update.
            # We can just send the invoice message manually here or refactor display_nowpayments_invoice.
            
            # Let's use a hack: set `update.callback_query` on the update object if we can, or just copy the display logic.
            # Copying display logic is safest to avoid breaking other things.
            
            # ...Actually, let's just try to pass a dummy query object or modify display_nowpayments_invoice to handle message updates.
            # But since I can't easily modify payment.py display logic without risk, I will implement the display here for this specific case.
            
            # -- Display Logic for Message Context --
            pay_address = payment_result.get('pay_address')
            pay_amount = payment_result.get('pay_amount')
            pay_currency = 'SOL'
            
            msg = f"💰 **Top Up: {format_currency(refill_eur_amount_decimal)} EUR**\n\n"
            msg += f"Send **{pay_amount} SOL** to:\n`{pay_address}`\n\n"
            msg += "⚠️ Send the **EXACT** amount.\n"
            msg += "⏳ Valid for 1 hour."
            
            # QR Code
            import qrcode
            from io import BytesIO
            qr = qrcode.QRCode(version=1, box_size=10, border=5)
            qr.add_data(pay_address)
            qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white")
            bio = BytesIO()
            img.save(bio, 'PNG')
            bio.seek(0)
            
            # Cancel button
            keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_payment")]]
            
            await context.bot.send_photo(chat_id=chat_id, photo=bio, caption=msg, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
            
    except ValueError:
        await send_message_with_retry(context.bot, chat_id, f"❌ {invalid_amount_format_msg}", parse_mode=None)
        return
    except Exception as e:
        logger.error(f"Error processing refill amount user {user_id}: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, f"❌ {unexpected_error_msg}", parse_mode=None)
        context.user_data.pop('state', None)
        context.user_data.pop('refill_eur_amount', None)

# --- Single Item Discount Code Message Handler ---
async def handle_single_item_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Handles the general discount code entered during the single item crypto pay flow """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_single_item_discount_code": return
    if not update.message or not update.message.text: return

    entered_code = update.message.text.strip()
    context.user_data.pop('state', None) # Clear state

    # Retrieve context for this flow
    single_item_snapshot = context.user_data.get('single_item_pay_snapshot')
    price_after_reseller_float = context.user_data.get('single_item_pay_final_eur')

    if single_item_snapshot is None or price_after_reseller_float is None:
        logger.error(f"User {user_id} sent single item discount code but snapshot/total context is missing.")
        await send_message_with_retry(context.bot, chat_id, "Error: Context lost. Returning to product selection.", parse_mode=None)
        context.user_data.pop('single_item_pay_snapshot', None)
        context.user_data.pop('single_item_pay_final_eur', None)
        context.user_data.pop('single_item_pay_discount_code', None)
        back_params = context.user_data.pop('single_item_pay_back_params', None)
        if back_params:
            return await handle_product_selection(update, context, params=back_params) # Go back to product selection
        return

    if not entered_code:
        await send_message_with_retry(context.bot, chat_id, lang_data.get("no_code_entered", "No code entered."), parse_mode=None)
        await handle_skip_discount_single_pay(update, context) # Proceed as if skipped
        return

    # SECURITY: Use atomic validation to prevent race conditions and multiple uses
    code_valid, validation_message, discount_details = validate_and_apply_discount_atomic(entered_code, price_after_reseller_float, user_id)
    feedback_msg_template = ""
    new_final_total_for_single_item_float = price_after_reseller_float

    if code_valid and discount_details:
        new_final_total_for_single_item_float = discount_details['final_total']
        context.user_data['single_item_pay_final_eur'] = new_final_total_for_single_item_float
        context.user_data['single_item_pay_discount_code'] = entered_code
        logger.info(f"User {user_id} applied valid single item discount '{entered_code}'. New FINAL price: {new_final_total_for_single_item_float:.2f} EUR")
        feedback_msg_template = lang_data.get("basket_pay_code_applied", "✅ Code '{code}' applied. New total: {total} EUR. Choose payment method:")
        feedback_msg = feedback_msg_template.format(code=entered_code, total=format_currency(new_final_total_for_single_item_float))
    else:
        context.user_data['single_item_pay_discount_code'] = None
        logger.warning(f"User {user_id} entered invalid single item discount '{entered_code}': {validation_message}")
        price_to_pay_str = format_currency(price_after_reseller_float)
        feedback_msg_template = lang_data.get("basket_pay_code_invalid", "❌ Code invalid: {reason}. Choose payment method to pay {total} EUR:")
        feedback_msg = feedback_msg_template.format(reason=validation_message, total=price_to_pay_str)

    try: await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception as e: logger.warning(f"Could not delete user's single item discount code message: {e}")

    await send_message_with_retry(context.bot, chat_id, feedback_msg, parse_mode=None)

    # Re-evaluate payment options
    final_total_decimal = Decimal(str(context.user_data['single_item_pay_final_eur']))
    snapshot = context.user_data['single_item_pay_snapshot']
    discount_code_to_use = context.user_data.get('single_item_pay_discount_code')

    conn_balance = None; user_balance = Decimal('0.0'); balance_check_error = False
    try:
        conn_balance = get_db_connection()
        c_balance = conn_balance.cursor()
        c_balance.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
        balance_result = c_balance.fetchone()
        user_balance = Decimal(str(balance_result['balance'])) if balance_result else Decimal('0.0')
    except sqlite3.Error as e:
        logger.error(f"DB error fetching balance after single item discount: {e}")
        await asyncio.to_thread(_unreserve_basket_items, snapshot)
        await send_message_with_retry(context.bot, chat_id, "❌ Error checking balance. Item released.", parse_mode=None)
        balance_check_error = True
    finally:
        if conn_balance: conn_balance.close()

    if balance_check_error:
        context.user_data.pop('single_item_pay_snapshot', None)
        context.user_data.pop('single_item_pay_final_eur', None)
        context.user_data.pop('single_item_pay_discount_code', None)
        context.user_data.pop('single_item_pay_back_params', None)
        return

    if user_balance >= final_total_decimal:
        logger.info(f"User {user_id} has sufficient balance ({user_balance:.2f} EUR) for single item purchase ({final_total_decimal:.2f} EUR). Processing balance payment.")
        success = await payment.process_purchase_with_balance(user_id, final_total_decimal, snapshot, discount_code_to_use, context)
        
        # Clear reservation tracking since payment completed
        from utils import clear_reservation_tracking
        clear_reservation_tracking(user_id)
        
        context.user_data.pop('single_item_pay_snapshot', None)
        context.user_data.pop('single_item_pay_final_eur', None)
        context.user_data.pop('single_item_pay_discount_code', None)
        context.user_data.pop('single_item_pay_back_params', None)
    else:
        context.user_data['basket_pay_snapshot'] = snapshot # Use the general basket_pay keys for _show_crypto_choices
        context.user_data['basket_pay_total_eur'] = float(final_total_decimal)
        context.user_data['basket_pay_discount_code'] = discount_code_to_use
        await _show_crypto_choices_for_basket(update, context, edit_message=False)

# --- NEW: Handler to Ask for Discount Code in Single Item Pay Flow ---
async def handle_apply_discount_single_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    # Check if the single item payment context exists
    if 'single_item_pay_snapshot' not in context.user_data or \
       'single_item_pay_final_eur' not in context.user_data or \
       'single_item_pay_back_params' not in context.user_data:
        logger.error(f"User {user_id} clicked apply_discount_single_pay but single_item context is missing.")
        await query.answer("Error: Payment context lost. Please try again.", show_alert=True)
        # Attempt to go back to the product selection
        back_params = context.user_data.get('single_item_pay_back_params')
        if back_params:
            # Ensure handle_product_selection is awaitable if called directly
            return await handle_product_selection(update, context, params=back_params)
        else: # Fallback if even back_params are lost
            return await handle_shop(update, context)


    context.user_data['state'] = 'awaiting_single_item_discount_code'
    prompt_msg = lang_data.get("basket_pay_enter_discount", "Please enter discount code for this purchase:") # Re-use existing lang string
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    
    # The cancel button should skip discount and go to crypto choices for single item
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="skip_discount_single_pay")]]

    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter discount code in chat.")

# --- NEW: Handler to Skip Discount in Single Item Pay Flow ---
async def handle_skip_discount_single_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    # Check if the single item payment context exists (copied from handle_apply_discount_single_pay for robustness)
    if 'single_item_pay_snapshot' not in context.user_data or 'single_item_pay_final_eur' not in context.user_data or 'single_item_pay_back_params' not in context.user_data:
        logger.warning(f"User {user_id} clicked skip_discount_single_pay but single_item context is missing. Redirecting to shop.")
        await query.answer("Session expired. Redirecting to shop...", show_alert=True)
        back_params = context.user_data.get('single_item_pay_back_params')
        if back_params:
            return await handle_product_selection(update, context, params=back_params)
        else:
            return await handle_shop(update, context)

    context.user_data['single_item_pay_discount_code'] = None # Ensure no discount code is carried forward
    
    # Check user balance before showing payment options
    snapshot = context.user_data.get('single_item_pay_snapshot', [])
    final_total_eur = Decimal(str(context.user_data.get('single_item_pay_final_eur', 0)))
    
    conn = None
    user_balance = Decimal('0.0')
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
        balance_result = c.fetchone()
        user_balance = Decimal(str(balance_result['balance'])) if balance_result else Decimal('0.0')
    except Exception as e:
        logger.error(f"Error checking balance for user {user_id}: {e}")
    finally:
        if conn:
            conn.close()
    
    # If user has sufficient balance, process with balance payment
    if user_balance >= final_total_eur:
        logger.info(f"User {user_id} has sufficient balance ({user_balance:.2f} EUR) for single item purchase ({final_total_eur:.2f} EUR). Processing balance payment.")
        discount_code = context.user_data.get('single_item_pay_discount_code')
        success = await payment.process_purchase_with_balance(user_id, final_total_eur, snapshot, discount_code, context)
        if success:
            # Clear single item payment context
            context.user_data.pop('single_item_pay_snapshot', None)
            context.user_data.pop('single_item_pay_final_eur', None)
            context.user_data.pop('single_item_pay_discount_code', None)
            context.user_data.pop('single_item_pay_back_params', None)
        return
    
    # Otherwise, show crypto payment options
    proceeding_msg = lang_data.get("proceeding_to_payment_answer", "Proceeding to payment options...")
    await query.answer(proceeding_msg)
    await _show_crypto_choices_for_basket(update, context, edit_message=True)

# --- NEW: Handler to Apply Referral Code in Single Item Pay Flow ---
async def handle_apply_referral_single_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle referral code application during single item purchase"""
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    
    # Check if single item payment context exists
    if 'single_item_pay_snapshot' not in context.user_data or 'single_item_pay_final_eur' not in context.user_data:
        logger.warning(f"User {user_id} clicked apply_referral_single_pay but context missing.")
        await query.answer("Session expired. Please try again.", show_alert=True)
        return await handle_shop(update, context)
    
    # Check if referral system is enabled
    from referral_system import get_referral_settings
    referral_settings = get_referral_settings()
    if not referral_settings.get('program_enabled', False):
        await query.answer("❌ Referral program is currently disabled.", show_alert=True)
        return
    
    # Check if user already used a referral code
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT referred_by FROM users WHERE user_id = %s", (user_id,))
        result = c.fetchone()
        if result and result['referred_by']:
            await query.answer("❌ You've already used a referral code!", show_alert=True)
            return
    except Exception as e:
        logger.error(f"Error checking referral status: {e}")
    finally:
        if conn:
            conn.close()
    
    prompt_msg = lang_data.get("enter_referral_code_prompt", "🎁 Enter your referral code in chat:")
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    
    # Set state to wait for referral code input
    context.user_data['state'] = 'awaiting_referral_code_single_pay'
    
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="cancel_referral_single_pay")]]
    
    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter referral code in chat.")

async def handle_referral_code_message_single_pay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text message with referral code during single item purchase"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    lang, lang_data = _get_lang_data(context)
    
    if context.user_data.get('state') != 'awaiting_referral_code_single_pay':
        return  # Not in referral code entry state
    
    entered_code = update.message.text.strip()
    
    # Process referral code
    from referral_system import apply_referral_code
    success, message = apply_referral_code(user_id, entered_code)
    
    # Clear state
    context.user_data.pop('state', None)
    
    if success:
        # Show success message with updated payment menu
        await send_message_with_retry(context.bot, chat_id, f"✅ {message}", parse_mode=None)
        
        # Return to payment menu
        if 'single_item_pay_back_params' in context.user_data:
            back_params = context.user_data['single_item_pay_back_params']
            # Show updated payment menu with referral bonus applied
            snapshot = context.user_data.get('single_item_pay_snapshot', [])
            if snapshot:
                item = snapshot[0]
                item_name_display = f"{PRODUCT_TYPES.get(item['product_type'], '')} {item['name']} {item['size']}"
                price_after_reseller = Decimal(str(context.user_data.get('single_item_pay_final_eur', 0)))
                price_display_str = format_currency(price_after_reseller)
                
                prompt_msg = (f"You are about to pay for: {item_name_display} ({price_display_str} EUR).\n\n"
                             f"✅ Referral bonus applied!\n\n"
                             f"{lang_data.get('prompt_discount_or_pay', 'Do you have a discount code to apply?')}")
                
                pay_now_direct_button_text = lang_data.get("pay_now_button", "Pay Now")
                apply_discount_button_text = lang_data.get("apply_discount_pay_button", "🏷️ Apply Discount Code")
                back_to_product_button_text = lang_data.get("back_options_button", "Back to Product")
                
                keyboard = [
                    [InlineKeyboardButton(pay_now_direct_button_text, callback_data="skip_discount_single_pay")],
                    [InlineKeyboardButton(apply_discount_button_text, callback_data="apply_discount_single_pay")],
                    [InlineKeyboardButton(f"⬅️ {back_to_product_button_text}", 
                                        callback_data=f"product|{back_params[0]}|{back_params[1]}|{back_params[2]}|{back_params[3]}|{back_params[4]}")]
                ]
                
                await send_message_with_retry(context.bot, chat_id, prompt_msg, 
                                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        # Show error and return to payment menu
        await send_message_with_retry(context.bot, chat_id, f"❌ {message}", parse_mode=None)
        
        # Return to payment menu
        if 'single_item_pay_back_params' in context.user_data:
            back_params = context.user_data['single_item_pay_back_params']
            snapshot = context.user_data.get('single_item_pay_snapshot', [])
            if snapshot:
                item = snapshot[0]
                item_name_display = f"{PRODUCT_TYPES.get(item['product_type'], '')} {item['name']} {item['size']}"
                price_after_reseller = Decimal(str(context.user_data.get('single_item_pay_final_eur', 0)))
                price_display_str = format_currency(price_after_reseller)
                
                prompt_msg = (f"You are about to pay for: {item_name_display} ({price_display_str} EUR).\n\n"
                             f"{lang_data.get('prompt_discount_or_pay', 'Do you have a discount code to apply?')}")
                
                pay_now_direct_button_text = lang_data.get("pay_now_button", "Pay Now")
                apply_discount_button_text = lang_data.get("apply_discount_pay_button", "🏷️ Apply Discount Code")
                apply_referral_button_text = lang_data.get("apply_referral_button", "🎁 Apply Referral Code")
                back_to_product_button_text = lang_data.get("back_options_button", "Back to Product")
                
                from referral_system import get_referral_settings
                referral_settings = get_referral_settings()
                referral_enabled = referral_settings.get('program_enabled', False)
                
                keyboard = [
                    [InlineKeyboardButton(pay_now_direct_button_text, callback_data="skip_discount_single_pay")],
                    [InlineKeyboardButton(apply_discount_button_text, callback_data="apply_discount_single_pay")]
                ]
                
                if referral_enabled:
                    keyboard.append([InlineKeyboardButton(apply_referral_button_text, callback_data="apply_referral_single_pay")])
                
                keyboard.append([InlineKeyboardButton(f"⬅️ {back_to_product_button_text}", 
                                        callback_data=f"product|{back_params[0]}|{back_params[1]}|{back_params[2]}|{back_params[3]}|{back_params[4]}")])
                
                await send_message_with_retry(context.bot, chat_id, prompt_msg, 
                                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_cancel_referral_single_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Cancel referral code entry and return to payment menu"""
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    
    # Clear state
    context.user_data.pop('state', None)
    
    # Return to payment menu
    if 'single_item_pay_back_params' not in context.user_data or 'single_item_pay_snapshot' not in context.user_data:
        await query.answer("Session expired.", show_alert=True)
        return await handle_shop(update, context)
    
    back_params = context.user_data['single_item_pay_back_params']
    snapshot = context.user_data.get('single_item_pay_snapshot', [])
    
    if snapshot:
        item = snapshot[0]
        item_name_display = f"{PRODUCT_TYPES.get(item['product_type'], '')} {item['name']} {item['size']}"
        price_after_reseller = Decimal(str(context.user_data.get('single_item_pay_final_eur', 0)))
        price_display_str = format_currency(price_after_reseller)
        
        prompt_msg = (f"You are about to pay for: {item_name_display} ({price_display_str} EUR).\n\n"
                     f"{lang_data.get('prompt_discount_or_pay', 'Do you have a discount code to apply?')}")
        
        pay_now_direct_button_text = lang_data.get("pay_now_button", "Pay Now")
        apply_discount_button_text = lang_data.get("apply_discount_pay_button", "🏷️ Apply Discount Code")
        apply_referral_button_text = lang_data.get("apply_referral_button", "🎁 Apply Referral Code")
        back_to_product_button_text = lang_data.get("back_options_button", "Back to Product")
        
        from referral_system import get_referral_settings
        referral_settings = get_referral_settings()
        referral_enabled = referral_settings.get('program_enabled', False)
        
        keyboard = [
            [InlineKeyboardButton(pay_now_direct_button_text, callback_data="skip_discount_single_pay")],
            [InlineKeyboardButton(apply_discount_button_text, callback_data="apply_discount_single_pay")]
        ]
        
        if referral_enabled:
            keyboard.append([InlineKeyboardButton(apply_referral_button_text, callback_data="apply_referral_single_pay")])
        
        keyboard.append([InlineKeyboardButton(f"⬅️ {back_to_product_button_text}", 
                                callback_data=f"product|{back_params[0]}|{back_params[1]}|{back_params[2]}|{back_params[3]}|{back_params[4]}")])
        
        await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer("Cancelled.")

# --- NEW: Direct Product View Discount/Referral Handlers ---

async def handle_apply_discount_product(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handler for 'Apply Discount' button on product view"""
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    
    if not params or len(params) < 5:
        await query.answer("Error: Missing params.", show_alert=True)
        return
        
    # Store params to return to product view
    context.user_data['product_view_params'] = params
    context.user_data['state'] = 'awaiting_product_discount_code'
    
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    # Callback to return to product view
    back_callback = f"product|{params[0]}|{params[1]}|{params[2]}|{params[3]}|{params[4]}"
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data=back_callback)]]
    
    msg = lang_data.get("prompt_enter_discount_code", "Please enter your discount code:")
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer()

async def handle_apply_referral_product(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handler for 'Apply Referral' button on product view"""
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    
    if not params or len(params) < 5:
        await query.answer("Error: Missing params.", show_alert=True)
        return
        
    context.user_data['product_view_params'] = params
    context.user_data['state'] = 'awaiting_product_referral_code'
    
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    back_callback = f"product|{params[0]}|{params[1]}|{params[2]}|{params[3]}|{params[4]}"
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data=back_callback)]]
    
    msg = lang_data.get("prompt_enter_referral_code", "Please enter the referral code:")
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer()

async def handle_product_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text message for product view discount code"""
    if context.user_data.get('state') != 'awaiting_product_discount_code': return
    
    code = update.message.text.strip()
    context.user_data.pop('state', None)
    
    # Verify code existence
    from utils import get_db_connection
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT code, discount_percent FROM discount_codes WHERE code = %s AND (uses_left > 0 OR uses_left IS NULL) AND (expires_at IS NULL OR expires_at > NOW())", (code,))
    discount = c.fetchone()
    conn.close()
    
    msg = ""
    if discount:
        context.user_data['pre_applied_discount_code'] = code
        msg = f"✅ Discount code '{code}' applied! ({discount['discount_percent']}%)"
    else:
        msg = f"❌ Invalid or expired discount code."
    
    params = context.user_data.get('product_view_params')
    if params:
        back_callback = f"product|{params[0]}|{params[1]}|{params[2]}|{params[3]}|{params[4]}"
        back_btn = InlineKeyboardButton("⬅️ Back to Product", callback_data=back_callback)
        await send_message_with_retry(context.bot, update.effective_chat.id, msg, reply_markup=InlineKeyboardMarkup([[back_btn]]), parse_mode=None)
    else:
        await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode=None)

async def handle_product_referral_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text message for product view referral code"""
    if context.user_data.get('state') != 'awaiting_product_referral_code': return
    
    code = update.message.text.strip()
    context.user_data.pop('state', None)
    user_id = update.effective_user.id
    
    from referral_system import apply_referral_code
    success, message = apply_referral_code(user_id, code)
    
    msg = f"✅ {message}" if success else f"❌ {message}"
    
    params = context.user_data.get('product_view_params')
    if params:
        back_callback = f"product|{params[0]}|{params[1]}|{params[2]}|{params[3]}|{params[4]}"
        back_btn = InlineKeyboardButton("⬅️ Back to Product", callback_data=back_callback)
        await send_message_with_retry(context.bot, update.effective_chat.id, msg, reply_markup=InlineKeyboardMarkup([[back_btn]]), parse_mode=None)
    else:
        await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode=None)
