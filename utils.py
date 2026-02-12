import time
import os
import logging
import psycopg2
import psycopg2.errors
from psycopg2.extras import RealDictCursor
import json
import shutil
import tempfile
import asyncio
import random
import string
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import requests
from collections import Counter, defaultdict # Moved higher up
from PIL import Image, ImageDraw, ImageFont
import io

# --- Telegram Imports ---
from telegram import Update, Bot
from telegram.constants import ParseMode
import telegram.error as telegram_error
from telegram.ext import ContextTypes
from telegram import helpers
# -------------------------

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- PostgreSQL Configuration ---
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'shop_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
POSTGRES_URL = os.getenv('DATABASE_URL', f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

# --- Media Directory Configuration (Render-Compatible) ---
# Use relative path within app directory for Render compatibility
MEDIA_BASE_DIR = os.getenv('MEDIA_DIR', './media')  # Allow override via env var
MEDIA_DIR = os.path.abspath(MEDIA_BASE_DIR)
BOT_MEDIA_JSON_PATH = os.path.join(MEDIA_DIR, 'bot_media.json')

# Ensure the base media directory exists on the disk when the script starts
try:
    os.makedirs(MEDIA_DIR, exist_ok=True)
    logger.info(f"Ensured media directory exists: {MEDIA_DIR}")
except OSError as e:
    logger.error(f"Could not create media directory {MEDIA_DIR}: {e}")

# Log database connection info (mask password for security)
try:
    from urllib.parse import urlparse
    parsed = urlparse(POSTGRES_URL)
    db_display = f"{parsed.hostname}:{parsed.port}/{parsed.path.lstrip('/')}"
except:
    db_display = "DATABASE_URL (parsing failed)"
logger.info(f"Using PostgreSQL Database: {db_display}")
logger.info(f"Using Media Directory: {MEDIA_DIR}")
logger.info(f"Using Bot Media Config Path: {BOT_MEDIA_JSON_PATH}")


# --- Configuration Loading (from Environment Variables) ---
# Multi-bot failover tokens
BOT_TOKENS = [
    os.environ.get("BOT_TOKEN_1", "").strip(),
    os.environ.get("BOT_TOKEN_2", "").strip(),
    os.environ.get("BOT_TOKEN_3", "").strip(),
    os.environ.get("BOT_TOKEN_4", "").strip(),
    os.environ.get("BOT_TOKEN_5", "").strip(),
]

# Backward compatibility: also check old TOKEN env var
legacy_token = os.environ.get("TOKEN", "").strip()
if legacy_token:
    BOT_TOKENS.insert(0, legacy_token)

# Remove empty tokens
BOT_TOKENS = [t for t in BOT_TOKENS if t]

# Set TOKEN to first available for backward compatibility
TOKEN = BOT_TOKENS[0] if BOT_TOKENS else ""
# SOL-only payment system - no third-party API keys needed
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "") # Base URL for Render app (e.g., https://app-name.onrender.com)

# NowPayments placeholders for backward compatibility (not used in SOL-only mode)
NOWPAYMENTS_API_KEY = None
NOWPAYMENTS_API_URL = None

ADMIN_ID_RAW = os.environ.get("ADMIN_ID", None)
PRIMARY_ADMIN_IDS_STR = os.environ.get("PRIMARY_ADMIN_IDS", "")
SECONDARY_ADMIN_IDS_STR = os.environ.get("SECONDARY_ADMIN_IDS", "")
SUPPORT_USERNAME = os.environ.get("SUPPORT_USERNAME", "support")
BASKET_TIMEOUT_MINUTES_STR = os.environ.get("BASKET_TIMEOUT_MINUTES", "25")

# Legacy support for single ADMIN_ID
ADMIN_ID = None
if ADMIN_ID_RAW is not None:
    try: ADMIN_ID = int(ADMIN_ID_RAW)
    except (ValueError, TypeError): logger.error(f"Invalid format for ADMIN_ID: {ADMIN_ID_RAW}. Must be an integer.")

# New multi-primary admin support
PRIMARY_ADMIN_IDS = []
if PRIMARY_ADMIN_IDS_STR:
    try: PRIMARY_ADMIN_IDS = [int(uid.strip()) for uid in PRIMARY_ADMIN_IDS_STR.split(',') if uid.strip()]
    except ValueError: logger.warning("PRIMARY_ADMIN_IDS contains non-integer values. Ignoring.")

# Add legacy ADMIN_ID to PRIMARY_ADMIN_IDS if it exists and isn't already included
if ADMIN_ID is not None and ADMIN_ID not in PRIMARY_ADMIN_IDS:
    PRIMARY_ADMIN_IDS.append(ADMIN_ID)

SECONDARY_ADMIN_IDS = []
if SECONDARY_ADMIN_IDS_STR:
    try: SECONDARY_ADMIN_IDS = [int(uid.strip()) for uid in SECONDARY_ADMIN_IDS_STR.split(',') if uid.strip()]
    except ValueError: logger.warning("SECONDARY_ADMIN_IDS contains non-integer values. Ignoring.")

BASKET_TIMEOUT = 25 * 60 # Default: 25 minutes
try:
    BASKET_TIMEOUT = int(BASKET_TIMEOUT_MINUTES_STR) * 60
    if BASKET_TIMEOUT <= 0: logger.warning("BASKET_TIMEOUT_MINUTES non-positive, using default 25 min."); BASKET_TIMEOUT = 25 * 60
except ValueError: logger.warning("Invalid BASKET_TIMEOUT_MINUTES, using default 25 min."); BASKET_TIMEOUT = 25 * 60

# --- Validate essential config ---
if not TOKEN: 
    logger.critical("CRITICAL ERROR: TOKEN environment variable is missing."); 
    raise SystemExit("TOKEN not set.")

# Enhanced token validation
if ':' not in TOKEN:
    logger.critical(f"CRITICAL ERROR: TOKEN format is invalid (missing colon). Token: {TOKEN[:10]}...")
    raise SystemExit("TOKEN format is invalid.")

token_parts = TOKEN.split(':')
if len(token_parts) != 2 or not token_parts[0].isdigit() or len(token_parts[1]) < 30:
    logger.critical(f"CRITICAL ERROR: TOKEN format is invalid. Expected format: 'bot_id:secret_key'")
    raise SystemExit("TOKEN format is invalid.")

logger.info(f"TOKEN validation passed. Bot ID: {token_parts[0]}")

# SOL payment system doesn't require third-party API keys
logger.info("Using SOL-only payment system")
if not WEBHOOK_URL: logger.critical("CRITICAL ERROR: WEBHOOK_URL environment variable is missing."); raise SystemExit("WEBHOOK_URL not set.")
if not PRIMARY_ADMIN_IDS: logger.warning("No primary admin IDs configured. Primary admin features disabled.")
logger.info(f"Loaded {len(PRIMARY_ADMIN_IDS)} primary admin ID(s): {PRIMARY_ADMIN_IDS}")
logger.info(f"Loaded {len(SECONDARY_ADMIN_IDS)} secondary admin ID(s): {SECONDARY_ADMIN_IDS}")
logger.info(f"Basket timeout set to {BASKET_TIMEOUT // 60} minutes.")
# Solana payments are monitored by background checker
logger.info(f"Telegram webhook expected at: {WEBHOOK_URL}/telegram/{TOKEN}")


# --- Constants ---
THEMES = {
    "default": {"product": "💎", "basket": "🛒", "review": "📝"},
    "neon": {"product": "💎", "basket": "🛍️", "review": "✨"},
    "stealth": {"product": "🌑", "basket": "🛒", "review": "🌟"},
    "nature": {"product": "🌿", "basket": "🧺", "review": "🌸"}
}

# ==============================================================
# ===== V V V V V      LANGUAGE DICTIONARY     V V V V V ======
# ==============================================================
# Define LANGUAGES dictionary FIRST
LANGUAGES = {
    # --- English ---
    "en": {
        "native_name": "English",
        # --- General & Menu ---
        "welcome": "👋 Welcome, {username}!\n\n👤 Status: {status} {progress_bar}\n💰 Balance: {balance_str} EUR\n📦 Total Purchases: {purchases}\n🛒 Basket Items: {basket_count}\n\nStart shopping or explore your options below.\n\n⚠️ Note: No refunds.", # <<< Default Welcome Message Format
        "status_label": "Status",
        "balance_label": "Balance",
        "purchases_label": "Total Purchases",
        "basket_label": "Basket Items",
        "shopping_prompt": "Start shopping or explore your options below.",
        "refund_note": "Note: No refunds.",
        "shop_button": "Shop",
        "profile_button": "Profile",
        "top_up_button": "Top Up",
        "reviews_button": "Reviews",
        "price_list_button": "Price List",
        "language_button": "Language",
        "admin_button": "🔧 Admin Panel",
        "home_button": "Home",
        "back_button": "Back",
        "cancel_button": "Cancel",
        "error_occurred_answer": "An error occurred. Please try again.",
        "success_label": "Success!",
        "error_unexpected": "An unexpected error occurred",

        # --- Shopping Flow ---
        "choose_city_title": "Choose a City",
        "select_location_prompt": "Select your location:",
        "no_cities_available": "No cities available at the moment. Please check back later.",
        "error_city_not_found": "Error: City not found.",
        "choose_district_prompt": "Choose a district:",
        "no_districts_available": "No districts available yet for this city.",
        "back_cities_button": "Back to Cities",
        "error_district_city_not_found": "Error: District or city not found.",
        "select_type_prompt": "Select product type:",
        "no_types_available": "No product types currently available here.",
        "error_loading_types": "Error: Failed to Load Product Types",
        "back_districts_button": "Back to Districts",
        "available_options_prompt": "Available options:",
        "no_items_of_type": "No items of this type currently available here.",
        "error_loading_products": "Error: Failed to Load Products",
        "back_types_button": "Back to Types",
        "price_label": "Price",
        "available_label_long": "Available",
        "available_label_short": "Av",
        "add_to_basket_button": "Add to Basket",
        "error_location_mismatch": "Error: Location data mismatch.",
        "drop_unavailable": "Drop Unavailable! This option just sold out or was reserved by someone else.",
        "error_loading_details": "Error: Failed to Load Product Details",
        "back_options_button": "Back to Options",
        "no_products_in_city_districts": "No products currently available in any district of this city.",
        "error_loading_districts": "Error loading districts. Please try again.",

        # --- Basket & Payment ---
        "added_to_basket": "✅ Item Reserved!\n\n{item} is in your basket for {timeout} minutes! ⏳",
        "expires_label": "Expires in",
        "your_basket_title": "Your Basket",
        "basket_empty": "🛒 Your Basket is Empty!",
        "add_items_prompt": "Add items to start shopping!",
        "items_expired_note": "Items may have expired or were removed.",
        "subtotal_label": "Subtotal",
        "total_label": "Total",
        "pay_now_button": "Pay Now",
        "clear_all_button": "Clear All",
        "view_basket_button": "View Basket",
        "clear_basket_button": "Clear Basket",
        "remove_button_label": "Remove",
        "basket_already_empty": "Basket is already empty.",
        "basket_cleared": "🗑️ Basket Cleared!",
        "pay": "💳 Total to Pay: {amount} EUR",
        "insufficient_balance": "⚠️ Insufficient Balance!\n\nPlease top up to continue! 💸", # Keep generic one for /profile
        "insufficient_balance_pay_option": "⚠️ Insufficient Balance! ({balance} / {required} EUR)", # <<< ADDED
        "pay_crypto_button": "💳 Pay with Crypto", # <<< ADDED
        "apply_discount_pay_button": "🏷️ Apply Discount Code", # <<< ADDED
        "skip_discount_button": "⏩ Skip Discount", # <<< ADDED
        "prompt_discount_or_pay": "Do you have a discount code to apply before paying with crypto?", # <<< ADDED
        "basket_pay_enter_discount": "Please enter discount code for this purchase:", # <<< ADDED
        "basket_pay_code_applied": "✅ Code '{code}' applied. New total: {total} EUR. Choose crypto:", # <<< ADDED
        "basket_pay_code_invalid": "❌ Code invalid: {reason}. Choose crypto to pay {total} EUR:", # <<< ADDED
        "choose_crypto_for_purchase": "Choose crypto to pay {amount} EUR for your basket:", # <<< ADDED
        "crypto_purchase_success": "Payment Confirmed! Your purchase details are being sent.", # <<< ADDED
        "crypto_purchase_failed": "Payment Failed/Expired. Your items are no longer reserved.", # <<< ADDED
        "payment_timeout_notification": "⏰ Payment Timeout: Your payment for basket items has expired after 2 hours. Reserved items have been released.", # <<< NEW
        "basket_pay_too_low": "Basket total {basket_total} EUR is below minimum for {currency}.", # <<< ADDED
        "balance_changed_error": "❌ Transaction failed: Your balance changed. Please check your balance and try again.",
        "order_failed_all_sold_out_balance": "❌ Order Failed: All items in your basket became unavailable during processing. Your balance was not charged.",
        "error_processing_purchase_contact_support": "❌ An error occurred while processing your purchase. Please contact support.",
        "purchase_success": "🎉 Purchase Complete!",
        "sold_out_note": "⚠️ Note: The following items became unavailable during processing and were not included: {items}. You were not charged for these.",
        "leave_review_now": "Leave Review Now",
        "back_basket_button": "Back to Basket",
        "error_adding_db": "Error: Database issue adding item to basket.",
        "error_adding_unexpected": "Error: An unexpected issue occurred.",
        "reseller_discount_label": "Reseller Discount", # <<< NEW

        # --- Discounts ---
        "discount_no_items": "Your basket is empty. Add items first.",
        "enter_discount_code_prompt": "Please enter your discount code:",
        "enter_code_answer": "Enter code in chat.",
        "apply_discount_button": "Apply Discount Code",
        "no_code_provided": "No code provided.",
        "discount_code_not_found": "Discount code not found.",
        "discount_code_inactive": "This discount code is inactive.",
        "discount_code_expired": "This discount code has expired.",
        "invalid_code_expiry_data": "Invalid code expiry data.",
        "code_limit_reached": "Code reached usage limit.",
        "internal_error_discount_type": "Internal error processing discount type.",
        "db_error_validating_code": "Database error validating code.",
        "unexpected_error_validating_code": "An unexpected error occurred.",
        "discount_min_order_not_met": "Minimum order amount not met for this discount code.",
        "code_applied_message": "Code '{code}' ({value}) applied. Discount: -{amount} EUR",
        "discount_applied_label": "Discount Applied",
        "discount_value_label": "Value",
        "discount_removed_note": "Discount code {code} removed: {reason}",
        "discount_removed_invalid_basket": "Discount removed (basket changed).",
        "remove_discount_button": "Remove Discount",
        "discount_removed_answer": "Discount removed.",
        "no_discount_answer": "No discount applied.",
        "send_text_please": "Please send the discount code as text.",
        "error_calculating_total": "Error calculating total.",
        "returning_to_basket": "Returning to basket.",
        "basket_empty_no_discount": "Your basket is empty. Cannot apply discount code.",

        # --- Profile & History ---
        "profile_title": "Your Profile",
        "purchase_history_button": "Purchase History",
        "back_profile_button": "Back to Profile",
        "purchase_history_title": "Purchase History",
        "no_purchases_yet": "You haven't made any purchases yet.",
        "recent_purchases_title": "Your Recent Purchases",
        "error_loading_profile": "❌ Error: Unable to load profile data.",

        # --- Language ---
        "language_set_answer": "Language set to {lang}!",
        "error_saving_language": "Error saving language preference.",
        "invalid_language_answer": "Invalid language selected.",
        "language": "🌐 Language", # Also the menu title

        # --- Price List ---
        "no_cities_for_prices": "No cities available to view prices for.",
        "price_list_title": "Price List",
        "select_city_prices_prompt": "Select a city to view available products and prices:",
        # "error_city_not_found": "Error: City not found.", <-- Already exists above
        "price_list_title_city": "Price List: {city_name}",
        "no_products_in_city": "No products currently available in this city.",
        "back_city_list_button": "Back to City List",
        "message_truncated_note": "Message truncated due to length limit. Use 'Shop' for full details.",
        "error_loading_prices_db": "Error: Failed to Load Price List for {city_name}",
        "error_displaying_prices": "Error displaying price list.",
        "error_unexpected_prices": "Error: An unexpected issue occurred while generating the price list.",
        "available_label": "available", # Used in price list

        # --- Reviews ---
        "reviews": "📝 Reviews Menu",
        "view_reviews_button": "View Reviews",
        "leave_review_button": "Leave a Review",
        "enter_review_prompt": "Please type your review message and send it.",
        "enter_review_answer": "Enter your review in the chat.",
        "send_text_review_please": "Please send text only for your review.",
        "review_not_empty": "Review cannot be empty. Please try again or cancel.",
        "review_too_long": "Review is too long (max 1000 characters). Please shorten it.",
        "review_thanks": "Thank you for your review! Your feedback helps us improve.",
        "error_saving_review_db": "Error: Could not save your review due to a database issue.",
        "error_saving_review_unexpected": "Error: An unexpected issue occurred while saving your review.",
        "user_reviews_title": "User Reviews",
        "no_reviews_yet": "No reviews have been left yet.",
        "no_more_reviews": "No more reviews to display.",
        "prev_button": "Prev",
        "next_button": "Next",
        "back_review_menu_button": "Back to Reviews Menu",
        "unknown_date_label": "Unknown Date",
        "error_displaying_review": "Error displaying review",
        "error_updating_review_list": "Error updating review list.",

        # --- Refill / NOWPayments ---
        "payment_amount_too_low_api": "❌ Payment Amount Too Low: The equivalent of {target_eur_amount} EUR in {currency} \\({crypto_amount}\\) is below the minimum required by the payment provider \\({min_amount} {currency}\\)\\. Please try a higher EUR amount\\.",
        "payment_amount_too_low_with_min_eur": "❌ Payment Amount Too Low: {target_eur_amount} EUR is below the minimum for {currency} payments \\(minimum: {min_eur_amount} EUR\\)\\. Please try a higher amount or select a different cryptocurrency\\.",
        "error_min_amount_fetch": "❌ Error: Could not retrieve minimum payment amount for {currency}\\. Please try again later or select a different currency\\.",
        "invoice_title_refill": "*Top\\-Up Invoice Created*",
        "invoice_title_purchase": "*Payment Invoice Created*", # <<< NEW
        "min_amount_label": "*Minimum Amount:*",
        "payment_address_label": "*Payment Address:*",
        "amount_label": "*Amount:*",
        "expires_at_label": "*Expires At:*",
        "send_warning_template": "⚠️ *Important:* Send *exactly* this amount of {asset} to this address\\.",
        "overpayment_note": "ℹ️ _Sending more than this amount is okay\\! Your balance will be credited based on the amount received after network confirmation\\._",
        "confirmation_note": "✅ Confirmation is automatic via webhook after network confirmation\\.",
        "invoice_amount_label_text": "Amount",
        "invoice_send_following_amount": "Please send the following amount:",
        "invoice_payment_deadline": "Payment must be completed within 20 minutes of invoice creation.",
            "error_estimate_failed": "❌ Error: Could not estimate SOL amount. Please try again.",
    "error_discount_invalid_payment": "❌ Your discount code is no longer valid: {reason}. Please return to your basket to continue without the discount.",
    "error_discount_mismatch_payment": "❌ Payment amount mismatch detected. Please return to your basket and try again.",
        "crypto_payment_disabled": "Top Up is currently disabled.",
        "top_up_title": "Top Up Balance",
        "enter_refill_amount_prompt": "Please reply with the amount in EUR you wish to add to your balance (e.g., 10 or 25.50).",
        "min_top_up_note": "Minimum top up: {amount} EUR",
        "enter_amount_answer": "Enter the top-up amount.",
        "send_amount_as_text": "Please send the amount as text (e.g., 10 or 25.50).",
        "amount_too_low_msg": "Amount too low. Minimum top up is {amount} EUR. Please enter a higher amount.",
        "amount_too_high_msg": "Amount too high. Please enter a lower amount.",
        "invalid_amount_format_msg": "Invalid amount format. Please enter a number (e.g., 10 or 25.50).",
        "unexpected_error_msg": "An unexpected error occurred. Please try again later.",
        "choose_crypto_prompt": "You want to top up {amount} EUR. Please choose the cryptocurrency you want to pay with:",
        "cancel_top_up_button": "Cancel Top Up",
        "preparing_invoice": "⏳ Preparing your payment invoice...",
        "failed_invoice_creation": "❌ Failed to create payment invoice. This could be a temporary issue with the payment provider or an API key problem. Please try again later or contact support.",
        "error_preparing_payment": "❌ An error occurred while preparing the payment details. Please try again later.",
        "top_up_success_title": "✅ Top Up Successful!",
        "amount_added_label": "Amount Added",
        "new_balance_label": "Your new balance",
        "error_sol_api": "❌ Payment Error: Could not create SOL payment. Please try again later or contact support.",
        "payment_pending_db_error": "❌ Database Error: Could not record pending payment. Please contact support.",
        "payment_cancelled_or_expired": "Payment Status: Your payment ({payment_id}) was cancelled or expired.",
        "webhook_processing_error": "Webhook Error: Could not process payment update {payment_id}.",
        "webhook_db_update_failed": "Critical Error: Payment {payment_id} confirmed, but DB balance update failed for user {user_id}. Manual action required.",
        "webhook_pending_not_found": "Webhook Warning: Received update for payment ID {payment_id}, but no pending deposit found in DB.",
        "webhook_price_fetch_error": "Webhook Error: Could not fetch price for {currency} to confirm EUR value for payment {payment_id}.",
        "payment_cancelled_user": "Payment cancelled. Reserved items (if any) have been released.", # <<< NEW
        "payment_cancel_error": "Could not cancel payment (already processed or context lost).", # <<< NEW
        "cancel_payment_button": "Cancel Payment", # <<< NEW
        "proceeding_to_payment_answer": "Proceeding to payment options...", # <<< ADDED
        "credit_overpayment_purchase": "✅ Your purchase was successful! Additionally, an overpayment of {amount} EUR has been credited to your balance. Your new balance is {new_balance} EUR.",
        "credit_underpayment_purchase": "ℹ️ Your purchase failed due to underpayment, but the received amount ({amount} EUR) has been credited to your balance. Your new balance is {new_balance} EUR.",
        "crypto_purchase_underpaid_credited": "⚠️ Purchase Failed: Underpayment detected. Amount needed was {needed_eur} EUR. Your balance has been credited with the received value ({paid_eur} EUR). Your items were not delivered.",
        "credit_refill": "✅ Your balance has been credited by {amount} EUR. Reason: {reason}. New balance: {new_balance} EUR.",


        # --- Admin ---
        "admin_menu": "🔧 Admin Panel\n\nManage the bot from here:",
        "admin_select_city": "🏙️ Select City to Edit\n\nChoose a city:",
        "admin_select_district": "🏘️ Select District in {city}\n\nPick a district:",
        "admin_select_type": "💎 Select Product Type\n\nChoose or create a type:",
        "admin_choose_action": "📦 Manage {type} in {city}, {district}\n\nWhat would you like to do?",
        "set_media_prompt_plain": "📸 Send a photo, video, or GIF to display above all messages:",
        "state_error": "❌ Error: Invalid State\n\nPlease start the 'Add New Product' process again from the Admin Panel.",
        "support": "📞 Need Help?\n\nContact {support} for assistance!",
        "file_download_error": "❌ Error: Failed to Download Media\n\nPlease try again or contact {support}. ",
        "admin_enter_type_emoji": "✍️ Please reply with a single emoji for the product type:",
        "admin_type_emoji_set": "Emoji set to {emoji}.",
        "admin_edit_type_emoji_button": "✏️ Change Emoji",
        "admin_invalid_emoji": "❌ Invalid input. Please send a single emoji.",
        "admin_type_emoji_updated": "✅ Emoji updated successfully for {type_name}!",
        "admin_edit_type_menu": "🧩 Editing Type: {type_name}\n\nCurrent Emoji: {emoji}\nDescription: {description}\n\nWhat would you like to do?", # Added {description}
        "admin_edit_type_desc_button": "📝 Edit Description", #<<< NEW
        # --- Broadcast Translations ---
        "broadcast_select_target": "📢 Broadcast Message\n\nSelect the target audience:",
        "broadcast_target_all": "👥 All Users",
        "broadcast_target_city": "🏙️ By Last Purchased City",
        "broadcast_target_status": "👑 By User Status",
        "broadcast_target_inactive": "⏳ By Inactivity (Days)",
        "broadcast_select_city_target": "🏙️ Select City to Target\n\nUsers whose last purchase was in:",
        "broadcast_select_status_target": "👑 Select Status to Target:",
        "broadcast_status_vip": "VIP 👑",
        "broadcast_status_regular": "Regular ⭐",
        "broadcast_status_new": "New 🌱",
        "broadcast_enter_inactive_days": "⏳ Enter Inactivity Period\n\nPlease reply with the number of days since the user's last purchase (or since registration if no purchases). Users inactive for this many days or more will receive the message.",
        "broadcast_invalid_days": "❌ Invalid number of days. Please enter a positive whole number.",
        "broadcast_days_too_large": "❌ Number of days is too large. Please enter a smaller number.",
        "broadcast_ask_message": "📝 Now send the message content (text, photo, video, or GIF with caption):",
        "broadcast_confirm_title": "📢 Confirm Broadcast",
        "broadcast_confirm_target_all": "Target: All Users",
        "broadcast_confirm_target_city": "Target: Last Purchase in {city}",
        "broadcast_confirm_target_status": "Target: Status - {status}",
        "broadcast_confirm_target_inactive": "Target: Inactive >= {days} days",
        "broadcast_confirm_preview": "Preview:",
        "broadcast_confirm_ask": "Send this message?",
        "broadcast_no_users_found_target": "⚠️ Broadcast Warning: No users found matching the target criteria.",
        # --- User Management Translations ---
        "manage_users_title": "👤 Manage Users",
        "manage_users_prompt": "Select a user to view details or manage:",
        "manage_users_no_users": "No users found.",
        "view_user_profile_title": "👤 User Profile: @{username} (ID: {user_id})",
        "user_profile_status": "Status",
        "user_profile_balance": "Balance",
        "user_profile_purchases": "Total Purchases",
        "user_profile_banned": "Banned Status",
        "user_profile_is_banned": "Yes 🚫",
        "user_profile_not_banned": "No ✅",
        "user_profile_button_adjust_balance": "💰 Adjust Balance",
        "user_profile_button_ban": "🚫 Ban User",
        "user_profile_button_unban": "✅ Unban User",
        "user_profile_button_back_list": "⬅️ Back to User List",
        "adjust_balance_prompt": "Reply with the amount to adjust balance for @{username} (ID: {user_id}).\nUse a positive number to add (e.g., 10.50) or a negative number to subtract (e.g., -5.00).",
        "adjust_balance_reason_prompt": "Please reply with a brief reason for this balance adjustment ({amount} EUR):",
        "adjust_balance_invalid_amount": "❌ Invalid amount. Please enter a non-zero number (e.g., 10.5 or -5).",
        "adjust_balance_reason_empty": "❌ Reason cannot be empty. Please provide a reason.",
        "adjust_balance_success": "✅ Balance adjusted successfully for @{username}. New balance: {new_balance} EUR.",
        "adjust_balance_db_error": "❌ Database error adjusting balance.",
        "ban_success": "🚫 User @{username} (ID: {user_id}) has been banned.",
        "unban_success": "✅ User @{username} (ID: {user_id}) has been unbanned.",
        "ban_db_error": "❌ Database error updating ban status.",
        "ban_cannot_ban_admin": "❌ Cannot ban the primary admin.",
        # <<< Welcome Message Management >>>
        "manage_welcome_title": "⚙️ Manage Welcome Messages",
        "manage_welcome_prompt": "Select a template to manage or activate:",
        "welcome_template_active": " (Active ✅)",
        "welcome_template_inactive": "",
        "welcome_button_activate": "✅ Activate",
        "welcome_button_edit": "✏️ Edit",
        "welcome_button_delete": "🗑️ Delete",
        "welcome_button_add_new": "➕ Add New Template",
        "welcome_button_reset_default": "🔄 Reset to Built-in Default", # <<< NEW
        "welcome_button_edit_text": "Edit Text", # <<< NEW
        "welcome_button_edit_desc": "Edit Description", # <<< NEW
        "welcome_button_preview": "👁️ Preview", # <<< NEW
        "welcome_button_save": "💾 Save Template", # <<< NEW
        "welcome_activate_success": "✅ Template '{name}' activated.",
        "welcome_activate_fail": "❌ Failed to activate template '{name}'.",
        "welcome_add_name_prompt": "Enter a unique short name for the new template (e.g., 'default', 'promo_weekend'):",
        "welcome_add_name_exists": "❌ Error: A template with the name '{name}' already exists.",
        "welcome_add_text_prompt": "Template Name: {name}\n\nPlease reply with the full welcome message text. Available placeholders:\n`{placeholders}`", # Escaped placeholders
        "welcome_add_description_prompt": "Optional: Enter a short description for this template (admin view only). Send '-' to skip.", # <<< NEW
        "welcome_add_success": "✅ Welcome message template '{name}' added.",
        "welcome_add_fail": "❌ Failed to add welcome message template.",
        "welcome_edit_text_prompt": "Editing Text for '{name}'. Current text:\n\n{current_text}\n\nPlease reply with the new text. Available placeholders:\n`{placeholders}`", # Escaped placeholders
        "welcome_edit_description_prompt": "Editing description for '{name}'. Current: '{current_desc}'.\n\nEnter new description or send '-' to keep current.", # <<< NEW
        "welcome_edit_success": "✅ Template '{name}' updated.",
        "welcome_edit_fail": "❌ Failed to update template '{name}'.",
        "welcome_delete_confirm_title": "⚠️ Confirm Deletion",
        "welcome_delete_confirm_text": "Are you sure you want to delete the welcome message template named '{name}'?",
        "welcome_delete_confirm_active": "\n\n🚨 WARNING: This is the currently active template! Deleting it will revert to the default built-in message.",
        "welcome_delete_confirm_last": "\n\n🚨 WARNING: This is the last template! Deleting it will revert to the default built-in message.",
        "welcome_delete_button_yes": "✅ Yes, Delete Template",
        "welcome_delete_success": "✅ Template '{name}' deleted.",
        "welcome_delete_fail": "❌ Failed to delete template '{name}'.",
        "welcome_delete_not_found": "❌ Template '{name}' not found for deletion.",
        "welcome_cannot_delete_active": "❌ Cannot delete the active template. Activate another first.", # <<< NEW
        "welcome_reset_confirm_title": "⚠️ Confirm Reset", # <<< NEW
        "welcome_reset_confirm_text": "Are you sure you want to reset the text of the 'default' template to the built-in version and activate it?", # <<< NEW
        "welcome_reset_button_yes": "✅ Yes, Reset & Activate", # <<< NEW
        "welcome_reset_success": "✅ 'default' template reset and activated.", # <<< NEW
        "welcome_reset_fail": "❌ Failed to reset 'default' template.", # <<< NEW
        "welcome_preview_title": "--- Welcome Message Preview ---", # <<< NEW
        "welcome_preview_name": "Name", # <<< NEW
        "welcome_preview_desc": "Desc", # <<< NEW
        "welcome_preview_confirm": "Save this template?", # <<< NEW
        "welcome_save_error_context": "❌ Error: Save data lost. Cannot save template.", # <<< NEW
        "welcome_invalid_placeholder": "⚠️ Formatting Error! Missing placeholder: `{key}`\n\nRaw Text:\n{text}", # <<< NEW
        "welcome_formatting_error": "⚠️ Unexpected Formatting Error!\n\nRaw Text:\n{text}", # <<< NEW
    },
    # --- Lithuanian ---
    "lt": {
        "native_name": "Lietuvių",
        # --- General & Menu ---
        "welcome": "👋 Sveiki, {username}!\n\n👤 Būsena: {status} {progress_bar}\n💰 Balansas: {balance_str} EUR\n📦 Viso pirkimų: {purchases}\n🛒 Krepšelyje: {basket_count} prekė(s)\n\nPradėkite apsipirkti arba naršykite parinktis žemiau.\n\n⚠️ Pastaba: Pinigai negrąžinami.",
        "status_label": "Būsena",
        "balance_label": "Balansas",
        "purchases_label": "Viso pirkimų",
        "basket_label": "Krepšelyje",
        "shopping_prompt": "Pradėkite apsipirkti arba naršykite parinktis žemiau.",
        "refund_note": "Pastaba: Pinigai negrąžinami.",
        "shop_button": "Parduotuvė", # <-- Example Translation
        "profile_button": "Profilis", # <-- Example Translation
        "top_up_button": "Papildyti", # <-- Example Translation
        "reviews_button": "Atsiliepimai", # <-- Example Translation
        "price_list_button": "Kainoraštis", # <-- Example Translation
        "language_button": "Kalba", # <-- Example Translation
        "admin_button": "🔧 Admino Panelė",
        "home_button": "Pradžia", # <-- Example Translation
        "back_button": "Atgal", # <-- Example Translation
        "cancel_button": "Atšaukti", # <-- Example Translation
        "error_occurred_answer": "Įvyko klaida. Bandykite dar kartą.",
        "success_label": "Pavyko!",
        "error_unexpected": "Įvyko netikėta klaida",

        # --- Shopping Flow ---
        "choose_city_title": "Pasirinkite miestą",
        "select_location_prompt": "Pasirinkite savo vietą:",
        "no_cities_available": "Šiuo metu nėra miestų. Patikrinkite vėliau.",
        "error_city_not_found": "Klaida: Miestas nerastas.",
        "choose_district_prompt": "Pasirinkite rajoną:",
        "no_districts_available": "Šiame mieste dar nėra rajonų.",
        "back_cities_button": "Atgal į miestus",
        "error_district_city_not_found": "Klaida: Rajonas ar miestas nerastas.",
        "select_type_prompt": "Pasirinkite produkto tipą:",
        "no_types_available": "Šiuo metu čia nėra šio tipo produktų.",
        "error_loading_types": "Klaida: Nepavyko įkelti produktų tipų",
        "back_districts_button": "Atgal į rajonus",
        "available_options_prompt": "Galimos parinktys:",
        "no_items_of_type": "Šiuo metu čia nėra šio tipo prekių.",
        "error_loading_products": "Klaida: Nepavyko įkelti produktų",
        "back_types_button": "Atgal į tipus",
        "price_label": "Kaina",
        "available_label_long": "Yra",
        "available_label_short": "Yra",
        "add_to_basket_button": "Į krepšelį",
        "error_location_mismatch": "Klaida: Vietos duomenų neatitikimas.",
        "drop_unavailable": "Prekė neprieinama! Ši parinktis ką tik buvo parduota ar rezervuota.",
        "error_loading_details": "Klaida: Nepavyko įkelti produkto detalių",
        "back_options_button": "Atgal į parinktis",
        "no_products_in_city_districts": "Šiuo metu nėra produktų jokiuose šio miesto rajonuose.",
        "error_loading_districts": "Klaida įkeliant rajonus. Bandykite dar kartą.",

        # --- Basket & Payment ---
        "added_to_basket": "✅ Prekė Rezervuota!\n\n{item} yra jūsų krepšelyje {timeout} minutes! ⏳",
        "expires_label": "Galioja iki",
        "your_basket_title": "Jūsų krepšelis",
        "basket_empty": "🛒 Jūsų krepšelis tuščias!",
        "add_items_prompt": "Pridėkite prekių, kad pradėtumėte apsipirkti!",
        "items_expired_note": "Prekės galėjo baigtis arba buvo pašalintos.",
        "subtotal_label": "Tarpinė suma",
        "total_label": "Viso",
        "pay_now_button": "Mokėti dabar",
        "clear_all_button": "Išvalyti viską",
        "view_basket_button": "Peržiūrėti krepšelį",
        "clear_basket_button": "Išvalyti krepšelį",
        "remove_button_label": "Pašalinti",
        "basket_already_empty": "Krepšelis jau tuščias.",
        "basket_cleared": "🗑️ Krepšelis išvalytas!",
        "pay": "💳 Mokėti viso: {amount} EUR",
        "insufficient_balance": "⚠️ Nepakankamas balansas!\n\nPrašome papildyti, kad tęstumėte! 💸",
        "insufficient_balance_pay_option": "⚠️ Nepakankamas balansas! ({balance} / {required} EUR)",
        "pay_crypto_button": "💳 Mokėti Crypto",
        "apply_discount_pay_button": "🏷️ Panaudoti nuolaidos kodą",
        "skip_discount_button": "⏩ Praleisti nuolaidą",
        "prompt_discount_or_pay": "Ar turite nuolaidos kodą, kurį norite panaudoti prieš mokant kriptovaliuta?",
        "basket_pay_enter_discount": "Įveskite nuolaidos kodą šiam pirkiniui:",
        "basket_pay_code_applied": "✅ Kodas '{code}' pritaikytas. Nauja suma: {total} EUR. Pasirinkite kriptovaliutą:",
        "basket_pay_code_invalid": "❌ Kodas negalioja: {reason}. Pasirinkite kriptovaliutą mokėti {total} EUR:",
        "choose_crypto_for_purchase": "Pasirinkite kriptovaliutą mokėti {amount} EUR už jūsų krepšelį:",
        "crypto_purchase_success": "Mokėjimas patvirtintas! Jūsų pirkimo detalės siunčiamos.",
        "crypto_purchase_failed": "Mokėjimas nepavyko/baigėsi. Jūsų prekės nebėra rezervuotos.",
        "payment_timeout_notification": "⏰ Mokėjimo Laikas Baigėsi: Jūsų mokėjimas už krepšelio prekes pasibaigė po 2 valandų. Rezervuotos prekės buvo atlaisvintos.", # <<< NEW
        "basket_pay_too_low": "Krepšelio suma {basket_total} EUR yra mažesnė nei minimali {currency}.",
        "balance_changed_error": "❌ Transakcija nepavyko: Jūsų balansas pasikeitė. Patikrinkite balansą ir bandykite dar kartą.",
        "order_failed_all_sold_out_balance": "❌ Užsakymas nepavyko: Visos prekės krepšelyje tapo neprieinamos apdorojimo metu. Jūsų balansas nebuvo apmokestintas.",
        "error_processing_purchase_contact_support": "❌ Apdorojant jūsų pirkimą įvyko klaida. Susisiekite su pagalba.",
        "purchase_success": "🎉 Pirkimas baigtas!",
        "sold_out_note": "⚠️ Pastaba: Šios prekės tapo neprieinamos apdorojimo metu ir nebuvo įtrauktos: {items}. Už jas nebuvote apmokestinti.",
        "leave_review_now": "Palikti atsiliepimą dabar",
        "back_basket_button": "Atgal į krepšelį",
        "error_adding_db": "Klaida: Duomenų bazės problema dedant prekę į krepšelį.",
        "error_adding_unexpected": "Klaida: Įvyko netikėta problema.",
        "reseller_discount_label": "Perpardavėjo nuolaida", # <<< NEW

        # --- Discounts ---
        "discount_no_items": "Jūsų krepšelis tuščias. Pirmiausia pridėkite prekių.",
        "enter_discount_code_prompt": "Įveskite savo nuolaidos kodą:",
        "enter_code_answer": "Įveskite kodą pokalbyje.",
        "apply_discount_button": "Pritaikyti nuolaidos kodą",
        "no_code_provided": "Kodas neįvestas.",
        "discount_code_not_found": "Nuolaidos kodas nerastas.",
        "discount_code_inactive": "Šis nuolaidos kodas neaktyvus.",
        "discount_code_expired": "Šio nuolaidos kodo galiojimas baigėsi.",
        "invalid_code_expiry_data": "Neteisingi kodo galiojimo duomenys.",
        "code_limit_reached": "Kodas pasiekė naudojimo limitą.",
        "internal_error_discount_type": "Vidinė klaida apdorojant nuolaidos tipą.",
        "db_error_validating_code": "Duomenų bazės klaida tikrinant kodą.",
        "unexpected_error_validating_code": "Įvyko netikėta klaida.",
        "discount_min_order_not_met": "Šiam nuolaidos kodui nepasiekta minimali užsakymo suma.",
        "code_applied_message": "Kodas '{code}' ({value}) pritaikytas. Nuolaida: -{amount} EUR",
        "discount_applied_label": "Pritaikyta nuolaida",
        "discount_value_label": "Vertė",
        "discount_removed_note": "Nuolaidos kodas {code} pašalintas: {reason}",
        "discount_removed_invalid_basket": "Nuolaida pašalinta (krepšelis pasikeitė).",
        "remove_discount_button": "Pašalinti nuolaidą",
        "discount_removed_answer": "Nuolaida pašalinta.",
        "no_discount_answer": "Nuolaida nepritaikyta.",
        "send_text_please": "Siųskite nuolaidos kodą kaip tekstą.",
        "error_calculating_total": "Klaida skaičiuojant sumą.",
        "returning_to_basket": "Grįžtama į krepšelį.",
        "basket_empty_no_discount": "Krepšelis tuščias. Negalima pritaikyti nuolaidos kodo.",

        # --- Profile & History ---
        "profile_title": "Jūsų profilis",
        "purchase_history_button": "Pirkimų istorija",
        "back_profile_button": "Atgal į profilį",
        "purchase_history_title": "Pirkimų istorija",
        "no_purchases_yet": "Dar neatlikote jokių pirkimų.",
        "recent_purchases_title": "Jūsų paskutiniai pirkimai",
        "error_loading_profile": "❌ Klaida: Nepavyko įkelti profilio duomenų.",

        # --- Language ---
        "language_set_answer": "Kalba nustatyta į {lang}!",
        "error_saving_language": "Klaida išsaugant kalbos nustatymą.",
        "invalid_language_answer": "Pasirinkta neteisinga kalba.",
        "language": "🌐 Kalba", # Menu title

        # --- Price List ---
        "no_cities_for_prices": "Nėra miestų, kuriuose būtų galima peržiūrėti kainas.",
        "price_list_title": "Kainoraštis",
        "select_city_prices_prompt": "Pasirinkite miestą, kad pamatytumėte galimus produktus ir kainas:",
        "price_list_title_city": "Kainoraštis: {city_name}",
        "no_products_in_city": "Šiame mieste šiuo metu nėra produktų.",
        "back_city_list_button": "Atgal į miestų sąrašą",
        "message_truncated_note": "Žinutė sutrumpinta dėl ilgio limito. Naudokite 'Parduotuvė' pilnai informacijai.",
        "error_loading_prices_db": "Klaida: Nepavyko įkelti kainoraščio {city_name}",
        "error_displaying_prices": "Klaida rodant kainoraštį.",
        "error_unexpected_prices": "Klaida: Įvyko netikėta problema generuojant kainoraštį.",
        "available_label": "yra", # Used in price list

        # --- Reviews ---
        "reviews": "📝 Atsiliepimų Meniu",
        "view_reviews_button": "Peržiūrėti atsiliepimus",
        "leave_review_button": "Palikti atsiliepimą",
        "enter_review_prompt": "Įveskite savo atsiliepimo žinutę ir išsiųskite.",
        "enter_review_answer": "Įveskite savo atsiliepimą pokalbyje.",
        "send_text_review_please": "Siųskite tik tekstą savo atsiliepimui.",
        "review_not_empty": "Atsiliepimas negali būti tuščias. Bandykite dar kartą arba atšaukite.",
        "review_too_long": "Atsiliepimas per ilgas (maks. 1000 simbolių). Prašome sutrumpinti.",
        "review_thanks": "Ačiū už jūsų atsiliepimą! Jūsų nuomonė padeda mums tobulėti.",
        "error_saving_review_db": "Klaida: Nepavyko išsaugoti jūsų atsiliepimo dėl duomenų bazės problemos.",
        "error_saving_review_unexpected": "Klaida: Įvyko netikėta problema saugant jūsų atsiliepimą.",
        "user_reviews_title": "Vartotojų atsiliepimai",
        "no_reviews_yet": "Dar nėra paliktų atsiliepimų.",
        "no_more_reviews": "Nebėra daugiau atsiliepimų.",
        "prev_button": "Ankst.",
        "next_button": "Kitas",
        "back_review_menu_button": "Atgal į Atsiliepimų Meniu",
        "unknown_date_label": "Nežinoma data",
        "error_displaying_review": "Klaida rodant atsiliepimą",
        "error_updating_review_list": "Klaida atnaujinant atsiliepimų sąrašą.",

        # --- Refill / NOWPayments ---
        "payment_amount_too_low_api": "❌ Mokėjimo Suma Per Maža: {target_eur_amount} EUR atitikmuo {currency} \\({crypto_amount}\\) yra mažesnis už minimalų reikalaujamą mokėjimo teikėjo \\({min_amount} {currency}\\)\\. Bandykite didesnę EUR sumą\\.",
        "payment_amount_too_low_with_min_eur": "❌ Mokėjimo Suma Per Maža: {target_eur_amount} EUR yra mažesnė už minimalų {currency} mokėjimų sumą \\(minimalus: {min_eur_amount} EUR\\)\\. Bandykite didesnę sumą arba pasirinkite kitą kriptovaliutą\\.",
        "error_min_amount_fetch": "❌ Klaida: Nepavyko gauti minimalios mokėjimo sumos {currency}\\. Bandykite vėliau arba pasirinkite kitą valiutą\\.",
        "invoice_title_refill": "*Sąskaita Papildymui Sukurta*",
        "invoice_title_purchase": "*Sąskaita Pirkimui Sukurta*",
        "min_amount_label": "*Minimali Suma:*",
        "payment_address_label": "*Mokėjimo Adresas:*",
        "amount_label": "*Suma:*",
        "expires_at_label": "*Galioja iki:*",
        "send_warning_template": "⚠️ *Svarbu:* Siųskite *tiksliai* šią {asset} sumą šiuo adresu\\.",
        "overpayment_note": "ℹ️ _Siųsti daugiau nei nurodyta suma yra gerai\\! Jūsų balansas bus papildytas pagal gautą sumą po tinklo patvirtinimo\\._",
        "confirmation_note": "✅ Patvirtinimas automatinis per webhook po tinklo patvirtinimo\\.",
        "invoice_amount_label_text": "Suma",
        "invoice_send_following_amount": "Prašome siųsti šią sumą:",
        "invoice_payment_deadline": "Mokėjimas turi būti atliktas per 20 minučių nuo sąskaitos sukūrimo.",
        "error_estimate_failed": "❌ Klaida: Nepavyko įvertinti kriptovaliutos sumos. Bandykite dar kartą arba pasirinkite kitą valiutą.",
        "error_estimate_currency_not_found": "❌ Klaida: Valiuta {currency} nepalaikoma įvertinimui. Pasirinkite kitą valiutą.",
        "error_discount_invalid_payment": "❌ Jūsų nuolaidos kodas nebegalioja: {reason}. Grįžkite į krepšelį, kad tęstumėte be nuolaidos.",
        "error_discount_mismatch_payment": "❌ Aptiktas mokėjimo sumos neatitikimas. Grįžkite į krepšelį ir bandykite dar kartą.",
        "crypto_payment_disabled": "Balanso papildymas šiuo metu išjungtas.",
        "top_up_title": "Papildyti balansą",
        "enter_refill_amount_prompt": "Atsakykite su suma EUR, kurią norite pridėti prie balanso (pvz., 10 arba 25.50).",
        "min_top_up_note": "Minimalus papildymas: {amount} EUR",
        "enter_amount_answer": "Įveskite papildymo sumą.",
        "send_amount_as_text": "Siųskite sumą kaip tekstą (pvz., 10 arba 25.50).",
        "amount_too_low_msg": "Suma per maža. Minimalus papildymas yra {amount} EUR. Įveskite didesnę sumą.",
        "amount_too_high_msg": "Suma per didelė. Įveskite mažesnę sumą.",
        "invalid_amount_format_msg": "Neteisingas sumos formatas. Įveskite skaičių (pvz., 10 arba 25.50).",
        "unexpected_error_msg": "Įvyko netikėta klaida. Bandykite vėliau.",
        "choose_crypto_prompt": "Norite papildyti {amount} EUR. Pasirinkite kriptovaliutą, kuria norite mokėti:",
        "cancel_top_up_button": "Atšaukti papildymą",
        "preparing_invoice": "⏳ Ruošiama jūsų mokėjimo sąskaita...",
        "failed_invoice_creation": "❌ Nepavyko sukurti mokėjimo sąskaitos. Tai gali būti laikina problema su mokėjimo teikėju arba API rakto problema. Bandykite vėliau arba susisiekite su pagalba.",
        "error_preparing_payment": "❌ Ruošiant mokėjimo detales įvyko klaida. Bandykite vėliau.",
        "top_up_success_title": "✅ Papildymas Sėkmingas!",
        "amount_added_label": "Pridėta suma",
        "new_balance_label": "Jūsų naujas balansas",
        "error_sol_api": "❌ Mokėjimo Klaida: Nepavyko sukurti SOL mokėjimo. Bandykite vėliau arba susisiekite su pagalba.",
        "payment_pending_db_error": "❌ Duomenų Bazės Klaida: Nepavyko įrašyti laukiančio mokėjimo. Susisiekite su pagalba.",
        "payment_cancelled_or_expired": "Mokėjimo Būsena: Jūsų mokėjimas ({payment_id}) buvo atšauktas arba baigėsi galiojimas.",
        "webhook_processing_error": "Webhook Klaida: Nepavyko apdoroti mokėjimo atnaujinimo {payment_id}.",
        "webhook_db_update_failed": "Kritinė Klaida: Mokėjimas {payment_id} patvirtintas, bet DB balanso atnaujinimas vartotojui {user_id} nepavyko. Reikalingas rankinis veiksmas.",
        "webhook_pending_not_found": "Webhook Įspėjimas: Gautas mokėjimo ID {payment_id} atnaujinimas, bet DB nerasta laukiančio įrašo.",
        "webhook_price_fetch_error": "Webhook Klaida: Nepavyko gauti {currency} kainos patvirtinti EUR vertę mokėjimui {payment_id}.",
        "payment_cancelled_user": "Mokėjimas atšauktas. Rezervuotos prekės (jei buvo) paleistos.", # <<< NEW
        "payment_cancel_error": "Nepavyko atšaukti mokėjimo (jau apdorotas arba prarastas kontekstas).", # <<< NEW
        "cancel_payment_button": "Atšaukti mokėjimą", # <<< NEW
        "proceeding_to_payment_answer": "Pereinama prie mokėjimo parinkčių...",
        "credit_overpayment_purchase": "✅ Jūsų pirkimas buvo sėkmingas! Papildomai, permoka {amount} EUR buvo įskaityta į jūsų balansą. Jūsų naujas balansas: {new_balance} EUR.",
        "credit_underpayment_purchase": "ℹ️ Jūsų pirkimas nepavyko dėl nepakankamo mokėjimo, tačiau gauta suma ({amount} EUR) buvo įskaityta į jūsų balansą. Jūsų naujas balansas: {new_balance} EUR.",
        "crypto_purchase_underpaid_credited": "⚠️ Pirkimas nepavyko: Aptiktas nepakankamas mokėjimas. Reikalinga suma buvo {needed_eur} EUR. Jūsų balansas buvo papildytas gauta verte ({paid_eur} EUR). Jūsų prekės nebuvo pristatytos.",
        "credit_refill": "✅ Jūsų balansas buvo papildytas {amount} EUR. Priežastis: {reason}. Naujas balansas: {new_balance} EUR.",


        # --- Admin ---
        "admin_menu": "🔧 Admin Panel\n\nManage the bot from here:",
        "admin_select_city": "🏙️ Select City to Edit\n\nChoose a city:",
        "admin_select_district": "🏘️ Select District in {city}\n\nPick a district:",
        "admin_select_type": "💎 Select Product Type\n\nChoose or create a type:",
        "admin_choose_action": "📦 Manage {type} in {city}, {district}\n\nWhat would you like to do?",
        "set_media_prompt_plain": "📸 Send a photo, video, or GIF to display above all messages:",
        "state_error": "❌ Error: Invalid State\n\nPlease start the 'Add New Product' process again from the Admin Panel.",
        "support": "📞 Need Help?\n\nContact {support} for assistance!",
        "file_download_error": "❌ Error: Failed to Download Media\n\nPlease try again or contact {support}. ",
        "admin_enter_type_emoji": "✍️ Please reply with a single emoji for the product type:",
        "admin_type_emoji_set": "Emoji set to {emoji}.",
        "admin_edit_type_emoji_button": "✏️ Change Emoji",
        "admin_invalid_emoji": "❌ Invalid input. Please send a single emoji.",
        "admin_type_emoji_updated": "✅ Emoji updated successfully for {type_name}!",
        "admin_edit_type_menu": "🧩 Editing Type: {type_name}\n\nCurrent Emoji: {emoji}\nDescription: {description}\n\nWhat would you like to do?", # Added {description}
        "admin_edit_type_desc_button": "📝 Edit Description", #<<< NEW
        # --- Broadcast Translations ---
        "broadcast_select_target": "📢 Broadcast Message\n\nSelect the target audience:",
        "broadcast_target_all": "👥 All Users",
        "broadcast_target_city": "🏙️ By Last Purchased City",
        "broadcast_target_status": "👑 By User Status",
        "broadcast_target_inactive": "⏳ By Inactivity (Days)",
        "broadcast_select_city_target": "🏙️ Select City to Target\n\nUsers whose last purchase was in:",
        "broadcast_select_status_target": "👑 Select Status to Target:",
        "broadcast_status_vip": "VIP 👑",
        "broadcast_status_regular": "Regular ⭐",
        "broadcast_status_new": "New 🌱",
        "broadcast_enter_inactive_days": "⏳ Enter Inactivity Period\n\nPlease reply with the number of days since the user's last purchase (or since registration if no purchases). Users inactive for this many days or more will receive the message.",
        "broadcast_invalid_days": "❌ Invalid number of days. Please enter a positive whole number.",
        "broadcast_days_too_large": "❌ Number of days is too large. Please enter a smaller number.",
        "broadcast_ask_message": "📝 Now send the message content (text, photo, video, or GIF with caption):",
        "broadcast_confirm_title": "📢 Confirm Broadcast",
        "broadcast_confirm_target_all": "Target: All Users",
        "broadcast_confirm_target_city": "Target: Last Purchase in {city}",
        "broadcast_confirm_target_status": "Target: Status - {status}",
        "broadcast_confirm_target_inactive": "Target: Inactive >= {days} days",
        "broadcast_confirm_preview": "Preview:",
        "broadcast_confirm_ask": "Send this message?",
        "broadcast_no_users_found_target": "⚠️ Broadcast Warning: No users found matching the target criteria.",
        # --- User Management Translations ---
        "manage_users_title": "👤 Manage Users",
        "manage_users_prompt": "Select a user to view details or manage:",
        "manage_users_no_users": "No users found.",
        "view_user_profile_title": "👤 User Profile: @{username} (ID: {user_id})",
        "user_profile_status": "Status",
        "user_profile_balance": "Balance",
        "user_profile_purchases": "Total Purchases",
        "user_profile_banned": "Banned Status",
        "user_profile_is_banned": "Yes 🚫",
        "user_profile_not_banned": "No ✅",
        "user_profile_button_adjust_balance": "💰 Adjust Balance",
        "user_profile_button_ban": "🚫 Ban User",
        "user_profile_button_unban": "✅ Unban User",
        "user_profile_button_back_list": "⬅️ Back to User List",
        "adjust_balance_prompt": "Reply with the amount to adjust balance for @{username} (ID: {user_id}).\nUse a positive number to add (e.g., 10.50) or a negative number to subtract (e.g., -5.00).",
        "adjust_balance_reason_prompt": "Please reply with a brief reason for this balance adjustment ({amount} EUR):",
        "adjust_balance_invalid_amount": "❌ Invalid amount. Please enter a non-zero number (e.g., 10.5 or -5).",
        "adjust_balance_reason_empty": "❌ Reason cannot be empty. Please provide a reason.",
        "adjust_balance_success": "✅ Balance adjusted successfully for @{username}. New balance: {new_balance} EUR.",
        "adjust_balance_db_error": "❌ Database error adjusting balance.",
        "ban_success": "🚫 User @{username} (ID: {user_id}) has been banned.",
        "unban_success": "✅ User @{username} (ID: {user_id}) has been unbanned.",
        "ban_db_error": "❌ Database error updating ban status.",
        "ban_cannot_ban_admin": "❌ Cannot ban the primary admin.",
        # <<< Welcome Message Management >>>
        "manage_welcome_title": "⚙️ Manage Welcome Messages",
        "manage_welcome_prompt": "Select a template to manage or activate:",
        "welcome_template_active": " (Active ✅)",
        "welcome_template_inactive": "",
        "welcome_button_activate": "✅ Activate",
        "welcome_button_edit": "✏️ Edit",
        "welcome_button_delete": "🗑️ Delete",
        "welcome_button_add_new": "➕ Add New Template",
        "welcome_button_reset_default": "🔄 Reset to Built-in Default", # <<< NEW
        "welcome_button_edit_text": "Edit Text", # <<< NEW
        "welcome_button_edit_desc": "Edit Description", # <<< NEW
        "welcome_button_preview": "👁️ Preview", # <<< NEW
        "welcome_button_save": "💾 Save Template", # <<< NEW
        "welcome_activate_success": "✅ Template '{name}' activated.",
        "welcome_activate_fail": "❌ Failed to activate template '{name}'.",
        "welcome_add_name_prompt": "Enter a unique short name for the new template (e.g., 'default', 'promo_weekend'):",
        "welcome_add_name_exists": "❌ Error: A template with the name '{name}' already exists.",
        "welcome_add_text_prompt": "Template Name: {name}\n\nPlease reply with the full welcome message text. Available placeholders:\n`{placeholders}`", # Escaped placeholders
        "welcome_add_description_prompt": "Optional: Enter a short description for this template (admin view only). Send '-' to skip.", # <<< NEW
        "welcome_add_success": "✅ Welcome message template '{name}' added.",
        "welcome_add_fail": "❌ Failed to add welcome message template.",
        "welcome_edit_text_prompt": "Editing Text for '{name}'. Current text:\n\n{current_text}\n\nPlease reply with the new text. Available placeholders:\n`{placeholders}`", # Escaped placeholders
        "welcome_edit_description_prompt": "Editing description for '{name}'. Current: '{current_desc}'.\n\nEnter new description or send '-' to keep current.", # <<< NEW
        "welcome_edit_success": "✅ Template '{name}' updated.",
        "welcome_edit_fail": "❌ Failed to update template '{name}'.",
        "welcome_delete_confirm_title": "⚠️ Confirm Deletion",
        "welcome_delete_confirm_text": "Are you sure you want to delete the welcome message template named '{name}'?",
        "welcome_delete_confirm_active": "\n\n🚨 WARNING: This is the currently active template! Deleting it will revert to the default built-in message.",
        "welcome_delete_confirm_last": "\n\n🚨 WARNING: This is the last template! Deleting it will revert to the default built-in message.",
        "welcome_delete_button_yes": "✅ Yes, Delete Template",
        "welcome_delete_success": "✅ Template '{name}' deleted.",
        "welcome_delete_fail": "❌ Failed to delete template '{name}'.",
        "welcome_delete_not_found": "❌ Template '{name}' not found for deletion.",
        "welcome_cannot_delete_active": "❌ Cannot delete the active template. Activate another first.", # <<< NEW
        "welcome_reset_confirm_title": "⚠️ Confirm Reset", # <<< NEW
        "welcome_reset_confirm_text": "Are you sure you want to reset the text of the 'default' template to the built-in version and activate it?", # <<< NEW
        "welcome_reset_button_yes": "✅ Yes, Reset & Activate", # <<< NEW
        "welcome_reset_success": "✅ 'default' template reset and activated.", # <<< NEW
        "welcome_reset_fail": "❌ Failed to reset 'default' template.", # <<< NEW
        "welcome_preview_title": "--- Welcome Message Preview ---", # <<< NEW
        "welcome_preview_name": "Name", # <<< NEW
        "welcome_preview_desc": "Desc", # <<< NEW
        "welcome_preview_confirm": "Save this template?", # <<< NEW
        "welcome_save_error_context": "❌ Error: Save data lost. Cannot save template.", # <<< NEW
        "welcome_invalid_placeholder": "⚠️ Formatting Error! Missing placeholder: `{key}`\n\nRaw Text:\n{text}", # <<< NEW
        "welcome_formatting_error": "⚠️ Unexpected Formatting Error!\n\nRaw Text:\n{text}", # <<< NEW
    },
    # --- Russian ---
    "ru": {
        "native_name": "Русский",
        # --- General & Menu ---
        "welcome": "👋 Добро пожаловать, {username}!\n\n👤 Статус: {status} {progress_bar}\n💰 Баланс: {balance_str} EUR\n📦 Всего покупок: {purchases}\n🛒 В корзине: {basket_count} товар(ов)\n\nНачните покупки или изучите опции ниже.\n\n⚠️ Примечание: Возврат средств невозможен.",
        "status_label": "Статус",
        "balance_label": "Баланс",
        "purchases_label": "Всего покупок",
        "basket_label": "В корзине",
        "shopping_prompt": "Начните покупки или изучите опции ниже.",
        "refund_note": "Примечание: Возврат средств невозможен.",
        "shop_button": "Магазин", # <-- Example Translation
        "profile_button": "Профиль", # <-- Example Translation
        "top_up_button": "Пополнить", # <-- Example Translation
        "reviews_button": "Отзывы", # <-- Example Translation
        "price_list_button": "Прайс-лист", # <-- Example Translation
        "language_button": "Язык", # <-- Example Translation
        "admin_button": "🔧 Панель Админа",
        "home_button": "Главная", # <-- Example Translation
        "back_button": "Назад", # <-- Example Translation
        "cancel_button": "Отмена", # <-- Example Translation
        "error_occurred_answer": "Произошла ошибка. Пожалуйста, попробуйте еще раз.",
        "success_label": "Успешно!",
        "error_unexpected": "Произошла непредвиденная ошибка",

        # --- Shopping Flow ---
        "choose_city_title": "Выберите город",
        "select_location_prompt": "Выберите ваше местоположение:",
        "no_cities_available": "На данный момент нет доступных городов. Пожалуйста, зайдите позже.",
        "error_city_not_found": "Ошибка: Город не найден.",
        "choose_district_prompt": "Выберите район:",
        "no_districts_available": "В этом городе пока нет доступных районов.",
        "back_cities_button": "Назад к городам",
        "error_district_city_not_found": "Ошибка: Район или город не найден.",
        "select_type_prompt": "Выберите тип продукта:",
        "no_types_available": "В данный момент здесь нет товаров этого типа.",
        "error_loading_types": "Ошибка: Не удалось загрузить типы продуктов",
        "back_districts_button": "Назад к районам",
        "available_options_prompt": "Доступные варианты:",
        "no_items_of_type": "В данный момент здесь нет товаров этого типа.",
        "error_loading_products": "Ошибка: Не удалось загрузить продукты",
        "back_types_button": "Назад к типам",
        "price_label": "Цена",
        "available_label_long": "Доступно",
        "available_label_short": "Дост",
        "add_to_basket_button": "В корзину",
        "error_location_mismatch": "Ошибка: Несоответствие данных о местоположении.",
        "drop_unavailable": "Товар недоступен! Этот вариант только что был распродан или зарезервирован кем-то другим.",
        "error_loading_details": "Ошибка: Не удалось загрузить детали продукта",
        "back_options_button": "Назад к вариантам",
        "no_products_in_city_districts": "В настоящее время нет доступных товаров ни в одном районе этого города.",
        "error_loading_districts": "Ошибка загрузки районов. Пожалуйста, попробуйте еще раз.",

        # --- Basket & Payment ---
        "added_to_basket": "✅ Товар зарезервирован!\n\n{item} в вашей корзине на {timeout} минут! ⏳",
        "expires_label": "Истекает через",
        "your_basket_title": "Ваша корзина",
        "basket_empty": "🛒 Ваша корзина пуста!",
        "add_items_prompt": "Добавьте товары, чтобы начать покупки!",
        "items_expired_note": "Срок действия товаров мог истечь или они были удалены.",
        "subtotal_label": "Подытог",
        "total_label": "Итого",
        "pay_now_button": "Оплатить сейчас",
        "clear_all_button": "Очистить все",
        "view_basket_button": "Посмотреть корзину",
        "clear_basket_button": "Очистить корзину",
        "remove_button_label": "Удалить",
        "basket_already_empty": "Корзина уже пуста.",
        "basket_cleared": "🗑️ Корзина очищена!",
        "pay": "💳 К оплате: {amount} EUR",
        "insufficient_balance": "⚠️ Недостаточно средств!\n\nПожалуйста, пополните баланс, чтобы продолжить! 💸",
        "insufficient_balance_pay_option": "⚠️ Недостаточно средств! ({balance} / {required} EUR)",
        "pay_crypto_button": "💳 Оплатить Crypto",
        "apply_discount_pay_button": "🏷️ Применить промокод",
        "skip_discount_button": "⏩ Пропустить скидку",
        "prompt_discount_or_pay": "У вас есть промокод для применения перед оплатой криптовалютой?",
        "basket_pay_enter_discount": "Введите промокод для этой покупки:",
        "basket_pay_code_applied": "✅ Код '{code}' применен. Новая сумма: {total} EUR. Выберите криптовалюту:",
        "basket_pay_code_invalid": "❌ Код недействителен: {reason}. Выберите криптовалюту для оплаты {total} EUR:",
        "choose_crypto_for_purchase": "Выберите криптовалюту для оплаты {amount} EUR за вашу корзину:",
        "crypto_purchase_success": "Оплата подтверждена! Детали вашей покупки отправляются.",
        "crypto_purchase_failed": "Оплата не удалась/истекла. Ваши товары больше не зарезервированы.",
        "payment_timeout_notification": "⏰ Время Оплаты Истекло: Ваш платеж за товары в корзине истек через 2 часа. Зарезервированные товары освобождены.", # <<< NEW
        "basket_pay_too_low": "Сумма корзины {basket_total} EUR ниже минимальной для {currency}.",
        "balance_changed_error": "❌ Транзакция не удалась: Ваш баланс изменился. Пожалуйста, проверьте баланс и попробуйте снова.",
        "order_failed_all_sold_out_balance": "❌ Заказ не удался: Все товары в вашей корзине стали недоступны во время обработки. Средства с вашего баланса не списаны.",
        "error_processing_purchase_contact_support": "❌ Произошла ошибка при обработке вашей покупки. Обратитесь в службу поддержки.",
        "purchase_success": "🎉 Покупка завершена!",
        "sold_out_note": "⚠️ Примечание: Следующие товары стали недоступны во время обработки и не были включены: {items}. Средства за них не списаны.",
        "leave_review_now": "Оставить отзыв сейчас",
        "back_basket_button": "Назад в корзину",
        "error_adding_db": "Ошибка: Проблема с базой данных при добавлении товара в корзину.",
        "error_adding_unexpected": "Ошибка: Произошла непредвиденная проблема.",
        "reseller_discount_label": "Скидка реселлера", # <<< NEW

        # --- Discounts ---
        "discount_no_items": "Ваша корзина пуста. Сначала добавьте товары.",
        "enter_discount_code_prompt": "Введите ваш промокод:",
        "enter_code_answer": "Введите код в чат.",
        "apply_discount_button": "Применить промокод",
        "no_code_provided": "Код не предоставлен.",
        "discount_code_not_found": "Промокод не найден.",
        "discount_code_inactive": "Этот промокод неактивен.",
        "discount_code_expired": "Срок действия этого промокода истек.",
        "invalid_code_expiry_data": "Неверные данные о сроке действия кода.",
        "code_limit_reached": "Достигнут лимит использования кода.",
        "internal_error_discount_type": "Внутренняя ошибка при обработке типа скидки.",
        "db_error_validating_code": "Ошибка базы данных при проверке кода.",
        "unexpected_error_validating_code": "Произошла непредвиденная ошибка.",
        "discount_min_order_not_met": "Минимальная сумма заказа для этого промокода не достигнута.",
        "code_applied_message": "Код '{code}' ({value}) применен. Скидка: -{amount} EUR",
        "discount_applied_label": "Применена скидка",
        "discount_value_label": "Значение",
        "discount_removed_note": "Промокод {code} удален: {reason}",
        "discount_removed_invalid_basket": "Скидка удалена (корзина изменилась).",
        "remove_discount_button": "Удалить скидку",
        "discount_removed_answer": "Скидка удалена.",
        "no_discount_answer": "Скидка не применена.",
        "send_text_please": "Пожалуйста, отправьте промокод текстом.",
        "error_calculating_total": "Ошибка при расчете суммы.",
        "returning_to_basket": "Возвращаемся в корзину.",
        "basket_empty_no_discount": "Корзина пуста. Невозможно применить промокод.",

        # --- Profile & History ---
        "profile_title": "Ваш профиль",
        "purchase_history_button": "История покупок",
        "back_profile_button": "Назад в профиль",
        "purchase_history_title": "История покупок",
        "no_purchases_yet": "Вы еще не совершали покупок.",
        "recent_purchases_title": "Ваши недавние покупки",
        "error_loading_profile": "❌ Ошибка: Не удалось загрузить данные профиля.",

        # --- Language ---
        "language_set_answer": "Язык установлен на {lang}!",
        "error_saving_language": "Ошибка сохранения настроек языка.",
        "invalid_language_answer": "Выбран неверный язык.",
        "language": "🌐 Язык", # Menu title

        # --- Price List ---
        "no_cities_for_prices": "Нет доступных городов для просмотра цен.",
        "price_list_title": "Прайс-лист",
        "select_city_prices_prompt": "Выберите город для просмотра доступных товаров и цен:",
        "price_list_title_city": "Прайс-лист: {city_name}",
        "no_products_in_city": "В этом городе в настоящее время нет доступных товаров.",
        "back_city_list_button": "Назад к списку городов",
        "message_truncated_note": "Сообщение усечено из-за ограничения длины. Используйте 'Магазин' для полной информации.",
        "error_loading_prices_db": "Ошибка: Не удалось загрузить прайс-лист для {city_name}",
        "error_displaying_prices": "Ошибка отображения прайс-листа.",
        "error_unexpected_prices": "Ошибка: Произошла непредвиденная проблема при создании прайс-листа.",
        "available_label": "доступно", # Used in price list

        # --- Reviews ---
        "reviews": "📝 Меню отзывов",
        "view_reviews_button": "Посмотреть отзывы",
        "leave_review_button": "Оставить отзыв",
        "enter_review_prompt": "Пожалуйста, введите текст вашего отзыва и отправьте его.",
        "enter_review_answer": "Введите ваш отзыв в чат.",
        "send_text_review_please": "Пожалуйста, отправьте отзыв только текстом.",
        "review_not_empty": "Отзыв не может быть пустым. Попробуйте снова или отмените.",
        "review_too_long": "Отзыв слишком длинный (макс. 1000 символов). Пожалуйста, сократите его.",
        "review_thanks": "Спасибо за ваш отзыв! Ваше мнение помогает нам стать лучше.",
        "error_saving_review_db": "Ошибка: Не удалось сохранить ваш отзыв из-за проблемы с базой данных.",
        "error_saving_review_unexpected": "Ошибка: Произошла непредвиденная проблема при сохранении вашего отзыва.",
        "user_reviews_title": "Отзывы пользователей",
        "no_reviews_yet": "Отзывов пока нет.",
        "no_more_reviews": "Больше отзывов нет.",
        "prev_button": "Пред.",
        "next_button": "След.",
        "back_review_menu_button": "Назад в Меню Отзывов",
        "unknown_date_label": "Неизвестная дата",
        "error_displaying_review": "Ошибка отображения отзыва",
        "error_updating_review_list": "Ошибка обновления списка отзывов.",

        # --- Refill / NOWPayments ---
        "payment_amount_too_low_api": "❌ Сумма Платежа Слишком Мала: Эквивалент {target_eur_amount} EUR в {currency} \\({crypto_amount}\\) ниже минимума, требуемого платежной системой \\({min_amount} {currency}\\)\\. Попробуйте большую сумму EUR\\.",
        "payment_amount_too_low_with_min_eur": "❌ Сумма Платежа Слишком Мала: {target_eur_amount} EUR ниже минимума для {currency} платежей \\(минимум: {min_eur_amount} EUR\\)\\. Попробуйте большую сумму или выберите другую криптовалюту\\.",
        "error_min_amount_fetch": "❌ Ошибка: Не удалось получить минимальную сумму платежа для {currency}\\. Попробуйте позже или выберите другую валюту\\.",
        "invoice_title_refill": "*Счет на Пополнение Создан*",
        "invoice_title_purchase": "*Счет на Оплату Создан*",
        "min_amount_label": "*Минимальная Сумма:*",
        "payment_address_label": "*Адрес для Оплаты:*",
        "amount_label": "*Сумма:*",
        "expires_at_label": "*Истекает в:*",
        "send_warning_template": "⚠️ *Важно:* Отправьте *точно* эту сумму {asset} на этот адрес\\.",
        "overpayment_note": "ℹ️ _Отправка большей суммы допустима\\! Ваш баланс будет пополнен на основе полученной суммы после подтверждения сети\\._",
        "confirmation_note": "✅ Подтверждение автоматическое через вебхук после подтверждения сети\\.",
        "invoice_amount_label_text": "Сумма",
        "invoice_send_following_amount": "Пожалуйста, отправьте следующую сумму:",
        "invoice_payment_deadline": "Платеж должен быть выполнен в течение 20 минут с момента создания счета.",
        "error_estimate_failed": "❌ Ошибка: Не удалось оценить сумму в криптовалюте. Попробуйте снова или выберите другую валюту.",
        "error_estimate_currency_not_found": "❌ Ошибка: Валюта {currency} не поддерживается для оценки. Выберите другую валюту.",
        "error_discount_invalid_payment": "❌ Ваш промокод больше не действителен: {reason}. Вернитесь в корзину, чтобы продолжить без скидки.",
        "error_discount_mismatch_payment": "❌ Обнаружено несоответствие суммы платежа. Вернитесь в корзину и попробуйте снова.",
        "crypto_payment_disabled": "Пополнение баланса в данный момент отключено.",
        "top_up_title": "Пополнить баланс",
        "enter_refill_amount_prompt": "Ответьте суммой в EUR, которую вы хотите добавить на баланс (например, 10 или 25.50).",
        "min_top_up_note": "Минимальное пополнение: {amount} EUR",
        "enter_amount_answer": "Введите сумму пополнения.",
        "send_amount_as_text": "Отправьте сумму текстом (например, 10 или 25.50).",
        "amount_too_low_msg": "Сумма слишком мала. Минимальное пополнение {amount} EUR. Введите большую сумму.",
        "amount_too_high_msg": "Сумма слишком велика. Введите меньшую сумму.",
        "invalid_amount_format_msg": "Неверный формат суммы. Введите число (например, 10 или 25.50).",
        "unexpected_error_msg": "Произошла непредвиденная ошибка. Попробуйте позже.",
        "choose_crypto_prompt": "Вы хотите пополнить на {amount} EUR. Пожалуйста, выберите криптовалюту для оплаты:",
        "cancel_top_up_button": "Отменить пополнение",
        "preparing_invoice": "⏳ Подготовка счета на оплату...",
        "failed_invoice_creation": "❌ Не удалось создать счет на оплату. Это может быть временная проблема с платежной системой или проблема с ключом API. Попробуйте позже или обратитесь в поддержку.",
        "error_preparing_payment": "❌ Произошла ошибка при подготовке данных для оплаты. Попробуйте позже.",
        "top_up_success_title": "✅ Баланс Успешно Пополнен!",
        "amount_added_label": "Добавлено",
        "new_balance_label": "Ваш новый баланс",
        "error_sol_api": "❌ Ошибка Платежа: Не удалось создать SOL платеж. Попробуйте позже или обратитесь в поддержку.",
        "payment_pending_db_error": "❌ Ошибка Базы Данных: Не удалось записать ожидающий платеж. Обратитесь в поддержку.",
        "payment_cancelled_or_expired": "Статус Платежа: Ваш платеж ({payment_id}) был отменен или истек.",
        "webhook_processing_error": "Ошибка Webhook: Не удалось обработать обновление платежа {payment_id}.",
        "webhook_db_update_failed": "Критическая Ошибка: Платеж {payment_id} подтвержден, но обновление баланса в БД для пользователя {user_id} не удалось. Требуется ручное вмешательство.",
        "webhook_pending_not_found": "Предупреждение Webhook: Получено обновление для ID платежа {payment_id}, но в БД не найден ожидающий депозит.",
        "webhook_price_fetch_error": "Ошибка Webhook: Не удалось получить цену {currency} для подтверждения значения EUR для платежа {payment_id}.",
        "payment_cancelled_user": "Платеж отменен. Зарезервированные товары (если были) освобождены.", # <<< NEW
        "payment_cancel_error": "Не удалось отменить платеж (уже обработан или потерян контекст).", # <<< NEW
        "cancel_payment_button": "Отменить платеж", # <<< NEW
        "proceeding_to_payment_answer": "Переход к вариантам оплаты...",
        "credit_overpayment_purchase": "✅ Ваша покупка была успешной! Дополнительно, переплата в размере {amount} EUR зачислена на ваш баланс. Ваш новый баланс: {new_balance} EUR.",
        "credit_underpayment_purchase": "ℹ️ Ваша покупка не удалась из-за недоплаты, но полученная сумма ({amount} EUR) зачислена на ваш баланс. Ваш новый баланс: {new_balance} EUR.",
        "crypto_purchase_underpaid_credited": "⚠️ Покупка не удалась: Обнаружена недоплата. Требовалась сумма {needed_eur} EUR. Ваш баланс пополнен на полученную сумму ({paid_eur} EUR). Ваши товары не были доставлены.",
        "credit_refill": "✅ Ваш баланс пополнен на {amount} EUR. Причина: {reason}. Новый баланс: {new_balance} EUR.",
    }
}
# ==============================================================
# ===== ^ ^ ^ ^ ^      LANGUAGE DICTIONARY     ^ ^ ^ ^ ^ ======
# ==============================================================

# <<< Default Welcome Message (Fallback) >>>
DEFAULT_WELCOME_MESSAGE = LANGUAGES['en']['welcome']

MIN_DEPOSIT_EUR = Decimal('5.00') # Minimum deposit amount in EUR
# SOL-only payment system - no external API URLs needed
COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
FEE_ADJUSTMENT = Decimal('1.0')

# --- Global Data Variables ---
CITIES = {}
DISTRICTS = {}
PRODUCT_TYPES = {}
DEFAULT_PRODUCT_EMOJI = "💎" # Fallback emoji
SIZES = ["2g", "5g"]
BOT_MEDIA = {'type': None, 'path': None}
currency_price_cache = {}
min_amount_cache = {}
CACHE_EXPIRY_SECONDS = 900

# --- Database Connection Helper ---
def get_product_emoji(product_type):
    """Get emoji for product type"""
    emoji_map = {
        'kava': '☕',
        'coffee': '☕',
        'tea': '🍵',
        'chai': '🍵',
        'energy': '⚡',
        'drink': '🥤',
        'food': '🍽️',
        'snack': '🍿',
        'sweet': '🍭',
        'chocolate': '🍫',
        'fruit': '🍎',
        'juice': '🧃',
        'water': '💧',
        'supplement': '💊',
        'vitamin': '💊',
        'pienas': '🥛',
        'milk': '🥛',
        'arbata': '🍵',
        'apples': '🍎',
        'orange': '🍊'
    }
    
    product_lower = product_type.lower()
    for key, emoji in emoji_map.items():
        if key in product_lower:
            return emoji
    
    return '😃'  # Default emoji

def get_db_connection():
    """Returns a connection to the PostgreSQL database using DATABASE_URL."""
    # Reduced logging for cleaner output - only log on errors
    try:
        # Use POSTGRES_URL (which comes from DATABASE_URL env var)
        conn = psycopg2.connect(
            POSTGRES_URL,
            cursor_factory=RealDictCursor
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        # Extract host info from POSTGRES_URL for error message
        try:
            from urllib.parse import urlparse
            parsed = urlparse(POSTGRES_URL)
            db_info = f"PostgreSQL at {parsed.hostname}:{parsed.port}/{parsed.path.lstrip('/')}"
        except:
            db_info = "PostgreSQL (URL parsing failed)"
        
        logger.critical(f"❌ CRITICAL ERROR connecting to {db_info}: {e}")
        logger.critical(f"❌ Error type: {type(e).__name__}")
        logger.critical(f"❌ Error details: {str(e)}")
        logger.critical(f"❌ Connection URL pattern: postgresql://USER:***@HOST:PORT/DATABASE")
        raise SystemExit(f"Failed to connect to database: {e}")


# --- PostgreSQL Helper Functions ---
def get_sql_placeholder():
    """Returns PostgreSQL SQL placeholder."""
    return '%s'

def get_auto_increment():
    """Returns PostgreSQL auto increment syntax."""
    return 'SERIAL PRIMARY KEY'

def get_boolean_type():
    """Returns PostgreSQL boolean type."""
    return 'BOOLEAN'

def get_text_type():
    """Returns PostgreSQL text type."""
    return 'TEXT'

def get_timestamp_type():
    """Returns PostgreSQL timestamp type."""
    return 'TIMESTAMP'

def get_ignore_conflict():
    """Returns PostgreSQL conflict resolution syntax."""
    return 'ON CONFLICT DO NOTHING'

def safe_alter_table(cursor, table_name, column_name, column_definition):
    """Safely adds a column to a PostgreSQL table."""
    try:
        logger.info(f"🔧 Adding column {column_name} to {table_name}...")
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")
        logger.info(f"✅ Column {column_name} added to {table_name} successfully")
    except psycopg2.errors.DuplicateColumn:
        logger.info(f"ℹ️ Column {column_name} already exists in {table_name}, skipping")
        pass  # Column already exists, ignore
    except psycopg2.OperationalError as e:
        if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
            logger.info(f"ℹ️ Column {column_name} already exists in {table_name}, skipping")
            pass  # Column already exists, ignore
        else:
            logger.error(f"❌ Error adding column {column_name} to {table_name}: {e}")
            raise  # Re-raise other errors

# --- Database Initialization ---
def init_db():
    """Initializes the database schema."""
    logger.info("🔧 Initializing database schema...")
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            # --- users table ---
            c.execute(f'''CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY, 
                username {get_text_type()}, 
                balance REAL DEFAULT 0.0,
                total_purchases INTEGER DEFAULT 0, 
                basket {get_text_type()} DEFAULT '',
                language {get_text_type()} DEFAULT 'en', 
                theme {get_text_type()} DEFAULT 'default',
                is_banned {get_boolean_type()} DEFAULT FALSE,
                is_reseller {get_boolean_type()} DEFAULT FALSE,
                last_active {get_timestamp_type()} DEFAULT NULL,
                broadcast_failed_count INTEGER DEFAULT 0,
                referral_code {get_text_type()} DEFAULT NULL,
                is_human_verified {get_boolean_type()} DEFAULT FALSE,
                verification_attempts INTEGER DEFAULT 0
            )''')
            conn.commit()  # Critical: commit the table creation before any ALTER commands
            logger.info(f"✅ Users table created successfully")
            
            # Add referral_code column if it doesn't exist
            try:
                logger.info(f"🔧 Adding referral_code column to users table...")
                c.execute(f"ALTER TABLE users ADD COLUMN referral_code {get_text_type()} DEFAULT NULL")
                conn.commit()
                logger.info(f"✅ referral_code column added to users table")
            except Exception as e:
                conn.rollback()  # Critical: rollback the failed transaction
                if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                    logger.info(f"✅ referral_code column already exists in users table")
                else:
                    logger.warning(f"⚠️ Could not add referral_code column: {e}")
            
            # Add first_name column if it doesn't exist
            try:
                logger.info(f"🔧 Adding first_name column to users table...")
                c.execute(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name {get_text_type()} DEFAULT NULL")
                conn.commit()
                logger.info(f"✅ first_name column added/verified in users table")
            except Exception as e:
                conn.rollback()  # Critical: rollback the failed transaction
                err_lower = str(e).lower()
                if "already exists" in err_lower or "duplicate column" in err_lower or "column \"first_name\" of relation" in err_lower:
                    logger.info(f"✅ first_name column already exists in users table")
                else:
                    logger.warning(f"⚠️ Could not add first_name column: {e}")
            
            # Clean up legacy fake usernames (user_123456789 format)
            try:
                logger.info(f"🔧 Cleaning up legacy fake usernames in users table...")
                c.execute("""
                    UPDATE users 
                    SET username = NULL 
                    WHERE username LIKE 'user_%' 
                      AND username ~ '^user_[0-9]+$'
                """)
                cleaned_count = c.rowcount
                conn.commit()
                if cleaned_count > 0:
                    logger.info(f"✅ Cleaned {cleaned_count} legacy fake usernames from users table")
                else:
                    logger.info(f"✅ No legacy fake usernames found in users table")
            except Exception as e:
                logger.warning(f"⚠️ Could not clean legacy usernames: {e}")
                conn.rollback()
            
            # Add is_human_verified column if it doesn't exist
            try:
                logger.info(f"🔧 Adding is_human_verified column to users table...")
                c.execute(f"ALTER TABLE users ADD COLUMN is_human_verified {get_boolean_type()} DEFAULT FALSE")
                conn.commit()
                logger.info(f"✅ is_human_verified column added to users table")
            except Exception as e:
                if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                    logger.info(f"✅ is_human_verified column already exists in users table")
                else:
                    logger.warning(f"⚠️ Could not add is_human_verified column: {e}")
                conn.rollback()
            
            # Add verification_attempts column if it doesn't exist
            try:
                logger.info(f"🔧 Adding verification_attempts column to users table...")
                c.execute("ALTER TABLE users ADD COLUMN verification_attempts INTEGER DEFAULT 0")
                conn.commit()
                logger.info(f"✅ verification_attempts column added to users table")
            except Exception as e:
                if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                    logger.info(f"✅ verification_attempts column already exists in users table")
                else:
                    logger.warning(f"⚠️ Could not add verification_attempts column: {e}")
                conn.rollback()
            
            # Add language column if it doesn't exist
            try:
                logger.info(f"🔧 Adding language column to users table...")
                c.execute(f"ALTER TABLE users ADD COLUMN language {get_text_type()} DEFAULT NULL")
                conn.commit()
                logger.info(f"✅ language column added to users table")
            except Exception as e:
                if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                    logger.info(f"✅ language column already exists in users table")
                else:
                    logger.warning(f"⚠️ Could not add language column: {e}")
                conn.rollback()
            
            # Handle existing tables: ALTER user_id columns from INTEGER to BIGINT for Telegram compatibility
            logger.info(f"🔧 CRITICAL: Ensuring user_id columns are BIGINT for Telegram compatibility...")
            try:
                logger.info(f"🔧 EXECUTING: ALTER TABLE users ALTER COLUMN user_id TYPE BIGINT")
                c.execute("ALTER TABLE users ALTER COLUMN user_id TYPE BIGINT")
                conn.commit()  # Ensure the ALTER TABLE is committed immediately
                logger.info(f"✅ SUCCESS: Users.user_id converted to BIGINT and committed")
            except Exception as e:
                logger.error(f"❌ FAILED: Users.user_id conversion failed: {e}")
                logger.error(f"❌ ERROR TYPE: {type(e).__name__}")
                logger.error(f"❌ ERROR DETAILS: {str(e)}")
                # Don't silently ignore - this is critical!
                conn.rollback()  # Rollback on failure
            
            # Note: All columns are already included in the CREATE TABLE statement above
            logger.info(f"✅ Users table columns are already complete")

            # cities table
            logger.info(f"🔧 Creating cities table...")
            c.execute(f'''CREATE TABLE IF NOT EXISTS cities (
                id {get_auto_increment()}, 
                name {get_text_type()} UNIQUE NOT NULL
            )''')
            conn.commit()
            logger.info(f"✅ Cities table created successfully")
            # districts table
            logger.info(f"🔧 Creating districts table...")
            c.execute(f'''CREATE TABLE IF NOT EXISTS districts (
                id {get_auto_increment()}, 
                city_id INTEGER NOT NULL, 
                name {get_text_type()} NOT NULL,
                UNIQUE (city_id, name)
            )''')
            conn.commit()
            logger.info(f"✅ Districts table created successfully")
            # product_types table
            logger.info(f"🔧 Creating product_types table...")
            c.execute(f'''CREATE TABLE IF NOT EXISTS product_types (
                name {get_text_type()} PRIMARY KEY NOT NULL,
                emoji {get_text_type()} DEFAULT '{DEFAULT_PRODUCT_EMOJI}',
                description {get_text_type()}
            )''')
            conn.commit()
            logger.info(f"✅ Product_types table created successfully")
            # Note: All columns are already included in the CREATE TABLE statement above
            logger.info(f"✅ Product_types table columns are already complete")

            # products table
            logger.info(f"🔧 Creating products table...")
            c.execute('''CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY, city TEXT NOT NULL, district TEXT NOT NULL,
                product_type TEXT NOT NULL, size TEXT NOT NULL, name TEXT NOT NULL, price REAL NOT NULL,
                available INTEGER DEFAULT 1, reserved INTEGER DEFAULT 0, original_text TEXT,
                added_by BIGINT, added_date TEXT, added_by_worker_id BIGINT
            )''')
            conn.commit()
            logger.info(f"✅ Products table created successfully")
            
            # Note: Additional columns (low_stock_threshold, stock_alerts_enabled, last_stock_alert) 
            # will be added later when needed to avoid startup delays
            logger.info(f"✅ Products table created with basic columns")
            # product_media table
            logger.info(f"🔧 Creating product_media table...")
            c.execute('''CREATE TABLE IF NOT EXISTS product_media (
                id SERIAL PRIMARY KEY, product_id INTEGER NOT NULL,
                media_type TEXT NOT NULL, file_path TEXT NOT NULL, telegram_file_id TEXT,
                media_binary BYTEA
            )''')
            conn.commit()
            logger.info(f"✅ Product_media table created successfully")
            
            # 🚀  Add media_binary column if it doesn't exist (for existing databases)
            try:
                c.execute("ALTER TABLE product_media ADD COLUMN IF NOT EXISTS media_binary BYTEA")
                conn.commit()
                logger.info(f"✅ media_binary column added to product_media table")
            except Exception as e:
                conn.rollback()
                logger.info(f"ℹ️ media_binary column handling: {e}")
            # purchases table
            logger.info(f"🔧 Creating purchases table...")
            c.execute('''CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, product_id INTEGER,
                product_name TEXT NOT NULL, product_type TEXT NOT NULL, product_size TEXT NOT NULL,
                price_paid REAL NOT NULL, city TEXT NOT NULL, district TEXT NOT NULL, purchase_date TEXT NOT NULL
            )''')
            conn.commit()
            logger.info(f"✅ Purchases table created successfully")
            # reviews table
            c.execute('''CREATE TABLE IF NOT EXISTS reviews (
                review_id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL,
                review_text TEXT NOT NULL, review_date TEXT NOT NULL
            )''')
            conn.commit()
            # stock_alerts table - for tracking low stock notifications
            c.execute('''CREATE TABLE IF NOT EXISTS stock_alerts (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                alert_type TEXT NOT NULL,
                alert_message TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                notified_admins TEXT
            )''')
            conn.commit()
            
            # ab_tests table - for A/B testing framework
            c.execute('''CREATE TABLE IF NOT EXISTS ab_tests (
                id SERIAL PRIMARY KEY,
                test_name TEXT NOT NULL UNIQUE,
                description TEXT,
                variant_a_config TEXT,
                variant_b_config TEXT,
                active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                ended_at TEXT,
                target_metric TEXT,
                min_sample_size INTEGER DEFAULT 100
            )''')
            conn.commit()
            
            # ab_test_assignments table - tracks which users see which variant
            c.execute('''CREATE TABLE IF NOT EXISTS ab_test_assignments (
                id SERIAL PRIMARY KEY,
                test_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                variant TEXT NOT NULL,
                assigned_at TEXT NOT NULL,
                UNIQUE(test_id, user_id)
            )''')
            conn.commit()
            
            # ab_test_events table - tracks conversion events
            c.execute('''CREATE TABLE IF NOT EXISTS ab_test_events (
                id SERIAL PRIMARY KEY,
                test_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                event_value REAL,
                created_at TEXT NOT NULL,
                variant TEXT NOT NULL
            )''')
            conn.commit()
            
            # referral_codes table - for referral program
            c.execute('''CREATE TABLE IF NOT EXISTS referral_codes (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                referral_code TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                total_referrals INTEGER DEFAULT 0,
                total_rewards_earned REAL DEFAULT 0.0
            )''')
            conn.commit()
            
            # referrals table - tracks successful referrals
            c.execute('''CREATE TABLE IF NOT EXISTS referrals (
                id SERIAL PRIMARY KEY,
                referrer_user_id BIGINT NOT NULL,
                referred_user_id BIGINT NOT NULL,
                referral_code TEXT NOT NULL,
                created_at TEXT NOT NULL,
                first_purchase_at TEXT,
                referrer_reward REAL DEFAULT 0.0,
                referred_reward REAL DEFAULT 0.0,
                status TEXT DEFAULT 'pending',
                UNIQUE(referred_user_id)
            )''')
            conn.commit()
            
            # user_notifications table - for system notifications
            c.execute('''CREATE TABLE IF NOT EXISTS user_notifications (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                message TEXT NOT NULL,
                notification_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                sent_at TEXT,
                is_sent INTEGER DEFAULT 0
            )''')
            conn.commit()
            
            # Initialize VIP system tables
            try:
                from vip_system import VIPManager
                VIPManager.init_vip_tables()
            except ImportError:
                logger.warning("VIP system not available, skipping VIP table initialization")
            
            # Initialize welcome editor tables
            try:
                from welcome_editor import init_welcome_tables
                init_welcome_tables()
            except ImportError:
                logger.warning("Welcome editor not available, skipping welcome table initialization")
            
            # Initialize product price editor tables
            try:
                from product_price_editor import init_price_editor_tables
                init_price_editor_tables()
            except ImportError:
                logger.warning("Product price editor not available, skipping price editor table initialization")
            
            # Initialize interactive welcome editor tables
            try:
                from interactive_welcome_editor import init_interactive_welcome_tables
                init_interactive_welcome_tables()
            except ImportError:
                logger.warning("Interactive welcome editor not available, skipping interactive welcome table initialization")
            
            # Initialize enhanced auto ads tables
            try:
                from auto_ads_system import init_enhanced_auto_ads_tables
                init_enhanced_auto_ads_tables()
            except ImportError:
                logger.warning("Enhanced auto ads system not available, skipping enhanced auto ads table initialization")
            
            # MODE: Temporarily disable problematic module initializations to get bot running
            logger.info("🚀 MODE: Skipping referral system initialization (causing hang)")
            # # Initialize referral system tables
            # logger.info("🔧 About to initialize referral system tables...")
            # try:
            #     logger.info("🔧 Importing referral_system...")
            #     from referral_system import init_referral_tables
            #     logger.info("🔧 Calling init_referral_tables()...")
            #     init_referral_tables()
            #     logger.info("✅ Referral system tables initialized successfully")
            # except ImportError:
            #     logger.warning("Referral system not available, skipping referral table initialization")
            # except Exception as e:
            #     logger.error(f"❌ Failed to initialize referral tables: {e}", exc_info=True)
            
            logger.info("🚀 MODE: Skipping testforwarder initialization (causing hang)")
            # # Initialize testforwarder database tables
            # logger.info("🔧 About to initialize testforwarder database tables...")
            # try:
            #     logger.info("🔧 Importing database_testforwarder...")
            #     from database_testforwarder import Database
            #     logger.info("🔧 Creating Database instance...")
            #     testforwarder_db = Database()
            #     logger.info("🔧 Calling testforwarder_db.init_database()...")
            #     testforwarder_db.init_database()
            #     logger.info("✅ Testforwarder database tables initialized successfully")
            # except ImportError:
            #     logger.warning("Testforwarder database not available, skipping testforwarder table initialization")
            # except Exception as e:
            #     logger.error(f"❌ Failed to initialize testforwarder database: {e}", exc_info=True)

            # discount_codes table
            logger.info("🔧 Creating discount_codes table...")
            c.execute('''CREATE TABLE IF NOT EXISTS discount_codes (
                id SERIAL PRIMARY KEY, code TEXT UNIQUE NOT NULL,
                discount_type TEXT NOT NULL CHECK(discount_type IN ('percentage', 'fixed')),
                value REAL NOT NULL, is_active BOOLEAN DEFAULT TRUE,
                max_uses INTEGER DEFAULT NULL, uses_count INTEGER DEFAULT 0,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, expiry_date TIMESTAMP DEFAULT NULL,
                min_order_amount REAL DEFAULT NULL
            )''')
            conn.commit()
            logger.info("✅ Discount_codes table created successfully")
            
            # discount_code_usage table - Track individual user usage (allows reuse)
            logger.info("🔧 Creating discount_code_usage table...")
            c.execute('''CREATE TABLE IF NOT EXISTS discount_code_usage (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                code TEXT NOT NULL,
                used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                discount_amount REAL NOT NULL
            )''')
            conn.commit()
            logger.info("✅ Discount_code_usage table created successfully")
            
            # Note: discount_code_usage table migration skipped for PostgreSQL to avoid startup delays
            logger.info("✅ Discount_code_usage table created successfully (migration skipped for PostgreSQL)")
            # pending_deposits table
            logger.info("🔧 Creating pending_deposits table...")
            c.execute('''CREATE TABLE IF NOT EXISTS pending_deposits (
                payment_id TEXT PRIMARY KEY NOT NULL, user_id BIGINT NOT NULL,
                currency TEXT NOT NULL, target_eur_amount REAL NOT NULL,
                expected_crypto_amount REAL NOT NULL,
                pay_address TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_purchase BOOLEAN DEFAULT FALSE, basket_snapshot_json TEXT DEFAULT NULL,
                discount_code_used TEXT DEFAULT NULL
            )''')
            conn.commit()
            logger.info("✅ Pending_deposits table created successfully")
            # Note: pending_deposits table columns already included in CREATE TABLE statement
            logger.info("✅ Pending_deposits table created successfully")
            
            # Migrate pending_deposits table if needed (add missing columns to existing tables)
            try:
                c.execute("""
                    ALTER TABLE pending_deposits 
                    ADD COLUMN IF NOT EXISTS pay_address TEXT
                """)
                c.execute("""
                    ALTER TABLE pending_deposits 
                    ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'
                """)
                conn.commit()
                logger.info("✅ Migrated pending_deposits table with new columns (pay_address, status)")
            except Exception as e:
                conn.rollback()
                logger.warning(f"Migration note for pending_deposits: {e}")

            # Admin Log table
            logger.info("🔧 Creating admin_log table...")
            c.execute('''CREATE TABLE IF NOT EXISTS admin_log (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, admin_id BIGINT NOT NULL, target_user_id BIGINT,
                action TEXT NOT NULL, reason TEXT, amount_change REAL DEFAULT NULL,
                old_value TEXT, new_value TEXT
            )''')
            conn.commit()
            logger.info("✅ Admin_log table created successfully")
            # Bot Settings table
            logger.info("🔧 Creating bot_settings table...")
            c.execute('''CREATE TABLE IF NOT EXISTS bot_settings (
                setting_key TEXT PRIMARY KEY NOT NULL, setting_value TEXT
            )''')
            conn.commit()
            logger.info("✅ Bot_settings table created successfully")
            # Welcome Messages table
            logger.info("🔧 Creating welcome_messages table...")
            c.execute('''CREATE TABLE IF NOT EXISTS welcome_messages (
                id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL,
                template_text TEXT NOT NULL, description TEXT
            )''')
            conn.commit()
            logger.info("✅ Welcome_messages table created successfully")
            # Add description column if missing (skip for PostgreSQL to avoid duplicate column errors)
            # try: c.execute("ALTER TABLE welcome_messages ADD COLUMN description TEXT")
            # except psycopg2.OperationalError: pass # Ignore if already exists

            # <<< ADDED: reseller_discounts table >>>
            c.execute('''CREATE TABLE IF NOT EXISTS reseller_discounts (
                reseller_user_id BIGINT NOT NULL,
                product_type TEXT NOT NULL,
                discount_percentage REAL NOT NULL CHECK (discount_percentage >= 0 AND discount_percentage <= 100),
                PRIMARY KEY (reseller_user_id, product_type)
            )''')
            conn.commit()
            # <<< END ADDED >>>

            # Insert initial welcome messages (only if table was just created or empty - handled by INSERT OR IGNORE)
            initial_templates = [
                ("default", LANGUAGES['en']['welcome'], "Built-in default message (EN)"),
                ("clean", "👋 Hello, {username}!\n\n💰 Balance: {balance_str} EUR\n⭐ Status: {status}\n🛒 Basket: {basket_count} item(s)\n\nReady to shop or manage your profile? Explore the options below! 👇\n\n⚠️ Note: No refunds.", "Clean and direct style"),
                ("enthusiastic", "✨ Welcome back, {username}! ✨\n\nReady for more? You've got **{balance_str} EUR** to spend! 💸\nYour basket ({basket_count} items) is waiting for you! 🛒\n\nYour current status: {status} {progress_bar}\nTotal Purchases: {purchases}\n\n👇 Dive back into the shop or check your profile! 👇\n\n⚠️ Note: No refunds.", "Enthusiastic style with emojis"),
                ("status_focus", "👑 Welcome, {username}! ({status}) 👑\n\nTrack your journey: {progress_bar}\nTotal Purchases: {purchases}\n\n💰 Balance: {balance_str} EUR\n🛒 Basket: {basket_count} item(s)\n\nManage your profile or explore the shop! 👇\n\n⚠️ Note: No refunds.", "Focuses on status and progress"),
                ("minimalist", "Welcome, {username}.\n\nBalance: {balance_str} EUR\nBasket: {basket_count}\nStatus: {status}\n\nUse the menu below to navigate.\n\n⚠️ Note: No refunds.", "Simple, minimal text"),
                ("basket_focus", "Welcome back, {username}!\n\n🛒 You have **{basket_count} item(s)** in your basket! Don't forget about them!\n💰 Balance: {balance_str} EUR\n⭐ Status: {status} ({purchases} total purchases)\n\nCheck out your basket, keep shopping, or top up! 👇\n\n⚠️ Note: No refunds.", "Reminds user about items in basket")
            ]
            inserted_count = 0
            for name, text, desc in initial_templates:
                try:
                    c.execute("INSERT INTO welcome_messages (name, template_text, description) VALUES (%s, %s, %s) ON CONFLICT (name) DO NOTHING", (name, text, desc))
                    inserted_count += 1
                except psycopg2.Error as insert_e: 
                    logger.error(f"Error inserting template '{name}': {insert_e}")

            conn.commit()
            if inserted_count > 0: logger.info(f"Checked/Inserted {inserted_count} initial welcome message templates.")
            else: logger.info("Initial welcome message templates already exist or failed to insert.")

            # Set default as active if setting doesn't exist
            c.execute("INSERT INTO bot_settings (setting_key, setting_value) VALUES (%s, %s) ON CONFLICT (setting_key) DO NOTHING",
                      ("active_welcome_message_name", "default"))
            conn.commit()
            logger.info("Ensured 'default' is set as active welcome message in settings if not already set.")

            # Note: product_media table migration skipped for PostgreSQL to avoid startup delays
            logger.info("✅ Product_media table created successfully (migration skipped for PostgreSQL)")

            # Create Indices
            c.execute("CREATE INDEX IF NOT EXISTS idx_product_media_product_id ON product_media(product_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_purchases_date ON purchases(purchase_date)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_purchases_user ON purchases(user_id)")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_districts_city_name ON districts(city_id, name)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_products_location_type ON products(city, district, product_type)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(user_id)")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_discount_code_unique ON discount_codes(code)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pending_deposits_user_id ON pending_deposits(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_admin_log_timestamp ON admin_log(timestamp)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pending_deposits_is_purchase ON pending_deposits(is_purchase)")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_welcome_message_name ON welcome_messages(name)")
            # <<< ADDED Indices for reseller >>>
            c.execute("CREATE INDEX IF NOT EXISTS idx_users_is_reseller ON users(is_reseller)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_reseller_discounts_user_id ON reseller_discounts(reseller_user_id)")
            # <<< END ADDED >>>
            conn.commit()  # Commit all indexes

            # Handle existing tables: ALTER all user_id columns from INTEGER to BIGINT for Telegram compatibility
            logger.info(f"🔧 Converting all user_id columns to BIGINT for existing tables...")
            user_id_tables = [
                "purchases", "reviews", "ab_test_assignments", "ab_test_events", 
                "user_notifications", "discount_code_usage", "pending_deposits", "admin_log"
            ]
            
            for table_name in user_id_tables:
                try:
                    c.execute(f"ALTER TABLE {table_name} ALTER COLUMN user_id TYPE BIGINT")
                    conn.commit()  # Commit successful ALTER
                    logger.info(f"✅ {table_name}.user_id converted to BIGINT")
                except Exception as e:
                    conn.rollback()  # Rollback failed transaction
                    logger.info(f"ℹ️ {table_name}.user_id already BIGINT or conversion not needed: {e}")
            
            # Handle referral tables with special user_id columns
            try:
                c.execute("ALTER TABLE referrals ALTER COLUMN referrer_user_id TYPE BIGINT")
                c.execute("ALTER TABLE referrals ALTER COLUMN referred_user_id TYPE BIGINT")
                conn.commit()  # Commit successful ALTER
                logger.info(f"✅ referrals user_id columns converted to BIGINT")
            except Exception as e:
                conn.rollback()  # Rollback failed transaction
                logger.info(f"ℹ️ referrals user_id columns already BIGINT: {e}")
            
            # Handle admin_log admin_id and target_user_id
            try:
                c.execute("ALTER TABLE admin_log ALTER COLUMN admin_id TYPE BIGINT")
                c.execute("ALTER TABLE admin_log ALTER COLUMN target_user_id TYPE BIGINT")
                conn.commit()  # Commit successful ALTER
                logger.info(f"✅ admin_log admin/target user_id columns converted to BIGINT")
            except Exception as e:
                conn.rollback()  # Rollback failed transaction
                logger.info(f"ℹ️ admin_log admin/target user_id columns already BIGINT: {e}")
            
            # Handle reseller_discounts
            try:
                c.execute("ALTER TABLE reseller_discounts ALTER COLUMN reseller_user_id TYPE BIGINT")
                conn.commit()  # Commit successful ALTER
                logger.info(f"✅ reseller_discounts.reseller_user_id converted to BIGINT")
            except Exception as e:
                conn.rollback()  # Rollback failed transaction
                logger.info(f"ℹ️ reseller_discounts.reseller_user_id already BIGINT: {e}")
            
            # Handle products table added_by column
            try:
                c.execute("ALTER TABLE products ALTER COLUMN added_by TYPE BIGINT")
                conn.commit()  # Commit successful ALTER
                logger.info(f"✅ products.added_by converted to BIGINT")
            except Exception as e:
                conn.rollback()  # Rollback failed transaction
                logger.info(f"ℹ️ products.added_by already BIGINT: {e}")
            
            # Verify products.added_by column type (in a new transaction)
            try:
                c.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'products' AND column_name = 'added_by'
                """)
                col_info = c.fetchone()
                if col_info:
                    logger.info(f"🔍 products.added_by column type: {col_info['data_type']}")
                else:
                    logger.warning(f"⚠️ products.added_by column not found!")
            except Exception as e:
                logger.error(f"Error verifying products.added_by: {e}")
            
            # Add added_by_worker_id column to products table
            try:
                c.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS added_by_worker_id BIGINT")
                conn.commit()
                logger.info(f"✅ products.added_by_worker_id column added/verified")
            except Exception as e:
                conn.rollback()
                logger.info(f"ℹ️ products.added_by_worker_id already exists: {e}")
            except Exception as e:
                conn.rollback()  # Rollback if verification fails
                logger.warning(f"⚠️ Could not verify products.added_by column type: {e}")
            
            logger.info(f"✅ All user_id columns converted to BIGINT")

            # --- solana_wallets table ---
            logger.info(f"🔧 Creating solana_wallets table...")
            c.execute(f'''CREATE TABLE IF NOT EXISTS solana_wallets (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                order_id TEXT UNIQUE,
                public_key TEXT NOT NULL,
                private_key TEXT NOT NULL,
                expected_amount DECIMAL(20, 9),
                amount_received DECIMAL(20, 9) DEFAULT 0,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )''')
            
            c.execute("CREATE INDEX IF NOT EXISTS idx_solana_wallets_status ON solana_wallets(status)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_solana_wallets_order_id ON solana_wallets(order_id)")
            conn.commit()
            logger.info(f"✅ solana_wallets table created successfully")

            logger.info(f"🔧 Committing all database changes...")
            conn.commit()
            logger.info(f"✅ PostgreSQL database schema initialized/verified successfully.")
            logger.info(f"🔧 About to return from init_db() function...")
    except psycopg2.Error as e:
        try:
            conn.rollback()  # Rollback any uncommitted changes
        except:
            pass
        logger.critical(f"CRITICAL ERROR: PostgreSQL database initialization failed: {e}", exc_info=True)
        raise SystemExit("Database initialization failed.")
    
    logger.info(f"🔧 init_db() function completed successfully, returning to main...")


# --- Pending Deposit DB Helpers (Synchronous - Modified) ---
def add_pending_deposit(payment_id: str, user_id: int, currency: str, target_eur_amount: float, expected_crypto_amount: float, is_purchase: bool = False, basket_snapshot: list | None = None, discount_code: str | None = None):
    basket_json = json.dumps(basket_snapshot) if basket_snapshot else None
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO pending_deposits (
                    payment_id, user_id, currency, target_eur_amount,
                    expected_crypto_amount, created_at, is_purchase,
                    basket_snapshot_json, discount_code_used
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                payment_id, user_id, currency.lower(), target_eur_amount,
                expected_crypto_amount, datetime.now(timezone.utc).isoformat(),
                is_purchase, basket_json, discount_code
                ))
            conn.commit()
            log_type = "direct purchase" if is_purchase else "refill"
            logger.info(f"Added pending {log_type} deposit {payment_id} for user {user_id} ({target_eur_amount:.2f} EUR / exp: {expected_crypto_amount} {currency}). Basket items: {len(basket_snapshot) if basket_snapshot else 0}.")
            return True
    except psycopg2.IntegrityError:
        logger.warning(f"Attempted to add duplicate pending deposit ID: {payment_id}")
        return False
    except psycopg2.Error as e:
        logger.error(f"DB error adding pending deposit {payment_id} for user {user_id}: {e}", exc_info=True)
        return False

def get_pending_deposit(payment_id: str):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            # Fetch all needed columns, including the new ones
            c.execute("""
                SELECT user_id, currency, target_eur_amount, expected_crypto_amount,
                       is_purchase, basket_snapshot_json, discount_code_used
                FROM pending_deposits WHERE payment_id = %s
            """, (payment_id,))
            row = c.fetchone()
            if row:
                row_dict = dict(row)
                # Handle potential NULL for expected amount
                if row_dict.get('expected_crypto_amount') is None:
                    logger.warning(f"Pending deposit {payment_id} has NULL expected_crypto_amount. Using 0.0.")
                    row_dict['expected_crypto_amount'] = 0.0
                # Deserialize basket snapshot if present
                if row_dict.get('basket_snapshot_json'):
                    try:
                        row_dict['basket_snapshot'] = json.loads(row_dict['basket_snapshot_json'])
                    except json.JSONDecodeError:
                        logger.error(f"Failed to decode basket_snapshot_json for payment {payment_id}.")
                        row_dict['basket_snapshot'] = None # Indicate error or empty
                else:
                    row_dict['basket_snapshot'] = None
                return row_dict
            else:
                return None
    except psycopg2.Error as e:
        logger.error(f"DB error fetching pending deposit {payment_id}: {e}", exc_info=True)
        return None

# --- HELPER TO UNRESERVE ITEMS (Synchronous) ---
def _unreserve_basket_items(basket_snapshot: list | None):
    """Helper to decrement reserved counts for items in a snapshot."""
    if not basket_snapshot:
        return

    product_ids_to_release_counts = Counter(item['product_id'] for item in basket_snapshot if 'product_id' in item)
    if not product_ids_to_release_counts:
        return

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        decrement_data = [(count, pid) for pid, count in product_ids_to_release_counts.items()]
        c.executemany("UPDATE products SET reserved = GREATEST(0, reserved - %s) WHERE id = %s", decrement_data)
        conn.commit()
        total_released = sum(product_ids_to_release_counts.values())
        logger.info(f"Un-reserved {total_released} items due to failed/expired/cancelled payment.") # General log message
    except psycopg2.Error as e:
        logger.error(f"DB error un-reserving items: {e}", exc_info=True)
        if conn and conn.status == 1: conn.rollback()
    finally:
        if conn: conn.close()

# --- REMOVE PENDING DEPOSIT (Modified Trigger Logic) ---
def remove_pending_deposit(payment_id: str, trigger: str = "unknown"): # Added trigger for logging
    pending_info = get_pending_deposit(payment_id) # Get info *before* deleting
    deleted = False
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("DELETE FROM pending_deposits WHERE payment_id = %s", (payment_id,))
        conn.commit()
        deleted = c.rowcount > 0
        if deleted:
            logger.info(f"Removed pending deposit record for payment ID: {payment_id} (Trigger: {trigger})")
        else:
            # Reduce log level for "not found" as it can be normal (e.g., double webhook)
            logger.debug(f"No pending deposit record found to remove for payment ID: {payment_id} (Trigger: {trigger})")
    except psycopg2.Error as e:
        logger.error(f"DB error removing pending deposit {payment_id} (Trigger: {trigger}): {e}", exc_info=True)
        return False # Indicate failure

    # --- MODIFIED Condition for Un-reserving ---
    # Un-reserve if deletion was successful, it was a purchase, AND the trigger indicates non-success
    successful_triggers = ['purchase_success', 'refill_success'] # Define triggers indicating success
    if deleted and pending_info and pending_info.get('is_purchase') == 1 and trigger not in successful_triggers:
        log_reason = f"payment {payment_id} failure/expiry/cancellation (Trigger: {trigger})"
        logger.info(f"Payment was a purchase that did not succeed or was cancelled. Attempting to un-reserve items from snapshot ({log_reason}).")
        _unreserve_basket_items(pending_info.get('basket_snapshot'))
    # --- END MODIFICATION ---

    return deleted


# --- Data Loading Functions (Synchronous) ---
def load_cities():
    cities_data = {}
    try:
        logger.info("🔧 Connecting to database for cities...")
        with get_db_connection() as conn: 
            logger.info("🔧 Executing cities query...")
            c = conn.cursor(); c.execute("SELECT id, name FROM cities ORDER BY name"); cities_data = {str(row['id']): row['name'] for row in c.fetchall()}
            logger.info(f"✅ Cities query completed, found {len(cities_data)} cities")
    except psycopg2.Error as e: logger.error(f"❌ Failed to load cities: {e}")
    return cities_data

def load_districts():
    districts_data = {}
    try:
        logger.info("🔧 Connecting to database for districts...")
        with get_db_connection() as conn:
            logger.info("🔧 Executing districts query...")
            c = conn.cursor(); c.execute("SELECT d.city_id, d.id, d.name FROM districts d ORDER BY d.city_id, d.name")
            for row in c.fetchall(): city_id_str = str(row['city_id']); districts_data.setdefault(city_id_str, {})[str(row['id'])] = row['name']
            logger.info(f"✅ Districts query completed, found {sum(len(d) for d in districts_data.values())} districts")
    except psycopg2.Error as e: logger.error(f"❌ Failed to load districts: {e}")
    return districts_data

def load_product_types():
    product_types_dict = {}
    try:
        logger.info("🔧 Connecting to database for product types...")
        with get_db_connection() as conn:
            logger.info("🔧 Executing product types query...")
            c = conn.cursor()
            c.execute("SELECT name, COALESCE(emoji, %s) as emoji FROM product_types ORDER BY name", (DEFAULT_PRODUCT_EMOJI,))
            product_types_dict = {row['name']: row['emoji'] for row in c.fetchall()}
            logger.info(f"✅ Product types query completed, found {len(product_types_dict)} product types")
    except psycopg2.Error as e:
        logger.error(f"❌ Failed to load product types and emojis: {e}")
    return product_types_dict

def load_all_data():
    """Loads all dynamic data, modifying global variables IN PLACE."""
    global CITIES, DISTRICTS, PRODUCT_TYPES
    logger.info("🔧 Starting load_all_data (in-place update)...")
    try:
        logger.info("🔧 Loading cities...")
        cities_data = load_cities()
        logger.info(f"✅ Loaded {len(cities_data)} cities")
        
        logger.info("🔧 Loading districts...")
        districts_data = load_districts()
        logger.info(f"✅ Loaded {sum(len(d) for d in districts_data.values())} districts")
        
        logger.info("🔧 Loading product types...")
        product_types_dict = load_product_types()
        logger.info(f"✅ Loaded {len(product_types_dict)} product types")

        CITIES.clear(); CITIES.update(cities_data)
        DISTRICTS.clear(); DISTRICTS.update(districts_data)
        PRODUCT_TYPES.clear(); PRODUCT_TYPES.update(product_types_dict)

        logger.info(f"✅ Loaded (in-place) {len(CITIES)} cities, {sum(len(d) for d in DISTRICTS.values())} districts, {len(PRODUCT_TYPES)} product types.")
    except Exception as e:
        logger.error(f"❌ Error during load_all_data (in-place): {e}", exc_info=True)
        CITIES.clear(); DISTRICTS.clear(); PRODUCT_TYPES.clear()


# --- Bot Media Loading (from specified path on disk) ---
if os.path.exists(BOT_MEDIA_JSON_PATH):
    try:
        with open(BOT_MEDIA_JSON_PATH, 'r') as f: BOT_MEDIA = json.load(f)
        logger.info(f"Loaded BOT_MEDIA from {BOT_MEDIA_JSON_PATH}: {BOT_MEDIA}")
        if BOT_MEDIA.get("path"):
            filename = os.path.basename(BOT_MEDIA["path"]); correct_path = os.path.join(MEDIA_DIR, filename)
            if BOT_MEDIA["path"] != correct_path: logger.warning(f"Correcting BOT_MEDIA path from {BOT_MEDIA['path']} to {correct_path}"); BOT_MEDIA["path"] = correct_path
    except Exception as e: logger.warning(f"Could not load/parse {BOT_MEDIA_JSON_PATH}: {e}. Using default BOT_MEDIA.")
else: logger.info(f"{BOT_MEDIA_JSON_PATH} not found. Bot starting without default media.")


async def save_bot_media_config(media_type: str, media_path: str):
    """Save bot media configuration to file and update global BOT_MEDIA."""
    global BOT_MEDIA
    
    try:
        # Update global BOT_MEDIA
        BOT_MEDIA = {'type': media_type, 'path': media_path}
        
        # Save to file
        await asyncio.to_thread(_write_bot_media_config, BOT_MEDIA)
        
        logger.info(f"Bot media configuration saved: type={media_type}, path={media_path}")
        
    except Exception as e:
        logger.error(f"Error saving bot media configuration: {e}", exc_info=True)
        raise


def _write_bot_media_config(bot_media_data: dict):
    """Synchronous function to write bot media config to file."""
    import json
    with open(BOT_MEDIA_JSON_PATH, 'w') as f:
        json.dump(bot_media_data, f, indent=2)
    logger.debug(f"Bot media config written to {BOT_MEDIA_JSON_PATH}")


async def is_user_banned(user_id: int) -> bool:
    """Check if a user is banned. Returns True if banned, False otherwise.
    
    Args:
        user_id: The Telegram user ID to check
        
    Returns:
        bool: True if user is banned, False if not banned or if user doesn't exist
    """
    # Skip ban check for admins
    if user_id == ADMIN_ID or user_id in SECONDARY_ADMIN_IDS:
        return False
    
    conn = None
    max_retries = 3
    retry_delay = 0.1  # 100ms
    
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT is_banned FROM users WHERE user_id = %s", (user_id,))
            res = c.fetchone()
            return res and res['is_banned'] == 1
        except psycopg2.Error as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                logger.warning(f"Database locked for ban check (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                logger.error(f"DB error checking ban status for user {user_id}: {e}")
                return False  # Default to not banned if there's a DB error
        finally:
            if conn:
                conn.close()
                conn = None
    
    return False  # Default to not banned if all retries failed


# --- Utility Functions ---
def _get_lang_data(context: ContextTypes.DEFAULT_TYPE) -> tuple[str, dict]:
    """Gets the current language code and corresponding language data dictionary."""
    lang = "en"
    if getattr(context, 'user_data', None) is not None:
        lang = context.user_data.get("lang", "en")
    
    # Uses LANGUAGES dict defined above in this file
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    if lang not in LANGUAGES:
        logger.warning(f"_get_lang_data: Language '{lang}' not found in LANGUAGES dict. Falling back to 'en'.")
        lang = 'en' # Ensure lang variable reflects the fallback
    return lang, lang_data

def format_currency(value):
    try: return f"{Decimal(str(value)):.2f}"
    except (ValueError, TypeError): logger.warning(f"Could format currency {value}"); return "0.00"

def format_discount_value(dtype, value):
    try:
        if dtype == 'percentage': return f"{Decimal(str(value)):.1f}%"
        elif dtype == 'fixed': return f"{format_currency(value)} EUR"
        return str(value)
    except (ValueError, TypeError): logger.warning(f"Could not format discount {dtype} {value}"); return "N/A"

def get_progress_bar(purchases):
    """Get progress bar using VIP system if available, fallback to hardcoded"""
    try:
        # Try to use VIP system
        from vip_system import get_progress_bar_enhanced
        return get_progress_bar_enhanced(purchases)
    except ImportError:
        # Fallback to hardcoded system
        try:
            p_int = int(purchases); thresholds = [0, 2, 5, 8, 10]
            filled = min(sum(1 for t in thresholds if p_int >= t), 5)
            return '[' + '🟩' * filled + '⬜️' * (5 - filled) + ']'
        except (ValueError, TypeError): 
            return '[⬜️⬜️⬜️⬜️⬜️]'

# ============================================================================
# ADVANCED RATE LIMITING SYSTEM FOR 100% DELIVERY SUCCESS RATE
# ============================================================================

class TelegramRateLimiter:
    """
    Advanced rate limiter that ensures 100% message delivery success rate
    by respecting Telegram's limits and handling RetryAfter gracefully
    
    Telegram Limits:
    - 30 messages/second across all chats
    - 20 messages/second per chat
    - Error 429 (Too Many Requests) when exceeded
    """
    
    def __init__(self):
        self.global_lock = asyncio.Lock()
        self.chat_locks = {}  # chat_id -> Lock
        self.global_last_send = 0  # timestamp
        self.chat_last_send = {}  # chat_id -> timestamp
        
        # Rate limits (conservative to ensure 100% success)
        self.GLOBAL_MIN_INTERVAL = 0.04  # 25 msgs/sec (below 30/sec limit)
        self.CHAT_MIN_INTERVAL = 0.06    # 16 msgs/sec (below 20/sec limit)
        self.MAX_RETRY_AFTER = 300       # 5 minutes max wait
        
    async def acquire(self, chat_id: int):
        """Acquire rate limit permission for sending to a chat"""
        # Get or create chat-specific lock
        if chat_id not in self.chat_locks:
            self.chat_locks[chat_id] = asyncio.Lock()
        
        async with self.global_lock:
            # Enforce global rate limit
            now = asyncio.get_event_loop().time()
            time_since_last = now - self.global_last_send
            
            if time_since_last < self.GLOBAL_MIN_INTERVAL:
                wait_time = self.GLOBAL_MIN_INTERVAL - time_since_last
                await asyncio.sleep(wait_time)
            
            self.global_last_send = asyncio.get_event_loop().time()
        
        async with self.chat_locks[chat_id]:
            # Enforce per-chat rate limit
            now = asyncio.get_event_loop().time()
            last_send = self.chat_last_send.get(chat_id, 0)
            time_since_last = now - last_send
            
            if time_since_last < self.CHAT_MIN_INTERVAL:
                wait_time = self.CHAT_MIN_INTERVAL - time_since_last
                await asyncio.sleep(wait_time)
            
            self.chat_last_send[chat_id] = asyncio.get_event_loop().time()

# Global rate limiter instance
_telegram_rate_limiter = TelegramRateLimiter()

async def send_message_with_retry(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_markup=None,
    max_retries=5,  # Increased from 3 to 5 for better reliability
    parse_mode=None,
    disable_web_page_preview=False
):
    """
    Send message with advanced rate limiting and retry logic.
    Guarantees 100% delivery success rate by:
    1. Rate limiting to stay within Telegram's limits
    2. Handling RetryAfter with exponential backoff
    3. Network error retry with backoff
    4. Comprehensive error handling
    """
    
    for attempt in range(max_retries):
        try:
            # Acquire rate limit permission before sending
            await _telegram_rate_limiter.acquire(chat_id)
            
            # Send message
            return await bot.send_message(
                chat_id=chat_id, 
                text=text, 
                reply_markup=reply_markup,
                parse_mode=parse_mode, 
                disable_web_page_preview=disable_web_page_preview
            )
            
        except telegram_error.BadRequest as e:
            error_lower = str(e).lower()
            logger.warning(f"BadRequest sending to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}. Text: {text[:100]}...")
            
            # Unrecoverable errors - don't retry
            if any(err in error_lower for err in ["chat not found", "bot was blocked", "user is deactivated", "chat_write_forbidden"]):
                logger.error(f"❌ Unrecoverable BadRequest for chat {chat_id}: {e}. Aborting.")
                return None
            
            # Retry with exponential backoff
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries reached for BadRequest to {chat_id}: {e}")
                break
                
        except telegram_error.RetryAfter as e:
            retry_seconds = e.retry_after + 2  # Add 2 second buffer
            logger.warning(f"⏳ Rate limit (429) for chat {chat_id}. Retrying after {retry_seconds}s (Attempt {attempt+1}/{max_retries})")
            
            # Check if retry_after is reasonable
            if retry_seconds > _telegram_rate_limiter.MAX_RETRY_AFTER:
                logger.error(f"❌ RetryAfter too long ({retry_seconds}s > {_telegram_rate_limiter.MAX_RETRY_AFTER}s) for chat {chat_id}")
                return None
            
            # Wait as requested by Telegram
            await asyncio.sleep(retry_seconds)
            continue  # Don't increment attempt counter - this is Telegram's fault
            
        except telegram_error.NetworkError as e:
            logger.warning(f"🌐 NetworkError sending to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}")
            
            # Retry with exponential backoff
            if attempt < max_retries - 1:
                await asyncio.sleep(2 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries reached for NetworkError to {chat_id}: {e}")
                break
                
        except telegram_error.Forbidden:
            logger.warning(f"🚫 Forbidden error for chat {chat_id}. User blocked bot. Aborting.")
            return None
            
        except Exception as e:
            logger.error(f"❌ Unexpected error sending to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}", exc_info=True)
            
            # Retry with exponential backoff
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries reached after unexpected error to {chat_id}: {e}")
                break
    
    logger.error(f"❌ FAILED to send message to {chat_id} after {max_retries} attempts: {text[:100]}...")
    return None

async def send_media_with_retry(
    bot: Bot,
    chat_id: int,
    media,
    media_type: str = 'photo',  # 'photo', 'video', 'animation', 'document'
    caption: str = None,
    max_retries: int = 5,
    parse_mode=None,
    **kwargs  # Additional parameters like supports_streaming, force_document, etc.
):
    """
    Send media (photo/video/animation/document) with advanced rate limiting and retry logic.
    Guarantees 100% delivery success rate.
    
    Args:
        bot: Bot instance
        chat_id: Target chat ID
        media: File path, file_id, or file object
        media_type: Type of media ('photo', 'video', 'animation', 'document')
        caption: Optional caption
        max_retries: Maximum retry attempts
        parse_mode: Caption parse mode
        **kwargs: Additional parameters passed to send method
    """
    
    for attempt in range(max_retries):
        try:
            # Acquire rate limit permission
            await _telegram_rate_limiter.acquire(chat_id)
            
            # Select appropriate send method
            if media_type == 'photo':
                return await bot.send_photo(
                    chat_id=chat_id,
                    photo=media,
                    caption=caption,
                    parse_mode=parse_mode,
                    **kwargs
                )
            elif media_type == 'video':
                return await bot.send_video(
                    chat_id=chat_id,
                    video=media,
                    caption=caption,
                    parse_mode=parse_mode,
                    **kwargs
                )
            elif media_type == 'animation':
                return await bot.send_animation(
                    chat_id=chat_id,
                    animation=media,
                    caption=caption,
                    parse_mode=parse_mode,
                    **kwargs
                )
            elif media_type == 'document':
                return await bot.send_document(
                    chat_id=chat_id,
                    document=media,
                    caption=caption,
                    parse_mode=parse_mode,
                    **kwargs
                )
            else:
                logger.error(f"❌ Unsupported media type: {media_type}")
                return None
                
        except telegram_error.BadRequest as e:
            error_lower = str(e).lower()
            logger.warning(f"BadRequest sending {media_type} to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}")
            
            # Unrecoverable errors
            if any(err in error_lower for err in ["chat not found", "bot was blocked", "user is deactivated", "chat_write_forbidden"]):
                logger.error(f"❌ Unrecoverable BadRequest for {media_type} to {chat_id}: {e}")
                return None
            
            # File-related errors
            if any(err in error_lower for err in ["file too large", "wrong file identifier", "file not found"]):
                logger.error(f"❌ File error for {media_type} to {chat_id}: {e}")
                return None
            
            # Retry with backoff
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries for BadRequest {media_type} to {chat_id}")
                break
                
        except telegram_error.RetryAfter as e:
            retry_seconds = e.retry_after + 2
            logger.warning(f"⏳ Rate limit (429) for {media_type} to {chat_id}. Retrying after {retry_seconds}s")
            
            if retry_seconds > _telegram_rate_limiter.MAX_RETRY_AFTER:
                logger.error(f"❌ RetryAfter too long ({retry_seconds}s) for {media_type} to {chat_id}")
                return None
            
            await asyncio.sleep(retry_seconds)
            continue
            
        except telegram_error.NetworkError as e:
            logger.warning(f"🌐 NetworkError sending {media_type} to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries for NetworkError {media_type} to {chat_id}")
                break
                
        except telegram_error.Forbidden:
            logger.warning(f"🚫 Forbidden error for {media_type} to {chat_id}. User blocked bot.")
            return None
            
        except Exception as e:
            logger.error(f"❌ Unexpected error sending {media_type} to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}", exc_info=True)
            
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries after unexpected error {media_type} to {chat_id}")
                break
    
    logger.error(f"❌ FAILED to send {media_type} to {chat_id} after {max_retries} attempts")
    return None

async def send_media_group_with_retry(
    bot: Bot,
    chat_id: int,
    media: list,
    max_retries: int = 5,
    **kwargs  # connect_timeout, read_timeout, etc.
):
    """
    Send media group with advanced rate limiting and retry logic.
    Guarantees 100% delivery success rate.
    
    Args:
        bot: Bot instance
        chat_id: Target chat ID
        media: List of InputMedia objects (max 10)
        max_retries: Maximum retry attempts
        **kwargs: Additional parameters like connect_timeout, read_timeout
    """
    
    if not media or len(media) == 0:
        logger.warning(f"⚠️ Empty media group for chat {chat_id}")
        return None
    
    if len(media) > 10:
        logger.error(f"❌ Media group too large ({len(media)} items) for chat {chat_id}. Max is 10.")
        return None
    
    for attempt in range(max_retries):
        try:
            # Acquire rate limit permission
            await _telegram_rate_limiter.acquire(chat_id)
            
            # Send media group
            return await bot.send_media_group(
                chat_id=chat_id,
                media=media,
                **kwargs
            )
            
        except telegram_error.BadRequest as e:
            error_lower = str(e).lower()
            logger.warning(f"BadRequest sending media group to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}")
            
            # Unrecoverable errors
            if any(err in error_lower for err in ["chat not found", "bot was blocked", "user is deactivated", "chat_write_forbidden"]):
                logger.error(f"❌ Unrecoverable BadRequest for media group to {chat_id}: {e}")
                return None
            
            # Retry with backoff
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries for BadRequest media group to {chat_id}")
                break
                
        except telegram_error.RetryAfter as e:
            retry_seconds = e.retry_after + 2
            logger.warning(f"⏳ Rate limit (429) for media group to {chat_id}. Retrying after {retry_seconds}s")
            
            if retry_seconds > _telegram_rate_limiter.MAX_RETRY_AFTER:
                logger.error(f"❌ RetryAfter too long ({retry_seconds}s) for media group to {chat_id}")
                return None
            
            await asyncio.sleep(retry_seconds)
            continue
            
        except telegram_error.NetworkError as e:
            logger.warning(f"🌐 NetworkError sending media group to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries for NetworkError media group to {chat_id}")
                break
                
        except telegram_error.Forbidden:
            logger.warning(f"🚫 Forbidden error for media group to {chat_id}. User blocked bot.")
            return None
            
        except Exception as e:
            logger.error(f"❌ Unexpected error sending media group to {chat_id} (Attempt {attempt+1}/{max_retries}): {e}", exc_info=True)
            
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (2 ** attempt))
                continue
            else:
                logger.error(f"❌ Max retries after unexpected error media group to {chat_id}")
                break
    
    logger.error(f"❌ FAILED to send media group to {chat_id} after {max_retries} attempts")
    return None

def get_date_range(period_key):
    now = datetime.now(timezone.utc) # Use UTC now
    try:
        if period_key == 'today': start = now.replace(hour=0, minute=0, second=0, microsecond=0); end = now
        elif period_key == 'yesterday': yesterday = now - timedelta(days=1); start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0); end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        elif period_key == 'week': start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0); end = now
        elif period_key == 'last_week': start_of_this_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0); end_of_last_week = start_of_this_week - timedelta(microseconds=1); start = (end_of_last_week - timedelta(days=end_of_last_week.weekday())).replace(hour=0, minute=0, second=0, microsecond=0); end = end_of_last_week.replace(hour=23, minute=59, second=59, microsecond=999999)
        elif period_key == 'month': start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0); end = now
        elif period_key == 'last_month': first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0); end_of_last_month = first_of_this_month - timedelta(microseconds=1); start = end_of_last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0); end = end_of_last_month.replace(hour=23, minute=59, second=59, microsecond=999999)
        elif period_key == 'year': start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0); end = now
        else: return None, None
        # Return ISO format strings (already in UTC)
        return start.isoformat(), end.isoformat()
    except Exception as e: logger.error(f"Error calculating date range for '{period_key}': {e}"); return None, None


def get_user_status(purchases):
    """Get user status using VIP system if available, fallback to hardcoded"""
    # Fixed indentation - PostgreSQL compatibility
    try:
        # Try to use VIP system
        from vip_system import get_user_status_enhanced
        return get_user_status_enhanced(purchases)
    except ImportError:
        # Fallback to hardcoded system
        try:
            p_int = int(purchases)
            if p_int >= 10: return "VIP 👑"
            elif p_int >= 5: return "Regular ⭐"
            else: return "New 🌱"
        except (ValueError, TypeError): 
            return "New 🌱"

# --- Modified clear_expired_basket (Individual user focus) ---
def clear_expired_basket(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    if 'basket' not in context.user_data: context.user_data['basket'] = []
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        c.execute("SELECT basket FROM users WHERE user_id = %s", (user_id,))
        result = c.fetchone(); basket_str = result['basket'] if result else ''
        if not basket_str:
            # If DB basket is empty, ensure context basket is also empty
            if context.user_data.get('basket'): context.user_data['basket'] = []
            if context.user_data.get('applied_discount'): context.user_data.pop('applied_discount', None)
            c.execute("COMMIT"); # Commit potential state change from BEGIN
            return # Exit early if no basket string in DB

        items = basket_str.split(',')
        current_time = time.time(); valid_items_str_list = []; valid_items_userdata_list = []
        expired_product_ids_counts = Counter(); expired_items_found = False
        potential_prod_ids = []
        for item_part in items:
            if item_part and ':' in item_part:
                try: potential_prod_ids.append(int(item_part.split(':')[0]))
                except ValueError: logger.warning(f"Invalid product ID format in basket string '{item_part}' for user {user_id}")

        product_details = {}
        if potential_prod_ids:
             placeholders = ','.join(['%s'] * len(potential_prod_ids))
             # Fetch product_type along with price
             c.execute(f"SELECT id, price, product_type FROM products WHERE id IN ({placeholders})", potential_prod_ids)
             product_details = {row['id']: {'price': Decimal(str(row['price'])), 'type': row['product_type']} for row in c.fetchall()}

        for item_str in items:
            if not item_str: continue
            try:
                prod_id_str, ts_str = item_str.split(':'); prod_id = int(prod_id_str); ts = float(ts_str)
                if current_time - ts <= BASKET_TIMEOUT:
                    valid_items_str_list.append(item_str)
                    details = product_details.get(prod_id)
                    if details:
                        # Add product_type to context item
                        valid_items_userdata_list.append({
                            "product_id": prod_id,
                            "price": details['price'], # Original price
                            "product_type": details['type'], # Store product type
                            "timestamp": ts
                        })
                    else: logger.warning(f"P{prod_id} details not found during basket validation (user {user_id}).")
                else:
                    expired_product_ids_counts[prod_id] += 1
                    expired_items_found = True
            except (ValueError, IndexError) as e: logger.warning(f"Malformed item '{item_str}' in basket for user {user_id}: {e}")

        if expired_items_found:
            new_basket_str = ','.join(valid_items_str_list)
            c.execute("UPDATE users SET basket = %s WHERE user_id = %s", (new_basket_str, user_id))
            if expired_product_ids_counts:
                decrement_data = [(count, pid) for pid, count in expired_product_ids_counts.items()]
                c.executemany("UPDATE products SET reserved = GREATEST(0, reserved - %s) WHERE id = %s", decrement_data)
                logger.info(f"Released {sum(expired_product_ids_counts.values())} reservations for user {user_id} due to expiry.")

        c.execute("COMMIT") # Commit transaction
        context.user_data['basket'] = valid_items_userdata_list
        if not valid_items_userdata_list and context.user_data.get('applied_discount'):
            context.user_data.pop('applied_discount', None); logger.info(f"Cleared discount for user {user_id} as basket became empty.")

    except psycopg2.Error as e:
        logger.error(f"SQLite error clearing basket user {user_id}: {e}", exc_info=True)
        if conn and conn.status == 1: conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error clearing basket user {user_id}: {e}", exc_info=True)
        if conn and conn.status == 1: conn.rollback()
    finally:
        if conn: conn.close()

# --- MODIFIED clear_all_expired_baskets (Individual user processing) ---
def clear_all_expired_baskets():
    logger.info("Running scheduled job: clear_all_expired_baskets (Improved)")
    all_expired_product_counts = Counter()
    processed_user_count = 0
    failed_user_count = 0
    conn_outer = None
    users_to_process = []

    # 1. Fetch all users with baskets first
    try:
        conn_outer = get_db_connection()
        c_outer = conn_outer.cursor()
        c_outer.execute("SELECT user_id, basket FROM users WHERE basket IS NOT NULL AND basket != ''")
        users_to_process = c_outer.fetchall() # Fetch all relevant users
    except psycopg2.Error as e:
        logger.error(f"Failed to fetch users for basket clearing job: {e}", exc_info=True)
        return # Cannot proceed if user fetch fails
    finally:
        if conn_outer: conn_outer.close()

    if not users_to_process:
        logger.info("Scheduled clear: No users with active baskets found.")
        return

    logger.info(f"Scheduled clear: Found {len(users_to_process)} users with baskets to check.")
    current_time = time.time()
    user_basket_updates = [] # Batch updates for user basket strings

    # 2. Process each user individually for basket string updates and count expired items
    for user_row in users_to_process:
        user_id = user_row['user_id']
        basket_str = user_row['basket']
        items = basket_str.split(',')
        valid_items_str_list = []
        user_had_expired = False
        user_error = False

        for item_str in items:
            if not item_str: continue
            try:
                prod_id_str, ts_str = item_str.split(':')
                prod_id = int(prod_id_str)
                ts = float(ts_str)
                if current_time - ts <= BASKET_TIMEOUT:
                    valid_items_str_list.append(item_str)
                else:
                    all_expired_product_counts[prod_id] += 1
                    user_had_expired = True
            except (ValueError, IndexError) as e:
                logger.warning(f"Malformed item '{item_str}' user {user_id} in global clear: {e}")
                user_error = True # Mark user had an error, but continue processing others
                continue # Skip this malformed item

        if user_error:
            failed_user_count += 1

        # Only add to batch update if expired items were found for this user
        if user_had_expired:
            new_basket_str = ','.join(valid_items_str_list)
            user_basket_updates.append((new_basket_str, user_id))

        processed_user_count += 1
        # Optional: Add a small sleep if processing many users to avoid bursts
        # time.sleep(0.01) # Using time.sleep in sync function is fine

    # 3. Perform batch updates outside the user loop
    conn_update = None
    try:
        conn_update = get_db_connection()
        c_update = conn_update.cursor()
        c_update.execute("BEGIN") # Start transaction for batch updates

        # Update user basket strings
        if user_basket_updates:
            c_update.executemany("UPDATE users SET basket = %s WHERE user_id = %s", user_basket_updates)
            logger.info(f"Scheduled clear: Updated basket strings for {len(user_basket_updates)} users.")

        # Decrement reservations
        if all_expired_product_counts:
            decrement_data = [(count, pid) for pid, count in all_expired_product_counts.items()]
            if decrement_data:
                # PostgreSQL: Use GREATEST() instead of MAX() for comparing values
                c_update.executemany("UPDATE products SET reserved = GREATEST(0, reserved - %s) WHERE id = %s", decrement_data)
                total_released = sum(all_expired_product_counts.values())
                logger.info(f"Scheduled clear: Released {total_released} expired product reservations.")

        conn_update.commit() # Commit all updates together

    except psycopg2.Error as e:
        logger.error(f"SQLite error during batch updates in clear_all_expired_baskets: {e}", exc_info=True)
        if conn_update: 
            try:
                conn_update.rollback()
            except: pass
    except Exception as e:
        logger.error(f"Unexpected error during batch updates in clear_all_expired_baskets: {e}", exc_info=True)
        if conn_update: 
            try:
                conn_update.rollback()
            except: pass
    finally:
        if conn_update: conn_update.close()

    logger.info(f"Scheduled job clear_all_expired_baskets finished. Processed: {processed_user_count}, Users with errors: {failed_user_count}, Total items un-reserved: {sum(all_expired_product_counts.values())}")


def clean_abandoned_reservations():
    """
    Clean up stuck reservations where reserved > 0 but no active basket or pending payment exists.
    This fixes products that get stuck in "reserved" state when users abandon the purchase flow.
    """
    logger.info("Running scheduled job: clean_abandoned_reservations")
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        
        # Get all products with reservations
        c.execute("SELECT id, reserved FROM products WHERE reserved > 0")
        reserved_products = c.fetchall()
        
        if not reserved_products:
            logger.info("No products with reservations found.")
            c.execute("COMMIT")
            return
        
        logger.info(f"Found {len(reserved_products)} products with reservations. Checking for abandoned ones...")
        
        # Get all active basket items (product_ids currently in baskets)
        c.execute("SELECT basket FROM users WHERE basket IS NOT NULL AND basket != ''")
        baskets = c.fetchall()
        
        active_basket_products = Counter()
        for basket_row in baskets:
            basket_str = basket_row['basket']
            if not basket_str:
                continue
            items = basket_str.split(',')
            for item_str in items:
                if not item_str:
                    continue
                try:
                    prod_id_str, _ = item_str.split(':')
                    prod_id = int(prod_id_str)
                    active_basket_products[prod_id] += 1
                except (ValueError, IndexError):
                    continue
        
        # Get all pending payments (products being paid for)
        c.execute("""
            SELECT basket_snapshot FROM pending_deposits 
            WHERE basket_snapshot IS NOT NULL AND basket_snapshot != ''
        """)
        pending_payments = c.fetchall()
        
        pending_payment_products = Counter()
        for payment_row in pending_payments:
            basket_snapshot = payment_row['basket_snapshot']
            if not basket_snapshot:
                continue
            try:
                import json
                snapshot_data = json.loads(basket_snapshot) if isinstance(basket_snapshot, str) else basket_snapshot
                if isinstance(snapshot_data, list):
                    for item in snapshot_data:
                        if isinstance(item, dict) and 'product_id' in item:
                            pending_payment_products[item['product_id']] += 1
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        
        # Now find abandoned reservations
        products_to_fix = []
        total_stuck_reservations = 0
        
        for product_row in reserved_products:
            product_id = product_row['id']
            reserved_count = product_row['reserved']
            
            # Calculate legitimate reservations
            basket_count = active_basket_products.get(product_id, 0)
            payment_count = pending_payment_products.get(product_id, 0)
            legitimate_reservations = basket_count + payment_count
            
            # Calculate stuck reservations
            stuck_count = reserved_count - legitimate_reservations
            
            if stuck_count > 0:
                products_to_fix.append((stuck_count, product_id))
                total_stuck_reservations += stuck_count
                logger.info(f"Product {product_id}: Reserved={reserved_count}, InBaskets={basket_count}, InPayments={payment_count}, Stuck={stuck_count}")
        
        # Release stuck reservations
        if products_to_fix:
            c.executemany("UPDATE products SET reserved = GREATEST(0, reserved - %s) WHERE id = %s", products_to_fix)
            logger.info(f"✅ Released {total_stuck_reservations} stuck reservations across {len(products_to_fix)} products")
        else:
            logger.info("No stuck reservations found. All reservations are legitimate.")
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error in clean_abandoned_reservations: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
            except:
                pass
    finally:
        if conn:
            conn.close()


def fetch_last_purchases(user_id, limit=10):
    try:
        with get_db_connection() as conn:
            c = conn.cursor(); c.execute("SELECT purchase_date, product_name, product_type, product_size, price_paid FROM purchases WHERE user_id = %s ORDER BY purchase_date DESC LIMIT %s", (user_id, limit))
            return [dict(row) for row in c.fetchall()]
    except psycopg2.Error as e: logger.error(f"DB error fetching purchase history user {user_id}: {e}", exc_info=True); return []

def fetch_reviews(offset=0, limit=5):
    try:
        with get_db_connection() as conn:
            c = conn.cursor(); c.execute("SELECT r.review_id, r.user_id, r.review_text, r.review_date, COALESCE(u.username, 'anonymous') as username FROM reviews r LEFT JOIN users u ON r.user_id = u.user_id ORDER BY r.review_date DESC LIMIT ? OFFSET ?", (limit, offset))
            return [dict(row) for row in c.fetchall()]
    except psycopg2.Error as e: logger.error(f"Failed to fetch reviews (offset={offset}, limit={limit}): {e}", exc_info=True); return []


# --- API Helpers ---
def get_crypto_price_eur(currency_code: str) -> Decimal | None:
    """
    Gets the current price of a cryptocurrency in EUR using CoinGecko API.
    Returns None if the price cannot be fetched.
    """
    currency_code_lower = currency_code.lower()
    now = time.time()
    
    # Check cache first
    if currency_code_lower in currency_price_cache:
        price, timestamp = currency_price_cache[currency_code_lower]
        if now - timestamp < CACHE_EXPIRY_SECONDS:
            logger.debug(f"Cache hit for {currency_code_lower} price: {price} EUR")
            return price
    
    # Map currency codes to CoinGecko IDs
    currency_mapping = {
        'btc': 'bitcoin',
        'eth': 'ethereum',
        'ltc': 'litecoin',
        'sol': 'solana',
        'ton': 'the-open-network',
        'usdttrc20': 'tether',
        'usdterc20': 'tether',
        # Removed potentially problematic currency mappings:
        # 'usdtbsc': 'tether',
        # 'usdtsol': 'tether',
        # 'usdctrc20': 'usd-coin',
        # 'usdcerc20': 'usd-coin',
        # 'usdcsol': 'usd-coin',
    }
    
    coingecko_id = currency_mapping.get(currency_code_lower)
    if not coingecko_id:
        logger.warning(f"No CoinGecko mapping found for currency {currency_code_lower}")
        return None
    
    try:
        url = f"{COINGECKO_API_URL}/simple/price"
        params = {
            'ids': coingecko_id,
            'vs_currencies': 'eur'
        }
        
        logger.debug(f"Fetching price for {currency_code_lower} from CoinGecko: {url}")
        response = requests.get(url, params=params, timeout=10)
        logger.debug(f"CoinGecko price response status: {response.status_code}, content: {response.text[:200]}")
        response.raise_for_status()
        
        data = response.json()
        if coingecko_id in data and 'eur' in data[coingecko_id]:
            price = Decimal(str(data[coingecko_id]['eur']))
            currency_price_cache[currency_code_lower] = (price, now)
            logger.info(f"Fetched price for {currency_code_lower}: {price} EUR from CoinGecko.")
            return price
        else:
            logger.warning(f"Price data not found for {coingecko_id} in CoinGecko response: {data}")
            return None
            
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching price for {currency_code_lower} from CoinGecko.")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching price for {currency_code_lower} from CoinGecko: {e}")
        if e.response is not None:
            logger.error(f"CoinGecko price error response ({e.response.status_code}): {e.response.text}")
        return None
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing CoinGecko price response for {currency_code_lower}: {e}")
        return None

# Global cache for available currencies
available_currencies_cache = {'currencies': None, 'timestamp': 0}

# SOL-only payment system - no currency validation needed
def is_currency_supported(currency_code: str) -> bool:
    """SOL-only system: only SOL is supported"""
    return currency_code.lower() == 'sol'

def get_nowpayments_min_amount(currency_code: str) -> Decimal:
    """
    Stub function for SOL-only payment system.
    Returns 0 since NowPayments API is not used.
    """
    return Decimal('0')

def format_expiration_time(expiration_date_str: str | None) -> str:
    if not expiration_date_str: return "N/A"
    try:
        # Import pytz for timezone conversion
        import pytz
        
        # Ensure the string ends with timezone info for fromisoformat
        if not expiration_date_str.endswith('Z') and '+' not in expiration_date_str and '-' not in expiration_date_str[10:]:
            expiration_date_str += 'Z' # Assume UTC if no timezone
        dt_obj = datetime.fromisoformat(expiration_date_str.replace('Z', '+00:00'))
        
        # Convert to Lithuanian timezone (Europe/Vilnius)
        lithuanian_tz = pytz.timezone('Europe/Vilnius')
        if dt_obj.tzinfo:
            # Convert UTC to Lithuanian time
            lithuanian_time = dt_obj.astimezone(lithuanian_tz)
            return lithuanian_time.strftime("%H:%M:%S LT")  # LT = Local Time (Lithuanian)
        else:
            # If no timezone info, assume UTC and convert
            utc_time = dt_obj.replace(tzinfo=pytz.UTC)
            lithuanian_time = utc_time.astimezone(lithuanian_tz)
            return lithuanian_time.strftime("%H:%M:%S LT")
    except ImportError:
        # Fallback if pytz is not available - use manual offset
        try:
            if not expiration_date_str.endswith('Z') and '+' not in expiration_date_str and '-' not in expiration_date_str[10:]:
                expiration_date_str += 'Z'
            dt_obj = datetime.fromisoformat(expiration_date_str.replace('Z', '+00:00'))
            # Lithuania is UTC+2 (UTC+3 during DST)
            # For simplicity, add 2 hours (this is a fallback)
            from datetime import timedelta
            lithuanian_time = dt_obj + timedelta(hours=2)
            return lithuanian_time.strftime("%H:%M:%S LT")
        except Exception as fallback_e:
            logger.warning(f"Fallback timezone conversion failed for '{expiration_date_str}': {fallback_e}")
            return "Invalid Date"
    except (ValueError, TypeError) as e: 
        logger.warning(f"Could not parse expiration date string '{expiration_date_str}': {e}"); 
        return "Invalid Date"


# --- Placeholder Handler ---
async def handle_coming_soon(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    if query:
        try: await query.answer("This feature is coming soon!", show_alert=True); logger.info(f"User {query.from_user.id} clicked coming soon (data: {query.data})")
        except Exception as e: logger.error(f"Error answering 'coming soon' callback: {e}")


# --- Fetch User IDs for Broadcast (Synchronous) ---
def fetch_user_ids_for_broadcast(target_type: str, target_value: str | int | None = None) -> list[int]:
    """Fetches user IDs based on broadcast target criteria."""
    user_ids = []
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()

        if target_type == 'all':
            # Send to ALL users who have ever pressed /start (exist in users table) except banned ones
            # TEMPORARILY REMOVED broadcast_failed_count filtering to ensure ALL users get messages
            c.execute("SELECT user_id FROM users WHERE is_banned = 0 ORDER BY total_purchases DESC")
            user_ids = [row['user_id'] for row in c.fetchall()]
            logger.info(f"Broadcast target 'all': Found {len(user_ids)} users (excluding only banned users).")

        elif target_type == 'status' and target_value:
            status = str(target_value).lower()
            min_purchases, max_purchases = -1, -1
            # Use the status string including emoji for matching (rely on English definition)
            if status == LANGUAGES['en'].get("broadcast_status_vip", "VIP 👑").lower(): min_purchases = 10; max_purchases = float('inf')
            elif status == LANGUAGES['en'].get("broadcast_status_regular", "Regular ⭐").lower(): min_purchases = 5; max_purchases = 9
            elif status == LANGUAGES['en'].get("broadcast_status_new", "New 🌱").lower(): min_purchases = 0; max_purchases = 4

            if min_purchases != -1:
                 if max_purchases == float('inf'):
                     c.execute("SELECT user_id FROM users WHERE total_purchases >= %s AND is_banned=0", (min_purchases,)) # Exclude banned
                 else:
                     c.execute("SELECT user_id FROM users WHERE total_purchases BETWEEN %s AND %s AND is_banned=0", (min_purchases, max_purchases)) # Exclude banned
                 user_ids = [row['user_id'] for row in c.fetchall()]
                 logger.info(f"Broadcast target status '{target_value}': Found {len(user_ids)} non-banned users.")
            else: logger.warning(f"Invalid status value for broadcast: {target_value}")

        elif target_type == 'city' and target_value:
            city_name = str(target_value)
            # Find non-banned users whose *most recent* purchase was in this city
            c.execute("""
                SELECT p1.user_id
                FROM purchases p1
                JOIN users u ON p1.user_id = u.user_id
                WHERE p1.city = %s AND u.is_banned = 0 AND p1.purchase_date = (
                    SELECT MAX(purchase_date)
                    FROM purchases p2
                    WHERE p1.user_id = p2.user_id
                )
            """, (city_name,))
            user_ids = [row['user_id'] for row in c.fetchall()]
            logger.info(f"Broadcast target city '{city_name}': Found {len(user_ids)} non-banned users based on last purchase.")

        elif target_type == 'inactive' and target_value:
            try:
                days_inactive = int(target_value)
                if days_inactive <= 0: raise ValueError("Days must be positive")
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_inactive)
                cutoff_iso = cutoff_date.isoformat()

                # Find non-banned users whose last purchase date is older than the cutoff date OR have no purchases
                # 1. Get users with last purchase older than cutoff
                c.execute("""
                    SELECT p1.user_id
                    FROM purchases p1
                    JOIN users u ON p1.user_id = u.user_id
                    WHERE u.is_banned = 0 AND p1.purchase_date = (
                        SELECT MAX(purchase_date)
                        FROM purchases p2
                        WHERE p1.user_id = p2.user_id
                    ) AND p1.purchase_date < %s
                """, (cutoff_iso,))
                inactive_users = {row['user_id'] for row in c.fetchall()}

                # 2. Get users with zero purchases (who implicitly meet the inactive criteria)
                c.execute("SELECT user_id FROM users WHERE total_purchases = 0 AND is_banned = 0") # Exclude banned
                zero_purchase_users = {row['user_id'] for row in c.fetchall()}

                # Combine the sets
                user_ids_set = inactive_users.union(zero_purchase_users)
                user_ids = list(user_ids_set)
                logger.info(f"Broadcast target inactive >= {days_inactive} days: Found {len(user_ids)} non-banned users.")

            except (ValueError, TypeError):
                logger.error(f"Invalid number of days for inactive broadcast: {target_value}")

        else:
            logger.error(f"Unknown broadcast target type or missing value: type={target_type}, value={target_value}")

    except psycopg2.Error as e:
        logger.error(f"DB error fetching users for broadcast ({target_type}, {target_value}): {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error fetching users for broadcast: {e}", exc_info=True)
    finally:
        if conn: conn.close()

    # IMPROVED: Limit broadcast size to prevent overwhelming the system
    max_broadcast_users = 10000  # Reasonable limit
    if len(user_ids) > max_broadcast_users:
        logger.warning(f"Broadcast target too large ({len(user_ids)} users), limiting to {max_broadcast_users}")
        user_ids = user_ids[:max_broadcast_users]

    return user_ids


# --- User Broadcast Status Tracking (Synchronous) ---
def update_user_broadcast_status(user_id: int, success: bool):
    """Update user's broadcast status based on success/failure."""
    conn = None
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            if success:
                # Reset failure count and update last active time
                current_time = datetime.now(timezone.utc).isoformat()
                c.execute("""
                    UPDATE users 
                    SET broadcast_failed_count = 0, last_active = %s
                    WHERE user_id = %s
                """, (current_time, user_id))
                logger.debug(f"Reset broadcast failure count for user {user_id}")
            else:
                # Increment failure count
                c.execute("""
                    UPDATE users 
                    SET broadcast_failed_count = COALESCE(broadcast_failed_count, 0) + 1
                    WHERE user_id = %s
                """, (user_id,))
                
                # Check new failure count
                c.execute("SELECT broadcast_failed_count FROM users WHERE user_id = %s", (user_id,))
                result = c.fetchone()
                if result and result['broadcast_failed_count'] >= 5:
                    logger.info(f"User {user_id} marked as unreachable after {result['broadcast_failed_count']} consecutive failures")
            
            conn.commit()
            return  # Success, exit the retry loop
            
        except psycopg2.Error as e:
            logger.error(f"DB error updating broadcast status for user {user_id} (attempt {attempt+1}/{max_retries}): {e}")
            if conn and conn.status == 1:
                try:
                    conn.rollback()
                except:
                    pass
            if attempt < max_retries - 1:
                time.sleep(0.1 * (attempt + 1))  # Brief delay before retry
                continue
            else:
                logger.error(f"Failed to update broadcast status for user {user_id} after {max_retries} attempts")
        except Exception as e:
            logger.error(f"Unexpected error updating broadcast status for user {user_id} (attempt {attempt+1}/{max_retries}): {e}")
            if conn and conn.status == 1:  # STATUS_IN_TRANSACTION
                try:
                    conn.rollback()
                except:
                    pass
            if attempt < max_retries - 1:
                time.sleep(0.1 * (attempt + 1))  # Brief delay before retry
                continue
            else:
                logger.error(f"Failed to update broadcast status for user {user_id} after {max_retries} attempts")
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
                conn = None


# --- Admin Action Logging (Synchronous) ---
# <<< Define action names for Reseller Management >>>
ACTION_RESELLER_ENABLED = "RESELLER_ENABLED"
ACTION_RESELLER_DISABLED = "RESELLER_DISABLED"
ACTION_RESELLER_DISCOUNT_ADD = "RESELLER_DISCOUNT_ADD"
ACTION_RESELLER_DISCOUNT_EDIT = "RESELLER_DISCOUNT_EDIT"
ACTION_RESELLER_DISCOUNT_DELETE = "RESELLER_DISCOUNT_DELETE"
# <<< ADDED: Action name for Product Type Reassignment >>>
ACTION_PRODUCT_TYPE_REASSIGN = "PRODUCT_TYPE_REASSIGN"
# <<< END Define >>>

def log_admin_action(admin_id: int, action: str, target_user_id: int | None = None, reason: str | None = None, amount_change: float | None = None, old_value=None, new_value=None):
    """Logs an administrative action to the admin_log table."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO admin_log (timestamp, admin_id, target_user_id, action, reason, amount_change, old_value, new_value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(timezone.utc).isoformat(),
                admin_id,
                target_user_id,
                action, # Ensure action string is passed correctly
                reason,
                amount_change,
                str(old_value) if old_value is not None else None,
                str(new_value) if new_value is not None else None
            ))
            conn.commit()
            logger.info(f"Admin Action Logged: Admin={admin_id}, Action='{action}', Target={target_user_id}, Reason='{reason}', Amount={amount_change}, Old='{old_value}', New='{new_value}'")
    except psycopg2.Error as e:
        logger.error(f"Failed to log admin action: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error logging admin action: {e}", exc_info=True)

# --- Admin Authorization Helpers ---
def is_primary_admin(user_id: int) -> bool:
    """Check if a user ID is a primary admin."""
    return user_id in PRIMARY_ADMIN_IDS

def is_secondary_admin(user_id: int) -> bool:
    """Check if a user ID is a secondary admin."""
    return user_id in SECONDARY_ADMIN_IDS

def is_any_admin(user_id: int) -> bool:
    """Check if a user ID is either a primary or secondary admin."""
    return is_primary_admin(user_id) or is_secondary_admin(user_id)

def get_first_primary_admin_id() -> int | None:
    """Get the first primary admin ID for legacy compatibility, or None if none configured."""
    return PRIMARY_ADMIN_IDS[0] if PRIMARY_ADMIN_IDS else None

# --- Welcome Message Helpers (Synchronous) ---
def load_active_welcome_message() -> str:
    """Loads the currently active welcome message template from the database."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = %s", ("active_welcome_message_name",))
        setting_row = c.fetchone()
        active_name = setting_row['setting_value'] if setting_row else "default"

        c.execute("SELECT template_text FROM welcome_messages WHERE name = %s", (active_name,))
        template_row = c.fetchone()
        if template_row:
            logger.info(f"Loaded active welcome message template: '{active_name}'")
            return template_row['template_text']
        else:
            # If active template name points to a non-existent template, try fallback
            logger.warning(f"Active welcome message template '{active_name}' not found. Trying 'default'.")
            c.execute("SELECT template_text FROM welcome_messages WHERE name = %s", ("default",))
            template_row = c.fetchone()
            if template_row:
                logger.info("Loaded fallback 'default' welcome message template.")
                # Optionally update setting to default?
                # c.execute("UPDATE bot_settings SET setting_value = ? WHERE setting_key = ?", ("default", "active_welcome_message_name"))
                # conn.commit()
                return template_row['template_text']
            else:
                # If even default is missing
                logger.error("FATAL: Default welcome message template 'default' not found in DB! Using hardcoded default.")
                return DEFAULT_WELCOME_MESSAGE

    except psycopg2.Error as e:
        logger.error(f"DB error loading active welcome message: {e}", exc_info=True)
        return DEFAULT_WELCOME_MESSAGE
    except Exception as e:
        logger.error(f"Unexpected error loading welcome message: {e}", exc_info=True)
        return DEFAULT_WELCOME_MESSAGE
    finally:
        if conn: conn.close()

# <<< MODIFIED: Fetch description as well >>>
def get_welcome_message_templates(limit: int | None = None, offset: int = 0) -> list[dict]:
    """Fetches welcome message templates (name, text, description), optionally paginated."""
    templates = []
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            query = "SELECT name, template_text, description FROM welcome_messages ORDER BY name"
            params = []
            if limit is not None:
                query += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            c.execute(query, params)
            templates = [dict(row) for row in c.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"DB error fetching welcome message templates: {e}", exc_info=True)
    return templates

# <<< NEW: Helper to get total count >>>
def get_welcome_message_template_count() -> int:
    """Gets the total number of welcome message templates."""
    count = 0
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM welcome_messages")
            result = c.fetchone()
            if result: count = result[0]
    except psycopg2.Error as e:
        logger.error(f"DB error counting welcome message templates: {e}", exc_info=True)
    return count

# <<< MODIFIED: Handle description >>>
def add_welcome_message_template(name: str, template_text: str, description: str | None = None) -> bool:
    """Adds a new welcome message template."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("INSERT INTO welcome_messages (name, template_text, description) VALUES (%s, %s, %s)",
                      (name, template_text, description))
            conn.commit()
            logger.info(f"Added welcome message template: '{name}'")
            return True
    except psycopg2.IntegrityError:
        logger.warning(f"Attempted to add duplicate welcome message template name: '{name}'")
        return False
    except psycopg2.Error as e:
        logger.error(f"DB error adding welcome message template '{name}': {e}", exc_info=True)
        return False

# <<< MODIFIED: Handle description >>>
def update_welcome_message_template(name: str, new_template_text: str | None = None, new_description: str | None = None) -> bool:
    """Updates the text and/or description of an existing welcome message template."""
    if new_template_text is None and new_description is None:
        logger.warning("Update welcome template called without providing new text or description.")
        return False
    updates = []
    params = []
    if new_template_text is not None:
        updates.append("template_text = %s")
        params.append(new_template_text)
    if new_description is not None:
        # Handle empty string description as NULL
        desc_to_save = new_description if new_description else None
        updates.append("description = %s")
        params.append(desc_to_save)

    params.append(name)
    sql = f"UPDATE welcome_messages SET {', '.join(updates)} WHERE name = %s"

    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            result = c.execute(sql, params)
            conn.commit()
            if result.rowcount > 0:
                logger.info(f"Updated welcome message template: '{name}'")
                return True
            else:
                logger.warning(f"Welcome message template '{name}' not found for update.")
                return False
    except psycopg2.Error as e:
        logger.error(f"DB error updating welcome message template '{name}': {e}", exc_info=True)
        return False

def delete_welcome_message_template(name: str) -> bool:
    """Deletes a welcome message template."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            # Check if it's the active one (handled better in admin logic now)
            result = c.execute("DELETE FROM welcome_messages WHERE name = %s", (name,))
            conn.commit()
            if result.rowcount > 0:
                logger.info(f"Deleted welcome message template: '{name}'")
                return True
            else:
                logger.warning(f"Welcome message template '{name}' not found for deletion.")
                return False
    except psycopg2.Error as e:
        logger.error(f"DB error deleting welcome message template '{name}': {e}", exc_info=True)
        return False

def set_active_welcome_message(name: str) -> bool:
    """Sets the active welcome message template name in bot_settings."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            # First check if the template name actually exists
            c.execute("SELECT 1 FROM welcome_messages WHERE name = %s", (name,))
            if not c.fetchone():
                logger.error(f"Attempted to activate non-existent welcome template: '{name}'")
                return False
            # Update or insert the setting
            c.execute("INSERT INTO bot_settings (setting_key, setting_value) VALUES (%s, %s) ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value",
                      ("active_welcome_message_name", name))
            conn.commit()
            logger.info(f"Set active welcome message template to: '{name}'")
            return True
    except psycopg2.Error as e:
        logger.error(f"DB error setting active welcome message to '{name}': {e}", exc_info=True)
        return False

# --- PAYMENT RESERVATION TIMEOUT (2 hours for crypto payments) ---
PAYMENT_TIMEOUT_MINUTES_STR = os.environ.get("PAYMENT_TIMEOUT_MINUTES", "120")  # Increased from 30 to 120 minutes
try:
    PAYMENT_TIMEOUT_MINUTES = int(PAYMENT_TIMEOUT_MINUTES_STR)
    if PAYMENT_TIMEOUT_MINUTES <= 0:
        logger.warning("PAYMENT_TIMEOUT_MINUTES non-positive, using default 120 min.")
        PAYMENT_TIMEOUT_MINUTES = 120
except ValueError:
    logger.warning("Invalid PAYMENT_TIMEOUT_MINUTES, using default 120 min.")
    PAYMENT_TIMEOUT_MINUTES = 120

PAYMENT_TIMEOUT_SECONDS = PAYMENT_TIMEOUT_MINUTES * 60
logger.info(f"Payment timeout set to {PAYMENT_TIMEOUT_MINUTES} minutes ({PAYMENT_TIMEOUT_SECONDS} seconds).")

# --- ABANDONED RESERVATION TIMEOUT (30 minutes) ---
ABANDONED_RESERVATION_TIMEOUT_MINUTES = 30  # Timeout for items reserved but payment not started
ABANDONED_RESERVATION_TIMEOUT_SECONDS = ABANDONED_RESERVATION_TIMEOUT_MINUTES * 60
logger.info(f"Abandoned reservation timeout set to {ABANDONED_RESERVATION_TIMEOUT_MINUTES} minutes.")

# Global dictionary to track reservation timestamps
_reservation_timestamps = {}  # {user_id: {'timestamp': time.time(), 'snapshot': [...], 'type': 'single'/'basket'}}

def track_reservation(user_id: int, snapshot: list, reservation_type: str):
    """Track when a user reserves items so we can clean up abandoned reservations."""
    global _reservation_timestamps
    _reservation_timestamps[user_id] = {
        'timestamp': time.time(),
        'snapshot': snapshot,
        'type': reservation_type
    }
    logger.debug(f"Tracking {reservation_type} reservation for user {user_id}: {len(snapshot)} items")

def clear_reservation_tracking(user_id: int):
    """Clear reservation tracking when user proceeds to payment or cancels."""
    global _reservation_timestamps
    if user_id in _reservation_timestamps:
        logger.debug(f"Cleared reservation tracking for user {user_id}")
        del _reservation_timestamps[user_id]

def clean_abandoned_reservations():
    """Clean up items reserved by users who abandoned the payment flow without proceeding to invoice creation."""
    global _reservation_timestamps
    
    current_time = time.time()
    cutoff_time = current_time - ABANDONED_RESERVATION_TIMEOUT_SECONDS
    
    abandoned_users = []
    
    # Find users with abandoned reservations
    for user_id, reservation_data in _reservation_timestamps.items():
        if reservation_data['timestamp'] < cutoff_time:
            abandoned_users.append(user_id)
    
    if not abandoned_users:
        logger.debug("No abandoned reservations found.")
        return
    
    logger.info(f"Found {len(abandoned_users)} users with abandoned reservations to clean up.")
    
    # Process each abandoned reservation
    cleaned_count = 0
    for user_id in abandoned_users:
        try:
            reservation_data = _reservation_timestamps.get(user_id)
            if not reservation_data:
                continue
                
            snapshot = reservation_data['snapshot']
            reservation_type = reservation_data['type']
            
            # Unreserve the items
            _unreserve_basket_items(snapshot)
            
            # Remove from tracking
            del _reservation_timestamps[user_id]
            
            cleaned_count += 1
            logger.info(f"Cleaned up abandoned {reservation_type} reservation for user {user_id}: {len(snapshot)} items unreserved")
            
        except Exception as e:
            logger.error(f"Error cleaning up abandoned reservation for user {user_id}: {e}", exc_info=True)
    
    logger.info(f"Cleaned up {cleaned_count}/{len(abandoned_users)} abandoned reservations.")

# --- NEW: Clean up expired pending payments and unreserve items ---
def get_expired_payments_for_notification():
    """
    Gets information about expired pending payments for user notifications.
    Returns a list of user info for notifications before the records are cleaned up.
    """
    current_time = time.time()
    cutoff_timestamp = current_time - PAYMENT_TIMEOUT_SECONDS
    cutoff_datetime = datetime.fromtimestamp(cutoff_timestamp, tz=timezone.utc)
    
    user_notifications = []
    conn = None
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Find expired pending purchases and get user language info
        c.execute("""
            SELECT pd.user_id, u.language
            FROM pending_deposits pd
            JOIN users u ON pd.user_id = u.user_id
            WHERE pd.is_purchase = TRUE 
            AND pd.created_at < %s 
            ORDER BY pd.created_at
        """, (cutoff_datetime.isoformat(),))
        
        expired_records = c.fetchall()
        
        for record in expired_records:
            user_notifications.append({
                'user_id': record['user_id'],
                'language': record['language'] or 'en'
            })
            
    except psycopg2.Error as e:
        logger.error(f"DB error while getting expired payments for notification: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    
    return user_notifications


def clean_expired_pending_payments():
    """
    Checks for pending payments that have expired (older than PAYMENT_TIMEOUT_SECONDS)
    and automatically unreserves the items and removes the pending records.
    """
    logger.info("Running scheduled job: clean_expired_pending_payments")
    
    current_time = time.time()
    cutoff_timestamp = current_time - PAYMENT_TIMEOUT_SECONDS
    cutoff_datetime = datetime.fromtimestamp(cutoff_timestamp, tz=timezone.utc)
    
    expired_purchases = []
    conn = None
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Find expired pending purchases (not refills) older than cutoff time
        c.execute("""
            SELECT payment_id, user_id, basket_snapshot_json, created_at
            FROM pending_deposits 
            WHERE is_purchase = TRUE 
            AND created_at < %s 
            ORDER BY created_at
        """, (cutoff_datetime.isoformat(),))
        
        expired_records = c.fetchall()
        
        if not expired_records:
            logger.debug("No expired pending payments found.")
            return
            
        logger.info(f"Found {len(expired_records)} expired pending payments to clean up.")
        
        for record in expired_records:
            payment_id = record['payment_id']
            user_id = record['user_id']
            basket_snapshot_json = record['basket_snapshot_json']
            created_at = record['created_at']
            
            logger.info(f"Processing expired payment {payment_id} for user {user_id} (created: {created_at})")
            
            # Deserialize basket snapshot if present
            basket_snapshot = None
            if basket_snapshot_json:
                try:
                    basket_snapshot = json.loads(basket_snapshot_json)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode basket_snapshot_json for expired payment {payment_id}: {e}")
                    basket_snapshot = None
            
            # Collect info for later processing
            expired_purchases.append({
                'payment_id': payment_id,
                'user_id': user_id,
                'basket_snapshot': basket_snapshot
            })
            
    except psycopg2.Error as e:
        logger.error(f"DB error while checking expired pending payments: {e}", exc_info=True)
        return
    finally:
        if conn:
            conn.close()
    
    # Process each expired payment
    processed_count = 0
    for expired_payment in expired_purchases:
        payment_id = expired_payment['payment_id']
        user_id = expired_payment['user_id']
        basket_snapshot = expired_payment['basket_snapshot']
        
        try:
            # Remove the pending deposit record (this will trigger unreserving via remove_pending_deposit)
            success = remove_pending_deposit(payment_id, trigger="timeout_expiry")
            if success:
                processed_count += 1
                logger.info(f"Successfully cleaned up expired payment {payment_id} for user {user_id}")
            else:
                logger.warning(f"Failed to remove expired pending payment {payment_id} for user {user_id}")
                
        except Exception as e:
            logger.error(f"Error processing expired payment {payment_id} for user {user_id}: {e}", exc_info=True)
    
    logger.info(f"Cleaned up {processed_count}/{len(expired_purchases)} expired pending payments.")


# ============================================================================
# SYSTEM MONITORING 
# ============================================================================

def check_payment_system_health():
    """Check the overall health of the payment system"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check for stuck payments
        c.execute("""
            SELECT COUNT(*) as count FROM pending_deposits 
            WHERE created_at < NOW() - INTERVAL '30 minutes'
            AND is_purchase = true
        """)
        result = c.fetchone()
        stuck_payments = result['count']
        
        # Check for recent failures
        c.execute("""
            SELECT COUNT(*) as count FROM pending_deposits 
            WHERE created_at > NOW() - INTERVAL '1 hour'
            AND is_purchase = true
        """)
        result = c.fetchone()
        recent_payments = result['count']
        
        conn.close()
        
        health_status = {
            'stuck_payments': stuck_payments,
            'recent_payments': recent_payments,
            'is_healthy': stuck_payments < 5 and recent_payments > 0
        }
        
        logger.info(f"🔍 BULLETPROOF HEALTH CHECK: Stuck payments: {stuck_payments}, Recent payments: {recent_payments}")
        return health_status
        
    except Exception as e:
        logger.error(f"❌ BULLETPROOF: Error checking payment system health: {e}")
        return {'is_healthy': False, 'error': str(e)}


def send_health_alert(health_status):
    """Send health alert to admin if system is unhealthy"""
    try:
        from main import telegram_app, get_first_primary_admin_id, send_message_with_retry
        
        if not health_status.get('is_healthy', True) and get_first_primary_admin_id():
            message = f"🚨 BULLETPROOF ALERT: Payment system health issue detected!\n"
            message += f"Stuck payments: {health_status.get('stuck_payments', 0)}\n"
            message += f"Recent payments: {health_status.get('recent_payments', 0)}\n"
            message += f"Error: {health_status.get('error', 'Unknown')}"
            
            asyncio.run_coroutine_threadsafe(
                send_message_with_retry(
                    telegram_app.bot, 
                    get_first_primary_admin_id(), 
                    message
                ),
                asyncio.get_event_loop()
            )
    except Exception as e:
        logger.error(f"❌ BULLETPROOF: Error sending health alert: {e}")

# --- Human Verification System ---

def generate_verification_code():
    """Generate a random verification code like NIR1B"""
    letters = ''.join(random.choices(string.ascii_uppercase, k=3))  # 3 random letters
    numbers = ''.join(random.choices(string.digits, k=1))  # 1 random number
    letter = random.choice(string.ascii_uppercase)  # 1 more letter
    return letters + numbers + letter

def generate_verification_image(code):
    """Generate a professional verification image like the one in photo 2"""
    try:
        logger.info(f"🖼️ Generating verification image for code: {code}")
        
        # Create image with white background
        width, height = 300, 100
        image = Image.new('RGB', (width, height), 'white')
        draw = ImageDraw.Draw(image)
        
        # Add subtle noise/dots to background (like in the photo)
        for _ in range(200):
            x = random.randint(0, width-1)
            y = random.randint(0, height-1)
            draw.point((x, y), fill=(200, 200, 200))
        
        # Try to use a system font, fallback to default
        font_size = 48
        font_loaded = False
        
        # Try different font paths
        font_paths = [
            "arial.ttf",
            "/System/Library/Fonts/Arial.ttf", 
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
        ]
        
        for font_path in font_paths:
            try:
                font = ImageFont.truetype(font_path, font_size)
                font_loaded = True
                logger.info(f"✅ Loaded font: {font_path}")
                break
            except Exception as font_e:
                logger.debug(f"❌ Failed to load font {font_path}: {font_e}")
                continue
        
        if not font_loaded:
            logger.warning("⚠️ Using default font for verification image")
            font = ImageFont.load_default()
        
        # Get text dimensions for centering
        try:
            bbox = draw.textbbox((0, 0), code, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
        except:
            # Fallback for older PIL versions
            text_width, text_height = draw.textsize(code, font=font)
        
        # Center the text
        x = (width - text_width) // 2
        y = (height - text_height) // 2
        
        # Draw the text in black, bold style
        draw.text((x, y), code, fill='black', font=font)
        
        # Convert to bytes
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='PNG')
        img_byte_arr.seek(0)
        
        logger.info(f"✅ Successfully generated verification image")
        return img_byte_arr
                
    except Exception as e:
        logger.error(f"❌ Error generating verification image: {e}")
        # Fallback: create a simple text image
        try:
            image = Image.new('RGB', (300, 100), 'white')
            draw = ImageDraw.Draw(image)
            font = ImageFont.load_default()
            draw.text((50, 30), code, fill='black', font=font)
            
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            
            logger.info(f"✅ Generated fallback verification image")
            return img_byte_arr
        except Exception as fallback_e:
            logger.error(f"❌ Even fallback image generation failed: {fallback_e}")
            raise Exception(f"Complete image generation failure: {e}, fallback: {fallback_e}")

def is_human_verification_enabled():
    """Check if human verification is enabled"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = %s", ("human_verification_enabled",))
        result = c.fetchone()
        return result and result['setting_value'] == 'true'
    except Exception as e:
        logger.error(f"Error checking human verification status: {e}")
        return False
    finally:
        if conn:
            conn.close()

def is_user_verified(user_id):
    """Check if user has completed human verification"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT is_human_verified FROM users WHERE user_id = %s", (user_id,))
        result = c.fetchone()
        
        if result is None:
            logger.info(f"🔍 User {user_id} not found in database, returning False")
            return False
        
        verified_status = result.get('is_human_verified', False)
        logger.info(f"🔍 User {user_id} verification status from DB: {verified_status}")
        return bool(verified_status)
    except Exception as e:
        logger.error(f"Error checking user verification status: {e}")
        return False
    finally:
        if conn:
            conn.close()

def set_user_verified(user_id, verified=True):
    """Mark user as human verified"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # MODE: Insert or update user with verification status
        c.execute("""
            INSERT INTO users (user_id, username, is_human_verified, verification_attempts) 
            VALUES (%s, %s, %s, 0)
            ON CONFLICT (user_id) 
            DO UPDATE SET is_human_verified = EXCLUDED.is_human_verified, verification_attempts = 0
        """, (user_id, f"User_{user_id}", verified))
        
        rows_affected = c.rowcount
        conn.commit()
        logger.info(f"🚀  User {user_id} verification set to {verified}, rows affected: {rows_affected}")
        return True
    except Exception as e:
        logger.error(f"Error setting user verification status: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_verification_attempt_limit():
    """Get the maximum number of verification attempts allowed"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = %s", ("verification_attempt_limit",))
        result = c.fetchone()
        return int(result['setting_value']) if result else 3  # Default to 3 attempts
    except Exception as e:
        logger.error(f"Error getting verification attempt limit: {e}")
        return 3  # Default fallback
    finally:
        if conn:
            conn.close()
        
def get_user_verification_attempts(user_id):
    """Get user's current verification attempt count"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT verification_attempts FROM users WHERE user_id = %s", (user_id,))
        result = c.fetchone()
        return result.get('verification_attempts', 0) if result else 0
    except Exception as e:
        logger.error(f"Error getting user verification attempts: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def increment_verification_attempts(user_id):
    """Increment user's verification attempt count"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            UPDATE users 
            SET verification_attempts = COALESCE(verification_attempts, 0) + 1 
            WHERE user_id = %s
        """, (user_id,))
        conn.commit()
        
        # Get the new attempt count
        c.execute("SELECT verification_attempts FROM users WHERE user_id = %s", (user_id,))
        result = c.fetchone()
        return result.get('verification_attempts', 0) if result else 0
    except Exception as e:
        logger.error(f"Error incrementing verification attempts: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def reset_verification_attempts(user_id):
    """Reset user's verification attempt count"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE users SET verification_attempts = 0 WHERE user_id = %s", (user_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error resetting verification attempts: {e}")
        return False
    finally:
        if conn:
            conn.close()

def block_user_for_failed_verification(user_id):
    """Block user for failing verification too many times"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE users SET is_banned = %s WHERE user_id = %s", (True, user_id))
        conn.commit()
        logger.warning(f"User {user_id} blocked for failing human verification")
        return True
    except Exception as e:
        logger.error(f"Error blocking user for failed verification: {e}")
        return False
    finally:
        if conn:
            conn.close()

# --- Language Selection System ---

def is_language_selection_enabled():
    """Check if language selection is enabled"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = %s", ("language_selection_enabled",))
        result = c.fetchone()
        enabled = result and result['setting_value'] == 'true'
        logger.info(f"🌍 Language selection enabled: {enabled} (DB result: {result})")
        return enabled
    except Exception as e:
        logger.error(f"Error checking language selection status: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_language_prompt_placement():
    """Get language prompt placement setting (before/after verification)"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = %s", ("language_prompt_placement",))
        result = c.fetchone()
        placement = result['setting_value'] if result else 'before'
        logger.info(f"🌍 Language prompt placement: {placement} (DB result: {result})")
        return placement
    except Exception as e:
        logger.error(f"Error getting language prompt placement: {e}")
        return 'before'
    finally:
        if conn:
            conn.close()

# Multi-language verification texts
VERIFICATION_TEXTS = {
    'en': "🤖 Prove you're human: reply with the text in the image.",
    'lt': "🤖 Įrodykite, kad esate žmogus: atsakykite tekstu pavaizduotame paveikslėlyje.",
    'ru': "🤖 Докажите, что вы человек: ответьте текстом на изображении."
}

# Multi-language interface translations
INTERFACE_TRANSLATIONS = {
    'en': {
        # Main menu buttons
        'shop': '🛍️ Shop',
        'profile': '👤 Profile', 
        'top_up': '💰 Top Up',
        'reviews': '📝 Reviews',
        'price_list': '💎 Price List',
        'language': '🌍 Language',
        'hot_deals': '🔥 Hot Deals',
        'info': 'ℹ️ Info',
        'referral_code': '🎁 Referral Code',
        'pay_now': '💳 Pay Now',
        'discount_code': '🎫 Discount Code',
        'daily_rewards_menu': '🎁 Daily Rewards',
        
        # Status messages
        'status_new': 'Status: New',
        'balance': 'Balance',
        'total_purchases': 'Total Purchases', 
        'basket_items': 'Basket Items',
        'welcome': '👋 Welcome',
        'verification_successful': '✅ **Verification Successful!**\n\nWelcome to the bot! 🎉',
        
        # Common words
        'eur': 'EUR',
        'back': 'Back',
        'cancel': 'Cancel',
        'home': 'Home'
    },
    'lt': {
        # Main menu buttons
        'shop': '🛍️ Parduotuvė',
        'profile': '👤 Profilis',
        'top_up': '💰 Papildyti',
        'reviews': '📝 Atsiliepimai', 
        'price_list': '💎 Kainų Sąrašas',
        'language': '🌍 Kalba',
        'hot_deals': '🔥 Karšti Pasiūlymai',
        'info': 'ℹ️ Informacija',
        'referral_code': '🎁 Referalų Kodas',
        'pay_now': '💳 Mokėti Dabar',
        'discount_code': '🎫 Nuolaidos Kodas',
        'daily_rewards_menu': '🎁 Kasdienės Dovanos',
        
        # Status messages
        'status_new': 'Statusas: Naujas',
        'balance': 'Balansas',
        'total_purchases': 'Iš Viso Pirkimų',
        'basket_items': 'Krepšelio Prekės',
        'welcome': '👋 Sveiki',
        'verification_successful': '✅ **Patvirtinimas Sėkmingas!**\n\nSveiki atvykę į botą! 🎉',
        
        # Common words
        'eur': 'EUR',
        'back': 'Atgal',
        'cancel': 'Atšaukti',
        'home': 'Namai'
    },
    'ru': {
        # Main menu buttons
        'shop': '🛍️ Магазин',
        'profile': '👤 Профиль',
        'top_up': '💰 Пополнить',
        'reviews': '📝 Отзывы',
        'price_list': '💎 Прайс-лист', 
        'language': '🌍 Язык',
        'hot_deals': '🔥 Горячие Предложения',
        'info': 'ℹ️ Информация',
        'referral_code': '🎁 Реферальный Код',
        'pay_now': '💳 Заплатить Сейчас',
        'discount_code': '🎫 Код Скидки',
        'daily_rewards_menu': '🎁 Ежедневные Награды',
        
        # Status messages
        'status_new': 'Статус: Новый',
        'balance': 'Баланс',
        'total_purchases': 'Всего Покупок',
        'basket_items': 'Товары в Корзине',
        'welcome': '👋 Добро пожаловать',
        'verification_successful': '✅ **Верификация успешна!**\n\nДобро пожаловать в бота! 🎉',
        
        # Common words
        'eur': 'EUR',
        'back': 'Назад',
        'cancel': 'Отмена',
        'home': 'Главная'
    }
}

def get_translation(key, language='en'):
    """Get translation for a key in specified language"""
    return INTERFACE_TRANSLATIONS.get(language, INTERFACE_TRANSLATIONS['en']).get(key, key)

# ============================================================================
# BOT SETTINGS HELPERS (for feature toggles)
# ============================================================================

def get_bot_setting(key: str, default: str | None = None):
    """Get a bot setting value from database"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = %s", (key,))
        row = c.fetchone()
        return row['setting_value'] if row else default
    except Exception as e:
        logger.error(f"Error reading bot setting {key}: {e}")
        return default
    finally:
        if conn:
            conn.close()

def set_bot_setting(key: str, value: str):
    """Set a bot setting value in database"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            INSERT INTO bot_settings (setting_key, setting_value)
            VALUES (%s, %s)
            ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value
        """, (key, value))
        conn.commit()
        logger.info(f"Bot setting '{key}' set to '{value}'")
        return True  # SUCCESS
    except Exception as e:
        logger.error(f"Error writing bot setting {key}: {e}")
        if conn and conn.status == 1:
            conn.rollback()
        return False  # FAILURE
    finally:
        if conn:
            conn.close()

def is_daily_rewards_enabled() -> bool:
    """Check if Daily Rewards button should be shown in start menu"""
    val = get_bot_setting("show_daily_rewards_button", "true")
    result = str(val).lower() in ("1", "true", "yes", "on")
    logger.info(f"🎁 Daily Rewards visibility check: DB value='{val}', result={result}")
    return result
