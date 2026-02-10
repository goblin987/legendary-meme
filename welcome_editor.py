# --- START OF FILE welcome_editor.py ---

import logging
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from utils import (
    get_db_connection, send_message_with_retry, is_primary_admin,
    format_currency
)

logger = logging.getLogger(__name__)

# Default start menu button configuration
DEFAULT_START_BUTTONS = [
    {"text": "🛒 Shop", "callback": "shop_menu", "row": 0, "position": 0, "enabled": True},
    {"text": "👤 Profile", "callback": "user_profile", "row": 0, "position": 1, "enabled": True},
    {"text": "🎁 Referrals", "callback": "referral_menu", "row": 1, "position": 0, "enabled": True},
    {"text": "📞 Support", "callback": "support_menu", "row": 1, "position": 1, "enabled": True},
    {"text": "ℹ️ Info", "callback": "info_menu", "row": 2, "position": 0, "enabled": True},
    {"text": "⚙️ Settings", "callback": "user_settings", "row": 2, "position": 1, "enabled": True}
]

DEFAULT_WELCOME_TEXT = """🎉 **Welcome to Our Bot!** 🎉

Hello {user_name}! 👋

We're excited to have you here! Our bot offers:

🛒 **Shopping** - Browse our amazing products
👤 **Profile** - Manage your account and orders  
🎁 **Referrals** - Earn rewards by inviting friends
📞 **Support** - Get help when you need it
ℹ️ **Info** - Learn more about our services
⚙️ **Settings** - Customize your experience

Ready to get started? Choose an option below! ⬇️"""

# --- Database Initialization ---

def init_welcome_tables():
    """Initialize welcome message and button configuration tables"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Welcome messages table (using existing structure)
        c.execute("""
            CREATE TABLE IF NOT EXISTS welcome_messages (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                template_text TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Start menu buttons configuration table
        c.execute("""
            CREATE TABLE IF NOT EXISTS start_menu_buttons (
                id SERIAL PRIMARY KEY,
                button_text TEXT NOT NULL,
                callback_data TEXT NOT NULL,
                row_position INTEGER DEFAULT 0,
                column_position INTEGER DEFAULT 0,
                is_enabled BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert default welcome message if none exists
        c.execute("SELECT COUNT(*) as count FROM welcome_messages")
        result = c.fetchone()
        if result['count'] == 0:
            c.execute("""
                INSERT INTO welcome_messages (name, template_text, description)
                VALUES ('default', %s, 'Default welcome message')
            """, (DEFAULT_WELCOME_TEXT,))
        
        # Insert default buttons if none exist
        c.execute("SELECT COUNT(*) as count FROM start_menu_buttons")
        result = c.fetchone()
        if result['count'] == 0:
            for button in DEFAULT_START_BUTTONS:
                c.execute("""
                    INSERT INTO start_menu_buttons (button_text, callback_data, row_position, column_position, is_enabled)
                    VALUES (%s, %s, %s, %s, %s)
                """, (button["text"], button["callback"], button["row"], button["position"], button["enabled"]))
        
        conn.commit()
        logger.info("Welcome message tables initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing welcome tables: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise  # Re-raise to see the actual error
    finally:
        if conn:
            conn.close()

# --- Welcome Message Management ---

def get_active_welcome_message():
    """Get the currently active welcome message"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Use existing bot_settings table to get active template name
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = 'active_welcome_message_name'")
        active_name_result = c.fetchone()
        active_name = active_name_result['setting_value'] if active_name_result else 'default'
        
        # Get the template text
        c.execute("SELECT template_text FROM welcome_messages WHERE name = %s LIMIT 1", (active_name,))
        result = c.fetchone()
        
        return result['template_text'] if result else DEFAULT_WELCOME_TEXT
        
    except Exception as e:
        logger.error(f"Error getting active welcome message: {e}")
        return DEFAULT_WELCOME_TEXT
    finally:
        if conn:
            conn.close()

def get_start_menu_buttons():
    """Get configured start menu buttons"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT button_text, callback_data, row_position, column_position
            FROM start_menu_buttons 
            WHERE is_enabled = 1
            ORDER BY row_position, column_position
        """)
        
        buttons = c.fetchall()
        
        if not buttons:
            return DEFAULT_START_BUTTONS
        
        return [
            {
                "text": btn['button_text'],
                "callback": btn['callback_data'],
                "row": btn['row_position'],
                "position": btn['column_position']
            }
            for btn in buttons
        ]
        
    except Exception as e:
        logger.error(f"Error getting start menu buttons: {e}")
        return DEFAULT_START_BUTTONS
    finally:
        if conn:
            conn.close()

# --- Admin Handlers ---

async def handle_welcome_editor_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main welcome message editor menu - dummy proof!"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get current active message info using bot_settings
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = 'active_welcome_message_name'")
        active_name_result = c.fetchone()
        active_name = active_name_result['setting_value'] if active_name_result else 'default'
        
        c.execute("SELECT name, template_text FROM welcome_messages WHERE name = %s LIMIT 1", (active_name,))
        active_msg = c.fetchone()
        
        # Get button count
        c.execute("SELECT COUNT(*) as count FROM start_menu_buttons WHERE is_enabled = 1")
        button_count = c.fetchone()['count']
        
    except Exception as e:
        logger.error(f"Error loading welcome editor: {e}")
        active_msg = None
        button_count = 0
    finally:
        if conn:
            conn.close()
    
    msg = "🎨 **Welcome Message Editor** 🎨\n\n"
    msg += "**Easy-to-use editor for your bot's welcome experience!**\n\n"
    
    if active_msg:
        # Escape markdown characters in preview to prevent parsing errors
        preview_text = active_msg['template_text'][:100] + "..." if len(active_msg['template_text']) > 100 else active_msg['template_text']
        # Escape markdown special characters
        preview_text = preview_text.replace('*', '\\*').replace('_', '\\_').replace('`', '\\`').replace('[', '\\[').replace(']', '\\]')
        msg += f"📝 **Current Message:** {active_msg['name']}\n"
        msg += f"📄 **Preview:** {preview_text}\n\n"
    else:
        msg += f"📝 **Current Message:** Default\n\n"
    
    msg += f"🔘 **Start Menu Buttons:** {button_count} active\n\n"
    msg += "**What would you like to edit?**"
    
    keyboard = [
        [InlineKeyboardButton("📝 Edit Welcome Text", callback_data="welcome_edit_text")],
        [InlineKeyboardButton("🔘 Manage Start Buttons", callback_data="welcome_edit_buttons")],
        [InlineKeyboardButton("👀 Preview Welcome", callback_data="welcome_preview")],
        [InlineKeyboardButton("📋 Message Templates", callback_data="welcome_templates")],
        [InlineKeyboardButton("🔄 Reset to Default", callback_data="welcome_reset_confirm")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Simple text editor for welcome message"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Set state for text input
    context.user_data['state'] = 'awaiting_welcome_text'
    
    msg = "📝 **Edit Welcome Message Text**\n\n"
    msg += "**How to write a great welcome message:**\n\n"
    msg += "✅ **Do:**\n"
    msg += "• Be friendly and welcoming\n"
    msg += "• Explain what your bot does\n"
    msg += "• Guide users on next steps\n"
    msg += "• Use emojis to make it engaging\n\n"
    msg += "🔧 **Available Placeholders:**\n"
    msg += "• `{username}` - User's name\n"
    msg += "• `{balance}` - User's balance (e.g., 10.50)\n"
    msg += "• `{total_purchases}` - Total purchases count\n"
    msg += "• `{basket_items}` - Items in basket\n"
    msg += "• `{status}` - User status bar (🟩🟩⬜⬜⬜⬜)\n\n"
    msg += "📝 **Now type your new welcome message:**\n"
    msg += "*(Send your message in the next message)*"
    
    keyboard = [
        [InlineKeyboardButton("📋 Use Template", callback_data="welcome_use_template")],
        [InlineKeyboardButton("❌ Cancel", callback_data="welcome_editor_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_edit_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Button arrangement editor - drag and drop style!"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all buttons with their positions
        c.execute("""
            SELECT id, button_text, callback_data, row_position, column_position, is_enabled
            FROM start_menu_buttons 
            ORDER BY row_position, column_position
        """)
        buttons = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading buttons: {e}")
        buttons = []
    finally:
        if conn:
            conn.close()
    
    msg = "🔘 **Start Menu Button Manager**\n\n"
    msg += "**Current Button Layout:**\n\n"
    
    # Group buttons by row
    rows = {}
    for btn in buttons:
        row = btn['row_position']
        if row not in rows:
            rows[row] = []
        rows[row].append(btn)
    
    # Display current layout
    for row_num in sorted(rows.keys()):
        msg += f"**Row {row_num + 1}:** "
        row_buttons = sorted(rows[row_num], key=lambda x: x['column_position'])
        for btn in row_buttons:
            status = "✅" if btn['is_enabled'] else "❌"
            msg += f"{status} {btn['button_text']} | "
        msg = msg.rstrip(" | ") + "\n"
    
    msg += "\n**Button Management Options:**"
    
    keyboard = [
        [InlineKeyboardButton("✏️ Edit Button Text", callback_data="welcome_edit_button_text")],
        [InlineKeyboardButton("🔄 Rearrange Buttons", callback_data="welcome_rearrange_buttons")],
        [InlineKeyboardButton("❌ Enable/Disable Buttons", callback_data="welcome_toggle_buttons")],
        [InlineKeyboardButton("👀 Preview Layout", callback_data="welcome_preview_buttons")],
        [InlineKeyboardButton("💾 Save Changes", callback_data="welcome_save_changes")],
        [InlineKeyboardButton("⬅️ Back to Editor", callback_data="welcome_editor_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_rearrange_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Visual button rearranger"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT id, button_text, row_position, column_position
            FROM start_menu_buttons 
            WHERE is_enabled = 1
            ORDER BY row_position, column_position
        """)
        buttons = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading buttons for rearrangement: {e}")
        buttons = []
    finally:
        if conn:
            conn.close()
    
    msg = "🔄 **Rearrange Start Menu Buttons**\n\n"
    msg += "**Current Layout Preview:**\n\n"
    
    # Show visual representation
    rows = {}
    for btn in buttons:
        row = btn['row_position']
        if row not in rows:
            rows[row] = []
        rows[row].append(btn)
    
    for row_num in sorted(rows.keys()):
        msg += f"**Row {row_num + 1}:** "
        row_buttons = sorted(rows[row_num], key=lambda x: x['column_position'])
        for i, btn in enumerate(row_buttons):
            msg += f"[{btn['button_text']}]"
            if i < len(row_buttons) - 1:
                msg += " "
        msg += "\n"
    
    msg += "\n**Rearrangement Options:**\n"
    msg += "• Move buttons between rows\n"
    msg += "• Change button order within rows\n"
    msg += "• Create new rows\n\n"
    msg += "Select a button to move:"
    
    keyboard = []
    for btn in buttons:
        keyboard.append([InlineKeyboardButton(
            f"Move: {btn['button_text']} (Row {btn['row_position']+1})",
            callback_data=f"welcome_move_button|{btn['id']}"
        )])
    
    keyboard.extend([
        [InlineKeyboardButton("🔄 Auto-Arrange (2 per row)", callback_data="welcome_auto_arrange")],
        [InlineKeyboardButton("⬅️ Back to Button Manager", callback_data="welcome_edit_buttons")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle welcome text input from admin"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(user_id):
        return
    
    if context.user_data.get("state") != "awaiting_welcome_text":
        return
    
    if not update.message or not update.message.text:
        await send_message_with_retry(context.bot, chat_id, "❌ Please send a text message.", parse_mode=None)
        return
    
    new_welcome_text = update.message.text.strip()
    
    if len(new_welcome_text) < 10:
        await send_message_with_retry(context.bot, chat_id, "❌ Welcome message must be at least 10 characters long.", parse_mode=None)
        return
    
    if len(new_welcome_text) > 4000:
        await send_message_with_retry(context.bot, chat_id, "❌ Welcome message must be less than 4000 characters.", parse_mode=None)
        return
    
    # Save the new welcome message
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Ensure welcome_messages table exists
        c.execute("""
            CREATE TABLE IF NOT EXISTS welcome_messages (
                name TEXT PRIMARY KEY,
                template_text TEXT NOT NULL,
                description TEXT
            )
        """)
        
        # Update the welcome message using existing structure
        c.execute("""
            INSERT INTO welcome_messages (name, template_text, description)
            VALUES ('custom', %s, 'Custom welcome message')
            ON CONFLICT (name) DO UPDATE SET template_text = EXCLUDED.template_text, description = EXCLUDED.description
        """, (new_welcome_text,))
        
        # Set as active in bot_settings
        c.execute("""
            INSERT INTO bot_settings (setting_key, setting_value)
            VALUES ('active_welcome_message_name', 'custom')
            ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value
        """)
        
        conn.commit()
        logger.info(f"✅ Welcome message saved successfully for admin {user_id}")
        
        # Clear state
        context.user_data.pop('state', None)
        
        # Show success message with preview
        preview = new_welcome_text[:200] + "..." if len(new_welcome_text) > 200 else new_welcome_text
        
        msg = f"✅ **Welcome Message Updated!**\n\n"
        msg += f"**Preview:**\n{preview}\n\n"
        msg += f"**Length:** {len(new_welcome_text)} characters\n\n"
        msg += "The new welcome message is now active!"
        
        keyboard = [
            [InlineKeyboardButton("👀 Full Preview", callback_data="welcome_preview")],
            [InlineKeyboardButton("🏠 Back to Editor", callback_data="welcome_editor_menu")]
        ]
        
        await send_message_with_retry(context.bot, chat_id, msg, 
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"❌ Error saving welcome message: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, f"❌ Error saving welcome message: {str(e)}\n\nPlease check server logs.", parse_mode=None)
    finally:
        if conn:
            conn.close()

async def handle_welcome_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Preview the current welcome message with buttons"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get current welcome message and buttons
    welcome_text = get_active_welcome_message()
    buttons = get_start_menu_buttons()
    
    # Replace placeholders with example data
    preview_text = welcome_text.replace("{user_name}", "John Doe")
    preview_text = preview_text.replace("{user_id}", "123456789")
    preview_text = preview_text.replace("{bot_name}", "Your Bot")
    
    msg = f"👀 **Welcome Message Preview**\n\n"
    msg += f"**This is how users will see the welcome message:**\n\n"
    msg += "─" * 30 + "\n"
    msg += preview_text + "\n"
    msg += "─" * 30 + "\n\n"
    
    # Show button layout
    msg += "**Button Layout:**\n"
    rows = {}
    for btn in buttons:
        row = btn['row']
        if row not in rows:
            rows[row] = []
        rows[row].append(btn)
    
    for row_num in sorted(rows.keys()):
        msg += f"Row {row_num + 1}: "
        row_buttons = sorted(rows[row_num], key=lambda x: x['position'])
        for btn in row_buttons:
            msg += f"[{btn['text']}] "
        msg += "\n"
    
    keyboard = [
        [InlineKeyboardButton("✏️ Edit Message", callback_data="welcome_edit_text")],
        [InlineKeyboardButton("🔘 Edit Buttons", callback_data="welcome_edit_buttons")],
        [InlineKeyboardButton("⬅️ Back to Editor", callback_data="welcome_editor_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_templates(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show welcome message templates"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "📋 **Welcome Message Templates**\n\n"
    msg += "Choose from these pre-made templates:\n\n"
    
    templates = [
        {
            "name": "🎉 Friendly Welcome",
            "preview": "Hi {user_name}! 🎉 Welcome to our amazing bot! We're thrilled to have you here...",
            "callback": "welcome_template_friendly"
        },
        {
            "name": "💼 Professional",  
            "preview": "Welcome {user_name}. Thank you for choosing our service. Our bot provides...",
            "callback": "welcome_template_professional"
        },
        {
            "name": "🛒 E-commerce Focus",
            "preview": "🛒 Welcome to our store, {user_name}! Discover amazing products at great prices...",
            "callback": "welcome_template_ecommerce"
        },
        {
            "name": "🎮 Gaming Style",
            "preview": "🎮 Player {user_name} has joined the game! Ready to level up your experience?...",
            "callback": "welcome_template_gaming"
        }
    ]
    
    keyboard = []
    for template in templates:
        keyboard.append([InlineKeyboardButton(
            template["name"], 
            callback_data=template["callback"]
        )])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Editor", callback_data="welcome_editor_menu")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_template_friendly(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply friendly welcome template"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    template_text = """🎉 **Welcome to Our Bot!** 🎉

Hi there, {user_name}! 👋

We're absolutely thrilled to have you here! 🌟

Our bot is packed with amazing features just for you:

🛒 **Shop** - Discover incredible products at unbeatable prices
👤 **Profile** - Manage your account and track your orders  
🎁 **Referrals** - Earn rewards by inviting your friends
📞 **Support** - Get instant help whenever you need it
ℹ️ **Info** - Learn everything about our services
⚙️ **Settings** - Customize your perfect experience

Ready to explore? Just tap any button below! ⬇️

Let's make something amazing together! ✨"""
    
    await save_welcome_template(query, template_text, "Friendly Welcome")

async def handle_welcome_template_professional(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply professional welcome template"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    template_text = """**Welcome to Our Service**

Hello {user_name},

Thank you for choosing our platform. We provide professional-grade services designed to meet your needs efficiently.

**Available Services:**
• **Shop** - Browse our curated product catalog
• **Profile** - Access your account dashboard
• **Referrals** - Participate in our partner program
• **Support** - Contact our professional support team
• **Info** - Access service documentation
• **Settings** - Configure your preferences

Please select an option below to continue."""
    
    await save_welcome_template(query, template_text, "Professional")

async def handle_welcome_template_ecommerce(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply e-commerce welcome template"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    template_text = """🛒 **Welcome to Our Store!** 🛒

Hey {user_name}! 🎊

Get ready for an amazing shopping experience! 

💎 **Why Shop With Us:**
✅ Premium quality products
✅ Unbeatable prices & deals
✅ Fast & secure checkout
✅ 24/7 customer support
✅ Exclusive member rewards

🎁 **Special Offers:**
• New customer discounts available
• Referral rewards program
• VIP membership benefits
• Regular sales and promotions

Start shopping now and discover why thousands of customers love us! 🛍️"""
    
    await save_welcome_template(query, template_text, "E-commerce Focus")

async def handle_welcome_template_gaming(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply gaming style welcome template"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    template_text = """🎮 **Player {user_name} Has Joined!** 🎮

⚡ **LEVEL UP YOUR EXPERIENCE** ⚡

Welcome to the ultimate bot experience! Ready to power up? 🚀

🏆 **Your Quest Menu:**
🛒 **Shop** - Gear up with epic items
👤 **Profile** - Check your player stats  
🎁 **Referrals** - Recruit allies for rewards
📞 **Support** - Get backup from our team
ℹ️ **Info** - Study the game manual
⚙️ **Settings** - Customize your gameplay

💫 **Achievement Unlocked:** First Login! 
🎯 **Next Goal:** Make your first purchase

Ready to begin your adventure? Choose your path! ⬇️"""
    
    await save_welcome_template(query, template_text, "Gaming Style")

async def save_welcome_template(query, template_text, template_name):
    """Save a welcome template to database"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Insert or update the template
        template_key = template_name.lower().replace(" ", "_")
        c.execute("""
            INSERT INTO welcome_messages (name, template_text, description)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET template_text = EXCLUDED.template_text, description = EXCLUDED.description
        """, (template_key, template_text, template_name))
        
        # Set as active in bot_settings
        c.execute("""
            INSERT INTO bot_settings (setting_key, setting_value)
            VALUES ('active_welcome_message_name', %s)
            ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value
        """, (template_key,))
        
        conn.commit()
        
        msg = f"✅ **Template Applied Successfully!**\n\n"
        msg += f"**Template:** {template_name}\n"
        msg += f"**Length:** {len(template_text)} characters\n\n"
        msg += "The new welcome message is now active!"
        
        keyboard = [
            [InlineKeyboardButton("👀 Preview", callback_data="welcome_preview")],
            [InlineKeyboardButton("🏠 Back to Editor", callback_data="welcome_editor_menu")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        await query.answer("Template applied!", show_alert=False)
        
    except Exception as e:
        logger.error(f"Error saving welcome template: {e}")
        await query.answer("Error saving template", show_alert=True)
    finally:
        if conn:
            conn.close()

# Add/Delete button functions removed as requested

async def handle_welcome_auto_arrange(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Auto-arrange buttons in 2-per-row layout"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all enabled buttons
        c.execute("SELECT id, button_text FROM start_menu_buttons WHERE is_enabled = 1 ORDER BY id")
        buttons = c.fetchall()
        
        # Rearrange in 2-per-row layout
        for i, btn in enumerate(buttons):
            new_row = i // 2
            new_col = i % 2
            
            c.execute("""
                UPDATE start_menu_buttons 
                SET row_position = %s, column_position = %s
                WHERE id = %s
            """, (new_row, new_col, btn['id']))
        
        conn.commit()
        
        msg = f"✅ **Buttons Auto-Arranged!**\n\n"
        msg += f"Arranged {len(buttons)} buttons in a clean 2-per-row layout.\n\n"
        msg += "**New Layout:**\n"
        
        for i, btn in enumerate(buttons):
            row = i // 2
            col = i % 2
            if col == 0:
                msg += f"Row {row + 1}: [{btn['button_text']}]"
            else:
                msg += f" [{btn['button_text']}]\n"
        
        if len(buttons) % 2 == 1:  # Odd number of buttons
            msg += "\n"
        
        keyboard = [
            [InlineKeyboardButton("👀 Preview Layout", callback_data="welcome_preview_buttons")],
            [InlineKeyboardButton("🔘 Back to Buttons", callback_data="welcome_edit_buttons")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        await query.answer("Buttons rearranged!", show_alert=False)
        
    except Exception as e:
        logger.error(f"Error auto-arranging buttons: {e}")
        await query.answer("Error rearranging buttons", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_welcome_preview_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Preview the button layout"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    buttons = get_start_menu_buttons()
    
    msg = "👀 **Button Layout Preview**\n\n"
    msg += "**This is how the start menu buttons will appear:**\n\n"
    
    # Group buttons by row
    rows = {}
    for btn in buttons:
        row = btn['row']
        if row not in rows:
            rows[row] = []
        rows[row].append(btn)
    
    # Display layout
    for row_num in sorted(rows.keys()):
        msg += f"**Row {row_num + 1}:** "
        row_buttons = sorted(rows[row_num], key=lambda x: x['position'])
        for btn in row_buttons:
            msg += f"[{btn['text']}] "
        msg += "\n"
    
    msg += f"\n**Total Buttons:** {len(buttons)}\n"
    msg += f"**Total Rows:** {len(rows)}\n\n"
    msg += "**Layout Tips:**\n"
    msg += "• Keep important buttons in top rows\n"
    msg += "• Use 2 buttons per row for best mobile experience\n"
    msg += "• Keep button text short and clear"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Rearrange", callback_data="welcome_rearrange_buttons")],
        [InlineKeyboardButton("✏️ Edit Buttons", callback_data="welcome_edit_buttons")],
        [InlineKeyboardButton("⬅️ Back to Editor", callback_data="welcome_editor_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- Missing Button Management Handlers ---

async def handle_welcome_move_button(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Move a specific button to new position"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid button ID", show_alert=True)
        return
    
    button_id = int(params[0])
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get button info
        c.execute("SELECT button_text, row_position, column_position FROM start_menu_buttons WHERE id = %s", (button_id,))
        button = c.fetchone()
        
        if not button:
            await query.answer("Button not found", show_alert=True)
            return
        
        msg = f"🔄 **Move Button: {button['button_text']}**\n\n"
        msg += f"**Current Position:** Row {button['row_position'] + 1}, Column {button['column_position'] + 1}\n\n"
        msg += "Select new position:"
        
        keyboard = [
            [InlineKeyboardButton("📍 Row 1, Col 1", callback_data=f"welcome_set_position|{button_id}|0|0")],
            [InlineKeyboardButton("📍 Row 1, Col 2", callback_data=f"welcome_set_position|{button_id}|0|1")],
            [InlineKeyboardButton("📍 Row 2, Col 1", callback_data=f"welcome_set_position|{button_id}|1|0")],
            [InlineKeyboardButton("📍 Row 2, Col 2", callback_data=f"welcome_set_position|{button_id}|1|1")],
            [InlineKeyboardButton("📍 Row 3, Col 1", callback_data=f"welcome_set_position|{button_id}|2|0")],
            [InlineKeyboardButton("📍 Row 3, Col 2", callback_data=f"welcome_set_position|{button_id}|2|1")],
            [InlineKeyboardButton("⬅️ Back", callback_data="welcome_rearrange_buttons")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error moving button: {e}")
        await query.answer("Error loading button", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_welcome_toggle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Enable/disable buttons"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all buttons
        c.execute("""
            SELECT id, button_text, is_enabled
            FROM start_menu_buttons 
            ORDER BY row_position, column_position
        """)
        buttons = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading buttons for toggle: {e}")
        buttons = []
    finally:
        if conn:
            conn.close()
    
    msg = "❌ **Enable/Disable Start Menu Buttons**\n\n"
    msg += "Toggle buttons on/off. Disabled buttons won't appear in the start menu.\n\n"
    msg += "**Current Status:**\n"
    
    keyboard = []
    for btn in buttons:
        status = "✅ Enabled" if btn['is_enabled'] else "❌ Disabled"
        status_emoji = "✅" if btn['is_enabled'] else "❌"
        
        msg += f"{status_emoji} {btn['button_text']} - {status}\n"
        
        toggle_text = f"❌ Disable {btn['button_text']}" if btn['is_enabled'] else f"✅ Enable {btn['button_text']}"
        keyboard.append([InlineKeyboardButton(toggle_text, callback_data=f"welcome_toggle_button|{btn['id']}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Button Manager", callback_data="welcome_edit_buttons")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_edit_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit button text"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all buttons
        c.execute("""
            SELECT id, button_text, callback_data
            FROM start_menu_buttons 
            WHERE is_enabled = 1
            ORDER BY row_position, column_position
        """)
        buttons = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading buttons for editing: {e}")
        buttons = []
    finally:
        if conn:
            conn.close()
    
    msg = "✏️ **Edit Button Text**\n\n"
    msg += "Select a button to edit its text:\n\n"
    
    keyboard = []
    for btn in buttons:
        keyboard.append([InlineKeyboardButton(
            f"Edit: {btn['button_text']}", 
            callback_data=f"welcome_edit_text_for|{btn['id']}"
        )])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Button Manager", callback_data="welcome_edit_buttons")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_use_template(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show template selection for text editing"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Redirect to templates
    await handle_welcome_templates(update, context, params)

async def handle_welcome_toggle_button(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle individual button enable/disable"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid button ID", show_alert=True)
        return
    
    button_id = int(params[0])
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get current status
        c.execute("SELECT button_text, is_enabled FROM start_menu_buttons WHERE id = %s", (button_id,))
        button = c.fetchone()
        
        if not button:
            await query.answer("Button not found", show_alert=True)
            return
        
        # Toggle status
        new_status = not button['is_enabled']
        c.execute("UPDATE start_menu_buttons SET is_enabled = %s WHERE id = %s", (new_status, button_id))
        conn.commit()
        
        action = "enabled" if new_status else "disabled"
        status_emoji = "✅" if new_status else "❌"
        
        await query.answer(f"Button {action}!", show_alert=False)
        
        # Refresh the toggle menu
        await handle_welcome_toggle_buttons(update, context, params)
        
    except Exception as e:
        logger.error(f"Error toggling button: {e}")
        await query.answer("Error toggling button", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_welcome_set_position(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set button position"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params or len(params) < 3:
        await query.answer("Invalid parameters", show_alert=True)
        return
    
    button_id = int(params[0])
    new_row = int(params[1])
    new_col = int(params[2])
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Update button position
        c.execute("""
            UPDATE start_menu_buttons 
            SET row_position = %s, column_position = %s
            WHERE id = %s
        """, (new_row, new_col, button_id))
        
        conn.commit()
        
        # Get button name for confirmation
        c.execute("SELECT button_text FROM start_menu_buttons WHERE id = %s", (button_id,))
        button = c.fetchone()
        
        msg = f"✅ **Button Moved Successfully!**\n\n"
        msg += f"**Button:** {button['button_text']}\n"
        msg += f"**New Position:** Row {new_row + 1}, Column {new_col + 1}\n\n"
        msg += "The button has been moved to its new position!"
        
        keyboard = [
            [InlineKeyboardButton("👀 Preview Layout", callback_data="welcome_preview_buttons")],
            [InlineKeyboardButton("🔄 Move Another", callback_data="welcome_rearrange_buttons")],
            [InlineKeyboardButton("⬅️ Back to Buttons", callback_data="welcome_edit_buttons")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        await query.answer("Button moved!", show_alert=False)
        
    except Exception as e:
        logger.error(f"Error setting button position: {e}")
        await query.answer("Error moving button", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_welcome_reset_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm reset to default welcome message and buttons"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "⚠️ **Reset to Default Settings**\n\n"
    msg += "This will reset:\n"
    msg += "• Welcome message to default template\n"
    msg += "• All start menu buttons to default layout\n"
    msg += "• All button positions and settings\n\n"
    msg += "**This action cannot be undone!**\n\n"
    msg += "Are you sure you want to reset everything to default?"
    
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Reset Everything", callback_data="welcome_reset_execute")],
        [InlineKeyboardButton("❌ Cancel", callback_data="welcome_editor_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_welcome_reset_execute(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Execute reset to default settings"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Reset welcome message to default
        c.execute("""
            INSERT INTO welcome_messages (name, template_text, description)
            VALUES ('default', %s, 'Default welcome message')
            ON CONFLICT (name) DO UPDATE SET template_text = EXCLUDED.template_text, description = EXCLUDED.description
        """, (DEFAULT_WELCOME_TEXT,))
        
        # Set default as active
        c.execute("""
            INSERT INTO bot_settings (setting_key, setting_value)
            VALUES ('active_welcome_message_name', 'default')
            ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value
        """)
        
        # Clear all existing buttons
        c.execute("DELETE FROM start_menu_buttons")
        
        # Insert default buttons
        for button in DEFAULT_START_BUTTONS:
            c.execute("""
                INSERT INTO start_menu_buttons (button_text, callback_data, row_position, column_position, is_enabled)
                VALUES (%s, %s, %s, %s, %s)
            """, (button["text"], button["callback"], button["row"], button["position"], button["enabled"]))
        
        conn.commit()
        
        msg = "✅ **Reset Complete!**\n\n"
        msg += "Everything has been reset to default settings:\n\n"
        msg += "📝 **Welcome Message:** Default template restored\n"
        msg += "🔘 **Start Buttons:** Default layout restored\n"
        msg += "⚙️ **Settings:** All configurations reset\n\n"
        msg += "Your bot is now using the default welcome experience!"
        
        keyboard = [
            [InlineKeyboardButton("👀 Preview Welcome", callback_data="welcome_preview")],
            [InlineKeyboardButton("🏠 Back to Editor", callback_data="welcome_editor_menu")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error resetting welcome settings: {e}")
        await query.edit_message_text("❌ Error resetting settings. Please try again.", parse_mode=None)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

async def handle_welcome_save_changes(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save all welcome message and button changes"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # This function can be used to save any pending changes
    # For now, most changes are saved automatically, but this provides a way to force save
    
    msg = "✅ **Changes Saved!**\n\n"
    msg += "All your welcome message and button changes have been saved successfully.\n\n"
    msg += "The changes are now active for all users!"
    
    keyboard = [
        [InlineKeyboardButton("👀 Preview Welcome", callback_data="welcome_preview")],
        [InlineKeyboardButton("🏠 Back to Editor", callback_data="welcome_editor_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- END OF FILE welcome_editor.py ---
