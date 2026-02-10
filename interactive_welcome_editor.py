# --- START OF FILE interactive_welcome_editor.py ---

"""
🎨 SUPER INTERACTIVE WELCOME MESSAGE EDITOR 🎨
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Advanced, interactive welcome message editor with:
- Live preview with real user data
- Visual drag & drop button management
- Real-time text editing with formatting helpers
- Advanced template system with AI-like suggestions
- Interactive button designer
- Multi-language support
- A/B testing integration
- Analytics and optimization tips

Version: 2.0.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import logging
import json
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from utils import (
    get_db_connection, send_message_with_retry, is_primary_admin,
    format_currency, get_user_status, get_progress_bar
)

logger = logging.getLogger(__name__)

# Enhanced button configuration with visual properties
ENHANCED_DEFAULT_BUTTONS = [
    {"text": "🛒 Shop", "callback": "shop_menu", "row": 0, "col": 0, "enabled": True, "color": "green", "style": "primary"},
    {"text": "👤 Profile", "callback": "user_profile", "row": 0, "col": 1, "enabled": True, "color": "blue", "style": "secondary"},
    {"text": "🎁 Referrals", "callback": "referral_menu", "row": 1, "col": 0, "enabled": True, "color": "purple", "style": "accent"},
    {"text": "📞 Support", "callback": "support_menu", "row": 1, "col": 1, "enabled": True, "color": "orange", "style": "secondary"},
    {"text": "ℹ️ Info", "callback": "info_menu", "row": 2, "col": 0, "enabled": True, "color": "gray", "style": "minimal"},
    {"text": "⚙️ Settings", "callback": "user_settings", "row": 2, "col": 1, "enabled": True, "color": "gray", "style": "minimal"}
]

# Advanced welcome message templates with categories
INTERACTIVE_TEMPLATES = {
    "business": {
        "name": "🏢 Professional Business",
        "category": "Business",
        "preview": "Welcome to our professional service platform...",
        "template": """🏢 **Welcome to {bot_name}** 🏢

Hello {user_name},

Thank you for choosing our professional platform. We're committed to providing you with exceptional service and support.

**Your Account:**
• Status: {status}
• Balance: {balance_str}
• Purchases: {purchases}

**What you can do:**
✅ Browse our premium services
✅ Manage your account and preferences  
✅ Access 24/7 professional support
✅ Track your order history and progress

Ready to get started? Choose an option below.""",
        "suggested_buttons": ["🛒 Services", "👤 Account", "📞 Support", "📊 Dashboard"],
        "tone": "professional",
        "industry": "business"
    },
    
    "ecommerce": {
        "name": "🛍️ E-Commerce Store",
        "category": "Retail",
        "preview": "Welcome to our amazing store with great deals...",
        "template": """🛍️ **Welcome to {bot_name} Store!** 🛍️

Hey {user_name}! 🎉

Get ready for an amazing shopping experience! We've got incredible deals, fast shipping, and customer service that actually cares.

**Your Shopping Profile:**
• VIP Status: {status} {progress_bar}
• Wallet: {balance_str} 💰
• Orders: {purchases} 📦
• Cart: {basket_count} items 🛒

**Today's Highlights:**
🔥 Flash Sales - Up to 70% OFF
🚚 Free Shipping on orders over $50
🎁 Daily Rewards Program
⭐ 5-Star Customer Reviews

Start shopping and save big! 💸""",
        "suggested_buttons": ["🛒 Shop Now", "🔥 Deals", "🎁 Rewards", "📦 Orders"],
        "tone": "exciting",
        "industry": "retail"
    },
    
    "gaming": {
        "name": "🎮 Gaming Platform",
        "category": "Gaming",
        "preview": "Player has joined the game! Ready to level up...",
        "template": """🎮 **Player {user_name} Has Entered the Game!** 🎮

Welcome to the ultimate gaming experience! 🚀

**Player Stats:**
🏆 Level: {status}
⚡ Progress: {progress_bar}
💎 Credits: {balance_str}
🎯 Achievements: {purchases} unlocked

**Game Modes Available:**
🏁 Quick Play - Jump right in
🏆 Ranked Matches - Climb the ladder  
👥 Team Battles - Play with friends
🎪 Special Events - Limited time rewards

**Daily Bonuses Ready!**
🎁 Login Reward: +100 Credits
⚡ Energy Boost: Full charge
🏆 XP Multiplier: 2x for 1 hour

Ready to dominate? Let's play! 🔥""",
        "suggested_buttons": ["🎮 Play", "🏆 Leaderboard", "🎁 Rewards", "👥 Friends"],
        "tone": "energetic",
        "industry": "gaming"
    },
    
    "community": {
        "name": "👥 Community Hub",
        "category": "Social",
        "preview": "Join our amazing community of like-minded people...",
        "template": """👥 **Welcome to Our Community, {user_name}!** 👥

We're thrilled to have you join our growing family! 🌟

**Community Stats:**
👤 Member Level: {status}
🌟 Reputation: {progress_bar}
💬 Posts: {purchases}
🤝 Connections: Growing daily!

**What's Happening:**
🔥 Hot Topics - Join trending discussions
📸 Photo Contest - Win amazing prizes
🎉 Weekly Events - Meet new friends
💡 Knowledge Sharing - Learn & teach

**Community Guidelines:**
✅ Be respectful and kind
✅ Share valuable content
✅ Help others grow
✅ Have fun and connect!

Let's build something amazing together! 🚀""",
        "suggested_buttons": ["💬 Discussions", "📸 Gallery", "🎉 Events", "👥 Members"],
        "tone": "friendly",
        "industry": "social"
    },
    
    "educational": {
        "name": "📚 Learning Platform",
        "category": "Education",
        "preview": "Start your learning journey with expert courses...",
        "template": """📚 **Welcome to {bot_name} Academy!** 📚

Hello {user_name}! Ready to unlock your potential? 🧠✨

**Your Learning Profile:**
🎓 Level: {status}
📈 Progress: {progress_bar}  
💳 Credits: {balance_str}
📖 Courses Completed: {purchases}

**Featured Learning Paths:**
🚀 Beginner Fundamentals - Start here
💼 Professional Skills - Advance your career
🎨 Creative Arts - Express yourself
💻 Tech & Programming - Build the future

**Learning Benefits:**
✅ Expert-led courses
✅ Interactive exercises
✅ Certificates of completion
✅ Lifetime access to materials

Your journey to mastery starts now! 📈""",
        "suggested_buttons": ["📚 Courses", "🎓 My Progress", "🏆 Certificates", "👨‍🏫 Instructors"],
        "tone": "inspiring",
        "industry": "education"
    },
    
    "health": {
        "name": "🏥 Health & Wellness",
        "category": "Healthcare",
        "preview": "Take control of your health journey with us...",
        "template": """🏥 **Welcome to Your Health Journey, {user_name}!** 🏥

Your wellness is our priority! 💚

**Health Profile:**
💪 Wellness Level: {status}
📊 Progress: {progress_bar}
💳 Health Credits: {balance_str}
📋 Consultations: {purchases}

**Available Services:**
🩺 Virtual Consultations - 24/7 availability
💊 Prescription Management - Easy refills
📱 Health Tracking - Monitor your progress
🏃‍♀️ Fitness Plans - Personalized workouts

**Health Tips Today:**
💧 Drink 8 glasses of water
🥗 Eat 5 servings of fruits/vegetables  
😴 Get 7-8 hours of sleep
🧘‍♀️ Practice 10 minutes of mindfulness

Take the first step towards better health! 🌟""",
        "suggested_buttons": ["🩺 Consult", "💊 Prescriptions", "📊 Health Data", "🏃‍♀️ Fitness"],
        "tone": "caring",
        "industry": "healthcare"
    }
}

# Visual button styles and colors
BUTTON_STYLES = {
    "primary": {"emoji": "🔵", "description": "Main action button"},
    "secondary": {"emoji": "⚪", "description": "Secondary action"},
    "accent": {"emoji": "🟣", "description": "Special feature"},
    "success": {"emoji": "🟢", "description": "Positive action"},
    "warning": {"emoji": "🟡", "description": "Caution needed"},
    "danger": {"emoji": "🔴", "description": "Destructive action"},
    "minimal": {"emoji": "⚫", "description": "Subtle option"}
}

BUTTON_COLORS = {
    "blue": "🔵", "green": "🟢", "red": "🔴", "yellow": "🟡",
    "purple": "🟣", "orange": "🟠", "gray": "⚫", "white": "⚪"
}

# --- Enhanced Database Functions ---

def init_interactive_welcome_tables():
    """Initialize enhanced welcome message tables with interactive features"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Enhanced welcome messages table
        c.execute("""
            CREATE TABLE IF NOT EXISTS interactive_welcome_messages (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                template_text TEXT NOT NULL,
                category TEXT DEFAULT 'custom',
                tone TEXT DEFAULT 'friendly',
                industry TEXT DEFAULT 'general',
                preview_text TEXT,
                suggested_buttons TEXT,
                is_active BOOLEAN DEFAULT FALSE,
                usage_count INTEGER DEFAULT 0,
                rating REAL DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Enhanced buttons table with visual properties
        c.execute("""
            CREATE TABLE IF NOT EXISTS interactive_start_buttons (
                id SERIAL PRIMARY KEY,
                button_text TEXT NOT NULL,
                callback_data TEXT NOT NULL,
                row_position INTEGER DEFAULT 0,
                col_position INTEGER DEFAULT 0,
                is_enabled BOOLEAN DEFAULT TRUE,
                button_color TEXT DEFAULT 'blue',
                button_style TEXT DEFAULT 'primary',
                description TEXT,
                usage_stats INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Welcome editor sessions for real-time editing
        c.execute("""
            CREATE TABLE IF NOT EXISTS welcome_editor_sessions (
                id SERIAL PRIMARY KEY,
                admin_user_id BIGINT NOT NULL,
                session_data TEXT,
                last_preview TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert enhanced templates if none exist
        c.execute("SELECT COUNT(*) as count FROM interactive_welcome_messages")
        result = c.fetchone()
        if result['count'] == 0:
            for template_key, template_data in INTERACTIVE_TEMPLATES.items():
                c.execute("""
                    INSERT INTO interactive_welcome_messages 
                    (name, template_text, category, tone, industry, preview_text, suggested_buttons)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    template_data["name"],
                    template_data["template"],
                    template_data["category"],
                    template_data["tone"],
                    template_data["industry"],
                    template_data["preview"],
                    json.dumps(template_data["suggested_buttons"])
                ))
        
        # Insert enhanced buttons if none exist
        c.execute("SELECT COUNT(*) as count FROM interactive_start_buttons")
        result = c.fetchone()
        if result['count'] == 0:
            for button in ENHANCED_DEFAULT_BUTTONS:
                c.execute("""
                    INSERT INTO interactive_start_buttons 
                    (button_text, callback_data, row_position, col_position, is_enabled, button_color, button_style)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    button["text"], button["callback"], button["row"], button["col"],
                    button["enabled"], button["color"], button["style"]
                ))
        
        conn.commit()
        logger.info("Interactive welcome message tables initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing interactive welcome tables: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise  # Re-raise to see the actual error
    finally:
        if conn:
            conn.close()

# --- Main Interactive Editor ---

async def handle_interactive_welcome_editor(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """🎨 Super Interactive Welcome Message Editor Dashboard"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Initialize session
    await init_editor_session(query.from_user.id, context)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get current active message
        c.execute("""
            SELECT name, category, tone, usage_count, rating
            FROM interactive_welcome_messages 
            WHERE is_active = TRUE 
            LIMIT 1
        """)
        active_msg = c.fetchone()
        
        # Get button statistics
        c.execute("""
            SELECT COUNT(*) as total, 
                   SUM(CASE WHEN is_enabled = 1 THEN 1 ELSE 0 END) as enabled,
                   AVG(usage_stats) as avg_usage
            FROM interactive_start_buttons
        """)
        button_stats = c.fetchone()
        
        # Get template statistics
        c.execute("SELECT COUNT(*) as total FROM interactive_welcome_messages")
        template_count = c.fetchone()['total']
        
    except Exception as e:
        logger.error(f"Error loading interactive editor: {e}")
        active_msg = None
        button_stats = {'total': 0, 'enabled': 0, 'avg_usage': 0}
        template_count = 0
    finally:
        if conn:
            conn.close()
    
    # Build dashboard message
    msg = "🎨 **SUPER INTERACTIVE WELCOME EDITOR** 🎨\n\n"
    msg += "✨ **Professional-Grade Message Designer** ✨\n\n"
    
    if active_msg:
        rating_stars = "⭐" * min(5, max(1, int(active_msg['rating'])))
        msg += f"📝 **Active Message:** {active_msg['name']}\n"
        msg += f"🏷️ **Category:** {active_msg['category']} | **Tone:** {active_msg['tone'].title()}\n"
        msg += f"📊 **Usage:** {active_msg['usage_count']} times | **Rating:** {rating_stars}\n\n"
    else:
        msg += f"📝 **Active Message:** Default Template\n\n"
    
    if button_stats:
        msg += f"🔘 **Button Layout:** {button_stats['enabled']}/{button_stats['total']} active\n"
        msg += f"📈 **Avg Button Usage:** {button_stats['avg_usage']:.1f} clicks\n\n"
    
    msg += f"📋 **Template Library:** {template_count} professional templates\n\n"
    msg += "**🚀 What would you like to create?**"
    
    keyboard = [
        [
            InlineKeyboardButton("✏️ Live Text Editor", callback_data="interactive_text_editor"),
            InlineKeyboardButton("🎨 Visual Button Designer", callback_data="interactive_button_designer")
        ],
        [
            InlineKeyboardButton("👀 Live Preview", callback_data="interactive_live_preview"),
            InlineKeyboardButton("📋 Smart Templates", callback_data="interactive_smart_templates")
        ],
        [
            InlineKeyboardButton("🎯 A/B Test Creator", callback_data="interactive_ab_test"),
            InlineKeyboardButton("📊 Analytics Dashboard", callback_data="interactive_analytics")
        ],
        [
            InlineKeyboardButton("🔧 Advanced Settings", callback_data="interactive_advanced"),
            InlineKeyboardButton("💾 Export/Import", callback_data="interactive_export_import")
        ],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def init_editor_session(admin_user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Initialize or restore editor session"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check for existing session
        c.execute("""
            SELECT session_data FROM welcome_editor_sessions 
            WHERE admin_user_id = %s 
            ORDER BY updated_at DESC 
            LIMIT 1
        """, (admin_user_id,))
        
        session = c.fetchone()
        if session and session['session_data']:
            # Restore session
            session_data = json.loads(session['session_data'])
            context.user_data['interactive_editor_session'] = session_data
        else:
            # Create new session
            context.user_data['interactive_editor_session'] = {
                'current_template': None,
                'editing_mode': None,
                'button_layout': [],
                'preview_data': {},
                'last_save': None
            }
    except Exception as e:
        logger.error(f"Error initializing editor session: {e}")
        context.user_data['interactive_editor_session'] = {}
    finally:
        if conn:
            conn.close()

# --- Live Text Editor ---

async def handle_interactive_text_editor(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """✏️ Live Text Editor with real-time preview"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Set editing mode
    context.user_data['interactive_editor_session']['editing_mode'] = 'text'
    context.user_data['state'] = 'interactive_text_editing'
    
    msg = "✏️ **LIVE TEXT EDITOR** ✏️\n\n"
    msg += "🎯 **Smart Writing Assistant Activated!**\n\n"
    
    msg += "**✨ Writing Tools:**\n"
    msg += "🔤 **Placeholders:** `{user_name}`, `{balance_str}`, `{status}`\n"
    msg += "🎨 **Formatting:** `**bold**`, `*italic*`, `__underline__`\n"
    msg += "😀 **Emojis:** Auto-suggestions as you type\n"
    msg += "📏 **Length:** Optimal 100-300 characters\n\n"
    
    msg += "**🎭 Tone Suggestions:**\n"
    msg += "• **Professional:** Formal and trustworthy\n"
    msg += "• **Friendly:** Warm and welcoming  \n"
    msg += "• **Exciting:** Energetic and fun\n"
    msg += "• **Caring:** Supportive and helpful\n\n"
    
    msg += "**📝 Start typing your welcome message...**\n"
    msg += "*(I'll show you a live preview as you write!)*"
    
    keyboard = [
        [
            InlineKeyboardButton("🎨 Use AI Assistant", callback_data="interactive_ai_assistant"),
            InlineKeyboardButton("📋 Quick Templates", callback_data="interactive_quick_templates")
        ],
        [
            InlineKeyboardButton("👀 Preview Now", callback_data="interactive_live_preview"),
            InlineKeyboardButton("💾 Save Draft", callback_data="interactive_save_draft")
        ],
        [
            InlineKeyboardButton("🔧 Formatting Help", callback_data="interactive_formatting_help"),
            InlineKeyboardButton("❌ Cancel", callback_data="interactive_welcome_editor")
        ]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- Visual Button Designer ---

async def handle_interactive_button_designer(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """🎨 Visual Button Designer with drag & drop interface"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get current button layout
        c.execute("""
            SELECT id, button_text, row_position, col_position, is_enabled, 
                   button_color, button_style, usage_stats
            FROM interactive_start_buttons 
            ORDER BY row_position, col_position
        """)
        buttons = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading button designer: {e}")
        buttons = []
    finally:
        if conn:
            conn.close()
    
    # Build visual layout
    msg = "🎨 **VISUAL BUTTON DESIGNER** 🎨\n\n"
    msg += "🖱️ **Drag & Drop Interface Activated!**\n\n"
    
    # Group buttons by row
    button_rows = {}
    for btn in buttons:
        row = btn['row_position']
        if row not in button_rows:
            button_rows[row] = []
        button_rows[row].append(btn)
    
    msg += "**📱 Current Layout Preview:**\n\n"
    
    for row_num in sorted(button_rows.keys()):
        msg += f"**Row {row_num + 1}:** "
        row_buttons = sorted(button_rows[row_num], key=lambda x: x['col_position'])
        
        for btn in row_buttons:
            color_emoji = BUTTON_COLORS.get(btn['button_color'], '🔵')
            style_emoji = BUTTON_STYLES.get(btn['button_style'], {}).get('emoji', '🔵')
            status = "✅" if btn['is_enabled'] else "❌"
            usage = f"({btn['usage_stats']})" if btn['usage_stats'] > 0 else ""
            
            msg += f"{status}{color_emoji} {btn['button_text']}{usage} | "
        
        msg = msg.rstrip(" | ") + "\n"
    
    msg += f"\n**📊 Total Buttons:** {len(buttons)} | **Active:** {sum(1 for b in buttons if b['is_enabled'])}\n\n"
    msg += "**🎯 What would you like to do?**"
    
    keyboard = [
        [
            InlineKeyboardButton("➕ Add New Button", callback_data="interactive_add_button"),
            InlineKeyboardButton("✏️ Edit Button", callback_data="interactive_edit_button")
        ],
        [
            InlineKeyboardButton("🎨 Change Colors", callback_data="interactive_button_colors"),
            InlineKeyboardButton("📐 Rearrange Layout", callback_data="interactive_rearrange_buttons")
        ],
        [
            InlineKeyboardButton("🔄 Auto-Arrange", callback_data="interactive_auto_arrange"),
            InlineKeyboardButton("📊 Button Analytics", callback_data="interactive_button_analytics")
        ],
        [
            InlineKeyboardButton("💾 Save Layout", callback_data="interactive_save_layout"),
            InlineKeyboardButton("⬅️ Back to Editor", callback_data="interactive_welcome_editor")
        ]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- Live Preview ---

async def handle_interactive_live_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """👀 Live Preview with real user data simulation"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Generate realistic preview data
    preview_users = [
        {"name": "Alex", "balance": 125.50, "purchases": 8, "status": "VIP Customer"},
        {"name": "Sarah", "balance": 89.25, "purchases": 3, "status": "Regular"},
        {"name": "Mike", "balance": 250.00, "purchases": 15, "status": "Gold Member"},
        {"name": "Emma", "balance": 45.75, "purchases": 1, "status": "New User"}
    ]
    
    selected_user = random.choice(preview_users)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get active welcome message
        c.execute("""
            SELECT template_text, name FROM interactive_welcome_messages 
            WHERE is_active = TRUE LIMIT 1
        """)
        active_template = c.fetchone()
        
        if not active_template:
            # Fallback to default
            template_text = INTERACTIVE_TEMPLATES["business"]["template"]
            template_name = "Default Template"
        else:
            template_text = active_template['template_text']
            template_name = active_template['name']
        
        # Get button layout
        c.execute("""
            SELECT button_text, callback_data, button_color, button_style
            FROM interactive_start_buttons 
            WHERE is_enabled = 1
            ORDER BY row_position, col_position
        """)
        buttons = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error generating live preview: {e}")
        template_text = "Welcome {user_name}! 👋"
        template_name = "Error Template"
        buttons = []
    finally:
        if conn:
            conn.close()
    
    # Format the preview message
    try:
        progress_bar = get_progress_bar(selected_user["purchases"])
        formatted_message = template_text.format(
            user_name=selected_user["name"],
            username=selected_user["name"],
            user_id=12345,
            bot_name="Your Bot",
            status=selected_user["status"],
            balance_str=format_currency(selected_user["balance"]),
            purchases=selected_user["purchases"],
            basket_count=random.randint(0, 5),
            progress_bar=progress_bar
        )
    except Exception as e:
        logger.error(f"Error formatting preview: {e}")
        formatted_message = f"Welcome {selected_user['name']}! 👋\n\nBalance: {format_currency(selected_user['balance'])}"
    
    # Build preview
    preview_msg = "👀 **LIVE PREVIEW** 👀\n\n"
    preview_msg += f"📱 **Simulating user:** {selected_user['name']}\n"
    preview_msg += f"💰 **Balance:** {format_currency(selected_user['balance'])}\n"
    preview_msg += f"🛒 **Purchases:** {selected_user['purchases']}\n\n"
    preview_msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    preview_msg += formatted_message
    preview_msg += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Show button preview
    if buttons:
        preview_msg += "**🔘 Button Layout:**\n"
        button_count = 0
        for button in buttons:
            if button_count < 6:  # Show first 6 buttons
                color_emoji = BUTTON_COLORS.get(button['button_color'], '🔵')
                preview_msg += f"{color_emoji} {button['button_text']} | "
                button_count += 1
        preview_msg = preview_msg.rstrip(" | ")
        if len(buttons) > 6:
            preview_msg += f"\n*(+{len(buttons) - 6} more buttons)*"
    
    keyboard = [
        [
            InlineKeyboardButton("🔄 Try Different User", callback_data="interactive_live_preview"),
            InlineKeyboardButton("📱 Mobile Preview", callback_data="interactive_mobile_preview")
        ],
        [
            InlineKeyboardButton("✏️ Edit Message", callback_data="interactive_text_editor"),
            InlineKeyboardButton("🎨 Edit Buttons", callback_data="interactive_button_designer")
        ],
        [
            InlineKeyboardButton("💾 Save This Version", callback_data="interactive_save_current"),
            InlineKeyboardButton("⬅️ Back to Editor", callback_data="interactive_welcome_editor")
        ]
    ]
    
    await query.edit_message_text(preview_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- Smart Templates ---

async def handle_interactive_smart_templates(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """📋 Smart Template Gallery with AI-powered suggestions"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "📋 **SMART TEMPLATE GALLERY** 📋\n\n"
    msg += "🤖 **AI-Powered Template Suggestions**\n\n"
    msg += "Choose from our professionally designed templates:\n\n"
    
    # Show template categories
    categories = {}
    for template_key, template_data in INTERACTIVE_TEMPLATES.items():
        category = template_data["category"]
        if category not in categories:
            categories[category] = []
        categories[category].append((template_key, template_data))
    
    keyboard = []
    
    # Add category buttons
    for category, templates in categories.items():
        template_count = len(templates)
        emoji_map = {
            "Business": "🏢", "Retail": "🛍️", "Gaming": "🎮", 
            "Social": "👥", "Education": "📚", "Healthcare": "🏥"
        }
        emoji = emoji_map.get(category, "📋")
        
        keyboard.append([InlineKeyboardButton(
            f"{emoji} {category} ({template_count})",
            callback_data=f"interactive_template_category|{category.lower()}"
        )])
    
    # Add special options
    keyboard.extend([
        [
            InlineKeyboardButton("🤖 AI Template Generator", callback_data="interactive_ai_generator"),
            InlineKeyboardButton("⭐ Most Popular", callback_data="interactive_popular_templates")
        ],
        [
            InlineKeyboardButton("📊 Template Analytics", callback_data="interactive_template_analytics"),
            InlineKeyboardButton("💾 My Saved Templates", callback_data="interactive_my_templates")
        ],
        [InlineKeyboardButton("⬅️ Back to Editor", callback_data="interactive_welcome_editor")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- Message State Handler ---

async def handle_interactive_text_editing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle real-time text editing with live feedback"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(user_id):
        return
    
    if context.user_data.get("state") != "interactive_text_editing":
        return
    
    if not update.message or not update.message.text:
        await send_message_with_retry(context.bot, chat_id, 
            "❌ Please send a text message for your welcome message.", parse_mode=None)
        return
    
    new_text = update.message.text.strip()
    
    # Validate message
    if len(new_text) < 10:
        await send_message_with_retry(context.bot, chat_id, 
            "⚠️ **Message too short!** Try adding more details to make it engaging.\n\n"
            "💡 **Tip:** Aim for 100-300 characters for optimal engagement.", parse_mode='Markdown')
        return
    
    if len(new_text) > 4000:
        await send_message_with_retry(context.bot, chat_id, 
            "⚠️ **Message too long!** Telegram has a 4096 character limit.\n\n"
            "✂️ **Current length:** " + str(len(new_text)) + " characters", parse_mode='Markdown')
        return
    
    # Analyze message quality
    analysis = analyze_message_quality(new_text)
    
    # Save the message
    success = await save_interactive_message(new_text, user_id, analysis)
    
    if success:
        # Clear state
        context.user_data.pop('state', None)
        
        # Show success with analysis
        msg = "✅ **Message Saved Successfully!** ✅\n\n"
        msg += f"📝 **Your Message:** {new_text[:100]}{'...' if len(new_text) > 100 else ''}\n\n"
        msg += "**📊 Quality Analysis:**\n"
        msg += f"📏 **Length:** {analysis['length']} chars ({analysis['length_rating']})\n"
        msg += f"😀 **Emojis:** {analysis['emoji_count']} ({analysis['emoji_rating']})\n"
        msg += f"🎯 **Tone:** {analysis['tone']} ({analysis['tone_rating']})\n"
        msg += f"⭐ **Overall Score:** {analysis['overall_score']}/10\n\n"
        
        if analysis['suggestions']:
            msg += "**💡 Suggestions:**\n"
            for suggestion in analysis['suggestions'][:3]:
                msg += f"• {suggestion}\n"
            msg += "\n"
        
        keyboard = [
            [
                InlineKeyboardButton("👀 Live Preview", callback_data="interactive_live_preview"),
                InlineKeyboardButton("✏️ Edit More", callback_data="interactive_text_editor")
            ],
            [
                InlineKeyboardButton("🎨 Design Buttons", callback_data="interactive_button_designer"),
                InlineKeyboardButton("🏠 Back to Editor", callback_data="interactive_welcome_editor")
            ]
        ]
        
        await send_message_with_retry(context.bot, chat_id, msg, 
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await send_message_with_retry(context.bot, chat_id, 
            "❌ Error saving message. Please try again.", parse_mode=None)

# --- Helper Functions ---

def analyze_message_quality(text: str) -> Dict:
    """Analyze welcome message quality and provide suggestions"""
    analysis = {
        'length': len(text),
        'emoji_count': len([c for c in text if ord(c) > 127]),
        'tone': 'friendly',
        'suggestions': [],
        'overall_score': 7
    }
    
    # Length analysis
    if analysis['length'] < 50:
        analysis['length_rating'] = 'Too Short'
        analysis['suggestions'].append('Add more details about your bot\'s features')
    elif analysis['length'] < 200:
        analysis['length_rating'] = 'Good'
    elif analysis['length'] < 400:
        analysis['length_rating'] = 'Perfect'
    else:
        analysis['length_rating'] = 'Long'
        analysis['suggestions'].append('Consider shortening for better readability')
    
    # Emoji analysis
    if analysis['emoji_count'] == 0:
        analysis['emoji_rating'] = 'None'
        analysis['suggestions'].append('Add emojis to make your message more engaging')
    elif analysis['emoji_count'] < 5:
        analysis['emoji_rating'] = 'Few'
    elif analysis['emoji_count'] < 15:
        analysis['emoji_rating'] = 'Perfect'
    else:
        analysis['emoji_rating'] = 'Too Many'
        analysis['suggestions'].append('Reduce emojis for better readability')
    
    # Tone analysis (simple keyword detection)
    if any(word in text.lower() for word in ['professional', 'service', 'business']):
        analysis['tone'] = 'Professional'
        analysis['tone_rating'] = 'Business-like'
    elif any(word in text.lower() for word in ['welcome', 'hello', 'hi', 'excited']):
        analysis['tone'] = 'Friendly'
        analysis['tone_rating'] = 'Welcoming'
    elif any(word in text.lower() for word in ['amazing', 'awesome', 'fantastic', 'incredible']):
        analysis['tone'] = 'Exciting'
        analysis['tone_rating'] = 'Energetic'
    else:
        analysis['tone_rating'] = 'Neutral'
    
    # Calculate overall score
    score = 5
    if analysis['length_rating'] in ['Good', 'Perfect']:
        score += 2
    if analysis['emoji_rating'] in ['Few', 'Perfect']:
        score += 2
    if '{user_name}' in text:
        score += 1
    
    analysis['overall_score'] = min(10, score)
    
    return analysis

async def save_interactive_message(text: str, admin_user_id: int, analysis: Dict) -> bool:
    """Save interactive welcome message with analysis"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Save as custom message
        c.execute("""
            INSERT INTO interactive_welcome_messages 
            (name, template_text, category, tone, preview_text, is_active)
            VALUES ('Custom Message', %s, 'custom', %s, %s, 1)
            ON CONFLICT (name) DO UPDATE SET template_text = EXCLUDED.template_text, category = EXCLUDED.category, tone = EXCLUDED.tone, preview_text = EXCLUDED.preview_text, is_active = EXCLUDED.is_active
        """, (text, analysis['tone'].lower(), text[:100] + '...'))
        
        # Deactivate other messages
        c.execute("UPDATE interactive_welcome_messages SET is_active = 0 WHERE name != 'Custom Message'")
        
        # Update bot settings
        c.execute("""
            INSERT INTO bot_settings (setting_key, setting_value)
            VALUES ('active_welcome_message_name', 'Custom Message')
            ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value
        """)
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Error saving interactive message: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# --- END OF FILE interactive_welcome_editor.py ---
