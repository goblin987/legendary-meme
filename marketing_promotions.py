"""
Marketing and Promotions System with Customizable UI Designs
Provides different bot interface themes and user experience flows
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes
from utils import get_db_connection, send_message_with_retry, is_primary_admin, get_translation, WEBHOOK_URL
from datetime import datetime, timezone

# Import worker permissions
try:
    from worker_management import is_worker, check_worker_permission
except ImportError:
    def is_worker(uid): return False
    def check_worker_permission(uid, perm): return False

logger = logging.getLogger(__name__)

# UI Theme Constants
UI_THEMES = {
    'minimalist': {
        'name': 'Minimalist (Apple Style)',
        'description': 'Clean, minimal interface with centered text and bold formatting',
        'welcome_buttons': [
            ['🛍️ Shop', '👤 Profile', '💳 Top Up']
        ],
        'style': 'minimal'
    },
    'classic': {
        'name': 'Classic (Full Featured)',
        'description': 'Traditional bot interface with comprehensive options',
        'welcome_buttons': [
            ['🛍️ Shop', '📊 Categories'],
            ['👤 Profile', '💳 Balance', '🎁 Promotions'],
            ['ℹ️ Help', '📞 Support']
        ],
        'style': 'classic'
    },
    'modern': {
        'name': 'Modern (Card Style)',
        'description': 'Modern card-based interface with visual elements',
        'welcome_buttons': [
            ['🛍️ Shop Now', '🔥 Hot Deals'],
            ['👤 My Account', '💰 Wallet'],
            ['🎯 Promotions', '📱 App']
        ],
        'style': 'modern'
    }
}

def init_marketing_tables():
    """Initialize marketing and UI theme tables"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        logger.info("🎨 Initializing marketing and UI theme tables...")
        
        # UI Themes configuration table
        c.execute('''CREATE TABLE IF NOT EXISTS ui_themes (
            id SERIAL PRIMARY KEY,
            theme_name TEXT NOT NULL UNIQUE,
            is_active BOOLEAN DEFAULT FALSE,
            welcome_message TEXT,
            button_layout TEXT,
            style_config TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # User UI preferences
        c.execute('''CREATE TABLE IF NOT EXISTS user_ui_preferences (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            theme_name TEXT NOT NULL,
            custom_settings TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # Marketing campaigns
        c.execute('''CREATE TABLE IF NOT EXISTS marketing_campaigns (
            id SERIAL PRIMARY KEY,
            campaign_name TEXT NOT NULL,
            campaign_type TEXT NOT NULL,
            target_audience TEXT,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            config_data TEXT,
            created_by_admin_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # Promotion codes
        c.execute('''CREATE TABLE IF NOT EXISTS promotion_codes (
            id SERIAL PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            discount_type TEXT NOT NULL, -- 'percentage', 'fixed_amount'
            discount_value DECIMAL(10,2) NOT NULL,
            min_purchase_amount DECIMAL(10,2) DEFAULT 0,
            max_uses INTEGER DEFAULT NULL,
            current_uses INTEGER DEFAULT 0,
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_until TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            created_by_admin_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # Promotion usage tracking
        c.execute('''CREATE TABLE IF NOT EXISTS promotion_usage (
            id SERIAL PRIMARY KEY,
            promotion_id INTEGER NOT NULL,
            user_id BIGINT NOT NULL,
            order_id INTEGER,
            discount_applied DECIMAL(10,2),
            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # Hot deals management table
        c.execute('''CREATE TABLE IF NOT EXISTS hot_deals (
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL,
            deal_title TEXT,
            deal_description TEXT,
            discount_percentage REAL DEFAULT 0,
            original_price REAL,
            deal_price REAL,
            quantity_limit INTEGER DEFAULT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            priority INTEGER DEFAULT 0,
            created_by BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        )''')
        
        # Create indexes for hot deals
        c.execute("CREATE INDEX IF NOT EXISTS idx_hot_deals_active ON hot_deals(is_active, priority)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_hot_deals_product ON hot_deals(product_id)")
        
        # App info management table
        c.execute('''CREATE TABLE IF NOT EXISTS app_info (
            id SERIAL PRIMARY KEY,
            info_title TEXT NOT NULL,
            info_content TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            display_order INTEGER DEFAULT 0,
            created_by BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # Create index for app info
        c.execute("CREATE INDEX IF NOT EXISTS idx_app_info_active ON app_info(is_active, display_order)")
        
        # Bot layout customization tables
        c.execute('''CREATE TABLE IF NOT EXISTS bot_layout_templates (
            id SERIAL PRIMARY KEY,
            template_name TEXT NOT NULL UNIQUE,
            template_description TEXT,
            layout_config TEXT NOT NULL,
            is_preset BOOLEAN DEFAULT FALSE,
            is_active BOOLEAN DEFAULT FALSE,
            created_by BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS bot_menu_layouts (
            id SERIAL PRIMARY KEY,
            menu_name TEXT NOT NULL UNIQUE,
            menu_display_name TEXT NOT NULL,
            button_layout TEXT NOT NULL,
            header_message TEXT DEFAULT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            template_id INTEGER,
            created_by BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # Create indexes for bot layout
        c.execute("CREATE INDEX IF NOT EXISTS idx_layout_templates_active ON bot_layout_templates(is_active)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_menu_layouts_active ON bot_menu_layouts(is_active, menu_name)")
        
        # Add unique constraint to menu_name if it doesn't exist (for existing databases)
        try:
            c.execute("ALTER TABLE bot_menu_layouts ADD CONSTRAINT unique_menu_name UNIQUE (menu_name)")
            conn.commit()  # Commit this change immediately
            logger.info("✅ Added unique constraint to bot_menu_layouts.menu_name")
        except Exception as e:
            conn.rollback()  # Rollback failed constraint addition
            if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                pass  # Silently ignore - constraint already exists
            else:
                logger.warning(f"⚠️ Could not add unique constraint to menu_name: {e}")
            # Continue with initialization
        
        # Add header_message column if it doesn't exist
        try:
            c.execute("ALTER TABLE bot_menu_layouts ADD COLUMN header_message TEXT DEFAULT NULL")
            conn.commit()
            logger.info("✅ Added header_message column to bot_menu_layouts")
        except Exception as e:
            conn.rollback()
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                pass
            else:
                logger.warning(f"⚠️ Could not add header_message column: {e}")
        
        # Add quantity_limit column to hot_deals if it doesn't exist
        try:
            c.execute("ALTER TABLE hot_deals ADD COLUMN quantity_limit INTEGER DEFAULT NULL")
            conn.commit()
            logger.info("✅ Added quantity_limit column to hot_deals")
        except Exception as e:
            conn.rollback()
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                pass
            else:
                logger.warning(f"⚠️ Could not add quantity_limit column: {e}")
        
        # Insert default themes if not exists (with proper error handling)
        try:
            for theme_key, theme_data in UI_THEMES.items():
                try:
                    c.execute("""
                        INSERT INTO ui_themes (theme_name, is_active, welcome_message, button_layout, style_config)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (theme_name) DO NOTHING
                    """, (
                        theme_key,
                        theme_key == 'classic',  # Set classic as default active
                        f"Welcome to our store! 🛍️\n\nChoose an option below:",
                        str(theme_data['welcome_buttons']),
                        str(theme_data)
                    ))
                except Exception as theme_error:
                    logger.warning(f"⚠️ Could not insert theme {theme_key}: {theme_error}")
                    conn.rollback()
                    continue
        except Exception as themes_error:
            logger.warning(f"⚠️ Error processing themes: {themes_error}")
            conn.rollback()
        
        # MODE: Create hot deals settings table for simple admin controls
        c.execute('''CREATE TABLE IF NOT EXISTS hot_deals_settings (
            id SERIAL PRIMARY KEY,
            setting_name TEXT NOT NULL UNIQUE,
            setting_value BOOLEAN DEFAULT TRUE,
            updated_by BIGINT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # Insert default setting for automatic deals (enabled by default)
        c.execute("""
            INSERT INTO hot_deals_settings (setting_name, setting_value)
            VALUES ('auto_deals_enabled', TRUE)
            ON CONFLICT (setting_name) DO NOTHING
        """)
        
        conn.commit()
        logger.info("✅ Marketing and UI theme tables initialized successfully")
        
    except Exception as e:
        logger.error(f"❌ Error initializing marketing tables: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def get_active_ui_theme():
    """Get the currently active UI theme"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # MODE: Check ui_themes table FIRST (preset themes take priority over custom layouts)
        c.execute("""
            SELECT theme_name, welcome_message, button_layout, style_config
            FROM ui_themes 
            WHERE is_active = TRUE
            LIMIT 1
        """)
        result = c.fetchone()
        
        if result:
            logger.info(f"Using preset theme: {result['theme_name']}")
            return {
                'theme_name': result['theme_name'],
                'welcome_message': result['welcome_message'],
                'button_layout': eval(result['button_layout']) if result['button_layout'] else [],
                'style_config': eval(result['style_config']) if result['style_config'] else {}
            }
        
        # If no preset theme, check if there are custom layouts
        c.execute("""
            SELECT COUNT(*) as count FROM bot_menu_layouts WHERE is_active = TRUE
        """)
        custom_layouts_count = c.fetchone()['count']
        
        if custom_layouts_count > 0:
            # Custom layouts exist, use custom theme
            logger.info("Using custom layout")
            return {
                'theme_name': 'custom',
                'welcome_message': "Custom layout active",
                'button_layout': [],
                'style_config': {'type': 'custom'}
            }
        else:
            # Default to classic theme if nothing is set
            return {
                'theme_name': 'classic',
                'welcome_message': "Welcome to our store! 🛍️\n\nChoose an option below:",
                'button_layout': [['🛍️ Shop', '👤 Profile', '💳 Top Up']],
                'style_config': UI_THEMES['classic']
            }
            
    except Exception as e:
        logger.error(f"Error getting active UI theme: {e}")
        return {
            'theme_name': 'classic',
            'welcome_message': "Welcome to our store! 🛍️\n\nChoose an option below:",
            'button_layout': [['🛍️ Shop', '👤 Profile', '💳 Top Up']],
            'style_config': UI_THEMES['classic']
        }
    finally:
        if conn:
            conn.close()

# --- Admin UI Theme Management ---

async def handle_marketing_promotions_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main marketing and promotions management menu"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check permissions
    is_admin = is_primary_admin(user_id)
    is_auth_worker = is_worker(user_id) and check_worker_permission(user_id, 'marketing')
    
    if not is_admin and not is_auth_worker:
        return await query.answer("Access denied.", show_alert=True)
    
    # Get current active theme
    active_theme = get_active_ui_theme()
    
    msg = "🎨 **Marketing & Promotions**\n\n"
    msg += "**Manage your bot's appearance and promotional campaigns:**\n\n"
    msg += f"🎯 **Current UI Theme:** {active_theme['theme_name'].title()}\n"
    msg += f"📱 **Style:** {active_theme['style_config'].get('description', 'Custom theme')}\n\n"
    msg += "**Available Options:**"
    
    keyboard = [
        [InlineKeyboardButton("🎨 UI Theme Designer", callback_data="ui_theme_designer")],
        [InlineKeyboardButton("🎛️ Edit Bot Look", callback_data="bot_look_custom")],
        [InlineKeyboardButton("🔥 Hot Deals Manager", callback_data="admin_hot_deals_menu")],
        [InlineKeyboardButton("ℹ️ App Info Manager", callback_data="admin_app_info_menu")],
        [InlineKeyboardButton("🎁 Promotion Codes", callback_data="promotion_codes_menu")],
        [InlineKeyboardButton("📊 Marketing Campaigns", callback_data="marketing_campaigns_menu")],
        [InlineKeyboardButton("👀 Preview Current Theme", callback_data="preview_current_theme")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_ui_theme_designer(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Professional Theme Management Interface with Card-Based Design"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get currently active theme
    active_theme = get_active_ui_theme()
    active_theme_name = active_theme.get('theme_name', 'classic') if active_theme else 'classic'
    
    # MODE: Determine what's actually active (preset vs custom)
    conn_check = None
    actually_active_custom_template_id = None
    try:
        conn_check = get_db_connection()
        c_check = conn_check.cursor()
        
        # Check if any custom template is truly active
        c_check.execute("SELECT id FROM bot_layout_templates WHERE is_active = TRUE LIMIT 1")
        active_custom = c_check.fetchone()
        if active_custom:
            actually_active_custom_template_id = active_custom['id']
            active_theme_name = 'custom'  # Override to show custom as active
            
    except Exception as e:
        logger.error(f"Error checking active custom theme: {e}")
    finally:
        if conn_check:
            conn_check.close()
    
    msg = "🎨 **THEME MANAGEMENT CENTER** 🎨\n\n"
    msg += f"**Currently Active:** `{active_theme_name.upper()}`\n\n"
    
    keyboard = []
    
    # SYSTEM PRESETS (Clean format)
    msg += "**🔧 SYSTEM PRESETS**\n\n"
    
    system_themes = [
        ('classic', 'CLASSIC', 'Traditional 6-button layout'),
        ('minimalist', 'MINIMALIST', 'Clean 3-button layout'),
        ('modern', 'MODERN', 'Premium card-style layout')
    ]
    
    for theme_key, theme_name, theme_desc in system_themes:
        is_active = active_theme_name == theme_key
        
        # Single line format with checkmark on button
        msg += f"**{theme_name}** - *{theme_desc}*\n"
        
        if is_active:
            # Active theme - show checkmark on the theme button itself
            keyboard.append([
                InlineKeyboardButton(f"✅ {theme_name}", callback_data="theme_noop"),
                InlineKeyboardButton("✏️ EDIT", callback_data=f"edit_preset_theme|{theme_key}")
            ])
        else:
            # Inactive theme - normal buttons
            keyboard.append([
                InlineKeyboardButton(f"📋 {theme_name}", callback_data=f"select_ui_theme|{theme_key}"),
                InlineKeyboardButton("✏️ EDIT", callback_data=f"edit_preset_theme|{theme_key}")
            ])
        
        msg += "\n"
    
    # CUSTOM THEMES (Clean format)
    msg += "**🎨 CUSTOM THEMES**\n\n"
    
    # Load custom templates from database
    conn = None
    custom_themes_found = False
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT id, template_name, template_description, is_active, created_at
            FROM bot_layout_templates 
            WHERE is_preset = FALSE
            ORDER BY created_at DESC
            LIMIT 10
        """)
        
        custom_templates = c.fetchall()
        
        if custom_templates:
            custom_themes_found = True
            for template in custom_templates:
                template_name = template['template_name']
                description = template['template_description'] or "Custom layout"
                template_id = template['id']
                
                # MODE: Check if THIS template is the actually active one
                is_actually_active = (actually_active_custom_template_id == template_id)
                
                # Single line format like system presets
                msg += f"**{template_name}** - *{description}*\n"
                
                if is_actually_active:
                    # Active custom theme - checkmark on theme button, edit only
                    keyboard.append([
                        InlineKeyboardButton(f"✅ {template_name}", callback_data="theme_noop"),
                        InlineKeyboardButton("✏️ EDIT", callback_data=f"edit_custom_theme|{template_id}"),
                        InlineKeyboardButton("🗑️ DELETE", callback_data=f"confirm_delete_theme|{template_id}|{template_name}")
                    ])
                else:
                    # Inactive custom theme - all three buttons in same row
                    keyboard.append([
                        InlineKeyboardButton(f"🎨 {template_name}", callback_data=f"select_custom_template|{template_id}"),
                        InlineKeyboardButton("✏️ EDIT", callback_data=f"edit_custom_theme|{template_id}"),
                        InlineKeyboardButton("🗑️ DELETE", callback_data=f"confirm_delete_theme|{template_id}|{template_name}")
                    ])
                
                msg += "\n"
    
    except Exception as e:
        logger.error(f"Error loading custom templates: {e}")
        msg += "⚠️ *Error loading custom themes*\n\n"
    finally:
        if conn:
            conn.close()
    
    if not custom_themes_found:
        msg += "📝 *No custom themes created yet*\n"
        msg += "*Use the layout editor below to create your first custom theme*\n\n"
    
    # ═══════════════════════════════════════════════════════════════
    # CREATION AND NAVIGATION SECTION
    # ═══════════════════════════════════════════════════════════════
    keyboard.extend([
        [InlineKeyboardButton("🎛️ CREATE NEW CUSTOM THEME", callback_data="admin_bot_look_editor")],
        [InlineKeyboardButton("📱 PREVIEW ACTIVE THEME", callback_data="preview_active_theme")],
        [InlineKeyboardButton("⬅️ BACK TO MARKETING", callback_data="marketing_promotions_menu")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_confirm_delete_theme(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show delete confirmation modal for custom themes"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params or len(params) < 2:
        await query.answer("Invalid delete request", show_alert=True)
        return
    
    template_id = params[0]
    template_name = params[1]
    
    msg = "🗑️ **DELETE CONFIRMATION** 🗑️\n\n"
    msg += f"**Are you sure you want to delete:**\n"
    msg += f"`{template_name}`\n\n"
    msg += "⚠️ **WARNING:** This action cannot be undone!\n"
    msg += "The custom theme will be permanently removed.\n\n"
    msg += "**What happens next:**\n"
    msg += "• Theme will be deleted from the system\n"
    msg += "• If this theme is currently active, the system will switch to Classic theme\n"
    msg += "• All layout configurations will be lost\n\n"
    
    keyboard = [
        [InlineKeyboardButton("❌ CANCEL", callback_data="ui_theme_designer")],
        [InlineKeyboardButton("🗑️ YES, DELETE PERMANENTLY", callback_data=f"execute_delete_theme|{template_id}|{template_name}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_execute_delete_theme(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Execute the deletion of a custom theme after confirmation"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params or len(params) < 2:
        await query.answer("Invalid delete request", show_alert=True)
        return
    
    template_id = params[0]
    template_name = params[1]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check if this theme is currently active
        c.execute("SELECT is_active FROM bot_layout_templates WHERE id = %s", (template_id,))
        template = c.fetchone()
        
        if not template:
            await query.answer("Theme not found", show_alert=True)
            return await handle_ui_theme_designer(update, context)
        
        was_active = template['is_active']
        
        # Delete the custom theme
        c.execute("DELETE FROM bot_layout_templates WHERE id = %s", (template_id,))
        c.execute("DELETE FROM bot_menu_layouts WHERE template_id = %s", (template_id,))
        
        # If deleted theme was active, activate classic theme
        if was_active:
            c.execute("UPDATE ui_themes SET is_active = FALSE")  # Clear all
            c.execute("UPDATE ui_themes SET is_active = TRUE WHERE theme_name = 'classic'")  # Activate classic
            
            # Ensure classic theme exists in ui_themes
            c.execute("SELECT COUNT(*) as count FROM ui_themes WHERE theme_name = 'classic'")
            if c.fetchone()['count'] == 0:
                c.execute("""
                    INSERT INTO ui_themes (theme_name, is_active, welcome_message, button_layout, style_config)
                    VALUES ('classic', TRUE, 'Welcome to our store! 🛍️\n\nChoose an option below:', 
                    '[[\"🛍️ Shop\"], [\"👤 Profile\", \"💳 Top Up\"], [\"📝 Reviews\", \"📋 Price List\", \"🌐 Language\"]]',
                    '{\"type\": \"classic\"}')
                """)
        
        conn.commit()
        
        success_msg = f"✅ **THEME DELETED SUCCESSFULLY**\n\n"
        success_msg += f"**Deleted:** `{template_name}`\n\n"
        if was_active:
            success_msg += "🔄 **System automatically switched to Classic theme**\n\n"
        success_msg += "Returning to Theme Management Center..."
        
        await query.edit_message_text(success_msg, parse_mode='Markdown')
        
        # Wait 2 seconds then return to theme designer
        import asyncio
        await asyncio.sleep(2)
        await handle_ui_theme_designer(update, context)
        
    except Exception as e:
        logger.error(f"Error deleting custom theme {template_id}: {e}")
        await query.answer("❌ Error deleting theme", show_alert=True)
        await handle_ui_theme_designer(update, context)
    finally:
        if conn:
            conn.close()

async def handle_edit_preset_theme(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit a preset theme by loading it into the custom editor with existing buttons"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid theme selection", show_alert=True)
        return
    
    theme_key = params[0]
    
    # Pre-load the preset theme layout into the editor context
    context.user_data['editing_preset_theme'] = theme_key
    
    # Get the preset layout and pre-load it
    preset_layouts = {
        'classic': [
            ['🛍️ Shop'],
            ['👤 Profile', '💳 Top Up'], 
            ['📝 Reviews', '📋 Price List', '🌐 Language']
        ],
        'minimalist': [
            ['🛍️ Shop'], 
            ['👤 Profile', '💳 Top Up']
        ],
        'modern': [
            ['🛍️ Shop', '🔥 Hot Deals'], 
            ['👤 Profile', '💳 Top Up']
        ]
    }
    
    # Pre-load the layout into editor context
    if theme_key in preset_layouts:
        context.user_data['editing_layout_start_menu'] = preset_layouts[theme_key]
        context.user_data['current_header_start_menu'] = f"Welcome back, {{user_mention}}! 💰 Balance: {{balance:.2f}} EUR\n\nChoose an option:"
    
    # Redirect directly to the editor with pre-loaded layout
    await handle_bot_edit_menu(update, context, ['start_menu'])

async def handle_edit_custom_theme(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit a custom theme by loading it directly into the layout editor with saved buttons"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid theme selection", show_alert=True)
        return
    
    template_id = params[0]
    
    # Store the template being edited in context for the editor
    context.user_data['editing_custom_theme'] = template_id
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get template info and layout config
        c.execute("SELECT template_name, layout_config FROM bot_layout_templates WHERE id = %s", (template_id,))
        template = c.fetchone()
        
        if not template:
            await query.answer("Theme not found", show_alert=True)
            return await handle_ui_theme_designer(update, context)
        
        template_name = template['template_name']
        layout_config = template['layout_config']
        
        # MODE: Pre-load the saved layout into editor context
        import json
        try:
            parsed_config = json.loads(layout_config) if layout_config else {}
            
            # Load each menu's layout and header into editor context
            for menu_name, menu_data in parsed_config.items():
                if isinstance(menu_data, dict) and 'button_layout' in menu_data:
                    context.user_data[f'editing_layout_{menu_name}'] = menu_data['button_layout']
                    
                    # Load header message if available
                    if 'header_message' in menu_data:
                        context.user_data[f'editing_header_{menu_name}'] = menu_data['header_message']
                    elif menu_name == 'start_menu':
                        # Default header for start menu
                        context.user_data[f'editing_header_{menu_name}'] = f"Welcome back, {{user_mention}}! 💰 Balance: {{balance:.2f}} EUR\n\nChoose an option:"
            
            logger.info(f"Pre-loaded custom theme '{template_name}' into editor context")
            
        except Exception as parse_error:
            logger.error(f"Error parsing layout config for theme {template_id}: {parse_error}")
            # Continue anyway, editor will start with empty layout
        
        # Redirect directly to start menu editor with pre-loaded layout
        await handle_bot_edit_menu(update, context, ['start_menu'])
        
    except Exception as e:
        logger.error(f"Error loading custom theme for editing {template_id}: {e}")
        await query.answer("❌ Error loading theme", show_alert=True)
        await handle_ui_theme_designer(update, context)
    finally:
        if conn:
            conn.close()

async def handle_preview_active_theme(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show preview of the currently active theme"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get currently active theme
    active_theme = get_active_ui_theme()
    active_theme_name = active_theme.get('theme_name', 'classic') if active_theme else 'classic'
    
    msg = f"📱 **THEME PREVIEW** 📱\n\n"
    msg += f"**Currently Active:** `{active_theme_name.upper()}`\n\n"
    msg += "**Preview Description:**\n"
    
    if active_theme_name == 'classic':
        msg += "🏛️ **Classic Theme**\n"
        msg += "• 6-button traditional layout\n"
        msg += "• Shop (full width)\n"
        msg += "• Profile + Top Up (second row)\n"
        msg += "• Reviews + Price List + Language (third row)\n"
    elif active_theme_name == 'minimalist':
        msg += "✨ **Minimalist Theme**\n"
        msg += "• 3-button clean layout\n"
        msg += "• Shop (full width)\n"
        msg += "• Profile + Top Up (second row)\n"
    elif active_theme_name == 'modern':
        msg += "🚀 **Modern Theme**\n"
        msg += "• Premium card-style layout\n"
        msg += "• Enhanced visual appeal\n"
        msg += "• Hot deals integration\n"
    else:
        msg += "🎨 **Custom Theme**\n"
        msg += "• User-created layout\n"
        msg += "• Custom button arrangement\n"
    
    msg += f"\n**To see the live preview, type `/start` in the bot.**\n\n"
    
    keyboard = [
        [InlineKeyboardButton("⬅️ BACK TO THEMES", callback_data="ui_theme_designer")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_select_custom_template(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Select and activate a custom template"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid template selection", show_alert=True)
        return
    
    template_id = params[0]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get template details
        c.execute("SELECT template_name, layout_config FROM bot_layout_templates WHERE id = %s", (template_id,))
        template = c.fetchone()
        
        if not template:
            await query.answer("Template not found", show_alert=True)
            return
        
        # Deactivate all themes and templates
        c.execute("UPDATE ui_themes SET is_active = FALSE")
        c.execute("UPDATE bot_layout_templates SET is_active = FALSE")
        
        # Activate this template
        c.execute("UPDATE bot_layout_templates SET is_active = TRUE WHERE id = %s", (template_id,))
        
        conn.commit()
        
        await query.answer(f"✅ Template '{template['template_name']}' activated!", show_alert=True)
        
        # Refresh the UI theme designer
        await handle_ui_theme_designer(update, context)
        
    except Exception as e:
        logger.error(f"Error activating template {template_id}: {e}")
        await query.answer("❌ Error activating template", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_delete_custom_template(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Delete a custom template"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid template selection", show_alert=True)
        return
    
    template_id = params[0]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get template name for confirmation
        c.execute("SELECT template_name, is_active FROM bot_layout_templates WHERE id = %s", (template_id,))
        template = c.fetchone()
        
        if not template:
            await query.answer("Template not found", show_alert=True)
            return
        
        # If this template is active, switch to classic theme
        if template['is_active']:
            c.execute("UPDATE ui_themes SET is_active = FALSE")
            c.execute("UPDATE ui_themes SET is_active = TRUE WHERE theme_name = 'classic'")
        
        # Delete the template
        c.execute("DELETE FROM bot_layout_templates WHERE id = %s", (template_id,))
        
        conn.commit()
        
        await query.answer(f"✅ Template '{template['template_name']}' deleted!", show_alert=True)
        
        # Refresh the UI theme designer
        await handle_ui_theme_designer(update, context)
        
    except Exception as e:
        logger.error(f"Error deleting template {template_id}: {e}")
        await query.answer("❌ Error deleting template", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_select_ui_theme(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Select and activate a UI theme"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid theme selection", show_alert=True)
        return
    
    theme_name = params[0]
    
    if theme_name not in UI_THEMES:
        await query.answer("Theme not found", show_alert=True)
        return
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Deactivate all themes
        c.execute("UPDATE ui_themes SET is_active = FALSE")
        
        # Activate selected theme
        c.execute("""
            UPDATE ui_themes 
            SET is_active = TRUE, updated_at = CURRENT_TIMESTAMP
            WHERE theme_name = %s
        """, (theme_name,))
        
        # If theme doesn't exist, create it
        if c.rowcount == 0:
            theme_data = UI_THEMES[theme_name]
            c.execute("""
                INSERT INTO ui_themes (theme_name, is_active, welcome_message, button_layout, style_config)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                theme_name,
                True,
                f"Welcome to our store! 🛍️\n\nChoose an option below:",
                str(theme_data['welcome_buttons']),
                str(theme_data)
            ))
        
        conn.commit()
        
        theme_data = UI_THEMES[theme_name]
        msg = f"✅ **Theme Activated Successfully!**\n\n"
        msg += f"**Theme:** {theme_data['name']}\n"
        msg += f"**Style:** {theme_data['description']}\n\n"
        msg += "The new theme is now active for all users!"
        
        await query.answer("Theme activated!", show_alert=False)
        
    except Exception as e:
        logger.error(f"Error activating theme: {e}")
        if conn:
            conn.rollback()
        msg = "❌ **Error activating theme.** Please try again."
        await query.answer("Activation failed", show_alert=True)
    finally:
        if conn:
            conn.close()
    
    keyboard = [
        [InlineKeyboardButton("👀 Preview Theme", callback_data="preview_current_theme")],
        [InlineKeyboardButton("🔧 Customize Theme", callback_data="customize_active_theme")],
        [InlineKeyboardButton("⬅️ Back to Themes", callback_data="ui_theme_designer")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- Minimalist UI Implementation ---

def get_user_status_bar(total_purchases):
    """Generate status bar based on user's total purchases"""
    if total_purchases == 0:
        return "New 🟩⬜⬜⬜⬜⬜"
    elif total_purchases < 5:
        return "Beginner 🟩🟩⬜⬜⬜⬜"
    elif total_purchases < 15:
        return "Regular 🟩🟩🟩⬜⬜⬜"
    elif total_purchases < 30:
        return "Frequent 🟩🟩🟩🟩⬜⬜"
    elif total_purchases < 50:
        return "Loyal 🟩🟩🟩🟩🟩⬜"
    else:
        return "VIP 🟩🟩🟩🟩🟩🟩"

async def handle_classic_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle welcome message with classic UI theme (6-button layout)"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Get user's language preference
    user_language = context.user_data.get('lang', 'en')
    
    # Get user data for personalized welcome with full status
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Try query with first_name column, fallback if it doesn't exist
        try:
            c.execute("SELECT username, first_name, balance, total_purchases, basket, language FROM users WHERE user_id = %s", (user_id,))
            user_data = c.fetchone()
            has_first_name_col = True
        except Exception as e:
            logger.warning(f"first_name column query failed: {e}, trying without it")
            conn.rollback()
            c.execute("SELECT username, balance, total_purchases, basket, language FROM users WHERE user_id = %s", (user_id,))
            user_data = c.fetchone()
            has_first_name_col = False
        
        # DEBUG: Log what we got from database
        if user_data:
            db_username = user_data.get('username')
            db_first_name = user_data.get('first_name') if has_first_name_col else None
            logger.info(f"🔍 DB READ for user {user_id}: username={repr(db_username)}, first_name={repr(db_first_name)}")
        else:
            logger.warning(f"⚠️ No user_data found in DB for user {user_id}")
        
        # Build display name with proper fallback: first_name > @username > User_ID
        if user_data:
            # Priority: first_name (if column exists), then @username, then User_ID
            if has_first_name_col and user_data.get('first_name'):
                username = user_data['first_name']
                logger.info(f"✅ Using first_name for user {user_id}: {username}")
            elif user_data.get('username'):
                username = f"@{user_data['username']}"
                logger.info(f"✅ Using @username for user {user_id}: {username}")
            else:
                username = f"User_{user_id}"
                logger.warning(f"⚠️ No username/first_name in DB, falling back to User_ID for {user_id}")
        else:
            username = f"User_{user_id}"
        
        balance = user_data['balance'] if user_data else 0.0
        total_purchases = user_data['total_purchases'] if user_data else 0
        basket_str = user_data['basket'] if user_data else ""
        
        # Get language from database if not in context
        if not user_language and user_data and user_data['language']:
            user_language = user_data['language']
            context.user_data['lang'] = user_language
        
        # Count basket items
        basket_items = len(basket_str.split(',')) if basket_str and basket_str.strip() else 0
        
    except Exception as e:
        logger.error(f"Error getting user data for classic welcome: {e}")
        username = "User"
        balance = 0.0
        total_purchases = 0
        basket_items = 0
    finally:
        if conn:
            conn.close()
    
    # Get status bar based on purchases
    status_bar = get_user_status_bar(total_purchases)
    
    # Check for custom welcome message from admin (for both bot and miniapp modes)
    from utils import get_bot_setting
    ui_mode_check = get_bot_setting("ui_mode", "bot")
    
    # Try to get custom welcome message from database
    custom_welcome_text = None
    try:
        conn_temp = get_db_connection()
        c_temp = conn_temp.cursor()
        
        # First check if there's a specific miniapp welcome text (for miniapp mode)
        if ui_mode_check == "miniapp":
            custom_miniapp_welcome = get_bot_setting("miniapp_welcome_text", None)
            if custom_miniapp_welcome:
                custom_welcome_text = custom_miniapp_welcome
                logger.info(f"📱 CLASSIC: Using custom Mini App welcome text for user {user_id}")
        
        # If no custom miniapp text (or in bot mode), check for general custom welcome message
        if not custom_welcome_text:
            c_temp.execute("SELECT template_text FROM welcome_messages WHERE name = 'custom'")
            result = c_temp.fetchone()
            if result and result['template_text']:
                custom_welcome_text = result['template_text']
                logger.info(f"✏️ CLASSIC: Using custom welcome message from admin editor for user {user_id}")
        
        conn_temp.close()
    except Exception as e:
        logger.error(f"Error fetching custom welcome message: {e}")
    
    # Build welcome message
    if custom_welcome_text:
        # Use custom text with placeholder replacement
        msg = custom_welcome_text.format(
            username=username,
            balance=f"{balance:.2f}",
            total_purchases=total_purchases,
            basket_items=basket_items,
            status=status_bar
        )
    else:
        # Use default translated welcome message
        welcome_text = get_translation('welcome', user_language)
        status_text = get_translation('status_new', user_language)
        balance_text = get_translation('balance', user_language)
        total_purchases_text = get_translation('total_purchases', user_language)
        basket_items_text = get_translation('basket_items', user_language)
        
        msg = f"{welcome_text}, {username}!\n\n"
        msg += f"👤 {status_text.replace('New', status_bar).replace('Naujas', status_bar).replace('Новый', status_bar)}\n"
        msg += f"💰 {balance_text}: {balance:.2f} EUR\n"
        msg += f"🛒 {total_purchases_text}: {total_purchases}\n"
        msg += f"🛍️ {basket_items_text}: {basket_items}"
    
    # MODE: Hardcoded 6-button classic layout exactly as requested
    keyboard = []
    
    # Add admin panel button for admins at the top
    if is_primary_admin(user_id):
        keyboard.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="admin_menu")])
    
    # Add Web App Button
    import time
    webapp_url = f"{WEBHOOK_URL.rstrip('/')}/webapp_fresh/app.html?v=3.0&t={int(time.time())}"
    keyboard.append([InlineKeyboardButton(text="🛍️ Shop", web_app=WebAppInfo(url=webapp_url))])
    
    # Classic 6-button layout with translations
    shop_btn = get_translation('shop', user_language)
    profile_btn = get_translation('profile', user_language)
    top_up_btn = get_translation('top_up', user_language)
    reviews_btn = get_translation('reviews', user_language)
    price_list_btn = get_translation('price_list', user_language)
    language_btn = get_translation('language', user_language)
    
    # Check UI mode - Mini App Only mode shows ONLY the Shop button
    from utils import get_bot_setting
    ui_mode = get_bot_setting("ui_mode", "bot")
    logger.info(f"🎨 CLASSIC WELCOME: ui_mode={ui_mode} for user {user_id}")
    
    if ui_mode == "bot":
        # Full bot UI mode - show all buttons
        keyboard.extend([
            [InlineKeyboardButton(shop_btn, callback_data="shop")],
            [InlineKeyboardButton(profile_btn, callback_data="profile"), 
             InlineKeyboardButton(top_up_btn, callback_data="refill")],
            [InlineKeyboardButton(reviews_btn, callback_data="reviews"),
             InlineKeyboardButton(price_list_btn, callback_data="price_list"),
             InlineKeyboardButton(language_btn, callback_data="language")]
        ])
    else:
        # Mini App Only mode - show ONLY Shop button (already added above)
        logger.info(f"🎨 CLASSIC: Mini App Only mode - hiding ALL bot UI buttons, keeping only Shop button")
        # Don't add any additional buttons - just leave the Shop button
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=reply_markup)
        else:
            await send_message_with_retry(context.bot, chat_id, msg, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Error sending classic welcome message: {e}")
        # Fallback to regular message
        await send_message_with_retry(context.bot, chat_id, msg, reply_markup=reply_markup)

async def handle_minimalist_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle welcome message with minimalist UI theme"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Get active theme configuration
    theme = get_active_ui_theme()
    
    if theme['theme_name'] != 'minimalist':
        # Fallback to regular welcome if not minimalist theme
        return
    
    # Format minimalist welcome message
    msg = theme['welcome_message']
    
    # Create clean, centered button layout - 2 rows as requested
    keyboard = []
    
    # Add admin panel button for admins at the top
    if is_primary_admin(user_id):
        keyboard.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="admin_menu")])
    
    # Add Web App Button
    import time
    webapp_url = f"{WEBHOOK_URL.rstrip('/')}/webapp_fresh/app.html?v=3.0&t={int(time.time())}"
    keyboard.append([InlineKeyboardButton(text="🛍️ Shop", web_app=WebAppInfo(url=webapp_url))])
    
    # Add regular user buttons
    keyboard.extend([
        [InlineKeyboardButton("🛍️ Shop", callback_data="minimalist_shop")],
        [InlineKeyboardButton("👤 Profile", callback_data="minimalist_profile"), 
         InlineKeyboardButton("💳 Top Up", callback_data="minimalist_topup")]
    ])
    
    await send_message_with_retry(
        context.bot, chat_id, msg, 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode='Markdown'
    )

async def handle_minimalist_shop(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle shop button in minimalist UI - show city selection"""
    query = update.callback_query
    user_id = query.from_user.id
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all cities with product counts
        c.execute("""
            SELECT 
                city,
                COUNT(*) as product_count,
                COUNT(DISTINCT district) as district_count
            FROM products 
            WHERE available > 0
            GROUP BY city
            ORDER BY city
        """)
        cities = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading cities for minimalist shop: {e}")
        await query.answer("Error loading cities", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not cities:
        await query.edit_message_text(
            "🚫 **No Products Available**\n\nSorry, no products are currently available.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]]),
            parse_mode='Markdown'
        )
        return
    
    # Clean, minimalist city selection
    msg = "🏙️ **Choose a City**\n\n"
    msg += "**Select your location:**"
    
    keyboard = []
    for city in cities:
        city_name = city['city']
        product_count = city['product_count']
        district_count = city['district_count']
        
        # Clean button text without clutter
        button_text = f"🏙️ {city_name}"
        callback_data = f"minimalist_city_select|{city_name}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    # Add home button
    keyboard.append([InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_minimalist_city_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle city selection in minimalist UI - show districts with products"""
    query = update.callback_query
    if not params:
        await query.answer("Invalid city selection", show_alert=True)
        return
    
    city_name = params[0]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get districts with sample products and counts
        c.execute("""
            SELECT 
                district,
                COUNT(*) as product_count,
                MIN(product_type) as sample_product,
                MIN(size) as sample_size,
                MIN(price) as min_price
            FROM products 
            WHERE city = %s AND available > 0
            GROUP BY district
            ORDER BY district
        """, (city_name,))
        districts = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading districts for city {city_name}: {e}")
        await query.answer("Error loading districts", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not districts:
        await query.edit_message_text(
            f"🚫 **No Products in {city_name}**\n\nSorry, no products are available in this city.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Cities", callback_data="minimalist_shop")],
                [InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]
            ]),
            parse_mode='Markdown'
        )
        return
    
    # Clean, Apple-style district display
    msg = f"🏙️ **{city_name}**\n\n"
    
    # Show sample products for each district
    for district in districts[:3]:  # Show max 3 districts in preview
        district_name = district['district']
        sample_product = district['sample_product']
        sample_size = district['sample_size']
        min_price = district['min_price']
        
        msg += f"🏘️ **{district_name}:**\n"
        msg += f"    • 😃 **{sample_product} {sample_size}** ({min_price:.2f}€)\n\n"
    
    if len(districts) > 3:
        msg += f"... and {len(districts) - 3} more districts\n\n"
    
    msg += "**Choose a district:**"
    
    keyboard = []
    for district in districts:
        district_name = district['district']
        button_text = f"🏘️ {district_name}"
        callback_data = f"minimalist_district_select|{city_name}|{district_name}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    # Navigation buttons on same row as requested
    keyboard.append([
        InlineKeyboardButton("⬅️ Back to Cities", callback_data="minimalist_shop"),
        InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_minimalist_district_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle district selection - show products in clean grid layout as requested"""
    query = update.callback_query
    if not params or len(params) < 2:
        await query.answer("Invalid district selection", show_alert=True)
        return
    
    city_name = params[0]
    district_name = params[1]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all products in this district
        c.execute("""
            SELECT 
                product_type,
                size,
                price,
                available,
                id
            FROM products 
            WHERE city = %s AND district = %s AND available > 0
            ORDER BY product_type, price, size
        """, (city_name, district_name))
        products = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading products for {city_name} -> {district_name}: {e}")
        await query.answer("Error loading products", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not products:
        await query.edit_message_text(
            f"🚫 **No Products Available**\n\n**{city_name} → {district_name}**\n\nSorry, no products are currently available in this location.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Districts", callback_data=f"minimalist_city_select|{city_name}")],
                [InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]
            ]),
            parse_mode='Markdown'
        )
        return
    
    # Group products by type for grid layout
    product_groups = {}
    for product in products:
        product_type = product['product_type']
        if product_type not in product_groups:
            product_groups[product_type] = []
        product_groups[product_type].append(product)
    
    # Create header message
    msg = f"🏙️ **{city_name}** | 🏘️ **{district_name}**\n"
    msg += "**Select product type:**\n\n"
    
    keyboard = []
    
    # MODE: BACK TO SIMPLE PRODUCT NAME BUTTONS ONLY
    # Click product name → shows price/weight options
    
    # SAVED FOR SUPER-INTERACTIVE EDITOR (commented out):
    # This was the horizontal layout with product names + prices on same row
    # for product_type, type_products in product_groups.items():
    #     unique_products = {}
    #     for product in type_products:
    #         key = f"{product['price']:.2f}_{product['size']}"
    #         if key not in unique_products:
    #             unique_products[key] = product
    #     unique_products_list = list(unique_products.values())
    #     if unique_products_list:
    #         row = []
    #         emoji = get_product_emoji(product_type)
    #         product_name_btn = InlineKeyboardButton(f"{emoji} {product_type}", callback_data="ignore")
    #         row.append(product_name_btn)
    #         for product in unique_products_list:
    #             price_text = f"{product['price']:.0f}€ {product['size']}"
    #             option_btn = InlineKeyboardButton(price_text, callback_data=f"minimalist_product_select|{product['id']}")
    #             row.append(option_btn)
    #         keyboard.append(row)
    
    # CURRENT: Simple product name buttons only
    for product_type in product_groups.keys():
        emoji = get_product_emoji(product_type)
        product_btn = InlineKeyboardButton(
            f"{emoji} {product_type}",
            callback_data=f"minimalist_product_type|{city_name}|{district_name}|{product_type}"
        )
        keyboard.append([product_btn])
    
    # Navigation buttons
    keyboard.extend([
        [InlineKeyboardButton("⬅️ Back to Districts", callback_data=f"minimalist_city_select|{city_name}")],
        [InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_minimalist_product_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show product variants (sizes/weights) for selected type"""
    query = update.callback_query
    if not params or len(params) < 3:
        await query.answer("Invalid product selection", show_alert=True)
        return
    
    city_name = params[0]
    district_name = params[1]
    product_type = params[2]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all variants of this product type
        c.execute("""
            SELECT 
                id,
                size,
                price,
                available
            FROM products 
            WHERE city = %s AND district = %s AND product_type = %s AND available > 0
            ORDER BY price
        """, (city_name, district_name, product_type))
        variants = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading product variants: {e}")
        await query.answer("Error loading products", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not variants:
        await query.edit_message_text(
            f"🚫 **No Variants Available**\n\n**{product_type}** in **{district_name}**\n\nSorry, no variants are currently available.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back", callback_data=f"minimalist_district_select|{city_name}|{district_name}")],
                [InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]
            ]),
            parse_mode='Markdown'
        )
        return
    
    emoji = get_product_emoji(product_type)
    
    # Clean product selection display
    msg = f"🏙️ **{city_name}** | 🏘️ **{district_name}**\n\n"
    msg += f"{emoji} **{product_type}**\n\n"
    msg += "**Select size and price:**"
    
    keyboard = []
    
    # MODE: Deduplicate variants by size and price (like original version)
    unique_variants = {}
    for variant in variants:
        size = variant['size']
        price = variant['price']
        key = f"{size}_{price:.2f}"  # Create unique key for size+price combo
        
        # Only keep first occurrence of each size+price combination
        if key not in unique_variants:
            unique_variants[key] = variant
    
    # Create size/price buttons - wide buttons as requested
    for variant in unique_variants.values():
        size = variant['size']
        price = variant['price']
        available = variant['available']
        product_id = variant['id']
        
        # Wide button with size and price (clean, no markdown symbols)
        button_text = f"{size} - {price:.2f}€"
        callback_data = f"minimalist_product_select|{product_id}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    # Navigation buttons on last row together as requested
    keyboard.append([
        InlineKeyboardButton("⬅️ Back to District", callback_data=f"minimalist_district_select|{city_name}|{district_name}"),
        InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_minimalist_product_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show product details and purchase options"""
    query = update.callback_query
    if not params:
        await query.answer("Invalid product selection", show_alert=True)
        return
    
    # Handle both old format (product_id) and new format (city|district|type|size|price)
    if len(params) == 1:
        # Old format: just product_id
        product_id = int(params[0])
        
        # Get product details from database
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("""
                SELECT id, city, district, product_type, size, price, available, name, original_text
                FROM products WHERE id = %s
            """, (product_id,))
            product = c.fetchone()
        except Exception as e:
            logger.error(f"Error loading product {product_id}: {e}")
            await query.answer("Error loading product", show_alert=True)
            return
        finally:
            if conn:
                conn.close()
                
        if not product:
            await query.answer("Product not found", show_alert=True)
            return
            
        city_name = product['city']
        district_name = product['district']
        product_type = product['product_type']
        size = product['size']
        price = product['price']
        available = product['available']
        
    elif len(params) == 5:
        # New format: city|district|type|size|price
        city_name = params[0]
        district_name = params[1]
        product_type = params[2]
        size = params[3]
        price = float(params[4])
        
        # Find the specific product
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("""
                SELECT id, available, name, original_text
                FROM products 
                WHERE city = %s AND district = %s AND product_type = %s AND size = %s AND price = %s
                ORDER BY id LIMIT 1
            """, (city_name, district_name, product_type, size, price))
            product = c.fetchone()
        except Exception as e:
            logger.error(f"Error loading product {city_name}-{district_name}-{product_type}-{size}-{price}: {e}")
            await query.answer("Error loading product", show_alert=True)
            return
        finally:
            if conn:
                conn.close()
                
        if not product:
            await query.answer("Product not found", show_alert=True)
            return
            
        product_id = product['id']
        available = product['available']
    else:
        await query.answer("Invalid product selection format", show_alert=True)
        return
    
    # Get user balance
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT balance FROM users WHERE user_id = %s", (query.from_user.id,))
        user_result = c.fetchone()
        user_balance = user_result['balance'] if user_result else 0.0
        
    except Exception as e:
        logger.error(f"Error loading user balance: {e}")
        await query.answer("Error loading user data", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    # Store product selection for purchase
    context.user_data['selected_product_id'] = product_id
    product_dict = {
        'id': product_id,
        'city': city_name,
        'district': district_name,
        'product_type': product_type,
        'size': size,
        'price': price,
        'available': available
    }
    context.user_data['selected_product'] = product_dict
    
    emoji = get_product_emoji(product_type)
    
    # Clean, minimal product details as requested
    msg = f"🏙️ **{city_name}** | 🏘️ **{district_name}**\n\n"
    msg += f"{emoji} **{product_type}** - **{size}**\n\n"
    msg += f"💰 **Price:** **{price:.2f} EUR**\n"
    msg += f"🔢 **Available:** **{available}**\n\n"
    
    # Check if user has sufficient balance
    has_balance = user_balance >= price
    
    # 4-button layout as requested: Pay Now, Apply Discount, Back to Products, Home
    keyboard = [
        [InlineKeyboardButton("💳 Pay Now", callback_data=f"minimalist_pay_options|{product_id}")],
        [InlineKeyboardButton("🎫 Apply Discount Code", callback_data=f"minimalist_discount_code|{product_id}")],
        [InlineKeyboardButton("⬅️ Back to Products", callback_data=f"minimalist_district_select|{city_name}|{district_name}"),
         InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_minimalist_pay_options(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle Pay Now - direct payment processing without intermediate screen"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not params:
        await query.answer("Invalid payment selection", show_alert=True)
        return
    
    product_id = int(params[0])
    product = context.user_data.get('selected_product')
    
    if not product:
        await query.answer("Product selection expired", show_alert=True)
        return
    
    await query.answer("⏳ Processing payment...")
    
    # Check user balance first
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get user balance
        c.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
        user_result = c.fetchone()
        user_balance = user_result['balance'] if user_result else 0.0
        product_price = float(product['price'])
        
        if user_balance >= product_price:
            # User has sufficient balance - process payment directly
            await process_minimalist_balance_payment(query, context, product, user_balance)
        else:
            # User doesn't have sufficient balance - show crypto payment options
            await show_minimalist_crypto_options(query, context, product, user_balance)
            
    except Exception as e:
        logger.error(f"Error processing minimalist payment for user {user_id}: {e}")
        await query.answer("Payment error occurred", show_alert=True)
    finally:
        if conn:
            conn.close()

async def process_minimalist_balance_payment(query, context, product, user_balance):
    """Process payment directly using user's balance"""
    user_id = query.from_user.id
    product_price = float(product['price'])
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        
        # Reserve the product
        c.execute("""
            UPDATE products 
            SET reserved = reserved + 1 
            WHERE id = %s AND available > reserved
        """, (product['id'],))
        
        if c.rowcount == 0:
            await query.edit_message_text("❌ Sorry, this item was just taken by another user!", parse_mode=None)
            return
        
        # Deduct from user balance
        new_balance = user_balance - product_price
        c.execute("""
            UPDATE users 
            SET balance = %s, total_purchases = total_purchases + 1 
            WHERE user_id = %s
        """, (new_balance, user_id))
        
        # Record the purchase
        c.execute("""
            INSERT INTO purchases (user_id, product_id, price, payment_method, status)
            VALUES (%s, %s, %s, 'balance', 'completed')
        """, (user_id, product['id'], product_price))
        
        # Reduce product availability
        c.execute("""
            UPDATE products 
            SET available = available - 1, reserved = reserved - 1
            WHERE id = %s
        """, (product['id'],))
        
        conn.commit()
        
        # Send success message with product details
        await send_minimalist_success_message(query, context, product, new_balance)
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error processing balance payment: {e}")
        await query.edit_message_text("❌ Payment failed. Please try again.", parse_mode=None)
    finally:
        if conn:
            conn.close()

async def show_minimalist_crypto_options(query, context, product, user_balance):
    """Show crypto payment options using the original working function"""
    from user import SUPPORTED_CRYPTO, format_currency
    from utils import is_currency_supported
    
    # Set up context for basket payment (what handle_select_basket_crypto expects)
    product_price = float(product['price'])
    
    # Create basket-style snapshot for the single product
    basket_snapshot = [{
        'product_id': product['id'],
        'price': product_price,
        'name': product['product_type'],
        'size': product['size'],
        'product_type': product['product_type'],
        'city': product['city'],
        'district': product['district'],
        'original_text': product.get('original_text', '')
    }]
    
    # Set the context variables that handle_select_basket_crypto expects
    context.user_data['basket_pay_snapshot'] = basket_snapshot
    context.user_data['basket_pay_total_eur'] = product_price
    context.user_data['basket_pay_discount_code'] = None  # No discount for direct purchase
    
    # Build crypto buttons using the original working logic
    asset_buttons = []
    row = []
    supported_currencies = {}
    
    # Validate each currency against NOWPayments API (same as original)
    for code, display_name in SUPPORTED_CRYPTO.items():
        if is_currency_supported(code):
            supported_currencies[code] = display_name
        else:
            logger.warning(f"Currency {code} not supported by NOWPayments API")
    
    # If no currencies are supported, show error
    if not supported_currencies:
        logger.error("No supported currencies found from NOWPayments API")
        msg = "❌ No payment methods available at the moment. Please try again later."
        back_callback = f"minimalist_district_select|{product['city']}|{product['district']}"
        kb = [[InlineKeyboardButton("⬅️ Back", callback_data=back_callback)]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        return
    
    # Build buttons for supported currencies (same layout as original)
    for code, display_name in supported_currencies.items():
        row.append(InlineKeyboardButton(display_name, callback_data=f"select_basket_crypto|{code}"))
        if len(row) >= 3:  # 3 buttons per row like original
            asset_buttons.append(row)
            row = []
    if row: 
        asset_buttons.append(row)

    # Add back button - use the consistent navigation
    back_callback = f"minimalist_district_select|{product['city']}|{product['district']}"
    asset_buttons.append([InlineKeyboardButton("❌ Cancel", callback_data=back_callback)])

    # Use the original message format
    amount_str = format_currency(product_price)
    prompt_msg = f"Choose crypto to pay {amount_str} EUR for your items:"

    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(asset_buttons), parse_mode=None)

async def send_minimalist_success_message(query, context, product, new_balance):
    """Send success message after payment"""
    emoji = get_product_emoji(product['product_type'])
    
    msg = f"✅ **Payment Successful!**\n\n"
    msg += f"{emoji} **Product:** {product['product_type']} {product['size']}\n"
    msg += f"💰 **Paid:** {product['price']:.2f} EUR\n"
    msg += f"💳 **New Balance:** {new_balance:.2f} EUR\n\n"
    msg += f"📦 **Your Product Details:**\n"
    msg += f"🏙️ **Location:** {product['city']} → {product['district']}\n"
    msg += f"📱 **Order ID:** #{product['id']}\n\n"
    msg += "**Thank you for your purchase!** 🎉\n"
    msg += "Your product is ready for pickup at the specified location."
    
    keyboard = [
        [InlineKeyboardButton("🛍️ Shop More", callback_data="minimalist_shop")],
        [InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


async def handle_minimalist_discount_code(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle discount code application - redirect to existing system"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not params:
        await query.answer("Invalid discount request", show_alert=True)
        return
    
    product_id = int(params[0])
    product = context.user_data.get('selected_product')
    
    if not product:
        await query.answer("Product selection expired", show_alert=True)
        return
    
    # First, we need to set up the payment context that the existing discount system expects
    # The existing system expects single_item_pay_snapshot to be set
    
    # Convert to the format expected by the existing payment system
    city_name = product['city']
    district_name = product['district']
    product_type = product['product_type'] 
    size = product['size']
    price = str(product['price'])
    
    # Find city_id and district_id from the names
    city_id = None
    district_id = None
    
    # Import the existing data structures
    from utils import CITIES, DISTRICTS
    
    # Find city_id
    for c_id, c_name in CITIES.items():
        if c_name == city_name:
            city_id = c_id
            break
    
    # Find district_id
    if city_id:
        for d_id, d_name in DISTRICTS.get(city_id, {}).items():
            if d_name == district_name:
                district_id = d_id
                break
    
    if not city_id or not district_id:
        await query.answer("Location data error", show_alert=True)
        return
    
    # Set up the payment context that the existing discount system expects
    context.user_data['single_item_pay_back_params'] = [city_id, district_id, product_type, size, price]
    context.user_data['single_item_pay_final_eur'] = float(price)
    context.user_data['single_item_pay_snapshot'] = product
    
    # Use the existing discount system
    from user import handle_apply_discount_single_pay
    
    # Call the existing discount handler
    await handle_apply_discount_single_pay(update, context, params)


def get_custom_layout(menu_name):
    """Get custom button layout for a specific menu"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT button_layout 
            FROM bot_menu_layouts 
            WHERE menu_name = %s AND is_active = TRUE
            ORDER BY updated_at DESC
            LIMIT 1
        """, (menu_name,))
        
        result = c.fetchone()
        if result:
            import json
            return json.loads(result['button_layout'])
        
        return None  # No custom layout found
        
    except Exception as e:
        logger.error(f"Error loading custom layout for {menu_name}: {e}")
        return None
    finally:
        if conn:
            conn.close()

def translate_button_text(button_text, user_language='en'):
    """Translate button text based on user language"""
    from utils import get_translation
    
    # Map button text to translation keys
    button_translation_map = {
        '🛍️ Shop': 'shop',
        '👤 Profile': 'profile', 
        '💳 Top Up': 'top_up',
        '🎁 Daily Rewards': 'daily_rewards_menu',  # Daily Rewards & Case Opening
        '📝 Reviews': 'reviews',
        '💎 Price List': 'price_list',
        '🌍 Language': 'language',
        '🔥 Hot Deals': 'hot_deals',
        'ℹ️ Info': 'info',
        '🎁 Referral Code': 'referral_code',
        '💳 Pay Now': 'pay_now',
        '🎫 Discount Code': 'discount_code',
        '⬅️ Back': 'back',
        '🏠 Home': 'home'
    }
    
    # Check if we have a translation for this button
    translation_key = button_translation_map.get(button_text)
    if translation_key:
        return get_translation(translation_key, user_language)
    
    # If no translation found, return original text
    return button_text

def apply_custom_layout_to_keyboard(menu_name, default_keyboard, user_language='en'):
    """Apply custom layout to keyboard if available, otherwise return default"""
    custom_layout = get_custom_layout(menu_name)
    
    if not custom_layout:
        return default_keyboard  # Return original if no custom layout
    
    try:
        # Convert custom layout to InlineKeyboardMarkup
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        from utils import get_translation
        
        custom_keyboard = []
        for row in custom_layout:
            keyboard_row = []
            for button_text in row:
                # 🚀 MODE: TRANSLATE BUTTON TEXT!
                translated_text = translate_button_text(button_text, user_language)
                # Map button text to callback data
                callback_data = map_button_text_to_callback(button_text)
                keyboard_row.append(InlineKeyboardButton(translated_text, callback_data=callback_data))
            if keyboard_row:  # Only add non-empty rows
                custom_keyboard.append(keyboard_row)
        
        # Inject Web App Button for start menu (Always visible)
        if menu_name == 'start_menu':
             import time
             webapp_url = f"{WEBHOOK_URL.rstrip('/')}/webapp_fresh/app.html?v=3.0&t={int(time.time())}"
             custom_keyboard.insert(0, [InlineKeyboardButton(text="🌐 Open Shop App", web_app=WebAppInfo(url=webapp_url))])
        
        return custom_keyboard
        
    except Exception as e:
        logger.error(f"Error applying custom layout for {menu_name}: {e}")
        return default_keyboard  # Return original on error

def map_button_text_to_callback(button_text):
    """Map button text to appropriate callback data"""
    # Mapping of button text to callback data
    button_mapping = {
        # Start menu buttons
        '🛍️ Shop': 'shop',
        '👤 Profile': 'profile', 
        '💳 Top Up': 'topup',
        '🎁 Daily Rewards': 'daily_rewards_menu',  # Daily Rewards & Case Opening
        '💰 Wallet': 'wallet',
        '📊 Stats': 'stats',
        '🎮 Games': 'games',
        '🔥 Hot Deals': 'modern_deals',  # Hot Deals work with any UI theme
        'ℹ️ Info': 'info',
        '⚙️ Settings': 'settings',
        '🎁 Promotions': 'promotions',
        '📞 Support': 'support',
        '🏆 Leaderboard': 'leaderboard',
        
        # City menu buttons
        '🏙️ Vilnius': 'city_vilnius',
        '🏙️ Kaunas': 'city_kaunas', 
        '🏙️ Klaipeda': 'city_klaipeda',
        '🏙️ Siauliai': 'city_siauliai',
        
        # District menu buttons
        '🏘️ Centras': 'district_centras',
        '🏘️ Naujamestis': 'district_naujamestis',
        '🏘️ Senamiestis': 'district_senamiestis',
        
        # Payment menu buttons
        '💳 Pay Now': 'pay_now',
        '🎫 Discount Code': 'discount_code',
        '💰 Add to Wallet': 'add_wallet',
        '🛒 Add to Cart': 'add_cart',
        
        # Navigation buttons
        '⬅️ Back': 'back',
        '🏠 Home': 'home',
        '⬅️ Back to Cities': 'back_cities',
    }
    
    return button_mapping.get(button_text, 'noop')  # Default to noop if not found

def get_available_variables(menu_name):
    """Get available dynamic variables for a specific menu"""
    # Base variables available in all menus
    base_variables = {
        '{user_mention}': 'User\'s name or @username',
        '{user_first_name}': 'User\'s first name',
        '{user_id}': 'User\'s Telegram ID',
        '{balance}': 'User\'s current balance',
        '{total_purchases}': 'User\'s total purchase count'
    }
    
    # Menu-specific variables
    menu_variables = {
        'start_menu': {
            '{vip_level}': 'User\'s VIP level status',
            '{referral_code}': 'User\'s referral code',
            '{last_purchase}': 'Date of last purchase',
            '{basket_count}': 'Items in current basket'
        },
        'city_menu': {
            '{available_cities}': 'Number of available cities',
            '{selected_city}': 'Currently selected city'
        },
        'district_menu': {
            '{city_name}': 'Selected city name',
            '{available_districts}': 'Number of districts in city'
        },
        'payment_menu': {
            '{item_name}': 'Selected product name',
            '{item_price}': 'Product price',
            '{item_size}': 'Product size/weight',
            '{discount_available}': 'Available discount percentage'
        }
    }
    
    # Combine base and menu-specific variables
    all_variables = base_variables.copy()
    if menu_name in menu_variables:
        all_variables.update(menu_variables[menu_name])
    
    return all_variables

def get_default_header_message(menu_name):
    """Get default header message for a menu"""
    default_messages = {
        'start_menu': 'Welcome back, {user_mention}! 💰 Balance: {balance} EUR\n\nChoose an option:',
        'city_menu': '🏙️ Choose Your City\n\nSelect a city to browse products:',
        'district_menu': '🏘️ {city_name} Districts\n\nSelect a district:',
        'payment_menu': '💳 Payment Options\n\n{item_name} - {item_size}\nPrice: {item_price} EUR'
    }
    
    return default_messages.get(menu_name, f'{menu_name.replace("_", " ").title()}\n\nChoose an option:')

def process_dynamic_variables(message, user_data=None, context_data=None):
    """Process dynamic variables in a message"""
    if not message:
        return message
    
    # Default user data if not provided
    if not user_data:
        user_data = {
            'user_mention': 'User',
            'user_first_name': 'User',
            'user_id': '123456789',
            'balance': '0.00',
            'total_purchases': '0',
            'vip_level': 'Standard',
            'referral_code': 'REF123',
            'last_purchase': 'Never',
            'basket_count': '0'
        }
    
    # Default context data if not provided
    if not context_data:
        context_data = {
            'available_cities': '4',
            'selected_city': 'Vilnius',
            'city_name': 'Vilnius',
            'available_districts': '3',
            'item_name': 'Product',
            'item_price': '10.00',
            'item_size': '1g',
            'discount_available': '0'
        }
    
    # Combine all data
    all_data = {**user_data, **context_data}
    
    # Replace variables in message
    processed_message = message
    for variable, value in all_data.items():
        placeholder = f'{{{variable}}}'
        if placeholder in processed_message:
            processed_message = processed_message.replace(placeholder, str(value))
    
    # Process line breaks - convert \n to actual line breaks
    processed_message = processed_message.replace('\\n', '\n')
    
    return processed_message

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
        'vitamin': '💊'
    }
    
    product_lower = product_type.lower()
    for key, emoji in emoji_map.items():
        if key in product_lower:
            return emoji
    
    return '😃'  # Default emoji

# --- Additional handlers for other UI elements ---

async def handle_minimalist_home(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Return to minimalist home screen"""
    query = update.callback_query
    await handle_minimalist_welcome(update, context)

async def handle_minimalist_profile(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show minimalist profile screen"""
    query = update.callback_query
    user_id = query.from_user.id
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get user info
        c.execute("""
            SELECT balance, total_purchases
            FROM users 
            WHERE user_id = %s
        """, (user_id,))
        user = c.fetchone()
        
        if not user:
            balance = 0.0
            total_purchases = 0
            member_since = "New User"
        else:
            balance = user['balance']
            total_purchases = user['total_purchases'] or 0
            member_since = "Member"  # Since we don't have created_at column
        
    except Exception as e:
        logger.error(f"Error loading user profile: {e}")
        balance = 0.0
        total_purchases = 0
        member_since = "Unknown"
    finally:
        if conn:
            conn.close()
    
    msg = f"👤 **Your Profile**\n\n"
    msg += f"💰 **Balance:** **{balance:.2f} EUR**\n"
    msg += f"🛍️ **Total Orders:** **{total_purchases}**\n"
    msg += f"📅 **Member Since:** **{member_since}**\n\n"
    
    keyboard = [
        [InlineKeyboardButton("💳 Top Up Balance", callback_data="minimalist_topup")],
        [InlineKeyboardButton("📋 Order History", callback_data="minimalist_order_history")],
        [InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_minimalist_topup(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show minimalist top-up options"""
    query = update.callback_query
    
    msg = f"💳 **Top Up Balance**\n\n"
    msg += "**Select amount to add:**\n\n"
    
    amounts = [5, 10, 20, 50, 100]
    keyboard = []
    
    for amount in amounts:
        button_text = f"💰 {amount} EUR"
        callback_data = f"minimalist_topup_amount|{amount}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    keyboard.extend([
        [InlineKeyboardButton("💎 Custom Amount", callback_data="minimalist_custom_topup")],
        [InlineKeyboardButton("🏠 Home", callback_data="minimalist_home")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_user_ui_theme_selector(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Allow users to select their preferred UI theme"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Get current user preference
    current_theme = context.user_data.get('ui_theme_preference', 'classic')
    
    msg = "🎨 **Choose Your UI Theme**\n\n"
    msg += "**Select your preferred interface style:**\n\n"
    
    keyboard = []
    
    # Theme options for users
    themes = {
        'classic': {
            'name': '📱 Classic Interface',
            'description': 'Traditional bot interface with all features'
        },
        'minimalist': {
            'name': '🍎 Minimalist (Apple Style)',
            'description': 'Clean, simple interface with minimal buttons'
        }
    }
    
    for theme_key, theme_data in themes.items():
        status = " ✅" if theme_key == current_theme else ""
        button_text = f"{theme_data['name']}{status}"
        callback_data = f"user_select_theme|{theme_key}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    keyboard.extend([
        [InlineKeyboardButton("👀 Preview Minimalist", callback_data="user_preview_minimalist")],
        [InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_start")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_user_select_theme(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle user theme selection"""
    query = update.callback_query
    if not params:
        await query.answer("Invalid theme selection", show_alert=True)
        return
    
    theme_name = params[0]
    user_id = query.from_user.id
    
    # Store user preference
    context.user_data['ui_theme_preference'] = theme_name
    
    # Get theme info
    theme_names = {
        'classic': 'Classic Interface',
        'minimalist': 'Minimalist (Apple Style)'
    }
    
    selected_theme_name = theme_names.get(theme_name, theme_name)
    
    msg = f"✅ **Theme Selected!**\n\n"
    msg += f"**Active Theme:** {selected_theme_name}\n\n"
    
    if theme_name == 'minimalist':
        msg += "🍎 **Minimalist Features:**\n"
        msg += "• Clean, Apple-style interface\n"
        msg += "• Simplified navigation\n"
        msg += "• Bold text formatting\n"
        msg += "• Centered layout design\n\n"
        msg += "Your interface will switch to minimalist mode when you return to the main menu."
    else:
        msg += "📱 **Classic Features:**\n"
        msg += "• Full-featured interface\n"
        msg += "• All bot functions accessible\n"
        msg += "• Traditional button layout\n"
        msg += "• Comprehensive options\n\n"
        msg += "Your interface will use the classic mode."
    
    keyboard = [
        [InlineKeyboardButton("🏠 Apply Theme (Go to Main Menu)", callback_data="back_start")],
        [InlineKeyboardButton("🎨 Change Theme", callback_data="user_ui_theme_selector")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    await query.answer(f"Theme changed to {selected_theme_name}!", show_alert=False)

async def handle_user_preview_minimalist(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show preview of minimalist theme without switching"""
    query = update.callback_query
    
    msg = "👀 **Minimalist Theme Preview**\n\n"
    msg += "**🍎 Apple-Style Interface:**\n\n"
    msg += "```\nWelcome to our store! 🛍️\n\nChoose an option below:\n\n[🛍️ Shop] [👤 Profile] [💳 Top Up]\n```\n\n"
    msg += "**🛍️ Shopping Flow:**\n"
    msg += "```\n🏙️ Choose a City\n\nSelect your location:\n\n[🏙️ Klaipeda]\n[🏙️ Vilnius]\n\n[🏠 Home]\n```\n\n"
    msg += "**📱 Product Display:**\n"
    msg += "```\n🏙️ klaipeda | 🏘️ naujamestis\n\n☕ kava - 2g\n\n💰 Price: 2.50 EUR\n🔢 Available: 1\n\n[💳 Pay Now]\n```\n\n"
    msg += "**Features:**\n"
    msg += "• Clean, centered layout\n"
    msg += "• Bold formatting for prices\n"
    msg += "• Wide buttons for options\n"
    msg += "• Minimal, distraction-free design"
    
    keyboard = [
        [InlineKeyboardButton("✅ Use Minimalist Theme", callback_data="user_select_theme|minimalist")],
        [InlineKeyboardButton("⬅️ Back to Theme Selection", callback_data="user_ui_theme_selector")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# MODE: MODERN CARD STYLE UI IMPLEMENTATION
# Full creative freedom - premium visual experience with card-style design

async def handle_modern_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern card-style welcome interface with premium visual design"""
    query = update.callback_query if update.callback_query else None
    user_id = update.effective_user.id
    
    # Get user data for personalized welcome
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Try query with first_name column, fallback if it doesn't exist
        try:
            c.execute("SELECT username, first_name, balance FROM users WHERE user_id = %s", (user_id,))
            user_data = c.fetchone()
            has_first_name_col = True
        except Exception:
            conn.rollback()
            c.execute("SELECT username, balance FROM users WHERE user_id = %s", (user_id,))
            user_data = c.fetchone()
            has_first_name_col = False
        
        # Build display name with proper fallback: first_name > @username > VIP Member
        if user_data:
            if has_first_name_col and user_data.get('first_name'):
                username = user_data['first_name']
            elif user_data.get('username'):
                username = f"@{user_data['username']}"
            else:
                username = "VIP Member"
        else:
            username = "VIP Member"
            
        balance = user_data['balance'] if user_data else 0.0
    except Exception as e:
        logger.error(f"Error getting user data: {e}")
        username = "VIP Member"
        balance = 0.0
    finally:
        if conn:
            conn.close()
    
    # Premium modern welcome with user data (no ugly symbols)
    msg = f"🎯 **WELCOME TO PREMIUM STORE** 🎯\n\n"
    msg += f"👤 **Hello, {username}!**\n"
    msg += f"💰 **Balance:** {balance:.2f} EUR\n\n"
    msg += "🔥 **What brings you here today?**\n\n"
    msg += "💎 *Premium quality guaranteed*\n"
    msg += "🚀 *Lightning-fast delivery*\n"
    msg += "🏆 *VIP customer experience*"
    
    keyboard = []
    
    # Add admin panel for primary admins
    if is_primary_admin(user_id):
        keyboard.append([InlineKeyboardButton("🔧 Admin Control Center", callback_data="admin_menu")])
    
    # Add Web App Button
    import time
    webapp_url = f"{WEBHOOK_URL.rstrip('/')}/webapp_fresh/app.html?v=3.0&t={int(time.time())}"
    keyboard.append([InlineKeyboardButton(text="🛍️ Shop", web_app=WebAppInfo(url=webapp_url))])
    
    # Modern premium button layout
    keyboard.extend([
        [InlineKeyboardButton("🛍️ Shop Now", callback_data="modern_shop"),
         InlineKeyboardButton("🔥 Hot Deals", callback_data="modern_deals")],
        [InlineKeyboardButton("👤 My Account", callback_data="modern_profile"),
         InlineKeyboardButton("💳 Top Up", callback_data="modern_wallet")],
        [InlineKeyboardButton("🎯 Promotions", callback_data="modern_promotions"),
         InlineKeyboardButton("ℹ️ Info", callback_data="modern_app")]
    ])
    
    if query:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_shop(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern card-style shop interface with premium city selection"""
    query = update.callback_query
    
    msg = "🛍️ **PREMIUM MARKETPLACE** 🛍️\n\n"
    msg += "🏙️ **DELIVERY ZONES**\n\n"
    msg += "📍 *Select your premium delivery location*\n\n"
    msg += "🚀 **Express delivery available**\n"
    msg += "💎 **Premium service guarantee**"
    
    # Get cities from utils
    from utils import CITIES
    keyboard = []
    
    # Create premium city selection cards
    for city_id, city_name in CITIES.items():
        keyboard.append([InlineKeyboardButton(f"🏙️ {city_name.title()} Premium Zone", callback_data=f"modern_city_select|{city_name}")])
    
    keyboard.extend([
        [InlineKeyboardButton("⬅️ Back to Home", callback_data="modern_home")],
        [InlineKeyboardButton("🔥 Today's Specials", callback_data="modern_deals")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_city_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern card-style district selection with premium zones"""
    query = update.callback_query
    if not params:
        await query.answer("Invalid city selection", show_alert=True)
        return
    
    city_name = params[0]
    
    msg = f"🏙️ **{city_name.upper()} PREMIUM ZONE** 🏙️\n\n"
    msg += "🏘️ **SELECT DISTRICT**\n\n"
    msg += "📍 *Choose your premium district*\n\n"
    msg += "🎯 **VIP service in all areas**\n"
    msg += "⚡ **Same-day delivery available**"
    
    # Get districts from utils
    from utils import DISTRICTS
    keyboard = []
    
    city_districts = DISTRICTS.get(city_name, {})
    for district_id, district_name in city_districts.items():
        keyboard.append([InlineKeyboardButton(f"🏘️ {district_name} VIP Zone", callback_data=f"modern_district_select|{city_name}|{district_name}")])
    
    keyboard.extend([
        [InlineKeyboardButton("⬅️ Back to Cities", callback_data="modern_shop")],
        [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_district_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern card-style product catalog with premium presentation"""
    query = update.callback_query
    if not params or len(params) < 2:
        await query.answer("Invalid district selection", show_alert=True)
        return
    
    city_name = params[0]
    district_name = params[1]
    
    # Get products from database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT DISTINCT product_type FROM products 
            WHERE city = %s AND district = %s AND available > 0
            ORDER BY product_type
        """, (city_name, district_name))
        product_types = [row['product_type'] for row in c.fetchall()]
    except Exception as e:
        logger.error(f"Error loading products: {e}")
        await query.answer("Error loading premium catalog", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not product_types:
        await query.edit_message_text(
            f"🚫 **PREMIUM CATALOG UNAVAILABLE**\n\n"
            f"**{district_name.upper()}** - **{city_name.upper()}**\n\n"
            f"🔄 *Restocking premium items...*\n"
            f"📞 *Contact support for availability*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Districts", callback_data=f"modern_city_select|{city_name}")],
                [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
            ]),
            parse_mode='Markdown'
        )
        return
    
    msg = f"🏙️ **{city_name.upper()}** | 🏘️ **{district_name.upper()}**\n\n"
    msg += "🛍️ **PREMIUM CATALOG**\n\n"
    msg += "🎯 *Select your premium product*\n\n"
    msg += "💎 **Luxury collection available**\n"
    msg += "🏆 **Highest quality guaranteed**"
    
    keyboard = []
    
    # Get product emoji function
    from utils import get_product_emoji
    
    # Create premium product catalog cards
    for product_type in product_types:
        emoji = get_product_emoji(product_type)
        keyboard.append([InlineKeyboardButton(f"{emoji} {product_type.title()} Premium", callback_data=f"modern_product_type|{city_name}|{district_name}|{product_type}")])
    
    keyboard.extend([
        [InlineKeyboardButton("⬅️ Back to Districts", callback_data=f"modern_city_select|{city_name}")],
        [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_product_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern card-style product variants with premium pricing display"""
    query = update.callback_query
    if not params or len(params) < 3:
        await query.answer("Invalid product selection", show_alert=True)
        return
    
    city_name = params[0]
    district_name = params[1]
    product_type = params[2]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all variants of this product type
        c.execute("""
            SELECT 
                id,
                size,
                price,
                available
            FROM products 
            WHERE city = %s AND district = %s AND product_type = %s AND available > 0
            ORDER BY price
        """, (city_name, district_name, product_type))
        variants = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading product variants: {e}")
        await query.answer("Error loading premium variants", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not variants:
        await query.edit_message_text(
            f"🚫 **PREMIUM VARIANT UNAVAILABLE**\n\n"
            f"**{product_type.upper()} PREMIUM**\n\n"
            f"🔄 *Restocking premium variants...*\n"
            f"📞 *Contact VIP support*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Catalog", callback_data=f"modern_district_select|{city_name}|{district_name}")],
                [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
            ]),
            parse_mode='Markdown'
        )
        return
    
    from utils import get_product_emoji
    emoji = get_product_emoji(product_type)
    
    # Premium product variant display
    msg = f"🏙️ **{city_name.upper()}** | 🏘️ **{district_name.upper()}**\n\n"
    msg += f"{emoji} **{product_type.upper()} PREMIUM** {emoji}\n\n"
    msg += "💎 **Select your premium variant:**\n\n"
    msg += "🏆 *VIP quality guarantee*\n"
    msg += "⚡ *Express processing*"
    
    keyboard = []
    
    # Deduplicate variants by size and price
    unique_variants = {}
    for variant in variants:
        size = variant['size']
        price = variant['price']
        key = f"{size}_{price:.2f}"
        
        if key not in unique_variants:
            unique_variants[key] = variant
    
    # Create premium variant cards
    for variant in unique_variants.values():
        size = variant['size']
        price = variant['price']
        available = variant['available']
        product_id = variant['id']
        
        # Premium pricing display
        button_text = f"💎 {size} - {price:.2f}€ Premium"
        callback_data = f"modern_product_select|{product_id}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    # Premium navigation
    keyboard.extend([
        [InlineKeyboardButton("⬅️ Back to Catalog", callback_data=f"modern_district_select|{city_name}|{district_name}")],
        [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_product_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern card-style product details with premium purchase options"""
    query = update.callback_query
    if not params:
        await query.answer("Invalid product selection", show_alert=True)
        return
    
    product_id = params[0]
    
    # Get product details from database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT id, city, district, product_type, size, price, available, name, original_text
            FROM products WHERE id = %s
        """, (product_id,))
        product = c.fetchone()
    except Exception as e:
        logger.error(f"Error loading product {product_id}: {e}")
        await query.answer("Error loading premium product", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
            
    if not product:
        await query.answer("Premium product not found", show_alert=True)
        return
        
    city_name = product['city']
    district_name = product['district']
    product_type = product['product_type']
    size = product['size']
    price = product['price']
    available = product['available']
    
    # Store product selection for purchase
    context.user_data['selected_product_id'] = product_id
    product_dict = {
        'id': product_id,
        'city': city_name,
        'district': district_name,
        'product_type': product_type,
        'size': size,
        'price': price,
        'available': available
    }
    context.user_data['selected_product'] = product_dict
    
    from utils import get_product_emoji
    emoji = get_product_emoji(product_type)
    
    # Premium product details display
    msg = f"🏙️ **{city_name.upper()}** | 🏘️ **{district_name.upper()}**\n\n"
    msg += f"{emoji} **PREMIUM SELECTION** {emoji}\n\n"
    msg += f"🎯 **Product:** {product_type.title()}\n"
    msg += f"📏 **Size:** {size}\n"
    msg += f"💰 **Premium Price:** {price:.2f}€\n"
    msg += f"📦 **Available:** {available} units\n\n"
    msg += f"🏆 **VIP Quality Guarantee**\n"
    msg += f"⚡ **Express Processing Ready**"
    
    keyboard = [
        [InlineKeyboardButton("💳 Premium Purchase", callback_data=f"modern_pay_options|{product_id}")],
        [InlineKeyboardButton("🎫 Apply VIP Code", callback_data=f"modern_discount_code|{product_id}")],
        [InlineKeyboardButton("⬅️ Back to Variants", callback_data=f"modern_product_type|{city_name}|{district_name}|{product_type}"),
         InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_pay_options(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern premium payment processing"""
    # Use existing payment system with modern styling
    from user import handle_pay_single_item
    
    # Get product details from context
    product = context.user_data.get('selected_product')
    if not product:
        await update.callback_query.answer("Product selection lost", show_alert=True)
        return
    
    # Convert to format expected by handle_pay_single_item
    city_id = product['city']
    district_id = product['district']
    product_type = product['product_type']
    size = product['size']
    price = product['price']
    
    new_params = [city_id, district_id, product_type, size, price]
    await handle_pay_single_item(update, context, new_params)

async def handle_modern_discount_code(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern premium discount code application"""
    # Use existing discount system
    from user import handle_apply_discount_single_pay
    
    # Get product details from context
    product = context.user_data.get('selected_product')
    if not product:
        await update.callback_query.answer("Product selection lost", show_alert=True)
        return
    
    await handle_apply_discount_single_pay(update, context, params)

async def handle_modern_deals(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern hot deals interface with REAL deals (manual + automatic)"""
    query = update.callback_query
    
    # FORCE CLEANUP: Remove any inactive deals first
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Clean up any deals that should not be active
        c.execute("DELETE FROM hot_deals WHERE is_active = FALSE")
        cleanup_count = c.rowcount
        if cleanup_count > 0:
            logger.info(f"🧹 Cleaned up {cleanup_count} inactive hot deals from database")
            conn.commit()
        else:
            logger.info("🧹 No inactive deals to clean up")
        
        # AGGRESSIVE FILTERING - Only show VERIFIED ACTIVE deals
        c.execute("""
            SELECT hd.id, hd.deal_title, hd.deal_description, hd.discount_percentage,
                   hd.original_price, hd.deal_price, hd.priority,
                   p.id as product_id, p.city, p.district, p.product_type, p.size, p.price
            FROM hot_deals hd
            JOIN products p ON hd.product_id = p.id
            WHERE hd.is_active = TRUE 
            AND p.available > 0
            AND (hd.expires_at IS NULL OR hd.expires_at > CURRENT_TIMESTAMP)
            AND EXISTS (SELECT 1 FROM hot_deals hd2 WHERE hd2.id = hd.id AND hd2.is_active = TRUE)
            ORDER BY hd.priority DESC, hd.created_at DESC
            LIMIT 5
        """)
        manual_deals = c.fetchall()
        
        # VERIFICATION: Double-check each deal exists and is active
        verified_deals = []
        for deal in manual_deals:
            c.execute("SELECT COUNT(*) as count FROM hot_deals WHERE id = %s AND is_active = TRUE", (deal['id'],))
            if c.fetchone()['count'] > 0:
                verified_deals.append(deal)
            else:
                logger.warning(f"Filtered out inactive deal ID {deal['id']}")
        
        manual_deals = verified_deals
        logger.info(f"Hot deals query returned {len(manual_deals)} VERIFIED active deals")
        
        # Get automatic lowest-price deals if we need more (and if enabled)
        remaining_slots = 5 - len(manual_deals)
        auto_deals = []
        if remaining_slots > 0:
            # Check if automatic deals are enabled by admin
            c.execute("SELECT setting_value FROM hot_deals_settings WHERE setting_name = 'auto_deals_enabled'")
            auto_enabled_result = c.fetchone()
            auto_deals_enabled = auto_enabled_result['setting_value'] if auto_enabled_result else True
            
            if auto_deals_enabled:
                c.execute("""
                    SELECT DISTINCT city, district, product_type, MIN(price) as min_price, 
                           COUNT(*) as available_count
                    FROM products 
                    WHERE available > 0 
                    GROUP BY city, district, product_type
                    ORDER BY min_price ASC
                    LIMIT %s
                """, (remaining_slots,))
                auto_deals = c.fetchall()
            else:
                logger.info("🚫 Automatic deals disabled by admin - not showing to users")
        
    except Exception as e:
        logger.error(f"Error loading hot deals: {e}")
        manual_deals = []
        auto_deals = []
    finally:
        if conn:
            conn.close()
    
    msg = "🔥 **PREMIUM HOT DEALS** 🔥\n\n"
    msg += "💥 **LIMITED TIME OFFERS**\n\n"
    
    keyboard = []
    
    if manual_deals or auto_deals:
        msg += "🎯 *Exclusive VIP offers available now:*\n\n"
        
        from utils import get_product_emoji
        
        # Organize deals by city for structured display
        deals_by_city = {}
        
        # Group manual deals by city
        for deal in manual_deals:
            city = deal['city']
            if city not in deals_by_city:
                deals_by_city[city] = []
            
            product_type = deal['product_type']
            emoji = get_product_emoji(product_type)
            
            # Format deal text with clear pricing
            if deal['deal_title']:
                deal_name = deal['deal_title']
            else:
                deal_name = f"{emoji} {product_type.title()} {deal['size']}"
            
            deal_text = f"{deal_name}: {deal['deal_price']:.2f}€ (was {deal['original_price']:.2f}€)"
            
            deal_info = {
                'text': deal_text,
                'callback': f"pay_single_item|{deal['city']}|{deal['district']}|{deal['product_type']}|{deal['size']}|{deal['deal_price']}",
                'type': 'manual'
            }
            deals_by_city[city].append(deal_info)
        
        # Group automatic deals by city
        for deal in auto_deals:
            city = deal['city']
            if city not in deals_by_city:
                deals_by_city[city] = []
            
            product_type = deal['product_type']
            emoji = get_product_emoji(product_type)
            
            deal_text = f"{emoji} {product_type.title()}: From {deal['min_price']:.2f}€"
            
            deal_info = {
                'text': deal_text,
                'callback': f"pay_single_item|{city}|{deal['district']}|{product_type}|unknown|{deal['min_price']}",
                'type': 'auto'
            }
            deals_by_city[city].append(deal_info)
        
        # Create structured menu with city headers and deals
        total_deals = 0
        
        # Helper function to get city_id and district_id from names
        def get_location_ids(city_name, district_name):
            from utils import CITIES, DISTRICTS
            city_id = None
            district_id = None
            
            # Find city_id by name
            for cid, cname in CITIES.items():
                if cname.lower() == city_name.lower():
                    city_id = cid
                    break
            
            # Find district_id by name within the city
            if city_id and city_id in DISTRICTS:
                for did, dname in DISTRICTS[city_id].items():
                    if dname.lower() == district_name.lower():
                        district_id = did
                        break
            
            return city_id, district_id
        
        for city, city_deals in deals_by_city.items():
            # Add non-clickable city header
            keyboard.append([InlineKeyboardButton(f"🏙️ {city.upper()}", callback_data="city_header_noop")])
            
            # Add all deals for this city
            for deal in city_deals:
                # Extract city and district from the original callback to convert to IDs
                callback_parts = deal['callback'].split('|')
                if len(callback_parts) >= 5:
                    city_name = callback_parts[1]
                    district_name = callback_parts[2]
                    product_type = callback_parts[3]
                    size = callback_parts[4]
                    price = callback_parts[5]
                    
                    # Get the correct IDs
                    city_id, district_id = get_location_ids(city_name, district_name)
                    
                    if city_id and district_id:
                        # Create correct callback with IDs and hot_deal flag
                        correct_callback = f"pay_single_item_hot_deal|{city_id}|{district_id}|{product_type}|{size}|{price}"
                        keyboard.append([InlineKeyboardButton(deal['text'], callback_data=correct_callback)])
                    else:
                        # Fallback to original callback if IDs not found
                        keyboard.append([InlineKeyboardButton(deal['text'], callback_data=deal['callback'])])
                else:
                    # Fallback to original callback if parsing fails
                    keyboard.append([InlineKeyboardButton(deal['text'], callback_data=deal['callback'])])
                
                total_deals += 1
        
        msg += f"⚡ **{total_deals} hot deals available**\n"
        msg += "💎 *Premium quality guaranteed*"
        
    else:
        msg += "🔄 *Updating deals - check back soon!*\n"
        msg += "💎 *Premium offers coming*"
    
    # Dynamic back button based on how user accessed hot deals
    keyboard.extend([
        [InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_start")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_city_header_noop(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle non-clickable city header - just show a message"""
    query = update.callback_query
    await query.answer("🏙️ City Header - Select a deal below", show_alert=False)

async def handle_pay_single_item_hot_deal(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle hot deal payment - NO DISCOUNTS ALLOWED (already discounted price)"""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    
    # Import necessary functions from user and utils modules
    from user import CITIES, DISTRICTS, PRODUCT_TYPES, DEFAULT_PRODUCT_EMOJI, _get_lang_data, format_currency, send_message_with_retry
    from utils import get_db_connection, track_reservation
    from decimal import Decimal, ROUND_DOWN
    import asyncio
    import telegram.error as telegram_error
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    lang, lang_data = _get_lang_data(context)

    if not params or len(params) < 5:
        await query.answer("Error: Incomplete product data.", show_alert=True)
        return

    city_id, dist_id, p_type, size, price_str = params  # price_str is the HOT DEAL price (already discounted)

    try:
        hot_deal_price = Decimal(price_str)
    except ValueError:
        await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None)
        return

    city = CITIES.get(city_id)
    district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district:
        await query.edit_message_text("❌ Error: Location data mismatch.", parse_mode=None)
        return

    await query.answer("⏳ Processing hot deal...")

    reserved_id = None
    conn = None
    product_details_for_snapshot = None
    error_occurred_reservation = False

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        
        # VERIFICATION: Check if this is actually from a valid hot deal
        # Extract hot deal info from context or verify the deal still exists
        deal_exists = True
        if hasattr(context.user_data, 'get') and context.user_data.get('from_hot_deal'):
            deal_id = context.user_data.get('hot_deal_id')
            if deal_id:
                c.execute("SELECT COUNT(*) as count FROM hot_deals WHERE id = %s AND is_active = TRUE", (deal_id,))
                deal_exists = c.fetchone()['count'] > 0
                if not deal_exists:
                    logger.warning(f"Hot deal {deal_id} no longer exists or is inactive")
        
        # Find any available product matching the criteria (hot deals can use any matching product)
        c.execute("""
            SELECT id, name, price, size, product_type, city, district, original_text 
            FROM products 
            WHERE city = %s AND district = %s AND product_type = %s AND size = %s AND available > reserved 
            ORDER BY id LIMIT 1
        """, (city, district, p_type, size))
        product_to_reserve = c.fetchone()

        if not product_to_reserve:
            conn.rollback()
            try:
                await query.edit_message_text("❌ Sorry, this hot deal is no longer available!", parse_mode=None)
            except Exception:
                pass
            error_occurred_reservation = True
        else:
            reserved_id = product_to_reserve['id']
            product_details_for_snapshot = dict(product_to_reserve)
            c.execute("UPDATE products SET reserved = reserved + 1 WHERE id = %s AND available > reserved", (reserved_id,))
            if c.rowcount == 1:
                conn.commit()
            else:
                conn.rollback()
                try:
                    await query.edit_message_text("❌ Sorry, this hot deal was just taken!", parse_mode=None)
                except Exception:
                    pass
                error_occurred_reservation = True
    except Exception as e:
        if conn:
            conn.rollback()
        try:
            await query.edit_message_text("❌ Database error during reservation.", parse_mode=None)
        except Exception:
            pass
        error_occurred_reservation = True
    finally:
        if conn:
            conn.close()

    if error_occurred_reservation:
        return

    if reserved_id and product_details_for_snapshot:
        # Create snapshot with hot deal price (NO DISCOUNTS APPLIED)
        single_item_snapshot = [{
            "product_id": reserved_id,
            "price": float(hot_deal_price),  # Use hot deal price, not original price
            "name": product_details_for_snapshot['name'],
            "size": product_details_for_snapshot['size'],
            "product_type": product_details_for_snapshot['product_type'],
            "city": product_details_for_snapshot['city'],
            "district": product_details_for_snapshot['district'],
            "original_text": product_details_for_snapshot.get('original_text')
        }]

        # Set context for direct payment (NO DISCOUNT CODES ALLOWED)
        context.user_data['single_item_pay_snapshot'] = single_item_snapshot
        context.user_data['single_item_pay_final_eur'] = float(hot_deal_price)  # Final price is hot deal price
        context.user_data['single_item_pay_discount_code'] = None  # NO DISCOUNTS
        context.user_data['single_item_pay_back_params'] = params
        
        # Track reservation for abandonment cleanup
        track_reservation(user_id, single_item_snapshot, "single")

        # Display payment options WITHOUT discount buttons
        item_name_display = f"{PRODUCT_TYPES.get(p_type, '')} {product_details_for_snapshot['name']} {product_details_for_snapshot['size']}"
        price_display_str = format_currency(hot_deal_price)
        
        prompt_msg = f"🔥 **HOT DEAL PAYMENT** 🔥\n\n"
        prompt_msg += f"📦 **Product:** {item_name_display}\n"
        prompt_msg += f"💰 **Hot Deal Price:** {price_display_str} EUR\n\n"
        prompt_msg += f"⚠️ *No additional discounts can be applied to hot deals*\n\n"
        prompt_msg += f"Choose your payment method:"

        pay_now_button_text = lang_data.get("pay_now_button", "Pay Now")
        back_to_deals_button_text = "⬅️ Back to Hot Deals"

        # Payment menu WITHOUT discount options
        keyboard = [
            [InlineKeyboardButton(f"💳 {pay_now_button_text}", callback_data="skip_discount_single_pay")],
            [InlineKeyboardButton(back_to_deals_button_text, callback_data="modern_deals")]
        ]
        
        try:
            await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except telegram_error.BadRequest as e:
            if "message is not modified" not in str(e).lower():
                await send_message_with_retry(context.bot, chat_id, prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                await query.answer()
    else:
        try:
            await query.edit_message_text("❌ An internal error occurred during payment initiation.", parse_mode=None)
        except Exception:
            pass

async def handle_modern_deal_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle hot deal selection - redirect to product selection"""
    query = update.callback_query
    if not params or len(params) < 3:
        await query.answer("Invalid deal selection", show_alert=True)
        return
    
    city_name = params[0]
    district_name = params[1] 
    product_type = params[2]
    
    # Redirect to product type selection for this deal
    await handle_modern_product_type(update, context, [city_name, district_name, product_type])

async def handle_modern_profile(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern premium profile interface"""
    query = update.callback_query
    
    msg = "👤 **VIP ACCOUNT DASHBOARD** 👤\n\n"
    msg += "🏆 **PREMIUM MEMBER**\n\n"
    msg += "💎 *VIP status active*\n"
    msg += "🚀 *Premium features unlocked*\n"
    msg += "🏆 *Exclusive access granted*\n\n"
    msg += "📊 **Account details coming soon**"
    
    keyboard = [
        [InlineKeyboardButton("💰 Premium Wallet", callback_data="modern_wallet")],
        [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern premium wallet interface"""
    query = update.callback_query
    
    msg = "💰 **PREMIUM WALLET** 💰\n\n"
    msg += "💎 **VIP BALANCE**\n\n"
    msg += "🏆 *Premium payment methods*\n"
    msg += "⚡ *Instant transactions*\n"
    msg += "🔒 *Secure VIP processing*\n\n"
    msg += "💳 **Wallet features coming soon**"
    
    keyboard = [
        [InlineKeyboardButton("👤 Back to Profile", callback_data="modern_profile")],
        [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_promotions(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern premium promotions interface"""
    query = update.callback_query
    
    msg = "🎯 **VIP PROMOTIONS** 🎯\n\n"
    msg += "🎁 **EXCLUSIVE OFFERS**\n\n"
    msg += "💎 *VIP-only promotions*\n"
    msg += "🏆 *Premium member benefits*\n"
    msg += "🎁 *Exclusive reward system*\n\n"
    msg += "🚀 **Premium rewards coming soon**"
    
    keyboard = [
        [InlineKeyboardButton("🔥 Hot Deals", callback_data="modern_deals")],
        [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_app(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Modern app info interface - shows admin-configured info"""
    query = update.callback_query
    
    # Get app info from database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT info_title, info_content
            FROM app_info 
            WHERE is_active = TRUE
            ORDER BY display_order ASC, created_at DESC
            LIMIT 5
        """)
        info_items = c.fetchall()
    except Exception as e:
        logger.error(f"Error loading app info: {e}")
        info_items = []
    finally:
        if conn:
            conn.close()
    
    msg = "ℹ️ **APP INFORMATION** ℹ️\n\n"
    
    if info_items:
        for info in info_items:
            msg += f"📋 **{info['info_title']}**\n"
            msg += f"{info['info_content']}\n\n"
    else:
        # Default content if no admin info is set
        msg += "🎯 **About Our Service**\n\n"
        msg += "💎 *Premium quality products*\n"
        msg += "🚀 *Fast delivery service*\n"
        msg += "🏆 *Excellent customer experience*\n"
        msg += "🔒 *Secure transactions*\n\n"
        msg += "🌟 **Contact admin to add custom info**"
    
    keyboard = [
        [InlineKeyboardButton("🛍️ Start Shopping", callback_data="modern_shop")],
        [InlineKeyboardButton("🏠 Home", callback_data="modern_home")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_modern_home(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Return to appropriate home - custom UI or modern UI"""
    # MODE: Dynamic navigation - check if user came from custom UI
    active_theme = get_active_ui_theme()
    
    if active_theme and active_theme.get('theme_name') == 'custom':
        # User has custom UI active - go back to custom start
        from user import handle_back_start
        return await handle_back_start(update, context, params)
    else:
        # User using modern UI - go to modern welcome
        return await handle_modern_welcome(update, context, params)

# MODE: HOT DEALS MANAGEMENT SYSTEM FOR ADMINS
async def handle_admin_hot_deals_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin hot deals management menu"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get current hot deals count
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM hot_deals WHERE is_active = TRUE")
        active_deals = c.fetchone()['count']
    except Exception as e:
        logger.error(f"Error getting hot deals count: {e}")
        active_deals = 0
    finally:
        if conn:
            conn.close()
    
    msg = "🔥 **HOT DEALS MANAGEMENT** 🔥\n\n"
    msg += "💥 **Manage Premium Hot Deals**\n\n"
    msg += f"📊 **Active Deals:** {active_deals}\n\n"
    msg += "🎯 *Create custom hot deals with special pricing*\n"
    msg += "⚡ *Set priorities and expiration dates*\n"
    msg += "💎 *Manage premium offers*"
    
    keyboard = [
        [InlineKeyboardButton("➕ Add New Hot Deal", callback_data="admin_add_hot_deal")],
        [InlineKeyboardButton("📋 Manage Existing Deals", callback_data="admin_manage_hot_deals")],
        [InlineKeyboardButton("👀 Preview Hot Deals", callback_data="modern_deals")],
        [InlineKeyboardButton("⬅️ Back to Marketing", callback_data="marketing_promotions_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_add_hot_deal(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start adding a new hot deal - select product"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get available products
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT DISTINCT product_type, size, price, city, district,
                   MIN(id) as id, SUM(available) as total_available
            FROM products 
            WHERE available > 0
            GROUP BY product_type, size, price, city, district
            ORDER BY city, district, product_type, price
            LIMIT 20
        """)
        products = c.fetchall()
    except Exception as e:
        logger.error(f"Error loading products for hot deals: {e}")
        products = []
    finally:
        if conn:
            conn.close()
    
    if not products:
        await query.edit_message_text(
            "🚫 **No Products Available**\n\n"
            "No products found to create hot deals.\n"
            "Add some products first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Hot Deals", callback_data="admin_hot_deals_menu")]
            ]),
            parse_mode='Markdown'
        )
        return
    
    msg = "➕ **ADD NEW HOT DEAL** ➕\n\n"
    msg += "🎯 **Select Product for Hot Deal:**\n\n"
    msg += "Choose a product to create a special offer:"
    
    keyboard = []
    
    from utils import get_product_emoji
    for product in products:
        emoji = get_product_emoji(product['product_type'])
        product_text = f"{emoji} {product['product_type']} {product['size']} - {product['price']:.2f}€"
        product_text += f" ({product['city']}/{product['district']}) [{product['total_available']} units]"
        
        keyboard.append([InlineKeyboardButton(product_text, callback_data=f"admin_hot_deal_product|{product['id']}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Hot Deals", callback_data="admin_hot_deals_menu")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_hot_deal_product(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Configure hot deal for selected product"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid product selection", show_alert=True)
        return
    
    product_id = params[0]
    
    # Get product details
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT id, city, district, product_type, size, price, available
            FROM products WHERE id = %s
        """, (product_id,))
        product = c.fetchone()
    except Exception as e:
        logger.error(f"Error loading product {product_id}: {e}")
        await query.answer("Error loading product", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not product:
        await query.answer("Product not found", show_alert=True)
        return
    
    # Store product in context for next steps
    context.user_data['hot_deal_product'] = product
    
    from utils import get_product_emoji
    emoji = get_product_emoji(product['product_type'])
    
    msg = f"🔥 **CREATE HOT DEAL** 🔥\n\n"
    msg += f"📦 **Selected Product:**\n"
    msg += f"{emoji} {product['product_type']} {product['size']}\n"
    msg += f"📍 {product['city']} / {product['district']}\n"
    msg += f"💰 Current Price: {product['price']:.2f}€\n"
    msg += f"📦 Available: {product['available']} units\n\n"
    msg += "🎯 **Choose Deal Type:**"
    
    keyboard = [
        [InlineKeyboardButton("💰 Set Custom Price", callback_data=f"admin_deal_custom_price|{product_id}")],
        [InlineKeyboardButton("📊 Set Discount %", callback_data=f"admin_deal_discount|{product_id}")],
        [InlineKeyboardButton("🏷️ Custom Title Only", callback_data=f"admin_deal_title_only|{product_id}")],
        [InlineKeyboardButton("🔢 Set Stock Limit", callback_data=f"admin_deal_quantity_limit|{product_id}")],
        [InlineKeyboardButton("⬅️ Back to Products", callback_data="admin_add_hot_deal")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_deal_custom_price(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set custom price for hot deal"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    product = context.user_data.get('hot_deal_product')
    if not product:
        await query.answer("Product selection lost", show_alert=True)
        return
    
    context.user_data['hot_deal_type'] = 'custom_price'
    context.user_data['state'] = 'awaiting_hot_deal_price'
    
    from utils import get_product_emoji
    emoji = get_product_emoji(product['product_type'])
    
    msg = f"💰 **SET CUSTOM PRICE** 💰\n\n"
    msg += f"📦 **Product:** {emoji} {product['product_type']} {product['size']}\n"
    msg += f"💰 **Current Price:** {product['price']:.2f}€\n\n"
    msg += "🎯 **Enter new deal price:**\n"
    msg += "*(Example: 15.99)*"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_hot_deal_product|{product['id']}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_hot_deal_price_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle custom price input for hot deal"""
    user_id = update.effective_user.id
    
    if context.user_data.get('state') != 'awaiting_hot_deal_price':
        return
    
    if not is_primary_admin(user_id):
        return
    
    product = context.user_data.get('hot_deal_product')
    if not product:
        await update.message.reply_text("❌ Product selection lost. Please try again.")
        context.user_data['state'] = None
        return
    
    try:
        price = float(update.message.text.strip())
        if price <= 0:
            await update.message.reply_text("❌ Price must be greater than 0. Try again:")
            return
        
        context.user_data['hot_deal_price'] = price
        context.user_data['state'] = 'awaiting_hot_deal_title'
        
        msg = f"💰 **CUSTOM PRICE SET** 💰\n\n"
        msg += f"📦 **Product:** {product['product_type']} {product['size']}\n"
        msg += f"💰 **Original Price:** {product['price']:.2f}€\n"
        msg += f"🔥 **Deal Price:** {price:.2f}€\n"
        msg += f"📊 **Savings:** {((product['price'] - price) / product['price'] * 100):.1f}%\n\n"
        msg += "🏷️ **Enter deal title (or skip):**\n"
        msg += "*(Example: 'Weekend Special' or 'Limited Edition')*"
        
        keyboard = [
            [InlineKeyboardButton("⏭️ Skip Title", callback_data=f"admin_deal_skip_title|{product['id']}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_hot_deal_product_preserve|{product['id']}")]
        ]
        
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except ValueError:
        await update.message.reply_text("❌ Invalid price format. Please enter a number (e.g., 15.99):")

async def handle_hot_deal_discount_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle discount percentage input for hot deal"""
    user_id = update.effective_user.id
    
    if context.user_data.get('state') != 'awaiting_hot_deal_discount':
        return
    
    if not is_primary_admin(user_id):
        return
    
    product = context.user_data.get('hot_deal_product')
    if not product:
        await update.message.reply_text("❌ Product selection lost. Please try again.")
        context.user_data['state'] = None
        return
    
    try:
        discount = float(update.message.text.strip())
        if discount <= 0 or discount >= 100:
            await update.message.reply_text("❌ Discount must be between 0 and 100%. Try again:")
            return
        
        deal_price = product['price'] * (1 - discount / 100)
        context.user_data['hot_deal_price'] = deal_price
        context.user_data['hot_deal_discount'] = discount
        context.user_data['state'] = 'awaiting_hot_deal_title'
        
        msg = f"📊 **DISCOUNT SET** 📊\n\n"
        msg += f"📦 **Product:** {product['product_type']} {product['size']}\n"
        msg += f"💰 **Original Price:** {product['price']:.2f}€\n"
        msg += f"📊 **Discount:** {discount:.1f}%\n"
        msg += f"🔥 **Deal Price:** {deal_price:.2f}€\n\n"
        msg += "🏷️ **Enter deal title (or skip):**\n"
        msg += "*(Example: 'Weekend Special' or 'Limited Edition')*"
        
        keyboard = [
            [InlineKeyboardButton("⏭️ Skip Title", callback_data=f"admin_deal_skip_title|{product['id']}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_hot_deal_product_preserve|{product['id']}")]
        ]
        
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except ValueError:
        await update.message.reply_text("❌ Invalid discount format. Please enter a number (e.g., 25):")

async def handle_hot_deal_title_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle title input for hot deal"""
    user_id = update.effective_user.id
    
    if context.user_data.get('state') != 'awaiting_hot_deal_title':
        return
    
    if not is_primary_admin(user_id):
        return
    
    product = context.user_data.get('hot_deal_product')
    deal_price = context.user_data.get('hot_deal_price')
    
    if not product or deal_price is None:
        await update.message.reply_text("❌ Deal configuration lost. Please try again.")
        context.user_data['state'] = None
        return
    
    title = update.message.text.strip()
    if not title or len(title) < 2:
        await update.message.reply_text("❌ Title must be at least 2 characters. Try again:")
        return
    
    if len(title) > 50:
        await update.message.reply_text("❌ Title must be 50 characters or less. Try again:")
        return
    
    context.user_data['hot_deal_title'] = title
    context.user_data['state'] = 'awaiting_hot_deal_quantity'
    
    msg = f"🔢 **SET STOCK LIMIT** 🔢\n\n"
    msg += f"📦 **Product:** {product['product_type']} {product['size']}\n"
    msg += f"🏷️ **Title:** {title}\n"
    msg += f"🔥 **Deal Price:** {deal_price:.2f}€\n\n"
    msg += "🎯 **Enter total maximum units for sale:**\n"
    msg += "*(Example: 50 for max 50 total units available)*\n"
    msg += "*(Or type 'unlimited' for no stock limit)*"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_hot_deal_product|{product['id']}")]
    ]
    
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_hot_deal_quantity_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle quantity limit input for hot deal"""
    user_id = update.effective_user.id
    
    if context.user_data.get('state') != 'awaiting_hot_deal_quantity':
        return
    
    if not is_primary_admin(user_id):
        return
    
    product = context.user_data.get('hot_deal_product')
    deal_price = context.user_data.get('hot_deal_price')
    title = context.user_data.get('hot_deal_title')
    discount = context.user_data.get('hot_deal_discount', 0)
    
    if not product or deal_price is None or not title:
        await update.message.reply_text("❌ Deal configuration lost. Please try again.")
        context.user_data['state'] = None
        return
    
    quantity_input = update.message.text.strip().lower()
    
    if quantity_input == 'unlimited':
        quantity_limit = None
    else:
        try:
            quantity_limit = int(quantity_input)
            if quantity_limit <= 0:
                await update.message.reply_text("❌ Stock limit must be greater than 0 or 'unlimited'. Try again:")
                return
        except ValueError:
            await update.message.reply_text("❌ Invalid format. Enter a number or 'unlimited'. Try again:")
            return
    
    # Save hot deal to database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Handle quantity-only deals (no custom title)
        deal_type = context.user_data.get('hot_deal_type', 'custom')
        if deal_type == 'quantity_only':
            final_title = f"Limited Stock - {product['product_type']} {product['size']}"
        else:
            final_title = title
        
        c.execute("""
            INSERT INTO hot_deals 
            (product_id, deal_title, deal_description, discount_percentage, 
             original_price, deal_price, quantity_limit, is_active, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            product['id'],
            final_title,
            f"Special offer: {final_title}",
            discount,
            product['price'],
            deal_price,
            quantity_limit,
            True,
            user_id
        ))
        
        conn.commit()
        
        # Clear state
        context.user_data['state'] = None
        for key in ['hot_deal_product', 'hot_deal_price', 'hot_deal_title', 'hot_deal_discount']:
            context.user_data.pop(key, None)
        
        msg = f"✅ **HOT DEAL CREATED** ✅\n\n"
        msg += f"📦 **Product:** {product['product_type']} {product['size']}\n"
        msg += f"🏷️ **Title:** {final_title}\n"
        msg += f"💰 **Original Price:** {product['price']:.2f}€\n"
        msg += f"🔥 **Deal Price:** {deal_price:.2f}€\n"
        if discount > 0:
            msg += f"📊 **Discount:** {discount:.1f}%\n"
        if quantity_limit:
            msg += f"🔢 **Stock Limit:** {quantity_limit} total units\n"
        else:
            msg += f"🔢 **Stock Limit:** Unlimited\n"
        msg += f"\n🚀 **Deal is now active!**"
        
        keyboard = [
            [InlineKeyboardButton("🔥 Manage Hot Deals", callback_data="admin_hot_deals_menu")],
            [InlineKeyboardButton("➕ Add Another Deal", callback_data="admin_add_hot_deal")],
            [InlineKeyboardButton("⬅️ Back to Marketing", callback_data="marketing_promotions_menu")]
        ]
        
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error creating hot deal: {e}")
        await update.message.reply_text("❌ Error creating hot deal. Please try again.")
        context.user_data['state'] = None

async def handle_admin_deal_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set discount percentage for hot deal"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    product = context.user_data.get('hot_deal_product')
    if not product:
        await query.answer("Product selection lost", show_alert=True)
        return
    
    context.user_data['hot_deal_type'] = 'discount'
    context.user_data['state'] = 'awaiting_hot_deal_discount'
    
    from utils import get_product_emoji
    emoji = get_product_emoji(product['product_type'])
    
    msg = f"📊 **SET DISCOUNT PERCENTAGE** 📊\n\n"
    msg += f"📦 **Product:** {emoji} {product['product_type']} {product['size']}\n"
    msg += f"💰 **Current Price:** {product['price']:.2f}€\n\n"
    msg += "🎯 **Enter discount percentage:**\n"
    msg += "*(Example: 25 for 25% off)*"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_hot_deal_product|{product['id']}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_deal_title_only(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set custom title only for hot deal"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    product = context.user_data.get('hot_deal_product')
    if not product:
        await query.answer("Product selection lost", show_alert=True)
        return
    
    context.user_data['hot_deal_type'] = 'title_only'
    context.user_data['hot_deal_price'] = product['price']  # Keep original price
    context.user_data['state'] = 'awaiting_hot_deal_title'
    
    from utils import get_product_emoji
    emoji = get_product_emoji(product['product_type'])
    
    msg = f"🏷️ **SET CUSTOM TITLE** 🏷️\n\n"
    msg += f"📦 **Product:** {emoji} {product['product_type']} {product['size']}\n"
    msg += f"💰 **Price:** {product['price']:.2f}€ (unchanged)\n\n"
    msg += "🎯 **Enter custom deal title:**\n"
    msg += "*(Example: 'Weekend Special' or 'Limited Edition')*"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_hot_deal_product|{product['id']}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_hot_deal_product_preserve(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Go back to product configuration while preserving context"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Clear current input state but preserve product context
    context.user_data['state'] = None
    
    # Redirect to product configuration
    await handle_admin_hot_deal_product(update, context, params)

async def handle_admin_deal_skip_title(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Skip title step and proceed to quantity limit"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    product = context.user_data.get('hot_deal_product')
    deal_price = context.user_data.get('hot_deal_price')
    
    if not product or deal_price is None:
        await query.answer("❌ Deal configuration lost. Please try again.", show_alert=True)
        return await handle_admin_add_hot_deal(update, context)
    
    # Set default title using product name
    context.user_data['hot_deal_title'] = f"{product['product_type']} {product['size']} Deal"
    context.user_data['state'] = 'awaiting_hot_deal_quantity'
    
    msg = f"🔢 **SET STOCK LIMIT** 🔢\n\n"
    msg += f"📦 **Product:** {product['product_type']} {product['size']}\n"
    msg += f"🏷️ **Title:** {context.user_data['hot_deal_title']}\n"
    msg += f"🔥 **Deal Price:** {deal_price:.2f}€\n\n"
    msg += "🎯 **Enter total maximum units for sale:**\n"
    msg += "*(Example: 50 for max 50 total units available)*\n"
    msg += "*(Or type 'unlimited' for no stock limit)*"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_hot_deal_product|{product['id']}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_deal_quantity_limit(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set quantity limit for hot deal"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    product = context.user_data.get('hot_deal_product')
    if not product:
        await query.answer("Product selection lost", show_alert=True)
        return
    
    # Set default price (original price) and prepare for quantity input
    context.user_data['hot_deal_price'] = product['price']
    context.user_data['hot_deal_type'] = 'quantity_only'
    context.user_data['state'] = 'awaiting_hot_deal_quantity'
    
    from utils import get_product_emoji
    emoji = get_product_emoji(product['product_type'])
    
    msg = f"🔢 **SET STOCK LIMIT** 🔢\n\n"
    msg += f"📦 **Product:** {emoji} {product['product_type']} {product['size']}\n"
    msg += f"💰 **Price:** {product['price']:.2f}€ (unchanged)\n\n"
    msg += "🎯 **Enter total maximum units for sale:**\n"
    msg += "*(Example: 50 for max 50 total units available)*\n"
    msg += "*(Or type 'unlimited' for no stock limit)*"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data=f"admin_hot_deal_product|{product['id']}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_manage_hot_deals(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Manage existing hot deals"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get BOTH manual hot deals AND automatic deals (like users see)
    conn = None
    manual_deals = []
    auto_deals = []
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get manual hot deals (from hot_deals table)
        c.execute("""
            SELECT hd.id, hd.deal_title, hd.deal_description, hd.discount_percentage,
                   hd.original_price, hd.deal_price, hd.priority, hd.is_active,
                   hd.created_at, hd.expires_at,
                   p.product_type, p.size, p.city, p.district, 'manual' as deal_type
            FROM hot_deals hd
            JOIN products p ON hd.product_id = p.id
            ORDER BY hd.is_active DESC, hd.priority DESC, hd.created_at DESC
            LIMIT 10
        """)
        manual_deals = c.fetchall()
        
        # Get automatic deals (same as user-facing display) - but check if enabled
        remaining_slots = 10 - len(manual_deals)
        if remaining_slots > 0:
            # Check if automatic deals are enabled
            c.execute("SELECT setting_value FROM hot_deals_settings WHERE setting_name = 'auto_deals_enabled'")
            auto_enabled_result = c.fetchone()
            auto_deals_enabled = auto_enabled_result['setting_value'] if auto_enabled_result else True
            
            if auto_deals_enabled:
                c.execute("""
                    SELECT DISTINCT city, district, product_type, MIN(price) as min_price, 
                           COUNT(*) as available_count, 'automatic' as deal_type
                    FROM products 
                    WHERE available > 0 
                    GROUP BY city, district, product_type
                    ORDER BY min_price ASC
                    LIMIT %s
                """, (remaining_slots,))
                auto_deals = c.fetchall()
            else:
                auto_deals = []
            
    except Exception as e:
        logger.error(f"Error loading hot deals: {e}")
        manual_deals = []
        auto_deals = []
    finally:
        if conn:
            conn.close()
    
    # Combine both types of deals
    all_deals = list(manual_deals) + list(auto_deals)
    
    if not all_deals:
        await query.edit_message_text(
            "📋 **NO HOT DEALS FOUND** 📋\n\n"
            "No hot deals created yet.\n"
            "Create your first hot deal!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add New Hot Deal", callback_data="admin_add_hot_deal")],
                [InlineKeyboardButton("⬅️ Back to Hot Deals", callback_data="admin_hot_deals_menu")]
            ]),
            parse_mode='Markdown'
        )
        return
    
    # Check auto deals status for display
    auto_deals_enabled = True
    try:
        c.execute("SELECT setting_value FROM hot_deals_settings WHERE setting_name = 'auto_deals_enabled'")
        auto_enabled_result = c.fetchone()
        auto_deals_enabled = auto_enabled_result['setting_value'] if auto_enabled_result else True
    except:
        auto_deals_enabled = True
    
    auto_status = "🟢 ENABLED" if auto_deals_enabled else "🚫 DISABLED"
    
    msg = "🔥 **MANAGE HOT DEALS** 🔥\n\n"
    msg += f"**Found:** {len(manual_deals)} manual + {len(auto_deals)} automatic = {len(all_deals)} total\n"
    msg += f"**Auto Deals Status:** {auto_status}\n\n"
    
    keyboard = []
    
    from utils import get_product_emoji
    
    # Display manual deals first
    if manual_deals:
        msg += "📝 **MANUAL HOT DEALS (you created):**\n"
        for deal in manual_deals:
            emoji = get_product_emoji(deal['product_type'])
            status = "🟢" if deal['is_active'] else "🔴"
            
            if deal['deal_title']:
                deal_name = deal['deal_title']
            else:
                deal_name = f"{emoji} {deal['product_type']} {deal['size']}"
            
            location = f"{deal['city']} - {deal['district']}"
            msg += f"{status} **{deal_name}** ({location})\n"
            msg += f"   💰 {deal['deal_price']:.2f}€ (was {deal['original_price']:.2f}€)\n\n"
            
            button_text = f"{status} EDIT: {deal_name}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"admin_edit_hot_deal|{deal['id']}")])
        
        msg += "\n"
    
    # Display automatic deals (users see these too)
    if auto_deals:
        msg += "🤖 **AUTOMATIC DEALS (system generated):**\n"
        for deal in auto_deals:
            emoji = get_product_emoji(deal['product_type'])
            product_name = deal['product_type'].title()
            location = f"{deal['city']} - {deal['district']}"
            
            msg += f"🟢 **{emoji} {product_name}** ({location})\n"
            msg += f"   💰 From {deal['min_price']:.2f}€ ({deal['available_count']} available)\n\n"
        
        msg += "ℹ️ *Automatic deals show lowest prices - cannot be edited*\n\n"
        
        # Add simple controls for automatic deals
        keyboard.extend([
            [InlineKeyboardButton("🚫 DISABLE All Automatic Deals", callback_data="admin_disable_auto_deals")],
            [InlineKeyboardButton("🟢 ENABLE All Automatic Deals", callback_data="admin_enable_auto_deals")]
        ])
    
    keyboard.extend([
        [InlineKeyboardButton("➕ Add New Manual Hot Deal", callback_data="admin_add_hot_deal")],
        [InlineKeyboardButton("⬅️ Back to Hot Deals Menu", callback_data="admin_hot_deals_menu")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_edit_hot_deal(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit existing hot deal"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid deal selection", show_alert=True)
        return
    
    deal_id = params[0]
    
    # Get deal details
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT hd.*, p.product_type, p.size, p.city, p.district
            FROM hot_deals hd
            JOIN products p ON hd.product_id = p.id
            WHERE hd.id = %s
        """, (deal_id,))
        deal = c.fetchone()
    except Exception as e:
        logger.error(f"Error loading deal {deal_id}: {e}")
        await query.answer("Error loading deal", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not deal:
        await query.answer("Deal not found", show_alert=True)
        return
    
    from utils import get_product_emoji
    emoji = get_product_emoji(deal['product_type'])
    status = "🟢 Active" if deal['is_active'] else "🔴 Inactive"
    
    msg = f"✏️ **EDIT HOT DEAL** ✏️\n\n"
    msg += f"📦 **Product:** {emoji} {deal['product_type']} {deal['size']}\n"
    msg += f"📍 **Location:** {deal['city']}/{deal['district']}\n"
    msg += f"🏷️ **Title:** {deal['deal_title']}\n"
    msg += f"💰 **Original Price:** {deal['original_price']:.2f}€\n"
    msg += f"🔥 **Deal Price:** {deal['deal_price']:.2f}€\n"
    if deal['discount_percentage'] > 0:
        msg += f"📊 **Discount:** {deal['discount_percentage']:.1f}%\n"
    if deal['quantity_limit']:
        msg += f"🔢 **Stock Limit:** {deal['quantity_limit']} units\n"
    else:
        msg += f"🔢 **Stock Limit:** Unlimited\n"
    msg += f"📊 **Status:** {status}\n\n"
    msg += "**Choose action:**"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Toggle Active/Inactive", callback_data=f"admin_toggle_hot_deal|{deal_id}")],
        [InlineKeyboardButton("🗑️ Delete Deal", callback_data=f"admin_delete_hot_deal|{deal_id}")],
        [InlineKeyboardButton("⬅️ Back to Deals", callback_data="admin_manage_hot_deals")],
        [InlineKeyboardButton("🏠 Back to Hot Deals Menu", callback_data="admin_hot_deals_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_toggle_hot_deal(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle hot deal active status"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid deal selection", show_alert=True)
        return
    
    deal_id = params[0]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Toggle status
        c.execute("UPDATE hot_deals SET is_active = NOT is_active WHERE id = %s", (deal_id,))
        conn.commit()
        
        # Get new status
        c.execute("SELECT is_active FROM hot_deals WHERE id = %s", (deal_id,))
        result = c.fetchone()
        new_status = "activated" if result['is_active'] else "deactivated"
        
        await query.answer(f"✅ Deal {new_status} successfully!", show_alert=True)
        
        # Redirect back to edit screen
        await handle_admin_edit_hot_deal(update, context, [deal_id])
        
    except Exception as e:
        logger.error(f"Error toggling deal {deal_id}: {e}")
        await query.answer("❌ Error updating deal", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_admin_delete_hot_deal(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Delete hot deal"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid deal selection", show_alert=True)
        return
    
    deal_id = params[0]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Delete deal
        c.execute("DELETE FROM hot_deals WHERE id = %s", (deal_id,))
        
        # Verify deletion
        c.execute("SELECT COUNT(*) as count FROM hot_deals WHERE id = %s", (deal_id,))
        remaining = c.fetchone()['count']
        
        conn.commit()
        
        if remaining == 0:
            await query.answer("✅ Deal deleted successfully!", show_alert=True)
            logger.info(f"Hot deal {deal_id} successfully deleted from database")
        else:
            await query.answer("⚠️ Deal deletion may have failed", show_alert=True)
            logger.warning(f"Hot deal {deal_id} deletion failed - still exists in database")
        
        # Force refresh by redirecting back to manage deals
        await handle_admin_manage_hot_deals(update, context)
        
    except Exception as e:
        logger.error(f"Error deleting deal {deal_id}: {e}")
        await query.answer("❌ Error deleting deal", show_alert=True)
    finally:
        if conn:
            conn.close()

# MODE: SIMPLE AUTO DEALS CONTROL - DUMMY PROOF
async def handle_admin_disable_auto_deals(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """SIMPLE: Disable all automatic hot deals"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Simple update - disable automatic deals
        c.execute("""
            UPDATE hot_deals_settings 
            SET setting_value = FALSE, updated_by = %s, updated_at = CURRENT_TIMESTAMP 
            WHERE setting_name = 'auto_deals_enabled'
        """, (query.from_user.id,))
        
        conn.commit()
        
        await query.answer("🚫 Automatic deals DISABLED! Users won't see them anymore.", show_alert=True)
        
        # Refresh the management page
        await handle_admin_manage_hot_deals(update, context)
        
    except Exception as e:
        logger.error(f"Error disabling auto deals: {e}")
        await query.answer("❌ Error disabling automatic deals", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_admin_enable_auto_deals(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """SIMPLE: Enable all automatic hot deals"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Simple update - enable automatic deals
        c.execute("""
            UPDATE hot_deals_settings 
            SET setting_value = TRUE, updated_by = %s, updated_at = CURRENT_TIMESTAMP 
            WHERE setting_name = 'auto_deals_enabled'
        """, (query.from_user.id,))
        
        conn.commit()
        
        await query.answer("🟢 Automatic deals ENABLED! Users will see them again.", show_alert=True)
        
        # Refresh the management page
        await handle_admin_manage_hot_deals(update, context)
        
    except Exception as e:
        logger.error(f"Error enabling auto deals: {e}")
        await query.answer("❌ Error enabling automatic deals", show_alert=True)
    finally:
        if conn:
            conn.close()

# MODE: APP INFO MANAGEMENT SYSTEM FOR ADMINS
async def handle_admin_app_info_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin app info management menu"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get current app info count
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM app_info WHERE is_active = TRUE")
        active_info = c.fetchone()['count']
    except Exception as e:
        logger.error(f"Error getting app info count: {e}")
        active_info = 0
    finally:
        if conn:
            conn.close()
    
    msg = "ℹ️ **APP INFO MANAGER** ℹ️\n\n"
    msg += "📱 **Manage App Information**\n\n"
    msg += f"📊 **Active Info Items:** {active_info}\n\n"
    msg += "🎯 *Create custom app information*\n"
    msg += "📝 *Add your username, channel, contact info*\n"
    msg += "💎 *Manage app details and descriptions*"
    
    keyboard = [
        [InlineKeyboardButton("➕ Add New Info", callback_data="admin_add_app_info")],
        [InlineKeyboardButton("📋 Manage Existing Info", callback_data="admin_manage_app_info")],
        [InlineKeyboardButton("👀 Preview App Info", callback_data="modern_app")],
        [InlineKeyboardButton("⬅️ Back to Marketing", callback_data="marketing_promotions_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_add_app_info(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start adding new app info"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    context.user_data['state'] = 'awaiting_app_info_title'
    
    msg = "➕ **ADD NEW APP INFO** ➕\n\n"
    msg += "📝 **Enter Info Title:**\n\n"
    msg += "Examples:\n"
    msg += "• Contact Information\n"
    msg += "• Channel Links\n"
    msg += "• Support Details\n"
    msg += "• About Us\n\n"
    msg += "🎯 **Type the title for this info section:**"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_app_info_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_app_info_title_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle app info title input from admin"""
    user_id = update.effective_user.id
    
    if context.user_data.get('state') != 'awaiting_app_info_title':
        return
    
    if not is_primary_admin(user_id):
        return
    
    title = update.message.text.strip()
    
    if not title or len(title) < 2:
        await update.message.reply_text(
            "❌ **Invalid Title**\n\nTitle must be at least 2 characters long.\n\n💬 **Try again:**"
        )
        return
    
    if len(title) > 100:
        await update.message.reply_text(
            "❌ **Title Too Long**\n\nTitle must be 100 characters or less.\n\n💬 **Try again:**"
        )
        return
    
    # Store title and ask for content
    context.user_data['app_info_title'] = title
    context.user_data['state'] = 'awaiting_app_info_content'
    
    msg = f"📝 **APP INFO: {title}**\n\n"
    msg += "✍️ **Enter Content:**\n\n"
    msg += "💡 **You can include:**\n"
    msg += "• Contact details\n"
    msg += "• Links\n"
    msg += "• Instructions\n"
    msg += "• Multiple lines\n\n"
    msg += "🎯 **Type the content for this info section:**"
    
    keyboard = [
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_app_info_menu")]
    ]
    
    await update.message.reply_text(
        msg, 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode='Markdown'
    )

async def handle_app_info_content_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle app info content input from admin"""
    user_id = update.effective_user.id
    
    if context.user_data.get('state') != 'awaiting_app_info_content':
        return
    
    if not is_primary_admin(user_id):
        return
    
    content = update.message.text.strip()
    
    if not content or len(content) < 5:
        await update.message.reply_text(
            "❌ **Content Too Short**\n\nContent must be at least 5 characters long.\n\n💬 **Try again:**"
        )
        return
    
    if len(content) > 2000:
        await update.message.reply_text(
            "❌ **Content Too Long**\n\nContent must be 2000 characters or less.\n\n💬 **Try again:**"
        )
        return
    
    # Get title from context
    title = context.user_data.get('app_info_title')
    if not title:
        await update.message.reply_text("❌ Error: Title lost. Please start again.")
        context.user_data['state'] = None
        return
    
    # Save to database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            INSERT INTO app_info 
            (info_title, info_content, is_active, display_order, created_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            title,
            content,
            True,
            0,  # Default order
            user_id
        ))
        
        conn.commit()
        
        # Clear state
        context.user_data['state'] = None
        context.user_data['app_info_title'] = None
        
        msg = "✅ **APP INFO CREATED** ✅\n\n"
        msg += f"📝 **Title:** {title}\n"
        msg += f"📄 **Content:** {content[:100]}{'...' if len(content) > 100 else ''}\n\n"
        msg += "🚀 **Info is now active and visible to users!**"
        
        keyboard = [
            [InlineKeyboardButton("📱 View App Info", callback_data="modern_app")],
            [InlineKeyboardButton("🔧 Manage App Info", callback_data="admin_app_info_menu")],
            [InlineKeyboardButton("⬅️ Back to Marketing", callback_data="marketing_promotions_menu")]
        ]
        
        await update.message.reply_text(
            msg, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Error creating app info: {e}")
        await update.message.reply_text("❌ Error creating app info. Please try again.")
        context.user_data['state'] = None
        context.user_data['app_info_title'] = None
    finally:
        if conn:
            conn.close()

async def handle_admin_manage_app_info(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Manage existing app info"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get existing app info
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT id, info_title, info_content, is_active, display_order, created_at
            FROM app_info
            ORDER BY is_active DESC, display_order ASC, created_at DESC
            LIMIT 10
        """)
        info_items = c.fetchall()
    except Exception as e:
        logger.error(f"Error loading app info: {e}")
        info_items = []
    finally:
        if conn:
            conn.close()
    
    if not info_items:
        await query.edit_message_text(
            "📋 **NO APP INFO FOUND** 📋\n\n"
            "No app info created yet.\n"
            "Create your first info item!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add New Info", callback_data="admin_add_app_info")],
                [InlineKeyboardButton("⬅️ Back to App Info", callback_data="admin_app_info_menu")]
            ]),
            parse_mode='Markdown'
        )
        return
    
    msg = "📱 **APP INFO MANAGER** 📱\n\n"
    msg += f"**Found {len(info_items)} info items:**\n\n"
    
    keyboard = []
    
    # MODE: Show detailed info with simple actions
    for i, info in enumerate(info_items, 1):
        status = "✅ ACTIVE" if info['is_active'] else "❌ INACTIVE"
        title = info['info_title']
        content_preview = info['info_content'][:50] + "..." if len(info['info_content']) > 50 else info['info_content']
        
        msg += f"**{i}. {title}**\n"
        msg += f"   Status: {status}\n"
        msg += f"   Content: {content_preview}\n\n"
        
        # Simple action buttons for each item
        keyboard.extend([
            [InlineKeyboardButton(f"✏️ EDIT: {title[:15]}...", callback_data=f"admin_edit_app_info|{info['id']}")],
            [InlineKeyboardButton(f"🔄 TOGGLE: {title[:15]}...", callback_data=f"admin_toggle_info_status|{info['id']}"),
             InlineKeyboardButton(f"🗑️ DELETE: {title[:15]}...", callback_data=f"admin_delete_app_info|{info['id']}")]
        ])
    
    keyboard.extend([
        [InlineKeyboardButton("➕ ADD NEW INFO ITEM", callback_data="admin_add_app_info")],
        [InlineKeyboardButton("⬅️ BACK TO MENU", callback_data="admin_app_info_menu")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# MODE: MISSING DELETE HANDLER - DUMMY PROOF
async def handle_admin_delete_app_info(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Delete app info -  MODE DUMMY PROOF"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid info selection", show_alert=True)
        return
    
    info_id = params[0]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get info title for confirmation
        c.execute("SELECT info_title FROM app_info WHERE id = %s", (info_id,))
        info = c.fetchone()
        
        if not info:
            await query.answer("Info not found", show_alert=True)
            return
        
        # MODE: Simple delete - no confirmation needed
        c.execute("DELETE FROM app_info WHERE id = %s", (info_id,))
        deleted_count = c.rowcount
        conn.commit()
        
        if deleted_count > 0:
            await query.answer(f"✅ '{info['info_title'][:20]}...' DELETED!", show_alert=True)
            logger.info(f"Admin {query.from_user.id} deleted app info: {info['info_title']}")
        else:
            await query.answer("❌ Delete failed - info not found", show_alert=True)
        
        # Refresh the manage page
        await handle_admin_manage_app_info(update, context)
        
    except Exception as e:
        logger.error(f"Error deleting app info {info_id}: {e}")
        await query.answer("❌ Error deleting info", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_admin_edit_app_info(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit existing app info"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid info selection", show_alert=True)
        return
    
    info_id = params[0]
    
    # Get info details
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT id, info_title, info_content, is_active, display_order, created_at
            FROM app_info WHERE id = %s
        """, (info_id,))
        info = c.fetchone()
    except Exception as e:
        logger.error(f"Error loading app info {info_id}: {e}")
        await query.answer("Error loading info", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    if not info:
        await query.answer("Info not found", show_alert=True)
        return
    
    status_text = "🟢 Active" if info['is_active'] else "🔴 Inactive"
    content_preview = info['info_content'][:100] + "..." if len(info['info_content']) > 100 else info['info_content']
    
    msg = f"✏️ **EDIT APP INFO** ✏️\n\n"
    msg += f"📝 **Title:** {info['info_title']}\n"
    msg += f"📊 **Status:** {status_text}\n"
    msg += f"📅 **Created:** {info['created_at'].strftime('%Y-%m-%d %H:%M')}\n\n"
    msg += f"📄 **Content Preview:**\n{content_preview}\n\n"
    msg += "🎯 **Choose Action:**"
    
    keyboard = [
        [InlineKeyboardButton("✏️ Edit Content", callback_data=f"admin_edit_info_content|{info_id}")],
        [InlineKeyboardButton("🔄 Toggle Status", callback_data=f"admin_toggle_info_status|{info_id}")],
        [InlineKeyboardButton("🗑️ Delete Info", callback_data=f"admin_delete_app_info|{info_id}")],
        [InlineKeyboardButton("⬅️ Back to List", callback_data="admin_manage_app_info")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_admin_toggle_info_status(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle app info active status"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid info selection", show_alert=True)
        return
    
    info_id = params[0]
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Toggle status
        c.execute("""
            UPDATE app_info 
            SET is_active = NOT is_active, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (info_id,))
        
        conn.commit()
        await query.answer("✅ Status updated successfully!", show_alert=True)
        
        # Redirect back to edit screen
        return await handle_admin_edit_app_info(update, context, [info_id])
        
    except Exception as e:
        logger.error(f"Error toggling app info status: {e}")
        await query.answer("❌ Error updating status", show_alert=True)
    finally:
        if conn:
            conn.close()

# MODE: VISUAL BUTTON BOARD EDITOR SYSTEM
# Available buttons for different menu types
AVAILABLE_BUTTONS = {
    'start_menu': [
        {'text': '🛍️ Shop', 'callback': 'shop', 'emoji': '🛍️'},
        {'text': '👤 Profile', 'callback': 'profile', 'emoji': '👤'},
        {'text': '💳 Top Up', 'callback': 'topup', 'emoji': '💳'},
        {'text': '🎁 Daily Rewards', 'callback': 'daily_rewards_menu', 'emoji': '🎁'},
        {'text': '🔥 Hot Deals', 'callback': 'modern_deals', 'emoji': '🔥'},
        {'text': 'ℹ️ Info', 'callback': 'info', 'emoji': 'ℹ️'},
        {'text': '⭐ Reviews', 'callback': 'reviews', 'emoji': '⭐'},
        {'text': '📋 Price List', 'callback': 'price_list', 'emoji': '📋'},
        {'text': '🌐 Language', 'callback': 'language', 'emoji': '🌐'},
        {'text': '🎁 Promotions', 'callback': 'promotions', 'emoji': '🎁'},
        {'text': '📞 Support', 'callback': 'support', 'emoji': '📞'}
    ],
    'city_menu': [
        {'text': '🏙️ Vilnius', 'callback': 'city_vilnius', 'emoji': '🏙️'},
        {'text': '🏙️ Kaunas', 'callback': 'city_kaunas', 'emoji': '🏙️'},
        {'text': '🏙️ Klaipeda', 'callback': 'city_klaipeda', 'emoji': '🏙️'},
        {'text': '🏙️ Siauliai', 'callback': 'city_siauliai', 'emoji': '🏙️'},
        {'text': '⬅️ Back', 'callback': 'back', 'emoji': '⬅️'},
        {'text': '🏠 Home', 'callback': 'home', 'emoji': '🏠'}
    ],
    'district_menu': [
        {'text': '🏘️ Centras', 'callback': 'district_centras', 'emoji': '🏘️'},
        {'text': '🏘️ Naujamestis', 'callback': 'district_naujamestis', 'emoji': '🏘️'},
        {'text': '🏘️ Senamiestis', 'callback': 'district_senamiestis', 'emoji': '🏘️'},
        {'text': '⬅️ Back to Cities', 'callback': 'back_cities', 'emoji': '⬅️'},
        {'text': '🏠 Home', 'callback': 'home', 'emoji': '🏠'}
    ],
    'payment_menu': [
        {'text': '💳 Pay Now', 'callback': 'pay_now', 'emoji': '💳'},
        {'text': '🎫 Discount Code', 'callback': 'discount', 'emoji': '🎫'},
        {'text': '🎁 Referral Code', 'callback': 'referral_code', 'emoji': '🎁'},
        {'text': '💰 Add to Wallet', 'callback': 'add_wallet', 'emoji': '💰'},
        {'text': '🛒 Add to Cart', 'callback': 'add_cart', 'emoji': '🛒'},
        {'text': '⬅️ Back', 'callback': 'back', 'emoji': '⬅️'},
        {'text': '🏠 Home', 'callback': 'home', 'emoji': '🏠'}
    ]
}

# Preset templates
PRESET_TEMPLATES = {
    'classic': {
        'name': 'Classic Layout',
        'description': 'Original 6-button layout (2x3 grid)',
        'menus': {
            'start_menu': [
                ['🛍️ Shop'],
                ['👤 Profile', '💳 Top Up'], 
                ['📝 Reviews', '📋 Price List', '🌐 Language']
            ],
            'city_menu': [['🏙️ Vilnius', '🏙️ Kaunas'], ['🏙️ Klaipeda', '🏙️ Siauliai'], ['⬅️ Back', '🏠 Home']],
            'district_menu': [['🏘️ Centras', '🏘️ Naujamestis'], ['🏘️ Senamiestis'], ['⬅️ Back to Cities', '🏠 Home']],
            'payment_menu': [['💳 Pay Now'], ['🎫 Discount Code', '🎁 Referral Code'], ['⬅️ Back', '🏠 Home']]
        }
    },
    'minimalist': {
        'name': 'Minimalist Layout',
        'description': 'Clean 3-button layout - Shop on top, Profile and Top Up below',
        'menus': {
            'start_menu': [
                ['🛍️ Shop'], 
                ['👤 Profile', '💳 Top Up']
            ],
            'city_menu': [['🏙️ Vilnius', '🏙️ Kaunas'], ['🏙️ Klaipeda', '🏙️ Siauliai'], ['⬅️ Back', '🏠 Home']],
            'district_menu': [['🏘️ Centras', '🏘️ Naujamestis'], ['🏘️ Senamiestis'], ['⬅️ Back to Cities', '🏠 Home']],
            'payment_menu': [['💳 Pay Now'], ['🎫 Discount Code', '🎁 Referral Code'], ['⬅️ Back', '🏠 Home']]
        }
    },
    'modern': {
        'name': 'Modern Grid',
        'description': '2x2 grid layout with deals',
        'menus': {
            'start_menu': [['🛍️ Shop', '🔥 Hot Deals'], ['👤 Profile', '💳 Top Up']],
            'city_menu': [['🏙️ Vilnius', '🏙️ Kaunas'], ['🏙️ Klaipeda', '🏙️ Siauliai'], ['⬅️ Back', '🏠 Home']],
            'district_menu': [['🏘️ Centras', '🏘️ Naujamestis'], ['🏘️ Senamiestis'], ['⬅️ Back to Cities', '🏠 Home']],
            'payment_menu': [['💳 Pay Now'], ['🎫 Discount Code', '🎁 Referral Code'], ['⬅️ Back', '🏠 Home']]
        }
    },
    'gaming': {
        'name': 'Gaming Focus',
        'description': 'Gaming-oriented layout',
        'menus': {
            'start_menu': [['🛍️ Shop', '🎮 Games'], ['🏆 Leaderboard', '🔥 Hot Deals'], ['👤 Profile', '💳 Top Up']],
            'city_menu': [['🏙️ Vilnius', '🏙️ Kaunas'], ['🏙️ Klaipeda', '🏙️ Siauliai'], ['⬅️ Back', '🏠 Home']],
            'district_menu': [['🏘️ Centras', '🏘️ Naujamestis'], ['🏘️ Senamiestis'], ['⬅️ Back to Cities', '🏠 Home']],
            'payment_menu': [['💳 Pay Now'], ['🎫 Discount Code', '🎁 Referral Code'], ['⬅️ Back', '🏠 Home']]
        }
    }
}

async def handle_admin_bot_look_editor(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Main bot look editor - choice between presets and custom"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "🎛️ **EDIT BOT LOOK** 🎛️\n\n"
    msg += "🎨 **Customize Your Bot's Interface**\n\n"
    msg += "Choose how you want to design your bot:\n\n"
    msg += "📋 **Preset Templates** - Quick setup with professional layouts\n"
    msg += "🎨 **Make Your Own** - Full custom visual editor\n\n"
    msg += "💡 *You can customize button layouts for all menus*"
    
    keyboard = [
        [InlineKeyboardButton("📋 Preset Templates", callback_data="bot_look_presets")],
        [InlineKeyboardButton("🎨 Make Your Own", callback_data="bot_look_custom")],
        [InlineKeyboardButton("👀 Preview Current Layout", callback_data="bot_look_preview")],
        [InlineKeyboardButton("⬅️ Back to Marketing", callback_data="marketing_promotions_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_bot_look_presets(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show preset template options"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "📋 **PRESET TEMPLATES** 📋\n\n"
    
    keyboard = []
    
    # Add built-in preset templates
    msg += "🎯 **Built-in Professional Layouts:**\n\n"
    for template_key, template_data in PRESET_TEMPLATES.items():
        msg += f"**{template_data['name']}**\n"
        msg += f"*{template_data['description']}*\n\n"
        
        keyboard.append([InlineKeyboardButton(
            f"📋 {template_data['name']}", 
            callback_data=f"bot_preset_select|{template_key}"
        )])
    
    # Add custom templates from database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT template_name, template_description, created_by
            FROM bot_layout_templates 
            WHERE is_preset = FALSE
            ORDER BY created_at DESC
            LIMIT 10
        """)
        
        custom_templates = c.fetchall()
        
        if custom_templates:
            msg += "\n🎨 **Your Custom Templates:**\n\n"
            for template in custom_templates:
                template_name = template['template_name']
                description = template['template_description'] or "Custom layout"
                
                msg += f"**{template_name}**\n"
                msg += f"*{description}*\n\n"
                
                keyboard.append([InlineKeyboardButton(
                    f"🎨 {template_name}", 
                    callback_data=f"bot_custom_select|{template_name}"
                )])
    
    except Exception as e:
        logger.error(f"Error loading custom templates: {e}")
    finally:
        if conn:
            conn.close()
    
    keyboard.extend([
        [InlineKeyboardButton("🎨 Make Your Own Instead", callback_data="bot_look_custom")],
        [InlineKeyboardButton("⬅️ Back to Bot Look", callback_data="admin_bot_look_editor")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_bot_preset_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply selected preset template"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid template selection", show_alert=True)
        return
    
    template_key = params[0]
    template_data = PRESET_TEMPLATES.get(template_key)
    
    if not template_data:
        await query.answer("Template not found", show_alert=True)
        return
    
    # Save template to database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # MODE: Clear ALL custom layouts when selecting preset (DO THIS FIRST!)
        c.execute("DELETE FROM bot_menu_layouts")  # Clear all custom layouts
        c.execute("UPDATE bot_layout_templates SET is_active = FALSE")  # Clear all templates
        c.execute("UPDATE ui_themes SET is_active = FALSE")  # Clear all ui themes
        
        # Now activate the selected preset theme in ui_themes table
        c.execute("""
            UPDATE ui_themes SET is_active = TRUE 
            WHERE theme_name = %s
        """, (template_key,))
        
        # If theme doesn't exist, insert it
        c.execute("SELECT COUNT(*) as count FROM ui_themes WHERE theme_name = %s", (template_key,))
        if c.fetchone()['count'] == 0:
            c.execute("""
                INSERT INTO ui_themes (theme_name, is_active, welcome_message, button_layout, style_config)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                template_key,
                True,
                f"Welcome to our store! 🛍️\n\nChoose an option below:",
                str(template_data['menus']['start_menu']),
                str({'type': template_key})
            ))
        
        for menu_name, layout in template_data['menus'].items():
            display_name = menu_name.replace('_', ' ').title()
            try:
                c.execute("""
                    INSERT INTO bot_menu_layouts 
                    (menu_name, menu_display_name, button_layout, created_by)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (menu_name) 
                    DO UPDATE SET 
                        button_layout = EXCLUDED.button_layout,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    menu_name,
                    display_name,
                    json.dumps(layout),
                    query.from_user.id
                ))
            except Exception as conflict_error:
                logger.warning(f"ON CONFLICT failed for template menu {menu_name}, using simple INSERT: {conflict_error}")
                # Fallback: simple insert (since we deleted existing ones above)
                c.execute("""
                    INSERT INTO bot_menu_layouts 
                    (menu_name, menu_display_name, button_layout, created_by)
                    VALUES (%s, %s, %s, %s)
                """, (
                    menu_name,
                    display_name,
                    json.dumps(layout),
                    query.from_user.id
                ))
        
        conn.commit()
        
        msg = f"✅ **TEMPLATE APPLIED** ✅\n\n"
        msg += f"🎨 **{template_data['name']}** has been applied!\n\n"
        msg += f"📱 *{template_data['description']}*\n\n"
        msg += "Your bot now uses this layout for all menus.\n"
        msg += "You can still customize individual menus if needed."
        
        keyboard = [
            [InlineKeyboardButton("🎨 Customize Further", callback_data="bot_look_custom")],
            [InlineKeyboardButton("👀 Preview Layout", callback_data="bot_look_preview")],
            [InlineKeyboardButton("⬅️ Back to Templates", callback_data="bot_look_presets")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error applying preset template: {e}")
        await query.answer("❌ Error applying template", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_bot_custom_select(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Apply selected custom template"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid template selection", show_alert=True)
        return
    
    template_name = params[0]
    
    # Load custom template from database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT template_name, template_description, layout_config
            FROM bot_layout_templates 
            WHERE template_name = %s AND is_preset = FALSE
        """, (template_name,))
        
        template = c.fetchone()
        
        if not template:
            await query.answer("Template not found", show_alert=True)
            return
        
        import json
        layout_config = json.loads(template['layout_config'])
        
        # Clear existing menu layouts for this admin
        c.execute("DELETE FROM bot_menu_layouts WHERE created_by = %s", (query.from_user.id,))
        
        # Apply custom template layouts
        applied_menus = []
        for menu_name, menu_config in layout_config.items():
            display_name = menu_config.get('display_name', menu_name.replace('_', ' ').title())
            button_layout = menu_config.get('button_layout', [])
            
            try:
                c.execute("""
                    INSERT INTO bot_menu_layouts 
                    (menu_name, menu_display_name, button_layout, created_by)
                    VALUES (%s, %s, %s, %s)
                """, (
                    menu_name,
                    display_name,
                    json.dumps(button_layout),
                    query.from_user.id
                ))
                applied_menus.append(display_name)
            except Exception as menu_error:
                logger.error(f"Error applying menu {menu_name}: {menu_error}")
        
        conn.commit()
        
        msg = f"✅ **CUSTOM TEMPLATE APPLIED** ✅\n\n"
        msg += f"🎨 **Template:** `{template_name}`\n"
        msg += f"📋 **Applied Menus:** {len(applied_menus)}\n\n"
        
        if applied_menus:
            msg += "**Menus Updated:**\n"
            for menu in applied_menus:
                msg += f"• {menu}\n"
        
        msg += f"\n🚀 **Your custom layout is now active!**\n"
        msg += f"📱 **Test it:** Type `/start` to see your layout"
        
        keyboard = [
            [InlineKeyboardButton("🎨 Customize Further", callback_data="bot_look_custom")],
            [InlineKeyboardButton("👀 Preview Layout", callback_data="bot_look_preview")],
            [InlineKeyboardButton("⬅️ Back to Templates", callback_data="bot_look_presets")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error applying custom template: {e}")
        await query.answer("❌ Error applying template", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_bot_look_custom(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start custom menu editing - select which menu to edit"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "🎨 **MAKE YOUR OWN LAYOUT** 🎨\n\n"
    msg += "🎯 **Select Menu to Customize:**\n\n"
    msg += "Edit each menu's button layout using our visual editor.\n"
    msg += "You can arrange buttons in rows (max 4 per row).\n\n"
    msg += "**Available Menus:**"
    
    keyboard = [
        [InlineKeyboardButton("🏠 Start Menu", callback_data="bot_edit_menu|start_menu")],
        [InlineKeyboardButton("🏙️ Choose City Menu", callback_data="bot_edit_menu|city_menu")],
        [InlineKeyboardButton("🏘️ Choose District Menu", callback_data="bot_edit_menu|district_menu")],
        [InlineKeyboardButton("💳 Payment Menu", callback_data="bot_edit_menu|payment_menu")],
        [InlineKeyboardButton("💾 Save All Changes", callback_data="bot_save_layout")],
        [InlineKeyboardButton("⬅️ Back to Bot Look", callback_data="admin_bot_look_editor")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_bot_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Visual button board editor for specific menu"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid menu selection", show_alert=True)
        return
    
    menu_type = params[0]
    available_buttons = AVAILABLE_BUTTONS.get(menu_type, [])
    
    # Get current layout from context or database
    current_layout = context.user_data.get(f'editing_layout_{menu_type}', [[]])
    
    # Ensure we have at least one empty row
    if not current_layout or not any(current_layout):
        current_layout = [[]]
    
    menu_display_name = menu_type.replace('_', ' ').title()
    
    # Get current header message from context or database
    current_header = context.user_data.get(f'editing_header_{menu_type}')
    if not current_header:
        # Try to load from database
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT header_message FROM bot_menu_layouts WHERE menu_name = %s AND created_by = %s", 
                     (menu_type, query.from_user.id))
            result = c.fetchone()
            current_header = result['header_message'] if result else get_default_header_message(menu_type)
        except Exception as e:
            logger.error(f"Error loading header message: {e}")
            current_header = get_default_header_message(menu_type)
        finally:
            if conn:
                conn.close()
    
    msg = f"🎛️ **EDITING: {menu_display_name}** 🎛️\n\n"
    
    # Header Message Section
    msg += "💬 **Header Message / Welcome Text:**\n"
    processed_header = process_dynamic_variables(current_header)
    msg += f"```\n{processed_header}\n```\n"
    msg += "📝 _Click 'Edit Header' to customize this message_\n\n"
    
    msg += "🎯 **How to Use:**\n"
    msg += "1️⃣ Click a button from the top board to select it\n"
    msg += "2️⃣ Click an empty slot below to place it\n"
    msg += "3️⃣ Max 4 buttons per row\n\n"
    
    # Header: Available buttons board
    msg += "📋 **Available Buttons:**\n"
    
    keyboard = []
    
    # Create header board with available buttons (2 per row for readability)
    header_rows = []
    for i in range(0, len(available_buttons), 2):
        row = []
        for j in range(2):
            if i + j < len(available_buttons):
                btn = available_buttons[i + j]
                row.append(InlineKeyboardButton(
                    f"📌 {btn['text']}", 
                    callback_data=f"bot_select_button|{menu_type}|{i+j}"
                ))
        header_rows.append(row)
    
    keyboard.extend(header_rows)
    
    # Visual split line
    keyboard.append([InlineKeyboardButton("--- 📍 Placement Area Below ---", callback_data="bot_noop")])
    
    # Current layout preview with placement slots
    for row_idx, row in enumerate(current_layout):
        layout_row = []
        
        # Add existing buttons in this row
        for btn_idx, button_text in enumerate(row):
            layout_row.append(InlineKeyboardButton(
                f"✅ {button_text}", 
                callback_data=f"bot_remove_button|{menu_type}|{row_idx}|{btn_idx}"
            ))
        
        # Add empty slots if row has less than 4 buttons
        while len(layout_row) < 4:
            slot_idx = len(layout_row)
            layout_row.append(InlineKeyboardButton(
                "➕ Empty", 
                callback_data=f"bot_place_button|{menu_type}|{row_idx}|{slot_idx}"
            ))
        
        keyboard.append(layout_row)
    
    # Add new row button if we have less than 6 rows
    if len(current_layout) < 6:
        keyboard.append([InlineKeyboardButton("➕ Add New Row", callback_data=f"bot_add_row|{menu_type}")])
    
    # Control buttons
    keyboard.extend([
        [InlineKeyboardButton("📝 Edit Header", callback_data=f"bot_edit_header|{menu_type}"),
         InlineKeyboardButton("🔧 Variables", callback_data=f"bot_show_variables|{menu_type}")],
        [InlineKeyboardButton("💾 Save Menu", callback_data=f"bot_save_menu|{menu_type}")],
        [InlineKeyboardButton("🗑️ Clear All", callback_data=f"bot_clear_menu|{menu_type}")],
        [InlineKeyboardButton("⬅️ Back to Menu List", callback_data="bot_look_custom")]
    ])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_bot_edit_header(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Edit header message for a menu"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid menu selection", show_alert=True)
        return
    
    menu_type = params[0]
    menu_display_name = menu_type.replace('_', ' ').title()
    
    # Get current header message
    current_header = context.user_data.get(f'editing_header_{menu_type}')
    if not current_header:
        # Try to load from database
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT header_message FROM bot_menu_layouts WHERE menu_name = %s AND created_by = %s", 
                     (menu_type, query.from_user.id))
            result = c.fetchone()
            current_header = result['header_message'] if result else get_default_header_message(menu_type)
        except Exception as e:
            logger.error(f"Error loading header message: {e}")
            current_header = get_default_header_message(menu_type)
        finally:
            if conn:
                conn.close()
    
    # Set state for receiving new header message
    context.user_data['state'] = 'awaiting_header_message'
    context.user_data['editing_header_menu'] = menu_type
    
    msg = f"📝 **EDIT HEADER MESSAGE** 📝\n\n"
    msg += f"🎛️ **Menu:** {menu_display_name}\n\n"
    msg += f"**Current Header:**\n```\n{current_header}\n```\n\n"
    msg += f"**Preview with Variables:**\n"
    processed_header = process_dynamic_variables(current_header)
    msg += f"```\n{processed_header}\n```\n\n"
    msg += f"💬 **Type your new header message:**\n"
    msg += f"• Use variables like `{{user_mention}}`, `{{balance}}`, etc.\n"
    msg += f"• Use \\n for line breaks\n"
    msg += f"• Click 'Variables' to see all available options"
    
    keyboard = [
        [InlineKeyboardButton("🔧 Show Variables", callback_data=f"bot_show_variables|{menu_type}")],
        [InlineKeyboardButton("🔄 Reset to Default", callback_data=f"bot_reset_header|{menu_type}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"bot_edit_menu|{menu_type}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_bot_show_variables(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show available variables for a menu"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid menu selection", show_alert=True)
        return
    
    menu_type = params[0]
    menu_display_name = menu_type.replace('_', ' ').title()
    available_vars = get_available_variables(menu_type)
    
    msg = f"🔧 **AVAILABLE VARIABLES** 🔧\n\n"
    msg += f"🎛️ **Menu:** {menu_display_name}\n\n"
    msg += f"📋 **Variables you can use:**\n\n"
    
    for variable, description in available_vars.items():
        msg += f"`{variable}` - {description}\n"
    
    msg += f"\n💡 **Usage Examples:**\n"
    msg += f"• `Welcome back, {{user_mention}}!`\n"
    msg += f"• `Balance: {{balance}} EUR`\n"
    msg += f"• `You are a {{vip_level}} member`\n\n"
    msg += f"📝 **Copy and paste variables into your header message**"
    
    keyboard = [
        [InlineKeyboardButton("📝 Edit Header", callback_data=f"bot_edit_header|{menu_type}")],
        [InlineKeyboardButton("⬅️ Back to Editor", callback_data=f"bot_edit_menu|{menu_type}")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_bot_reset_header(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Reset header message to default"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid menu selection", show_alert=True)
        return
    
    menu_type = params[0]
    default_header = get_default_header_message(menu_type)
    
    # Store in context
    context.user_data[f'editing_header_{menu_type}'] = default_header
    
    await query.answer("✅ Header reset to default!", show_alert=True)
    
    # Return to header editor
    await handle_bot_edit_header(update, context, params)

async def handle_header_message_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle header message input from admin"""
    user_id = update.effective_user.id
    
    # Check if we're expecting a header message
    if context.user_data.get('state') != 'awaiting_header_message':
        return  # Not in header editing mode
    
    if not is_primary_admin(user_id):
        return
    
    menu_type = context.user_data.get('editing_header_menu')
    if not menu_type:
        await update.message.reply_text("❌ Error: Menu type not found. Please try again.")
        context.user_data['state'] = None
        return
    
    new_header = update.message.text.strip()
    
    # Validate header message
    if not new_header:
        await update.message.reply_text(
            "❌ **Empty Message**\n\nHeader message cannot be empty.\n\n💬 **Try again:**"
        )
        return
    
    if len(new_header) > 1000:
        await update.message.reply_text(
            "❌ **Message Too Long**\n\nHeader message must be 1000 characters or less.\n\n💬 **Try again:**"
        )
        return
    
    # Store the new header message in context
    context.user_data[f'editing_header_{menu_type}'] = new_header
    
    # Clear state
    context.user_data['state'] = None
    context.user_data['editing_header_menu'] = None
    
    # Show preview and success message
    processed_header = process_dynamic_variables(new_header)
    
    msg = "✅ **HEADER MESSAGE UPDATED** ✅\n\n"
    msg += f"**Your Message:**\n```\n{new_header}\n```\n\n"
    msg += f"**Preview with Variables:**\n```\n{processed_header}\n```\n\n"
    msg += f"💾 **Don't forget to save your menu!**"
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Back to Editor", callback_data=f"bot_edit_menu|{menu_type}")],
        [InlineKeyboardButton("💾 Save Menu Now", callback_data=f"bot_save_menu|{menu_type}")]
    ]
    
    await update.message.reply_text(
        msg, 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode='Markdown'
    )

async def handle_bot_select_button(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Select a button from the available buttons board"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params or len(params) < 2:
        await query.answer("Invalid button selection", show_alert=True)
        return
    
    menu_type = params[0]
    button_idx = int(params[1])
    
    available_buttons = AVAILABLE_BUTTONS.get(menu_type, [])
    if button_idx >= len(available_buttons):
        await query.answer("Button not found", show_alert=True)
        return
    
    selected_button = available_buttons[button_idx]
    context.user_data['selected_button'] = selected_button['text']
    
    await query.answer(f"✅ Selected: {selected_button['text']}\nNow click an empty slot to place it!", show_alert=True)

async def handle_bot_place_button(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Place selected button in the layout"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params or len(params) < 3:
        await query.answer("Invalid placement", show_alert=True)
        return
    
    menu_type = params[0]
    row_idx = int(params[1])
    slot_idx = int(params[2])
    
    selected_button = context.user_data.get('selected_button')
    if not selected_button:
        await query.answer("❌ No button selected! Click a button from the top board first.", show_alert=True)
        return
    
    # Get current layout
    current_layout = context.user_data.get(f'editing_layout_{menu_type}', [[]])
    
    # Ensure we have enough rows
    while len(current_layout) <= row_idx:
        current_layout.append([])
    
    # Ensure we have enough slots in the row
    while len(current_layout[row_idx]) <= slot_idx:
        current_layout[row_idx].append("")
    
    # Place the button
    current_layout[row_idx][slot_idx] = selected_button
    
    # Remove empty strings from the end of rows
    for row in current_layout:
        while row and row[-1] == "":
            row.pop()
    
    # Save layout
    context.user_data[f'editing_layout_{menu_type}'] = current_layout
    context.user_data['selected_button'] = None  # Clear selection
    
    await query.answer(f"✅ Placed: {selected_button}", show_alert=True)
    
    # Refresh the editor
    return await handle_bot_edit_menu(update, context, [menu_type])

async def handle_bot_remove_button(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Remove button from layout"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params or len(params) < 3:
        await query.answer("Invalid removal", show_alert=True)
        return
    
    menu_type = params[0]
    row_idx = int(params[1])
    btn_idx = int(params[2])
    
    # Get current layout
    current_layout = context.user_data.get(f'editing_layout_{menu_type}', [[]])
    
    if row_idx < len(current_layout) and btn_idx < len(current_layout[row_idx]):
        removed_button = current_layout[row_idx][btn_idx]
        current_layout[row_idx].pop(btn_idx)
        
        # Remove empty rows
        current_layout = [row for row in current_layout if row]
        if not current_layout:
            current_layout = [[]]
        
        context.user_data[f'editing_layout_{menu_type}'] = current_layout
        
        await query.answer(f"🗑️ Removed: {removed_button}", show_alert=True)
        
        # Refresh the editor
        return await handle_bot_edit_menu(update, context, [menu_type])
    
    await query.answer("❌ Button not found", show_alert=True)

async def handle_bot_add_row(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Add new empty row to layout"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid menu type", show_alert=True)
        return
    
    menu_type = params[0]
    
    # Get current layout
    current_layout = context.user_data.get(f'editing_layout_{menu_type}', [[]])
    
    if len(current_layout) < 6:  # Max 6 rows
        current_layout.append([])
        context.user_data[f'editing_layout_{menu_type}'] = current_layout
        
        await query.answer("✅ Added new row", show_alert=True)
        
        # Refresh the editor
        return await handle_bot_edit_menu(update, context, [menu_type])
    
    await query.answer("❌ Maximum 6 rows allowed", show_alert=True)

async def handle_bot_save_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save current menu layout"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid menu type", show_alert=True)
        return
    
    menu_type = params[0]
    current_layout = context.user_data.get(f'editing_layout_{menu_type}', [[]])
    current_header = context.user_data.get(f'editing_header_{menu_type}')
    
    # Clean up layout (remove empty rows and buttons)
    cleaned_layout = []
    for row in current_layout:
        cleaned_row = [btn for btn in row if btn and btn.strip()]
        if cleaned_row:
            cleaned_layout.append(cleaned_row)
    
    if not cleaned_layout:
        await query.answer("❌ Cannot save empty layout", show_alert=True)
        return
    
    # Use default header if none set
    if not current_header:
        current_header = get_default_header_message(menu_type)
    
    # Save to database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        import json
        menu_display_name = menu_type.replace('_', ' ').title()
        
        # Check if we're editing a preset theme
        editing_preset = context.user_data.get('editing_preset_theme')
        if editing_preset:
            # Save back to the preset theme (overwrite existing)
            c.execute("UPDATE ui_themes SET is_active = FALSE")  # Clear all
            c.execute("""
                INSERT INTO ui_themes (theme_name, is_active, welcome_message, button_layout, style_config)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (theme_name) 
                DO UPDATE SET 
                    button_layout = EXCLUDED.button_layout,
                    welcome_message = EXCLUDED.welcome_message,
                    is_active = TRUE
            """, (
                editing_preset,
                True,
                current_header,
                json.dumps(cleaned_layout),
                json.dumps({'type': editing_preset})
            ))
            conn.commit()
            
            # Clear editing context
            context.user_data.pop('editing_preset_theme', None)
            context.user_data.pop(f'editing_layout_{menu_type}', None)
            context.user_data.pop(f'editing_header_{menu_type}', None)
            
            success_msg = f"✅ **PRESET THEME UPDATED**\n\n"
            success_msg += f"**Theme:** `{editing_preset.upper()}`\n\n"
            success_msg += "Your changes have been saved to the preset theme.\n"
            success_msg += "The theme is now active with your modifications.\n\n"
            success_msg += "Returning to Theme Management Center..."
            
            await query.edit_message_text(success_msg, parse_mode='Markdown')
            
            # Wait 2 seconds then return to theme designer
            import asyncio
            await asyncio.sleep(2)
            await handle_ui_theme_designer(update, context)
            return
        
        # Regular custom theme save logic
        # Update or insert menu layout (with fallback for databases without unique constraint)
        try:
            c.execute("""
                INSERT INTO bot_menu_layouts 
                (menu_name, menu_display_name, button_layout, header_message, created_by)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (menu_name) 
                DO UPDATE SET 
                    button_layout = EXCLUDED.button_layout,
                    header_message = EXCLUDED.header_message,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                menu_type,
                menu_display_name,
                json.dumps(cleaned_layout),
                current_header,
                query.from_user.id
            ))
        except Exception as conflict_error:
            logger.warning(f"ON CONFLICT failed, trying manual upsert: {conflict_error}")
            # Fallback: manual check and update/insert
            c.execute("SELECT id FROM bot_menu_layouts WHERE menu_name = %s", (menu_type,))
            existing = c.fetchone()
            
            if existing:
                # Update existing
                c.execute("""
                    UPDATE bot_menu_layouts 
                    SET button_layout = %s, header_message = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE menu_name = %s
                """, (json.dumps(cleaned_layout), current_header, menu_type))
            else:
                # Insert new
                c.execute("""
                    INSERT INTO bot_menu_layouts 
                    (menu_name, menu_display_name, button_layout, header_message, created_by)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    menu_type,
                    menu_display_name,
                    json.dumps(cleaned_layout),
                    current_header,
                    query.from_user.id
                ))
        
        # AUTO-ACTIVATE: Deactivate all preset themes and activate custom layout
        c.execute("UPDATE ui_themes SET is_active = FALSE")
        c.execute("UPDATE bot_menu_layouts SET is_active = TRUE WHERE menu_name = %s", (menu_type,))
        
        conn.commit()
        
        await query.answer(f"✅ {menu_display_name} saved and activated!", show_alert=True)
        
        # DON'T clear editing data - keep it for global save
        # The editing context will be cleared by global save or when user exits
        
        # Return to menu list
        return await handle_bot_look_custom(update, context)
        
    except Exception as e:
        logger.error(f"Error saving menu layout: {e}")
        await query.answer("❌ Error saving menu", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_bot_clear_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Clear all buttons from current menu layout"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    if not params:
        await query.answer("Invalid menu type", show_alert=True)
        return
    
    menu_type = params[0]
    
    # Clear the layout
    context.user_data[f'editing_layout_{menu_type}'] = [[]]
    
    await query.answer("🗑️ Menu cleared!", show_alert=True)
    
    # Refresh the editor
    return await handle_bot_edit_menu(update, context, [menu_type])

async def handle_bot_save_layout(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Save all menu layouts (global save)"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get all editing layouts from context AND check for existing saved layouts
    menu_types = ['start_menu', 'city_menu', 'district_menu', 'payment_menu']
    saved_menus = []
    errors = []
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        import json
        
        # Check if there are ANY editing contexts or existing saved layouts
        has_editing_data = any(f'editing_layout_{menu_type}' in context.user_data for menu_type in menu_types)
        
        # Also check for existing saved layouts in database
        c.execute("SELECT COUNT(*) as count FROM bot_menu_layouts WHERE created_by = %s", (query.from_user.id,))
        existing_layouts_count = c.fetchone()['count']
        
        if not has_editing_data and existing_layouts_count == 0:
            # No editing data and no existing layouts
            msg = "ℹ️ **NO CHANGES TO SAVE** ℹ️\n\n"
            msg += "No menu layouts were found in editing mode and no existing layouts found.\n"
            msg += "Edit some menus first, then save."
            
            keyboard = [
                [InlineKeyboardButton("🎨 Continue Editing", callback_data="bot_look_custom")],
                [InlineKeyboardButton("📋 Use Preset Template", callback_data="bot_look_presets")],
                [InlineKeyboardButton("⬅️ Back to Bot Look", callback_data="admin_bot_look_editor")]
            ]
            
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return
        
        for menu_type in menu_types:
            layout_key = f'editing_layout_{menu_type}'
            if layout_key in context.user_data:
                current_layout = context.user_data[layout_key]
                
                # Clean up layout (remove empty rows and buttons)
                cleaned_layout = []
                for row in current_layout:
                    cleaned_row = [btn for btn in row if btn and btn.strip()]
                    if cleaned_row:
                        cleaned_layout.append(cleaned_row)
                
                if cleaned_layout:  # Only save non-empty layouts
                    menu_display_name = menu_type.replace('_', ' ').title()
                    
                    try:
                        # Update or insert menu layout (with fallback)
                        try:
                            c.execute("""
                                INSERT INTO bot_menu_layouts 
                                (menu_name, menu_display_name, button_layout, created_by)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (menu_name) 
                                DO UPDATE SET 
                                    button_layout = EXCLUDED.button_layout,
                                    updated_at = CURRENT_TIMESTAMP
                            """, (
                                menu_type,
                                menu_display_name,
                                json.dumps(cleaned_layout),
                                query.from_user.id
                            ))
                        except Exception as conflict_error:
                            logger.warning(f"ON CONFLICT failed for {menu_type}, trying manual upsert: {conflict_error}")
                            # Fallback: manual check and update/insert
                            c.execute("SELECT id FROM bot_menu_layouts WHERE menu_name = %s", (menu_type,))
                            existing = c.fetchone()
                            
                            if existing:
                                # Update existing
                                c.execute("""
                                    UPDATE bot_menu_layouts 
                                    SET button_layout = %s, updated_at = CURRENT_TIMESTAMP
                                    WHERE menu_name = %s
                                """, (json.dumps(cleaned_layout), menu_type))
                            else:
                                # Insert new
                                c.execute("""
                                    INSERT INTO bot_menu_layouts 
                                    (menu_name, menu_display_name, button_layout, created_by)
                                    VALUES (%s, %s, %s, %s)
                                """, (
                                    menu_type,
                                    menu_display_name,
                                    json.dumps(cleaned_layout),
                                    query.from_user.id
                                ))
                        
                        saved_menus.append(menu_display_name)
                        
                        # Clear editing data
                        del context.user_data[layout_key]
                        
                    except Exception as save_error:
                        logger.error(f"Error saving {menu_type}: {save_error}")
                        errors.append(f"{menu_display_name}: {str(save_error)}")
        
        if saved_menus:
            conn.commit()
        
        # MODE: Check if we're editing an existing custom theme
        editing_existing_theme = context.user_data.get('editing_custom_theme')
        
        if editing_existing_theme:
            # EDITING EXISTING THEME - Update the template and return to theme center
            try:
                # Get template info
                c.execute("SELECT template_name FROM bot_layout_templates WHERE id = %s", (editing_existing_theme,))
                template_info = c.fetchone()
                
                if template_info:
                    template_name = template_info['template_name']
                    
                    # Build layout config from saved menus
                    template_config = {}
                    for menu_type in menu_types:
                        c.execute("SELECT button_layout, header_message FROM bot_menu_layouts WHERE menu_name = %s AND created_by = %s", 
                                (menu_type, query.from_user.id))
                        menu_data = c.fetchone()
                        if menu_data:
                            template_config[menu_type] = {
                                'display_name': menu_type.replace('_', ' ').title(),
                                'button_layout': json.loads(menu_data['button_layout']),
                                'header_message': menu_data['header_message']
                            }
                    
                    # Update the existing template
                    c.execute("""
                        UPDATE bot_layout_templates 
                        SET layout_config = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (json.dumps(template_config), editing_existing_theme))
                    
                    conn.commit()
                    
                    # Clear editing context
                    del context.user_data['editing_custom_theme']
                    
                    # Success message and return to theme center
                    msg = "✅ **THEME UPDATED SUCCESSFULLY** ✅\n\n"
                    msg += f"🎨 **Updated Theme:** `{template_name}`\n"
                    msg += f"📋 **Updated Menus:** {len(saved_menus)}\n\n"
                    for menu in saved_menus:
                        msg += f"• {menu}\n"
                    msg += f"\n🚀 **Your changes have been saved to the existing theme!**\n"
                    msg += f"📱 **Test your updated theme:** Type `/start` to see it in action!"
                    
                    keyboard = [
                        [InlineKeyboardButton("📱 Preview Updated Theme", callback_data="preview_active_theme")],
                        [InlineKeyboardButton("🎨 Back to Theme Center", callback_data="ui_theme_designer")]
                    ]
                    
                    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
                    return
                    
            except Exception as update_error:
                logger.error(f"Error updating existing theme {editing_existing_theme}: {update_error}")
                # Fall through to regular save logic
        
        # CREATING NEW THEME - Regular save logic with naming step
        if saved_menus and not errors:
            msg = "✅ **ALL LAYOUTS SAVED** ✅\n\n"
            msg += f"🎨 **Saved {len(saved_menus)} menus:**\n"
            for menu in saved_menus:
                msg += f"• {menu}\n"
            msg += f"\n🎯 **Next Step: Name Your Custom Layout**\n"
            msg += f"Give your custom layout a name to save it as a template!"
        elif saved_menus and errors:
            msg = "⚠️ **PARTIALLY SAVED** ⚠️\n\n"
            msg += f"✅ **Saved {len(saved_menus)} menus:**\n"
            for menu in saved_menus:
                msg += f"• {menu}\n"
            msg += f"\n❌ **{len(errors)} errors:**\n"
            for error in errors:
                msg += f"• {error}\n"
        elif errors and not saved_menus:
            msg = "❌ **SAVE FAILED** ❌\n\n"
            msg += "No menus were saved due to errors:\n"
            for error in errors:
                msg += f"• {error}\n"
        else:
            msg = "ℹ️ **NO CHANGES TO SAVE** ℹ️\n\n"
            msg += "No menu layouts were found in editing mode.\n"
            msg += "Edit some menus first, then save."
        
        keyboard = [
            [InlineKeyboardButton("📝 Name This Layout", callback_data="bot_name_layout")],
            [InlineKeyboardButton("👀 Preview Layouts", callback_data="bot_look_preview")],
            [InlineKeyboardButton("🎨 Continue Editing", callback_data="bot_look_custom")],
            [InlineKeyboardButton("⬅️ Back to Bot Look", callback_data="admin_bot_look_editor")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in global save: {e}")
        await query.answer("❌ Error saving layouts", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_bot_name_layout(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Prompt admin to name their custom layout"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Check if there are saved layouts to name
    menu_types = ['start_menu', 'city_menu', 'district_menu', 'payment_menu']
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("SELECT COUNT(*) as count FROM bot_menu_layouts WHERE created_by = %s", (query.from_user.id,))
        existing_layouts_count = c.fetchone()['count']
        
        if existing_layouts_count == 0:
            await query.answer("❌ No layouts to name. Create some layouts first.", show_alert=True)
            return
        
        # Set state for receiving template name
        context.user_data['state'] = 'awaiting_template_name'
        
        msg = "📝 **NAME YOUR CUSTOM LAYOUT** 📝\n\n"
        msg += "🎨 **Create a Template**\n\n"
        msg += "Give your custom layout a **unique name** to save it as a reusable template.\n\n"
        msg += "**Examples:**\n"
        msg += "• `Gaming Layout`\n"
        msg += "• `Minimalist Design`\n"
        msg += "• `VIP Customer UI`\n"
        msg += "• `Quick Access Menu`\n\n"
        msg += "💬 **Type your template name:**"
        
        keyboard = [
            [InlineKeyboardButton("❌ Cancel", callback_data="bot_look_custom")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in name layout: {e}")
        await query.answer("❌ Error starting naming process", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_template_name_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle template name input from admin"""
    user_id = update.effective_user.id
    
    # Check if we're expecting a template name
    if context.user_data.get('state') != 'awaiting_template_name':
        return  # Not in naming mode
    
    if not is_primary_admin(user_id):
        return
    
    template_name = update.message.text.strip()
    
    # Validate template name
    if not template_name or len(template_name) < 2:
        await update.message.reply_text(
            "❌ **Invalid Name**\n\nTemplate name must be at least 2 characters long.\n\n💬 **Try again:**"
        )
        return
    
    if len(template_name) > 50:
        await update.message.reply_text(
            "❌ **Name Too Long**\n\nTemplate name must be 50 characters or less.\n\n💬 **Try again:**"
        )
        return
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check if template name already exists
        c.execute("SELECT id FROM bot_layout_templates WHERE template_name = %s", (template_name,))
        existing = c.fetchone()
        
        if existing:
            await update.message.reply_text(
                f"❌ **Name Already Exists**\n\nA template named `{template_name}` already exists.\n\n💬 **Choose a different name:**"
            )
            return
        
        # Get current layouts to save as template
        c.execute("""
            SELECT menu_name, menu_display_name, button_layout
            FROM bot_menu_layouts 
            WHERE created_by = %s AND is_active = TRUE
        """, (user_id,))
        
        layouts = c.fetchall()
        
        if not layouts:
            await update.message.reply_text(
                "❌ **No Layouts Found**\n\nNo saved layouts to create template from.\n\nCreate some layouts first."
            )
            context.user_data['state'] = None
            return
        
        # Create template configuration
        import json
        template_config = {}
        for layout in layouts:
            template_config[layout['menu_name']] = {
                'display_name': layout['menu_display_name'],
                'button_layout': json.loads(layout['button_layout'])
            }
        
        # Save as template
        c.execute("""
            INSERT INTO bot_layout_templates 
            (template_name, template_description, layout_config, is_preset, created_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            template_name,
            f"Custom template created by admin",
            json.dumps(template_config),
            False,  # Not a preset, it's custom
            user_id
        ))
        
        conn.commit()
        
        # Clear naming state
        context.user_data['state'] = None
        
        # Success message
        msg = "✅ **TEMPLATE CREATED** ✅\n\n"
        msg += f"🎨 **Template Name:** `{template_name}`\n"
        msg += f"📋 **Saved Menus:** {len(layouts)}\n\n"
        msg += "🚀 **Your template is now available in:**\n"
        msg += "• **UI Theme Designer** → **Preset Templates**\n"
        msg += "• **Edit Bot Look** → **Preset Templates**\n\n"
        msg += "📱 **Test your layout:** Type `/start` to see it in action!"
        
        keyboard = [
            [InlineKeyboardButton("🎨 UI Theme Designer", callback_data="ui_theme_designer")],
            [InlineKeyboardButton("🎛️ Edit Bot Look", callback_data="bot_look_custom")],
            [InlineKeyboardButton("⬅️ Back to Marketing", callback_data="marketing_promotions_menu")]
        ]
        
        await update.message.reply_text(
            msg, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Error creating template: {e}")
        await update.message.reply_text("❌ Error creating template. Please try again.")
        context.user_data['state'] = None
    finally:
        if conn:
            conn.close()

async def handle_bot_look_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Preview current bot layout configurations"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    # Get current saved layouts from database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT menu_name, menu_display_name, button_layout, updated_at
            FROM bot_menu_layouts 
            WHERE is_active = TRUE
            ORDER BY 
                CASE menu_name 
                    WHEN 'start_menu' THEN 1
                    WHEN 'city_menu' THEN 2
                    WHEN 'district_menu' THEN 3
                    WHEN 'payment_menu' THEN 4
                    ELSE 5
                END
        """)
        layouts = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error loading layout preview: {e}")
        layouts = []
    finally:
        if conn:
            conn.close()
    
    msg = "👀 **LAYOUT PREVIEW** 👀\n\n"
    
    if layouts:
        msg += f"🎨 **Current Bot Layouts ({len(layouts)} menus):**\n\n"
        
        import json
        for layout in layouts:
            menu_name = layout['menu_display_name']
            try:
                button_layout = json.loads(layout['button_layout'])
                updated = layout['updated_at'].strftime('%Y-%m-%d %H:%M') if layout['updated_at'] else 'Unknown'
                
                msg += f"**📋 {menu_name}:**\n"
                
                # Show button layout preview
                for row_idx, row in enumerate(button_layout):
                    if row:  # Only show non-empty rows
                        row_text = " | ".join([f"`{btn}`" for btn in row])
                        msg += f"Row {row_idx + 1}: {row_text}\n"
                
                msg += f"*Updated: {updated}*\n\n"
                
            except Exception as parse_error:
                logger.warning(f"Error parsing layout for {menu_name}: {parse_error}")
                msg += f"**📋 {menu_name}:** *(Error loading layout)*\n\n"
    else:
        msg += "ℹ️ **No Custom Layouts Found**\n\n"
        msg += "No custom button layouts have been saved yet.\n"
        msg += "The bot is using default layouts.\n\n"
        msg += "**To create custom layouts:**\n"
        msg += "1. Go to 🎨 Make Your Own\n"
        msg += "2. Edit menu layouts\n"
        msg += "3. Save your changes"
    
    keyboard = [
        [InlineKeyboardButton("🎨 Edit Layouts", callback_data="bot_look_custom")],
        [InlineKeyboardButton("📋 Use Preset", callback_data="bot_look_presets")],
        [InlineKeyboardButton("⬅️ Back to Bot Look", callback_data="admin_bot_look_editor")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# --- END OF FILE marketing_promotions.py ---
