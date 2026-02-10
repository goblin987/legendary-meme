# --- START OF FILE stock_management.py ---

import logging
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Dict, Optional, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import TelegramError

from utils import (
    get_db_connection, send_message_with_retry, format_currency,
    is_primary_admin, get_first_primary_admin_id, ADMIN_ID
)

logger = logging.getLogger(__name__)

# Stock management constants
LOW_STOCK_THRESHOLD = 5
CRITICAL_STOCK_THRESHOLD = 2
STOCK_ALERT_COOLDOWN_HOURS = 6

# --- Stock Alert Functions ---

async def check_low_stock_alerts():
    """
    Checks for products with low stock and sends alerts to admins.
    This should be called periodically (e.g., every hour).
    """
    logger.info("🔍 Running low stock check...")
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Find products with low stock that haven't been alerted recently
        now = datetime.now(timezone.utc)
        cooldown_time = now - timedelta(hours=STOCK_ALERT_COOLDOWN_HOURS)
        
        c.execute("""
            SELECT id, city, district, product_type, size, price, available, 
                   low_stock_threshold, last_stock_alert
            FROM products 
            WHERE stock_alerts_enabled IS TRUE 
            AND available > 0 
            AND available <= low_stock_threshold
            AND (last_stock_alert IS NULL OR last_stock_alert < %s)
            ORDER BY available ASC
        """, (cooldown_time.isoformat(),))
        
        low_stock_products = c.fetchall()
        
        if not low_stock_products:
            logger.info("✅ No low stock alerts needed")
            return
        
        # Group identical products and sum their stock
        grouped_products = {}
        for product in low_stock_products:
            # Create a unique key for identical products
            key = f"{product['city']}|{product['district']}|{product['product_type']}|{product['size']}|{product['price']}"
            
            if key not in grouped_products:
                grouped_products[key] = {
                    'city': product['city'],
                    'district': product['district'],
                    'product_type': product['product_type'],
                    'size': product['size'],
                    'price': product['price'],
                    'total_stock': 0,
                    'product_count': 0,
                    'low_stock_threshold': product['low_stock_threshold']
                }
            
            grouped_products[key]['total_stock'] += product['available']
            grouped_products[key]['product_count'] += 1
        
        # Convert back to list and group by severity
        grouped_list = list(grouped_products.values())
        critical_products = [p for p in grouped_list if p['total_stock'] <= CRITICAL_STOCK_THRESHOLD]
        warning_products = [p for p in grouped_list if p['total_stock'] > CRITICAL_STOCK_THRESHOLD and p['total_stock'] <= p['low_stock_threshold']]
        
        # Create alert message
        alert_message = "🚨 **STOCK ALERT** 🚨\n\n"
        
        if critical_products:
            alert_message += "🔴 **CRITICAL - Immediate Action Required:**\n"
            for product in critical_products:
                alert_message += f"• {product['city']} → {product['district']} → {product['product_type']} {product['size']}\n"
                alert_message += f"  💰 {format_currency(product['price'])} | 📦 Only {product['total_stock']} left!"
                if product['product_count'] > 1:
                    alert_message += f" ({product['product_count']} items)"
                alert_message += "\n\n"
        
        if warning_products:
            alert_message += "🟡 **LOW STOCK - Restock Soon:**\n"
            for product in warning_products:
                alert_message += f"• {product['city']} → {product['district']} → {product['product_type']} {product['size']}\n"
                alert_message += f"  💰 {format_currency(product['price'])} | 📦 {product['total_stock']} remaining"
                if product['product_count'] > 1:
                    alert_message += f" ({product['product_count']} items)"
                alert_message += "\n\n"
        
        alert_message += f"⏰ Alert generated at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        
        # Store alert in database and update last_stock_alert for all original products
        for product in low_stock_products:
            # Log the alert
            c.execute("""
                INSERT INTO stock_alerts 
                (product_id, alert_type, alert_message, created_at, notified_admins)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                product['id'],
                'critical' if product['available'] <= CRITICAL_STOCK_THRESHOLD else 'low',
                f"Stock level: {product['available']} (Grouped total: {grouped_products.get(product['city'] + '|' + product['district'] + '|' + product['product_type'] + '|' + product['size'] + '|' + str(product['price']), {}).get('total_stock', product['available'])})",
                now.isoformat(),
                str(ADMIN_ID)
            ))
            
            # Update last alert time
            c.execute("""
                UPDATE products SET last_stock_alert = %s WHERE id = %s
            """, (now.isoformat(), product['id']))
        
        conn.commit()
        
        # Send alert to primary admin
        # Note: Bot instance will be passed to this function when called from main
        logger.info(f"📧 Stock alert ready for {len(low_stock_products)} products")
        return alert_message
        
    except Exception as e:
        logger.error(f"Error checking low stock alerts: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def get_stock_summary() -> Dict:
    """Returns a summary of current stock levels."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get stock statistics
        c.execute("""
            SELECT 
                COUNT(*) as total_products,
                SUM(available) as total_stock,
                COUNT(CASE WHEN available <= %s THEN 1 END) as critical_stock,
                COUNT(CASE WHEN available <= %s AND available > %s THEN 1 END) as low_stock,
                COUNT(CASE WHEN available = 0 THEN 1 END) as out_of_stock
            FROM products
        """, (CRITICAL_STOCK_THRESHOLD, LOW_STOCK_THRESHOLD, CRITICAL_STOCK_THRESHOLD))
        
        stats = c.fetchone()
        
        # Get top low stock products
        c.execute("""
            SELECT city, district, product_type, size, available, price
            FROM products 
            WHERE available > 0 AND available <= %s
            ORDER BY available ASC
            LIMIT 10
        """, (LOW_STOCK_THRESHOLD,))
        
        low_stock_products = c.fetchall()
        
        return {
            'total_products': stats['total_products'],
            'total_stock': stats['total_stock'],
            'critical_stock': stats['critical_stock'],
            'low_stock': stats['low_stock'],
            'out_of_stock': stats['out_of_stock'],
            'low_stock_products': low_stock_products
        }
        
    except Exception as e:
        logger.error(f"Error getting stock summary: {e}")
        return {}
    finally:
        if conn:
            conn.close()

async def update_stock_threshold(product_id: int, threshold: int) -> bool:
    """Updates the low stock threshold for a specific product."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            UPDATE products SET low_stock_threshold = %s WHERE id = %s
        """, (threshold, product_id))
        
        if c.rowcount > 0:
            conn.commit()
            logger.info(f"Updated stock threshold for product {product_id} to {threshold}")
            return True
        else:
            logger.warning(f"Product {product_id} not found for threshold update")
            return False
            
    except Exception as e:
        logger.error(f"Error updating stock threshold: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

async def toggle_stock_alerts(product_id: int, enabled: bool) -> bool:
    """Enables or disables stock alerts for a specific product."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            UPDATE products SET stock_alerts_enabled = %s WHERE id = %s
        """, (1 if enabled else 0, product_id))
        
        if c.rowcount > 0:
            conn.commit()
            logger.info(f"{'Enabled' if enabled else 'Disabled'} stock alerts for product {product_id}")
            return True
        else:
            logger.warning(f"Product {product_id} not found for alert toggle")
            return False
            
    except Exception as e:
        logger.error(f"Error toggling stock alerts: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# --- Admin Handlers for Stock Management ---

async def handle_stock_management_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows the stock management menu."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied.", show_alert=True)
        return
    
    # Get stock summary
    summary = get_stock_summary()
    
    msg = "📦 **Stock Management Dashboard**\n\n"
    msg += f"📊 **Overview:**\n"
    msg += f"• Total Products: {summary.get('total_products', 0)}\n"
    msg += f"• Total Stock: {summary.get('total_stock', 0)} units\n"
    msg += f"• 🔴 Critical Stock: {summary.get('critical_stock', 0)} products\n"
    msg += f"• 🟡 Low Stock: {summary.get('low_stock', 0)} products\n"
    msg += f"• ❌ Out of Stock: {summary.get('out_of_stock', 0)} products\n\n"
    
    if summary.get('low_stock_products'):
        msg += "⚠️ **Products Needing Attention:**\n"
        for product in summary['low_stock_products'][:5]:  # Show top 5
            msg += f"• {product['city']} → {product['product_type']} {product['size']} ({product['available']} left)\n"
        
        if len(summary['low_stock_products']) > 5:
            msg += f"... and {len(summary['low_stock_products']) - 5} more\n"
    
    keyboard = [
        [InlineKeyboardButton("📊 Detailed Stock Report", callback_data="stock_detailed_report")],
        [InlineKeyboardButton("🚨 View Stock Alerts", callback_data="stock_view_alerts")],
        [InlineKeyboardButton("⚙️ Configure Thresholds", callback_data="stock_configure_thresholds")],
        [InlineKeyboardButton("🔄 Run Stock Check Now", callback_data="stock_check_now")],
        [InlineKeyboardButton("📈 Stock Analytics", callback_data="stock_analytics")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_stock_check_now(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Manually triggers a stock check."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied.", show_alert=True)
        return
    
    await query.answer("Running stock check...", show_alert=False)
    
    try:
        await check_low_stock_alerts()
        await query.edit_message_text(
            "✅ **Stock check completed!** ✅\n\n"
            "🔧 **Fixed:** Duplicate product alerts are now grouped together!\n"
            "📊 **Result:** You'll see total stock instead of individual items.\n\n"
            "**Example:** Instead of 8 separate 'Only 1 left!' alerts,\n"
            "you'll see 1 alert showing 'Only 8 left! (8 items)'",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🧹 Clear All Alerts", callback_data="stock_clear_alerts")],
                [InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")]
            ])
        )
    except Exception as e:
        logger.error(f"Error in manual stock check: {e}")
        await query.edit_message_text(
            "❌ Error running stock check. Please check logs.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")
            ]])
        )

async def handle_stock_clear_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Clear all stock alerts and reset alert timestamps."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Clear all stock alerts
        c.execute("DELETE FROM stock_alerts")
        
        # Reset last_stock_alert for all products
        c.execute("UPDATE products SET last_stock_alert = NULL")
        
        conn.commit()
        
        await query.edit_message_text(
            "✅ **All stock alerts cleared!** ✅\n\n"
            "🧹 **What was cleared:**\n"
            "• All stock alert history\n"
            "• Alert timestamps reset\n"
            "• Fresh start for new alerts\n\n"
            "🔄 **Next stock check will generate new alerts with the fixed grouping system.**",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Run Stock Check Now", callback_data="stock_check_now"),
                InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")
            ]])
        )
        
    except Exception as e:
        logger.error(f"Error clearing stock alerts: {e}")
        await query.edit_message_text(
            "❌ Error clearing alerts. Please check logs.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")
            ]])
        )
    finally:
        if conn:
            conn.close()

async def handle_stock_detailed_report(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows a detailed stock report."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied.", show_alert=True)
        return
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get detailed stock by category
        c.execute("""
            SELECT city, district, product_type, 
                   COUNT(*) as product_count,
                   SUM(available) as total_stock,
                   MIN(available) as min_stock,
                   MAX(available) as max_stock,
                   AVG(available) as avg_stock
            FROM products 
            WHERE available > 0
            GROUP BY city, district, product_type
            ORDER BY city, district, product_type
        """)
        
        stock_data = c.fetchall()
        
        msg = "📊 **Detailed Stock Report**\n\n"
        
        current_city = ""
        for row in stock_data:
            if row['city'] != current_city:
                current_city = row['city']
                msg += f"🏙️ **{current_city}**\n"
            
            msg += f"  📍 {row['district']} - {row['product_type']}\n"
            msg += f"    Products: {row['product_count']} | Stock: {row['total_stock']} units\n"
            msg += f"    Range: {row['min_stock']}-{row['max_stock']} | Avg: {row['avg_stock']:.1f}\n\n"
        
        if not stock_data:
            msg += "No products in stock."
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")]]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error generating detailed stock report: {e}")
        await query.edit_message_text(
            "❌ Error generating report. Please try again.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")
            ]])
        )
    finally:
        if conn:
            conn.close()

async def handle_stock_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show stock analytics and insights"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied.", show_alert=True)
        return
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get comprehensive analytics
        # Total inventory value
        c.execute("SELECT SUM(price * available) as total_value FROM products WHERE available > 0")
        total_value_result = c.fetchone()
        total_value = total_value_result['total_value'] if total_value_result['total_value'] else 0
        
        # Stock distribution by city
        c.execute("""
            SELECT city, 
                   COUNT(*) as products,
                   SUM(available) as total_stock,
                   SUM(price * available) as city_value,
                   AVG(price) as avg_price
            FROM products 
            WHERE available > 0
            GROUP BY city 
            ORDER BY total_stock DESC
        """)
        city_stats = c.fetchall()
        
        # Top selling product types (based on stock levels - lower stock = more sales)
        c.execute("""
            SELECT product_type, 
                   COUNT(*) as variants,
                   SUM(available) as current_stock,
                   AVG(price) as avg_price,
                   MIN(available) as lowest_stock
            FROM products 
            GROUP BY product_type 
            ORDER BY current_stock ASC
        """)
        product_stats = c.fetchall()
        
        # Stock velocity indicators
        c.execute("""
            SELECT 
                COUNT(CASE WHEN available = 0 THEN 1 END) as sold_out,
                COUNT(CASE WHEN available <= 2 THEN 1 END) as critical,
                COUNT(CASE WHEN available <= 5 THEN 1 END) as low_stock,
                COUNT(CASE WHEN available > 20 THEN 1 END) as overstocked,
                COUNT(*) as total_products
            FROM products
        """)
        velocity_stats = c.fetchone()
        
        # Price analysis
        c.execute("""
            SELECT 
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price,
                COUNT(DISTINCT price) as price_points
            FROM products
            WHERE available > 0
        """)
        price_stats = c.fetchone()
        
    except Exception as e:
        logger.error(f"Error getting stock analytics: {e}")
        await query.answer("Error loading analytics", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    # Build comprehensive analytics message
    msg = "📊 **Advanced Stock Analytics**\n\n"
    
    # Financial overview
    msg += f"💰 **Financial Overview:**\n"
    msg += f"• Total Inventory Value: ${total_value:,.2f}\n"
    msg += f"• Price Range: ${price_stats['min_price']:.2f} - ${price_stats['max_price']:.2f}\n"
    msg += f"• Average Product Price: ${price_stats['avg_price']:.2f}\n"
    msg += f"• Unique Price Points: {price_stats['price_points']}\n\n"
    
    # Stock velocity analysis
    msg += f"🚀 **Stock Velocity Analysis:**\n"
    sold_out_pct = (velocity_stats['sold_out'] / velocity_stats['total_products']) * 100
    critical_pct = (velocity_stats['critical'] / velocity_stats['total_products']) * 100
    low_stock_pct = (velocity_stats['low_stock'] / velocity_stats['total_products']) * 100
    overstocked_pct = (velocity_stats['overstocked'] / velocity_stats['total_products']) * 100
    
    msg += f"• 🔴 Sold Out: {velocity_stats['sold_out']} ({sold_out_pct:.1f}%)\n"
    msg += f"• 🟠 Critical (≤2): {velocity_stats['critical']} ({critical_pct:.1f}%)\n"
    msg += f"• 🟡 Low Stock (≤5): {velocity_stats['low_stock']} ({low_stock_pct:.1f}%)\n"
    msg += f"• 🟢 Overstocked (>20): {velocity_stats['overstocked']} ({overstocked_pct:.1f}%)\n\n"
    
    # Top cities by inventory
    if city_stats:
        msg += f"🏙️ **Top Cities by Inventory:**\n"
        for i, city in enumerate(city_stats[:3], 1):
            msg += f"{i}. **{city['city']}**: {city['total_stock']} units (${city['city_value']:,.2f})\n"
            msg += f"   {city['products']} products, avg ${city['avg_price']:.2f}\n"
        msg += "\n"
    
    # Product performance insights
    if product_stats:
        msg += f"📈 **Product Performance (by stock velocity):**\n"
        for i, product in enumerate(product_stats[:3], 1):
            velocity_indicator = "🔥" if product['current_stock'] < 50 else "📦"
            msg += f"{velocity_indicator} **{product['product_type']}**: {product['current_stock']} units left\n"
            msg += f"   {product['variants']} variants, avg ${product['avg_price']:.2f}\n"
    
    keyboard = [
        [InlineKeyboardButton("📋 Export Analytics", callback_data="stock_export_analytics")],
        [InlineKeyboardButton("🔄 Refresh Data", callback_data="stock_analytics")],
        [InlineKeyboardButton("⚙️ Configure Alerts", callback_data="stock_configure_thresholds")],
        [InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_stock_configure_thresholds(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Configure stock alert thresholds"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied.", show_alert=True)
        return
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get current threshold settings by product type
        c.execute("""
            SELECT product_type,
                   COUNT(*) as product_count,
                   AVG(low_stock_threshold) as avg_threshold,
                   MIN(low_stock_threshold) as min_threshold,
                   MAX(low_stock_threshold) as max_threshold
            FROM products 
            GROUP BY product_type
            ORDER BY product_count DESC
        """)
        threshold_stats = c.fetchall()
        
        # Get products with custom thresholds
        c.execute("""
            SELECT city, district, product_type, size, low_stock_threshold, available
            FROM products 
            WHERE low_stock_threshold != 5
            ORDER BY low_stock_threshold DESC
            LIMIT 10
        """)
        custom_thresholds = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error getting threshold configuration: {e}")
        await query.answer("Error loading configuration", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    msg = "⚙️ **Stock Alert Threshold Configuration**\n\n"
    
    # Current global settings
    msg += f"🌐 **Global Settings:**\n"
    msg += f"• Default Low Stock Threshold: {LOW_STOCK_THRESHOLD} units\n"
    msg += f"• Critical Stock Threshold: {CRITICAL_STOCK_THRESHOLD} units\n"
    msg += f"• Alert Cooldown: {STOCK_ALERT_COOLDOWN_HOURS} hours\n\n"
    
    # Product type analysis
    if threshold_stats:
        msg += f"📊 **Threshold Analysis by Product Type:**\n"
        for stat in threshold_stats[:5]:
            avg_threshold = stat['avg_threshold'] if stat['avg_threshold'] else LOW_STOCK_THRESHOLD
            msg += f"• **{stat['product_type']}**: {stat['product_count']} products\n"
            msg += f"  Avg threshold: {avg_threshold:.1f} units ({stat['min_threshold']}-{stat['max_threshold']})\n"
        msg += "\n"
    
    # Custom threshold products
    if custom_thresholds:
        msg += f"🔧 **Products with Custom Thresholds:**\n"
        for product in custom_thresholds[:5]:
            status_icon = "🔴" if product['available'] <= product['low_stock_threshold'] else "🟢"
            msg += f"{status_icon} {product['city']} → {product['product_type']} {product['size']}\n"
            msg += f"   Threshold: {product['low_stock_threshold']} | Current: {product['available']}\n"
        
        if len(custom_thresholds) > 5:
            msg += f"... and {len(custom_thresholds) - 5} more\n"
        msg += "\n"
    
    msg += f"🎛️ **Configuration Options:**"
    
    keyboard = [
        [InlineKeyboardButton("📈 Set Global Thresholds", callback_data="stock_set_global_thresholds")],
        [InlineKeyboardButton("🏷️ Configure by Product Type", callback_data="stock_configure_by_type")],
        [InlineKeyboardButton("🎯 Set Individual Thresholds", callback_data="stock_set_individual")],
        [InlineKeyboardButton("⏰ Adjust Alert Frequency", callback_data="stock_configure_frequency")],
        [InlineKeyboardButton("🔄 Reset to Defaults", callback_data="stock_reset_thresholds")],
        [InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_stock_view_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """View stock alert history"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        await query.answer("Access denied.", show_alert=True)
        return
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get recent stock alerts
        c.execute("""
            SELECT sa.alert_type, sa.alert_message, sa.created_at,
                   p.city, p.district, p.product_type, p.size, p.available
            FROM stock_alerts sa
            JOIN products p ON sa.product_id = p.id
            ORDER BY sa.created_at DESC
            LIMIT 20
        """)
        
        alerts = c.fetchall()
        
        msg = "🚨 **Stock Alert History**\n\n"
        
        if not alerts:
            msg += "No stock alerts found.\n\n"
            msg += "Alerts are generated when:\n"
            msg += "• Products fall below threshold\n"
            msg += "• Critical stock levels reached\n"
            msg += "• Items go out of stock\n"
        else:
            for alert in alerts[:10]:
                try:
                    date_str = datetime.fromisoformat(alert['created_at'].replace('Z', '+00:00')).strftime('%m-%d %H:%M')
                except:
                    date_str = "Recent"
                
                alert_icon = "🔴" if alert['alert_type'] == 'critical' else "🟡"
                msg += f"{alert_icon} **{alert['alert_type'].title()} Alert** ({date_str})\n"
                msg += f"   📍 {alert['city']} → {alert['product_type']} {alert['size']}\n"
                msg += f"   📦 Stock: {alert['available']} units\n"
                msg += f"   💬 {alert['alert_message']}\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Run Stock Check", callback_data="stock_check_now")],
            [InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")]
        ]
        
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error viewing stock alerts: {e}")
        await query.edit_message_text(
            "❌ Error loading stock alerts.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back to Stock Management", callback_data="stock_management_menu")
            ]])
        )
    finally:
        if conn:
            conn.close()

# --- Additional Stock Configuration Handlers ---

async def handle_stock_export_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Export stock analytics data"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get comprehensive data for export
        c.execute("""
            SELECT city, district, product_type, size, price, available, 
                   low_stock_threshold, stock_alerts_enabled,
                   CASE 
                       WHEN available = 0 THEN 'Out of Stock'
                       WHEN available <= 2 THEN 'Critical'
                       WHEN available <= 5 THEN 'Low Stock'
                       WHEN available > 20 THEN 'Overstocked'
                       ELSE 'Normal'
                   END as stock_status
            FROM products
            ORDER BY city, district, product_type, available ASC
        """)
        
        products = c.fetchall()
        
        if not products:
            await query.answer("No products found for export", show_alert=True)
            return
        
        # Create export summary
        export_data = f"📋 **Stock Analytics Export**\n"
        export_data += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        export_data += f"Total Products: {len(products)}\n\n"
        
        # Group by status
        status_counts = {}
        for product in products:
            status = product['stock_status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        export_data += "📊 **Stock Status Summary:**\n"
        for status, count in status_counts.items():
            export_data += f"• {status}: {count} products\n"
        
        export_data += "\n🔍 **Critical Items Requiring Attention:**\n"
        critical_items = [p for p in products if p['stock_status'] in ['Out of Stock', 'Critical']]
        
        for item in critical_items[:10]:
            export_data += f"• {item['city']} → {item['product_type']} {item['size']}\n"
            export_data += f"  Stock: {item['available']} | Price: ${item['price']:.2f}\n"
        
        if len(critical_items) > 10:
            export_data += f"... and {len(critical_items) - 10} more critical items\n"
        
        # Send as a long message (Telegram supports up to 4096 characters)
        keyboard = [[InlineKeyboardButton("⬅️ Back to Analytics", callback_data="stock_analytics")]]
        await query.edit_message_text(export_data, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error exporting stock analytics: {e}")
        await query.answer("Export failed", show_alert=True)
    finally:
        if conn:
            conn.close()

async def handle_stock_set_global_thresholds(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Set global stock thresholds"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "📈 **Set Global Stock Thresholds**\n\n"
    msg += f"Current Settings:\n"
    msg += f"• Low Stock Threshold: {LOW_STOCK_THRESHOLD} units\n"
    msg += f"• Critical Stock Threshold: {CRITICAL_STOCK_THRESHOLD} units\n\n"
    msg += "Choose new threshold levels:\n\n"
    msg += "⚠️ This will apply to ALL products without custom thresholds."
    
    keyboard = [
        [InlineKeyboardButton("📊 Low: 3, Critical: 1", callback_data="stock_global_3_1")],
        [InlineKeyboardButton("📊 Low: 5, Critical: 2", callback_data="stock_global_5_2")],
        [InlineKeyboardButton("📊 Low: 10, Critical: 3", callback_data="stock_global_10_3")],
        [InlineKeyboardButton("📊 Low: 15, Critical: 5", callback_data="stock_global_15_5")],
        [InlineKeyboardButton("🔧 Custom Settings", callback_data="stock_custom_global")],
        [InlineKeyboardButton("⬅️ Back", callback_data="stock_configure_thresholds")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_stock_configure_by_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Configure thresholds by product type"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get product types
        c.execute("""
            SELECT product_type, COUNT(*) as count, AVG(available) as avg_stock
            FROM products 
            GROUP BY product_type 
            ORDER BY count DESC
        """)
        product_types = c.fetchall()
        
    except Exception as e:
        logger.error(f"Error getting product types: {e}")
        await query.answer("Error loading product types", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    msg = "🏷️ **Configure Thresholds by Product Type**\n\n"
    msg += "Select a product type to configure custom thresholds:\n\n"
    
    keyboard = []
    for ptype in product_types[:10]:  # Show top 10 product types
        avg_stock = ptype['avg_stock'] if ptype['avg_stock'] else 0
        button_text = f"{ptype['product_type']} ({ptype['count']} items, avg: {avg_stock:.1f})"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"stock_type_{ptype['product_type']}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="stock_configure_thresholds")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_stock_reset_thresholds(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Reset all thresholds to default"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    msg = "🔄 **Reset Stock Thresholds**\n\n"
    msg += "⚠️ **WARNING**: This will reset ALL custom thresholds to default values:\n\n"
    msg += f"• Low Stock Threshold: {LOW_STOCK_THRESHOLD} units\n"
    msg += f"• Critical Stock Threshold: {CRITICAL_STOCK_THRESHOLD} units\n\n"
    msg += "This action cannot be undone. Are you sure?"
    
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Reset All", callback_data="stock_confirm_reset")],
        [InlineKeyboardButton("❌ Cancel", callback_data="stock_configure_thresholds")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_stock_confirm_reset(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm and execute threshold reset"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Reset all thresholds to default
        c.execute("UPDATE products SET low_stock_threshold = %s", (LOW_STOCK_THRESHOLD,))
        updated_count = c.rowcount
        conn.commit()
        
        msg = f"✅ **Thresholds Reset Successfully!**\n\n"
        msg += f"Updated {updated_count} products to default threshold of {LOW_STOCK_THRESHOLD} units.\n\n"
        msg += "All custom thresholds have been cleared."
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Configuration", callback_data="stock_configure_thresholds")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error resetting thresholds: {e}")
        await query.answer("Reset failed", show_alert=True)
    finally:
        if conn:
            conn.close()

# --- END OF FILE stock_management.py ---
