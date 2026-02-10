"""
Case Rewards System - CS:GO Style Product Cases
Admin creates cases with product types (not individual products)
Users win products and select delivery city
"""

import logging
import json
from typing import Dict, List, Optional
from utils import get_db_connection, is_primary_admin

logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE SCHEMA
# ============================================================================

def init_case_rewards_tables():
    """Initialize case rewards system tables"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Case reward pools (what products can be won from each case)
        c.execute('''
            CREATE TABLE IF NOT EXISTS case_reward_pools (
                id SERIAL PRIMARY KEY,
                case_type TEXT NOT NULL,
                product_type_name TEXT NOT NULL,
                product_size TEXT NOT NULL,
                win_chance_percent REAL NOT NULL,
                reward_emoji TEXT DEFAULT '🎁',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(case_type, product_type_name, product_size)
            )
        ''')
        
        # Lose emoji for each case (what shows when user wins nothing)
        c.execute('''
            CREATE TABLE IF NOT EXISTS case_lose_emojis (
                id SERIAL PRIMARY KEY,
                case_type TEXT UNIQUE NOT NULL,
                lose_emoji TEXT DEFAULT '💸',
                lose_message TEXT DEFAULT 'Better luck next time!',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # User product wins (pending city selection)
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_product_wins (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                case_type TEXT NOT NULL,
                product_type_name TEXT NOT NULL,
                product_size TEXT NOT NULL,
                win_emoji TEXT,
                estimated_value REAL,
                status TEXT DEFAULT 'pending_city',
                selected_city_id INTEGER,
                selected_district_id INTEGER,
                selected_product_id INTEGER,
                converted_to_balance BOOLEAN DEFAULT FALSE,
                won_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                delivered_at TIMESTAMP WITH TIME ZONE
            )
        ''')
        
        # Insert default lose emojis for existing cases
        default_lose_emojis = {
            'basic': ('💸', 'Better luck next time!'),
            'premium': ('😢', 'So close! Try again!'),
            'legendary': ('💔', 'Not this time, champion!')
        }
        
        for case_type, (emoji, message) in default_lose_emojis.items():
            c.execute('''
                INSERT INTO case_lose_emojis (case_type, lose_emoji, lose_message)
                VALUES (%s, %s, %s)
                ON CONFLICT (case_type) DO NOTHING
            ''', (case_type, emoji, message))
        
        conn.commit()
        logger.info("✅ Case rewards tables initialized successfully")
        
    except Exception as e:
        logger.error(f"❌ Error initializing case rewards tables: {e}")
        conn.rollback()
    finally:
        conn.close()

# ============================================================================
# ADMIN FUNCTIONS
# ============================================================================

def get_all_product_types() -> List[Dict]:
    """Get all unique product types from products table"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            SELECT DISTINCT 
                product_type as name,
                size,
                MIN(price) as min_price,
                MAX(price) as max_price,
                SUM(available) as total_available
            FROM products
            WHERE available > 0
            GROUP BY product_type, size
            ORDER BY product_type, size
        ''')
        
        return c.fetchall()
    finally:
        conn.close()

def get_case_reward_pool(case_type: str) -> List[Dict]:
    """Get all rewards configured for a case"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            SELECT 
                id,
                product_type_name,
                product_size,
                win_chance_percent,
                reward_emoji,
                is_active
            FROM case_reward_pools
            WHERE case_type = %s AND is_active = TRUE
            ORDER BY win_chance_percent DESC
        ''', (case_type,))
        
        return c.fetchall()
    finally:
        conn.close()

def add_product_to_case_pool(case_type: str, product_type: str, size: str, 
                             win_chance: float, emoji: str = '🎁') -> bool:
    """Add a product type to case reward pool"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            INSERT INTO case_reward_pools 
            (case_type, product_type_name, product_size, win_chance_percent, reward_emoji)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (case_type, product_type_name, product_size)
            DO UPDATE SET 
                win_chance_percent = EXCLUDED.win_chance_percent,
                reward_emoji = EXCLUDED.reward_emoji,
                is_active = TRUE
        ''', (case_type, product_type, size, win_chance, emoji))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding product to case pool: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def remove_product_from_case_pool(pool_id: int) -> bool:
    """Remove a product from case reward pool"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            UPDATE case_reward_pools 
            SET is_active = FALSE
            WHERE id = %s
        ''', (pool_id,))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error removing product from case pool: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def set_case_lose_emoji(case_type: str, emoji: str, message: str) -> bool:
    """Set the lose emoji and message for a case"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            INSERT INTO case_lose_emojis (case_type, lose_emoji, lose_message)
            VALUES (%s, %s, %s)
            ON CONFLICT (case_type)
            DO UPDATE SET 
                lose_emoji = EXCLUDED.lose_emoji,
                lose_message = EXCLUDED.lose_message
        ''', (case_type, emoji, message))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error setting lose emoji: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

# ============================================================================
# CASE OPENING LOGIC
# ============================================================================

def open_product_case(user_id: int, case_type: str, points_spent: int) -> Dict:
    """
    Open a case and determine if user wins a product
    Returns: {
        'success': bool,
        'outcome': 'win' or 'lose',
        'product_type': str (if win),
        'product_size': str (if win),
        'emoji': str,
        'message': str,
        'win_id': int (if win, for city selection)
    }
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get reward pool for this case
        rewards = get_case_reward_pool(case_type)
        
        if not rewards:
            return {
                'success': False,
                'message': 'No rewards configured for this case'
            }
        
        # Calculate total win chance
        total_win_chance = sum(r['win_chance_percent'] for r in rewards)
        
        # Get lose emoji
        c.execute('''
            SELECT lose_emoji, lose_message
            FROM case_lose_emojis
            WHERE case_type = %s
        ''', (case_type,))
        lose_data = c.fetchone()
        lose_emoji = lose_data['lose_emoji'] if lose_data else '💸'
        lose_message = lose_data['lose_message'] if lose_data else 'Better luck next time!'
        
        # Roll the dice
        import random
        roll = random.uniform(0, 100)
        
        if roll > total_win_chance:
            # User loses
            # Deduct points
            c.execute('''
                UPDATE user_points
                SET points = points - %s
                WHERE user_id = %s
            ''', (points_spent, user_id))
            
            # Log the loss
            c.execute('''
                INSERT INTO case_openings 
                (user_id, case_type, points_spent, outcome_type, outcome_value)
                VALUES (%s, %s, %s, 'lose', %s)
            ''', (user_id, case_type, points_spent, lose_message))
            
            conn.commit()
            
            return {
                'success': True,
                'outcome': 'lose',
                'emoji': lose_emoji,
                'message': lose_message
            }
        
        # User wins! Determine which product
        cumulative = 0
        won_reward = None
        
        for reward in rewards:
            cumulative += reward['win_chance_percent']
            if roll <= cumulative:
                won_reward = reward
                break
        
        if not won_reward:
            won_reward = rewards[0]  # Fallback
        
        # Get estimated value
        c.execute('''
            SELECT AVG(price) as avg_price
            FROM products
            WHERE product_type = %s AND size = %s AND available > 0
        ''', (won_reward['product_type_name'], won_reward['product_size']))
        
        price_result = c.fetchone()
        estimated_value = float(price_result['avg_price']) if price_result and price_result['avg_price'] else 0.0
        
        # Deduct points
        c.execute('''
            UPDATE user_points
            SET points = points - %s,
                total_products_won = total_products_won + 1
            WHERE user_id = %s
        ''', (points_spent, user_id))
        
        # Create pending win (user needs to select city)
        c.execute('''
            INSERT INTO user_product_wins 
            (user_id, case_type, product_type_name, product_size, win_emoji, estimated_value)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (user_id, case_type, won_reward['product_type_name'], 
              won_reward['product_size'], won_reward['reward_emoji'], estimated_value))
        
        win_id = c.fetchone()['id']
        
        # Log the win
        c.execute('''
            INSERT INTO case_openings 
            (user_id, case_type, points_spent, outcome_type, outcome_value, product_id)
            VALUES (%s, %s, %s, 'win_product', %s, %s)
        ''', (user_id, case_type, points_spent, 
              f"{won_reward['product_type_name']} {won_reward['product_size']}", win_id))
        
        conn.commit()
        
        return {
            'success': True,
            'outcome': 'win',
            'product_type': won_reward['product_type_name'],
            'product_size': won_reward['product_size'],
            'emoji': won_reward['reward_emoji'],
            'message': f"You won {won_reward['product_type_name']} {won_reward['product_size']}!",
            'estimated_value': estimated_value,
            'win_id': win_id
        }
        
    except Exception as e:
        logger.error(f"Error opening product case: {e}")
        conn.rollback()
        return {
            'success': False,
            'message': f'Error: {e}'
        }
    finally:
        conn.close()

def get_available_cities_for_product(product_type: str, size: str) -> List[Dict]:
    """Get cities that have this product available"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # products table uses TEXT columns (city, district), not foreign keys
        c.execute('''
            SELECT DISTINCT 
                c.id as city_id,
                c.name as city_name,
                COUNT(p.id) as product_count
            FROM cities c
            JOIN products p ON p.city = c.name
            WHERE p.product_type = %s 
                AND p.size = %s 
                AND p.available > 0
            GROUP BY c.id, c.name
            ORDER BY c.name
        ''', (product_type, size))
        
        return c.fetchall()
    finally:
        conn.close()

def select_delivery_city(win_id: int, city_id: int, district_id: int, product_id: int) -> bool:
    """User selects city/district/product for delivery"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            UPDATE user_product_wins
            SET 
                selected_city_id = %s,
                selected_district_id = %s,
                selected_product_id = %s,
                status = 'awaiting_delivery',
                delivered_at = CURRENT_TIMESTAMP
            WHERE id = %s
        ''', (city_id, district_id, product_id, win_id))
        
        # Decrease product availability
        c.execute('''
            UPDATE products
            SET available = available - 1
            WHERE id = %s AND available > 0
        ''', (product_id,))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error selecting delivery city: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def convert_win_to_balance(win_id: int, user_id: int) -> bool:
    """Convert product win to balance (if city unavailable)"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get win details
        c.execute('''
            SELECT estimated_value
            FROM user_product_wins
            WHERE id = %s AND user_id = %s
        ''', (win_id, user_id))
        
        win = c.fetchone()
        if not win:
            return False
        
        value = win['estimated_value']
        
        # Add to balance
        c.execute('''
            UPDATE users
            SET balance = balance + %s
            WHERE user_id = %s
        ''', (value, user_id))
        
        # Mark as converted
        c.execute('''
            UPDATE user_product_wins
            SET 
                converted_to_balance = TRUE,
                status = 'converted_to_balance'
            WHERE id = %s
        ''', (win_id,))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error converting win to balance: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

