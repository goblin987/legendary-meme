"""
Daily Rewards & Case Opening System
Premium gamification with next-level animations
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
import random
import json
from utils import get_db_connection, is_primary_admin

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Daily Streak Rewards - NOW LOADED FROM DATABASE (customizable by admin)
DAILY_REWARDS = {}  # Will be populated from daily_reward_schedule table

def get_reward_schedule() -> Dict[int, Dict]:
    """Get the current reward schedule from database"""
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''
            SELECT day_number, points, description
            FROM daily_reward_schedule
            ORDER BY day_number
        ''')
        schedule = {}
        for row in c.fetchall():
            schedule[row['day_number']] = {
                'points': row['points'],
                'description': row['description'] or f'Day {row["day_number"]} reward'
            }
        return schedule
    finally:
        conn.close()

def update_reward_for_day(day_number: int, points: int, description: str = None) -> bool:
    """Update reward amount for a specific day"""
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO daily_reward_schedule (day_number, points, description, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (day_number)
            DO UPDATE SET 
                points = EXCLUDED.points,
                description = EXCLUDED.description,
                updated_at = CURRENT_TIMESTAMP
        ''', (day_number, points, description))
        conn.commit()
        logger.info(f"✅ Updated Day {day_number} reward to {points} points")
        return True
    except Exception as e:
        logger.error(f"Error updating reward schedule: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_reward_for_day(day_number: int) -> int:
    """Get reward points for a specific day (infinite days, no bonus)"""
    schedule = get_reward_schedule()
    
    # If day is in schedule, return it
    if day_number in schedule:
        return schedule[day_number]['points']
    
    # For days beyond the schedule, repeat the pattern (NO BONUS)
    max_day = max(schedule.keys()) if schedule else 7
    day_in_cycle = ((day_number - 1) % max_day) + 1
    
    base_reward = schedule.get(day_in_cycle, {}).get('points', 1)
    
    return base_reward

def get_rolling_calendar(user_id: int, current_streak: int) -> List[Dict]:
    """
    Get rolling 7-day calendar view
    Shows last 6 claimed days + next unclaimed day
    After Day 7, calendar rolls forward (Day 7 becomes position 1, Day 8 at position 7)
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get user's claim history
        c.execute('''
            SELECT login_date, streak_count, points_awarded, claimed
            FROM daily_logins
            WHERE user_id = %s
            ORDER BY login_date DESC
            LIMIT 7
        ''', (user_id,))
        
        history = c.fetchall()
        
        # Build rolling calendar
        calendar = []
        
        # Determine the range to show
        if current_streak <= 7:
            # First week: show Day 1-7
            start_day = 1
            end_day = 7
        else:
            # After first week: show last 6 days + next day
            start_day = current_streak - 6
            end_day = current_streak
        
        # Build calendar entries
        for day_num in range(start_day, end_day + 1):
            points = get_reward_for_day(day_num)
            
            # Check if this day was claimed
            claimed = False
            for record in history:
                if record['streak_count'] == day_num and record['claimed']:
                    claimed = True
                    points = record['points_awarded']  # Use actual awarded points
                    break
            
            calendar.append({
                'day_number': day_num,
                'points': points,
                'claimed': claimed,
                'is_next': day_num == current_streak and not claimed
            })
        
        return calendar
        
    finally:
        conn.close()

# Case Types - loaded from database (admin creates them)
CASE_TYPES = {}

def get_all_cases() -> Dict:
    """Get all cases from database"""
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute('''
            SELECT case_type, enabled, cost, rewards_config
            FROM case_settings
            WHERE enabled = TRUE
            ORDER BY cost
        ''')
        cases = {}
        for row in c.fetchall():
            # rewards_config is already a dict (JSONB in PostgreSQL), no need to json.loads()
            rewards = row['rewards_config'] if row['rewards_config'] else {}
            cases[row['case_type']] = {
                'name': row['case_type'].title(),
                'cost': row['cost'],
                'emoji': '🎁',  # Default, can be customized
                'enabled': row['enabled'],
                'rewards': rewards,
                'color': '#FFD700',  # Default gold color
                'animation_speed': 'fast',  # Default animation speed
                'description': f'Open {row["case_type"]} case'  # Default description
            }
        return cases
    finally:
        conn.close()

# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

def init_daily_rewards_tables():
    """Initialize daily rewards and case opening tables"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        logger.info("🔧 Creating daily_reward_schedule table...")
        # Daily reward schedule (customizable by admin)
        c.execute('''
            CREATE TABLE IF NOT EXISTS daily_reward_schedule (
                day_number INTEGER PRIMARY KEY,
                points INTEGER NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logger.info("✅ daily_reward_schedule table created successfully")
        
        # Insert default schedule if empty
        c.execute('SELECT COUNT(*) as count FROM daily_reward_schedule')
        result = c.fetchone()
        count = result['count'] if result else 0
        if count == 0:
            default_schedule = [
                (1, 50, 'Welcome bonus'),
                (2, 15, 'Day 2 reward'),
                (3, 25, 'Day 3 reward'),
                (4, 40, 'Day 4 reward'),
                (5, 60, 'Day 5 reward'),
                (6, 90, 'Day 6 reward'),
                (7, 150, 'Week complete!'),
            ]
            c.executemany('''
                INSERT INTO daily_reward_schedule (day_number, points, description)
                VALUES (%s, %s, %s)
            ''', default_schedule)
            logger.info("✅ Inserted default 7-day reward schedule")
        
        # Daily login tracking
        c.execute('''
            CREATE TABLE IF NOT EXISTS daily_logins (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                login_date DATE NOT NULL,
                streak_count INTEGER DEFAULT 1,
                points_awarded INTEGER DEFAULT 0,
                claimed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, login_date)
            )
        ''')
        
        # User points balance
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_points (
                user_id BIGINT PRIMARY KEY,
                points INTEGER DEFAULT 0,
                lifetime_points INTEGER DEFAULT 0,
                total_cases_opened INTEGER DEFAULT 0,
                total_products_won INTEGER DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Case opening history
        c.execute('''
            CREATE TABLE IF NOT EXISTS case_openings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                case_type TEXT NOT NULL,
                points_spent INTEGER NOT NULL,
                outcome_type TEXT NOT NULL,
                outcome_value TEXT,
                product_id INTEGER,
                points_won INTEGER DEFAULT 0,
                opened_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Case opening settings (admin configurable)
        c.execute('''
            CREATE TABLE IF NOT EXISTS case_settings (
                id SERIAL PRIMARY KEY,
                case_type TEXT UNIQUE NOT NULL,
                enabled BOOLEAN DEFAULT TRUE,
                cost INTEGER NOT NULL,
                rewards_config JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # DELETE OLD PRE-CREATED CASES (one-time cleanup)
        try:
            c.execute('SELECT COUNT(*) FROM case_settings')
            result = c.fetchone()
            existing_count = result[0] if result else 0
            
            if existing_count > 0:
                logger.info(f"🗑️ Found {existing_count} old pre-created cases, deleting them...")
                # Delete old cases and their data
                c.execute('DELETE FROM case_reward_pools')
                c.execute('DELETE FROM case_lose_emojis')
                c.execute('DELETE FROM case_settings')
                conn.commit()
                logger.info("✅ Deleted all old pre-created cases. Database is clean!")
        except Exception as e:
            logger.warning(f"⚠️ Could not delete old cases: {e}")
        
        # No default cases - admin creates them all
        # Cases are created through admin interface only
        
        # Add product_emoji column to products table (for case opening display)
        try:
            c.execute('''
                ALTER TABLE products 
                ADD COLUMN IF NOT EXISTS product_emoji TEXT DEFAULT '🎁'
            ''')
            logger.info("✅ Added product_emoji column to products table")
        except Exception as e:
            logger.warning(f"⚠️ Could not add product_emoji column: {e}")
        
        logger.info("🔧 Committing all daily rewards table changes...")
        conn.commit()
        logger.info("✅ Commit successful")
        
        # VERIFY TABLE WAS CREATED
        logger.info("🔍 Verifying daily_reward_schedule table exists...")
        c.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'daily_reward_schedule'
            ) as exists
        """)
        result = c.fetchone()
        table_exists = result['exists'] if result else False
        logger.info(f"📊 Table 'daily_reward_schedule' exists: {table_exists}")
        
        if not table_exists:
            raise Exception("❌ CRITICAL: daily_reward_schedule table was not created!")
        
        c.execute('SELECT COUNT(*) as count FROM daily_reward_schedule')
        result = c.fetchone()
        row_count = result['count'] if result else 0
        logger.info(f"📊 Table has {row_count} rows")
        
        logger.info("✅✅✅ Daily rewards tables initialized and committed successfully ✅✅✅")
        
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR initializing daily rewards tables: {e}", exc_info=True)
        try:
            conn.rollback()
            logger.info("🔄 Rolled back failed transaction")
        except Exception as rollback_error:
            logger.error(f"❌ Error during rollback: {rollback_error}")
        raise  # Re-raise to make the error visible
    finally:
        try:
            conn.close()
        except Exception as close_error:
            logger.error(f"❌ Error closing connection: {close_error}")

# ============================================================================
# DAILY REWARDS LOGIC
# ============================================================================

def check_daily_login(user_id: int) -> Dict:
    """Check user's daily login status and calculate streak"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        
        # Get last login
        c.execute('''
            SELECT login_date, streak_count, claimed
            FROM daily_logins
            WHERE user_id = %s
            ORDER BY login_date DESC
            LIMIT 1
        ''', (user_id,))
        
        last_login = c.fetchone()
        
        if not last_login:
            # First time login
            return {
                'can_claim': True,
                'streak': 1,
                'points_to_award': get_reward_for_day(1),
                'next_reward': get_reward_for_day(2),
                'last_login': None,
                'is_first_time': True
            }
        
        last_date = last_login['login_date']
        last_streak = last_login['streak_count']
        last_claimed = last_login['claimed']
        
        if last_date == today:
            # Already logged in today
            return {
                'can_claim': not last_claimed,
                'streak': last_streak,
                'points_to_award': get_reward_for_day(last_streak),
                'next_reward': get_reward_for_day(last_streak + 1),
                'last_login': last_date,
                'is_first_time': False
            }
        
        elif last_date == yesterday:
            # Streak continues (NO MAX LIMIT - infinite progression!)
            new_streak = last_streak + 1
            return {
                'can_claim': True,
                'streak': new_streak,
                'points_to_award': get_reward_for_day(new_streak),
                'next_reward': get_reward_for_day(new_streak + 1),
                'last_login': last_date,
                'is_first_time': False
            }
        
        else:
            # Streak broken, reset to day 1
            return {
                'can_claim': True,
                'streak': 1,
                'points_to_award': get_reward_for_day(1),
                'next_reward': get_reward_for_day(2),
                'last_login': last_date,
                'is_first_time': False,
                'streak_broken': True
            }
    
    finally:
        conn.close()

def claim_daily_reward(user_id: int) -> Dict:
    """Claim daily reward and update streak"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        login_info = check_daily_login(user_id)
        
        if not login_info['can_claim']:
            return {
                'success': False,
                'message': '❌ You already claimed today\'s reward!',
                'points_awarded': 0
            }
        
        today = datetime.now(timezone.utc).date()
        streak = login_info['streak']
        points = login_info['points_to_award']
        
        # Record login
        c.execute('''
            INSERT INTO daily_logins (user_id, login_date, streak_count, points_awarded, claimed)
            VALUES (%s, %s, %s, %s, TRUE)
            ON CONFLICT (user_id, login_date) 
            DO UPDATE SET claimed = TRUE, points_awarded = %s
        ''', (user_id, today, streak, points, points))
        
        # Add points to user
        c.execute('''
            INSERT INTO user_points (user_id, points, lifetime_points)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET 
                points = user_points.points + %s,
                lifetime_points = user_points.lifetime_points + %s,
                updated_at = CURRENT_TIMESTAMP
        ''', (user_id, points, points, points, points))
        
        conn.commit()
        
        return {
            'success': True,
            'points_awarded': points,
            'new_streak': streak,
            'total_points': get_user_points(user_id),
            'message': f'🎁 +{points} points! Day {streak} streak!'
        }
    
    except Exception as e:
        logger.error(f"Error claiming daily reward: {e}")
        conn.rollback()
        return {'success': False, 'message': str(e)}
    finally:
        conn.close()

def get_user_points(user_id: int) -> int:
    """Get user's current points balance"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('SELECT points FROM user_points WHERE user_id = %s', (user_id,))
        result = c.fetchone()
        return result['points'] if result else 0
    finally:
        conn.close()

# ============================================================================
# CASE OPENING LOGIC
# ============================================================================

def open_case(user_id: int, case_type: str) -> Dict:
    """Open a case and determine outcome"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get cases from database
        all_cases = get_all_cases()
        logger.info(f"📦 All cases: {list(all_cases.keys())}")
        
        if case_type not in all_cases:
            logger.error(f"❌ Case type '{case_type}' not found in {list(all_cases.keys())}")
            return {'success': False, 'message': '❌ Invalid case type!'}
        
        case_config = all_cases[case_type]
        logger.info(f"📦 Case config for '{case_type}': {case_config}")
        
        cost = case_config['cost']
        user_points = get_user_points(user_id)
        
        if user_points < cost:
            return {
                'success': False,
                'message': f'❌ Not enough points! Need {cost}, you have {user_points}'
            }
        
        # Check if case has rewards configured
        rewards = case_config.get('rewards', {})
        if not rewards:
            logger.error(f"❌ Case '{case_type}' has no rewards configured!")
            return {
                'success': False,
                'message': f'❌ Case not configured! Admin needs to set up rewards for this case.'
            }
        
        # Deduct points
        c.execute('''
            UPDATE user_points 
            SET points = points - %s, 
                total_cases_opened = total_cases_opened + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s
        ''', (cost, user_id))
        
        # Determine outcome (weighted random)
        logger.info(f"🎲 Determining outcome with rewards: {rewards}")
        outcome = determine_case_outcome(rewards)
        
        # Process outcome
        reward_data = process_case_outcome(user_id, case_type, outcome, cost, c)
        
        # Record opening
        c.execute('''
            INSERT INTO case_openings 
            (user_id, case_type, points_spent, outcome_type, outcome_value, product_id, points_won)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (
            user_id, 
            case_type, 
            cost, 
            outcome,
            json.dumps(reward_data),
            reward_data.get('product_id'),
            reward_data.get('points', 0)
        ))
        
        conn.commit()
        
        # Generate animation data
        animation_data = generate_animation_data(case_config, outcome, reward_data)
        
        return {
            'success': True,
            'outcome': outcome,
            'reward': reward_data,
            'animation_data': animation_data,
            'new_balance': get_user_points(user_id)
        }
    
    except Exception as e:
        logger.error(f"Error opening case: {e}")
        conn.rollback()
        return {'success': False, 'message': str(e)}
    finally:
        conn.close()

def determine_case_outcome(rewards: Dict[str, int]) -> str:
    """Weighted random outcome selection"""
    if not rewards:
        logger.error("Empty rewards dict passed to determine_case_outcome")
        return 'lose_all'  # Default fallback
    
    outcomes = list(rewards.keys())
    weights = list(rewards.values())
    
    if not outcomes or not weights:
        logger.error(f"Empty outcomes or weights: outcomes={outcomes}, weights={weights}")
        return 'lose_all'  # Default fallback
    
    try:
        return random.choices(outcomes, weights=weights, k=1)[0]
    except (IndexError, ValueError) as e:
        logger.error(f"Error in random.choices: {e}, outcomes={outcomes}, weights={weights}")
        return 'lose_all'  # Default fallback

def process_case_outcome(user_id: int, case_type: str, outcome: str, cost: int, cursor) -> Dict:
    """Process the outcome and update database"""
    
    if 'win_product' in outcome:
        # Award random product from available stock
        product_id = get_random_available_product()
        if product_id:
            cursor.execute('''
                UPDATE user_points 
                SET total_products_won = total_products_won + 1
                WHERE user_id = %s
            ''', (user_id,))
            
            return {
                'type': 'product',
                'product_id': product_id,
                'message': '🎁 YOU WON A PRODUCT!',
                'value': f'Product #{product_id}'
            }
        else:
            # No products available, give 3x points instead
            points_won = cost * 3
            cursor.execute('''
                UPDATE user_points 
                SET points = points + %s
                WHERE user_id = %s
            ''', (points_won, user_id))
            
            return {
                'type': 'points_win',
                'points': points_won,
                'multiplier': 3,
                'message': '💰 3x POINTS (No products available)',
                'value': f'+{points_won} points'
            }
    
    elif 'win_points' in outcome:
        # Award points multiplier
        multiplier = int(outcome.split('_')[-1].replace('x', ''))
        points_won = cost * multiplier
        
        cursor.execute('''
            UPDATE user_points 
            SET points = points + %s
            WHERE user_id = %s
        ''', (points_won, user_id))
        
        return {
            'type': 'points_win',
            'points': points_won,
            'multiplier': multiplier,
            'message': f'💰 {multiplier}x MULTIPLIER!',
            'value': f'+{points_won} points'
        }
    
    elif 'lose_half' in outcome:
        # Lose half points
        points_lost = cost // 2
        return {
            'type': 'points_loss',
            'points': -points_lost,
            'message': '😢 Lost half your bet',
            'value': f'-{points_lost} points'
        }
    
    else:  # lose_all
        return {
            'type': 'points_loss',
            'points': -cost,
            'message': '💸 Better luck next time!',
            'value': f'-{cost} points'
        }

def get_random_available_product() -> Optional[int]:
    """Get random product ID from available stock"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get products with available > 0
        c.execute('''
            SELECT id FROM products 
            WHERE available > 0 
            ORDER BY RANDOM() 
            LIMIT 1
        ''')
        
        result = c.fetchone()
        return result['id'] if result else None
    finally:
        conn.close()

# ============================================================================
# PREMIUM ANIMATION GENERATION
# ============================================================================

def generate_animation_data(case_config: Dict, outcome: str, reward: Dict) -> Dict:
    """Generate animation sequence data for premium experience"""
    
    # Reel items (what spins past before landing on outcome)
    reel_items = generate_reel_sequence(case_config, outcome)
    
    return {
        'case_emoji': case_config['emoji'],
        'case_color': case_config['color'],
        'animation_speed': case_config['animation_speed'],
        'reel_items': reel_items,
        'final_outcome': {
            'emoji': get_outcome_emoji(outcome),
            'message': reward['message'],
            'value': reward['value'],
            'glow_color': get_outcome_color(outcome)
        },
        'particles': generate_particle_effects(outcome),
        'sound': get_outcome_sound(outcome),
        'duration_ms': get_animation_duration(case_config['animation_speed'])
    }

def generate_reel_sequence(case_config: Dict, final_outcome: str) -> List[Dict]:
    """Generate 30 items that "spin" before landing on outcome"""
    emojis = ['💎', '🎁', '💰', '🏆', '💸', '😢', '🔥', '⭐', '💵', '🎉']
    
    reel = []
    for i in range(30):
        if i == 29:  # Last item is the outcome
            reel.append({
                'emoji': get_outcome_emoji(final_outcome),
                'is_outcome': True
            })
        else:
            reel.append({
                'emoji': random.choice(emojis),
                'is_outcome': False
            })
    
    return reel

def get_outcome_emoji(outcome: str) -> str:
    """Get emoji for outcome type"""
    mapping = {
        'win_product': '🎁',
        'win_points_5x': '🏆',
        'win_points_3x': '💎',
        'win_points_2x': '💰',
        'win_points_1x': '💵',
        'lose_half': '😢',
        'lose_all': '💸'
    }
    return mapping.get(outcome, '❓')

def get_outcome_color(outcome: str) -> str:
    """Get glow color for outcome"""
    if 'win_product' in outcome or '5x' in outcome:
        return '🟡'  # Gold/Legendary
    elif '3x' in outcome:
        return '🟣'  # Purple/Epic
    elif '2x' in outcome or '1x' in outcome:
        return '🟢'  # Green/Win
    else:
        return '🔴'  # Red/Loss

def generate_particle_effects(outcome: str) -> List[str]:
    """Generate particle effect emojis"""
    if 'win_product' in outcome:
        return ['✨', '🎆', '🎇', '💫', '⭐', '🌟'] * 3
    elif 'win_points' in outcome:
        return ['💰', '💵', '💴', '💶', '💷'] * 2
    else:
        return ['💨', '💭'] * 2

def get_outcome_sound(outcome: str) -> str:
    """Get sound effect identifier"""
    if 'win_product' in outcome:
        return 'jackpot'
    elif 'win_points' in outcome:
        return 'win'
    else:
        return 'lose'

def get_animation_duration(speed: str) -> int:
    """Get animation duration in milliseconds"""
    durations = {
        'normal': 3000,  # 3 seconds
        'fast': 2000,    # 2 seconds  
        'epic': 5000     # 5 seconds (dramatic)
    }
    return durations.get(speed, 3000)

# ============================================================================
# STATISTICS & LEADERBOARD
# ============================================================================

def get_user_stats(user_id: int) -> Dict:
    """Get user's case opening statistics"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            SELECT 
                points,
                lifetime_points,
                total_cases_opened,
                total_products_won
            FROM user_points
            WHERE user_id = %s
        ''', (user_id,))
        
        stats = c.fetchone()
        
        if not stats:
            return {
                'points': 0,
                'lifetime_points': 0,
                'cases_opened': 0,
                'products_won': 0,
                'win_rate': 0
            }
        
        # Calculate win rate
        c.execute('''
            SELECT COUNT(*) as wins
            FROM case_openings
            WHERE user_id = %s AND outcome_type LIKE 'win_%'
        ''', (user_id,))
        
        wins = c.fetchone()['wins']
        total = stats['total_cases_opened']
        win_rate = (wins / total * 100) if total > 0 else 0
        
        # Get streak info
        c.execute('''
            SELECT streak_count
            FROM daily_logins
            WHERE user_id = %s
            ORDER BY login_date DESC
            LIMIT 1
        ''', (user_id,))
        
        streak_row = c.fetchone()
        current_streak = streak_row['streak_count'] if streak_row else 0
        
        # Get longest streak (max streak_count)
        c.execute('''
            SELECT MAX(streak_count) as longest
            FROM daily_logins
            WHERE user_id = %s
        ''', (user_id,))
        
        longest_row = c.fetchone()
        longest_streak = longest_row['longest'] if longest_row and longest_row['longest'] else 0
        
        return {
            'current_points': stats['points'],
            'points': stats['points'],
            'lifetime_points': stats['lifetime_points'],
            'total_cases_opened': stats['total_cases_opened'],
            'total_products_won': stats['total_products_won'],
            'current_streak': current_streak,
            'longest_streak': longest_streak,
            'win_rate': round(win_rate, 1)
        }
    
    finally:
        conn.close()

def get_leaderboard(limit: int = 10) -> List[Dict]:
    """Get top players leaderboard (top 10 only)"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            SELECT 
                user_id,
                total_cases_opened,
                total_products_won,
                points,
                lifetime_points
            FROM user_points
            WHERE total_cases_opened > 0
            ORDER BY lifetime_points DESC, total_cases_opened DESC
            LIMIT %s
        ''', (limit,))
        
        return c.fetchall()
    
    finally:
        conn.close()

