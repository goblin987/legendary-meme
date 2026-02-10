"""
Worker Management System - Core Functions
Handles CRUD operations, permissions, and activity logging for workers.
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from decimal import Decimal

# Import database connection from utils
from utils import get_db_connection

logger = logging.getLogger(__name__)

# ============= WORKER CRUD OPERATIONS =============

def add_worker(username: str, user_id: int, added_by_admin_id: int, 
               permissions: List[str], allowed_locations: Dict[str, Any]) -> Optional[int]:
    """
    Add a new worker to the system.
    
    Args:
        username: Telegram username (without @)
        user_id: Telegram user ID
        added_by_admin_id: Admin who added this worker
        permissions: List of permission strings like ["add_products", "check_stock", "marketing"]
        allowed_locations: Dict like {"city_id": ["dist_id1", "dist_id2"]} or {"city_id": "all"}
    
    Returns:
        Worker ID if successful, None otherwise
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Check if worker already exists
        c.execute("SELECT id, is_active FROM workers WHERE user_id = %s", (user_id,))
        existing = c.fetchone()
        
        if existing:
            if existing['is_active']:
                logger.warning(f"Worker with user_id {user_id} already exists and is active")
                return None
            else:
                # Reactivate existing worker
                c.execute("""
                    UPDATE workers 
                    SET is_active = true, permissions = %s, allowed_locations = %s,
                        username = %s, added_by = %s, added_date = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                    RETURNING id
                """, (json.dumps(permissions), json.dumps(allowed_locations), 
                      username, added_by_admin_id, user_id))
                worker_id = c.fetchone()['id']
                conn.commit()
                logger.info(f"✅ Reactivated worker {username} (ID: {worker_id})")
                return worker_id
        
        # Insert new worker
        c.execute("""
            INSERT INTO workers (user_id, username, added_by, permissions, allowed_locations)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (user_id, username, added_by_admin_id, json.dumps(permissions), json.dumps(allowed_locations)))
        
        worker_id = c.fetchone()['id']
        conn.commit()
        logger.info(f"✅ Added new worker {username} (ID: {worker_id})")
        return worker_id
        
    except Exception as e:
        logger.error(f"❌ Error adding worker: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

def remove_worker(worker_id: int) -> bool:
    """
    Deactivate a worker (soft delete).
    
    Args:
        worker_id: Worker ID to deactivate
    
    Returns:
        True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("UPDATE workers SET is_active = false WHERE id = %s", (worker_id,))
        conn.commit()
        
        logger.info(f"✅ Deactivated worker ID {worker_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error removing worker: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def get_worker_by_user_id(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Get worker record by Telegram user ID.
    
    Args:
        user_id: Telegram user ID
    
    Returns:
        Worker record dict or None
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT id, user_id, username, added_by, added_date, 
                   permissions, allowed_locations, is_active
            FROM workers
            WHERE user_id = %s AND is_active = true
        """, (user_id,))
        
        worker = c.fetchone()
        if worker:
            return dict(worker)
        return None
        
    except Exception as e:
        # Silent fail if workers table doesn't exist yet
        if "does not exist" not in str(e):
            logger.error(f"❌ Error getting worker by user_id: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

def get_worker_by_id(worker_id: int) -> Optional[Dict[str, Any]]:
    """
    Get worker record by worker ID.
    
    Args:
        worker_id: Worker ID
    
    Returns:
        Worker record dict or None
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT id, user_id, username, added_by, added_date, 
                   permissions, allowed_locations, is_active
            FROM workers
            WHERE id = %s
        """, (worker_id,))
        
        worker = c.fetchone()
        if worker:
            return dict(worker)
        return None
        
    except Exception as e:
        logger.error(f"❌ Error getting worker by ID: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

def get_all_workers(include_inactive: bool = False) -> List[Dict[str, Any]]:
    """
    Get all workers.
    
    Args:
        include_inactive: Include deactivated workers
    
    Returns:
        List of worker dicts
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        if include_inactive:
            c.execute("""
                SELECT id, user_id, username, added_by, added_date, 
                       permissions, allowed_locations, is_active
                FROM workers
                ORDER BY added_date DESC
            """)
        else:
            c.execute("""
                SELECT id, user_id, username, added_by, added_date, 
                       permissions, allowed_locations, is_active
                FROM workers
                WHERE is_active = true
                ORDER BY added_date DESC
            """)
        
        return [dict(row) for row in c.fetchall()]
        
    except Exception as e:
        logger.error(f"❌ Error getting all workers: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

def update_worker_permissions(worker_id: int, permissions: List[str]) -> bool:
    """
    Update worker permissions.
    
    Args:
        worker_id: Worker ID
        permissions: New list of permissions
    
    Returns:
        True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            UPDATE workers 
            SET permissions = %s
            WHERE id = %s AND is_active = true
        """, (json.dumps(permissions), worker_id))
        
        conn.commit()
        logger.info(f"✅ Updated permissions for worker ID {worker_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating worker permissions: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def update_worker_locations(worker_id: int, locations: Dict[str, Any]) -> bool:
    """
    Update worker allowed locations.
    
    Args:
        worker_id: Worker ID
        locations: New locations dict
    
    Returns:
        True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("""
            UPDATE workers 
            SET allowed_locations = %s
            WHERE id = %s AND is_active = true
        """, (json.dumps(locations), worker_id))
        
        conn.commit()
        logger.info(f"✅ Updated locations for worker ID {worker_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating worker locations: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# ============= PERMISSION CHECKING =============

def check_worker_permission(user_id: int, permission_name: str) -> bool:
    """
    Check if worker has a specific permission.
    
    Args:
        user_id: Telegram user ID
        permission_name: Permission to check (e.g., "add_products", "check_stock", "marketing")
    
    Returns:
        True if worker has permission, False otherwise
    """
    worker = get_worker_by_user_id(user_id)
    if not worker:
        return False
    
    permissions = worker.get('permissions', [])
    if isinstance(permissions, str):
        permissions = json.loads(permissions)
    
    return permission_name in permissions

def check_worker_location_access(user_id: int, city: str, district: str) -> bool:
    """
    Check if worker has access to a specific city/district.
    
    Args:
        user_id: Telegram user ID
        city: City name
        district: District name
    
    Returns:
        True if worker has access, False otherwise
    """
    worker = get_worker_by_user_id(user_id)
    if not worker:
        return False
    
    allowed_locations = worker.get('allowed_locations', {})
    if isinstance(allowed_locations, str):
        allowed_locations = json.loads(allowed_locations)
    
    # Check if worker has access to this city
    city_access = allowed_locations.get(city)
    if not city_access:
        return False
    
    # If city_access is "all", worker has access to all districts
    if city_access == "all":
        return True
    
    # Otherwise, check if district is in the list
    return district in city_access

def is_worker(user_id: int) -> bool:
    """
    Check if user is an active worker.
    
    Args:
        user_id: Telegram user ID
    
    Returns:
        True if user is an active worker, False otherwise
    """
    worker = get_worker_by_user_id(user_id)
    return worker is not None

# ============= ACTIVITY LOGGING =============

def log_worker_activity(worker_id: int, action_type: str, product_id: Optional[int] = None,
                        product_count: int = 1, details: Optional[Dict[str, Any]] = None) -> bool:
    """
    Log worker activity.
    
    Args:
        worker_id: Worker ID
        action_type: Type of action (e.g., "add_product", "bulk_add", "view_stock")
        product_id: Product ID if applicable
        product_count: Number of products (for bulk operations)
        details: Additional details dict (city, district, type, etc.)
    
    Returns:
        True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        if details is None:
            details = {}
        
        c.execute("""
            INSERT INTO worker_activity_log (worker_id, action_type, product_id, product_count, details)
            VALUES (%s, %s, %s, %s, %s)
        """, (worker_id, action_type, product_id, product_count, json.dumps(details)))
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error logging worker activity: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# ============= ANALYTICS =============

def get_worker_stats(worker_id: int, date_from: Optional[datetime] = None, 
                     date_to: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Get performance statistics for a specific worker.
    
    Args:
        worker_id: Worker ID
        date_from: Start date (defaults to 30 days ago)
        date_to: End date (defaults to now)
    
    Returns:
        Dict with statistics
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        if date_from is None:
            date_from = datetime.now() - timedelta(days=30)
        if date_to is None:
            date_to = datetime.now()
        
        # Get worker info
        worker = get_worker_by_id(worker_id)
        if not worker:
            return {}
        
        # Total products added
        c.execute("""
            SELECT COUNT(*) as total
            FROM products
            WHERE added_by_worker_id = %s
        """, (worker_id,))
        total_added = c.fetchone()['total']
        
        # Products added by type
        c.execute("""
            SELECT product_type, COUNT(*) as count
            FROM products
            WHERE added_by_worker_id = %s
            GROUP BY product_type
            ORDER BY count DESC
        """, (worker_id,))
        by_type = {row['product_type']: row['count'] for row in c.fetchall()}
        
        # Products added by location
        c.execute("""
            SELECT city, district, COUNT(*) as count
            FROM products
            WHERE added_by_worker_id = %s
            GROUP BY city, district
            ORDER BY count DESC
        """, (worker_id,))
        by_location = [{"city": row['city'], "district": row['district'], "count": row['count']} 
                       for row in c.fetchall()]
        
        # Products sold (from purchases table)
        c.execute("""
            SELECT COUNT(*) as sold, COALESCE(SUM(pr.price), 0) as revenue
            FROM purchases pu
            JOIN products pr ON pu.product_id = pr.id
            WHERE pr.added_by_worker_id = %s AND pu.status = 'completed'
        """, (worker_id,))
        sold_data = c.fetchone()
        total_sold = sold_data['sold']
        revenue = float(sold_data['revenue']) if sold_data['revenue'] else 0.0
        
        # Activity log stats
        c.execute("""
            SELECT action_type, COUNT(*) as count
            FROM worker_activity_log
            WHERE worker_id = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY action_type
        """, (worker_id, date_from, date_to))
        activity = {row['action_type']: row['count'] for row in c.fetchall()}
        
        return {
            'worker': worker,
            'total_added': total_added,
            'by_type': by_type,
            'by_location': by_location,
            'total_sold': total_sold,
            'revenue': revenue,
            'activity': activity,
            'date_from': date_from,
            'date_to': date_to
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting worker stats: {e}", exc_info=True)
        return {}
    finally:
        if conn:
            conn.close()

def get_all_workers_stats(date_from: Optional[datetime] = None, 
                          date_to: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """
    Get aggregated statistics for all workers.
    
    Args:
        date_from: Start date (defaults to 30 days ago)
        date_to: End date (defaults to now)
    
    Returns:
        List of dicts with worker statistics
    """
    workers = get_all_workers()
    stats = []
    
    for worker in workers:
        worker_stats = get_worker_stats(worker['id'], date_from, date_to)
        if worker_stats:
            stats.append(worker_stats)
    
    return stats

