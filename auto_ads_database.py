"""
Auto Ads Database Management (PostgreSQL)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Professional database layer for auto ads system using PostgreSQL.
Provides secure data persistence, account management, campaign storage, and performance analytics.

Version: 2.0.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import logging
from typing import Dict, List, Optional
from utils import get_db_connection

logger = logging.getLogger(__name__)

class AutoAdsDatabase:
    """PostgreSQL database manager for auto ads system"""
    
    def __init__(self):
        """Initialize database connection using utils.get_db_connection()"""
        pass  # Connection is managed per-query via get_db_connection()
    
    def _get_conn(self):
        """Get PostgreSQL database connection from utils"""
        return get_db_connection()
    
    def init_tables(self):
        """Initialize auto ads database tables (PostgreSQL)"""
        conn = None
        try:
            logger.info("🔧 Initializing auto ads database tables...")
            conn = self._get_conn()
            cur = conn.cursor()
            
            logger.info("📊 Creating auto_ads_accounts table...")
            # Auto ads accounts table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS auto_ads_accounts (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    account_name TEXT NOT NULL,
                    phone_number TEXT NOT NULL,
                    api_id TEXT NOT NULL,
                    api_hash TEXT NOT NULL,
                    session_string TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            logger.info("✅ auto_ads_accounts table created/verified")
            
            logger.info("📊 Creating auto_ads_campaigns table...")
            # Auto ads campaigns table - persist campaigns across restarts
            cur.execute('''
                CREATE TABLE IF NOT EXISTS auto_ads_campaigns (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    account_id INTEGER REFERENCES auto_ads_accounts(id) ON DELETE CASCADE,
                    campaign_name TEXT NOT NULL,
                    ad_content JSONB,
                    target_chats JSONB,
                    buttons JSONB,
                    schedule_type TEXT,
                    schedule_time TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    sent_count INTEGER DEFAULT 0,
                    last_sent TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            logger.info("✅ auto_ads_campaigns table created fresh")
            
            # No column migration needed - table is fresh!
            
            logger.info("📊 Creating account_usage_tracking table...")
            # Account usage tracking for anti-ban
            cur.execute('''
                CREATE TABLE IF NOT EXISTS account_usage_tracking (
                    id SERIAL PRIMARY KEY,
                    account_id INTEGER REFERENCES auto_ads_accounts(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    messages_sent INTEGER DEFAULT 0,
                    last_message_time TIMESTAMP,
                    UNIQUE(account_id, date)
                )
            ''')
            logger.info("✅ account_usage_tracking table created/verified")
            
            logger.info("📊 Creating indexes...")
            # Create indexes for performance
            cur.execute('CREATE INDEX IF NOT EXISTS idx_aa_accounts_user_id ON auto_ads_accounts(user_id)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_aa_campaigns_user_id ON auto_ads_campaigns(user_id)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_aa_campaigns_account_id ON auto_ads_campaigns(account_id)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_aa_tracking_account_id ON account_usage_tracking(account_id)')
            logger.info("✅ Indexes created/verified")
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info("✅ Auto ads database tables initialized successfully")
        except Exception as e:
            logger.error(f"❌ Error initializing auto ads tables: {type(e).__name__}: {e}")
            logger.exception("Full traceback:")
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
    
    def add_telegram_account(self, user_id: int, account_name: str, phone_number: str, 
                           api_id: str, api_hash: str, session_string: str = None) -> int:
        """Add Telegram account"""
        conn = None
        try:
            logger.info(f"🔍 Attempting to add account: {account_name} for user {user_id}")
            conn = self._get_conn()
            logger.info(f"✅ Database connection established")
            cur = conn.cursor()
            
            # First check if table exists (simpler approach)
            try:
                cur.execute("SELECT 1 FROM auto_ads_accounts LIMIT 1")
                table_exists = True
                logger.info(f"📊 Table auto_ads_accounts exists: True")
            except Exception as check_error:
                table_exists = False
                logger.warning(f"📊 Table auto_ads_accounts does not exist: {check_error}")
                logger.info("🔧 Creating tables now...")
                cur.close()
                conn.close()
                self.init_tables()
                conn = self._get_conn()
                cur = conn.cursor()
                logger.info("✅ Tables created, ready to insert")
            
            logger.info(f"💾 Inserting account data...")
            cur.execute('''
                INSERT INTO auto_ads_accounts 
                (user_id, account_name, phone_number, api_id, api_hash, session_string)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (user_id, account_name, phone_number, api_id, api_hash, session_string))
            account_id = cur.fetchone()['id']  # Use dict access like existing userbot system
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"✅ Added auto ads account {account_name} (ID: {account_id})")
            return account_id
        except Exception as e:
            logger.error(f"❌ Error adding telegram account: {type(e).__name__}: {e}")
            logger.exception("Full traceback:")
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
            raise Exception(f"Database error: {type(e).__name__}: {str(e)}")
    
    def get_user_accounts(self, user_id: int) -> List[Dict]:
        """Get all Telegram accounts for a user"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                SELECT id, user_id, account_name, phone_number, api_id, api_hash, 
                       session_string, is_active, created_at
                FROM auto_ads_accounts 
                WHERE user_id = %s AND is_active = TRUE
                ORDER BY created_at DESC
            ''', (user_id,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            return [{
                'id': row['id'],
                'user_id': row['user_id'],
                'account_name': row['account_name'],
                'phone_number': row['phone_number'],
                'api_id': row['api_id'],
                'api_hash': row['api_hash'],
                'session_string': row['session_string'],
                'is_active': row['is_active'],
                'created_at': row['created_at']
            } for row in rows]
        except Exception as e:
            logger.error(f"❌ Error getting user accounts: {e}")
            return []

    def get_all_accounts(self) -> List[Dict]:
        """Get all active Telegram accounts (for workers/admins)"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                SELECT id, user_id, account_name, phone_number, api_id, api_hash, 
                       session_string, is_active, created_at
                FROM auto_ads_accounts 
                WHERE is_active = TRUE
                ORDER BY created_at DESC
            ''')
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            return [{
                'id': row['id'],
                'user_id': row['user_id'],
                'account_name': row['account_name'],
                'phone_number': row['phone_number'],
                'api_id': row['api_id'],
                'api_hash': row['api_hash'],
                'session_string': row['session_string'],
                'is_active': row['is_active'],
                'created_at': row['created_at']
            } for row in rows]
        except Exception as e:
            logger.error(f"❌ Error getting all accounts: {e}")
            return []
    
    def get_account(self, account_id: int) -> Optional[Dict]:
        """Get account by ID"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                SELECT id, user_id, account_name, phone_number, api_id, api_hash, 
                       session_string, is_active, created_at
                FROM auto_ads_accounts 
                WHERE id = %s
            ''', (account_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                return {
                    'id': row['id'],
                    'user_id': row['user_id'],
                    'account_name': row['account_name'],
                    'phone_number': row['phone_number'],
                    'api_id': row['api_id'],
                    'api_hash': row['api_hash'],
                    'session_string': row['session_string'],
                    'is_active': row['is_active'],
                    'created_at': row['created_at']
                }
            return None
        except Exception as e:
            logger.error(f"❌ Error getting account: {e}")
            return None
    
    def update_account_session(self, account_id: int, session_string: str):
        """Update account session string"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                UPDATE auto_ads_accounts 
                SET session_string = %s
                WHERE id = %s
            ''', (session_string, account_id))
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"✅ Updated session for account {account_id}")
        except Exception as e:
            logger.error(f"❌ Error updating account session: {e}")
            if conn:
                conn.rollback()
                conn.close()
    
    def delete_account(self, account_id: int):
        """Delete Telegram account and clean up all related data"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            
            # Get account info before deletion for logging
            cur.execute('SELECT account_name, phone_number FROM auto_ads_accounts WHERE id = %s', (account_id,))
            account_info = cur.fetchone()
            
            # Completely remove the account record (not just deactivate)
            cur.execute('DELETE FROM auto_ads_accounts WHERE id = %s', (account_id,))
            
            # Also clean up related data
            cur.execute('DELETE FROM auto_ads_campaigns WHERE account_id = %s', (account_id,))
            cur.execute('DELETE FROM account_usage_tracking WHERE account_id = %s', (account_id,))
            
            conn.commit()
            cur.close()
            conn.close()
            
            if account_info:
                logger.info(f"✅ Completely deleted account '{account_info[0]}' ({account_info[1]}) and all related data")
            else:
                logger.info(f"✅ Deleted account {account_id} and all related data")
        except Exception as e:
            logger.error(f"❌ Error deleting account: {e}")
            if conn:
                conn.rollback()
                conn.close()
    
    def add_campaign(self, user_id: int, account_id: int, campaign_name: str, 
                    ad_content: any, target_chats: list, buttons: list = None,
                    schedule_type: str = 'once', schedule_time: str = None) -> int:
        """Add a new campaign"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            
            # Convert data to JSON
            ad_content_json = json.dumps(ad_content) if isinstance(ad_content, (dict, list)) else ad_content
            target_chats_json = json.dumps(target_chats) if isinstance(target_chats, list) else target_chats
            buttons_json = json.dumps(buttons) if buttons else None
            
            cur.execute('''
                INSERT INTO auto_ads_campaigns 
                (user_id, account_id, campaign_name, ad_content, target_chats, buttons,
                 schedule_type, schedule_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (user_id, account_id, campaign_name, ad_content_json, target_chats_json, 
                  buttons_json, schedule_type, schedule_time))
            
            campaign_id = cur.fetchone()['id']  # Use dict access like existing userbot system
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"✅ Added campaign '{campaign_name}' (ID: {campaign_id})")
            return campaign_id
        except Exception as e:
            logger.error(f"❌ Error adding campaign: {e}")
            if conn:
                conn.rollback()
                conn.close()
            raise
    
    def get_user_campaigns(self, user_id: int) -> List[Dict]:
        """Get all campaigns for a user"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                SELECT c.id, c.user_id, c.account_id, c.campaign_name, c.ad_content,
                       c.target_chats, c.buttons, c.schedule_type, c.schedule_time,
                       c.is_active, c.sent_count, c.last_sent, c.created_at,
                       a.account_name, a.phone_number
                FROM auto_ads_campaigns c
                LEFT JOIN auto_ads_accounts a ON c.account_id = a.id
                WHERE c.user_id = %s
                ORDER BY c.created_at DESC
            ''', (user_id,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            campaigns = []
            for row in rows:
                campaign = {
                    'id': row['id'],
                    'user_id': row['user_id'],
                    'account_id': row['account_id'],
                    'campaign_name': row['campaign_name'],
                    'ad_content': self._parse_json(row['ad_content']),
                    'target_chats': self._parse_json(row['target_chats']),
                    'buttons': self._parse_json(row['buttons']),
                    'schedule_type': row['schedule_type'],
                    'schedule_time': row['schedule_time'],
                    'is_active': row['is_active'],
                    'sent_count': row['sent_count'] or 0,
                    'last_sent': row['last_sent'],
                    'created_at': row['created_at'],
                    'account_name': row['account_name'],
                    'phone_number': row['phone_number']
                }
                campaigns.append(campaign)
            
            return campaigns
        except Exception as e:
            logger.error(f"❌ Error getting user campaigns: {e}")
            return []
            
    def get_all_campaigns(self) -> List[Dict]:
        """Get all campaigns (for admins)"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                SELECT c.id, c.user_id, c.account_id, c.campaign_name, c.ad_content,
                       c.target_chats, c.buttons, c.schedule_type, c.schedule_time,
                       c.is_active, c.sent_count, c.last_sent, c.created_at,
                       a.account_name, a.phone_number
                FROM auto_ads_campaigns c
                LEFT JOIN auto_ads_accounts a ON c.account_id = a.id
                ORDER BY c.created_at DESC
            ''')
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            campaigns = []
            for row in rows:
                campaign = {
                    'id': row['id'],
                    'user_id': row['user_id'],
                    'account_id': row['account_id'],
                    'campaign_name': row['campaign_name'],
                    'ad_content': self._parse_json(row['ad_content']),
                    'target_chats': self._parse_json(row['target_chats']),
                    'buttons': self._parse_json(row['buttons']),
                    'schedule_type': row['schedule_type'],
                    'schedule_time': row['schedule_time'],
                    'is_active': row['is_active'],
                    'sent_count': row['sent_count'] or 0,
                    'last_sent': row['last_sent'],
                    'created_at': row['created_at'],
                    'account_name': row['account_name'],
                    'phone_number': row['phone_number']
                }
                campaigns.append(campaign)
            
            return campaigns
        except Exception as e:
            logger.error(f"❌ Error getting all campaigns: {e}")
            return []
    
    def get_campaign(self, campaign_id: int) -> Optional[Dict]:
        """Get a campaign by ID"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                SELECT c.id, c.user_id, c.account_id, c.campaign_name, c.ad_content,
                       c.target_chats, c.buttons, c.schedule_type, c.schedule_time,
                       c.is_active, c.sent_count, c.last_sent, c.created_at,
                       a.account_name, a.phone_number
                FROM auto_ads_campaigns c
                LEFT JOIN auto_ads_accounts a ON c.account_id = a.id
                WHERE c.id = %s
            ''', (campaign_id,))
            
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                return {
                    'id': row['id'],
                    'user_id': row['user_id'],
                    'account_id': row['account_id'],
                    'campaign_name': row['campaign_name'],
                    'ad_content': self._parse_json(row['ad_content']),
                    'target_chats': self._parse_json(row['target_chats']),
                    'buttons': self._parse_json(row['buttons']),
                    'schedule_type': row['schedule_type'],
                    'schedule_time': row['schedule_time'],
                    'is_active': row['is_active'],
                    'sent_count': row['sent_count'] or 0,
                    'last_sent': row['last_sent'],
                    'created_at': row['created_at'],
                    'account_name': row['account_name'],
                    'phone_number': row['phone_number']
                }
            return None
        except Exception as e:
            logger.error(f"❌ Error getting campaign: {e}")
            return None
    
    def update_campaign_last_run(self, campaign_id: int):
        """Update the last run time for a campaign"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                UPDATE auto_ads_campaigns 
                SET last_sent = NOW(),
                    sent_count = COALESCE(sent_count, 0) + 1
                WHERE id = %s
            ''', (campaign_id,))
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"✅ Updated last run for campaign {campaign_id}")
        except Exception as e:
            logger.error(f"❌ Error updating campaign last run: {e}")
            if conn:
                conn.rollback()
                conn.close()
    
    def update_campaign_status(self, campaign_id: int, is_active: bool):
        """Update campaign active status"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('''
                UPDATE auto_ads_campaigns 
                SET is_active = %s
                WHERE id = %s
            ''', (is_active, campaign_id))
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"✅ Updated campaign {campaign_id} status to {is_active}")
        except Exception as e:
            logger.error(f"❌ Error updating campaign status: {e}")
            if conn:
                conn.rollback()
                conn.close()
    
    def delete_campaign(self, campaign_id: int):
        """Delete a campaign"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute('DELETE FROM auto_ads_campaigns WHERE id = %s', (campaign_id,))
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"✅ Deleted campaign {campaign_id}")
        except Exception as e:
            logger.error(f"❌ Error deleting campaign: {e}")
            if conn:
                conn.rollback()
                conn.close()
    
    def _parse_json(self, json_str):
        """Parse JSON string, return empty dict/list on error"""
        if not json_str:
            return None
        try:
            if isinstance(json_str, (dict, list)):
                return json_str
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return {}


