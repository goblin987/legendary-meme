"""
Auto Ads Campaign Scheduler
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Background scheduler that runs campaigns based on their schedule type:
- hourly: Every 1 hour
- daily: Every 24 hours
- weekly: Every 7 days
- once: Manual execution only

Version: 1.0.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
from datetime import datetime, timedelta
from auto_ads_database import AutoAdsDatabase
from auto_ads_bump_service import AutoAdsBumpService

logger = logging.getLogger(__name__)

class AutoAdsScheduler:
    """Background scheduler for automated campaign execution"""
    
    def __init__(self, bot_instance=None):
        self.db = AutoAdsDatabase()
        self.service = AutoAdsBumpService(bot_instance)
        self.running = False
        self.task = None
        
    async def start(self):
        """Start the scheduler"""
        if self.running:
            logger.warning("Scheduler already running")
            return
        
        self.running = True
        self.task = asyncio.create_task(self._scheduler_loop())
        logger.info("✅ Auto Ads Scheduler started")
        
    async def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("⏹️ Auto Ads Scheduler stopped")
        
    async def _scheduler_loop(self):
        """Main scheduler loop - checks every minute"""
        while self.running:
            try:
                await self._check_and_run_campaigns()
                # Sleep for 60 seconds between checks
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60)  # Continue even if there's an error
                
    async def _check_and_run_campaigns(self):
        """Check all active campaigns and run those that are due"""
        try:
            conn = self.db._get_conn()
            cur = conn.cursor()
            
            # Get all active campaigns with scheduling
            cur.execute("""
                SELECT id, campaign_name, schedule_type, last_sent, created_at, user_id
                FROM auto_ads_campaigns
                WHERE is_active = TRUE
                AND schedule_type IN ('hourly', 'daily', 'weekly')
            """)
            
            campaigns = cur.fetchall()
            
            for campaign in campaigns:
                campaign_id = campaign['id']
                campaign_name = campaign['campaign_name']
                schedule_type = campaign['schedule_type']
                last_sent = campaign['last_sent']
                created_at = campaign['created_at']
                user_id = campaign['user_id']
                
                # Determine if campaign should run
                should_run = False
                now = datetime.now()
                
                if last_sent is None:
                    # Never run before, use created_at as reference
                    last_sent = created_at
                
                if schedule_type == 'hourly':
                    # Run if more than 1 hour since last send
                    if now >= last_sent + timedelta(hours=1):
                        should_run = True
                        
                elif schedule_type == 'daily':
                    # Run if more than 24 hours since last send
                    if now >= last_sent + timedelta(days=1):
                        should_run = True
                        
                elif schedule_type == 'weekly':
                    # Run if more than 7 days since last send
                    if now >= last_sent + timedelta(weeks=1):
                        should_run = True
                
                if should_run:
                    logger.info(f"⏰ Scheduler: Running campaign '{campaign_name}' (ID: {campaign_id}) - {schedule_type}")
                    try:
                        results = await self.service.execute_campaign(campaign_id)
                        if results['success']:
                            logger.info(f"✅ Scheduler: Campaign '{campaign_name}' executed - Sent: {results['sent_count']}, Failed: {results['failed_count']}")
                        else:
                            logger.error(f"❌ Scheduler: Campaign '{campaign_name}' failed - {results.get('message', 'Unknown error')}")
                    except Exception as e:
                        logger.error(f"❌ Scheduler: Error executing campaign '{campaign_name}': {e}")
            
            cur.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error checking campaigns: {e}")

# Global scheduler instance
auto_ads_scheduler = None

def get_scheduler(bot_instance=None):
    """Get or create the global scheduler instance"""
    global auto_ads_scheduler
    if auto_ads_scheduler is None:
        auto_ads_scheduler = AutoAdsScheduler(bot_instance)
    return auto_ads_scheduler

