"""
Userbot Load Balancer
Distributes delivery tasks across multiple userbots using different strategies
"""

import logging
import random
from typing import Optional, Dict, Any, List
from userbot_database import get_available_userbots, get_global_settings

logger = logging.getLogger(__name__)


class UserbotLoadBalancer:
    """Manages load balancing across multiple userbots"""
    
    def __init__(self):
        self.last_used_index = -1
        self.strategy = 'round_robin'
    
    async def get_next_userbot(self) -> Optional[Dict[str, Any]]:
        """
        Get next available userbot based on load balancing strategy
        Returns userbot dict or None if no userbots available
        """
        try:
            # Get global settings
            settings = get_global_settings()
            if not settings.get('enabled', True):
                logger.info("ℹ️ Userbot system is globally disabled")
                return None
            
            self.strategy = settings.get('load_balancing_strategy', 'round_robin')
            
            # Get available userbots
            userbots = get_available_userbots()
            
            if not userbots:
                logger.warning("⚠️ No available userbots for delivery!")
                return None
            
            # Apply strategy
            if self.strategy == 'round_robin':
                return self._round_robin(userbots)
            elif self.strategy == 'priority':
                return self._priority_based(userbots)
            elif self.strategy == 'least_loaded':
                return self._least_loaded(userbots)
            elif self.strategy == 'random':
                return self._random(userbots)
            else:
                logger.warning(f"⚠️ Unknown strategy '{self.strategy}', using round_robin")
                return self._round_robin(userbots)
                
        except Exception as e:
            logger.error(f"❌ Error in load balancer: {e}", exc_info=True)
            return None
    
    def _round_robin(self, userbots: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Round robin: Cycle through all userbots"""
        self.last_used_index = (self.last_used_index + 1) % len(userbots)
        selected = userbots[self.last_used_index]
        logger.info(f"🔄 Round Robin: Selected userbot {selected['id']} ({selected['name']})")
        return selected
    
    def _priority_based(self, userbots: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Priority-based: Use highest priority userbot that's available"""
        # Sort by priority descending, then by least loaded
        userbots.sort(
            key=lambda u: (
                -u.get('priority', 0),
                u.get('deliveries_last_hour', 0)
            )
        )
        selected = userbots[0]
        logger.info(f"⭐ Priority: Selected userbot {selected['id']} ({selected['name']}) with priority {selected.get('priority', 0)}")
        return selected
    
    def _least_loaded(self, userbots: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Least loaded: Select userbot with fewest deliveries in last hour"""
        selected = min(userbots, key=lambda u: u.get('deliveries_last_hour', 0))
        logger.info(f"📊 Least Loaded: Selected userbot {selected['id']} ({selected['name']}) with {selected.get('deliveries_last_hour', 0)} deliveries")
        return selected
    
    def _random(self, userbots: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Random: Randomly select from available userbots"""
        selected = random.choice(userbots)
        logger.info(f"🎲 Random: Selected userbot {selected['id']} ({selected['name']})")
        return selected
    
    def reset_index(self):
        """Reset round robin index (useful after userbot list changes)"""
        self.last_used_index = -1


# Global load balancer instance
load_balancer = UserbotLoadBalancer()


async def select_userbot_for_delivery() -> Optional[Dict[str, Any]]:
    """
    Main function to select a userbot for delivery
    Use this in payment.py and other places
    """
    return await load_balancer.get_next_userbot()

