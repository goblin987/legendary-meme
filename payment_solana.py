import logging
import json
import time
import asyncio
import requests
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from solana.rpc.api import Client
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction
from solders.message import Message
from utils import get_db_connection, send_message_with_retry, format_currency

# --- CONFIGURATION ---
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
ADMIN_WALLET = os.getenv("SOLANA_ADMIN_WALLET") # Must be set in Render
ENABLE_AUTO_SWEEP = True # Automatically send funds to admin wallet after payment

logger = logging.getLogger(__name__)
client = Client(SOLANA_RPC_URL)

# ===== PRODUCTION-GRADE PRICE CACHING SYSTEM =====
_price_cache = {'price': None, 'timestamp': 0, 'last_api_used': None}
PRICE_CACHE_TTL = 300  # 5 minutes cache (production-grade)
STALE_CACHE_MAX_AGE = 3600  # Accept stale cache up to 1 hour if all APIs fail

def get_sol_price_from_db():
    """Get cached price from database (survives restarts)"""
    try:
        from utils import get_db_connection
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT setting_value, updated_at 
            FROM bot_settings 
            WHERE setting_key = 'sol_price_eur_cache'
        """)
        result = c.fetchone()
        conn.close()
        
        if result:
            import time
            price = Decimal(str(result['setting_value']))
            # Check if cache is fresh (< 10 minutes)
            cache_age = time.time() - result['updated_at'].timestamp()
            if cache_age < 600:  # 10 minutes
                logger.info(f"📊 DB cached SOL price: {price} EUR (age: {int(cache_age)}s)")
                return price
    except Exception as e:
        logger.debug(f"Could not fetch DB price cache: {e}")
    return None

def save_sol_price_to_db(price):
    """Save price to database for persistence"""
    try:
        from utils import get_db_connection
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            INSERT INTO bot_settings (setting_key, setting_value, updated_at)
            VALUES ('sol_price_eur_cache', %s, NOW())
            ON CONFLICT (setting_key) DO UPDATE 
            SET setting_value = EXCLUDED.setting_value, updated_at = NOW()
        """, (str(price),))
        conn.commit()
        conn.close()
        logger.debug(f"💾 Saved SOL price to DB: {price} EUR")
    except Exception as e:
        logger.debug(f"Could not save price to DB: {e}")

def fetch_price_from_api(api_name, url, parser_func):
    """Generic API fetcher with timeout and error handling"""
    try:
        response = requests.get(url, timeout=3)  # Faster timeout
        if response.status_code == 200:
            price = parser_func(response.json())
            if price:
                logger.info(f"✅ {api_name} SOL price: {price} EUR")
                return price
        elif response.status_code == 429:
            logger.warning(f"⚠️ {api_name} rate limited (429)")
        else:
            logger.warning(f"⚠️ {api_name} returned status {response.status_code}")
    except requests.Timeout:
        logger.warning(f"⏱️ {api_name} timeout")
    except Exception as e:
        logger.debug(f"{api_name} error: {e}")
    return None

def get_sol_price_eur():
    """
    PRODUCTION-GRADE: Multi-layer caching + smart API rotation
    
    Strategy:
    1. Check memory cache (instant, 5 min TTL)
    2. Check DB cache (fast, 10 min TTL)
    3. Try APIs in rotation (avoid hammering one)
    4. Use stale cache up to 1 hour (last resort)
    """
    import time
    now = time.time()
    
    # Layer 1: Memory cache (instant)
    if _price_cache['price'] and (now - _price_cache['timestamp']) < PRICE_CACHE_TTL:
        cache_age = int(now - _price_cache['timestamp'])
        logger.info(f"💰 Memory cached SOL price: {_price_cache['price']} EUR (age: {cache_age}s)")
        return _price_cache['price']
    
    # Layer 2: Database cache (survives restarts)
    db_price = get_sol_price_from_db()
    if db_price:
        _price_cache['price'] = db_price
        _price_cache['timestamp'] = now
        return db_price
    
    # Layer 3: Fetch from APIs (smart rotation to avoid rate limits)
    apis = [
        ('Binance', 'https://api.binance.com/api/v3/ticker/price?symbol=SOLEUR', 
         lambda data: Decimal(str(data['price'])) if 'price' in data else None),
        
        ('CryptoCompare', 'https://min-api.cryptocompare.com/data/price?fsym=SOL&tsyms=EUR',
         lambda data: Decimal(str(data['EUR'])) if 'EUR' in data else None),
        
        ('CoinGecko', 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=eur',
         lambda data: Decimal(str(data['solana']['eur'])) if 'solana' in data and 'eur' in data['solana'] else None),
    ]
    
    # Start with API that was NOT used last time (rotation)
    last_used = _price_cache.get('last_api_used')
    start_idx = ((last_used + 1) % len(apis)) if (last_used is not None) else 0
    
    # Try all APIs in rotated order
    for i in range(len(apis)):
        idx = (start_idx + i) % len(apis)
        api_name, url, parser = apis[idx]
        
        price = fetch_price_from_api(api_name, url, parser)
        
        if price:
            # Success! Update all caches
            _price_cache['price'] = price
            _price_cache['timestamp'] = now
            _price_cache['last_api_used'] = idx
            save_sol_price_to_db(price)
            return price
    
    # Layer 4: Stale cache (up to 1 hour old - better than failing)
    if _price_cache['price']:
        age = int(now - _price_cache['timestamp'])
        if age < STALE_CACHE_MAX_AGE:
            logger.warning(f"⚠️ All APIs failed, using stale cache ({age}s old): {_price_cache['price']} EUR")
            return _price_cache['price']
        else:
            logger.error(f"❌ Stale cache too old ({age}s), cannot use")
    
    logger.error(f"❌ CRITICAL: All price sources failed!")
    return None

async def refresh_price_cache(context=None):
    """
    Background job: Proactively refresh price cache every 4 minutes
    This prevents rate limiting during high traffic
    
    Args:
        context: Telegram context (required by job queue, but not used)
    """
    logger.info("🔄 Background price refresh triggered")
    
    # Force cache refresh by temporarily invalidating it
    old_timestamp = _price_cache['timestamp']
    _price_cache['timestamp'] = 0
    
    price = get_sol_price_eur()
    
    if price:
        logger.info(f"✅ Background refresh successful: {price} EUR")
    else:
        logger.warning(f"⚠️ Background refresh failed, restoring old cache")
        _price_cache['timestamp'] = old_timestamp

async def create_solana_payment(user_id, order_id, eur_amount):
    """
    Generates a unique SOL wallet for this transaction.
    Returns: dict with address, amount, and qr_code data
    """
    price = get_sol_price_eur()
    if not price:
        logger.error("Could not fetch SOL price, using fallback or failing.")
        return {'error': 'estimate_failed'}

    # Calculate SOL amount (add small buffer or just exact)
    # Quantize to 5 decimal places for easier reading/typing (approx 0.001 EUR precision)
    sol_amount = (Decimal(eur_amount) / price).quantize(Decimal("0.00001"))
    
    # Generate new Keypair
    kp = Keypair()
    pubkey = str(kp.pubkey())
    # Store private key as list of integers for storage
    private_key_json = json.dumps(list(bytes(kp)))

    conn = get_db_connection()
    c = conn.cursor()
    try:
        # Check if order_id already exists (retry case)
        c.execute("SELECT public_key, expected_amount FROM solana_wallets WHERE order_id = %s", (order_id,))
        existing = c.fetchone()
        
        if existing:
            logger.info(f"Found existing Solana wallet for order {order_id}")
            return {
                'pay_address': existing['public_key'],
                'pay_amount': str(existing['expected_amount']),
                'pay_currency': 'SOL',
                'exchange_rate': float(price),
                'payment_id': order_id # Use order_id as payment_id
            }

        c.execute("""
            INSERT INTO solana_wallets (user_id, order_id, public_key, private_key, expected_amount, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
        """, (user_id, order_id, pubkey, private_key_json, float(sol_amount)))
        conn.commit()
    except Exception as e:
        logger.error(f"DB Error creating solana payment: {e}")
        return {'error': 'internal_server_error'}
    finally:
        conn.close()

    return {
        'pay_address': pubkey,
        'pay_amount': str(sol_amount),
        'pay_currency': 'SOL',
        'exchange_rate': float(price),
        'payment_id': order_id
    }

async def check_balance_only_payments(context):
    """
    Check for balance-only payments (status='paid', pay_address='balance_payment')
    These bypass Solana and need immediate processing.
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Find balance-only payments marked as 'paid' but not yet finalized
        c.execute("""
            SELECT payment_id, user_id, basket_snapshot_json as basket_snapshot
            FROM pending_deposits
            WHERE status = 'paid' 
            AND pay_address = 'balance_payment'
            AND is_purchase = TRUE
        """)
        balance_payments = c.fetchall()
        
        if not balance_payments:
            return
        
        logger.info(f"🔍 Found {len(balance_payments)} balance-only payments to process")
        
        for payment in balance_payments:
            order_id = payment['payment_id']
            user_id = payment['user_id']
            
            try:
                logger.info(f"💰 Processing balance-only payment {order_id} for user {user_id}")
                
                # Parse basket snapshot
                basket_snapshot = payment.get('basket_snapshot')
                if isinstance(basket_snapshot, str):
                    try:
                        basket_snapshot = json.loads(basket_snapshot)
                    except:
                        logger.error(f"Failed to parse basket snapshot for {order_id}")
                        continue
                
                # Extract items
                items = basket_snapshot
                if isinstance(basket_snapshot, dict) and 'items' in basket_snapshot:
                    items = basket_snapshot['items']
                
                # Normalize item format
                if isinstance(items, list):
                    for item in items:
                        if 'id' in item and 'product_id' not in item:
                            item['product_id'] = item['id']
                        if 'type' in item and 'product_type' not in item:
                            item['product_type'] = item['type']
                        # Extract size from name if missing (common pattern "Name - Size")
                        if 'size' not in item or not item['size']:
                            if ' - ' in item.get('name', ''):
                                item['size'] = item['name'].split(' - ')[-1]
                            else:
                                item['size'] = ''
                        # Ensure other fields exist
                        if 'city' not in item: item['city'] = 'Unknown'
                        if 'district' not in item: item['district'] = ''
                        if 'name' not in item: item['name'] = 'Product'
                        if 'price' not in item: item['price'] = 0.0
                
                # Call finalize_purchase from payment.py
                from payment import _finalize_purchase
                
                # Balance was already deducted in main.py when creating the invoice
                # Correct signature: _finalize_purchase(user_id, basket_snapshot, discount_code_used, context)
                await _finalize_purchase(
                    user_id, 
                    items,  # This is the basket_snapshot (list of items)
                    None,   # discount_code_used
                    context
                )
                
                # Update status to 'confirmed' to prevent reprocessing
                c.execute("UPDATE pending_deposits SET status = 'confirmed' WHERE payment_id = %s", (order_id,))
                conn.commit()
                
                logger.info(f"✅ Balance-only payment {order_id} finalized successfully")
                
            except Exception as process_err:
                logger.error(f"Error processing balance-only payment {order_id}: {process_err}", exc_info=True)
                # Don't mark as confirmed so it can retry
                
    except Exception as e:
        logger.error(f"Error in check_balance_only_payments: {e}", exc_info=True)
    finally:
        conn.close()

async def check_solana_deposits(context):
    """
    Background task to check all pending wallets for deposits.
    Call this periodically (e.g., every 30-60 seconds).
    """
    # FIRST: Check for balance-only payments (status='paid' in pending_deposits, no Solana wallet)
    await check_balance_only_payments(context)
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Get pending wallets
        # Assuming RealDictCursor from utils
        c.execute("SELECT * FROM solana_wallets WHERE status = 'pending'")
        pending = c.fetchall()
        
        if not pending:
            return

        for wallet in pending:
            try:
                pubkey_str = wallet['public_key']
                expected = Decimal(str(wallet['expected_amount']))
                wallet_id = wallet['id']
                order_id = wallet['order_id']
                user_id = wallet['user_id']
                created_at = wallet['created_at']
                if created_at.tzinfo is None: created_at = created_at.replace(tzinfo=timezone.utc)
                
                # Rate limit RPC calls
                await asyncio.sleep(0.2)
                
                # Check Balance (RPC call)
                # Note: get_balance returns lamports (1 SOL = 10^9 lamports)
                try:
                    balance_resp = client.get_balance(Pubkey.from_string(pubkey_str))
                    lamports = balance_resp.value
                    sol_balance = Decimal(lamports) / Decimal(10**9)
                except Exception as rpc_e:
                    logger.warning(f"RPC Error checking wallet {pubkey_str}: {rpc_e}")
                    continue
                
                # 1. Check if Paid (allowing very small tolerance, e.g. 99.5%)
                if sol_balance > 0 and sol_balance >= (expected * Decimal("0.99")):
                    logger.info(f"✅ Payment detected for Order {order_id}: {sol_balance} SOL")
                    
                    # Mark as Paid in DB first
                    c.execute("UPDATE solana_wallets SET status = 'paid', amount_received = %s, updated_at = NOW() WHERE id = %s", (float(sol_balance), wallet_id))
                    conn.commit()
                    
                    # Handle Overpayment (Surplus > 0.0005 SOL ~ 0.10 EUR)
                    surplus = sol_balance - expected
                    if surplus > Decimal("0.0005"):
                        try:
                            price = get_sol_price_eur()
                            if price:
                                surplus_eur = (surplus * price).quantize(Decimal("0.01"))
                                if surplus_eur > 0:
                                    logger.info(f"💰 Overpayment of {surplus} SOL ({surplus_eur} EUR) detected for {order_id}. Crediting user.")
                                    from payment import credit_user_balance
                                    await credit_user_balance(user_id, surplus_eur, f"Overpayment bonus for order {order_id}", context)
                        except Exception as over_e:
                            logger.error(f"Error processing overpayment: {over_e}")
                    
                    # 2. Trigger Payment Success Logic
                    # Import here to avoid circular dependency
                    from payment import process_successful_crypto_purchase, process_successful_refill
                    
                    # Determine if it's a purchase or refill based on order_id prefix
                    # If order_id isn't descriptive, we need to look up pending_deposits table using order_id as payment_id
                    
                    c.execute("SELECT is_purchase, basket_snapshot_json as basket_snapshot, discount_code_used as discount_code FROM pending_deposits WHERE payment_id = %s", (order_id,))
                    deposit_info = c.fetchone()
                    
                    if deposit_info:
                        is_purchase = deposit_info['is_purchase']
                        
                        if is_purchase:
                            # Reconstruct basket snapshot if stored as JSON string
                            basket_snapshot = deposit_info.get('basket_snapshot')
                            if isinstance(basket_snapshot, str):
                                try: 
                                    basket_snapshot = json.loads(basket_snapshot)
                                except: 
                                    pass
                            
                            # Extract balance_used if present (new format)
                            balance_to_deduct = Decimal('0.0')
                            if isinstance(basket_snapshot, dict):
                                balance_to_deduct = Decimal(str(basket_snapshot.get('balance_used', 0)))
                            
                            # Handle new format: basket_snapshot is a dict with 'items' key
                            if isinstance(basket_snapshot, dict) and 'items' in basket_snapshot:
                                basket_snapshot = basket_snapshot['items']
                            
                            # Normalize item format: Mini App uses different keys than payment code expects
                            if isinstance(basket_snapshot, list):
                                for item in basket_snapshot:
                                    # Map: id -> product_id
                                    if 'id' in item and 'product_id' not in item:
                                        item['product_id'] = item['id']
                                    
                                    # Map: type -> product_type
                                    if 'type' in item and 'product_type' not in item:
                                        item['product_type'] = item['type']
                                    
                                    # Add missing fields with defaults
                                    if 'size' not in item:
                                        # Try to extract size from name (e.g., "alus 2g" -> "2g")
                                        name = item.get('name', '')
                                        parts = name.split()
                                        item['size'] = parts[-1] if parts else 'N/A'
                                    
                                    if 'original_text' not in item:
                                        # Generate original_text from available data
                                        item['original_text'] = f"{item.get('name', 'Product')} | {item.get('city', 'City')} | {item.get('district', 'District')}"
                            
                            # Deduct balance if specified
                            if balance_to_deduct > 0:
                                try:
                                    # Round to 2 decimal places to avoid floating point precision issues
                                    balance_to_deduct_rounded = round(float(balance_to_deduct), 2)
                                    logger.info(f"💰 Deducting {balance_to_deduct_rounded} EUR from user {user_id} balance (auto-applied)")
                                    
                                    # PostgreSQL needs explicit NUMERIC cast for ROUND function
                                    c.execute("""
                                        UPDATE users 
                                        SET balance = ROUND((balance - %s)::numeric, 2) 
                                        WHERE user_id = %s AND balance >= %s
                                    """, (balance_to_deduct_rounded, user_id, balance_to_deduct_rounded - 0.01))
                                    
                                    if c.rowcount > 0:
                                        conn.commit()
                                        logger.info(f"✅ Successfully deducted {balance_to_deduct_rounded} EUR balance for user {user_id}")
                                    else:
                                        # Check actual balance for debugging
                                        c.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
                                        result = c.fetchone()
                                        actual_balance = result[0] if result else 0
                                        logger.warning(f"⚠️ Could not deduct balance for user {user_id} - actual balance: {actual_balance}, needed: {balance_to_deduct_rounded}")
                                except Exception as balance_err:
                                    logger.error(f"Error deducting balance for user {user_id}: {balance_err}")
                                
                            discount_code = deposit_info.get('discount_code')
                            
                            await process_successful_crypto_purchase(user_id, basket_snapshot, discount_code, order_id, context)
                            
                            # CRITICAL: Update pending_deposits status to 'confirmed' to prevent reprocessing
                            c.execute("UPDATE pending_deposits SET status = 'confirmed' WHERE payment_id = %s", (order_id,))
                            conn.commit()
                            logger.info(f"✅ Purchase {order_id}: Updated pending_deposits status to 'confirmed'")
                        else:
                            # Refill
                            # We need to calculate EUR amount. Use the stored rate or current rate?
                            # Ideally we stored the target EUR amount in pending_deposits
                            c.execute("SELECT target_eur_amount FROM pending_deposits WHERE payment_id = %s", (order_id,))
                            amount_res = c.fetchone()
                            amount_eur = Decimal(str(amount_res['target_eur_amount'])) if amount_res else Decimal("0.0")
                            
                            # Process refill WITHOUT sending bot message (Mini App will notify user)
                            await process_successful_refill(user_id, amount_eur, order_id, context, send_notification=False)
                            
                            # CRITICAL: Update pending_deposits status to 'paid' so Mini App polling detects it
                            c.execute("UPDATE pending_deposits SET status = 'paid' WHERE payment_id = %s", (order_id,))
                            conn.commit()
                            logger.info(f"✅ Refill {order_id}: Updated pending_deposits status to 'paid' for Mini App")
                    else:
                        logger.error(f"Could not find pending_deposit record for solana order {order_id}")
                    
                    # 3. Sweep Funds (Optional but recommended)
                    if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
                        # Run sweep in background
                        asyncio.create_task(sweep_wallet(wallet, lamports))
                
                # 2. Check for Underpayment (Partial amount received) - IMMEDIATE
                elif sol_balance > 0:
                    logger.info(f"📉 Underpayment detected for {order_id} ({sol_balance} SOL). Refunding immediately.")
                    try:
                        price = get_sol_price_eur()
                        if price:
                            refund_eur = (sol_balance * price).quantize(Decimal("0.01"))
                            if refund_eur > 0:
                                from payment import credit_user_balance
                                # Minimalistic message as requested
                                msg = f"⚠️ Underpayment detected ({sol_balance} SOL). Refunded {refund_eur} EUR to balance. Please use Top Up."
                                await send_message_with_retry(context.bot, user_id, msg, parse_mode=None)
                                await credit_user_balance(user_id, refund_eur, f"Underpayment refund {order_id}", context)
                                
                                # Mark as refunded (cancelled)
                                c.execute("UPDATE solana_wallets SET status = 'refunded', amount_received = %s, updated_at = NOW() WHERE id = %s", (float(sol_balance), wallet_id))
                                conn.commit()
                                
                                # Sweep the partial funds
                                if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
                                    asyncio.create_task(sweep_wallet(wallet, lamports))
                    except Exception as refund_e:
                        logger.error(f"Error refunding underpayment {order_id}: {refund_e}")

                # 3. Check for Expiration (Empty) - 20 minutes
                elif datetime.now(timezone.utc) - created_at > timedelta(minutes=20):
                    # Expired and empty
                    c.execute("UPDATE solana_wallets SET status = 'expired', updated_at = NOW() WHERE id = %s", (wallet_id,))
                    conn.commit()
                        
            except Exception as e:
                logger.error(f"Error checking wallet {wallet.get('public_key')}: {e}", exc_info=True)
                
    except Exception as e:
        logger.error(f"Error in check_solana_deposits loop: {e}", exc_info=True)
    finally:
        conn.close()
        
    # RECOVERY: Check for 'paid' wallets that haven't been marked 'swept' (e.g. due to crash)
    # Only run recovery if no active sweep tasks are pending (wait 5 seconds after main loop)
    if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
        try:
            await asyncio.sleep(2)  # Wait for concurrent sweeps to complete
            conn = get_db_connection()
            c = conn.cursor()
            # Only get wallets that are STILL 'paid' after the main sweep should have completed
            c.execute("SELECT * FROM solana_wallets WHERE status = 'paid' AND updated_at < NOW() - INTERVAL '10 seconds'")
            paid_wallets = c.fetchall()
            conn.close()
            
            if paid_wallets:
                logger.info(f"🔄 Recovery sweep: Found {len(paid_wallets)} wallets still marked 'paid'")
                for wallet in paid_wallets:
                    # Attempt sweep (it will check balance first)
                    asyncio.create_task(sweep_wallet(wallet))
        except Exception as e:
            logger.error(f"Error in sweep recovery loop: {e}")

async def sweep_wallet(wallet_data, current_lamports=0):
    """Moves funds from temp wallet to ADMIN_WALLET"""
    wallet_id = wallet_data.get('id', 'unknown')
    wallet_pubkey = wallet_data.get('public_key', 'unknown')
    logger.info(f"🧹 SWEEP START: Wallet {wallet_id} ({wallet_pubkey[:16]}...) - Initial lamports: {current_lamports}")
    
    try:
        # Fetch balance if not provided
        if current_lamports == 0:
            try:
                balance_resp = client.get_balance(Pubkey.from_string(wallet_pubkey))
                current_lamports = balance_resp.value
                logger.info(f"🧹 SWEEP: Fetched balance for wallet {wallet_id}: {current_lamports} lamports ({current_lamports/10**9:.6f} SOL)")
            except Exception as e:
                logger.error(f"🧹 SWEEP ERROR: Error fetching balance for wallet {wallet_id} ({wallet_pubkey}): {e}")
                return

        if current_lamports < 5000: # Ignore dust (less than 0.000005 SOL)
            # If it's 'paid' but empty, maybe it was already swept or emptied?
            # Mark as swept to stop retrying if it's really empty
            if wallet_data.get('status') == 'paid' and current_lamports < 5000:
                 conn = get_db_connection()
                 conn.cursor().execute("UPDATE solana_wallets SET status = 'swept' WHERE id = %s", (wallet_data['id'],))
                 conn.commit()
                 conn.close()
            return

        # Load Keypair
        priv_key_list = json.loads(wallet_data['private_key'])
        kp = Keypair.from_bytes(bytes(priv_key_list))
        
        # Calculate fee (simple transfer is usually 5000 lamports)
        fee = 5000
        amount_to_send = current_lamports - fee
        
        if amount_to_send <= 0:
            return

        logger.info(f"🧹 Sweeping {amount_to_send} lamports from {wallet_data['public_key']} to {ADMIN_WALLET}...")

        # Create Transaction using solders
        ix = transfer(
            TransferParams(
                from_pubkey=kp.pubkey(),
                to_pubkey=Pubkey.from_string(ADMIN_WALLET),
                lamports=int(amount_to_send)
            )
        )
        
        # Get blockhash
        latest_blockhash = client.get_latest_blockhash().value.blockhash
        
        # Construct message and sign transaction (solders >= 0.18 API)
        msg = Message.new_with_blockhash(
            [ix],
            kp.pubkey(),
            latest_blockhash
        )
        
        # Sign the message
        sig = kp.sign_message(bytes(msg))
        
        # Create the signed transaction
        transaction = Transaction.populate(msg, [sig])
        
        # Send
        logger.info(f"🧹 SWEEP: Sending transaction for wallet {wallet_id}...")
        txn_sig = client.send_transaction(transaction)
        
        logger.info(f"✅ SWEEP SUCCESS: Wallet {wallet_id} - Swept {amount_to_send} lamports ({amount_to_send/10**9:.6f} SOL). Sig: {txn_sig.value}")
        
        # Update DB
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE solana_wallets SET status = 'swept' WHERE id = %s", (wallet_id,))
        conn.commit()
        conn.close()
        logger.info(f"✅ SWEEP DB: Wallet {wallet_id} marked as 'swept' in database")
        
    except Exception as e:
        logger.error(f"❌ SWEEP FAILED: Wallet {wallet_id} ({wallet_pubkey}): {e}", exc_info=True)
        # Don't mark as swept if failed - it will be retried

