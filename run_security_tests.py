#!/usr/bin/env python3
"""
CRITICAL SECURITY TEST SUITE
Tests payment system, delivery logic, and anti-abuse mechanisms.
"""

import asyncio
import sys
from datetime import datetime, timezone
from decimal import Decimal

# Import critical functions
try:
    from utils import get_db_connection
    from payment import _finalize_purchase, process_successful_crypto_purchase
    from product_delivery import deliver_products_to_user
except ImportError as e:
    print(f"❌ Import Error: {e}")
    print("   Run this from the project root directory.")
    sys.exit(1)

class SecurityTester:
    def __init__(self):
        self.results = {
            "passed": [],
            "failed": [],
            "warnings": []
        }
    
    def log_test(self, test_name, passed, details=""):
        if passed:
            self.results["passed"].append(f"✅ {test_name}")
            print(f"✅ PASS: {test_name}")
        else:
            self.results["failed"].append(f"❌ {test_name}: {details}")
            print(f"❌ FAIL: {test_name}")
            print(f"   Details: {details}")
    
    def log_warning(self, warning):
        self.results["warnings"].append(f"⚠️  {warning}")
        print(f"⚠️  WARNING: {warning}")
    
    # ===== TEST 1: Stock Validation =====
    def test_stock_validation(self):
        """Verify that purchases cannot exceed available stock."""
        print("\n" + "="*60)
        print("TEST 1: Stock Validation")
        print("="*60)
        
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            # Get a product with limited stock
            c.execute("SELECT id, available FROM products WHERE available > 0 AND available < 5 LIMIT 1")
            product = c.fetchone()
            
            if not product:
                self.log_warning("No products with limited stock found for testing")
                return
            
            product_id = product['id']
            available = product['available']
            
            # Test 1.1: Can we reserve more than available?
            c.execute("""
                UPDATE products 
                SET reserved = available + 1 
                WHERE id = %s
                RETURNING available, reserved
            """, (product_id,))
            result = c.fetchone()
            conn.rollback()
            
            # This should have been blocked by application logic (not DB constraint)
            # But we manually set it to test the delivery logic
            if result and result['reserved'] > result['available']:
                self.log_test(
                    "Stock Overflow Prevention", 
                    False,
                    f"Reserved ({result['reserved']}) > Available ({result['available']})"
                )
            else:
                self.log_test("Stock Overflow Prevention", True)
            
            conn.close()
            
        except Exception as e:
            self.log_test("Stock Validation", False, str(e))
    
    # ===== TEST 2: Payment Amount Validation =====
    def test_payment_validation(self):
        """Verify payment amounts are validated correctly."""
        print("\n" + "="*60)
        print("TEST 2: Payment Amount Validation")
        print("="*60)
        
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            # Test 2.1: Can we create a payment with negative amount?
            test_user_id = 999999999 # Test user
            
            try:
                from payment import create_sol_payment
                result = asyncio.run(create_sol_payment(
                    user_id=test_user_id,
                    target_eur_amount=Decimal('-10.00'),
                    is_purchase=False
                ))
                
                # If this succeeds, it's a vulnerability
                if result.get('success'):
                    self.log_test("Negative Amount Prevention", False, "Negative payment accepted")
                else:
                    self.log_test("Negative Amount Prevention", True)
            except Exception as e:
                # Exception is expected for negative amounts
                self.log_test("Negative Amount Prevention", True)
            
            # Test 2.2: Check if balance can go negative
            c.execute("SELECT balance FROM users WHERE user_id = %s", (test_user_id,))
            user = c.fetchone()
            
            if user:
                current_balance = Decimal(str(user['balance']))
                if current_balance < 0:
                    self.log_test("Negative Balance Prevention", False, f"Balance is {current_balance}")
                else:
                    self.log_test("Negative Balance Prevention", True)
            else:
                self.log_warning("Test user not found for balance check")
            
            conn.close()
            
        except Exception as e:
            self.log_test("Payment Validation", False, str(e))
    
    # ===== TEST 3: Reservation Race Condition =====
    def test_reservation_race_condition(self):
        """Test if two users can reserve the same last item."""
        print("\n" + "="*60)
        print("TEST 3: Reservation Race Condition")
        print("="*60)
        
        try:
            conn1 = get_db_connection()
            conn2 = get_db_connection()
            c1 = conn1.cursor()
            c2 = conn2.cursor()
            
            # Find a product with exactly 1 available
            c1.execute("SELECT id, available FROM products WHERE available = 1 LIMIT 1")
            product = c1.fetchone()
            
            if not product:
                self.log_warning("No products with stock=1 for race condition test")
                conn1.close()
                conn2.close()
                return
            
            product_id = product['id']
            
            # Simulate two users trying to reserve simultaneously
            # User 1 starts transaction
            c1.execute("BEGIN")
            c1.execute("""
                UPDATE products 
                SET reserved_by = 111111, reserved_until = NOW() + INTERVAL '15 minutes'
                WHERE id = %s AND (reserved_until IS NULL OR reserved_until < NOW())
                RETURNING id
            """, (product_id,))
            result1 = c1.fetchone()
            
            # User 2 tries to reserve before User 1 commits
            c2.execute("BEGIN")
            c2.execute("""
                UPDATE products 
                SET reserved_by = 222222, reserved_until = NOW() + INTERVAL '15 minutes'
                WHERE id = %s AND (reserved_until IS NULL OR reserved_until < NOW())
                RETURNING id
            """, (product_id,))
            result2 = c2.fetchone()
            
            # Commit both
            conn1.commit()
            conn2.commit()
            
            # Only ONE should succeed
            success_count = (1 if result1 else 0) + (1 if result2 else 0)
            
            if success_count <= 1:
                self.log_test("Reservation Race Condition Prevention", True)
            else:
                self.log_test(
                    "Reservation Race Condition Prevention", 
                    False,
                    f"Both users reserved the same item"
                )
            
            # Cleanup
            c1.execute("UPDATE products SET reserved_by = NULL, reserved_until = NULL WHERE id = %s", (product_id,))
            conn1.commit()
            
            conn1.close()
            conn2.close()
            
        except Exception as e:
            self.log_test("Reservation Race Condition", False, str(e))
    
    # ===== TEST 4: Basket Manipulation =====
    def test_basket_manipulation(self):
        """Test if basket can be manipulated after creation."""
        print("\n" + "="*60)
        print("TEST 4: Basket Manipulation")
        print("="*60)
        
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            test_user_id = 999999998
            
            # Test 4.1: Check if basket has max item limit
            # Try to create a basket with > 10 items
            large_basket = json.dumps([{"id": i, "price": 10} for i in range(15)])
            
            c.execute("UPDATE users SET basket = %s WHERE user_id = %s", (large_basket, test_user_id))
            c.execute("SELECT basket FROM users WHERE user_id = %s", (test_user_id,))
            result = c.fetchone()
            conn.rollback()
            
            if result:
                basket_items = json.loads(result['basket']) if result['basket'] else []
                if len(basket_items) > 10:
                    self.log_test(
                        "Basket Size Limit", 
                        False,
                        f"Basket contains {len(basket_items)} items (max should be 10)"
                    )
                else:
                    self.log_test("Basket Size Limit", True)
            
            conn.close()
            
        except Exception as e:
            self.log_test("Basket Manipulation", False, str(e))
    
    # ===== REPORT =====
    def print_report(self):
        print("\n" + "="*60)
        print("SECURITY AUDIT REPORT")
        print("="*60)
        print(f"Tests Passed: {len(self.results['passed'])}")
        print(f"Tests Failed: {len(self.results['failed'])}")
        print(f"Warnings: {len(self.results['warnings'])}")
        
        if self.results['failed']:
            print("\n🚨 CRITICAL FAILURES:")
            for failure in self.results['failed']:
                print(f"  {failure}")
        
        if self.results['warnings']:
            print("\n⚠️  WARNINGS:")
            for warning in self.results['warnings']:
                print(f"  {warning}")
        
        print("\n" + "="*60)
        if self.results['failed']:
            print("❌ AUDIT FAILED - CRITICAL VULNERABILITIES DETECTED")
        else:
            print("✅ AUDIT PASSED - NO CRITICAL VULNERABILITIES DETECTED")
        print("="*60)

def main():
    print("🔒 STARTING CRITICAL SECURITY AUDIT")
    print("="*60)
    
    tester = SecurityTester()
    
    # Run all tests
    tester.test_stock_validation()
    tester.test_payment_validation()
    tester.test_reservation_race_condition()
    tester.test_basket_manipulation()
    
    # Print final report
    tester.print_report()
    
    # Exit code
    sys.exit(0 if not tester.results['failed'] else 1)

if __name__ == "__main__":
    try:
        import json
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        sys.exit(1)

