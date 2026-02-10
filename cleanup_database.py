#!/usr/bin/env python3
"""
Database Cleanup Script
Run this to see and optionally delete all cities and districts from your database.
"""
import os
import sys
from utils import get_db_connection, load_all_data, logger

def view_database_locations():
    """View all cities and districts currently in the database"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        print("\n" + "="*60)
        print("🏙️  CURRENT DATABASE LOCATIONS")
        print("="*60)
        
        # Get all cities
        c.execute("SELECT id, name FROM cities ORDER BY id")
        cities = c.fetchall()
        
        if not cities:
            print("\n✅ No cities found in database - already clean!")
            conn.close()
            return []
        
        print(f"\n📍 Found {len(cities)} cities:\n")
        for city in cities:
            city_id = city['id']
            city_name = city['name']
            
            # Get districts for this city
            c.execute("SELECT id, name FROM districts WHERE city_id = %s ORDER BY id", (city_id,))
            districts = c.fetchall()
            
            # Get product count for this city
            c.execute("SELECT COUNT(*) as count FROM products WHERE city = %s", (city_name,))
            product_count = c.fetchone()['count']
            
            print(f"  {city_id}. {city_name}")
            print(f"     Districts: {len(districts)}")
            print(f"     Products: {product_count}")
            
            if districts:
                for dist in districts:
                    # Get product count for this district
                    c.execute("SELECT COUNT(*) as count FROM products WHERE city = %s AND district = %s", 
                             (city_name, dist['name']))
                    dist_products = c.fetchone()['count']
                    print(f"       - {dist['name']} ({dist_products} products)")
            print()
        
        conn.close()
        return cities
        
    except Exception as e:
        logger.error(f"Error viewing database: {e}")
        print(f"\n❌ Error: {e}")
        return []

def delete_all_cities():
    """Delete ALL cities, districts, and their products"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        print("\n⚠️  DELETING ALL CITIES...")
        
        c.execute("BEGIN")
        
        # Delete all product media
        c.execute("DELETE FROM product_media")
        print("✅ Deleted all product media")
        
        # Delete all products
        c.execute("DELETE FROM products")
        products_deleted = c.rowcount
        print(f"✅ Deleted {products_deleted} products")
        
        # Delete all districts
        c.execute("DELETE FROM districts")
        districts_deleted = c.rowcount
        print(f"✅ Deleted {districts_deleted} districts")
        
        # Delete all cities
        c.execute("DELETE FROM cities")
        cities_deleted = c.rowcount
        print(f"✅ Deleted {cities_deleted} cities")
        
        conn.commit()
        conn.close()
        
        print("\n✅ Database cleaned successfully!")
        print("   You can now add your real cities through the admin panel.")
        
        # Reload data
        load_all_data()
        
    except Exception as e:
        logger.error(f"Error cleaning database: {e}")
        print(f"\n❌ Error: {e}")
        if conn:
            conn.rollback()
            conn.close()

def delete_specific_city(city_id):
    """Delete a specific city by ID"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get city name first
        c.execute("SELECT name FROM cities WHERE id = %s", (city_id,))
        result = c.fetchone()
        if not result:
            print(f"\n❌ City with ID {city_id} not found!")
            conn.close()
            return
        
        city_name = result['name']
        
        print(f"\n⚠️  DELETING CITY: {city_name} (ID: {city_id})...")
        
        c.execute("BEGIN")
        
        # Get product IDs for this city
        c.execute("SELECT id FROM products WHERE city = %s", (city_name,))
        product_ids = [row['id'] for row in c.fetchall()]
        
        if product_ids:
            # Delete product media
            placeholders = ','.join(['%s'] * len(product_ids))
            c.execute(f"DELETE FROM product_media WHERE product_id IN ({placeholders})", product_ids)
            print(f"✅ Deleted media for {len(product_ids)} products")
        
        # Delete products
        c.execute("DELETE FROM products WHERE city = %s", (city_name,))
        products_deleted = c.rowcount
        print(f"✅ Deleted {products_deleted} products")
        
        # Delete districts
        c.execute("DELETE FROM districts WHERE city_id = %s", (city_id,))
        districts_deleted = c.rowcount
        print(f"✅ Deleted {districts_deleted} districts")
        
        # Delete city
        c.execute("DELETE FROM cities WHERE id = %s", (city_id,))
        print(f"✅ Deleted city: {city_name}")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ City '{city_name}' deleted successfully!")
        
        # Reload data
        load_all_data()
        
    except Exception as e:
        logger.error(f"Error deleting city: {e}")
        print(f"\n❌ Error: {e}")
        if conn:
            conn.rollback()
            conn.close()

def main():
    print("\n" + "="*60)
    print("🧹  DATABASE CLEANUP TOOL")
    print("="*60)
    
    # View current database
    cities = view_database_locations()
    
    if not cities:
        return
    
    print("\n" + "="*60)
    print("OPTIONS:")
    print("="*60)
    print("  1. Delete ALL cities and start fresh")
    print("  2. Delete a specific city by ID")
    print("  3. Exit (no changes)")
    print()
    
    choice = input("Enter your choice (1-3): ").strip()
    
    if choice == "1":
        print("\n⚠️  WARNING: This will delete ALL cities, districts, and products!")
        confirm = input("Type 'DELETE ALL' to confirm: ").strip()
        if confirm == "DELETE ALL":
            delete_all_cities()
        else:
            print("\n❌ Cancelled - no changes made")
    
    elif choice == "2":
        city_id = input("\nEnter city ID to delete: ").strip()
        try:
            city_id = int(city_id)
            print(f"\n⚠️  WARNING: This will delete city {city_id} and all its districts and products!")
            confirm = input(f"Type 'DELETE {city_id}' to confirm: ").strip()
            if confirm == f"DELETE {city_id}":
                delete_specific_city(city_id)
            else:
                print("\n❌ Cancelled - no changes made")
        except ValueError:
            print("\n❌ Invalid city ID")
    
    elif choice == "3":
        print("\n✅ No changes made - exiting")
    
    else:
        print("\n❌ Invalid choice")

if __name__ == "__main__":
    main()

