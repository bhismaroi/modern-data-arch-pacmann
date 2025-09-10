#!/usr/bin/env python3
"""
Generate dummy sales data for testing the pipeline
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import argparse
import os

def generate_sales_data(num_records=1000, output_file='data/generated_sales.csv'):
    """
    Generate synthetic sales data
    """
    
    # Product categories and details
    products = {
        'Electronics': {
            'Laptop': ['Dell XPS 13', 'MacBook Pro', 'ThinkPad X1', 'HP Spectre', 'ASUS ZenBook', 'Surface Laptop'],
            'Smartphone': ['iPhone 14', 'Samsung Galaxy S23', 'Google Pixel 7', 'OnePlus 11', 'Xiaomi 13', 'Motorola Edge'],
            'Tablet': ['iPad Pro', 'Samsung Tab S8', 'Microsoft Surface', 'Amazon Fire HD', 'Lenovo Tab', 'Huawei MatePad'],
            'Headphones': ['AirPods Pro', 'Sony WH-1000XM5', 'Bose QC45', 'Sennheiser HD600', 'JBL Tune', 'Beats Studio']
        },
        'Clothing': {
            'Shirts': ['Cotton T-Shirt', 'Formal Shirt', 'Polo Shirt', 'Denim Shirt', 'Linen Shirt', 'Flannel Shirt'],
            'Pants': ['Jeans', 'Chinos', 'Formal Trousers', 'Cargo Pants', 'Sweatpants', 'Shorts'],
            'Shoes': ['Running Shoes', 'Formal Shoes', 'Sneakers', 'Boots', 'Sandals', 'Loafers'],
            'Accessories': ['Belt', 'Wallet', 'Watch', 'Sunglasses', 'Hat', 'Scarf']
        },
        'Home & Garden': {
            'Furniture': ['Office Chair', 'Desk', 'Bookshelf', 'Sofa', 'Dining Table', 'Bed Frame'],
            'Kitchen': ['Coffee Maker', 'Blender', 'Air Fryer', 'Microwave', 'Toaster', 'Mixer'],
            'Garden': ['Lawn Mower', 'Garden Hose', 'Plant Pots', 'Garden Tools', 'BBQ Grill', 'Outdoor Furniture'],
            'Decor': ['Wall Art', 'Lamp', 'Rug', 'Curtains', 'Mirror', 'Vase']
        },
        'Books': {
            'Fiction': ['Mystery Novel', 'Science Fiction', 'Romance Novel', 'Thriller', 'Fantasy', 'Historical Fiction'],
            'Non-Fiction': ['Biography', 'Self-Help', 'History', 'Science', 'Travel', 'Cookbook'],
            'Educational': ['Textbook', 'Reference Guide', 'Dictionary', 'Encyclopedia', 'Study Guide', 'Workbook'],
            'Children': ['Picture Book', 'Young Adult', 'Activity Book', 'Comics', 'Fairy Tales', 'Educational']
        },
        'Sports & Outdoors': {
            'Equipment': ['Tennis Racket', 'Basketball', 'Yoga Mat', 'Dumbbells', 'Bicycle', 'Golf Clubs'],
            'Apparel': ['Sports Jersey', 'Running Shorts', 'Gym Leggings', 'Swimming Suit', 'Track Jacket', 'Sports Bra'],
            'Outdoor': ['Tent', 'Sleeping Bag', 'Backpack', 'Hiking Boots', 'Camping Stove', 'Flashlight'],
            'Fitness': ['Treadmill', 'Exercise Bike', 'Resistance Bands', 'Jump Rope', 'Foam Roller', 'Fitness Tracker']
        }
    }
    
    regions = ['North', 'South', 'East', 'West', 'Central']
    cities = {
        'North': ['New York', 'Boston', 'Chicago', 'Detroit', 'Minneapolis'],
        'South': ['Houston', 'Miami', 'Atlanta', 'Dallas', 'New Orleans'],
        'East': ['Philadelphia', 'Washington', 'Baltimore', 'Pittsburgh', 'Richmond'],
        'West': ['Los Angeles', 'San Francisco', 'Seattle', 'Portland', 'Denver'],
        'Central': ['Kansas City', 'St. Louis', 'Indianapolis', 'Milwaukee', 'Columbus']
    }
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer', 'Apple Pay', 'Google Pay']
    customer_segments = ['Premium', 'Standard', 'Budget', 'Corporate', 'Wholesale']
    
    data = []
    start_date = datetime.now() - timedelta(days=30)
    
    print(f"Generating {num_records} sales records...")
    
    for i in range(num_records):
        # Random date within the last 30 days
        transaction_date = start_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Random product selection
        category = random.choice(list(products.keys()))
        subcategory = random.choice(list(products[category].keys()))
        product_name = random.choice(products[category][subcategory])
        
        # Generate product ID
        product_id = f"PRD-{category[:3].upper()}-{random.randint(1000, 9999)}"
        
        # Pricing based on category
        price_ranges = {
            'Electronics': (50, 3000),
            'Clothing': (15, 500),
            'Home & Garden': (20, 2000),
            'Books': (5, 150),
            'Sports & Outdoors': (10, 1500)
        }
        
        min_price, max_price = price_ranges.get(category, (10, 1000))
        unit_price = round(random.uniform(min_price, max_price), 2)
        
        # Quantity (with weighted distribution)
        quantity_weights = [0.4, 0.3, 0.15, 0.08, 0.04, 0.02, 0.01]
        quantities = [1, 2, 3, 4, 5, 10, 20]
        quantity = np.random.choice(quantities, p=quantity_weights)
        
        # Discount (with weighted distribution)
        discount_weights = [0.3, 0.2, 0.2, 0.15, 0.1, 0.05]
        discounts = [0, 5, 10, 15, 20, 25]
        discount_percentage = np.random.choice(discounts, p=discount_weights)
        
        # Calculate total amount
        subtotal = unit_price * quantity
        discount_amount = subtotal * (discount_percentage / 100)
        total_amount = round(subtotal - discount_amount, 2)
        
        # Customer information
        customer_id = f"CUST-{str(random.randint(1, 2000)).zfill(5)}"
        customer_name = f"Customer {customer_id[-4:]}"
        customer_segment = random.choice(customer_segments)
        
        # Region and city
        region = random.choice(regions)
        city = random.choice(cities[region])
        
        # Payment method (weighted)
        payment_weights = [0.35, 0.25, 0.15, 0.1, 0.05, 0.05, 0.05]
        payment_method = np.random.choice(payment_methods, p=payment_weights)
        
        # Profit margin (influenced by category and segment)
        base_margin = random.uniform(15, 35)
        if customer_segment == 'Premium':
            base_margin += 5
        elif customer_segment == 'Budget':
            base_margin -= 5
        profit_margin = round(base_margin, 2)
        
        # Create transaction ID
        transaction_id = f"TXN-{transaction_date.strftime('%Y%m%d')}-{str(i+1).zfill(5)}"
        
        data.append({
            'transaction_id': transaction_id,
            'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'subcategory': subcategory,
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_percentage': discount_percentage,
            'total_amount': total_amount,
            'customer_id': customer_id,
            'customer_name': customer_name,
            'customer_segment': customer_segment,
            'region': region,
            'city': city,
            'payment_method': payment_method,
            'profit_margin': profit_margin
        })
        
        if (i + 1) % 100 == 0:
            print(f"  Generated {i + 1} records...")
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Sort by date
    df = df.sort_values('transaction_date')
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    
    print(f"\nSuccessfully generated {num_records} sales records")
    print(f"Output file: {output_file}")
    print(f"\nData summary:")
    print(f"  Date range: {df['transaction_date'].min()} to {df['transaction_date'].max()}")
    print(f"  Unique products: {df['product_id'].nunique()}")
    print(f"  Unique customers: {df['customer_id'].nunique()}")
    print(f"  Total revenue: ${df['total_amount'].sum():,.2f}")
    print(f"  Average order value: ${df['total_amount'].mean():.2f}")
    print(f"\nCategory distribution:")
    print(df['category'].value_counts())
    
    return df

def main():
    parser = argparse.ArgumentParser(description='Generate dummy sales data')
    parser.add_argument(
        '--records',
        type=int,
        default=1000,
        help='Number of records to generate (default: 1000)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='data/generated_sales.csv',
        help='Output CSV file path (default: data/generated_sales.csv)'
    )
    args = parser.parse_args()
    generate_sales_data(num_records=args.records, output_file=args.output)

if __name__ == '__main__':
    main()