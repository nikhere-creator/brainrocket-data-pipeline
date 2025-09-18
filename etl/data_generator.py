"""Data generator for synthetic gaming transaction data."""
import argparse
import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker for realistic data
fake = Faker()


def generate_transactions(num_records: int = 1000, output_file: str = None) -> pd.DataFrame:
    """Generate synthetic gaming transaction data.
    
    Args:
        num_records: Number of transactions to generate
        output_file: Optional output CSV file path
        
    Returns:
        pd.DataFrame: Generated transaction data
    """
    logger.info(f"Generating {num_records} synthetic gaming transactions")
    
    # Game IDs (1-10 from seed data)
    game_ids = list(range(1, 11))
    
    # Location IDs (1-15 from seed data)
    location_ids = list(range(1, 16))
    
    # Transaction types with weights
    transaction_types = ['purchase', 'in-game', 'subscription']
    type_weights = [0.3, 0.5, 0.2]  # 30% purchases, 50% in-game, 20% subscriptions
    
    # Platform types
    platforms = ['web', 'mobile', 'desktop', 'console']
    
    transactions = []
    
    for _ in range(num_records):
        # Generate random transaction data
        game_id = random.choice(game_ids)
        location_id = random.choice(location_ids)
        user_id = f"user_{fake.uuid4()[:8]}"
        transaction_type = random.choices(transaction_types, weights=type_weights)[0]
        
        # Generate appropriate amount based on transaction type
        if transaction_type == 'purchase':
            amount = round(random.uniform(4.99, 99.99), 2)
        elif transaction_type == 'in-game':
            amount = round(random.uniform(0.99, 49.99), 2)
        else:  # subscription
            amount = round(random.uniform(9.99, 29.99), 2)
        
        # Generate transaction date (last 30 days)
        transaction_date = fake.date_time_between(
            start_date='-30d', 
            end_date='now'
        )
        
        # Additional optional fields
        platform = random.choice(platforms)
        session_duration = random.randint(5, 180) if random.random() > 0.3 else None
        items_purchased = random.randint(1, 5) if transaction_type in ['purchase', 'in-game'] else 1
        
        transaction = {
            'game_id': game_id,
            'location_id': location_id,
            'user_id': user_id,
            'transaction_type': transaction_type,
            'amount': amount,
            'currency': 'USD',
            'transaction_date': transaction_date,
            'platform': platform,
            'session_duration': session_duration,
            'items_purchased': items_purchased
        }
        
        transactions.append(transaction)
    
    # Create DataFrame
    df = pd.DataFrame(transactions)
    
    # Save to file if output path provided
    if output_file:
        try:
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df)} transactions to {output_file}")
        except Exception as e:
            logger.error(f"Failed to save transactions to {output_file}: {e}")
            raise
    
    return df


def main():
    """Main function to generate transaction data."""
    parser = argparse.ArgumentParser(description='Generate synthetic gaming transaction data')
    parser.add_argument('--num-records', type=int, default=1000, 
                       help='Number of transactions to generate (default: 1000)')
    parser.add_argument('--output', type=str, default='data/sample_transactions.csv',
                       help='Output CSV file path (default: data/sample_transactions.csv)')
    
    args = parser.parse_args()
    
    try:
        # Ensure output directory exists
        import os
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        
        # Generate transactions
        df = generate_transactions(args.num_records, args.output)
        
        # Print summary statistics
        logger.info(f"Generated {len(df)} transactions")
        logger.info(f"Transaction type distribution:")
        for ttype, count in df['transaction_type'].value_counts().items():
            logger.info(f"  {ttype}: {count} ({count/len(df)*100:.1f}%)")
        
        logger.info(f"Total revenue: ${df['amount'].sum():.2f}")
        logger.info(f"Average transaction: ${df['amount'].mean():.2f}")
        
    except Exception as e:
        logger.error(f"Error generating transactions: {e}")
        raise


if __name__ == '__main__':
    main()
