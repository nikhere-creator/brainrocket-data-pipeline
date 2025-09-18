"""Simulated Kafka producer for gaming transaction events."""
import json
import time
import random
from datetime import datetime
from typing import Dict, Any
import argparse
import logging
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker for realistic data
fake = Faker()


def generate_transaction_event() -> Dict[str, Any]:
    """Generate a single gaming transaction event.
    
    Returns:
        Dict: Transaction event data
    """
    # Game IDs (1-10 from seed data)
    game_ids = list(range(1, 11))
    
    # Location IDs (1-15 from seed data)
    location_ids = list(range(1, 16))
    
    # Transaction types with weights
    transaction_types = ['purchase', 'in-game', 'subscription']
    type_weights = [0.3, 0.5, 0.2]
    
    # Generate transaction data
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
    
    # Platform types
    platforms = ['web', 'mobile', 'desktop', 'console']
    
    event = {
        'event_id': fake.uuid4(),
        'game_id': game_id,
        'location_id': location_id,
        'user_id': user_id,
        'transaction_type': transaction_type,
        'amount': amount,
        'currency': 'USD',
        'transaction_date': datetime.now().isoformat(),
        'platform': random.choice(platforms),
        'session_duration': random.randint(5, 180) if random.random() > 0.3 else None,
        'items_purchased': random.randint(1, 5) if transaction_type in ['purchase', 'in-game'] else 1,
        'event_timestamp': datetime.now().isoformat(),
        'source': 'kafka_producer'
    }
    
    return event


def main():
    """Main function to produce streaming events."""
    parser = argparse.ArgumentParser(description='Simulated Kafka producer for gaming transactions')
    parser.add_argument('--rate', type=float, default=1.0,
                       help='Events per second (default: 1.0)')
    parser.add_argument('--max-events', type=int, default=100,
                       help='Maximum number of events to produce (default: 100, 0 for unlimited)')
    parser.add_argument('--pretty', action='store_true',
                       help='Pretty print JSON output')
    
    args = parser.parse_args()
    
    logger.info(f"Starting producer with rate: {args.rate} events/sec, max: {args.max_events}")
    
    event_count = 0
    try:
        while True:
            if args.max_events > 0 and event_count >= args.max_events:
                logger.info(f"Reached maximum events limit: {args.max_events}")
                break
            
            # Generate and emit event
            event = generate_transaction_event()
            
            # Print as JSON (simulating Kafka message)
            if args.pretty:
                print(json.dumps(event, indent=2))
            else:
                print(json.dumps(event))
            
            event_count += 1
            
            # Log progress every 10 events
            if event_count % 10 == 0:
                logger.info(f"Produced {event_count} events")
            
            # Sleep to control rate
            if args.rate > 0:
                time.sleep(1.0 / args.rate)
                
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer error: {e}")
        raise
    
    logger.info(f"Total events produced: {event_count}")


if __name__ == '__main__':
    main()
