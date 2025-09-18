"""Simulated Kafka consumer for gaming transaction events."""
import json
import sys
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
import argparse
from etl.utils import get_db_connection, refresh_materialized_view, logger


class StreamingConsumer:
    """Consumer that processes streaming transaction events."""
    
    def __init__(self, batch_size: int = 100, max_batch_time: int = 30):
        """Initialize the consumer.
        
        Args:
            batch_size: Number of events to process in a batch
            max_batch_time: Maximum time (seconds) to wait before processing a batch
        """
        self.batch_size = batch_size
        self.max_batch_time = max_batch_time
        self.batch: List[Dict[str, Any]] = []
        self.last_batch_time = datetime.now()
        self.db_engine = None
        
    def connect_to_database(self):
        """Establish database connection."""
        if self.db_engine is None:
            self.db_engine = get_db_connection()
    
    def process_event(self, event: Dict[str, Any]):
        """Process a single event and add to batch.
        
        Args:
            event: Transaction event data
        """
        # Validate required fields
        required_fields = ['game_id', 'location_id', 'user_id', 'transaction_type', 'amount', 'transaction_date']
        if not all(field in event for field in required_fields):
            logger.warning(f"Skipping invalid event: missing required fields")
            return
        
        # Clean and transform event data
        cleaned_event = {
            'game_id': int(event['game_id']),
            'location_id': int(event['location_id']),
            'user_id': str(event['user_id']),
            'transaction_type': str(event['transaction_type']).lower().strip(),
            'amount': float(event['amount']),
            'currency': event.get('currency', 'USD'),
            'transaction_date': datetime.fromisoformat(event['transaction_date'].replace('Z', '+00:00')),
            'platform': event.get('platform', 'web'),
            'session_duration': event.get('session_duration'),
            'items_purchased': event.get('items_purchased', 1)
        }
        
        # Validate transaction type
        if cleaned_event['transaction_type'] not in ['purchase', 'in-game', 'subscription']:
            logger.warning(f"Skipping event with invalid transaction type: {cleaned_event['transaction_type']}")
            return
        
        # Validate amount
        if cleaned_event['amount'] <= 0:
            logger.warning(f"Skipping event with invalid amount: {cleaned_event['amount']}")
            return
        
        self.batch.append(cleaned_event)
        
        # Check if we should process the batch
        current_time = datetime.now()
        time_since_last_batch = (current_time - self.last_batch_time).total_seconds()
        
        if len(self.batch) >= self.batch_size or time_since_last_batch >= self.max_batch_time:
            self.process_batch()
    
    def process_batch(self):
        """Process the current batch of events."""
        if not self.batch:
            return
        
        logger.info(f"Processing batch of {len(self.batch)} events")
        
        try:
            self.connect_to_database()
            
            # Convert batch to DataFrame
            df = pd.DataFrame(self.batch)
            
            # Load to database
            rows_inserted = df.to_sql(
                'fact_transactions', 
                self.db_engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully loaded {rows_inserted} events to database")
            
            # Refresh materialized view
            refresh_materialized_view(self.db_engine)
            
            # Clear batch
            self.batch.clear()
            self.last_batch_time = datetime.now()
            
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            # Keep the batch for retry (in real scenario, you might want to implement retry logic)
    
    def process_stdin(self):
        """Process events from standard input."""
        logger.info("Starting consumer, reading from stdin...")
        logger.info(f"Batch size: {self.batch_size}, Max batch time: {self.max_batch_time}s")
        
        try:
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    event = json.loads(line)
                    self.process_event(event)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON: {line}")
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
        
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            # Process any remaining events in the batch
            if self.batch:
                self.process_batch()
            logger.info("Consumer shutdown complete")


def main():
    """Main function to run the streaming consumer."""
    parser = argparse.ArgumentParser(description='Simulated Kafka consumer for gaming transactions')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of events to process in a batch (default: 100)')
    parser.add_argument('--max-batch-time', type=int, default=30,
                       help='Maximum time (seconds) to wait before processing a batch (default: 30)')
    
    args = parser.parse_args()
    
    consumer = StreamingConsumer(
        batch_size=args.batch_size,
        max_batch_time=args.max_batch_time
    )
    
    consumer.process_stdin()


if __name__ == '__main__':
    main()
