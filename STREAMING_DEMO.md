# BrainRocket Streaming Pipeline Demo

This document demonstrates how to test and run the BrainRocket streaming pipeline without Docker.

## 🚀 Quick Start

### 1. Setup Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas sqlalchemy psycopg2-binary faker python-dotenv
```

### 2. Test the Streaming Pipeline
```bash
# Set Python path
export PYTHONPATH=.

# Test producer alone
python3 streaming/producer.py --max-events 5 --rate 2

# Test pipe functionality (producer -> consumer)
python3 streaming/producer.py --max-events 10 --rate 2 | python3 streaming/consumer.py --batch-size 3
```

## 📋 Components

### Producer (`streaming/producer.py`)
Generates realistic gaming transaction events simulating a Kafka producer.

**Features:**
- Realistic gaming transaction data with Faker
- Configurable event rate (events per second)
- Configurable maximum events
- JSON output format

**Usage:**
```bash
python3 streaming/producer.py --max-events 20 --rate 5 --pretty
```

### Consumer (`streaming/consumer.py`)
Processes streaming events and simulates database insertion.

**Features:**
- Batch processing (configurable batch size)
- Time-based batch triggering
- Data validation and cleansing
- Simulated database operations

**Usage:**
```bash
python3 streaming/consumer.py --batch-size 5 --max-batch-time 10
```

## 🧪 Testing the Pipeline

### Manual Testing
```bash
# Basic test (5 events)
python3 streaming/producer.py --max-events 5 --rate 1 | python3 streaming/consumer.py --batch-size 1

# Higher volume test
python3 streaming/producer.py --max-events 20 --rate 3 | python3 streaming/consumer.py --batch-size 5 --max-batch-time 15
```

### Automated Test
```bash
# Run the comprehensive test
python3 test_streaming_pipeline.py
```

## ⚙️ Configuration Options

### Producer Options
- `--max-events`: Maximum number of events to produce (0 = unlimited)
- `--rate`: Events per second
- `--pretty`: Pretty-print JSON output

### Consumer Options
- `--batch-size`: Number of events to process in a batch
- `--max-batch-time`: Maximum time (seconds) to wait before processing a batch

## 🎯 What's Demonstrated

✅ **Real-time Event Production**: Simulated Kafka-like event streaming  
✅ **Stream Processing**: Batch processing of incoming events  
✅ **Data Validation**: Clean and validate transaction data  
✅ **Pipe Functionality**: Unix pipe integration (producer | consumer)  
✅ **Configurable Processing**: Adjustable batch sizes and timing  

## 🔄 Pipeline Flow

1. **Producer** generates gaming transaction events
2. Events are streamed via stdout (simulating Kafka)
3. **Consumer** reads events from stdin
4. Events are validated and batched
5. Batches are processed (simulated database insertion)
6. Materialized views are refreshed (simulated)

## 🚀 Next Steps

For a complete production setup:
1. Set up PostgreSQL database (`docker compose up -d`)
2. Run with actual database operations
3. Scale up event rates for performance testing
4. Integrate with actual Kafka/RabbitMQ for real streaming

## 📊 Sample Output

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "game_id": 7,
  "location_id": 12,
  "user_id": "user_abc123",
  "transaction_type": "purchase",
  "amount": 29.99,
  "currency": "USD",
  "transaction_date": "2025-09-18T12:34:56.789012",
  "platform": "mobile",
  "session_duration": 45,
  "items_purchased": 2,
  "event_timestamp": "2025-09-18T12:34:56.789012",
  "source": "kafka_producer"
}
```

## 🛠️ Troubleshooting

**Issue**: `ModuleNotFoundError: No module named 'etl'`  
**Solution**: Set `PYTHONPATH=.` or run from project root

**Issue**: Missing dependencies  
**Solution**: Install required packages: `pip install -r requirements.txt`

**Issue**: Virtual environment not activated  
**Solution**: Run `source venv/bin/activate`
