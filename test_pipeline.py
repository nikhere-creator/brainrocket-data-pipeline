#!/usr/bin/env python3
"""Quick test script to verify the complete pipeline is working."""
import subprocess
import sys
import os

def test_data_generator():
    """Test the data generator."""
    print("🧪 Testing data generator...")
    try:
        result = subprocess.run([
            sys.executable, "etl/data_generator.py", 
            "--num-records", "10",
            "--output", "data/test_pipeline.csv"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Data generator test passed")
            return True
        else:
            print(f"❌ Data generator test failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Data generator test error: {e}")
        return False

def test_producer():
    """Test the streaming producer."""
    print("🧪 Testing streaming producer...")
    try:
        result = subprocess.run([
            sys.executable, "streaming/producer.py",
            "--max-events", "2"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Streaming producer test passed")
            return True
        else:
            print(f"❌ Streaming producer test failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Streaming producer test error: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Testing BrainRocket Data Pipeline Components")
    print("=" * 50)
    
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    # Run tests
    tests = [
        test_data_generator,
        test_producer,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The pipeline is ready to use.")
        print("\nNext steps:")
        print("1. Start database: docker compose up -d")
        print("2. Initialize database: python etl/etl_batch.py --init-db --input data/test_pipeline.csv")
        print("3. Test streaming: python streaming/producer.py --rate 1 | python streaming/consumer.py")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
