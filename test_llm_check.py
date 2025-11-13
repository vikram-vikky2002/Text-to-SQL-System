"""Quick check if LLM is available and working"""
import os
from pathlib import Path
from src.llm_strategy import LLMStrategy

# Check environment variables
print("=== Environment Check ===")
print(f"LLM_PROVIDER: {os.getenv('LLM_PROVIDER', 'NOT SET')}")
print(f"OPENAI_API_KEY: {'SET' if os.getenv('OPENAI_API_KEY') else 'NOT SET'}")
print(f"OPENAI_BASE_URL: {os.getenv('OPENAI_BASE_URL', 'NOT SET (will use default)')}")

# Try to load .env file
print("\n=== Checking .env file ===")
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    print(f"✓ .env file found at: {env_path}")
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"✓ .env loaded")
        print(f"LLM_PROVIDER after load: {os.getenv('LLM_PROVIDER', 'NOT SET')}")
        print(f"OPENAI_API_KEY after load: {'SET (length: ' + str(len(os.getenv('OPENAI_API_KEY', ''))) + ')' if os.getenv('OPENAI_API_KEY') else 'NOT SET'}")
    except Exception as e:
        print(f"✗ Error loading .env: {e}")
else:
    print(f"✗ .env file NOT found at: {env_path}")

# Initialize LLM Strategy
print("\n=== LLM Strategy Initialization ===")
db_path = Path(__file__).parent / 'financial.db'
llm = LLMStrategy(db_path)
print(f"Provider: {llm.provider}")
print(f"API Key: {'SET (length: ' + str(len(llm.api_key)) + ')' if llm.api_key else 'NOT SET'}")
print(f"Base URL: {llm.base_url}")
print(f"Available: {llm.available()}")

# Test SQL generation if available
if llm.available():
    print("\n=== Testing SQL Generation ===")
    test_question = "What was revenue in 2024-25?"
    sql, status = llm.generate_sql(test_question)
    print(f"Question: {test_question}")
    print(f"Status: {status}")
    if sql:
        print(f"Generated SQL: {sql}")
    else:
        print(f"No SQL generated (status: {status})")
else:
    print("\n✗ LLM is NOT available - cannot test SQL generation")
    print("\nTo fix:")
    print("1. Create a .env file in the root directory (E:\\EY\\.env)")
    print("2. Add: LLM_PROVIDER=openai")
    print("3. Add: OPENAI_API_KEY=your-api-key-here")
