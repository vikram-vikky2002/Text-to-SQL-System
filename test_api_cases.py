"""
Test script to verify both LLM and heuristic routing through the API
"""
import requests
import json

BASE_URL = "http://localhost:8000"

# Test cases with expected method (LLM or heuristic)
test_cases = [
    # LLM cases - analytical queries with keywords
    {
        "name": "Revenue and EBITDA growth comparison",
        "question": "Compare revenue and EBITDA growth between 2023-24 and 2024-25",
        "expected_method": "LLM",
        "keywords": ["compare", "growth"]
    },
    {
        "name": "Revenue growth trend",
        "question": "What is the revenue growth trend over the years?",
        "expected_method": "LLM",
        "keywords": ["growth", "trend"]
    },
    {
        "name": "EBITDA change analysis",
        "question": "Explain the change in EBITDA from 2022-23 to 2024-25",
        "expected_method": "LLM",
        "keywords": ["explain", "change"]
    },
    {
        "name": "Revenue forecast",
        "question": "What is the forecast for revenue growth?",
        "expected_method": "LLM",
        "keywords": ["forecast", "growth"]
    },
    {
        "name": "Ratio comparison",
        "question": "Compare the ratio of EBITDA to revenue across years",
        "expected_method": "LLM",
        "keywords": ["compare", "ratio"]
    },
    
    # Heuristic cases - specific patterns
    {
        "name": "YOY growth (specific keyword)",
        "question": "What is the year over year growth in EBITDA between 2023-24 and 2024-25?",
        "expected_method": "heuristic",
        "keywords": ["year over year growth"]
    },
    {
        "name": "Revenue margin trend",
        "question": "Compare revenue and margin trend over last 3 years",
        "expected_method": "heuristic",
        "keywords": ["compare", "revenue", "margin", "trend"]
    },
    {
        "name": "Correlation heuristic",
        "question": "What is the correlation between revenue and EBITDA margin?",
        "expected_method": "heuristic",
        "keywords": ["correlation", "revenue", "margin"]
    },
    {
        "name": "Capital employed vs EBIT",
        "question": "Compare average capital employed and EBIT trend over last 4 years",
        "expected_method": "heuristic",
        "keywords": ["average capital employed", "ebit", "compare", "trend"]
    },
    
    # Simple queries without analytical keywords - should use heuristic fallback
    {
        "name": "Simple revenue query",
        "question": "What was the revenue in 2024-25?",
        "expected_method": "heuristic",
        "keywords": []
    },
    {
        "name": "Simple EBITDA query",
        "question": "Show EBITDA for 2023-24",
        "expected_method": "heuristic",
        "keywords": []
    },
    {
        "name": "Top ports ranking",
        "question": "Rank the top 5 ports by EBIT",
        "expected_method": "heuristic",
        "keywords": []
    },
    {
        "name": "Port volumes",
        "question": "What are the cargo volumes by port?",
        "expected_method": "heuristic",
        "keywords": []
    },
]

def test_llm_status():
    """Check if LLM is available"""
    print("=" * 80)
    print("CHECKING LLM STATUS")
    print("=" * 80)
    try:
        response = requests.get(f"{BASE_URL}/llm-status")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ LLM Available: {data['available']}")
            print(f"  Provider: {data['provider']}")
            print(f"  Has API Key: {data['has_api_key']}")
            print(f"  Base URL: {data['base_url']}")
            return data['available']
        else:
            print(f"✗ Failed to get LLM status: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Error checking LLM status: {e}")
        return False

def run_test(test_case):
    """Run a single test case"""
    question = test_case["question"]
    expected_method = test_case["expected_method"]
    
    try:
        response = requests.post(
            f"{BASE_URL}/ask",
            json={"question": question},
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code != 200:
            return {
                "success": False,
                "error": f"HTTP {response.status_code}",
                "method": None,
                "answer": None
            }
        
        data = response.json()
        actual_method = data.get("method", "unknown")
        answer = data.get("answer", "")
        status = data.get("status", "")
        
        # Check if method matches expected
        method_match = actual_method.lower() == expected_method.lower()
        
        return {
            "success": method_match and status == "OK",
            "method": actual_method,
            "expected_method": expected_method,
            "method_match": method_match,
            "answer": answer,
            "status": status
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "method": None,
            "answer": None
        }

def main():
    print("\n" + "=" * 80)
    print("API TEST SUITE - LLM vs Heuristic Routing")
    print("=" * 80 + "\n")
    
    # Check LLM status first
    llm_available = test_llm_status()
    print()
    
    if not llm_available:
        print("⚠️  WARNING: LLM is not available. LLM tests will likely fail.\n")
    
    # Run all tests
    results = []
    passed = 0
    failed = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{'=' * 80}")
        print(f"Test {i}/{len(test_cases)}: {test_case['name']}")
        print(f"{'=' * 80}")
        print(f"Question: {test_case['question']}")
        print(f"Expected Method: {test_case['expected_method'].upper()}")
        if test_case['keywords']:
            print(f"Keywords: {', '.join(test_case['keywords'])}")
        
        result = run_test(test_case)
        results.append({**test_case, **result})
        
        if result.get("error"):
            print(f"\n✗ ERROR: {result['error']}")
            failed += 1
        else:
            actual_method = result['method']
            method_match = result['method_match']
            
            print(f"\nActual Method: {actual_method.upper()}")
            print(f"Status: {result['status']}")
            
            if method_match:
                print(f"✓ PASSED - Method matches expected")
                passed += 1
            else:
                print(f"✗ FAILED - Expected {test_case['expected_method'].upper()}, got {actual_method.upper()}")
                failed += 1
            
            # Show answer (truncated)
            answer = result['answer']
            if len(answer) > 100:
                print(f"Answer: {answer[:100]}...")
            else:
                print(f"Answer: {answer}")
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Total Tests: {len(test_cases)}")
    print(f"✓ Passed: {passed}")
    print(f"✗ Failed: {failed}")
    print(f"Success Rate: {(passed/len(test_cases)*100):.1f}%")
    
    # Breakdown by method
    print("\n" + "-" * 80)
    print("BREAKDOWN BY EXPECTED METHOD")
    print("-" * 80)
    
    llm_tests = [r for r in results if r['expected_method'] == 'LLM']
    llm_passed = sum(1 for r in llm_tests if r.get('method_match', False))
    print(f"LLM Tests: {llm_passed}/{len(llm_tests)} passed")
    
    heuristic_tests = [r for r in results if r['expected_method'] == 'heuristic']
    heuristic_passed = sum(1 for r in heuristic_tests if r.get('method_match', False))
    print(f"Heuristic Tests: {heuristic_passed}/{len(heuristic_tests)} passed")
    
    # Show failures
    failures = [r for r in results if not r.get('method_match', False) and not r.get('error')]
    if failures:
        print("\n" + "-" * 80)
        print("FAILED TESTS (Method Mismatch)")
        print("-" * 80)
        for f in failures:
            print(f"\n• {f['name']}")
            print(f"  Question: {f['question']}")
            print(f"  Expected: {f['expected_method']}, Got: {f['method']}")
    
    print("\n" + "=" * 80 + "\n")
    
    return passed == len(test_cases)

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
