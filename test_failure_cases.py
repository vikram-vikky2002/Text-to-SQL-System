"""
Test cases designed to identify potential failure scenarios for the Text-to-SQL agent
"""
import requests
import json

BASE_URL = "http://localhost:8000"

# Edge cases and potential failure scenarios
failure_test_cases = [
    # ========== Data Availability Issues ==========
    {
        "category": "Missing Data",
        "name": "Future period query",
        "question": "What is the revenue for 2025-26?",
        "expected_issue": "Period doesn't exist in database",
        "severity": "high"
    },
    {
        "category": "Missing Data",
        "name": "Very old period",
        "question": "What was EBITDA in 2018-19?",
        "expected_issue": "Period may not exist",
        "severity": "medium"
    },
    {
        "category": "Missing Data",
        "name": "Non-existent port",
        "question": "What is the cargo volume at Mumbai Port?",
        "expected_issue": "Port not in database",
        "severity": "high"
    },
    {
        "category": "Missing Data",
        "name": "Unsupported metric",
        "question": "What is the share price in 2024-25?",
        "expected_issue": "Share price is explicitly unsupported",
        "severity": "high"
    },
    
    # ========== Ambiguous Queries ==========
    {
        "category": "Ambiguity",
        "name": "Multiple metrics without clear intent",
        "question": "Show me revenue EBITDA profit tax for all years",
        "expected_issue": "Too many metrics, unclear output format",
        "severity": "medium"
    },
    {
        "category": "Ambiguity",
        "name": "Vague time reference",
        "question": "What was revenue recently?",
        "expected_issue": "Unclear what 'recently' means",
        "severity": "medium"
    },
    {
        "category": "Ambiguity",
        "name": "Unclear comparison",
        "question": "Compare things",
        "expected_issue": "No specific metrics or periods",
        "severity": "high"
    },
    {
        "category": "Ambiguity",
        "name": "Multiple ports without aggregation",
        "question": "Revenue for Mundra and Hazira ports",
        "expected_issue": "Unclear if sum or separate values wanted",
        "severity": "low"
    },
    
    # ========== Complex Calculations ==========
    {
        "category": "Complex Calculation",
        "name": "Multi-year CAGR",
        "question": "What is the CAGR of revenue from 2021-22 to 2024-25?",
        "expected_issue": "CAGR calculation not implemented",
        "severity": "high"
    },
    {
        "category": "Complex Calculation",
        "name": "Year-to-date comparison",
        "question": "Compare Q1 2024-25 with Q1 2023-24",
        "expected_issue": "Quarterly data handling unclear",
        "severity": "medium"
    },
    {
        "category": "Complex Calculation",
        "name": "Percentage point change",
        "question": "What is the percentage point change in EBITDA margin?",
        "expected_issue": "May confuse percentage vs percentage point",
        "severity": "medium"
    },
    {
        "category": "Complex Calculation",
        "name": "Weighted average",
        "question": "What is the weighted average EBITDA margin across all ports?",
        "expected_issue": "Weighted average logic not implemented",
        "severity": "high"
    },
    
    # ========== Synonym/Terminology Issues ==========
    {
        "category": "Terminology",
        "name": "Uncommon synonym",
        "question": "What is the turnover in 2024-25?",
        "expected_issue": "Turnover not in synonym list for Revenue",
        "severity": "medium"
    },
    {
        "category": "Terminology",
        "name": "Abbreviation not recognized",
        "question": "Show me the PAT for last year",
        "expected_issue": "PAT (Profit After Tax) may not be mapped",
        "severity": "medium"
    },
    {
        "category": "Terminology",
        "name": "Industry jargon",
        "question": "What is the TEU handled at Mundra?",
        "expected_issue": "TEU (Twenty-foot Equivalent Unit) not recognized",
        "severity": "low"
    },
    
    # ========== LLM-Specific Failures ==========
    {
        "category": "LLM Generation",
        "name": "Complex multi-table join",
        "question": "Compare cargo volume growth with revenue growth by port across all years",
        "expected_issue": "LLM may generate overly complex SQL",
        "severity": "high"
    },
    {
        "category": "LLM Generation",
        "name": "Nested aggregations",
        "question": "What is the average of quarterly EBITDA margins?",
        "expected_issue": "May require nested aggregation not supported",
        "severity": "high"
    },
    {
        "category": "LLM Generation",
        "name": "Window functions",
        "question": "Show revenue with running total over years",
        "expected_issue": "Window functions may not be generated correctly",
        "severity": "medium"
    },
    
    # ========== Edge Cases in Logic ==========
    {
        "category": "Logic Edge Case",
        "name": "Division by zero scenario",
        "question": "What is the ratio of profit to revenue for all years?",
        "expected_issue": "May have zero revenue in some cases",
        "severity": "medium"
    },
    {
        "category": "Logic Edge Case",
        "name": "Growth from zero",
        "question": "What is the growth rate from 2021 to 2022 for new ports?",
        "expected_issue": "Cannot calculate growth from zero baseline",
        "severity": "low"
    },
    {
        "category": "Logic Edge Case",
        "name": "NULL value handling",
        "question": "Show all metrics for 2020-21",
        "expected_issue": "May have NULL values causing calculation errors",
        "severity": "medium"
    },
    
    # ========== Formatting/Output Issues ==========
    {
        "category": "Output Format",
        "name": "Too many results",
        "question": "Show me all transactions for all ports all years",
        "expected_issue": "May return too much data to display",
        "severity": "low"
    },
    {
        "category": "Output Format",
        "name": "Mixed metric types",
        "question": "Show revenue in millions and cargo in MMT",
        "expected_issue": "Mixed units may be confusing",
        "severity": "low"
    },
    
    # ========== Scope Violations ==========
    {
        "category": "Out of Scope",
        "name": "External data request",
        "question": "How does our revenue compare to competitors?",
        "expected_issue": "Competitor data not available",
        "severity": "high"
    },
    {
        "category": "Out of Scope",
        "name": "Future prediction",
        "question": "What will revenue be next year?",
        "expected_issue": "Forecasting not available",
        "severity": "high"
    },
    {
        "category": "Out of Scope",
        "name": "Macroeconomic data",
        "question": "How did GDP affect our revenue?",
        "expected_issue": "External economic data not available",
        "severity": "high"
    },
    
    # ========== Typos and Misspellings ==========
    {
        "category": "Typos",
        "name": "Metric misspelling",
        "question": "What is the EBITDA in 2024-25?",  # Correct spelling but testing
        "expected_issue": "Should handle EBIDTA variant",
        "severity": "low"
    },
    {
        "category": "Typos",
        "name": "Period format variation",
        "question": "Show revenue for FY2024-25",
        "expected_issue": "FY prefix may not be parsed",
        "severity": "medium"
    },
    {
        "category": "Typos",
        "name": "Port name variation",
        "question": "What is cargo at Mundra port?",
        "expected_issue": "May have 'port' suffix not in database",
        "severity": "low"
    },
]

def run_failure_test(test_case):
    """Run a failure test case and capture the response"""
    question = test_case["question"]
    
    try:
        response = requests.post(
            f"{BASE_URL}/ask",
            json={"question": question},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code != 200:
            return {
                "http_error": True,
                "status_code": response.status_code,
                "error": f"HTTP {response.status_code}",
                "method": None,
                "answer": None,
                "response_status": None
            }
        
        data = response.json()
        method = data.get("method", "unknown")
        answer = data.get("answer", "")
        status = data.get("status", "")
        
        # Detect common failure patterns
        failure_indicators = {
            "no_data": "no matching data" in answer.lower() or "no data" in answer.lower(),
            "insufficient_data": "insufficient" in answer.lower(),
            "unavailable": "unavailable" in answer.lower(),
            "not_found": "not found" in answer.lower(),
            "error_message": status == "FAIL",
            "out_of_scope": "can only answer questions about" in answer.lower(),
            "specify_more": "specify" in answer.lower(),
            "empty_result": len(answer.strip()) == 0,
            "very_short": len(answer) < 20,
        }
        
        return {
            "http_error": False,
            "status_code": 200,
            "method": method,
            "answer": answer,
            "response_status": status,
            "failure_indicators": {k: v for k, v in failure_indicators.items() if v},
            "has_failure_indicator": any(failure_indicators.values())
        }
    except requests.Timeout:
        return {
            "http_error": True,
            "error": "Request timeout",
            "method": None,
            "answer": None,
            "response_status": None
        }
    except Exception as e:
        return {
            "http_error": True,
            "error": str(e),
            "method": None,
            "answer": None,
            "response_status": None
        }

def main():
    print("\n" + "=" * 100)
    print("FAILURE CASE ANALYSIS - Testing Edge Cases and Potential Issues")
    print("=" * 100 + "\n")
    
    # Group tests by category
    categories = {}
    for test in failure_test_cases:
        cat = test["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(test)
    
    total_tests = len(failure_test_cases)
    failed_gracefully = 0  # Handled the error gracefully
    returned_data = 0  # Actually returned data
    crashed = 0  # HTTP errors or exceptions
    
    all_results = []
    
    for category, tests in categories.items():
        print(f"\n{'=' * 100}")
        print(f"CATEGORY: {category} ({len(tests)} tests)")
        print(f"{'=' * 100}\n")
        
        for i, test_case in enumerate(tests, 1):
            print(f"Test {i}/{len(tests)}: {test_case['name']}")
            print(f"  Question: {test_case['question']}")
            print(f"  Expected Issue: {test_case['expected_issue']}")
            print(f"  Severity: {test_case['severity'].upper()}")
            
            result = run_failure_test(test_case)
            result['test_case'] = test_case
            all_results.append(result)
            
            if result.get('http_error'):
                print(f"  ✗ CRASHED: {result.get('error', 'Unknown error')}")
                crashed += 1
            else:
                method = result['method']
                status = result['response_status']
                answer = result['answer']
                
                print(f"  Method: {method}")
                print(f"  Status: {status}")
                
                if result['has_failure_indicator']:
                    indicators = ", ".join(result['failure_indicators'].keys())
                    print(f"  ⚠️  FAILED GRACEFULLY: {indicators}")
                    failed_gracefully += 1
                else:
                    print(f"  ✓ RETURNED DATA (may or may not be correct)")
                    returned_data += 1
                
                # Show truncated answer
                if len(answer) > 80:
                    print(f"  Answer: {answer[:80]}...")
                else:
                    print(f"  Answer: {answer}")
            
            print()
    
    # Summary
    print("\n" + "=" * 100)
    print("FAILURE ANALYSIS SUMMARY")
    print("=" * 100)
    print(f"Total Tests: {total_tests}")
    print(f"  ✓ Returned Data: {returned_data} ({returned_data/total_tests*100:.1f}%)")
    print(f"  ⚠️  Failed Gracefully: {failed_gracefully} ({failed_gracefully/total_tests*100:.1f}%)")
    print(f"  ✗ Crashed/Error: {crashed} ({crashed/total_tests*100:.1f}%)")
    
    # Breakdown by severity
    print("\n" + "-" * 100)
    print("BREAKDOWN BY SEVERITY")
    print("-" * 100)
    
    for severity in ['high', 'medium', 'low']:
        severity_tests = [r for r in all_results if r['test_case']['severity'] == severity]
        if severity_tests:
            crashed_count = sum(1 for r in severity_tests if r.get('http_error'))
            failed_count = sum(1 for r in severity_tests if not r.get('http_error') and r.get('has_failure_indicator'))
            success_count = sum(1 for r in severity_tests if not r.get('http_error') and not r.get('has_failure_indicator'))
            
            print(f"\n{severity.upper()} Severity ({len(severity_tests)} tests):")
            print(f"  Returned Data: {success_count}")
            print(f"  Failed Gracefully: {failed_count}")
            print(f"  Crashed: {crashed_count}")
    
    # Most problematic categories
    print("\n" + "-" * 100)
    print("MOST PROBLEMATIC CATEGORIES")
    print("-" * 100)
    
    category_stats = {}
    for result in all_results:
        cat = result['test_case']['category']
        if cat not in category_stats:
            category_stats[cat] = {'total': 0, 'crashed': 0, 'failed': 0, 'success': 0}
        
        category_stats[cat]['total'] += 1
        if result.get('http_error'):
            category_stats[cat]['crashed'] += 1
        elif result.get('has_failure_indicator'):
            category_stats[cat]['failed'] += 1
        else:
            category_stats[cat]['success'] += 1
    
    sorted_cats = sorted(category_stats.items(), 
                        key=lambda x: (x[1]['crashed'] + x[1]['failed']), 
                        reverse=True)
    
    for cat, stats in sorted_cats:
        issue_rate = (stats['crashed'] + stats['failed']) / stats['total'] * 100
        print(f"\n{cat}:")
        print(f"  Issue Rate: {issue_rate:.1f}%")
        print(f"  Total: {stats['total']}, Success: {stats['success']}, Failed: {stats['failed']}, Crashed: {stats['crashed']}")
    
    # Specific recommendations
    print("\n" + "=" * 100)
    print("RECOMMENDATIONS FOR IMPROVEMENT")
    print("=" * 100)
    
    recommendations = []
    
    # Check for high-severity crashes
    high_severity_crashes = [r for r in all_results 
                            if r['test_case']['severity'] == 'high' and r.get('http_error')]
    if high_severity_crashes:
        recommendations.append(f"• Fix {len(high_severity_crashes)} high-severity crashes")
    
    # Check for missing data handling
    missing_data_issues = [r for r in all_results 
                          if r['test_case']['category'] == 'Missing Data' 
                          and not r.get('has_failure_indicator')
                          and not r.get('http_error')]
    if missing_data_issues:
        recommendations.append(f"• Improve missing data detection ({len(missing_data_issues)} cases returned data for non-existent items)")
    
    # Check terminology gaps
    terminology_issues = [r for r in all_results 
                         if r['test_case']['category'] == 'Terminology'
                         and r.get('has_failure_indicator')]
    if terminology_issues:
        recommendations.append(f"• Expand synonym dictionary ({len(terminology_issues)} terminology issues)")
    
    # Check LLM generation issues
    llm_issues = [r for r in all_results 
                 if r['test_case']['category'] == 'LLM Generation'
                 and (r.get('http_error') or r.get('has_failure_indicator'))]
    if llm_issues:
        recommendations.append(f"• Improve LLM SQL generation or add heuristics ({len(llm_issues)} complex query failures)")
    
    if recommendations:
        for rec in recommendations:
            print(rec)
    else:
        print("✓ No critical issues found!")
    
    print("\n" + "=" * 100 + "\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
