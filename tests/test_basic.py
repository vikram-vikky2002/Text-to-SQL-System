from src.engine import QAEngine

def test_basic_ebitda():
    engine = QAEngine()
    answer, status, method = engine.answer("What was EBITDA in 2024-25?")
    engine.close()
    assert status == 'OK'
    assert method == 'heuristic'
    assert any(ch.isdigit() for ch in answer)

def test_yoy_growth():
    engine = QAEngine()
    answer, status, method = engine.answer("Year over year growth in EBITDA between 2023-24 and 2024-25")
    engine.close()
    assert status == 'OK'
    assert method == 'heuristic'  # deterministic path
    assert 'YOY growth' in answer and '%' in answer

def test_llm_fallback_disabled():
    engine = QAEngine()
    answer, status, method = engine.answer("Year over year growth in EBITDA between 2023-24 and 2024-25")
    engine.close()
    assert status == 'OK'
    assert method == 'heuristic'  # LLM not configured in test env
    assert 'YOY growth' in answer and '%' in answer

def test_correlation():
    engine = QAEngine()
    answer, status, method = engine.answer("Explain correlation between revenue growth and EBITDA margin change")
    engine.close()
    assert status == 'OK'
    assert 'Correlation between revenue YoY growth and EBITDA margin change' in answer

def test_capital_ebit_compare():
    engine = QAEngine()
    answer, status, method = engine.answer("Explain change in average capital employed versus EBIT trend")
    engine.close()
    assert status == 'OK'
    assert 'Avg Cap Empl' in answer and 'ROCE' in answer
    # Ensure no duplicate consecutive periods
    periods = [seg.split(':')[0].strip() for seg in answer.split('|')]
    assert len(periods) == len(set(periods))
