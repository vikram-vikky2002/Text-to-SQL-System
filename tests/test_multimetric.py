from src.engine import QAEngine

def test_multi_metric():
    engine = QAEngine()
    answer, status, method = engine.answer("Show revenue and EBITDA performance")
    engine.close()
    assert status == 'OK'
    assert method == 'heuristic'
    assert 'Revenue' in answer and 'EBITDA' in answer and 'Margin' in answer

def test_dry_cargo_volumes():
    engine = QAEngine()
    answer, status, method = engine.answer("List dry cargo volumes by port for 2024-25")
    engine.close()
    assert status == 'OK'
    assert 'Cargo volumes by port:' in answer