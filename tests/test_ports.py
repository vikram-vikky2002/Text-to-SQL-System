from src.engine import QAEngine

def test_top_ports_ebit():
    engine = QAEngine()
    answer, status, method = engine.answer("Top 5 ports by EBIT in 2024-25")
    engine.close()
    assert status == 'OK'
    assert method == 'heuristic'
    assert 'Top ports by EBIT:' in answer
    # Ensure no duplicate port names in the formatted list
    listed = [seg.split('(')[0].strip() for seg in answer.split(':',1)[1].split(',')]
    assert len(listed) == len(set(listed))

def test_port_ebit_per_mmt():
    engine = QAEngine()
    answer, status, method = engine.answer("EBIT per MMT in 2024-25")
    engine.close()
    assert status == 'OK'
    assert method == 'heuristic'
    assert any(ch.isdigit() for ch in answer)