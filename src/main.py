import argparse
from .engine import QAEngine

def main():
    parser = argparse.ArgumentParser(description='Ask a financial/operational question.')
    parser.add_argument('question', help='Natural language question enclosed in quotes')
    args = parser.parse_args()
    engine = QAEngine()
    answer, status, method = engine.answer(args.question)
    print(f"[{method}] {answer}")
    engine.close()

if __name__ == '__main__':
    main()
