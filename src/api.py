from fastapi import FastAPI
from pydantic import BaseModel
from .engine import QAEngine

app = FastAPI(title="Text-to-SQL PoC")
engine = QAEngine()

class Query(BaseModel):
    question: str

class Answer(BaseModel):
    answer: str
    status: str
    method: str

@app.post('/ask', response_model=Answer)
def ask(q: Query):
    answer, status, method = engine.answer(q.question)
    return Answer(answer=answer, status=status, method=method)

@app.on_event('shutdown')
def shutdown():
    engine.close()
