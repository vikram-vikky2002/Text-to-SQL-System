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

class LLMStatus(BaseModel):
    available: bool
    provider: str
    has_api_key: bool
    base_url: str

@app.get('/llm-status', response_model=LLMStatus)
def llm_status():
    """Check if LLM is available and configured"""
    return LLMStatus(
        available=engine.llm.available(),
        provider=engine.llm.provider,
        has_api_key=bool(engine.llm.api_key),
        base_url=engine.llm.base_url
    )

@app.post('/ask', response_model=Answer)
def ask(q: Query):
    answer, status, method = engine.answer(q.question)
    return Answer(answer=answer, status=status, method=method)

@app.on_event('shutdown')
def shutdown():
    engine.close()
