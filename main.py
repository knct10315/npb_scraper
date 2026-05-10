from fastapi import FastAPI
from datetime import datetime

from npb_app import run_job as run_npb_job
from mlb_app import run_job as run_mlb_job

app = FastAPI()


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/run_npb")
def run_npb():
    results = run_npb_job()

    return {
        "status": "completed",
        "league": "NPB",
        "match_count": len(results),
        "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


@app.get("/run_mlb")
def run_mlb():
    results = run_mlb_job()

    return {
        "status": "completed",
        "league": "MLB",
        "match_count": len(results),
        "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }