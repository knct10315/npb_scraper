from fastapi import FastAPI
from datetime import datetime

from npb_app import run_job as run_npb_job
from mlb_app import run_job as run_mlb_job

app = FastAPI()

IS_NPB_RUNNING = False
IS_MLB_RUNNING = False


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/run_npb")
def run_npb():
    global IS_NPB_RUNNING

    if IS_NPB_RUNNING:
        return {
            "status": "already_running",
            "league": "NPB",
            "message": "NPB処理中です。完了まで再実行しないでください。"
        }

    IS_NPB_RUNNING = True

    try:
        results = run_npb_job()

        return {
            "status": "completed",
            "league": "NPB",
            "match_count": len(results),
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    finally:
        IS_NPB_RUNNING = False


@app.get("/run_mlb")
def run_mlb():
    global IS_MLB_RUNNING

    if IS_MLB_RUNNING:
        return {
            "status": "already_running",
            "league": "MLB",
            "message": "MLB処理中です。完了まで再実行しないでください。"
        }

    IS_MLB_RUNNING = True

    try:
        results = run_mlb_job()

        return {
            "status": "completed",
            "league": "MLB",
            "match_count": len(results),
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    finally:
        IS_MLB_RUNNING = False