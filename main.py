from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import PlainTextResponse
import os
import threading
import time
import traceback

app = FastAPI()


def restart_after_response(delay_seconds=3):
    """
    レスポンス返却後にPythonプロセスを終了させる。
    Render Web Serviceはプロセス終了後に自動再起動するため、
    実行ごとにPythonが保持したメモリをリセットできる。
    """
    def _exit_later():
        time.sleep(delay_seconds)
        os._exit(0)

    threading.Thread(target=_exit_later, daemon=True).start()


@app.get("/")
def root():
    return PlainTextResponse("OK")


@app.get("/health")
def health():
    return PlainTextResponse("OK")


@app.get("/run_npb")
def run_npb(background_tasks: BackgroundTasks):
    try:
        from npb_app import run_job

        result = run_job()
        count = len(result) if result is not None else 0

        background_tasks.add_task(restart_after_response)

        return PlainTextResponse(f"NPB done: {count} games. Service will restart shortly.")

    except Exception:
        error_text = traceback.format_exc()

        # エラー時もメモリをリセットしたいので再起動する
        background_tasks.add_task(restart_after_response)

        return PlainTextResponse(
            "NPB error:\n" + error_text,
            status_code=500
        )


@app.get("/run_mlb")
def run_mlb(background_tasks: BackgroundTasks):
    try:
        from mlb_app import run_job

        result = run_job()
        count = len(result) if result is not None else 0

        background_tasks.add_task(restart_after_response)

        return PlainTextResponse(f"MLB done: {count} games. Service will restart shortly.")

    except Exception:
        error_text = traceback.format_exc()

        # エラー時もメモリをリセットしたいので再起動する
        background_tasks.add_task(restart_after_response)

        return PlainTextResponse(
            "MLB error:\n" + error_text,
            status_code=500
        )


# 以前 /npb や /mlb で実行していた場合の互換用
@app.get("/npb")
def run_npb_alias(background_tasks: BackgroundTasks):
    return run_npb(background_tasks)


@app.get("/mlb")
def run_mlb_alias(background_tasks: BackgroundTasks):
    return run_mlb(background_tasks)
