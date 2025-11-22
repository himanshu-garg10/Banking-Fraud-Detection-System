# api_server.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pandas as pd
import os, glob
from typing import Any

app = FastAPI()

BASE = "output"

def read_latest_parquet(folder: str) -> pd.DataFrame:
    # read all parquet files under folder and return combined DF (safe)
    files = []
    for root, _, fs in os.walk(folder):
        for f in fs:
            if f.endswith(".parquet"):
                files.append(os.path.join(root, f))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True, sort=False)
    return df

@app.get("/")
def root():
    return {"status": "API running", "message": "Fraud detection backend active"}

@app.get("/latest")
def latest(limit: int = 50) -> Any:
    df = read_latest_parquet(os.path.join(BASE, "final"))
    if df.empty:
        return JSONResponse(content={"rows": []})
    # sort by ts/timestamp if present
    time_col = None
    for c in ["ts", "timestamp", "reported_ts"]:
        if c in df.columns:
            time_col = c
            break
    if time_col:
        df = df.sort_values(by=time_col, ascending=False)
    result = df.head(limit).to_dict(orient="records")
    return JSONResponse(content={"rows": result})

@app.get("/aggregates")
def aggregates(limit: int = 200) -> Any:
    df = read_latest_parquet(os.path.join(BASE, "aggregates"))
    if df.empty:
        return JSONResponse(content={"rows": []})
    result = df.head(limit).to_dict(orient="records")
    return JSONResponse(content={"rows": result})

@app.get("/health")
def health():
    return {"ok": True}
