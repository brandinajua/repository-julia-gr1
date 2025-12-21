from __future__ import annotations

import io
import time
from typing import Any, Dict

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel

from .core import summarize_dataset, missing_table, compute_quality_flags

app = FastAPI(title="eda-cli API", version="0.1.0")


class QualityRequest(BaseModel):
    n_rows: int
    n_cols: int
    missing_share: float


class QualityResponse(BaseModel):
    ok_for_model: bool
    quality_score: float
    latency_ms: float
    flags: Dict[str, Any]


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/quality", response_model=QualityResponse)
def quality(req: QualityRequest) -> QualityResponse:
    t0 = time.perf_counter()

    # Простая демо-логика: можно менять как хочешь
    flags: Dict[str, Any] = {
        "too_few_rows": req.n_rows < 20,
        "too_many_columns": req.n_cols > 200,
        "too_many_missing": req.missing_share > 0.3,
    }

    ok_for_model = not (flags["too_few_rows"] or flags["too_many_columns"] or flags["too_many_missing"])

    # quality_score: 1.0 если всё ок, иначе чуть ниже
    quality_score = 1.0 if ok_for_model else 0.6

    latency_ms = (time.perf_counter() - t0) * 1000.0
    return QualityResponse(
        ok_for_model=ok_for_model,
        quality_score=quality_score,
        latency_ms=latency_ms,
        flags=flags,
    )


@app.post("/quality-from-csv", response_model=QualityResponse)
async def quality_from_csv(file: UploadFile = File(...)) -> QualityResponse:
    t0 = time.perf_counter()

    try:
        raw = await file.read()
        df = pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read CSV: {e}")

    if df.shape[0] == 0 or df.shape[1] == 0:
        raise HTTPException(status_code=400, detail="Empty CSV")

    summary = summarize_dataset(df)
    missing_df = missing_table(df)

    # ВАЖНО: у тебя compute_quality_flags требует (df, summary, missing_df)
    flags = compute_quality_flags(df, summary, missing_df)

    # Нормализуем numpy.bool_ → bool, чтобы JSON был чистый
    flags_json: Dict[str, Any] = {}
    for k, v in flags.items():
        if hasattr(v, "item"):
            try:
                flags_json[k] = v.item()
            except Exception:
                flags_json[k] = v
        else:
            flags_json[k] = v

    ok_for_model = not bool(flags_json.get("too_many_missing", False)) and not bool(flags_json.get("too_many_columns", False)) and not bool(flags_json.get("too_few_rows", False))
    quality_score = float(flags_json.get("quality_score", 1.0))

    latency_ms = (time.perf_counter() - t0) * 1000.0
    return QualityResponse(
        ok_for_model=ok_for_model,
        quality_score=quality_score,
        latency_ms=latency_ms,
        flags=flags_json,
    )


# ОБЯЗАТЕЛЬНЫЙ "свой" эндпоинт под HW04:
# Возвращает именно флаги качества из HW03
@app.post("/quality-flags-from-csv")
async def quality_flags_from_csv(file: UploadFile = File(...)) -> Dict[str, Any]:
    try:
        raw = await file.read()
        df = pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read CSV: {e}")

    if df.shape[0] == 0 or df.shape[1] == 0:
        raise HTTPException(status_code=400, detail="Empty CSV")

    summary = summarize_dataset(df)
    missing_df = missing_table(df)
    flags = compute_quality_flags(df, summary, missing_df)

    flags_json: Dict[str, Any] = {}
    for k, v in flags.items():
        if hasattr(v, "item"):
            try:
                flags_json[k] = v.item()
            except Exception:
                flags_json[k] = v
        else:
            flags_json[k] = v

    return {"flags": flags_json}
