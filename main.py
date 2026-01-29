from fastapi import FastAPI, UploadFile, File, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from uuid import uuid4

import polars as pl
import datetime as dt
import json

from database import SessionLocal, engine, Base
from models.database import UploadData
from schemas.upload_data import UploadDataResponse

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

Base.metadata.create_all(bind=engine)

def build_mapping_config(df: pl.DataFrame, file_id: str):
    columns = []

    for col, dtype in df.schema.items():
        columns.append({
            "id": col,
            "original_name": col,
            "mapping_name": col.lower().replace(" ", "_"),
            "data_type": str(dtype),
            "sanitization": {
                "is_categorical": False,
                "is_duplicity": False,
                "is_encrypted": False,
                "null_handling": "none",
                "default_value": ""
            }
        })

    return {
        "file_id": file_id,
        "columns": columns
    }

@app.post("/upload", response_model=UploadDataResponse)
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    contents = await file.read()
    if file.filename.endswith(".csv"):
        df = pl.read_csv(contents)
    elif file.filename.endswith(".xlsx"):
        df = pl.read_excel(contents)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file format")

    file_id = str(uuid4())
    mapping_config = build_mapping_config(df, file_id)

    upload_record = UploadData(
        filename=file.filename,
        mapping_config=mapping_config,
        operator_pipeline=None
    )

    db.add(upload_record)
    db.commit()
    db.refresh(upload_record)

    return upload_record
