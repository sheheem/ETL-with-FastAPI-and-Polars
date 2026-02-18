from fastapi import FastAPI, UploadFile, File, Depends, Query
from pydantic import validate_call
from sqlalchemy.orm import Session
from sqlalchemy import text
from uuid import uuid4
from fastapi.middleware.cors import CORSMiddleware

import polars as pl
import datetime as dt
import json

from database import SessionLocal, engine, Base
from models.database import UploadData, UploadRawData
from schemas.upload_data import UploadDataResponse, MappingConfig

app = FastAPI()
origins = [
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


def apply_sanitization(df: pl.DataFrame, mapping_config: dict):
    for col in mapping_config["columns"]:
        name = col["original_name"]
        rules = col["sanitization"]

        if rules["null_handling"] == "default":
            df = df.with_columns(
                pl.col(name).fill_null(rules["default_value"])
            )
        
        if rules["is_categorical"]:
            df = df.with_columns(
                pl.col(name).cast(pl.Categorical)
            )

        if rules["is_duplicity"]:
            df = df.unique(subset=[name])

        if rules["is_encrypted"]:
            df = df.with_columns(
                pl.col(name).map_elements(lambda x: "*******")
            )
        
    return df

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

    for row in df.to_dicts():
        db.add(
            UploadRawData(
                upload_id=upload_record.id,
                data=row
            )
        )
    db.commit()
    return upload_record

@app.get("/preview/{upload_id}")
async def preview_data(upload_id: int, page:int = 1, page_size:int = Query(10, le = 100), db: Session = Depends(get_db)):
    upload = db.query(UploadData).get(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    page_size = min(page_size, 100)
    offset = (page - 1) * page_size
    # rows = (
    #     db.query(UploadRawData).filter(UploadRawData.upload_id == upload_id).all()
    # )

    total_count = db.query(UploadRawData).filter(UploadRawData.upload_id == upload_id).count()
    
    # raw_rows = upload.raw_data.offset(offset).limit(page_size)

    raw_rows = db.query(UploadRawData).filter(UploadRawData.upload_id == upload_id).offset(offset).limit(page_size).all()


    df = pl.DataFrame([r.data for r in raw_rows])
    preview_df = apply_sanitization(df, upload.mapping_config)

    return {
        "columns": preview_df.columns,
        "rows": preview_df.to_dicts(),
        "page_size": page_size,
        "total_count": total_count,
        "total_page": (total_count + page_size - 1) // page_size
    }
