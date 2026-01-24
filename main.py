from fastapi import FastAPI, UploadFile, File, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

import polars as pl
import datetime as dt
import json

from database import SessionLocal, engine, Base
from models import User, Product

app = FastAPI()



# df = pl.DataFrame(
#     {
#         "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
#         "birthdate": [
#             dt.date(1997, 1, 10),
#             dt.date(1985, 2, 15),
#             dt.date(1983, 3, 22),
#             dt.date(1981, 4, 30),
#         ],
#         "weight": [57.9, 72.5, 53.6, 83.1],  # (kg)
#         "height": [1.56, 1.77, 1.65, 1.75],  # (m)
#     }
# )


# new_table = df.with_columns(
#     birth_year = pl.col("birthdate").dt.year(),
#     bmi = pl.col("weight")/pl.col("height")**2
# )

# print(new_table, "new table")

# df.write_csv("assets/data/output.csv")

# BMI = pl.col("weight") / (pl.col("height") ** 2)
# result = df.select(
#     pl.col("name"),
#     pl.col("birthdate"),
#     BMI.alias("BMI")
# )
# print(result, "BMI")
# print(df)

# print(df.select(pl.col("weight").mean()))

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

Base.metadata.create_all(bind=engine)

@app.post("/create")
def create_user(name: str, email: str, db: Session = Depends(get_db)):
    new_user = User(name=name, email=email)
    db.add(new_user)
    db.commit()

    return {"id": new_user.id, "name": new_user.name, "email": new_user.email}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    contents = await file.read()
    if file.filename.endswith(".csv"):
        df = pl.read_csv(contents)
    elif file.filename.endswith(".xlsx"):
        df = pl.read_excel(contents)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file format")
    
    rows = df.to_dicts()

    for row in rows:
        new_record = UploadData(
            filename=file.filename,
            data=json.dumps(row)
        )
        db.add(new_record)
    
    db.commit()
    # df = pl.read_csv(file.file)
    # print(df)
    return {
        "rows": df.shape[0],
        "columns": df.columns
    }
