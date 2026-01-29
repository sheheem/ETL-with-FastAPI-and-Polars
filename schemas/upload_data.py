from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class SanitizeConfig(BaseModel):
    is_categorical: bool
    is_duplicity: bool
    is_encrypted: bool
    null_handling: str
    default_value: str


class ColumnMapping(BaseModel):
    id: str
    original_name: str
    mapping_name: str
    data_type: str
    sanitization: SanitizeConfig

class MappingConfig(BaseModel):
    file_id: str
    columns: List[ColumnMapping]

class UploadDataCreate(BaseModel):
    filename: str
    mapping_config: Optional[MappingConfig] = None
    operator_pipeline: Optional[dict] = None

class UploadDataResponse(BaseModel):
    id: int
    filename: str
    mapping_config: Optional[MappingConfig] = None
    operator_pipeline: Optional[dict] = None
    create_at: datetime
    update_at: datetime

    model_config = {
        "from_attributes": True
    }