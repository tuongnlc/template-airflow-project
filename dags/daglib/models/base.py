from pydantic import BaseModel


class Metadata(BaseModel):
    name: str

class BaseConfig(BaseModel):
    kind: str
    metadata: Metadata
