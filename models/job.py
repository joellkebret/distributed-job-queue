from pydantic import BaseModel

class JobRequest(BaseModel):
    filename: str