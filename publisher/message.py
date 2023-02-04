import datetime
import json
from pydantic import BaseModel

class Sample(BaseModel):
    tag: str
    timestamp: datetime.datetime
    value: float

    