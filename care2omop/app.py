from fastapi import FastAPI, Response, status
from pydantic import BaseModel
import uvicorn
import pandas as pd
# from utils import DataTransformation
from main import CARE2OMOP

app = FastAPI()

class UserRequest(BaseModel):
    username: str
    password: str
    endpoint: str

@app.get("/")
async def test():
    return {"CARE2OMOP service":"Running"} 

@app.post("/care2omop/")
async def run_etl_workflow(user_request: UserRequest):

    configuration_file = {
        "TRIPLESTORE_URL": user_request.endpoint,
        "TRIPLESTORE_USERNAME": user_request.username,
        "TRIPLESTORE_PASSWORD": user_request.password
    }
    
    CARE2OMOP(configuration_file)
    
    return {"Structural Transformation":"Finish"}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000)