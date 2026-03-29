import os
import shutil
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import json

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


@app.post("/upload")
async def upload(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    uploaded_info = []

    for file in files:
        try:
            file_path = UPLOAD_DIR / file.filename
            with file_path.open("wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

            uploaded_info.append(str(file_path))
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Could not save {file.filename}: {e}"
            )
        finally:
            await file.close()

        # uploaded_info.append(
        #     {
        #         "filename": file.filename,
        #         "content_type": file.content_type,
        #         "size": file.size,
        #     }
        # )
    print("done")
    if files[0].filename.endswith(".CEL"):
        return {
            "status": "success",
            "message": f"Successfully received {len(files)} files.",
            "data": uploaded_info,
            "type": "commandConsole",
        }
    elif files[0].filename.endswith(".idat"):
        return {
            "status": "success",
            "message": f"Successfully received {len(files)} files.",
            "data": uploaded_info,
            "type": "v3",
        }


@app.get("/pipeline")
def get_pipeline():
    temp = json.load(open("./test-execution.json", "r"))
    return JSONResponse(temp)
