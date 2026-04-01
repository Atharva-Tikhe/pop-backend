import os
import re
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
    "http://0.0.0.0:5173"
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

def extract_idat_sample_name(file_stem):
    regex = re.compile(r"(.*)(_Grn|_Red)", flags = re.UNICODE)

    matches = regex.finditer(file_stem)

    groups = []

    for _, match in enumerate(matches, start=1):        
        for _, group in enumerate(match.groups(), start=1):
            groups.append(group)
    
    return groups[0]


async def save_files_and_get_samples(files: List[UploadFile]):
    sample_manifest = {}
    sample_manifest['idats'] = []
    sample_manifest['cel'] = []
    idat_groups = {}

    # files -> [mytest_sample1_Grn.idat, mytest_sample1_Red.idat, mytest_sample2_Red.idat, mytest_sample2_Red.idat]

    for file in files:        
        try:
            file_path = UPLOAD_DIR / file.filename # type: ignore
            with file_path.open('wb') as buffer:
                shutil.copyfileobj(file.file, buffer)


            if file_path.suffix.upper() == '.IDAT':
                print(file_path.stem)
                sample_id = extract_idat_sample_name(file_path.stem)
                print(sample_id)
                if sample_id not in list(idat_groups.keys()):
                    idat_groups[sample_id] = [None, None]

                if "Grn" in file_path.stem:
                    idat_groups[sample_id][0] = str(file_path)
                elif "Red" in file_path.stem:
                    idat_groups[sample_id][1] = str(file_path)            
            elif file_path.suffix.upper() == '.CEL':
                sample_manifest['cel'].append([file_path.stem, file_path])
    
        except Exception as e:
            print(e)
            raise HTTPException(
                status_code=500, detail=f"Could not save {file.filename}: {e}"
            )
        finally:
            await file.close()
        
    sample_manifest['idats'].extend([[sid, paths[0], paths[1]] for sid, paths, in idat_groups.items()])
    
    return sample_manifest



@app.post("/upload")
async def upload(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    manifest = await save_files_and_get_samples(files)

    print(f'manifest: {manifest}')
    return manifest




@app.get("/pipeline")
def get_pipeline():
    temp = json.load(open("./test-execution.json", "r"))
    return JSONResponse(temp)

@app.get("/pipelines/active")
def get_active_pipeline():
    temp = json.load(open("./test-execution.json", "r"))
    temp2 = json.load(open("./test-execution-2.json", "r"))
    return JSONResponse([temp, temp2])

@app.get("/pipeline/{pipeline_id}")
def get_pipeline_by_id(pipeline_id):
    temp = json.load(open('./test-execution.json', 'r'))
    print(temp)
    if temp['pipeline_id'] == pipeline_id:
        return JSONResponse(temp)
    else:
        return HTTPException(404)
    
@app.get("/pipeline/{pipeline_id}/tasks/{task_id}")
def get_task_by_id(pipeline_id, task_id):
    if pipeline_id == '1234':
        temp = json.load(open('./test-execution.json', 'r'))
    else:
        temp = json.load(open('./test-execution-2.json', 'r'))

    if temp['pipeline_id'] == pipeline_id:
        return JSONResponse(temp['tasks'][int(task_id)])
    else:
        return HTTPException(404)