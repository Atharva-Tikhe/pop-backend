import os
import re
import shutil
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import json

UPLOAD_DIR = Path("uploads")

class UploadValidator:
    def __init__(self, files: List[UploadFile]):
        self.files = files
        self.sample_manifest = {}
        self.flat = []
    
    
    @staticmethod
    def extract_idat_sample_name(file_stem):
        regex = re.compile(r"(.*)(_Grn|_Red)", flags = re.UNICODE)

        matches = regex.finditer(file_stem)

        groups = []

        for _, match in enumerate(matches, start=1):        
            for _, group in enumerate(match.groups(), start=1):
                groups.append(group)
        
        return groups[0]


    async def save_files_and_get_samples(self):
        self.sample_manifest['idats'] = []
        self.sample_manifest['cel'] = []
        idat_groups = {}

        # files -> [mytest_sample1_Grn.idat, mytest_sample1_Red.idat, mytest_sample2_Red.idat, mytest_sample2_Red.idat]

        for file in self.files:        
            try:
                file_path = UPLOAD_DIR / file.filename # type: ignore
                with file_path.open('wb') as buffer:
                    shutil.copyfileobj(file.file, buffer)

                suffix = file_path.suffix.upper()

                if suffix == '.IDAT':
                    
                    sample_id = UploadValidator.extract_idat_sample_name(file_path.stem)
                    
                    if sample_id not in list(idat_groups.keys()):
                        idat_groups[sample_id] = {'Grn': None, 'Red': None}

                    if file_path.stem.endswith('Grn'):
                        idat_groups[sample_id]['Grn'] = str(file_path)
                    elif file_path.stem.endswith('Red'):
                        idat_groups[sample_id]['Red'] = str(file_path)            
                elif suffix == '.CEL':
                    self.sample_manifest['cel'].append([file_path.stem, str(file_path)])
        
            except Exception as e:
                print(e)
                raise HTTPException(
                    status_code=500, detail=f"Could not save {file.filename}: {e}"
                )
            finally:
                await file.close()
        
        for sample_id, channels in idat_groups.items():
            
            if not channels["Grn"] or not channels["Red"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Incomplete IDAT pair for sample {sample_id}"
                )

            self.sample_manifest['idats'].append({
                "sample_name": sample_id,
                "grn_path": channels["Grn"],
                "red_path": channels["Red"]
            })
        
        return self.sample_manifest
    
    def flatten_manifest(self):
        
        for s in self.sample_manifest['idats']:
            self.flat.append({
                "sample_name": s['sample_name'],
                'file_type': 'idat',
                'platform': 'illumina',
                'files' : {'Grn': s['grn_path'], 'Red': s['red_path']},
                "priority": 'normal',
                'analyst_name': '-'
            })
        
        for s in self.sample_manifest['cel']:
            self.flat.append({
                "sample_name": s[0],
                'file_type': 'cel',
                'platform': 'affymetrix',
                'files' : s[1],
                "priority": 'normal',
                'analyst_name': '-'
            })
    
        return self.flat

        


