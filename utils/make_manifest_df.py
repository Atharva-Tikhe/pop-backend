from models import flatManifest
import pandas as pd
import json

def manifest_to_df(manifest: list[flatManifest]) -> pd.DataFrame:
    rows = []
    for sample in manifest:
        rows.append(json.loads(sample.model_dump_json()))
    
    return pd.DataFrame(rows)