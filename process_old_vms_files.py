import os
import pandas as pd
from pathlib import Path

def process_old_vms_files(folder_path):
    dfs = []
    path = Path(folder_path).joinpath('vms')
    print(f"Processing VMS files from: {path}")
    
    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            file_path = os.path.join(path, filename)
            df = pd.read_csv(file_path)
            df.rename(columns={'JobId': 'Job Id', 'JobStatus': 'Job Status'}, inplace=True)
            df['Source_File'] = filename
            dfs.append(df)
    
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df.drop_duplicates(subset='Job Id', inplace=True)
        merged_df['Job Status'] = merged_df['Job Status'].replace('Manually Frozen', 'On-Hold')
        merged_df = merged_df[['Job Id', 'Job Status']]
        
        output_file_path = Path(folder_path).joinpath('merged', 'old_merged_vms.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_csv(output_file_path, index=False)
        print(f"VMS processing done. Output saved to: {output_file_path}")
    else:
        print("No VMS CSV files found or processed.")
