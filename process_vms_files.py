import os
import pandas as pd


def process_vms_files(folder_path):
    dfs = []
    path = folder_path.joinpath('vms')
    print(f"Processing VMS files from: {path}")
    
    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            file_path = os.path.join(path, filename)
            df = pd.read_csv(file_path)
            df['Source_File'] = filename
            dfs.append(df)
    
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df.drop_duplicates(subset='JobId', inplace=True)
        merged_df['JobStatus'] = merged_df['JobStatus'].replace('Manually Frozen', 'On-Hold')
        merged_df = merged_df[['JobName','JobId', 'JobStatus']]
        
        output_file_path = folder_path.joinpath('merged', 'merged_vms.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_csv(output_file_path, index=False)
        print(f"VMS processing done. Output saved to: {output_file_path}")
    else:
        print("No VMS CSV files found or processed.")
