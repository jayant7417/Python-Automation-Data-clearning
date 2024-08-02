# Function to process Job Board files
import os
import pandas as pd


def process_job_board_files(folder_path):
    dfs = []
    path = folder_path.joinpath('job board')
    print(f"Processing Job Board files from: {path}")
    
    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            file_path = os.path.join(path, filename)
            df = pd.read_csv(file_path)
            df['Source_File'] = filename
            dfs.append(df)
    
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df.drop_duplicates(subset='External Job Posting Id', inplace=True)
        
        # Cleaning 'External Job Posting Id' column
        replace_patterns = ['(48 hours)', '(48hours)', '(48 hrs)', '(48hrs)', "'",'(48 HOURS)', '(48HOURS)', '(48 HRS)', '(48HRS)','(48 Hours)', '(48Hours)', '(48 Hrs)', '(48Hrs)', "''", '"']
        for pattern in replace_patterns:
            merged_df['External Job Posting Id'] = merged_df['External Job Posting Id'].str.replace(pattern, '', regex=False)
        
        merged_df = merged_df[['External Job Posting Id', 'Job Status']]
        
        merged_df['External Job Posting Id'] = pd.to_numeric(merged_df['External Job Posting Id'],errors='coerce')
        
        merged_df = merged_df.dropna()
        
        output_file_path = folder_path.joinpath('merged', 'job_final.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_csv(output_file_path, index=False)
        print(f"Job Board processing done. Output saved to: {output_file_path}")
    else:
        print("No Job Board CSV files found or processed.")
