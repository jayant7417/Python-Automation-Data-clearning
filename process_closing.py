import pandas as pd


def process_closing(folder_path):

    path1 = folder_path.joinpath('merged').joinpath('old_merged_vms.csv')
    path2 = folder_path.joinpath('merged').joinpath('merged_vms.csv')
    path3 = folder_path.joinpath('merged').joinpath('job_final.csv')
    print(f"Processing VMS: {path1}")
    print(f"Processing Job Board files from: {path2}")
    print(f"Processing old VMS: {path3}")
    
    # Read the CSV files into DataFrames
    try:
        df1 = pd.read_csv(path1)
    except FileNotFoundError:
        print(f"File not found: {path1}")
        return
    
    try:
        df2 = pd.read_csv(path2)
    except FileNotFoundError:
        print(f"File not found: {path2}")
        return
    
    try:
        df3 = pd.read_csv(path3)
    except FileNotFoundError:
        print(f"File not found: {path3}")
        return
    print("posting")
    
    
    # Process remaining job IDs
    if not df1.empty and not df2.empty and not df3.empty:
        merged_df = pd.merge(df1, df2, left_on='Job Id', right_on='JobId', how='outer')
        
        merged_df = merged_df[['Job Id', 'JobStatus' ,'JobId']]
        
        merged_df = merged_df.dropna(subset=['Job Id'])
        merged_df = merged_df[merged_df['JobId'].isna()]
        
        merged_df0 = merged_df[['Job Id']]
        
        merged_df1 = pd.merge(merged_df0,df3, left_on='Job Id', right_on='External Job Posting Id', how='outer')
        
        merged_df1 = merged_df1[['Job Id' ,'External Job Posting Id']]
        merged_df1 = merged_df1.dropna(subset=['Job Id'])
        merged_df1 = merged_df1.dropna(subset=['External Job Posting Id'])
        
        merged_df1 = merged_df1[['External Job Posting Id']].astype('int64')
        '''
        
        merged_df = merged_df[merged_df['result'] == False]
        
        merged_df = merged_df.dropna(subset=['JobStatus'])
        merged_df = merged_df.sort_values(by='Job Status',ascending=True)
        '''
        
        # Specify the path to save the merged CSV file
        output_file_path = folder_path.joinpath('result', 'closing.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the merged DataFrame to a new CSV file
        merged_df1.to_csv(output_file_path, index=False)
        print(f"Processing done. Output saved to: {output_file_path}")
    else:
        print("One or both of the input CSV files are empty.")