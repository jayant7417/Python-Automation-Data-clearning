import pandas as pd


def process_posting(folder_path):

    path1 = folder_path.joinpath('merged').joinpath('merged_vms.csv')
    path2 = folder_path.joinpath('merged').joinpath('job_final.csv')
    path3 = folder_path.joinpath('do not post').joinpath('do_not_post_medefis.csv')
    print(f"Processing VMS: {path1}")
    print(f"Processing Job Board files from: {path2}")
    print(f"do not post: {path3}")
    
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
        
        word_to_remove = ['home', 'social', 'extension', 'reserved', 'Home', 'Extension', 'Social']
        mask = ~df1['JobName'].str.contains('|'.join(word_to_remove),case=False)
        df1 = df1[mask]
        
        job_ids = set(df1['JobId'].dropna())
        external_ids = set(df2['External Job Posting Id'].dropna())
        dnt_ids = set(df3['jobId'].dropna())
        
        remaining_ids = job_ids - external_ids
        remaining_ids = remaining_ids - dnt_ids
        remaining_ids_df = pd.DataFrame(list(remaining_ids), columns=['RemainingJobIds'])
        
        # Save the remaining job IDs
        output_file_path = folder_path.joinpath('result', 'Posting.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        remaining_ids_df.to_csv(output_file_path, index=False)
        print(f"Remaining job IDs processing done. Output saved to: {output_file_path}")
    else:
        print("One or more of the input CSV files are empty.")
