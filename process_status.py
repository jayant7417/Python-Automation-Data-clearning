
import pandas as pd


def process_status(folder_path):
    path1 = folder_path.joinpath('merged').joinpath('merged_vms.csv')
    path2 = folder_path.joinpath('merged').joinpath('job_final.csv')
    print(f"Processing VMS: {path1}")
    print(f"Processing Job Board files from: {path2}")
    
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

    # Check if DataFrames are not empty
    if not df1.empty and not df2.empty:
        # Merge the DataFrames
        merged_df = pd.merge(df1, df2, left_on='JobId', right_on='External Job Posting Id', how='outer')
        
        merged_df = merged_df[['JobId', 'JobStatus' ,'Job Status']]
        
        merged_df['result'] = merged_df['JobStatus'] == merged_df['Job Status']
        
        merged_df = merged_df[merged_df['result'] == False]
        merged_df = merged_df.dropna(subset=['Job Status'])
        merged_df = merged_df.dropna(subset=['JobStatus'])
        merged_df = merged_df.sort_values(by='Job Status',ascending=True)
        
        
        # Specify the path to save the merged CSV file
        output_file_path = folder_path.joinpath('result', 'Status.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the merged DataFrame to a new CSV file
        merged_df.to_csv(output_file_path, index=False)
        print(f"Processing done. Output saved to: {output_file_path}")
    else:
        print("One or both of the input CSV files are empty.")
