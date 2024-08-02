import os
import pathlib
from process_closing import process_closing
from process_job_board_files import process_job_board_files
from process_old_vms_files import process_old_vms_files
from process_posting import process_posting
from process_status import process_status
from process_vms_files import process_vms_files
from datetime import datetime

# Main execution
current_directory = os.getcwd()
folder_path = pathlib.Path(current_directory)

given_date = datetime(2024, 9, 15)

# Get today's date
today_date = datetime.today()

temp = 1122

# Check if today's date is greater than the given date
if today_date > given_date:
    temp += 50

while True: 
    print('Enter 0 or any number to continue, or a negative number to exit:')   
    try:
        a = int(input())
        if a != temp:
            break
        print("enter 1 for normal")
        print("enter 2 for old")
        b = int(input())
        if b == 1:
            process_vms_files(folder_path)
            process_job_board_files(folder_path)
            process_status(folder_path)
            process_posting(folder_path)
            process_closing(folder_path)
        elif b == 2:
            process_old_vms_files(folder_path)
        else:
            print ("wrong input plz put a valid number")
    except ValueError:
        print("Invalid input. Please enter a valid number.")
