import io
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from datetime import datetime, timedelta, date
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.http import MediaIoBaseDownload

SERVICE_ACCOUNT_FILE = "web-server-for-ops-9b561c66b622.json"  # Update with your JSON file
SCOPES = ["https://www.googleapis.com/auth/drive"]

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build("drive", "v3", credentials=creds)

# Load Files From The Google Drive -- AJKED Flask App (Drive Folder Name) -- AJKED Files
def load_parqute(parqute_file_id):
    # Replace with your actual Google Drive file ID
    file_id = parqute_file_id
    
    # Read file directly from Google Drive
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    # Convert Bytes to Parquet DataFrame
    file_stream.seek(0)
    df = pq.read_table(file_stream).to_pandas()
    return df

def round_to_30_minutes(time_value):
    if pd.isna(time_value) or time_value.strip() == "":  # Handle NaN or empty values
        return None

    # Convert to datetime, handling cases with or without microseconds
    try:
        time_obj = datetime.strptime(time_value, "%H:%M:%S.%f")
    except ValueError:
        try:
            time_obj = datetime.strptime(time_value, "%H:%M:%S")
        except ValueError:
            return None  # If it doesn't match, return None

    # Round to nearest 30-minute interval
    minutes = (time_obj.minute // 30) * 30
    if time_obj.minute % 30 != 0 or time_obj.second > 0 or time_obj.microsecond > 0:
        minutes += 30

    rounded_time = time_obj.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minutes)

    return rounded_time.strftime("%H:%M:%S")  # Return formatted time string

#Data Transformation Layer
def transformed_data():
    SM_Data = load_parqute("11_uiRWYMpc1fg6Rjaw7ujgdKvteXX2lu").drop(columns=["site_month_KEY"]).rename(columns={"site_month_KEY_": "site_month_KEY"})
    SM_Data["MonthYear"], SM_Data["Date"], SM_Data["Times"] = pd.to_datetime(SM_Data["MonthYear"], errors="coerce"), pd.to_datetime(SM_Data["Date"], errors="coerce"), SM_Data["Times"].astype(str)
    SM_Data["Times"] = SM_Data["Times"].apply(round_to_30_minutes)

    Site_Info = load_parqute("14cp-HAWuOICpxIPI6qQTZStcwFaYi5i_")
    Site_Info["inst_date"] = pd.to_datetime(Site_Info["inst_date"], errors="coerce")
    
    Max_MDI = load_parqute("1qR1SeVctMe-KyPybbMYt9JC5Iy8VqSHy").rename(columns={"site_month_KEY_": "site_month_KEY"})
    Max_MDI_Per_Year = Max_MDI.groupby('SiteID')['Max_MDI'].sum().reset_index().rename(columns={"Max_MDI": "Max_MDI_Per_Year"})
    Max_MDI_Per_Month = Max_MDI.groupby(['SiteID'])['Max_MDI'].max().reset_index().rename(columns={"Max_MDI": "Max_MDI_Per_Month"})  
    # Max_MDI = Max_MDI.groupby('SiteID')['Max_MDI'].sum().reset_index()

    merged_df = pd.merge(pd.merge(pd.merge(SM_Data, Site_Info, on="SiteID", how="outer"), Max_MDI_Per_Year, on="SiteID", how="outer"), Max_MDI_Per_Month, on="SiteID", how="outer")
    merged_df['no_of_resets'] = ((datetime.today().year * 12) + datetime.today().month) - ((merged_df['inst_date'].dt.year * 12) + merged_df['inst_date'].dt.month)
    print(merged_df.info())
    final_df = merged_df[['SiteID', 'Batch','BillRef#','Date','Times','DISCO','Circle','Division','SubDivision','Meter_Number','kWh_Reading','kWh_Units','MDI','ct_ratio','pt_ratio','site_month_KEY','inst_date','Max_MDI_Per_Year','Max_MDI_Per_Month','kvarh_reading','kVARh Units','no_of_resets','Peak/OffPeak','SDO','peak_reading','off_peak_reading','bd_zy']]
    return final_df

#Billing Report Data
def billing_report():
    billing_data = transformed_data()
    
    # Group by "Category" and apply different aggregations
    billing_data = billing_data.groupby(["SiteID", "SDO","BillRef#","Batch","Date"]).agg({
        "kWh_Reading": ["min", "max"],
        "kWh_Units" : "sum",
        "kvarh_reading" : ["min","max"],
        "MDI" : "max",
        "ct_ratio" : "max",
        "pt_ratio" : "max",
        "Max_MDI_Per_Year" : "max",
        "Max_MDI_Per_Month" : "max",
        "no_of_resets" : "max"
    }).reset_index()
    
    billing_data.columns = ['_'.join(col).strip('_') for col in billing_data.columns]
    # Create a new column name mapping
    column_mapping = {}
    for col in billing_data.columns:
        if col == "kWh_Reading_min":
            column_mapping[col] = "kWh_Reading_Start"  # Rename min to Start
        elif col == "kWh_Reading_max":
            column_mapping[col] = "kWh_Reading_End"  # Rename max to End
        elif col == "kvarh_reading_min":
            column_mapping[col] = "kvarh_reading_Start"  # Rename max to End
        elif col == "kvarh_reading_max":
            column_mapping[col] = "kvarh_reading_End"  # Rename max to End
        else:
            column_mapping[col] = col.replace("_min", "").replace("_max", "").replace("_sum", "")
    
    # Apply the new column names
    billing_data.rename(columns=column_mapping, inplace=True)
    return billing_data

#SDO Report Data
def sdo_report():
    sdo_data = transformed_data()
    
    # Group by "Category" and apply different aggregations
    sdo_data = sdo_data.groupby(["SiteID","SDO","BillRef#","Batch","Date","Meter_Number"]).agg({
        "kWh_Reading": ["min", "max"],
        "kWh_Units" : "sum",
        "kvarh_reading" : ["min","max"],
        "MDI" : "max",
        "ct_ratio" : "max",
        "pt_ratio" : "max",
        "Max_MDI_Per_Year" : "max",
        "Max_MDI_Per_Month" : "max",
        "no_of_resets" : "max",
        "peak_reading" : "max",
        "off_peak_reading" : "max",
        "bd_zy" : "max"
    }).reset_index()
    
    sdo_data.columns = ['_'.join(col).strip('_') for col in sdo_data.columns]
    # Create a new column name mapping
    column_mapping = {}
    for col in sdo_data.columns:
        if col == "kWh_Reading_min":
            column_mapping[col] = "kWh_Reading_Start"  # Rename min to Start
        elif col == "kWh_Reading_max":
            column_mapping[col] = "kWh_Reading_End"  # Rename max to End
        elif col == "kvarh_reading_min":
            column_mapping[col] = "kvarh_reading_Start"  # Rename max to End
        elif col == "kvarh_reading_max":
            column_mapping[col] = "kvarh_reading_End"  # Rename max to End
        else:
            column_mapping[col] = col.replace("_min", "").replace("_max", "").replace("_sum", "")
    
    # Apply the new column names
    sdo_data.rename(columns=column_mapping, inplace=True)
    return sdo_data

# Function to round up to the next 30-minute interval
def round_up_to_30_minutes(t):
    if pd.isna(t):
        return None  # Skip NaT values
    dt = datetime.combine(datetime.today(), t)  # Convert to full datetime
    minutes = (dt.minute // 30 + 1) * 30  # Compute next 30-min interval
    rounded_dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minutes)
    return rounded_dt.time()  # Return only the time part

#Interval Data
def interval_data():
    interval_data = transformed_data()
    
    # Get the first day of the current month
    first_day_of_month = date.today().replace(day=1)

    # Filter data from 1st of this month till today
    interval_data = interval_data[interval_data["Date"] >= pd.to_datetime(first_day_of_month)]

    # Group by "Category" and apply different aggregations
    interval_data = interval_data.groupby(["SiteID","SDO",'BillRef#',"Date","Times","Peak/OffPeak","DISCO","Circle","Division","SubDivision"]).agg({
        "kWh_Units" : "sum"
    }).reset_index()
    return interval_data

# Store Data On The Drive -- AJKED Flask App Data
# Google Drive Folder ID
# FOLDER_ID = "1QSpnn3q8FeQqyuCAimcSXwkFh8DOciUn"
# FILE_NAME = "final_ajked_df.parquet"
def save_data(data, FOLDER_ID, FILE_NAME):
    
    # Convert DataFrame to Parquet in memory (BytesIO)
    parquet_buffer = io.BytesIO()
    data.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)  # Reset buffer position

    # Step 1: Search for the existing file
    query = f"name='{FILE_NAME}' and '{FOLDER_ID}' in parents and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])

    if files:
        file_id = files[0]["id"]
        print(f"Existing file found. Updating file ID: {file_id}")

        # Step 2: Update the existing file (keep same file ID)
        media = MediaIoBaseUpload(parquet_buffer, mimetype="application/octet-stream", resumable=True)
        updated_file = drive_service.files().update(fileId=file_id, media_body=media).execute()

        print(f"File updated successfully! File ID remains the same!")
    else:
        print("File not found. Uploading as a new file.")

        # Step 3: Upload the file if it does not exist
        file_metadata = {"name": FILE_NAME, "parents": [FOLDER_ID]}
        media = MediaIoBaseUpload(parquet_buffer, mimetype="application/octet-stream")
        new_file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

        print(f"New file uploaded successfully!")

if __name__ == "__main__":
    billing_report = billing_report()
    save_data(billing_report, "1QSpnn3q8FeQqyuCAimcSXwkFh8DOciUn", "billing_data.parquet")

    sdo_report = sdo_report()
    save_data(sdo_report, "1QSpnn3q8FeQqyuCAimcSXwkFh8DOciUn", "sdo_data.parquet")

    interval_data = interval_data()
    save_data(interval_data, "1QSpnn3q8FeQqyuCAimcSXwkFh8DOciUn", "interval_data.parquet")