# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0764a8c0-384e-41ba-bb2f-edc6eefd9dc1",
# META       "default_lakehouse_name": "Lakehouse_STG",
# META       "default_lakehouse_workspace_id": "72b0dce6-c772-43c1-a4bd-501b8778f6b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "0764a8c0-384e-41ba-bb2f-edc6eefd9dc1"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

list_type ='Daily'
keyVaultURL = 'test'
LakeHousePath ='*******'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import requests
from notebookutils import mssparkutils
from azure.storage.blob import BlobServiceClient
import pandas as pd, numpy as np
import os, uuid
from io import BytesIO
from datetime import datetime
from urllib.parse import urlparse
import csv
import ast

# # Set the variables for the Key Vault

# Retrieve secret
client_id= notebookutils.credentials.getSecret(keyVaultURL, 'MicrosoftGraphClientID')
client_secret = notebookutils.credentials.getSecret(keyVaultURL, 'MicrosoftGraphClientSecret')

print(list_type)

# Authentication details
tenant_id = "jackhenry.onmicrosoft.com"


scope = "https://graph.microsoft.com/.default"


def graph_get(url, token):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()



def azure_upload_df(container=None, dataframe=None, filename=None):
    """
    Upload DataFrame to Azure Blob Storage for given container
    Keyword arguments:
    container -- the container name (default None)
    dataframe -- the dataframe(df) object (default None)
    filename -- the filename to use for the blob (default None)
    
    Function uses following enviornment variables 
    AZURE_STORAGE_CONNECTION_STRING -- the connection string for the account
    OUTPUT -- the ouput folder name
    eg: upload_file(container="test", dataframe=df, filename="test.csv")
    """
    if all([container, len(dataframe), filename]):
        file_path = f"SPList"
        upload_file_path = os.path.join(file_path, f"{filename}.csv")
        connect_str = '*******'
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        print(blob_service_client)
        blob_client = blob_service_client.get_blob_client(
            container=container, blob=upload_file_path
        )
        try:
            output = dataframe.to_csv(index=False, encoding="utf-8")
        except Exception as e:
            pass
            print(e)
        try:
            x = blob_client.upload_blob(output, blob_type="BlockBlob")
            print(x)
        except Exception as e:
            pass
            print(e)


url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token' # URL for OAuth2

data = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope={scope}'  # Content that will be sent for OAuth2 
headers = {'Content-Type': 'application/x-www-form-urlencoded'} # Headers for OAuth2
response = requests.post(url, headers=headers, data=data)

access_token = response.json()["access_token"] # Return Bearer Token


auth_header = {
    'Content-Type': 'application/json',
    'Authorization': "Bearer " + access_token
}

# print(access_token)

# List of system fields to exclude
exclude_system_fields = ["_ComplianceFlags", "_ComplianceTag", "_ComplianceTagWrittenTime", "_ComplianceTagUserId", "AuthorLookupId", "EditorLookupId", "_UIVersionString", "Attachments", "Edit", "ItemChildCount", "FolderChildCount"]


# Function to read SP list metadata from data lake
def fetch_list_metadata(list_type):
    input_csv_path = LakeHousePath+"/Files/SPData/SPLibraryFiles/Utils/"+list_type+".csv"
    try:
        input_df = pd.read_csv(input_csv_path)
        return input_df
    except requests.exceptions.RequestException as e:
        return []

# Function to read SP list mergeFiles from data lake
def fetch_list_mergeFiles(list_type):
    input_csv_path = LakeHousePath+"/Files/SPData/SPLibraryFiles/Merge/"+list_type+".csv"
    try:
        input_df = pd.read_csv(input_csv_path)
        return input_df
    except requests.exceptions.RequestException as e:
        return []

# ---------------------------------------------------------
# Procurement CSV Processing
# ---------------------------------------------------------
def process_procurement_csv(df):

    # Split "Assigned To" into multiple rows
    df["Assigned To"] = df["Assigned To"].str.rstrip(",")

    df_expanded = (
        df.assign(**{"Assigned To": df["Assigned To"].str.split(",")})
        .explode("Assigned To")
    )

    df_expanded["Assigned To"] = df_expanded["Assigned To"].str.strip()
    return df_expanded


# Fetch metadata
metadata_list = fetch_list_metadata(list_type)
metadata_df = pd.DataFrame(metadata_list)

# Fetch SiteMetaData
# MergeFiles_list = fetch_list_mergeFiles(list_type)
# MergeFiles_df = pd.DataFrame(MergeFiles_list)

#error dataframe
error_df = pd.DataFrame(columns=['Site', 'List', 'Error'])

for index, row in metadata_df.iterrows():

    site_path = row["Site"]
    drive_name = row["Drive"]
    subfolder_value = row["SubFolder"] 
    if pd.isna(subfolder_value): 
        subfolder = "" 
    else: 
        subfolder = str(subfolder_value).strip()
    sp_file = row["File"]
    merge = row['Merge'] 

    # get site id
    site_id = None
    site_uri = "https://graph.microsoft.com/v1.0/sites/jackhenry.sharepoint.com:" + site_path
    site_query = requests.get(site_uri, headers=auth_header)
    site_object = site_query.json()
    site_id = site_object['id']

    if not site_id:
        error_row = {'Site': site_path, 'Drive': drive_name, 'Error': 'Site not found'}        
        # Add a new row using .loc[]
        error_df.loc[len(error_df)] = error_row

    drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives?$select=name,id"

    try:
        drives_query = requests.get(drives_url, headers=auth_header)
        drives_object = drives_query.json()
        drives = drives_object["value"]
       #

        drive_id = next((d["id"] for d in drives if d["name"] == drive_name), None)

    except Exception:
       drive_id = None
       error_row = {'Site': site_path, 'Drive': drive_name, 'Error': 'Drive not found'}        
        # Add a new row using .loc[]
       error_df.loc[len(error_df)] = error_row

    #print("drive id : "+drive_id)
    # print("subfolder : "+subfolder)
    try:
            if subfolder:
                #print("inside subfolder : ")
                item_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{subfolder}"
                item_query = requests.get(item_url, headers=auth_header)
                item_object = item_query.json()
                item_id = item_object["id"]

                children_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/children"
                children_query = requests.get(children_url, headers=auth_header)
                children_object = children_query.json()
                children = children_object["value"]

                file_obj = next((f for f in children if f["name"] == sp_file), None)
                download_url = file_obj["@microsoft.graph.downloadUrl"] if file_obj else None
                #print("Sub folder"+children_url+";         "+download_url)
            else:
                file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{sp_file}"
                print(file_url)
                file_query = requests.get(file_url, headers=auth_header)
                file_obj = file_query.json()
                download_url = file_obj["@microsoft.graph.downloadUrl"]
                #print("Sub folder"+file_url+";    "+download_url)
                
            #print(download_url)
            resp = requests.get(download_url)
            resp.raise_for_status()




            folder_path = f"/lakehouse/default/Files/SPData/SPLibraryFiles/Data/"+list_type
            file_path = f"{folder_path}/{sp_file}" 
            with open(file_path, "wb") as f: 
                f.write(resp.content)
    except Exception:
            download_url = None
            error_row = {'Site': site_path, 'Drive': drive_name, 'Error': 'File not found'}        
            # Add a new row using .loc[]
            error_df.loc[len(error_df)] = error_row



       
if len(error_df) > 0:
    current_datetime = datetime.now()
    timestamp_str = current_datetime.strftime("%Y%m%d_%H%M%S")
    error_csv_path = LakeHousePath+"/Files/SPData/SPLibraryFiles/Error/"+list_type+"/"+list_type+"_"+timestamp_str+".csv"             
    error_df.to_csv(error_csv_path, quoting=csv.QUOTE_ALL, index=False )

   




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
