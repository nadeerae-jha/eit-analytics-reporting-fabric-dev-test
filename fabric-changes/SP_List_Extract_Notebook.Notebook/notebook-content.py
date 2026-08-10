# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

list_type ='Every6Hours'
keyVaultURL = 'https://dev-ts-synapse-kv.vault.azure.net/'
LakeHousePath ='abfss://72b0dce6-c772-43c1-a4bd-501b8778f6b7@onelake.dfs.fabric.microsoft.com/0764a8c0-384e-41ba-bb2f-edc6eefd9dc1'


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
keyVaultURL = "https://dev-ts-synapse-kv.vault.azure.net/"
# Retrieve secret
client_id= notebookutils.credentials.getSecret(keyVaultURL, 'MicrosoftGraphClientID')
client_secret = notebookutils.credentials.getSecret(keyVaultURL, 'MicrosoftGraphClientSecret')

#print(list_type)

# Authentication details
tenant_id = "jackhenry.onmicrosoft.com"


scope = "https://graph.microsoft.com/.default"





url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token' # URL for OAuth2

data = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope={scope}'  # Content that will be sent for OAuth2 
headers = {'Content-Type': 'application/x-www-form-urlencoded'} # Headers for OAuth2
response = requests.post(url, headers=headers, data=data)

access_token = response.json()["access_token"] # Return Bearer Token


#print(access_token)

auth_header = {
    'Content-Type': 'application/json',
    'Authorization': "Bearer " + access_token
}

# List of system fields to exclude
exclude_system_fields = ["_ComplianceFlags", "_ComplianceTag", "_ComplianceTagWrittenTime", "_ComplianceTagUserId", "AuthorLookupId", "EditorLookupId", "_UIVersionString", "Attachments", "Edit", "ItemChildCount", "FolderChildCount"]

# Function to fetch columns of a SharePoint list
def fetch_list_columns(site_id, list_id):
    list_columns_uri = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
    #print(list_columns_uri)
    try:
        list_columns_query = requests.get(list_columns_uri, headers=auth_header)
        list_columns_query.raise_for_status()  # Raise an error for bad status codes
        columns = list_columns_query.json().get('value', [])
        return columns
    except requests.exceptions.RequestException as e:
        print(f"An error occurred that could not be resolved: {e}")
        return []

# Function to read SP list metadata from data lake
def fetch_list_metadata(list_type):
    input_csv_path = LakeHousePath+"/Files/SPData/List/Utils/"+list_type+".csv"
    columnorder_csv_path = LakeHousePath+"/Files/SPData/List/Utils/"+list_type+"_ColumnOrder.csv"

    try:
        input_df = pd.read_csv(input_csv_path)

        # input_df = input_df.rename(columns={'list': 'List'}, inplace=True)
        input_df2 = pd.read_csv(columnorder_csv_path,delimiter='|')
        # print(input_df)
        # input_df2 = input_df2.rename(columns={"Columns": "ColumnOrder"}, inplace=True)
        input_df = input_df.merge( input_df2[["List", "ColumnOrder"]], on="List", how="left" )
        input_df['ColumnOrder'] = input_df['ColumnOrder'].fillna('a')
        return input_df
    except requests.exceptions.RequestException as e:
        print(e)
        return []

# Function to read SP list metadata from data lake
def fetch_list_SiteMetaData(list_type):
    input_csv_path = LakeHousePath+"/Files/SPData/List/Utils/"+list_type+"_SiteMetaData.csv"
    try:
        input_df = pd.read_csv(input_csv_path)
        return input_df
    except requests.exceptions.RequestException as e:
        return []

# Function to process columns and create hash arrays
def process_columns(columns, exclude_system_fields,list_name):
    fields_columnname_hash = []
    fields_columns_hash = {}

    column_count = 0
    for column in columns:
        if column['name'] not in exclude_system_fields and not column.get('hidden', False):
            column_count += 1
            columnhash = {
                'ID': column_count,
                'Name': column['name'],
                'DisplayName': column['displayName'],
                'DisplayNameUpdated': None
            }
            fields_columnname_hash.append(columnhash)

    # Group columns by DisplayName and add rank
    columnhash_with_rank = []
    name_to_columns = {}
    for col in fields_columnname_hash:
        if col['DisplayName'] not in name_to_columns:
            name_to_columns[col['DisplayName']] = []
        name_to_columns[col['DisplayName']].append(col)

    for display_name, cols in name_to_columns.items():
        for i, col in enumerate(cols, start=1):
            col['Rank'] = i
            columnhash_with_rank.append(col)

    for col in columnhash_with_rank:
        if col['Rank'] != 1:
            fields_columns_hash[col['Name']] = col['DisplayName'] + str(col['Rank'])
        else:
            fields_columns_hash[col['Name']] = col['DisplayName']

            # Fetch SiteMetaData
    SiteMetaData_list = fetch_list_SiteMetaData(list_type)
    SiteMetaDataAll_df = pd.DataFrame(SiteMetaData_list)

    SiteMetaDataAll_df_filtered= SiteMetaDataAll_df[SiteMetaDataAll_df['List'] == list_name]
            #print('Columns1')
            #print(df.columns)
    #print('fields_columns_hash1****************')
    #print(fields_columns_hash)
    for row_index, row in SiteMetaDataAll_df_filtered.iterrows():
        fields_columns_hash[row["Name"]] = row["NewName"]
    #print('fields_columns_hash2****************')
    #print(fields_columns_hash)
    #for col in fields_columnname_hash:


    # sitemetadata_path = LakeHousePath+"/Files/SPData/Utils/SiteMetaData/"+list_name+".txt"

    # if mssparkutils.fs.exists(sitemetadata_path):
      #  sitemetadata_df = spark.read.format("csv").option("header", "true").load(sitemetadata_path)
       # for field in sitemetadata_df.collect(): 
        #    fields_columns_hash[field['Name']] = field['NewName']
    #else:
     #   print(f"The file '{sitemetadata_path}' does not exist.")

    return fields_columns_hash

# Fetch metadata
metadata_list = fetch_list_metadata(list_type)
metadata_df = pd.DataFrame(metadata_list)
#error dataframe
error_df = pd.DataFrame(columns=['Site', 'List', 'Error'])

for index, row in metadata_df.iterrows():

    site_relative_path = row['Site']
    list_name_filename = row['List'] 

    try:
        list_name = row['List'] 
        list_name = list_name.replace("'", "''").replace("#", "%23").replace("&", "%26").replace("+", "%2b"); 
        metaDataFields = None
        metaDataFields = row['ColumnOrder'] 

        print(list_name)

        # get site id
        site_id = None
        site_uri = "https://graph.microsoft.com/v1.0/sites/jackhenry.sharepoint.com:" + site_relative_path
        site_query = requests.get(site_uri, headers=auth_header)
        site_object = site_query.json()
        site_id = site_object['id']

        if not site_id:
            error_row = {'Site': site_relative_path, 'List': list_name_filename, 'Error': 'Site not found'}        
            # Add a new row using .loc[]
            error_df.loc[len(error_df)] = error_row



        list_id = None
        list_uri = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$filter=displayName eq '{list_name}'"
        try:
            list_query = requests.get(list_uri, headers=auth_header)
            list_query.raise_for_status()  # Raise an error for bad status codes
            list_object = list_query.json().get('value', [])

            if not list_object:
                error_row = {'Site': site_relative_path, 'List': list_name_filename, 'Error': 'Site not found'}        
                # Add a new row using .loc[]
                error_df.loc[len(error_df)] = error_row
            
            if list_object:
                list_id = list_object[0].get('id', None)

                columns = fetch_list_columns(site_id, list_id)


                fields_columns_hash1 = process_columns(columns, exclude_system_fields,list_name)


                field_list = ",".join(fields_columns_hash1.keys())
                #fields_data = None





                list_uri = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?expand=fields($select={field_list})&$"+"top=1000"
                #print(list_uri)
                nextlink = None
                list_objects = []  # Initialize list_objects

                while True:
                    list_query = requests.get(url=list_uri, headers=auth_header)

                    list_objects += list_query.json().get('value', [])
                    nextlink = None
                    nextlink = list_query.json().get('@odata.nextLink')
                    # print(nextlink)
                    if nextlink is None:
                        break
                    else:
                        list_uri = nextlink


                fields_data = ([item['fields'] for item in list_objects])            

                # Convert to DataFrame

                df = pd.DataFrame(fields_data)

                df = df.convert_dtypes()

                object_df = df.select_dtypes(include='object')


                for column_name in df:
                    if pd.api.types.is_object_dtype(df[column_name]):
                        column_index = df.columns.get_loc(column_name)
                        #print('*****1 '+list_name_filename)
                        #for current_value in df[column_name]:
                        for row_index, row in df.iterrows():
                            
                            current_value =  row[column_name]

                            value_text = None
                            if isinstance(current_value, list):


                                for index, value in enumerate(current_value):
                                    value_temp = None
                                    if('Label' in value):
                                        value_temp = (value['Label'])
                                    elif('LookupValue' in value):
                                        value_temp = (value['LookupValue'])
                                    elif('Url' in value):
                                        value_temp = (value['Description'])+(value['Url'])
                                    else:
                                        value_temp = value
                                    if(len(current_value)>1):
                                        if(value_text is None): value_text = value_temp 
                                        else: value_text = value_text+';'+value_temp
                                    else:
                                        value_text = value_temp


                            else:
                                if(current_value == '' or pd.isnull(current_value)):
                                    value_text = ""
                                else:
                                    if('Label' in current_value):
                                        value_text = (current_value['Label'])
                                    elif('LookupValue' in  current_value):
                                        value_text  = (current_value['LookupValue'])
                                    elif('Url' in current_value):
                                        value_text  = (current_value['Description'])+(current_value['Url'])
                                #print('Start**************************')

                            #print(f"RowIndex: {row_index}, column_index: {column_index}")
                            #df[column_name][index] = value_text
                            if value_text is not None:
                                value_text = value_text.replace('"', '')
                            
                            df.iloc[row_index, column_index] = value_text
                            #print(df.iloc[row_index, column_index])
                            #print('End**************************')

                
                
                #print('df original')

                #print(df)


                #print('df renamed')

                #print(fields_columns_hash1)

                df = df.rename(columns=fields_columns_hash1)

                
                #df_sorted = df.sort_index(axis=1, ascending=True)

                df_list= df.columns.tolist()


                #print('Columns2')
                #print(df.columns)
                #print('columns list')
                #print(columns)
                #print(fields_columns_hash1)
                #df.columns = [fields_columns_hash1[col] if col in fields_columns_hash1 else col for col in df.columns]
                df.columns = df.columns.str.replace(r"_x0020_", " ")

                df.rename(columns={'id': 'ID'}, inplace=True)
                #df = df.sort_values(by='ID', ascending=True)

                if(len(metaDataFields)>1):
                    metaDataFields = metaDataFields.split('|$|')


                if len(metaDataFields) <= 1:
                    if '@odata.etag' in df_list:
                        df_list.remove('@odata.etag')
                    if 'id' in df_list:
                        index = df_list.index('id')
                        df_list[index] = "ID"
                    metaDataFields= df_list


                #print('metaDataFields')
                #print(metaDataFields)
                #pd.options.mode.copy_on_write = True 
                mapping_df = pd.DataFrame(columns=metaDataFields)
                #print('data frame')
                #print(mapping_df.columns)

                for i in list(mapping_df):
                    if i not in list(df):
                        df[i] = np.nan


                final_output_df = df[metaDataFields]


                #for col in final_output_df.columns:
                #    try:
                #        final_output_df[col] = pd.to_datetime(final_output_df[col], format='mixed')
                #        print(final_output_df[col])
                #    except (ValueError, TypeError):
                #        pass

                


                output_csv_path = LakeHousePath+"/Files/SPData/List/Data/"+list_type+"/"+list_name_filename+".csv"

                final_output_df = final_output_df.replace(r"[\n\r]", " ", regex=True)

               # final_output_df = final_output_df.replace('"', '', regex=True)


                string_cols = final_output_df.select_dtypes(include=["object", "string"]).columns
                final_output_df[string_cols] = final_output_df[string_cols].fillna("")

                #final_output_df.columns = final_output_df.columns.str.replace('"', '', regex=False)
                #print(list(final_output_df.columns))


                header = [f'"{c}"' for c in final_output_df.columns]

                final_output_df.to_csv(
                    output_csv_path,
                    index=False,
                    quoting=csv.QUOTE_ALL,
                    quotechar='"',
                    lineterminator="\r\n"
                )





        except requests.exceptions.RequestException as e:
            error_msg = f"Error while processing list '{list_name_filename}' on site '{site_relative_path}': {e}"
            print(error_msg)
            error_row = {'Site': site_relative_path, 'List': list_name_filename, 'Error': error_msg}
            error_df.loc[len(error_df)] = error_row

    except Exception as e:
        # Catch any unexpected errors for this list and include list name in the log
        error_msg = f"Unexpected error for list '{list_name_filename}' on site '{site_relative_path}': {e}"
        print(error_msg)
        error_df.loc[len(error_df)] = {'Site': site_relative_path, 'List': list_name_filename, 'Error': error_msg}

    
if len(error_df) > 0:
    current_datetime = datetime.now()
    timestamp_str = current_datetime.strftime("%Y%m%d_%H%M%S")
    error_csv_path = LakeHousePath+"/Files/SPData/List/Error/"+list_type+"/"+list_type+"_"+timestamp_str+".csv"             
    error_df.to_csv(error_csv_path, quoting=csv.QUOTE_ALL, index=False )

   




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
