# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

list_type ='Daily'
keyVaultURL = 'test'
LakeHousePath ='*****'


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

print(list_type)

# Authentication details
tenant_id = "jackhenry.onmicrosoft.com"


scope = "https://graph.microsoft.com/.default"


def graph_get(url, token):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
