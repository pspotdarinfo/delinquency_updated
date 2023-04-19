try:
    import evidently
    print(evidently.__version__)
except:
    pip install git+https://github.com/evidentlyai/evidently.git



import pandas as pd

import evidently
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from evidently.test_suite import TestSuite
from evidently.test_preset import NoTargetPerformanceTestPreset


import pandas as pd

import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try:
    print("Azure Blob Storage Python quickstart sample")

    # Quickstart code goes here

except Exception as ex:
    print('Exception:')
    print(ex)


# Retrieve the connection string for use with the application. The storage
# connection string is stored in an environment variable on the machine
# running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
# created after the application is launched in a console or with Visual Studio,
# the shell or application needs to be closed and reloaded to take the
# environment variable into account.
connect_str = "DefaultEndpointsProtocol=https;AccountName=mlpp1amlsa;AccountKey=X0qvtp6ACm0xTDR201/3OwsI5OoeDTZAveQMCbqJI/lEqfWs7+rOBBDgd1sP/hKpl2s06gvnSeh9+ASt0PCRJw==;EndpointSuffix=core.windows.net"


# Create the BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connect_str)


# Create a unique name for the container
container_name = "azureml-blobstore-1edd53da-3739-4e22-9a03-efa27028537c"

# Create the container
#container_client = blob_service_client.create_container(container_name)

#download csv file from Azure Storage
from azure.storage.blob import BlobClient

blob = BlobClient.from_connection_string(conn_str= connect_str, container_name= container_name, blob_name="delinquent/delq.csv")

with open("./delq.csv", "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)


df_old = pd.read_csv("./delq.csv")





try:
    print("Azure Blob Storage Python quickstart sample")

    # Quickstart code goes here

except Exception as ex:
    print('Exception:')
    print(ex)


# Retrieve the connection string for use with the application. The storage
# connection string is stored in an environment variable on the machine
# running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
# created after the application is launched in a console or with Visual Studio,
# the shell or application needs to be closed and reloaded to take the
# environment variable into account.
connect_str_n = "DefaultEndpointsProtocol=https;AccountName=mlpp1amlsa;AccountKey=X0qvtp6ACm0xTDR201/3OwsI5OoeDTZAveQMCbqJI/lEqfWs7+rOBBDgd1sP/hKpl2s06gvnSeh9+ASt0PCRJw==;EndpointSuffix=core.windows.net"


# Create the BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connect_str_n)


# Create a unique name for the container
container_name_n = "azureml-blobstore-1edd53da-3739-4e22-9a03-efa27028537c"

# Create the container
#container_client = blob_service_client.create_container(container_name_n)

#download csv file from Azure Storage
from azure.storage.blob import BlobClient

blob = BlobClient.from_connection_string(conn_str= connect_str_n, container_name= container_name_n, blob_name="new_delinquent/new_delq.csv")

with open("./new_delq.csv", "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)


df_new = pd.read_csv("./new_delq.csv")


#Function for detecting drift


def detect_drift(old_df, new_df):   
    
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=old_df, current_data=new_df)
    dict_report = report.as_dict()
    drift_flag = dict_report.get("metrics")[0].get("result").get("dataset_drift") #STR True or False

    col_drift_df = get_column_drift(old_df, new_df)
    log(f'Column Drift Output: \n{col_drift_df}')
    
    return drift_flag



def get_column_drift(old_df, new_df):
    
    suite = TestSuite(tests=[NoTargetPerformanceTestPreset()])
    suite.run(reference_data=old_df, current_data=new_df)
    data_j = suite.as_dict()
    df = pd.DataFrame(data_j['tests'][0]['parameters']['features']).T.reset_index()
      
    return df


def control_by_thresholds(cdf):
    
    print(len(cdf))

    detpc = cdf.data_drift.value_counts()[0]/len(cdf)
    ndetpc = cdf.data_drift.value_counts()[1]/len(cdf)

    print(detpc, ndetpc)
    threshold = 0.15

    print(f'Feature Drift Detection Percentage: {detpc*100}%\nThreshold set for Drift Flag State Change: {threshold} ({threshold*100}%)')

    if detpc >= threshold:
        drift_flag = True
    else:
        drift_flag = False
                
    return drift_flag



#Calling drift detection function


#print(f'input dataframe received for Drift Check (df.head(1)): \n{df.head(1)}')
#print(f'input dataframe shape: {df.shape}')

columns_to_be_dropped = ['InvoiceID', 'InvoiceSequenceID', 'OMONumber', 'PaymentID','InvoiceDate', 'DateTransferDoF' ]

df_new = df_new.drop(columns_to_be_dropped,axis=1)
print(f'Preprocessed input dataframe: {df.shape}')

#old_df= pd.read_csv("Predict Delinquency Test Data.csv")
#old_df = simple_s3_download(old_file_path)
df_old = df_old.drop(columns_to_be_dropped, axis=1)

# drift_flag = detect_drift(old_df, df_new)


col_drift_df = get_column_drift(df_old, df_new)
print(f'Column Drift Output: \n{col_drift_df}')

drift_flag = control_by_thresholds(col_drift_df)
print(f'Drift Flag Output: {drift_flag}')










