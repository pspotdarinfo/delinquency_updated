import pandas as pd
import numpy as np
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
connect_str = "DefaultEndpointsProtocol=https;AccountName=mlpp1amlsa;AccountKey=DB63JsfL+tlq9DVJqjqCFmq5AG18ni1Q6WBvWug7ionzZvppOEOSyHPQsdHuB/TMOMNXWwm5N2hq+AStMbcSgg==;EndpointSuffix=core.windows.net"


# Create the BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connect_str)


# Create a unique name for the container
container_name = "azureml-blobstore-19ca71ed-da0e-40d6-8ee5-818e9977c04d"

# Create the container
#container_client = blob_service_client.create_container(container_name)

#download csv file from Azure Storage
from azure.storage.blob import BlobClient

blob = BlobClient.from_connection_string(conn_str= connect_str, container_name= container_name, blob_name="delinquent/delq.csv")

with open("./delq.csv", "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)


df = pd.read_csv("./delq.csv")



df=df.copy()
df['PaymentID']=df['PaymentID'].fillna('ID Missing')
df = df[df.PaymentID != 'ID Missing']
df['ChargeAmount']=df['ChargeAmount'].apply(lambda x:np.log(x+1))
df['SalesTax']=df['SalesTax'].apply(lambda x:np.log(x+1))
df['AdminFee']=df['AdminFee'].apply(lambda x:np.log(x+1))
df['InvoicePayAmount']=df['InvoicePayAmount'].apply(lambda x:np.log(x+1))
df["DateTransferDoF"] = pd.to_datetime(df["DateTransferDoF"])
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df['Daylag']=df['DateTransferDoF']-df['InvoiceDate']
for i in range(0, len(df)):
    

    df.iloc[i,14]=df.iloc[i,14].days
df['Daylag']=df['Daylag'].astype('Int64')
df['Daylag']=df['Daylag'].fillna(291)
df['year']=np.nan
for i in range(0, len(df)):
    df.iloc[i,15] = df.iloc[i,11].year
df['month']=np.nan
for i in range(0, len(df)):
    df.iloc[i,16] = df.iloc[i,11].month
df=df.drop(columns=['InvoiceSequenceID','InvoiceBillAmount','PaymentID','InvoiceDate','DateTransferDoF'])
df=df.drop(columns=['InvoicePayAmount','SalesTax','AdminFee'])
df=df.drop(columns=['month'])
df=df.drop(columns=['OMONumber'])
from sklearn import preprocessing
label_encoder = preprocessing.LabelEncoder()
dfinvoptcpy2=df.copy()
dfinvoptcpy2['InvoiceStatus']= label_encoder.fit_transform(dfinvoptcpy2['InvoiceStatus'])

dfinvoptcpy2['DaysDel']= label_encoder.fit_transform(dfinvoptcpy2['DaysDel'])
dfinvoptcpy2['Payment Term']= label_encoder.fit_transform(dfinvoptcpy2['Payment Term'])

dfinvoptcpy2['year']= label_encoder.fit_transform(dfinvoptcpy2['year'])
ncols=['ChargeAmount','Daylag']
from sklearn.preprocessing import StandardScaler
for i in ncols:
    scale= StandardScaler().fit(dfinvoptcpy2[[i]])
    dfinvoptcpy2[i]=scale.transform(dfinvoptcpy2[[i]])
dfinvoptcpy2=dfinvoptcpy2[['InvoiceID','ChargeAmount','Payment Term','InvoiceStatus','Daylag','year','DaysDel']]



dfinvoptcpy2.to_csv('preproc_delq.csv',index=False)

#Upload preprocessed csv file

from azure.storage.blob import BlobClient

blob = BlobClient.from_connection_string(conn_str= connect_str, container_name= container_name, blob_name="delinquent_pre/preproc_delq.csv")

with open("./preproc_delq.csv", "rb") as data:
    blob.upload_blob(data, overwrite=True)
