import time
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from io import StringIO

# Azure Blob Storage credentials
blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=mytwiterstorage001;AccountKey=a6DkinbvD5wgM5Z1ysz3wHUV7SDicuJyLLas7pM5UTlmAlaLM2zMnoMyTPBcn84if0HDOVuko3vB+ASt6ec1AA==;EndpointSuffix=core.windows.net"
container_name = "twiterdata001"
source_blob_name = "merch_sales.csv"
destination_blob_name = "processed_merch_sales.csv"

# Azure Text Analytics credentials
text_analytics_key = "6jmdIbQuVWoz39itQbuymMX7v1iejhyL8ZG1h6RZXLL4HG5ell0XJQQJ99BBACYeBjFXJ3w3AAAaACOGdhKv"
text_analytics_endpoint = "https://mytextanalytics001.cognitiveservices.azure.com/"

# Initialize the Blob Service client and Text Analytics client
blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
container_client = blob_service_client.get_container_client(container_name)
blob_client = container_client.get_blob_client(source_blob_name)

# Function to fetch data from Azure Blob Storage
def fetch_csv_from_blob(blob_client):
    download_stream = blob_client.download_blob()
    csv_data = download_stream.readall().decode("utf-8")
    return pd.read_csv(StringIO(csv_data))

# Function to analyze sentiment using the Text Analytics API
def analyze_sentiment(client, documents):
    sentiments = []
    
    # Split documents into batches of 10
    batch_size = 10
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        response = client.analyze_sentiment(documents=batch)
        
        # Collect the sentiment for each document in the batch
        for doc in response:
            sentiments.append(doc.sentiment)
    
    return sentiments

# Initialize the Text Analytics client
text_analytics_client = TextAnalyticsClient(
    endpoint=text_analytics_endpoint, 
    credential=AzureKeyCredential(text_analytics_key)
)

# Function to append data to destination blob
def append_to_blob(destination_blob_client, data):
    try:
        download_stream = destination_blob_client.download_blob()
        existing_data = download_stream.readall().decode("utf-8")
        existing_df = pd.read_csv(StringIO(existing_data))
        combined_df = pd.concat([existing_df, data], ignore_index=True)
    except:
        # If the file doesn't exist yet, create it
        combined_df = data
    new_csv = combined_df.to_csv(index=False)
    destination_blob_client.upload_blob(new_csv, overwrite=True)

# Main process to fetch data, analyze sentiment, and append to destination
def process_reviews():
    # Get the destination blob client
    destination_blob_client = container_client.get_blob_client(destination_blob_name)
    
    while True:
        # Fetch the CSV data from the source blob (200 records at a time)
        df = fetch_csv_from_blob(blob_client)
        df.columns = df.columns.str.strip()  # Clean column names to remove any leading/trailing spaces
        print(f"Columns found in the CSV: {df.columns}")
        
        # Check if the column 'Review' exists or update it to the correct one
        if 'Review' not in df.columns:
            raise KeyError("'Review' column not found in the CSV file. Please check the column name.")
        
        total_rows = len(df)
        
        # Process in batches of 200 rows
        for i in range(0, total_rows, 200):
            batch_df = df.iloc[i:i + 200]
            reviews = batch_df['Review'].tolist()  # Ensure this column exists

            # Analyze sentiment of the Review
            sentiments = analyze_sentiment(text_analytics_client, reviews)

            # Add the sentiment to the dataframe
            batch_df['sentiment'] = sentiments

            # Append the batch to the destination blob
            append_to_blob(destination_blob_client, batch_df)

            # Wait for 10 minutes before processing the next batch
            print(f"Processed batch {i//200 + 1}. Waiting for 10 minutes before fetching the next batch...")
            time.sleep(10 * 60)  # 10 minutes

# Run the process
if __name__ == "__main__":
    process_reviews()
