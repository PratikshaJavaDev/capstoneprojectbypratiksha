import pandas as pd
import os
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Replace with your actual paths
input_file_path = r"C:\Users\pratikshars\Desktop\capstone\Dataset\transactions.csv"
batch_folder_path = r"C:\Users\pratikshars\Desktop\capstone\Dataset\transactionsBatchFile"

# Read the entire dataset
transactions_df = pd.read_csv(input_file_path)

# Calculate the number of batches needed
num_records = len(transactions_df)
batch_size = 50
num_batches = num_records // batch_size

if not os.path.exists(batch_folder_path):
    os.makedirs(batch_folder_path)

try:
    for batch_num in range(num_batches):
        start_index = batch_num * batch_size
        end_index = start_index + batch_size
        batch_df = transactions_df.iloc[start_index:end_index]
        
        batch_file_path = os.path.join(batch_folder_path, f"transactions_batch_{batch_num + 1}.csv")
        batch_df.to_csv(batch_file_path, index=False)
        
        logging.info(f"Batch {batch_num + 1} saved to {batch_file_path}.")

        # Wait for 30 seconds before processing the next batch
        time.sleep(30)

except KeyboardInterrupt:
    logging.info("Script interrupted by user. Exiting...")
except Exception as e:
    logging.error(f"An error occurred: {e}")
