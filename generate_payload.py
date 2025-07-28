import boto3
import pandas as pd
import json
import numpy as np
from io import StringIO  # For reading CSV from string buffer
from itertools import combinations, product

# AWS Credentials (use environment variables or IAM role in production)
aws_access_key = 'AAAAA'
aws_secret_key = 'WAAAAA'
bucket_name = 'demo-anomaly'
s3_file_key = 'test2.csv'  

def clean_df(df):
    # Remove the space before each feature names
    df.columns = df.columns.str.strip()
    print('dataset shape', df.shape)

    # This set of feature should have >= 0 values
    num = df._get_numeric_data()
    num[num < 0] = 0

    zero_variance_cols = []
    for col in df.columns:
        if len(df[col].unique()) == 1:
            zero_variance_cols.append(col)
    # df.drop(zero_variance_cols, axis = 1, inplace = True)
    print('zero variance columns', zero_variance_cols, 'dropped')
    print('shape after removing zero variance columns:', df.shape)

    df.replace([np.inf, -np.inf], np.nan, inplace = True)
    print(df.isna().any(axis = 1).sum(), 'rows dropped')
    df.dropna(inplace = True)
    print('shape after removing nan:', df.shape)

    # Drop duplicate rows
    df.drop_duplicates(inplace = True)
    print('shape after dropping duplicates:', df.shape)

    column_pairs = [(i, j) for i, j in combinations(df, 2) if df[i].equals(df[j])]
    ide_cols = []
    for column_pair in column_pairs:
        ide_cols.append(column_pair[1])
    df.drop(ide_cols, axis = 1, inplace = True)
    print('columns which have identical values', column_pairs, 'dropped')
    print('shape after removing identical value columns:', df.shape)
    return df


# Define the expected columns (as per your training)
columns = ['Destination Port', 'Flow Duration', 'Total Fwd Packets',
           'Total Backward Packets', 'Total Length of Fwd Packets',
           'Total Length of Bwd Packets', 'Fwd Packet Length Max',
           'Fwd Packet Length Min', 'Fwd Packet Length Mean',
           'Fwd Packet Length Std', 'Bwd Packet Length Max',
           'Bwd Packet Length Min', 'Bwd Packet Length Mean',
           'Bwd Packet Length Std', 'Flow Bytes/s', 'Flow Packets/s',
           'Flow IAT Mean', 'Flow IAT Std', 'Flow IAT Max', 'Flow IAT Min',
           'Fwd IAT Total', 'Fwd IAT Mean', 'Fwd IAT Std', 'Fwd IAT Max',
           'Fwd IAT Min', 'Bwd IAT Total', 'Bwd IAT Mean', 'Bwd IAT Std',
           'Bwd IAT Max', 'Bwd IAT Min', 'Fwd PSH Flags', 'Bwd PSH Flags',
           'Fwd URG Flags', 'Fwd Header Length', 'Bwd Header Length',
           'Fwd Packets/s', 'Bwd Packets/s', 'Min Packet Length',
           'Max Packet Length', 'Packet Length Mean', 'Packet Length Std',
           'Packet Length Variance', 'FIN Flag Count', 'RST Flag Count',
           'PSH Flag Count', 'ACK Flag Count', 'URG Flag Count', 'ECE Flag Count',
           'Down/Up Ratio', 'Average Packet Size', 'Avg Bwd Segment Size',
           'Subflow Fwd Bytes', 'Subflow Bwd Bytes', 'Init_Win_bytes_forward',
           'Init_Win_bytes_backward', 'act_data_pkt_fwd', 'min_seg_size_forward',
           'Active Mean', 'Active Std', 'Active Max', 'Active Min', 'Idle Mean',
           'Idle Std', 'Idle Max', 'Idle Min']

# Step 1: Connect to S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# Step 2: Download the CSV as a string
response = s3.get_object(Bucket=bucket_name, Key=s3_file_key)
csv_content = response['Body'].read().decode('utf-8')

# Step 3: Load into pandas
df = pd.read_csv(StringIO(csv_content))

# df = clean_df(df)
print("length", len(df))
df.columns = df.columns.str.strip()

# print(df.columns)

# print("❌ Missing columns:", [col for col in columns if col not in df.columns])

# # Step 4: Keep only expected columns
df = df[columns]

# # Step 5: Convert each row to FastAPI JSON payload
payloads = [{"data": row.tolist()} for _, row in df.iterrows()]

# Print the first 3 payloads
for i, payload in enumerate(payloads[:3]):
    print(f"--- Payload {i+1} ---")
    print(json.dumps(payload, indent=2))
