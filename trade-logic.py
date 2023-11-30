import boto3
import pandas as pd
import io
from datetime import datetime
import sha256
import json


s3_client = boto3.client('s3')
bucket_name = 'gascity'
current_date = datetime.now().strftime('%Y/%m/%d/%H')
predictions_file_key = f'cmc/predictions/{current_date}/cmcdata.csv'
open_trades_key = f'cmc/trades/opentrades.csv'

# Get predictions data from S3 and read into pandas DataFrame
csv_data = s3_client.get_object(Bucket=bucket_name, Key=predictions_file_key)
csv_content = csv_data['Body'].read()
df = pd.read_csv(io.BytesIO(csv_content))

# Select highest probability trades
targets = df[df['y'] > 10.0].sort_values(by=['y', 'cmc_rank'], ascending=False).head(4)
targets = targets.assign(timestamp=current_date, strategy='cmcmodel', action='BUY', amount='0.0005')[['symbol', 'timestamp', 'action', 'amount', 'y']]


# Create JSON payload & send to SQS
payload = json.dumps(targets.to_dict(orient='records'))
sns_client = boto3.client('sqs')
response = sns_client.send_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/386166838496/uniswap-queue.fifo',
    MessageBody=payload,
    MessageGroupId = 'cmcmodel',
    MessageDeduplicationId = sha256.sha256_hash(payload)
)

# Get open trades to S3

#csv_data = s3_client.get_object(Bucket=bucket_name, Key=open_trades_key)
#csv_content = csv_data['Body'].read()
#opentrades = pd.read_csv(io.BytesIO(csv_content))


