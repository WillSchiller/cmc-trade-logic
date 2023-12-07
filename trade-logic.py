import boto3
import pandas as pd
import numpy as np
import io
from datetime import datetime
import sha256
import json


s3_client = boto3.client('s3')
current_date = datetime.now().strftime('%Y/%m/%d/%H')

predictions_file = f'cmc/predictions/{current_date}/cmcdata.csv'
balances_file = f'cmc/trades/balances.csv'
orders_log_file = f'cmc/trades/orders.csv' 
def get_object_from_s3(key):
    try:
        csv_data = s3_client.get_object(Bucket='gascity', Key=key)
        csv_content = csv_data['Body'].read()
        return pd.read_csv(io.BytesIO(csv_content))
    except:
        return pd.DataFrame()


# get data from s3
predictions_df = get_object_from_s3(predictions_file)
balances_df = get_object_from_s3(balances_file)
orders_df = get_object_from_s3(orders_log_file)

#sell
sells = balances_df.merge(predictions_df[predictions_df['y'] < 5.0], how='left', on='symbol')
for index, row in sells.iterrows():
    exchange_rate = 1 / float(row['price'])
    balance = float(row['balance'])
    sells.loc[sells['symbol'] == 'usdc']['balance'] = sells.loc[sells['symbol'] == 'usdc']['balance'] + (exchange_rate * balance)
    row['balance'] = 0
balances_df = sells

#buy
buys = predictions_df[predictions_df['y'] > 10.0].sort_values(by=['y', 'cmc_rank'], ascending=False).head(1)
if (balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] > 10).any():
    for index, row in buys.iterrows():
        if row['symbol'] not in balances_df['symbol'].values:
            new_row = pd.DataFrame({
            'symbol': [row['symbol']],
            'balance': [10 / float(row['price'])]
            })
            balances_df = pd.concat([balances_df, new_row])
        else:
            balances_df.loc[balances_df['symbol'] == row['symbol'], 'balance'] = 25 / float(row['price'])

        balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] = balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] - 10

    
balances_df = balances_df[['symbol', 'balance']]
balances_df['balance'] = balances_df['balance'].round(5)

s3_client.put_object(Bucket='gascity', Key=balances_file, Body=balances_df.to_csv(index=False))

# email via gmail api





# Create JSON payload & send to SQS
#payload = json.dumps(targets.to_dict(orient='records'))
#sns_client = boto3.client('sqs')
#response = sns_client.send_message(
#    QueueUrl='https://sqs.us-east-1.amazonaws.com/386166838496/uniswap-queue.fifo',
#    MessageBody=payload,
#    MessageGroupId = 'cmcmodel',
#    MessageDeduplicationId = sha256.sha256_hash(payload)
#)

# Get open trades to S3

#csv_data = s3_client.get_object(Bucket=bucket_name, Key=open_trades_key)
#csv_content = csv_data['Body'].read()
#opentrades = pd.read_csv(io.BytesIO(csv_content))


