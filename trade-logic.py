import boto3
import pandas as pd
import numpy as np
import io
from datetime import datetime
import sha256
import json


s3_client = boto3.client('s3')
current_date = datetime.now().strftime('%Y/%m/%d/%H')
trades = pd.DataFrame()

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
#balances_df = pd.DataFrame(data={'symbol': ['usdc', 'UBT'], 'balance': [100, 90]})


#sell
balances_df = balances_df.merge(predictions_df, how='left', on='symbol')
balances_df = balances_df.fillna(0)

for index, row in balances_df.iterrows():
    if row['symbol'] != 'usdc' and row['y'] < 0.0:
        print(f'selling: {row["symbol"]}')
        exchange_rate = 1 / float(row['price'])
        balance = float(row['balance'])
        balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] = balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] + (balance / exchange_rate)
        index = balances_df.loc[balances_df['symbol'] == row['symbol'], 'balance'].index
        balances_df = balances_df.drop(index)
        new_row = pd.DataFrame({'symbol': row['symbol'], 'name': row['name'], 'direction': 'SELL', 'price': row['price'], 'amount': balance, 'exchange_rate': exchange_rate, 'date': current_date, 'y': row['y']}, index=[0])
        trades = pd.concat([trades, new_row]) 




#buy
buys = predictions_df[predictions_df['y'] > 34.0].sort_values(by=['y', 'cmc_rank'], ascending=False).head(1)

if (balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] > 10).any():
    #if bug keeps happen we can remove look and just take .head(1)
    for index, row in buys.iterrows():
        if row['symbol'] not in balances_df['symbol'].values:
            print(f'buying: {row["symbol"]}')
            balance = 10 / float(row['price'])
            new_row = pd.DataFrame({
            'symbol': [row['symbol']],
            'balance': [balance]
            })
            balances_df = pd.concat([balances_df, new_row])
            balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] = balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] - 10
            new_row = pd.DataFrame({'symbol': row['symbol'], 'name': row['name'], 'direction': 'BUY', 'price': row['price'], 'amount': balance, 'exchange_rate': 0, 'date': current_date, 'y': row['y']}, index=[0])
            trades = pd.concat([trades, new_row])         #else:
            #Comment out so will not buy if position alreadt open
            #balances_df.loc[balances_df['symbol'] == row['symbol'], 'balance'] = balances_df.loc[balances_df['symbol'] == row['symbol'], 'balance'] + (10 / float(row['price']))
            #balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] = balances_df.loc[balances_df['symbol'] == 'usdc', 'balance'] - 10

    
balances_df = balances_df[['symbol', 'balance']]
s3_client.put_object(Bucket='gascity', Key=balances_file, Body=balances_df.to_csv(index=False))


# Create JSON payload & send to SQS
msg = ''
for index, row in trades.iterrows():
    if row['direction'] == 'BUY':
        text = f'\n{row["direction"]} order for {row["symbol"]} {row["name"]} of amount {row["amount"]} at {row["price"]} and y: {row["y"]}'
    else:
        text = f'\n{row["direction"]} order for {row["symbol"]}, {row["name"]} of amount {row["amount"]} at {row["exchange_rate"]} and y: {row["y"]}'
    msg = msg + text

if not trades.empty:
    sns_client = boto3.client('sns', region_name='us-east-1')
    response = sns_client.publish(
    TopicArn='arn:aws:sns:us-east-1:386166838496:trades',
    Message=msg,
    Subject='Trade Message'
)
