import json
import os
import boto3
import urllib.parse
import pandas as pd
import io
import unicodedata

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']

def clean_col(c):
    c = ''.join(ch for ch in unicodedata.normalize('NFKD', c) if not unicodedata.combining(ch))
    return c.strip().lower().replace(' ', '_')

def handler(event, context):
    processed = 0
    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        
        df = pd.read_csv(io.BytesIO(content), sep=';', encoding='latin-1')
        df.columns = [clean_col(c) for c in df.columns]
        
        entries = []
        for _, row in df.iterrows():
            msg = row.to_dict()
            entries.append({
                'Id': str(processed),
                'MessageBody': json.dumps(msg, ensure_ascii=False)
            })
            processed += 1
            
            if len(entries) == 10:
                sqs_client.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
                entries = []
                
        if entries:
            sqs_client.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
            
    print(f"Enviadas {processed} mensagens para a fila SQS.")
    return {"statusCode": 200, "body": f"Enviadas {processed} mensagens"}
