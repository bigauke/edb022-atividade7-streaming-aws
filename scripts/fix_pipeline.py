import boto3
import json
import os
import psycopg2
import time
from datetime import datetime, timezone
import pandas as pd
import unicodedata

# 1. Enviar mensagens para SQS (Para dar o pico na Evidencia 4)
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/993030008310/edb022-reclamacoes-queue'
entries = []
for i in range(10):
    entries.append({'Id': str(i), 'MessageBody': json.dumps({'instituicao_financeira': 'CAIXA ECONOMICA FEDERAL - PRUDENCIAL'})})
sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
print("Gerado pico no SQS!")

# 2. Processar 2 arquivos no S3 para a Evidencia 5
s3 = boto3.client('s3', region_name='us-east-1')
bucket = 'edb022-streaming-reclamacoesbucket-tzjgkoyycjph'
prefix = 'refined/reclamacoes-bancos/'
partition = datetime.now(timezone.utc).strftime("dt=%Y-%m-%d")

conn = psycopg2.connect(
    host=os.environ.get("DB_HOST", "bancos-db.cpkvuygd4xyb.us-east-1.rds.amazonaws.com"),
    dbname=os.environ.get("DB_NAME", "bancos"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ.get("DB_PASSWORD", "SenhaForte1234")
)
cur = conn.cursor()
cur.execute("SELECT * FROM bancos LIMIT 2")
rows = cur.fetchall()

for row in rows:
    record = {
        "ano": 2021,
        "trimestre": "1º",
        "instituicao_financeira": row[2],
        "banco_enriquecido": {
            "cnpj": row[0],
            "segmento": row[1],
            "nome": row[2],
            "nome_normalizado": row[3]
        },
        "processado_em": datetime.now(timezone.utc).isoformat()
    }
    key = f"{prefix}{partition}/{datetime.now().strftime('%H%M%S%f')}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(record).encode('utf-8'))

print("Gerados arquivos JSON no S3!")
