"""
EDB022 - Atividade 7 - Produtor
Le os registros de Reclamacoes (dados trimestrais do Banco Central) na
camada Raw do S3 e publica cada um como uma mensagem individual na fila SQS.

Formato de origem: CSV separado por ";", encoding Latin-1, com colunas:
Ano;Trimestre;Categoria;Tipo;CNPJ IF;Instituicao financeira;Indice;
Quantidade de reclamacoes reguladas procedentes;
Quantidade de reclamacoes reguladas - outras;
Quantidade de reclamacoes nao reguladas;Quantidade total de reclamacoes;
Quantidade total de clientes  CCS e SCR;Quantidade de clientes  CCS;
Quantidade de clientes  SCR;

Gatilho sugerido: EventBridge Schedule (ex.: a cada N minutos) ou
invocacao manual/S3 event quando um novo arquivo chega em raw/reclamacoes/.
"""
import io
import json
import os
import sys

import boto3
import pandas as pd

S3_BUCKET = os.environ.get("S3_BUCKET", "edb022-datalake")
S3_RAW_PREFIX = os.environ.get("S3_RAW_PREFIX", "raw/reclamacoes/")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Execucao local (sem S3), usada pelo modo --local / testes
LOCAL_INPUT_DIR = os.environ.get("LOCAL_RECLAMACOES_DIR", "data/reclamacoes_sample")

s3_client = boto3.client("s3", region_name=AWS_REGION)
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# Mapeia as colunas originais (em portugues, com espacos/acentos) para
# chaves normalizadas, estaveis para uso downstream (mensagens, JSON, SQL).
COLUMN_MAP = {
    "Ano": "ano",
    "Trimestre": "trimestre",
    "Categoria": "categoria",
    "Tipo": "tipo",
    "CNPJ IF": "cnpj_if",
    "Instituição financeira": "instituicao_financeira",
    "Índice": "indice",
    "Quantidade de reclamações reguladas procedentes": "qtd_reclamacoes_reguladas_procedentes",
    "Quantidade de reclamações reguladas - outras": "qtd_reclamacoes_reguladas_outras",
    "Quantidade de reclamações não reguladas": "qtd_reclamacoes_nao_reguladas",
    "Quantidade total de reclamações": "qtd_total_reclamacoes",
}

# Alguns cabecalhos usam um travessao especial (ex.: "\x96") no lugar de
# espaco duplo, dependendo da exportacao do relatorio. Por isso o
# casamento destas 3 colunas finais e feito por prefixo, nao por igualdade.
COLUMN_PREFIX_MAP = {
    "Quantidade total de clientes": "qtd_total_clientes_ccs_scr",
    "Quantidade de clientes": None,  # resolvido dinamicamente (CCS ou SCR)
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: c.strip())
    rename_map = {orig.strip(): dest for orig, dest in COLUMN_MAP.items()}

    final_map = {}
    for col in df.columns:
        if col in rename_map:
            final_map[col] = rename_map[col]
        elif col.startswith("Quantidade total de clientes"):
            final_map[col] = "qtd_total_clientes_ccs_scr"
        elif col.startswith("Quantidade de clientes") and col.rstrip().endswith("CCS"):
            final_map[col] = "qtd_clientes_ccs"
        elif col.startswith("Quantidade de clientes") and col.rstrip().endswith("SCR"):
            final_map[col] = "qtd_clientes_scr"
    df = df.rename(columns=final_map)
    # Remove colunas sem nome / vazias (rodape ou ";" final do CSV original)
    df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed:")]]
    return df


def _list_raw_objects():
    """Lista os objetos disponiveis na camada Raw de reclamacoes no S3."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_RAW_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return keys


def _read_s3_object_as_dataframe(key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"].read()
    return pd.read_csv(io.BytesIO(body), sep=";", encoding="latin-1")


def _read_local_files_as_dataframe() -> pd.DataFrame:
    frames = []
    for name in sorted(os.listdir(LOCAL_INPUT_DIR)):
        if name.endswith(".csv"):
            path = os.path.join(LOCAL_INPUT_DIR, name)
            frames.append(pd.read_csv(path, sep=";", encoding="latin-1"))
    if not frames:
        raise FileNotFoundError(f"Nenhum CSV encontrado em {LOCAL_INPUT_DIR}")
    return pd.concat(frames, ignore_index=True)


def _send_batch(records: list[dict]):
    """Envia registros para a SQS em lotes de ate 10 mensagens (limite da API)."""
    sent = 0
    for i in range(0, len(records), 10):
        batch = records[i : i + 10]
        entries = [
            {"Id": str(idx), "MessageBody": json.dumps(rec, default=str)}
            for idx, rec in enumerate(batch)
        ]
        if SQS_QUEUE_URL:
            response = sqs_client.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
            sent += len(response.get("Successful", []))
            for failure in response.get("Failed", []):
                print(f"[WARN] Falha ao enviar mensagem: {failure}", file=sys.stderr)
        else:
            # Sem fila configurada (ex.: execucao local sem AWS): apenas imprime.
            for rec in batch:
                print(json.dumps(rec, default=str, ensure_ascii=False))
            sent += len(batch)
    return sent


def handler(event, context):
    """Ponto de entrada da AWS Lambda."""
    is_local = bool(event.get("local")) if isinstance(event, dict) else False

    if is_local or not SQS_QUEUE_URL:
        df = _read_local_files_as_dataframe()
        arquivos = 1
    else:
        keys = _list_raw_objects()
        arquivos = len(keys)
        frames = [_read_s3_object_as_dataframe(k) for k in keys]
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    df = _normalize_columns(df)
    # Remove a linha em branco de rodape/paginacao, caso exista.
    df = df.dropna(how="all")
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    total_sent = _send_batch(records)

    result = {"arquivos_processados": arquivos, "mensagens_enviadas": total_sent}
    print(json.dumps(result))
    return {"statusCode": 200, "body": json.dumps(result)}


if __name__ == "__main__":
    # Execucao local: `python lambda_function.py --local` (le de data/reclamacoes_sample)
    local_event = {"local": "--local" in sys.argv}
    handler(local_event, None)
