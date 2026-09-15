import json
import logging
import os
import re
import sys
from datetime import datetime, timezone

import boto3
import psycopg2
import psycopg2.extras

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET", "edb022-datalake")
S3_OUTPUT_PREFIX = os.environ.get("S3_OUTPUT_PREFIX", "refined/reclamacoes-bancos/")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "case_dados")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")

LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "output/reclamacoes_bancos")

s3_client = boto3.client("s3", region_name=AWS_REGION)

_SUFFIXES = [r"\(conglomerado\)", r"-\s*prudencial", r"s/a", r"s\.a\.?"]

_conn = None


def _get_connection():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
    return _conn


def normalize_nome(nome: str) -> str:
    if not nome:
        return ""
    texto = nome.strip().upper()
    for pattern in _SUFFIXES:
        texto = re.sub(pattern, "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"[^A-Z0-9 ]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def _enrich_with_bancos(conn, instituicao_financeira: str) -> dict:
    nome_normalizado = normalize_nome(instituicao_financeira)
    if not nome_normalizado:
        return {}

    query = """
        SELECT segmento, cnpj, nome, nome_normalizado
        FROM bancos
        WHERE nome_normalizado = %s
        LIMIT 1
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (nome_normalizado,))
        row = cur.fetchone()
    if not row:
        logger.warning(
            "Sem correspondencia em `bancos` para instituicao=%r (normalizado=%r)",
            instituicao_financeira,
            nome_normalizado,
        )
        return {}
    return dict(row)


def _write_output(record: dict, local: bool):
    now = datetime.now(timezone.utc)
    partition = now.strftime("dt=%Y-%m-%d")
    filename = f"{now.strftime('%H%M%S%f')}.json"

    if local:
        out_dir = os.path.join(LOCAL_OUTPUT_DIR, partition)
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, default=str, ensure_ascii=False)
        return path

    key = f"{S3_OUTPUT_PREFIX}{partition}/{filename}"
    s3_client.put_object(
        Bucket=S3_BUCKET, Key=key, Body=json.dumps(record, default=str).encode("utf-8")
    )
    return key


def _process_message(conn, message: dict, local: bool) -> str:

    body = json.loads(message["body"])
    instituicao = body.get("instituicao_financeira", "")

    enrichment = _enrich_with_bancos(conn, instituicao)
    enriched_record = {
        **body,
        "banco_enriquecido": enrichment,
        "processado_em": datetime.now(timezone.utc).isoformat(),
    }
    return _write_output(enriched_record, local)


def handler(event, context):

    local = bool(event.get("local")) if isinstance(event, dict) else False

    conn = _get_connection()
    processed = 0
    batch_item_failures = []

    records = event.get("Records", [])
    for message in records:
        message_id = message.get("messageId", "local")
        try:
            destino = _process_message(conn, message, local)
            processed += 1
            logger.info("Registro enriquecido gravado em %s", destino)
        except Exception:
            logger.exception(
                "Falha ao processar mensagem messageId=%s -- sera reenviada pela SQS",
                message_id,
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    return {
        "statusCode": 200,
        "body": json.dumps({"mensagens_processadas": processed}),
        "batchItemFailures": batch_item_failures,
    }


if __name__ == "__main__":
    sample_event = {
        "local": "--local" in sys.argv,
        "Records": [
            {
                "messageId": "local-1",
                "body": json.dumps(
                    {
                        "ano": 2021,
                        "trimestre": "1º",
                        "categoria": "Grupo Secundário",
                        "tipo": "Conglomerado",
                        "cnpj_if": None,
                        "instituicao_financeira": "BRADESCO (conglomerado)",
                        "qtd_total_reclamacoes": 4308,
                    },
                    ensure_ascii=False,
                ),
            }
        ],
    }
    logging.basicConfig(level=logging.INFO)
    print(handler(sample_event, None))
