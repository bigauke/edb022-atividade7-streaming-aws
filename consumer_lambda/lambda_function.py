"""
EDB022 - Atividade 7 - Consumidor / Enriquecedor
Disparado por evento da fila SQS. Para cada mensagem (uma linha do
relatorio trimestral de Reclamacoes do Banco Central), consulta a base
SQL (tabela `bancos`, carregada a partir de EnquadramentoInicia_v2.tsv)
para enriquecer o registro com Segmento e CNPJ oficiais, e grava o
resultado da juncao em uma nova camada no S3 (ou em disco, no modo local).

Fonte da tabela `bancos` (Segmento; CNPJ; Nome):
    S1  60746948  BRADESCO - PRUDENCIAL
    S3  655522    APE POUPEX
    ...

O nome da instituicao em Reclamacoes costuma vir como:
    "BRADESCO (conglomerado)"       -> nome do conglomerado
    "AGORACRED S/A ..."             -> banco/financeira individual
E em Bancos costuma vir como:
    "BRADESCO - PRUDENCIAL"
Por isso o casamento eh feito por nome normalizado (maiusculo, sem
sufixos "(conglomerado)" / "- PRUDENCIAL" / espacos extras).
"""
import json
import os
import re
import sys
from datetime import datetime, timezone

import boto3
import psycopg2
import psycopg2.extras

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


def normalize_nome(nome: str) -> str:
    """Normaliza nomes de instituicoes para permitir o casamento entre
    Reclamacoes (Banco Central) e Bancos (EnquadramentoInicia_v2)."""
    if not nome:
        return ""
    texto = nome.strip().upper()
    for pattern in _SUFFIXES:
        texto = re.sub(pattern, "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"[^A-Z0-9 ]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def _get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def _enrich_with_bancos(conn, instituicao_financeira: str) -> dict:
    """Busca o registro correspondente na tabela `bancos` pelo nome
    normalizado da instituicao financeira."""
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
    return dict(row) if row else {}


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


def handler(event, context):
    """Ponto de entrada da AWS Lambda, disparado por evento da SQS
    (ou chamado localmente com um evento de teste no modo --local)."""
    local = bool(event.get("local")) if isinstance(event, dict) else False

    conn = _get_connection()
    processed = 0
    try:
        records = event.get("Records", [])
        for message in records:
            body = json.loads(message["body"])
            instituicao = body.get("instituicao_financeira", "")

            enrichment = _enrich_with_bancos(conn, instituicao)
            enriched_record = {
                **body,
                "banco_enriquecido": enrichment,
                "processado_em": datetime.now(timezone.utc).isoformat(),
            }
            destino = _write_output(enriched_record, local)
            processed += 1
            print(f"[OK] Registro enriquecido gravado em {destino}")
    finally:
        conn.close()

    return {"statusCode": 200, "body": json.dumps({"mensagens_processadas": processed})}


if __name__ == "__main__":
    # Execucao local com uma mensagem de exemplo (`python lambda_function.py --local`)
    sample_event = {
        "local": "--local" in sys.argv,
        "Records": [
            {
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
                )
            }
        ],
    }
    handler(sample_event, None)
