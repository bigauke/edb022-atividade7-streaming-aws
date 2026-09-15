"""
EDB022 - Atividade 7 - utilitario
Carrega o arquivo EnquadramentoInicia_v2.tsv (Segmento; CNPJ; Nome) na
tabela `bancos` do banco de dados relacional (Postgres/RDS), calculando
tambem a coluna `nome_normalizado`, usada pelo consumer_lambda para o
casamento com as Reclamacoes.

Uso:
    python scripts/load_bancos.py --path data/bancos_sample/enquadramento_amostra.tsv
    python scripts/load_bancos.py --path /caminho/completo/EnquadramentoInicia_v2.tsv
"""
import argparse
import os
import re
import sys

import pandas as pd
import psycopg2
import psycopg2.extras

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "consumer_lambda"))
from lambda_function import normalize_nome  # noqa: E402

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "case_dados")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")


def main(path: str):
    df = pd.read_csv(path, sep="\t", encoding="latin-1")
    df.columns = [c.strip().lower() for c in df.columns]  # segmento, cnpj, nome
    df["nome_normalizado"] = df["nome"].apply(normalize_nome)
    df["cnpj"] = df["cnpj"].astype(str).str.strip()

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bancos (
                    cnpj VARCHAR(20) PRIMARY KEY,
                    segmento VARCHAR(10),
                    nome VARCHAR(200) NOT NULL,
                    nome_normalizado VARCHAR(200) NOT NULL
                )
                """
            )
            rows = [
                (r["cnpj"], r["segmento"], r["nome"], r["nome_normalizado"])
                for r in df.to_dict(orient="records")
            ]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO bancos (cnpj, segmento, nome, nome_normalizado)
                VALUES %s
                ON CONFLICT (cnpj) DO UPDATE
                SET segmento = EXCLUDED.segmento,
                    nome = EXCLUDED.nome,
                    nome_normalizado = EXCLUDED.nome_normalizado
                """,
                rows,
            )
        conn.commit()
        print(f"[load_bancos] {len(rows)} registros carregados/atualizados a partir de {path}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        default="data/bancos_sample/enquadramento_amostra.tsv",
        help="Caminho do arquivo EnquadramentoInicia_v2.tsv (ou amostra)",
    )
    args = parser.parse_args()
    main(args.path)
