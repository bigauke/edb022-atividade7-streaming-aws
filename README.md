# EDB022 — Atividade 7: Pipeline em Cloud Computing (Streaming)

Disciplina **eEDB-011 — Ingestão de Dados** (PECE / Escola Politécnica da USP)
Prof. Leandro Mendes Ferreira

Alunos:

Antonio Daniel de Souza Linhares 

Hercules Ramos Veloso de Freitas 

Yuri Alexandre Barbosa Rodrigues  

## Objetivo

Implementar um pipeline de **streaming serverless na AWS**, no qual:

1. Um **Produtor** (AWS Lambda) lê os dados de **Reclamações** (relatório trimestral do Banco Central) no S3 (camada Raw) e publica cada registro como uma mensagem em uma fila **AWS SQS**.
2. Uma fila **AWS SQS** desacopla produção e consumo.
3. Um **Consumidor/Enriquecedor** (AWS Lambda, disparada pela SQS) recebe cada mensagem, consulta o banco de dados SQL (dados de **Bancos**, já tratados) para enriquecer a reclamação com Segmento e CNPJ oficiais, e grava o resultado da junção em um bucket **S3** (camada de saída).

```
 Produtor          Fila           Consumidor (enriquecedor)
┌──────────┐   ┌──────────┐   ┌─────────────────────────────┐
│ S3 / RAW │──▶│  SQS     │──▶│ Lambda + consulta SQL (Bancos)│──▶ S3
│Reclamações│  └──────────┘   └─────────────────────────────┘   (junção)
└──────────┘
```

## Dados utilizados

Este repositório usa o mesmo conjunto de dados das demais atividades da disciplina:

| Fonte           | Arquivo(s) originais                                    | Formato          | Uso nesta atividade                                    |
| --------------- | ------------------------------------------------------- | ---------------- | ------------------------------------------------------ |
| **Reclamações** | `Dados/Reclamações/2021_tri_01.csv` … `2022_tri_04.csv` | CSV `;`, Latin-1 | Entrada do **Produtor** (uma mensagem por linha)       |
| **Bancos**      | `Dados/Bancos/EnquadramentoInicia_v2.tsv`               | TSV, Latin-1     | Fonte de enriquecimento consultada pelo **Consumidor** |

Colunas reais de **Reclamações** (renomeadas para `snake_case` pelo Produtor):

```
Ano;Trimestre;Categoria;Tipo;CNPJ IF;Instituição financeira;Índice;
Quantidade de reclamações reguladas procedentes;
Quantidade de reclamações reguladas - outras;
Quantidade de reclamações não reguladas;
Quantidade total de reclamações;
Quantidade total de clientes  CCS e SCR;
Quantidade de clientes  CCS;
Quantidade de clientes  SCR;
```

Colunas de **Bancos** (`Segmento`, `CNPJ`, `Nome`), por exemplo:

```
S1  60746948   BRADESCO - PRUDENCIAL
S3  655522     APE POUPEX
```

> **Desafio de junção**: em Reclamações, a instituição aparece como
> `"BRADESCO (conglomerado)"`; em Bancos, como `"BRADESCO - PRUDENCIAL"`.
> Por isso o Consumidor normaliza os nomes (maiúsculo, sem `(conglomerado)`,
> `- PRUDENCIAL`, `S/A`, pontuação) antes de casar os dois lados — ver
> `consumer_lambda/lambda_function.py::normalize_nome`.

Como os arquivos completos são grandes, o repositório inclui **amostras
reais** (20–30 linhas) em `data/reclamacoes_sample/` e `data/bancos_sample/`
para permitir testes locais sem precisar do dataset completo. Para rodar
com a base completa, aponte `LOCAL_RECLAMACOES_DIR` / `scripts/load_bancos.py --path`
para os arquivos originais (ou envie-os para o S3 e o RDS, respectivamente).

## Estrutura do repositório

```
edb022-atividade7-streaming-aws/
├── producer_lambda/
│   └── lambda_function.py     # lê reclamações (S3 ou local) e publica na fila SQS
├── consumer_lambda/
│   └── lambda_function.py     # consome SQS, normaliza nomes, enriquece com Bancos (SQL) e grava a saída
├── scripts/
│   └── load_bancos.py         # carrega EnquadramentoInicia_v2.tsv na tabela `bancos`
├── infra/
│   └── template.yaml          # infraestrutura como código (AWS SAM) — S3, SQS, Lambdas, IAM
├── docs/
│   └── arquitetura.md         # detalhamento da arquitetura e decisões
├── data/
│   ├── reclamacoes_sample/    # amostra real (20 linhas) de um trimestre de Reclamações
│   └── bancos_sample/         # amostra real (30 linhas) do Enquadramento Inicial de Bancos
├── requirements.txt
├── .env.example
└── README.md
```

## Pré-requisitos

- Conta AWS com permissões para S3, SQS, Lambda e IAM
- AWS CLI + [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) configurados
- Um banco de dados relacional (RDS/PostgreSQL, ou Postgres local para testes) — a tabela `bancos` é criada automaticamente por `scripts/load_bancos.py`
- Python 3.11

## Configuração

```bash
cp .env.example .env
# preencha S3_BUCKET, SQS_QUEUE_URL e as credenciais do banco (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
pip install -r requirements.txt
```

## Carregar a tabela `bancos`

```bash
# usando a amostra incluída no repositório
python scripts/load_bancos.py --path data/bancos_sample/enquadramento_amostra.tsv

# ou com o arquivo completo (EnquadramentoInicia_v2.tsv), fornecido pela disciplina
python scripts/load_bancos.py --path /caminho/para/EnquadramentoInicia_v2.tsv
```

## Deploy (AWS SAM)

```bash
sam build -t infra/template.yaml
sam deploy --guided
```

O `template.yaml` provisiona:

- Bucket S3 (camadas `raw/reclamacoes/` e `refined/reclamacoes-bancos/`)
- Fila SQS (`edb022-reclamacoes-queue`), incluindo uma Dead Letter Queue
- Lambda `producer_lambda`, com permissão de leitura no S3 e envio de mensagens na SQS
- Lambda `consumer_lambda`, disparada por evento SQS, com permissão de leitura na fila, escrita no S3 e acesso à base SQL (via Secrets Manager/variáveis de ambiente)

## Execução local (sem AWS, usando as amostras de dados)

```bash
# 1) sobe um Postgres local (ou aponte para um RDS) e carregue a tabela `bancos`
python scripts/load_bancos.py --path data/bancos_sample/enquadramento_amostra.tsv

# 2) "produz" as mensagens a partir da amostra de reclamações (imprime no console,
#    pois SQS_QUEUE_URL não está configurada)
python producer_lambda/lambda_function.py --local

# 3) processa um evento de exemplo e grava o resultado enriquecido em ./output/
python consumer_lambda/lambda_function.py --local
```

## Fluxo de dados

| Camada                                                                            | Papel                                                                      | Formato                     |
| --------------------------------------------------------------------------------- | -------------------------------------------------------------------------- | --------------------------- |
| `s3://bucket/raw/reclamacoes/` (ou `data/reclamacoes_sample/` local)              | Entrada do Produtor                                                        | CSV `;`, Latin-1            |
| SQS `edb022-reclamacoes-queue`                                                    | Fila de mensagens (1 linha de Reclamações por mensagem, JSON)              | JSON                        |
| Banco SQL — tabela `bancos`                                                       | Fonte de enriquecimento (Segmento + CNPJ), consultada por nome normalizado | Tabela relacional           |
| `s3://bucket/refined/reclamacoes-bancos/` (ou `output/reclamacoes_bancos/` local) | Saída do Consumidor (junção enriquecida)                                   | JSON, particionado por data |

## 
