# Arquitetura — Atividade 7

## Componentes

| Componente | Serviço AWS | Responsabilidade |
|---|---|---|
| Produtor | AWS Lambda | Lê `s3://bucket/raw/reclamacoes/` (CSV `;`, Latin-1), publica 1 mensagem SQS por linha do relatório trimestral |
| Fila | AWS SQS (+ DLQ) | Desacopla produção e consumo; garante reentrega em falhas |
| Consumidor / Enriquecedor | AWS Lambda (gatilho SQS) | Normaliza o nome da instituição e consulta a tabela `bancos` para enriquecer cada mensagem |
| Armazenamento final | AWS S3 | Grava a junção reclamações + bancos, particionada por data |

## O desafio de casamento entre Reclamações e Bancos

Os dois arquivos de origem nomeiam as instituições de formas diferentes:

- **Reclamações** (Banco Central): `"BRADESCO (conglomerado)"`, `"AGORACRED S/A SOCIEDADE DE CRÉDITO..."`
- **Bancos** (`EnquadramentoInicia_v2.tsv`): `"BRADESCO - PRUDENCIAL"`, `"BTG PACTUAL - PRUDENCIAL"`

A função `normalize_nome()` (em `consumer_lambda/lambda_function.py`) resolve isso:

1. Coloca o nome em maiúsculas;
2. Remove sufixos comuns: `(conglomerado)`, `- PRUDENCIAL`, `S/A`, `S.A.`;
3. Remove pontuação e espaços duplicados.

O resultado (`nome_normalizado`) é calculado tanto ao carregar a tabela
`bancos` (`scripts/load_bancos.py`) quanto em tempo de consumo, permitindo
um `WHERE nome_normalizado = %s` direto — sem `LIKE`/fuzzy matching, já
que a normalização é suficiente para a maior parte dos bancos S1/S2/S3
do dataset.

## Decisões de design

- **Desacoplamento via fila**: o Produtor não sabe nada sobre o Consumidor; se o Consumidor cair, as mensagens continuam na fila (até o `VisibilityTimeout` expirar) ou vão para a Dead Letter Queue após 5 tentativas.
- **Enriquecimento just-in-time**: a junção com `bancos` acontece no momento do consumo (não em lote), simulando um cenário de streaming/near-real-time.
- **Idempotência**: cada mensagem processada gera um arquivo novo (nome com timestamp de microssegundos), evitando conflitos em reprocessamentos.
- **Particionamento por data (`dt=YYYY-MM-DD`)**: facilita consultas posteriores (Athena/Glue) e a organização em camadas Medallion.
- **Amostras versionadas**: como o dataset completo é grande, o repositório versiona apenas amostras reais (20–30 linhas) em `data/`, suficientes para testar o pipeline de ponta a ponta localmente.

## Possíveis evoluções

- Trocar o polling/schedule do Produtor por um gatilho `S3 ObjectCreated` (event-driven puro).
- Persistir também em uma tabela `delivery` no banco relacional, além do S3.
- Adicionar métricas de fila (CloudWatch: `ApproximateNumberOfMessagesVisible`) para monitorar atraso de processamento.
- Registrar, para instituições sem correspondência em `bancos` (ex.: fintechs fora do Enquadramento Inicial), um indicador `banco_enriquecido: {}` para acompanhamento de cobertura da junção.
