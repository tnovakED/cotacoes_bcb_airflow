# PTAX Daily Exchange Rates Pipeline

Este projeto implementa uma **pipeline de dados diária** utilizando **Apache Airflow** para extração, transformação e carga (ETL) das **cotações PTAX de fechamento** disponibilizadas pelo Banco Central do Brasil (BCB).

## 📌 Objetivo

Automatizar a coleta diária das cotações PTAX (D-1), processar os dados e armazená-los em um banco PostgreSQL para análises posteriores.

## 🛠️ Tecnologias Utilizadas

- Apache Airflow
- Python
- Pandas
- PostgreSQL
- Requests
- SQL

## ⚙️ Funcionamento da Pipeline

A DAG executa diariamente e segue as etapas:

1. **Extract**  
   Baixa o arquivo CSV de fechamento PTAX referente ao dia anterior (`D-1`), respeitando o `logical_date` do Airflow.

2. **Transform**  
   - Leitura do CSV
   - Tratamento de tipos e datas
   - Padronização dos dados
   - Geração de arquivo temporário para carga

3. **Load**  
   Insere os dados no PostgreSQL utilizando chave primária composta (`dt_fechamento`, `cod_moeda`) para evitar duplicidades.

4. **Check (Debug)**  
   Valida a quantidade de registros carregados na tabela.

## ⏱️ Agendamento

- Frequência: **Diária**
- `catchup=True`  
  Permite backfill automático via scheduler para datas passadas.
- Execuções manuais não utilizam catchup.

## 📂 Fonte dos Dados

- Banco Central do Brasil – PTAX  
  Endpoint clássico de download de fechamento diário.

## ✅ Observações

- O arquivo PTAX é disponibilizado pelo BCB normalmente após o horário comercial.
- A DAG trata corretamente a indisponibilidade do arquivo quando executada antes da publicação.

---

