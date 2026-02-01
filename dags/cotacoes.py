# =========================================================
# IMPORTS
# =========================================================
import logging
from datetime import datetime, timedelta
from io import StringIO
import os

import pandas as pd
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =========================================================
# CONSTANTES / CONFIGURAÇÕES
# =========================================================
TMP_PATH = "/tmp/cotacoes_bcb.csv"
BASE_URL = "https://www4.bcb.gov.br/Download/fechamento"


# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="fin-cotacoes_bcb_classic",
    schedule="@daily",
    start_date=datetime(2026, 1, 29),
    catchup=True,
    tags=["bcb"],
) as dag:


    # =====================================================
    # EXTRACT
    # =====================================================


    def extract(**kwargs):
        logical_date = kwargs["logical_date"]

        data_ref = (logical_date - timedelta(days=1)).strftime("%Y%m%d")

        url = f"{BASE_URL}/{data_ref}.csv"
        logging.info(f"Baixando arquivo PTAX fechamento: {url}")

        response = requests.get(url)

        if response.status_code == 404:
            logging.warning(
                f"Arquivo de fechamento não disponível para {data_ref}, pulando execucao"
            )
            return None

        return response.text



    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )


        # =====================================================
        # TRANSFORM
        # =====================================================
    def transform(**kwargs):
        ti = kwargs["ti"]
        csv_data = ti.xcom_pull(task_ids="extract")

        if not csv_data:
            raise ValueError("Nenhum dado recebido do extract")

        df = pd.read_csv(
            StringIO(csv_data),
            sep=";",
            decimal=",",
            thousands=".",
            header=None,
            names=[
                "dt_fechamento",
                "cod_moeda",
                "tipo_moeda",
                "desc_moeda",
                "taxa_compra",
                "taxa_venda",
                "paridade_compra",
                "paridade_venda",
            ],
        )
        
        df["dt_fechamento"] = pd.to_datetime(
            df['dt_fechamento'],
            format='%d/%m/%Y'
            ).dt.date
        
        df["data_processamento"] = datetime.now()
        
        df.to_csv(TMP_PATH, index=False)

        return TMP_PATH


    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )


    # =====================================================
    # CREATE TABLE
    # =====================================================
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS cotacoes (
            dt_fechamento DATE,
            cod_moeda TEXT,
            tipo_moeda TEXT,
            desc_moeda TEXT,
            taxa_compra REAL,
            taxa_venda REAL,
            paridade_compra REAL,
            paridade_venda REAL,
            data_processamento TIMESTAMP,
            CONSTRAINT cotacoes_pk PRIMARY KEY (dt_fechamento, cod_moeda)
        );
    """

    create_table_task = SQLExecuteQueryOperator(
        task_id="create_table_postgres",
        conn_id="postgres_astro",
        sql=create_table_sql,
    )


    # =====================================================
    # LOAD
    # =====================================================
    def load(**kwargs):
        ti = kwargs["ti"]
        path = ti.xcom_pull(task_ids="transform")

        if not path or not os.path.exists(path):
            logging.warning("Arquivo não encontrado para carga.")
            return

        df = pd.read_csv(path)

        postgres_hook = PostgresHook(postgres_conn_id="postgres_astro")

        rows = [tuple(row) for row in df.to_numpy()]

        postgres_hook.insert_rows(
            table="cotacoes",
            rows=rows,
            target_fields=list(df.columns),
            replace=True,
            replace_index=["dt_fechamento", "cod_moeda"],
        )


    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )


    # ======================== DEGUB ======================

    def check_load():
        hook = PostgresHook(postgres_conn_id="postgres_astro")
        records = hook.get_records(
            "SELECT COUNT(*) FROM cotacoes"
        )
        print(f"Total de registros: {records[0][0]}")

    debug_task = PythonOperator(
        task_id = "debug",
        python_callable=check_load
    )


    # =====================================================
    # ORQUESTRAÇÃO
    # =====================================================
    extract_task >> transform_task >> create_table_task >> load_task >> debug_task
