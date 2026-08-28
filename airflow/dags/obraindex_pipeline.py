"""
ObraIndex - Pipeline Airflow
============================

DAG principal do projeto ObraIndex.

Fluxo atual:
    validate_environment
            ↓
    validate_connections
            ↓
       run_bronze
            ↓
     summarize_run

Neste estágio do projeto, somente a camada Bronze está conectada à DAG.
As etapas Silver e Gold serão adicionadas quando seus respectivos
módulos estiverem implementados.

Execução:
- Agendada diariamente.
- Por padrão executa carga INCREMENTAL.
- Pode ser executada manualmente como FULL através do parâmetro
  `load_type`.

Estrutura esperada:
    airflow/
        dags/
            obraindex_pipeline.py

    src/
        bronze/
            bronze.py

    config/
        materiais.yml
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import dag, get_current_context, task

from src.bronze.bronze import (
    BronzeError,
    BronzeSettings,
    full_load,
    incremental_load,
    test_connections,
)


# ============================================================
# LOGGING
# ============================================================

LOGGER = logging.getLogger("obraindex.pipeline")


# ============================================================
# CONSTANTES
# ============================================================

DAG_ID = "obraindex_pipeline"

MATERIALS_FILE = os.getenv(
    "OBRAINDEX_MATERIALS_FILE",
    "/opt/airflow/project-config/materiais.yml",
)

DEFAULT_LOAD_TYPE = os.getenv(
    "OBRAINDEX_DEFAULT_LOAD_TYPE",
    "INCREMENTAL",
).upper()


# ============================================================
# CONFIGURAÇÃO PADRÃO DA DAG
# ============================================================

DEFAULT_ARGS = {
    "owner": "obraindex",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================


@dag(
    dag_id=DAG_ID,
    description=(
        "Pipeline de Engenharia de Dados do ObraIndex "
        "para preços de materiais da construção civil."
    ),
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=[
        "obraindex",
        "engenharia-de-dados",
        "compras-gov",
        "bronze",
    ],
    params={
        "load_type": DEFAULT_LOAD_TYPE,
    },
)
def obraindex_pipeline() -> None:
    """
    Pipeline principal do ObraIndex.

    Parâmetros
    ----------
    load_type:
        FULL ou INCREMENTAL.

    Observação
    ----------
    A carga incremental depende do controle de watermark.
    Enquanto o armazenamento definitivo do watermark não estiver
    implementado no PostgreSQL, a DAG aceita um dicionário vazio e
    delega o comportamento à camada Bronze.
    """

    # ========================================================
    # TASK 1 - VALIDAR AMBIENTE
    # ========================================================

    @task
    def validate_environment() -> dict[str, Any]:
        """
        Verifica se as configurações mínimas do pipeline existem.
        """

        LOGGER.info("Validando ambiente do ObraIndex...")

        settings = BronzeSettings.from_env()

        materials_path = Path(MATERIALS_FILE)

        if not materials_path.exists():
            raise FileNotFoundError(
                f"Arquivo de materiais não encontrado: {MATERIALS_FILE}"
            )

        context = get_current_context()

        requested_load_type = str(
            context["params"].get(
                "load_type",
                DEFAULT_LOAD_TYPE,
            )
        ).upper()

        if requested_load_type not in {
            "FULL",
            "INCREMENTAL",
        }:
            raise ValueError(
                "Parâmetro load_type inválido. "
                "Utilize FULL ou INCREMENTAL."
            )

        result = {
            "status": "SUCCESS",
            "load_type": requested_load_type,
            "materials_file": str(materials_path),
            "bronze_bucket": settings.bronze_bucket,
            "start_date": settings.start_date,
            "environment": os.getenv(
                "OBRAINDEX_ENV",
                "development",
            ),
        }

        LOGGER.info(
            "Ambiente validado: %s",
            json.dumps(
                result,
                ensure_ascii=False,
                default=str,
            ),
        )

        return result

    # ========================================================
    # TASK 2 - TESTAR CONEXÕES
    # ========================================================

    @task
    def validate_connections(
        environment: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Testa a comunicação com:
        - Compras.gov.br
        - MinIO
        """

        LOGGER.info(
            "Testando conexões externas | load_type=%s",
            environment["load_type"],
        )

        settings = BronzeSettings.from_env()

        connection_status = test_connections(settings)

        LOGGER.info(
            "Status das conexões: %s",
            connection_status,
        )

        failed = [
            service
            for service, status in connection_status.items()
            if not status
        ]

        if failed:
            raise BronzeError(
                "Falha nas conexões obrigatórias: "
                + ", ".join(failed)
            )

        return {
            **environment,
            "connections": connection_status,
        }

    # ========================================================
    # TASK 3 - EXECUTAR BRONZE
    # ========================================================

    @task
    def run_bronze(
        validated_environment: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Executa a carga da camada Bronze.

        FULL:
            Percorre os materiais ativos e executa a carga completa.

        INCREMENTAL:
            Executa a rotina incremental.

        Importante:
            O controle persistente de watermark será adicionado
            posteriormente no PostgreSQL.
        """

        load_type = validated_environment[
            "load_type"
        ]

        settings = BronzeSettings.from_env()

        LOGGER.info(
            "Iniciando camada Bronze | tipo=%s",
            load_type,
        )

        started_at = datetime.utcnow()

        if load_type == "FULL":

            results = full_load(
                materials_file=MATERIALS_FILE,
                settings=settings,
            )

        else:

            # ------------------------------------------------
            # Watermark
            #
            # Nesta primeira versão ainda não existe uma tabela
            # PostgreSQL persistindo o watermark.
            #
            # Quando o módulo de controle for criado, este
            # dicionário será preenchido automaticamente.
            # ------------------------------------------------

            watermark_by_catmat: dict[str, str] = {}

            results = incremental_load(
                materials_file=MATERIALS_FILE,
                watermark_by_catmat=watermark_by_catmat,
                settings=settings,
            )

        finished_at = datetime.utcnow()

        success_count = sum(
            1
            for item in results
            if item.get("status") == "SUCCESS"
        )

        failed_count = sum(
            1
            for item in results
            if item.get("status") == "FAILED"
        )

        records_written = sum(
            int(item.get("records_written", 0) or 0)
            for item in results
        )

        pages_processed = sum(
            int(item.get("pages_processed", 0) or 0)
            for item in results
        )

        result = {
            "status": (
                "SUCCESS"
                if failed_count == 0
                else "PARTIAL_SUCCESS"
            ),
            "load_type": load_type,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": (
                finished_at - started_at
            ).total_seconds(),
            "materials_processed": len(results),
            "materials_success": success_count,
            "materials_failed": failed_count,
            "pages_processed": pages_processed,
            "records_written": records_written,
            "results": results,
        }

        LOGGER.info(
            "Bronze concluída | "
            "materiais=%s | "
            "sucesso=%s | "
            "falhas=%s | "
            "registros=%s",
            len(results),
            success_count,
            failed_count,
            records_written,
        )

        return result

    # ========================================================
    # TASK 4 - RESUMO DA EXECUÇÃO
    # ========================================================

    @task
    def summarize_run(
        bronze_result: dict[str, Any],
    ) -> None:
        """
        Registra um resumo final da execução da DAG.
        """

        LOGGER.info(
            """
============================================================
OBRAINDEX - RESUMO DA EXECUÇÃO
============================================================

Tipo de carga:        %s
Status:               %s
Materiais processados:%s
Materiais com sucesso:%s
Materiais com falha:  %s
Páginas processadas:  %s
Registros gravados:   %s
Duração (segundos):   %s

============================================================
""",
            bronze_result.get("load_type"),
            bronze_result.get("status"),
            bronze_result.get("materials_processed"),
            bronze_result.get("materials_success"),
            bronze_result.get("materials_failed"),
            bronze_result.get("pages_processed"),
            bronze_result.get("records_written"),
            bronze_result.get("duration_seconds"),
        )

        failed_materials = [
            item
            for item in bronze_result.get(
                "results",
                [],
            )
            if item.get("status") == "FAILED"
        ]

        if failed_materials:

            LOGGER.warning(
                "Materiais que apresentaram falha: %s",
                json.dumps(
                    failed_materials,
                    ensure_ascii=False,
                    default=str,
                ),
            )

    # ========================================================
    # ORQUESTRAÇÃO
    # ========================================================

    environment = validate_environment()

    connections = validate_connections(
        environment
    )

    bronze = run_bronze(
        connections
    )

    summarize_run(
        bronze
    )


# ============================================================
# REGISTRA A DAG NO AIRFLOW
# ============================================================

obraindex_pipeline()
