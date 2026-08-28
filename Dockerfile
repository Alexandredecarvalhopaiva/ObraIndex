# ============================================================
# OBRAINDEX
# Dockerfile customizado - Apache Airflow
# ============================================================


# ------------------------------------------------------------
# VERSÃO DO AIRFLOW
#
# Recebida pelo docker-compose.yml através do .env:
#
# AIRFLOW_VERSION=3.3.1
# ------------------------------------------------------------

ARG AIRFLOW_VERSION=3.3.1

FROM apache/airflow:${AIRFLOW_VERSION}


# Necessário declarar novamente após o FROM
ARG AIRFLOW_VERSION


# ============================================================
# METADADOS
# ============================================================

LABEL project="ObraIndex"
LABEL description="Pipeline de Engenharia de Dados para análise de preços da construção civil"
LABEL version="1.0.0"


# ============================================================
# VARIÁVEIS DO CONTAINER
# ============================================================

ENV PYTHONDONTWRITEBYTECODE=1

ENV PYTHONUNBUFFERED=1

ENV PYTHONPATH="/opt/airflow:/opt/airflow/src"

ENV TZ="America/Fortaleza"


# ============================================================
# DEPENDÊNCIAS DO SISTEMA OPERACIONAL
# ============================================================

USER root


RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        git \
        gcc \
        build-essential \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# ============================================================
# DIRETÓRIOS DO OBRAINDEX
# ============================================================

RUN mkdir -p \
    /opt/airflow/dags \
    /opt/airflow/logs \
    /opt/airflow/plugins \
    /opt/airflow/config \
    /opt/airflow/src \
    /opt/airflow/project-config


# ============================================================
# PERMISSÕES
# ============================================================

RUN chown -R airflow:root \
    /opt/airflow/dags \
    /opt/airflow/logs \
    /opt/airflow/plugins \
    /opt/airflow/config \
    /opt/airflow/src \
    /opt/airflow/project-config


# ============================================================
# USUÁRIO AIRFLOW
#
# Pacotes Python devem ser instalados como usuário airflow.
# ============================================================

USER airflow


# ============================================================
# REQUIREMENTS
# ============================================================

COPY --chown=airflow:root \
    requirements.txt \
    /opt/airflow/requirements.txt


# ============================================================
# DEPENDÊNCIAS PYTHON
#
# Mantemos explicitamente a mesma versão do Airflow usada
# pela imagem base para evitar upgrade/downgrade acidental.
#
# Também utilizamos o arquivo oficial de constraints compatível
# com a versão do Python presente na imagem.
# ============================================================

RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
    && pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /opt/airflow/requirements.txt \
        --constraint \
        "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


# ============================================================
# CÓDIGO DO PROJETO
# ============================================================

COPY --chown=airflow:root \
    src/ \
    /opt/airflow/src/


# ============================================================
# CONFIGURAÇÕES DO PROJETO
#
# config.yml
# materiais.yml
# ============================================================

COPY --chown=airflow:root \
    config/ \
    /opt/airflow/project-config/


# ============================================================
# DAGs
# ============================================================

COPY --chown=airflow:root \
    airflow/dags/ \
    /opt/airflow/dags/


# ============================================================
# WORKDIR
# ============================================================

WORKDIR /opt/airflow


# ============================================================
# USUÁRIO FINAL
# ============================================================

USER airflow