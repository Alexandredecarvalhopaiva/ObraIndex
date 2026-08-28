"""
ObraIndex - Camada Gold
=======================

Responsabilidades:
- Ler arquivos Parquet da camada Silver no MinIO.
- Selecionar registros válidos para análise.
- Calcular indicadores mensais de preço por material.
- Calcular indicadores mensais por material e UF.
- Calcular:
    * preço médio
    * preço mediano
    * preço médio ponderado por quantidade
    * mínimo e máximo
    * percentis 25 e 75
    * desvio padrão
    * coeficiente de variação
    * quantidade de compras
    * quantidade total
    * quantidade de fornecedores
    * quantidade de órgãos
    * variação mensal
    * variação anual (YoY)
    * variação desde o período-base
    * médias móveis
    * volatilidade
    * choque de preço
- Gravar os Data Marts da camada Gold no MinIO.
- Publicar os Data Marts no PostgreSQL analítico para consumo no Metabase.

Estrutura esperada:

    src/
        bronze/
            bronze.py

        silver/
            silver.py

        gold/
            gold.py

Variáveis esperadas no .env:

    MINIO_INTERNAL_ENDPOINT
    MINIO_ROOT_USER
    MINIO_ROOT_PASSWORD
    MINIO_SECURE

    MINIO_BUCKET_SILVER
    MINIO_BUCKET_GOLD

    ANALYTICS_DB_HOST
    ANALYTICS_DB_PORT
    ANALYTICS_DB_NAME
    ANALYTICS_DB_USER
    ANALYTICS_DB_PASSWORD

O código usa SQLAlchemy URL.create(), portanto a senha do PostgreSQL pode
conter caracteres especiais como "@", sem necessidade de usar a versão
URL-encoded.

Exemplo:

    python -m src.gold.gold
"""

from __future__ import annotations

import io
import logging
import os
import uuid

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd

from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL


# ============================================================
# CONFIGURAÇÃO INICIAL
# ============================================================

load_dotenv()

LOGGER = logging.getLogger("obraindex.gold")

if not LOGGER.handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# ============================================================
# EXCEÇÕES
# ============================================================


class GoldError(Exception):
    """Erro base da camada Gold."""


class GoldStorageError(GoldError):
    """Erro de leitura/gravação no MinIO."""


class GoldDatabaseError(GoldError):
    """Erro de publicação no PostgreSQL."""


class GoldQualityError(GoldError):
    """Erro relacionado à qualidade dos dados analíticos."""


# ============================================================
# CONFIGURAÇÕES
# ============================================================


@dataclass(frozen=True)
class GoldSettings:
    """Configurações da camada Gold."""

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool

    silver_bucket: str
    gold_bucket: str

    analytics_host: str
    analytics_port: int
    analytics_database: str
    analytics_user: str
    analytics_password: str

    analytics_schema: str = "obraindex"

    silver_prefix: str = "compras_gov/precos_material/"
    gold_prefix: str = "construction_material_prices/"

    base_period: str = "2020-01"
    exclude_outliers: bool = True

    price_shock_std: float = 2.0

    @classmethod
    def from_env(cls) -> "GoldSettings":
        """Carrega as configurações a partir das variáveis do ambiente."""

        required = {
            "MINIO_INTERNAL_ENDPOINT": os.getenv("MINIO_INTERNAL_ENDPOINT"),
            "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
            "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
            "MINIO_BUCKET_SILVER": os.getenv("MINIO_BUCKET_SILVER"),
            "MINIO_BUCKET_GOLD": os.getenv("MINIO_BUCKET_GOLD"),
            "ANALYTICS_DB_HOST": os.getenv("ANALYTICS_DB_HOST"),
            "ANALYTICS_DB_PORT": os.getenv("ANALYTICS_DB_PORT"),
            "ANALYTICS_DB_NAME": os.getenv("ANALYTICS_DB_NAME"),
            "ANALYTICS_DB_USER": os.getenv("ANALYTICS_DB_USER"),
            "ANALYTICS_DB_PASSWORD": os.getenv("ANALYTICS_DB_PASSWORD"),
        }

        missing = [
            key
            for key, value in required.items()
            if value in (None, "")
        ]

        if missing:
            raise GoldError(
                "Variáveis obrigatórias não configuradas: "
                + ", ".join(missing)
            )

        return cls(
            minio_endpoint=required["MINIO_INTERNAL_ENDPOINT"],
            minio_access_key=required["MINIO_ROOT_USER"],
            minio_secret_key=required["MINIO_ROOT_PASSWORD"],
            minio_secure=_to_bool(
                os.getenv("MINIO_SECURE", "false")
            ),
            silver_bucket=required["MINIO_BUCKET_SILVER"],
            gold_bucket=required["MINIO_BUCKET_GOLD"],
            analytics_host=required["ANALYTICS_DB_HOST"],
            analytics_port=int(required["ANALYTICS_DB_PORT"]),
            analytics_database=required["ANALYTICS_DB_NAME"],
            analytics_user=required["ANALYTICS_DB_USER"],
            analytics_password=required["ANALYTICS_DB_PASSWORD"],
            analytics_schema=os.getenv(
                "ANALYTICS_DB_SCHEMA",
                "obraindex",
            ),
            base_period=os.getenv(
                "OBRAINDEX_BASE_PERIOD",
                "2020-01",
            ),
            exclude_outliers=_to_bool(
                os.getenv(
                    "GOLD_EXCLUDE_OUTLIERS",
                    "true",
                )
            ),
            price_shock_std=float(
                os.getenv(
                    "GOLD_PRICE_SHOCK_STD",
                    "2.0",
                )
            ),
        )


# ============================================================
# UTILITÁRIOS
# ============================================================


def _to_bool(value: str | bool | None) -> bool:
    """Converte valores textuais para booleano."""

    if isinstance(value, bool):
        return value

    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def safe_nunique(series: pd.Series) -> int:
    """Conta valores únicos ignorando nulos."""

    return int(series.dropna().nunique())


def weighted_average(
    values: pd.Series,
    weights: pd.Series,
) -> float:
    """
    Calcula média ponderada de forma segura.

    Retorna NaN quando não existem valores/pesos válidos.
    """

    mask = (
        values.notna()
        & weights.notna()
        & weights.gt(0)
    )

    if not mask.any():
        return np.nan

    filtered_values = values.loc[mask].astype(float)
    filtered_weights = weights.loc[mask].astype(float)

    weight_sum = filtered_weights.sum()

    if weight_sum <= 0:
        return np.nan

    return float(
        np.average(
            filtered_values,
            weights=filtered_weights,
        )
    )


# ============================================================
# MINIO
# ============================================================


def get_minio_client(
    settings: GoldSettings,
) -> Minio:
    """Cria o cliente MinIO."""

    return Minio(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def ensure_bucket(
    client: Minio,
    bucket: str,
) -> None:
    """Garante que um bucket exista."""

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

            LOGGER.info(
                "Bucket criado: %s",
                bucket,
            )

    except Exception as exc:
        raise GoldStorageError(
            f"Erro ao validar/criar bucket {bucket}: {exc}"
        ) from exc


def list_parquet_objects(
    client: Minio,
    *,
    bucket: str,
    prefix: str,
) -> list[str]:
    """Lista arquivos Parquet de um prefixo."""

    try:
        return [
            item.object_name
            for item in client.list_objects(
                bucket,
                prefix=prefix,
                recursive=True,
            )
            if item.object_name.endswith(".parquet")
        ]

    except Exception as exc:
        raise GoldStorageError(
            f"Erro ao listar objetos em {bucket}/{prefix}: {exc}"
        ) from exc


def read_parquet_object(
    client: Minio,
    *,
    bucket: str,
    object_name: str,
) -> pd.DataFrame:
    """Lê um Parquet do MinIO."""

    response = None

    try:
        response = client.get_object(
            bucket,
            object_name,
        )

        data = response.read()

        return pd.read_parquet(
            io.BytesIO(data),
            engine="pyarrow",
        )

    except Exception as exc:
        raise GoldStorageError(
            f"Erro ao ler {bucket}/{object_name}: {exc}"
        ) from exc

    finally:
        if response is not None:
            response.close()
            response.release_conn()


def read_silver(
    *,
    client: Minio,
    settings: GoldSettings,
    object_names: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Lê e concatena os arquivos Silver."""

    if object_names is None:
        object_names = list_parquet_objects(
            client,
            bucket=settings.silver_bucket,
            prefix=settings.silver_prefix,
        )

    object_names = list(object_names)

    if not object_names:
        raise GoldError(
            "Nenhum arquivo Parquet encontrado na camada Silver."
        )

    frames: list[pd.DataFrame] = []

    for object_name in object_names:
        LOGGER.info(
            "Lendo Silver | objeto=%s",
            object_name,
        )

        frame = read_parquet_object(
            client,
            bucket=settings.silver_bucket,
            object_name=object_name,
        )

        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )


def dataframe_to_parquet_bytes(
    df: pd.DataFrame,
) -> bytes:
    """Converte DataFrame para Parquet em memória."""

    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )

    buffer.seek(0)

    return buffer.getvalue()


def save_gold_dataframe(
    df: pd.DataFrame,
    *,
    client: Minio,
    bucket: str,
    object_name: str,
) -> int:
    """
    Salva um Data Mart Gold.

    O nome do objeto é determinístico, portanto cada execução substitui
    o snapshot anterior e mantém o processo idempotente.
    """

    if df.empty:
        LOGGER.warning(
            "DataFrame Gold vazio: %s",
            object_name,
        )
        return 0

    try:
        data = dataframe_to_parquet_bytes(df)

        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/octet-stream",
        )

        LOGGER.info(
            "Gold salva | bucket=%s | objeto=%s | registros=%s",
            bucket,
            object_name,
            len(df),
        )

        return len(df)

    except Exception as exc:
        raise GoldStorageError(
            f"Erro ao salvar {object_name}: {exc}"
        ) from exc


# ============================================================
# PREPARAÇÃO DA SILVER
# ============================================================


def prepare_silver_for_gold(
    df: pd.DataFrame,
    *,
    exclude_outliers: bool = True,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Seleciona os registros elegíveis para os cálculos analíticos.

    Regras:
    - is_valid_record deve ser True, quando a coluna existir.
    - preço deve ser positivo.
    - quantidade deve ser positiva.
    - CATMAT e data devem existir.
    - outliers podem ser excluídos dos indicadores.

    Os outliers continuam preservados na Silver.
    """

    if df.empty:
        return df.copy(), {
            "records_input": 0,
            "records_valid": 0,
            "records_invalid": 0,
            "records_outlier_excluded": 0,
        }

    output = df.copy()

    required = [
        "codigo_item_catalogo",
        "data_compra",
        "preco_normalizado",
        "quantidade",
    ]

    missing = [
        column
        for column in required
        if column not in output.columns
    ]

    if missing:
        raise GoldQualityError(
            "Colunas obrigatórias ausentes na Silver: "
            + ", ".join(missing)
        )

    output["data_compra"] = pd.to_datetime(
        output["data_compra"],
        errors="coerce",
    )

    output["preco_normalizado"] = pd.to_numeric(
        output["preco_normalizado"],
        errors="coerce",
    )

    output["quantidade"] = pd.to_numeric(
        output["quantidade"],
        errors="coerce",
    )

    records_input = len(output)

    valid_mask = (
        output["codigo_item_catalogo"].notna()
        & output["data_compra"].notna()
        & output["preco_normalizado"].notna()
        & output["preco_normalizado"].gt(0)
        & output["quantidade"].notna()
        & output["quantidade"].gt(0)
    )

    if "is_valid_record" in output.columns:
        valid_mask &= output["is_valid_record"].fillna(False)

    valid = output.loc[valid_mask].copy()

    invalid_count = records_input - len(valid)

    outlier_excluded = 0

    if (
        exclude_outliers
        and "is_outlier" in valid.columns
    ):
        outlier_mask = (
            valid["is_outlier"]
            .fillna(False)
            .astype(bool)
        )

        outlier_excluded = int(
            outlier_mask.sum()
        )

        valid = valid.loc[
            ~outlier_mask
        ].copy()

    LOGGER.info(
        "Silver preparada para Gold | entrada=%s | válidos=%s | "
        "inválidos=%s | outliers_excluídos=%s",
        records_input,
        len(valid),
        invalid_count,
        outlier_excluded,
    )

    return valid, {
        "records_input": records_input,
        "records_valid": len(valid),
        "records_invalid": invalid_count,
        "records_outlier_excluded": outlier_excluded,
    }


# ============================================================
# AGREGAÇÃO
# ============================================================


def aggregate_group(
    group: pd.DataFrame,
) -> pd.Series:
    """Calcula as métricas básicas de um grupo."""

    prices = group["preco_normalizado"]
    quantities = group["quantidade"]

    return pd.Series(
        {
            "preco_medio": prices.mean(),
            "preco_mediano": prices.median(),
            "preco_medio_ponderado": weighted_average(
                prices,
                quantities,
            ),
            "preco_minimo": prices.min(),
            "preco_maximo": prices.max(),
            "percentil_25": prices.quantile(0.25),
            "percentil_75": prices.quantile(0.75),
            "desvio_padrao": prices.std(ddof=1),
            "quantidade_compras": len(group),
            "quantidade_unidades": quantities.sum(),
            "quantidade_fornecedores": (
                safe_nunique(group["ni_fornecedor"])
                if "ni_fornecedor" in group.columns
                else 0
            ),
            "quantidade_orgaos": (
                safe_nunique(group["codigo_orgao"])
                if "codigo_orgao" in group.columns
                else 0
            ),
        }
    )


def add_coefficient_variation(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Adiciona o coeficiente de variação em percentual."""

    output = df.copy()

    output["coeficiente_variacao_pct"] = np.where(
        output["preco_medio"].gt(0),
        (
            output["desvio_padrao"]
            / output["preco_medio"]
        )
        * 100,
        np.nan,
    )

    return output


# ============================================================
# DATA MART MENSAL
# ============================================================


def build_material_monthly(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cria o Data Mart mensal por material.

    Granularidade:
        material + CATMAT + ano/mês
    """

    if df.empty:
        return pd.DataFrame()

    output = df.copy()

    output["ano"] = (
        output["data_compra"]
        .dt.year
        .astype("Int64")
    )

    output["mes"] = (
        output["data_compra"]
        .dt.month
        .astype("Int64")
    )

    output["ano_mes"] = (
        output["data_compra"]
        .dt.to_period("M")
        .astype("string")
    )

    dimensions = [
        "codigo_item_catalogo",
        "material_id",
        "material",
        "categoria_material",
        "unidade_normalizada",
        "ano",
        "mes",
        "ano_mes",
    ]

    for column in dimensions:
        if column not in output.columns:
            output[column] = pd.NA

    grouped = (
        output.groupby(
            dimensions,
            dropna=False,
            observed=True,
        )
        .apply(
            aggregate_group,
            include_groups=False,
        )
        .reset_index()
    )

    grouped = add_coefficient_variation(
        grouped
    )

    return grouped


# ============================================================
# DATA MART REGIONAL
# ============================================================


def build_material_state_monthly(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cria o Data Mart mensal por material e estado.

    Granularidade:
        material + CATMAT + UF + ano/mês
    """

    if df.empty:
        return pd.DataFrame()

    output = df.copy()

    output["ano"] = (
        output["data_compra"]
        .dt.year
        .astype("Int64")
    )

    output["mes"] = (
        output["data_compra"]
        .dt.month
        .astype("Int64")
    )

    output["ano_mes"] = (
        output["data_compra"]
        .dt.to_period("M")
        .astype("string")
    )

    dimensions = [
        "codigo_item_catalogo",
        "material_id",
        "material",
        "categoria_material",
        "unidade_normalizada",
        "uf",
        "regiao",
        "ano",
        "mes",
        "ano_mes",
    ]

    for column in dimensions:
        if column not in output.columns:
            output[column] = pd.NA

    grouped = (
        output.groupby(
            dimensions,
            dropna=False,
            observed=True,
        )
        .apply(
            aggregate_group,
            include_groups=False,
        )
        .reset_index()
    )

    grouped = add_coefficient_variation(
        grouped
    )

    return grouped


# ============================================================
# INDICADORES TEMPORAIS
# ============================================================


def add_time_indicators(
    df: pd.DataFrame,
    *,
    group_columns: list[str],
    base_period: str = "2020-01",
    price_column: str = "preco_mediano",
    price_shock_std: float = 2.0,
) -> pd.DataFrame:
    """
    Adiciona indicadores temporais.

    Inclui:
    - variação mensal (MoM)
    - variação anual (YoY)
    - variação desde período-base
    - médias móveis 3/6/12 meses
    - volatilidade 6/12 meses
    - choque de preço
    """

    if df.empty:
        return df.copy()

    output = df.copy()

    output["_period"] = pd.PeriodIndex(
        output["ano_mes"],
        freq="M",
    )

    output = output.sort_values(
        group_columns + ["_period"]
    ).reset_index(drop=True)

    grouped = output.groupby(
        group_columns,
        dropna=False,
        observed=True,
        group_keys=False,
    )

    # --------------------------------------------------------
    # Variações
    # --------------------------------------------------------

    output["variacao_mensal_pct"] = (
        grouped[price_column]
        .pct_change(fill_method=None)
        * 100
    )

    output["variacao_yoy_pct"] = (
        grouped[price_column]
        .pct_change(
            periods=12,
            fill_method=None,
        )
        * 100
    )

    # --------------------------------------------------------
    # Médias móveis
    # --------------------------------------------------------

    for window in (3, 6, 12):
        output[
            f"media_movel_{window}m"
        ] = grouped[price_column].transform(
            lambda series, w=window: (
                series.rolling(
                    window=w,
                    min_periods=1,
                ).mean()
            )
        )

    # --------------------------------------------------------
    # Volatilidade
    #
    # Desvio padrão das variações mensais, em pontos percentuais.
    # --------------------------------------------------------

    for window in (6, 12):
        output[
            f"volatilidade_{window}m"
        ] = grouped[
            "variacao_mensal_pct"
        ].transform(
            lambda series, w=window: (
                series.rolling(
                    window=w,
                    min_periods=2,
                ).std()
            )
        )

    # --------------------------------------------------------
    # Base histórica
    # --------------------------------------------------------

    def calculate_base_change(
        group: pd.DataFrame,
    ) -> pd.DataFrame:
        group = group.copy()

        base_rows = group.loc[
            group["ano_mes"] == base_period,
            price_column,
        ]

        # Caso o material não tenha exatamente o período-base,
        # utiliza a primeira observação disponível da série.
        if base_rows.empty:
            base_price = group[
                price_column
            ].dropna().iloc[0] if group[
                price_column
            ].notna().any() else np.nan

            base_period_used = (
                group.loc[
                    group[price_column].notna(),
                    "ano_mes",
                ].iloc[0]
                if group[price_column].notna().any()
                else pd.NA
            )

        else:
            base_price = float(
                base_rows.iloc[0]
            )
            base_period_used = base_period

        group["preco_base"] = base_price
        group["periodo_base_utilizado"] = (
            base_period_used
        )

        group[
            "variacao_desde_base_pct"
        ] = np.where(
            pd.notna(base_price)
            and base_price > 0,
            (
                (
                    group[price_column]
                    / base_price
                )
                - 1
            )
            * 100,
            np.nan,
        )

        group["indice_base_100"] = np.where(
            pd.notna(base_price)
            and base_price > 0,
            (
                group[price_column]
                / base_price
            )
            * 100,
            np.nan,
        )

        return group

    output = (
        output.groupby(
            group_columns,
            dropna=False,
            observed=True,
            group_keys=False,
        )
        .apply(
            calculate_base_change,
            include_groups=False,
        )
        .reset_index(drop=True)
    )

    # --------------------------------------------------------
    # Choque de preço
    #
    # Um choque é marcado quando a variação mensal fica acima
    # ou abaixo de N desvios padrão históricos daquele material.
    # --------------------------------------------------------

    def add_shock(
        group: pd.DataFrame,
    ) -> pd.DataFrame:
        group = group.copy()

        mean_change = group[
            "variacao_mensal_pct"
        ].mean()

        std_change = group[
            "variacao_mensal_pct"
        ].std()

        if (
            pd.isna(std_change)
            or std_change <= 0
        ):
            group["is_price_shock"] = False
            return group

        upper = (
            mean_change
            + price_shock_std * std_change
        )

        lower = (
            mean_change
            - price_shock_std * std_change
        )

        group["is_price_shock"] = (
            (
                group[
                    "variacao_mensal_pct"
                ] > upper
            )
            | (
                group[
                    "variacao_mensal_pct"
                ] < lower
            )
        ).fillna(False)

        return group

    output = (
        output.groupby(
            group_columns,
            dropna=False,
            observed=True,
            group_keys=False,
        )
        .apply(
            add_shock,
            include_groups=False,
        )
        .reset_index(drop=True)
    )

    output = output.drop(
        columns=["_period"],
        errors="ignore",
    )

    return output


# ============================================================
# VALIDAÇÃO GOLD
# ============================================================


def validate_gold(
    df: pd.DataFrame,
    *,
    table_name: str,
) -> dict[str, Any]:
    """Executa validações básicas dos Data Marts Gold."""

    if df.empty:
        raise GoldQualityError(
            f"A tabela {table_name} ficou vazia."
        )

    checks = {
        "records": len(df),
        "negative_median_prices": 0,
        "invalid_purchase_counts": 0,
        "null_catmat": 0,
        "null_period": 0,
    }

    if "preco_mediano" in df.columns:
        checks["negative_median_prices"] = int(
            df["preco_mediano"]
            .lt(0)
            .fillna(False)
            .sum()
        )

    if "quantidade_compras" in df.columns:
        checks["invalid_purchase_counts"] = int(
            df["quantidade_compras"]
            .lt(1)
            .fillna(False)
            .sum()
        )

    checks["null_catmat"] = int(
        df["codigo_item_catalogo"]
        .isna()
        .sum()
    )

    checks["null_period"] = int(
        df["ano_mes"]
        .isna()
        .sum()
    )

    critical = (
        checks["negative_median_prices"]
        + checks["invalid_purchase_counts"]
        + checks["null_catmat"]
        + checks["null_period"]
    )

    if critical > 0:
        raise GoldQualityError(
            f"Falha de qualidade em {table_name}: {checks}"
        )

    LOGGER.info(
        "Gold validada | tabela=%s | checks=%s",
        table_name,
        checks,
    )

    return checks


# ============================================================
# POSTGRESQL
# ============================================================


def get_postgres_engine(
    settings: GoldSettings,
) -> Engine:
    """
    Cria a conexão SQLAlchemy.

    URL.create() evita problemas com senhas contendo caracteres
    especiais, por exemplo: Mudar@123.
    """

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=settings.analytics_user,
        password=settings.analytics_password,
        host=settings.analytics_host,
        port=settings.analytics_port,
        database=settings.analytics_database,
    )

    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
    )


def ensure_analytics_schema(
    engine: Engine,
    schema: str,
) -> None:
    """Cria o schema analítico caso ainda não exista."""

    # O schema vem da configuração do projeto, e não de input do usuário.
    safe_schema = (
        schema.replace('"', "")
        .replace(";", "")
        .strip()
    )

    if not safe_schema:
        raise GoldDatabaseError(
            "Nome de schema inválido."
        )

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f'CREATE SCHEMA IF NOT EXISTS "{safe_schema}"'
                )
            )

    except Exception as exc:
        raise GoldDatabaseError(
            f"Erro ao criar schema {schema}: {exc}"
        ) from exc


def publish_dataframe(
    df: pd.DataFrame,
    *,
    engine: Engine,
    schema: str,
    table_name: str,
) -> int:
    """
    Publica um DataFrame no PostgreSQL.

    Para o MVP, usamos replace:
    - simples;
    - idempotente;
    - adequado aos Data Marts Gold recalculados integralmente.

    Em uma futura evolução, pode ser substituído por UPSERT/MERGE.
    """

    if df.empty:
        LOGGER.warning(
            "Tabela %s vazia. Publicação ignorada.",
            table_name,
        )
        return 0

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

        LOGGER.info(
            "PostgreSQL atualizado | %s.%s | registros=%s",
            schema,
            table_name,
            len(df),
        )

        return len(df)

    except Exception as exc:
        raise GoldDatabaseError(
            f"Erro ao publicar {schema}.{table_name}: {exc}"
        ) from exc


# ============================================================
# CONSTRUÇÃO DA GOLD
# ============================================================


def build_gold(
    silver_df: pd.DataFrame,
    *,
    settings: GoldSettings,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    dict[str, Any],
]:
    """
    Constrói os Data Marts Gold principais.
    """

    valid_df, preparation_metrics = (
        prepare_silver_for_gold(
            silver_df,
            exclude_outliers=(
                settings.exclude_outliers
            ),
        )
    )

    if valid_df.empty:
        raise GoldQualityError(
            "Nenhum registro válido disponível para construção da Gold."
        )

    monthly = build_material_monthly(
        valid_df
    )

    monthly = add_time_indicators(
        monthly,
        group_columns=[
            "codigo_item_catalogo",
            "material_id",
        ],
        base_period=settings.base_period,
        price_shock_std=settings.price_shock_std,
    )

    regional = build_material_state_monthly(
        valid_df
    )

    regional = add_time_indicators(
        regional,
        group_columns=[
            "codigo_item_catalogo",
            "material_id",
            "uf",
        ],
        base_period=settings.base_period,
        price_shock_std=settings.price_shock_std,
    )

    processing_timestamp = datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    monthly["_gold_processed_at"] = (
        processing_timestamp
    )

    regional["_gold_processed_at"] = (
        processing_timestamp
    )

    monthly_quality = validate_gold(
        monthly,
        table_name="material_price_monthly",
    )

    regional_quality = validate_gold(
        regional,
        table_name="material_price_state_monthly",
    )

    metrics = {
        **preparation_metrics,
        "monthly_records": len(monthly),
        "regional_records": len(regional),
        "monthly_quality": monthly_quality,
        "regional_quality": regional_quality,
    }

    return (
        monthly,
        regional,
        metrics,
    )


# ============================================================
# EXTRAÇÃO DOS OBJETOS DO RESULTADO SILVER
# ============================================================


def collect_silver_objects(
    silver_result: dict[str, Any] | list[dict[str, Any]],
) -> list[str]:
    """
    Extrai nomes de objetos a partir do resultado de silver_load().
    """

    objects: list[str] = []

    def collect(item: Any) -> None:
        if not isinstance(item, dict):
            return

        values = item.get("objects", [])

        if isinstance(values, list):
            for value in values:
                if isinstance(value, str):
                    objects.append(value)

                elif (
                    isinstance(value, dict)
                    and value.get("object_name")
                ):
                    objects.append(
                        value["object_name"]
                    )

        nested = item.get("results", [])

        if isinstance(nested, list):
            for value in nested:
                collect(value)

    if isinstance(silver_result, list):
        for item in silver_result:
            collect(item)
    else:
        collect(silver_result)

    return list(
        dict.fromkeys(objects)
    )


# ============================================================
# GOLD LOAD
# ============================================================


def gold_load(
    silver_result: dict[str, Any] | list[dict[str, Any]] | None = None,
    *,
    settings: GoldSettings | None = None,
    publish_postgres: bool = True,
    run_id: str | None = None,
) -> dict[str, Any]:
    """
    Executa toda a camada Gold.

    Se silver_result for informado:
        lê somente os objetos produzidos pela execução Silver.

    Se silver_result for None:
        lê todos os Parquets atualmente existentes na Silver.

    Para consistência dos indicadores de série temporal, recomenda-se
    processar a Silver histórica completa quando forem calculadas
    variações, médias móveis e volatilidade.
    """

    settings = settings or GoldSettings.from_env()

    run_id = run_id or uuid.uuid4().hex

    client = get_minio_client(settings)

    ensure_bucket(
        client,
        settings.silver_bucket,
    )

    ensure_bucket(
        client,
        settings.gold_bucket,
    )

    started_at = datetime.now(timezone.utc)

    # --------------------------------------------------------
    # Série temporal:
    #
    # Para MoM, YoY, médias móveis e volatilidade fazerem sentido,
    # a Gold é reconstruída usando o histórico completo da Silver.
    #
    # silver_result é usado apenas para rastreabilidade/log.
    # --------------------------------------------------------

    produced_objects = (
        collect_silver_objects(silver_result)
        if silver_result is not None
        else []
    )

    silver_objects = list_parquet_objects(
        client,
        bucket=settings.silver_bucket,
        prefix=settings.silver_prefix,
    )

    LOGGER.info(
        "Construindo Gold | silver_total=%s | silver_execucao_atual=%s",
        len(silver_objects),
        len(produced_objects),
    )

    silver_df = read_silver(
        client=client,
        settings=settings,
        object_names=silver_objects,
    )

    (
        monthly,
        regional,
        metrics,
    ) = build_gold(
        silver_df,
        settings=settings,
    )

    # --------------------------------------------------------
    # MinIO Gold
    #
    # Snapshot determinístico.
    # put_object substitui o objeto anterior.
    # --------------------------------------------------------

    monthly_object = (
        settings.gold_prefix
        + "material_price_monthly.parquet"
    )

    regional_object = (
        settings.gold_prefix
        + "material_price_state_monthly.parquet"
    )

    monthly_written = save_gold_dataframe(
        monthly,
        client=client,
        bucket=settings.gold_bucket,
        object_name=monthly_object,
    )

    regional_written = save_gold_dataframe(
        regional,
        client=client,
        bucket=settings.gold_bucket,
        object_name=regional_object,
    )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    postgres_result: dict[str, int] = {}

    if publish_postgres:
        engine = get_postgres_engine(
            settings
        )

        try:
            ensure_analytics_schema(
                engine,
                settings.analytics_schema,
            )

            postgres_result = {
                "material_price_monthly": (
                    publish_dataframe(
                        monthly,
                        engine=engine,
                        schema=settings.analytics_schema,
                        table_name="material_price_monthly",
                    )
                ),
                "material_price_state_monthly": (
                    publish_dataframe(
                        regional,
                        engine=engine,
                        schema=settings.analytics_schema,
                        table_name="material_price_state_monthly",
                    )
                ),
            }

        finally:
            engine.dispose()

    finished_at = datetime.now(timezone.utc)

    result = {
        "run_id": run_id,
        "status": "SUCCESS",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": (
            finished_at - started_at
        ).total_seconds(),
        "silver_objects_read": len(
            silver_objects
        ),
        "silver_objects_current_run": len(
            produced_objects
        ),
        "gold_objects": {
            "material_price_monthly": (
                monthly_object
            ),
            "material_price_state_monthly": (
                regional_object
            ),
        },
        "gold_records": {
            "material_price_monthly": (
                monthly_written
            ),
            "material_price_state_monthly": (
                regional_written
            ),
        },
        "postgres": postgres_result,
        "metrics": metrics,
    }

    LOGGER.info(
        "Gold concluída | mensal=%s | regional=%s | duração=%ss",
        monthly_written,
        regional_written,
        result["duration_seconds"],
    )

    return result


# ============================================================
# TESTE DE CONEXÕES
# ============================================================


def test_gold_connections(
    settings: GoldSettings | None = None,
) -> dict[str, bool]:
    """Testa MinIO e PostgreSQL analítico."""

    settings = settings or GoldSettings.from_env()

    result = {
        "minio": False,
        "silver_bucket": False,
        "gold_bucket": False,
        "postgres": False,
    }

    # --------------------------------------------------------
    # MinIO
    # --------------------------------------------------------

    try:
        client = get_minio_client(
            settings
        )

        client.list_buckets()

        result["minio"] = True

        result["silver_bucket"] = (
            client.bucket_exists(
                settings.silver_bucket
            )
        )

        ensure_bucket(
            client,
            settings.gold_bucket,
        )

        result["gold_bucket"] = (
            client.bucket_exists(
                settings.gold_bucket
            )
        )

    except Exception:
        LOGGER.exception(
            "Falha ao testar MinIO da Gold."
        )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    try:
        engine = get_postgres_engine(
            settings
        )

        with engine.connect() as connection:
            connection.execute(
                text("SELECT 1")
            )

        result["postgres"] = True

        engine.dispose()

    except Exception:
        LOGGER.exception(
            "Falha ao testar PostgreSQL analítico."
        )

    return result


# ============================================================
# EXECUÇÃO MANUAL
# ============================================================


if __name__ == "__main__":

    try:
        config = GoldSettings.from_env()

        connection_status = test_gold_connections(
            config
        )

        LOGGER.info(
            "Conexões Gold: %s",
            connection_status,
        )

        if not all(
            connection_status.values()
        ):
            raise GoldError(
                "Uma ou mais conexões obrigatórias falharam."
            )

        result = gold_load(
            settings=config,
            publish_postgres=True,
        )

        print(result)

    except Exception as exc:
        LOGGER.exception(
            "Falha na camada Gold: %s",
            exc,
        )
        raise
