"""
ObraIndex - Camada Silver
=========================

Responsabilidades:
- Ler arquivos Parquet da camada Bronze no MinIO.
- Padronizar nomes de colunas.
- Tipar datas, quantidades e preços.
- Aplicar o recorte histórico do ObraIndex (a partir de 2020).
- Remover duplicidades.
- Enriquecer os registros com o catálogo local de materiais.
- Criar colunas derivadas para análise.
- Identificar outliers sem apagar o dado original.
- Executar validações básicas de qualidade.
- Gravar a camada Silver em Parquet no MinIO.

Estrutura esperada:

    src/
        bronze/
            bronze.py

        silver/
            silver.py

    config/
        materiais.yml

Variáveis esperadas no .env:

    OBRAINDEX_START_DATE

    MINIO_INTERNAL_ENDPOINT
    MINIO_ROOT_USER
    MINIO_ROOT_PASSWORD
    MINIO_SECURE

    MINIO_BUCKET_BRONZE
    MINIO_BUCKET_SILVER

Exemplo de uso:

    python -m src.silver.silver
"""

from __future__ import annotations

import io
import logging
import os
import re
import uuid

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import yaml

from dotenv import load_dotenv
from minio import Minio


# ============================================================
# CONFIGURAÇÃO INICIAL
# ============================================================

load_dotenv()

LOGGER = logging.getLogger("obraindex.silver")

if not LOGGER.handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# ============================================================
# EXCEÇÕES
# ============================================================


class SilverError(Exception):
    """Erro base da camada Silver."""


class SilverStorageError(SilverError):
    """Erro relacionado ao MinIO ou arquivos Parquet."""


class SilverQualityError(SilverError):
    """Erro de qualidade de dados da camada Silver."""


# ============================================================
# CONFIGURAÇÕES
# ============================================================


@dataclass(frozen=True)
class SilverSettings:
    """Configurações necessárias para a camada Silver."""

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool

    bronze_bucket: str
    silver_bucket: str

    start_date: str = "2020-01-01"

    bronze_prefix: str = "compras_gov/precos_material/"
    silver_prefix: str = "compras_gov/precos_material/"

    outlier_iqr_multiplier: float = 1.5
    minimum_outlier_group_size: int = 10

    @classmethod
    def from_env(cls) -> "SilverSettings":
        """Cria as configurações usando variáveis do ambiente."""

        required = {
            "MINIO_INTERNAL_ENDPOINT": os.getenv("MINIO_INTERNAL_ENDPOINT"),
            "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
            "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
            "MINIO_BUCKET_BRONZE": os.getenv("MINIO_BUCKET_BRONZE"),
            "MINIO_BUCKET_SILVER": os.getenv("MINIO_BUCKET_SILVER"),
        }

        missing = [
            key
            for key, value in required.items()
            if not value
        ]

        if missing:
            raise SilverError(
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
            bronze_bucket=required["MINIO_BUCKET_BRONZE"],
            silver_bucket=required["MINIO_BUCKET_SILVER"],
            start_date=os.getenv(
                "OBRAINDEX_START_DATE",
                "2020-01-01",
            ),
            outlier_iqr_multiplier=float(
                os.getenv(
                    "SILVER_OUTLIER_IQR_MULTIPLIER",
                    "1.5",
                )
            ),
            minimum_outlier_group_size=int(
                os.getenv(
                    "SILVER_OUTLIER_MIN_GROUP_SIZE",
                    "10",
                )
            ),
        )


# ============================================================
# CONSTANTES
# ============================================================


REGION_BY_UF = {
    "AC": "Norte",
    "AP": "Norte",
    "AM": "Norte",
    "PA": "Norte",
    "RO": "Norte",
    "RR": "Norte",
    "TO": "Norte",
    "AL": "Nordeste",
    "BA": "Nordeste",
    "CE": "Nordeste",
    "MA": "Nordeste",
    "PB": "Nordeste",
    "PE": "Nordeste",
    "PI": "Nordeste",
    "RN": "Nordeste",
    "SE": "Nordeste",
    "DF": "Centro-Oeste",
    "GO": "Centro-Oeste",
    "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    "ES": "Sudeste",
    "MG": "Sudeste",
    "RJ": "Sudeste",
    "SP": "Sudeste",
    "PR": "Sul",
    "RS": "Sul",
    "SC": "Sul",
}


COLUMN_ALIASES = {
    "codigo_item_catalogo": (
        "codigo_item_catalogo",
        "codigoitemcatalogo",
        "catmat",
        "_catmat",
    ),
    "descricao_item": (
        "descricao_item",
        "descricaoitem",
        "descricao",
    ),
    "unidade_original": (
        "nome_unidade_medida",
        "nomeunidademedida",
        "sigla_unidade_medida",
        "siglaunidademedida",
        "unidade",
        "unidade_medida",
    ),
    "quantidade": (
        "quantidade",
        "qtd",
    ),
    "preco_unitario": (
        "preco_unitario",
        "precounitario",
        "valor_unitario",
        "valorunitario",
    ),
    "data_compra": (
        "data_compra",
        "datacompra",
    ),
    "data_resultado": (
        "data_resultado",
        "dataresultado",
    ),
    "data_hora_atualizacao_compra": (
        "data_hora_atualizacao_compra",
        "datahoraatualizacaocompra",
    ),
    "data_hora_atualizacao_item": (
        "data_hora_atualizacao_item",
        "datahoraatualizacaoitem",
    ),
    "uf": (
        "estado",
        "uf",
        "sigla_uf",
        "siglauf",
    ),
    "municipio": (
        "municipio",
        "nome_municipio",
        "nomemunicipio",
    ),
    "codigo_municipio": (
        "codigo_municipio",
        "codigomunicipio",
    ),
    "codigo_uasg": (
        "codigo_uasg",
        "codigouasg",
    ),
    "nome_uasg": (
        "nome_uasg",
        "nomeuasg",
    ),
    "codigo_orgao": (
        "codigo_orgao",
        "codigoorgao",
    ),
    "nome_orgao": (
        "nome_orgao",
        "nomeorgao",
    ),
    "ni_fornecedor": (
        "ni_fornecedor",
        "nifornecedor",
        "cnpj_cpf_fornecedor",
    ),
    "nome_fornecedor": (
        "nome_fornecedor",
        "nomefornecedor",
    ),
    "modalidade": (
        "modalidade",
        "nome_modalidade",
        "nomemodalidade",
    ),
    "esfera": (
        "esfera",
    ),
    "poder": (
        "poder",
    ),
    "id_compra": (
        "id_compra",
        "idcompra",
    ),
    "id_item_compra": (
        "id_item_compra",
        "iditemcompra",
    ),
    "record_hash": (
        "_record_hash",
        "record_hash",
    ),
    "pipeline_run_id": (
        "_pipeline_run_id",
        "pipeline_run_id",
    ),
    "ingestion_timestamp": (
        "_ingestion_timestamp",
        "ingestion_timestamp",
    ),
    "extract_type": (
        "_extract_type",
        "extract_type",
    ),
}


# ============================================================
# UTILITÁRIOS
# ============================================================


def _to_bool(value: str | bool | None) -> bool:
    """Converte texto para booleano."""

    if isinstance(value, bool):
        return value

    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def to_snake_case(value: str) -> str:
    """
    Converte nomes de colunas para snake_case.

    Preserva o conteúdo; altera apenas o nome técnico da coluna.
    """

    value = str(value).strip()

    leading_underscore = value.startswith("_")

    value = re.sub(
        r"([a-z0-9])([A-Z])",
        r"\1_\2",
        value,
    )
    value = re.sub(
        r"[^a-zA-Z0-9]+",
        "_",
        value,
    )
    value = re.sub(
        r"_+",
        "_",
        value,
    )
    value = value.strip("_").lower()

    if leading_underscore:
        return f"_{value}"

    return value


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza todos os nomes das colunas."""

    output = df.copy()

    output.columns = [
        to_snake_case(column)
        for column in output.columns
    ]

    return output


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Carrega um arquivo YAML."""

    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {file_path}"
        )

    with file_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return yaml.safe_load(file) or {}


def load_materials(
    path: str | Path = "config/materiais.yml",
) -> list[dict[str, Any]]:
    """Carrega materiais ativos do materiais.yml."""

    config = load_yaml(path)

    materials = (
        config.get("materiais")
        or config.get("materials")
        or []
    )

    if not isinstance(materials, list):
        raise SilverError(
            "O arquivo materiais.yml deve possuir "
            "uma lista 'materiais' ou 'materials'."
        )

    return [
        material
        for material in materials
        if material.get(
            "ativo",
            material.get("active", True),
        )
    ]


def normalize_catmat(value: Any) -> str | None:
    """Padroniza o CATMAT como string sem '.0'."""

    if pd.isna(value):
        return None

    text = str(value).strip()

    if text.endswith(".0"):
        text = text[:-2]

    return text or None


# ============================================================
# MINIO
# ============================================================


def get_minio_client(
    settings: SilverSettings,
) -> Minio:
    """Cria cliente MinIO."""

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
    """Garante a existência do bucket."""

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            LOGGER.info(
                "Bucket criado: %s",
                bucket,
            )

    except Exception as exc:
        raise SilverStorageError(
            f"Erro ao validar/criar bucket {bucket}: {exc}"
        ) from exc


def list_parquet_objects(
    client: Minio,
    *,
    bucket: str,
    prefix: str,
) -> list[str]:
    """Lista objetos Parquet de um prefixo do MinIO."""

    try:
        objects = client.list_objects(
            bucket,
            prefix=prefix,
            recursive=True,
        )

        return [
            item.object_name
            for item in objects
            if item.object_name.endswith(".parquet")
        ]

    except Exception as exc:
        raise SilverStorageError(
            f"Erro ao listar objetos do bucket {bucket}: {exc}"
        ) from exc


def read_parquet_object(
    client: Minio,
    *,
    bucket: str,
    object_name: str,
) -> pd.DataFrame:
    """Lê um Parquet diretamente do MinIO."""

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
        raise SilverStorageError(
            f"Erro ao ler {bucket}/{object_name}: {exc}"
        ) from exc

    finally:
        if response is not None:
            response.close()
            response.release_conn()


def read_bronze_objects(
    client: Minio,
    *,
    bucket: str,
    object_names: Iterable[str],
) -> pd.DataFrame:
    """Lê e concatena uma lista de arquivos Bronze."""

    frames: list[pd.DataFrame] = []

    for object_name in object_names:
        LOGGER.info(
            "Lendo Bronze | objeto=%s",
            object_name,
        )

        frame = read_parquet_object(
            client,
            bucket=bucket,
            object_name=object_name,
        )

        frame["_bronze_object"] = object_name

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
    """Converte DataFrame em Parquet na memória."""

    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )

    buffer.seek(0)

    return buffer.getvalue()


def save_dataframe_to_minio(
    df: pd.DataFrame,
    *,
    client: Minio,
    bucket: str,
    object_name: str,
) -> int:
    """Grava um DataFrame em Parquet no MinIO."""

    if df.empty:
        LOGGER.warning(
            "DataFrame vazio. Objeto não será gravado: %s",
            object_name,
        )
        return 0

    try:
        parquet_data = dataframe_to_parquet_bytes(df)

        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=io.BytesIO(parquet_data),
            length=len(parquet_data),
            content_type="application/octet-stream",
        )

        LOGGER.info(
            "Silver salva | bucket=%s | objeto=%s | registros=%s",
            bucket,
            object_name,
            len(df),
        )

        return len(df)

    except Exception as exc:
        raise SilverStorageError(
            f"Erro ao salvar {object_name}: {exc}"
        ) from exc


# ============================================================
# PADRONIZAÇÃO DE SCHEMA
# ============================================================


def _find_column(
    columns: Iterable[str],
    aliases: Iterable[str],
) -> str | None:
    """Localiza uma coluna usando aliases."""

    column_set = set(columns)

    for alias in aliases:
        normalized_alias = to_snake_case(alias)

        if normalized_alias in column_set:
            return normalized_alias

    return None


def build_canonical_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cria colunas canônicas sem apagar as colunas originais.

    Isso mantém rastreabilidade e ao mesmo tempo cria uma interface
    estável para Silver/Gold.
    """

    output = normalize_column_names(df)

    for canonical_name, aliases in COLUMN_ALIASES.items():
        source_column = _find_column(
            output.columns,
            aliases,
        )

        if source_column is not None:
            output[canonical_name] = output[source_column]

        elif canonical_name not in output.columns:
            output[canonical_name] = pd.NA

    return output


# ============================================================
# TIPAGEM E LIMPEZA
# ============================================================


def clean_numeric(
    series: pd.Series,
) -> pd.Series:
    """
    Converte uma série para número.

    Trata valores textuais com vírgula decimal.
    """

    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(
            series,
            errors="coerce",
        )

    cleaned = (
        series.astype("string")
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    return pd.to_numeric(
        cleaned,
        errors="coerce",
    )


def cast_types(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Converte campos principais para tipos apropriados."""

    output = df.copy()

    output["codigo_item_catalogo"] = (
        output["codigo_item_catalogo"]
        .map(normalize_catmat)
        .astype("string")
    )

    output["quantidade"] = clean_numeric(
        output["quantidade"]
    )

    output["preco_unitario"] = clean_numeric(
        output["preco_unitario"]
    )

    date_columns = [
        "data_compra",
        "data_resultado",
        "data_hora_atualizacao_compra",
        "data_hora_atualizacao_item",
        "ingestion_timestamp",
    ]

    for column in date_columns:
        if column in output.columns:
            output[column] = pd.to_datetime(
                output[column],
                errors="coerce",
                utc=True,
            ).dt.tz_localize(None)

    text_columns = [
        "descricao_item",
        "unidade_original",
        "uf",
        "municipio",
        "nome_uasg",
        "nome_orgao",
        "nome_fornecedor",
        "modalidade",
        "esfera",
        "poder",
    ]

    for column in text_columns:
        if column in output.columns:
            output[column] = (
                output[column]
                .astype("string")
                .str.strip()
            )

    if "uf" in output.columns:
        output["uf"] = (
            output["uf"]
            .astype("string")
            .str.upper()
            .str.strip()
        )

    return output


# ============================================================
# RECORTE TEMPORAL
# ============================================================


def filter_period(
    df: pd.DataFrame,
    *,
    start_date: str,
) -> pd.DataFrame:
    """
    Mantém somente registros do período analítico do ObraIndex.

    Registros sem data_compra são mantidos temporariamente para que
    a etapa de qualidade possa classificá-los como inválidos.
    """

    output = df.copy()

    start = pd.Timestamp(start_date)

    mask = (
        output["data_compra"].isna()
        | (output["data_compra"] >= start)
    )

    removed = int((~mask).sum())

    if removed:
        LOGGER.info(
            "Registros anteriores a %s removidos da Silver: %s",
            start_date,
            removed,
        )

    return output.loc[mask].copy()


# ============================================================
# CATÁLOGO DE MATERIAIS
# ============================================================


def build_material_mapping(
    materials_file: str | Path,
) -> pd.DataFrame:
    """Transforma materiais.yml em DataFrame de referência."""

    materials = load_materials(materials_file)

    rows: list[dict[str, Any]] = []

    for material in materials:
        catmat = (
            material.get("catmat")
            or material.get("codigo_catmat")
            or material.get("codigoItemCatalogo")
        )

        if not catmat:
            continue

        rows.append(
            {
                "codigo_item_catalogo": normalize_catmat(catmat),
                "material_id": material.get(
                    "material_id",
                    normalize_catmat(catmat),
                ),
                "material": (
                    material.get("nome")
                    or material.get("name")
                    or normalize_catmat(catmat)
                ),
                "categoria_material": (
                    material.get("categoria")
                    or material.get("category")
                ),
                "unidade_padrao": (
                    material.get("unidade_padrao")
                    or material.get("standard_unit")
                ),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "codigo_item_catalogo",
                "material_id",
                "material",
                "categoria_material",
                "unidade_padrao",
            ]
        )

    mapping = pd.DataFrame(rows)

    mapping["codigo_item_catalogo"] = (
        mapping["codigo_item_catalogo"]
        .astype("string")
    )

    return mapping.drop_duplicates(
        subset=["codigo_item_catalogo"],
        keep="last",
    )


def enrich_materials(
    df: pd.DataFrame,
    *,
    materials_file: str | Path,
) -> pd.DataFrame:
    """Adiciona dados do catálogo local de materiais."""

    mapping = build_material_mapping(
        materials_file
    )

    output = df.copy()

    if mapping.empty:
        output["material_id"] = (
            output["codigo_item_catalogo"]
        )
        output["material"] = output["descricao_item"]
        output["categoria_material"] = pd.NA
        output["unidade_padrao"] = pd.NA

        return output

    # Remove colunas para evitar suffix durante o merge.
    output = output.drop(
        columns=[
            "material_id",
            "material",
            "categoria_material",
            "unidade_padrao",
        ],
        errors="ignore",
    )

    return output.merge(
        mapping,
        how="left",
        on="codigo_item_catalogo",
        validate="many_to_one",
    )


# ============================================================
# DEDUPLICAÇÃO
# ============================================================


def deduplicate(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """
    Remove registros duplicados.

    Prioridade:
    1. record_hash
    2. id_compra + id_item_compra + CATMAT
    """

    output = df.copy()

    before = len(output)

    if (
        "record_hash" in output.columns
        and output["record_hash"].notna().any()
    ):
        output = output.drop_duplicates(
            subset=["record_hash"],
            keep="last",
        )

    else:
        keys = [
            "id_compra",
            "id_item_compra",
            "codigo_item_catalogo",
        ]

        valid_keys = [
            column
            for column in keys
            if column in output.columns
            and output[column].notna().any()
        ]

        if valid_keys:
            output = output.drop_duplicates(
                subset=valid_keys,
                keep="last",
            )

        else:
            output = output.drop_duplicates(
                keep="last"
            )

    removed = before - len(output)

    LOGGER.info(
        "Deduplicação concluída | removidos=%s",
        removed,
    )

    return output, removed


# ============================================================
# COLUNAS DERIVADAS
# ============================================================


def add_derived_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Cria campos analíticos básicos."""

    output = df.copy()

    output["valor_total_item"] = (
        output["quantidade"]
        * output["preco_unitario"]
    )

    output["ano"] = (
        output["data_compra"].dt.year
        .astype("Int64")
    )

    output["mes"] = (
        output["data_compra"].dt.month
        .astype("Int64")
    )

    output["ano_mes"] = (
        output["data_compra"]
        .dt.to_period("M")
        .astype("string")
    )

    output["trimestre"] = (
        output["data_compra"]
        .dt.to_period("Q")
        .astype("string")
    )

    output["semestre"] = np.where(
        output["mes"].between(1, 6),
        output["ano"].astype("string") + "-S1",
        np.where(
            output["mes"].between(7, 12),
            output["ano"].astype("string") + "-S2",
            pd.NA,
        ),
    )

    output["regiao"] = output["uf"].map(
        REGION_BY_UF
    )

    # Nesta primeira versão não fazemos conversões arbitrárias
    # entre unidades. O valor normalizado é igual ao original
    # até existir uma regra explícita em materiais.yml.
    output["preco_original"] = (
        output["preco_unitario"]
    )

    output["preco_normalizado"] = (
        output["preco_unitario"]
    )

    output["unidade_normalizada"] = (
        output["unidade_padrao"]
        .fillna(output["unidade_original"])
    )

    output["_silver_processed_at"] = datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    return output


# ============================================================
# QUALIDADE
# ============================================================


def add_quality_flags(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Adiciona flags de qualidade.

    O registro não é apagado neste momento; apenas classificado.
    """

    output = df.copy()

    output["is_valid_catmat"] = (
        output["codigo_item_catalogo"]
        .notna()
        & output["codigo_item_catalogo"].ne("")
    )

    output["is_valid_date"] = (
        output["data_compra"].notna()
    )

    output["is_valid_price"] = (
        output["preco_unitario"].notna()
        & output["preco_unitario"].gt(0)
    )

    output["is_valid_quantity"] = (
        output["quantidade"].notna()
        & output["quantidade"].gt(0)
    )

    output["is_valid_uf"] = (
        output["uf"].isna()
        | output["uf"].isin(REGION_BY_UF)
    )

    output["is_valid_record"] = (
        output["is_valid_catmat"]
        & output["is_valid_date"]
        & output["is_valid_price"]
        & output["is_valid_quantity"]
        & output["is_valid_uf"]
    )

    def reasons(row: pd.Series) -> str | None:
        issues: list[str] = []

        if not row["is_valid_catmat"]:
            issues.append("CATMAT_INVALIDO")

        if not row["is_valid_date"]:
            issues.append("DATA_COMPRA_INVALIDA")

        if not row["is_valid_price"]:
            issues.append("PRECO_INVALIDO")

        if not row["is_valid_quantity"]:
            issues.append("QUANTIDADE_INVALIDA")

        if not row["is_valid_uf"]:
            issues.append("UF_INVALIDA")

        return ";".join(issues) if issues else None

    invalid_mask = ~output["is_valid_record"]

    output["quality_issues"] = pd.NA

    if invalid_mask.any():
        output.loc[
            invalid_mask,
            "quality_issues",
        ] = output.loc[
            invalid_mask
        ].apply(
            reasons,
            axis=1,
        )

    return output


# ============================================================
# OUTLIERS
# ============================================================


def add_outlier_flags(
    df: pd.DataFrame,
    *,
    multiplier: float = 1.5,
    minimum_group_size: int = 10,
) -> pd.DataFrame:
    """
    Identifica outliers de preço pelo método IQR.

    Agrupamento:
        CATMAT + ano_mes

    O dado original permanece na Silver.
    """

    output = df.copy()

    output["is_outlier"] = False
    output["outlier_reason"] = pd.NA

    group_columns = [
        "codigo_item_catalogo",
        "ano_mes",
    ]

    valid = output[
        output["preco_normalizado"].notna()
        & output["codigo_item_catalogo"].notna()
        & output["ano_mes"].notna()
    ]

    for _, index in valid.groupby(
        group_columns,
        dropna=False,
    ).groups.items():

        group = output.loc[index]

        if len(group) < minimum_group_size:
            continue

        q1 = group["preco_normalizado"].quantile(
            0.25
        )

        q3 = group["preco_normalizado"].quantile(
            0.75
        )

        iqr = q3 - q1

        if pd.isna(iqr) or iqr <= 0:
            continue

        lower = q1 - multiplier * iqr
        upper = q3 + multiplier * iqr

        mask = (
            (group["preco_normalizado"] < lower)
            | (group["preco_normalizado"] > upper)
        )

        outlier_index = group.index[mask]

        if len(outlier_index) == 0:
            continue

        output.loc[
            outlier_index,
            "is_outlier",
        ] = True

        output.loc[
            outlier_index,
            "outlier_reason",
        ] = (
            f"IQR_{multiplier:g}"
        )

    LOGGER.info(
        "Outliers identificados: %s",
        int(output["is_outlier"].sum()),
    )

    return output


# ============================================================
# TRANSFORMAÇÃO SILVER
# ============================================================


def transform_silver(
    df: pd.DataFrame,
    *,
    materials_file: str | Path,
    settings: SilverSettings,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Executa toda a transformação Bronze -> Silver.
    """

    if df.empty:
        return df.copy(), {
            "records_input": 0,
            "records_output": 0,
            "duplicates_removed": 0,
            "invalid_records": 0,
            "outliers": 0,
        }

    records_input = len(df)

    output = build_canonical_columns(df)

    output = cast_types(output)

    output = filter_period(
        output,
        start_date=settings.start_date,
    )

    output = enrich_materials(
        output,
        materials_file=materials_file,
    )

    output, duplicates_removed = deduplicate(
        output
    )

    output = add_derived_columns(output)

    output = add_quality_flags(output)

    output = add_outlier_flags(
        output,
        multiplier=settings.outlier_iqr_multiplier,
        minimum_group_size=(
            settings.minimum_outlier_group_size
        ),
    )

    output = output.sort_values(
        by=[
            "codigo_item_catalogo",
            "data_compra",
        ],
        na_position="last",
    ).reset_index(drop=True)

    metrics = {
        "records_input": records_input,
        "records_output": len(output),
        "duplicates_removed": duplicates_removed,
        "invalid_records": int(
            (~output["is_valid_record"]).sum()
        ),
        "outliers": int(
            output["is_outlier"].sum()
        ),
    }

    LOGGER.info(
        "Silver transformada | entrada=%s | saída=%s | "
        "duplicados=%s | inválidos=%s | outliers=%s",
        metrics["records_input"],
        metrics["records_output"],
        metrics["duplicates_removed"],
        metrics["invalid_records"],
        metrics["outliers"],
    )

    return output, metrics


# ============================================================
# CAMINHO SILVER
# ============================================================


def build_silver_object_name(
    *,
    catmat: str,
    year: int | str,
    month: int | str,
    run_id: str,
) -> str:
    """Monta o caminho de um arquivo Silver."""

    return (
        "compras_gov/precos_material/"
        f"catmat={catmat}/"
        f"ano={int(year):04d}/"
        f"mes={int(month):02d}/"
        f"silver_{run_id}.parquet"
    )


# ============================================================
# GRAVAÇÃO PARTICIONADA
# ============================================================


def save_partitioned_silver(
    df: pd.DataFrame,
    *,
    settings: SilverSettings,
    client: Minio,
    run_id: str,
) -> list[dict[str, Any]]:
    """
    Grava a Silver particionada por:
        CATMAT / ano / mês
    """

    if df.empty:
        return []

    output_objects: list[dict[str, Any]] = []

    valid_partition = df[
        df["codigo_item_catalogo"].notna()
        & df["ano"].notna()
        & df["mes"].notna()
    ].copy()

    groups = valid_partition.groupby(
        [
            "codigo_item_catalogo",
            "ano",
            "mes",
        ],
        dropna=False,
    )

    for (
        catmat,
        year,
        month,
    ), group in groups:

        object_name = build_silver_object_name(
            catmat=str(catmat),
            year=int(year),
            month=int(month),
            run_id=run_id,
        )

        written = save_dataframe_to_minio(
            group,
            client=client,
            bucket=settings.silver_bucket,
            object_name=object_name,
        )

        output_objects.append(
            {
                "object_name": object_name,
                "catmat": str(catmat),
                "ano": int(year),
                "mes": int(month),
                "records_written": written,
            }
        )

    return output_objects


# ============================================================
# PROCESSAMENTO DE OBJETOS BRONZE
# ============================================================


def process_bronze_objects(
    object_names: Iterable[str],
    *,
    materials_file: str | Path = "config/materiais.yml",
    run_id: str | None = None,
    settings: SilverSettings | None = None,
) -> dict[str, Any]:
    """
    Processa uma lista específica de arquivos Bronze.

    Essa é a função recomendada para integração com o Airflow,
    pois a Bronze já retorna a lista de objetos criados.
    """

    settings = settings or SilverSettings.from_env()

    run_id = run_id or uuid.uuid4().hex

    object_names = list(object_names)

    client = get_minio_client(settings)

    ensure_bucket(
        client,
        settings.bronze_bucket,
    )

    ensure_bucket(
        client,
        settings.silver_bucket,
    )

    started_at = datetime.now(timezone.utc)

    LOGGER.info(
        "Iniciando Silver | objetos_bronze=%s | run_id=%s",
        len(object_names),
        run_id,
    )

    bronze_df = read_bronze_objects(
        client,
        bucket=settings.bronze_bucket,
        object_names=object_names,
    )

    silver_df, metrics = transform_silver(
        bronze_df,
        materials_file=materials_file,
        settings=settings,
    )

    objects_written = save_partitioned_silver(
        silver_df,
        settings=settings,
        client=client,
        run_id=run_id,
    )

    finished_at = datetime.now(timezone.utc)

    result = {
        "run_id": run_id,
        "status": "SUCCESS",
        "bronze_objects_read": len(
            object_names
        ),
        "silver_objects_written": len(
            objects_written
        ),
        **metrics,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": (
            finished_at - started_at
        ).total_seconds(),
        "objects": objects_written,
    }

    LOGGER.info(
        "Silver concluída | arquivos=%s | registros=%s",
        result["silver_objects_written"],
        result["records_output"],
    )

    return result


# ============================================================
# INTEGRAÇÃO DIRETA COM RESULTADO DA BRONZE
# ============================================================


def silver_load(
    bronze_result: dict[str, Any] | list[dict[str, Any]],
    *,
    materials_file: str | Path = "config/materiais.yml",
    run_id: str | None = None,
    settings: SilverSettings | None = None,
) -> dict[str, Any]:
    """
    Processa diretamente o retorno da camada Bronze.

    Aceita:
    - resultado de extract_material_to_bronze()
    - lista retornada por full_load()/incremental_load()
    - resultado resumido da task run_bronze da DAG
    """

    object_names: list[str] = []

    def collect(item: Any) -> None:
        if not isinstance(item, dict):
            return

        objects = item.get("objects", [])

        if isinstance(objects, list):
            for obj in objects:
                if isinstance(obj, str):
                    object_names.append(obj)

                elif (
                    isinstance(obj, dict)
                    and obj.get("object_name")
                ):
                    object_names.append(
                        obj["object_name"]
                    )

        nested_results = item.get("results", [])

        if isinstance(nested_results, list):
            for nested_item in nested_results:
                collect(nested_item)

    if isinstance(bronze_result, list):
        for item in bronze_result:
            collect(item)

    else:
        collect(bronze_result)

    object_names = list(
        dict.fromkeys(object_names)
    )

    if not object_names:
        raise SilverError(
            "Nenhum objeto Bronze foi encontrado "
            "no resultado recebido."
        )

    return process_bronze_objects(
        object_names,
        materials_file=materials_file,
        run_id=run_id,
        settings=settings,
    )


# ============================================================
# PROCESSAMENTO COMPLETO DA BRONZE EXISTENTE
# ============================================================


def process_all_bronze(
    *,
    materials_file: str | Path = "config/materiais.yml",
    prefix: str | None = None,
    run_id: str | None = None,
    settings: SilverSettings | None = None,
) -> dict[str, Any]:
    """
    Processa todos os Parquets já existentes na Bronze.

    Útil para:
    - primeira execução manual;
    - reprocessamento completo;
    - testes.

    Para execução incremental no Airflow, prefira silver_load().
    """

    settings = settings or SilverSettings.from_env()

    client = get_minio_client(settings)

    ensure_bucket(
        client,
        settings.bronze_bucket,
    )

    prefix = (
        prefix
        or settings.bronze_prefix
    )

    object_names = list_parquet_objects(
        client,
        bucket=settings.bronze_bucket,
        prefix=prefix,
    )

    LOGGER.info(
        "Objetos Bronze encontrados: %s",
        len(object_names),
    )

    if not object_names:
        raise SilverError(
            "Nenhum arquivo Parquet encontrado "
            "na camada Bronze."
        )

    return process_bronze_objects(
        object_names,
        materials_file=materials_file,
        run_id=run_id,
        settings=settings,
    )


# ============================================================
# TESTE DE CONEXÃO
# ============================================================


def test_silver_connections(
    settings: SilverSettings | None = None,
) -> dict[str, bool]:
    """Valida acesso aos buckets Bronze e Silver."""

    settings = settings or SilverSettings.from_env()

    result = {
        "minio": False,
        "bronze_bucket": False,
        "silver_bucket": False,
    }

    try:
        client = get_minio_client(settings)

        client.list_buckets()

        result["minio"] = True

        result["bronze_bucket"] = (
            client.bucket_exists(
                settings.bronze_bucket
            )
        )

        ensure_bucket(
            client,
            settings.silver_bucket,
        )

        result["silver_bucket"] = (
            client.bucket_exists(
                settings.silver_bucket
            )
        )

    except Exception:
        LOGGER.exception(
            "Falha ao testar conexões da Silver."
        )

    return result


# ============================================================
# EXECUÇÃO MANUAL
# ============================================================


if __name__ == "__main__":

    try:
        config = SilverSettings.from_env()

        status = test_silver_connections(
            config
        )

        LOGGER.info(
            "Conexões Silver: %s",
            status,
        )

        if not all(status.values()):
            raise SilverError(
                "Uma ou mais validações "
                "de conexão falharam."
            )

        result = process_all_bronze(
            materials_file=(
                "config/materiais.yml"
            ),
            settings=config,
        )

        print(result)

    except Exception as exc:
        LOGGER.exception(
            "Falha na camada Silver: %s",
            exc,
        )
        raise
