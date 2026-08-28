"""
ObraIndex - Camada Bronze
=========================

Responsabilidades:
- Consumir a API pública do Compras.gov.br.
- Executar paginação.
- Suportar carga FULL e incremental.
- Preservar os dados o mais próximo possível da origem.
- Adicionar metadados técnicos de ingestão.
- Gravar arquivos Parquet no MinIO.
- Manter o código independente de credenciais hardcoded.

Variáveis esperadas no .env:
    COMPRAS_GOV_BASE_URL
    COMPRAS_GOV_PRICE_ENDPOINT
    OBRAINDEX_START_DATE

    MINIO_INTERNAL_ENDPOINT
    MINIO_ROOT_USER
    MINIO_ROOT_PASSWORD
    MINIO_SECURE
    MINIO_BUCKET_BRONZE

Exemplo de uso:
    python -m src.bronze.bronze
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import time
import uuid

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import requests
import yaml

from dotenv import load_dotenv
from minio import Minio
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# CONFIGURAÇÃO INICIAL
# ============================================================

load_dotenv()

LOGGER = logging.getLogger("obraindex.bronze")

if not LOGGER.handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# ============================================================
# EXCEÇÕES
# ============================================================


class BronzeError(Exception):
    """Erro base da camada Bronze."""


class APIRequestError(BronzeError):
    """Erro durante comunicação com a API do Compras.gov.br."""


class StorageError(BronzeError):
    """Erro durante gravação no MinIO."""


# ============================================================
# CONFIGURAÇÕES
# ============================================================


@dataclass(frozen=True)
class BronzeSettings:
    """Configurações necessárias para execução da camada Bronze."""

    compras_base_url: str
    compras_price_endpoint: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    bronze_bucket: str

    start_date: str = "2020-01-01"

    request_timeout: int = 60
    page_size: int = 500
    max_attempts: int = 5
    request_delay_seconds: float = 0.25

    @classmethod
    def from_env(cls) -> "BronzeSettings":
        """Cria as configurações a partir das variáveis do ambiente."""

        required = {
            "COMPRAS_GOV_BASE_URL": os.getenv("COMPRAS_GOV_BASE_URL"),
            "COMPRAS_GOV_PRICE_ENDPOINT": os.getenv(
                "COMPRAS_GOV_PRICE_ENDPOINT"
            ),
            "MINIO_INTERNAL_ENDPOINT": os.getenv("MINIO_INTERNAL_ENDPOINT"),
            "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
            "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
            "MINIO_BUCKET_BRONZE": os.getenv("MINIO_BUCKET_BRONZE"),
        }

        missing = [key for key, value in required.items() if not value]

        if missing:
            raise BronzeError(
                "Variáveis obrigatórias não configuradas: "
                + ", ".join(missing)
            )

        return cls(
            compras_base_url=required["COMPRAS_GOV_BASE_URL"].rstrip("/"),
            compras_price_endpoint=required[
                "COMPRAS_GOV_PRICE_ENDPOINT"
            ],
            minio_endpoint=required["MINIO_INTERNAL_ENDPOINT"],
            minio_access_key=required["MINIO_ROOT_USER"],
            minio_secret_key=required["MINIO_ROOT_PASSWORD"],
            minio_secure=_to_bool(os.getenv("MINIO_SECURE", "false")),
            bronze_bucket=required["MINIO_BUCKET_BRONZE"],
            start_date=os.getenv("OBRAINDEX_START_DATE", "2020-01-01"),
            request_timeout=int(os.getenv("API_TIMEOUT_SECONDS", "60")),
            page_size=int(os.getenv("API_PAGE_SIZE", "500")),
            max_attempts=int(os.getenv("API_MAX_ATTEMPTS", "5")),
            request_delay_seconds=float(
                os.getenv("API_REQUEST_DELAY_SECONDS", "0.25")
            ),
        )


# ============================================================
# FUNÇÕES DE CONFIGURAÇÃO
# ============================================================


def _to_bool(value: str | bool | None) -> bool:
    """Converte diferentes representações de booleano."""

    if isinstance(value, bool):
        return value

    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Carrega um arquivo YAML."""

    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    with file_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def load_materials(
    path: str | Path = "config/materiais.yml",
) -> list[dict[str, Any]]:
    """
    Carrega os materiais ativos do arquivo materiais.yml.

    Aceita as chaves de primeiro nível:
    - materiais
    - materials
    """

    config = load_yaml(path)

    materials = config.get("materiais") or config.get("materials") or []

    if not isinstance(materials, list):
        raise BronzeError(
            "O arquivo materiais.yml deve possuir uma lista em "
            "'materiais' ou 'materials'."
        )

    return [
        material
        for material in materials
        if material.get("ativo", material.get("active", True))
    ]


# ============================================================
# HTTP / API
# ============================================================


def build_http_session(max_attempts: int = 5) -> requests.Session:
    """Cria uma Session HTTP com retry e backoff."""

    retry = Retry(
        total=max_attempts,
        connect=max_attempts,
        read=max_attempts,
        status=max_attempts,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "ObraIndex/1.0",
        }
    )

    return session


def build_url(base_url: str, endpoint: str) -> str:
    """Monta a URL final sem duplicar barras."""

    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout: int,
) -> dict[str, Any] | list[Any]:
    """Executa uma requisição GET e retorna JSON."""

    LOGGER.info("Consultando API | url=%s | params=%s", url, params)

    try:
        response = session.get(
            url,
            params=params,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise APIRequestError(
            f"Erro ao acessar a API: {exc}"
        ) from exc

    if response.status_code != 200:
        raise APIRequestError(
            "API retornou status "
            f"{response.status_code}: {response.text[:500]}"
        )

    try:
        return response.json()
    except ValueError as exc:
        raise APIRequestError(
            "A resposta da API não é um JSON válido."
        ) from exc


# ============================================================
# NORMALIZAÇÃO DA RESPOSTA
# ============================================================


def extract_records(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    """
    Localiza os registros dentro da resposta da API.

    A função é propositalmente tolerante a diferentes nomes de chaves,
    pois endpoints públicos podem retornar estruturas diferentes.
    """

    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if not isinstance(payload, dict):
        return []

    candidate_keys = (
        "resultado",
        "resultados",
        "data",
        "dados",
        "items",
        "content",
        "registros",
    )

    for key in candidate_keys:
        value = payload.get(key)

        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]

        if isinstance(value, dict):
            nested = extract_records(value)
            if nested:
                return nested

    # Fallback: se a própria resposta parecer um único registro.
    if payload and not any(
        isinstance(value, (list, dict))
        for value in payload.values()
    ):
        return [payload]

    return []


def extract_total_pages(
    payload: dict[str, Any] | list[Any],
) -> int | None:
    """Tenta identificar o número total de páginas da resposta."""

    if not isinstance(payload, dict):
        return None

    possible_keys = (
        "totalPaginas",
        "total_paginas",
        "totalPages",
        "pages",
        "total_pages",
    )

    containers = [payload]

    for key in ("paginacao", "pagination", "meta", "metadata"):
        value = payload.get(key)
        if isinstance(value, dict):
            containers.append(value)

    for container in containers:
        for key in possible_keys:
            value = container.get(key)

            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    continue

    return None


# ============================================================
# PAGINAÇÃO
# ============================================================


def iter_price_pages(
    *,
    catmat: str | int,
    settings: BronzeSettings,
    extra_params: dict[str, Any] | None = None,
    session: requests.Session | None = None,
) -> Iterator[tuple[int, list[dict[str, Any]], Any]]:
    """
    Percorre as páginas da API de preços para um CATMAT.

    O nome exato dos parâmetros pode ser ajustado no extra_params
    conforme o Swagger atual da API.

    Por padrão utiliza:
        codigoItemCatalogo
        pagina
        tamanhoPagina

    Caso o endpoint utilize outros nomes, sobrescreva esses parâmetros
    via configuração antes de executar em produção.
    """

    session = session or build_http_session(settings.max_attempts)

    url = build_url(
        settings.compras_base_url,
        settings.compras_price_endpoint,
    )

    page = 1

    while True:
        params: dict[str, Any] = {
            "codigoItemCatalogo": catmat,
            "pagina": page,
            "tamanhoPagina": settings.page_size,
        }

        if extra_params:
            params.update(extra_params)

        payload = request_json(
            session=session,
            url=url,
            params=params,
            timeout=settings.request_timeout,
        )

        records = extract_records(payload)
        total_pages = extract_total_pages(payload)

        LOGGER.info(
            "Página processada | catmat=%s | pagina=%s | registros=%s | "
            "total_paginas=%s",
            catmat,
            page,
            len(records),
            total_pages,
        )

        if not records:
            break

        yield page, records, payload

        if total_pages is not None and page >= total_pages:
            break

        if len(records) < settings.page_size and total_pages is None:
            break

        page += 1

        if settings.request_delay_seconds > 0:
            time.sleep(settings.request_delay_seconds)


# ============================================================
# METADADOS BRONZE
# ============================================================


def generate_record_hash(record: dict[str, Any]) -> str:
    """Gera SHA-256 do registro original para rastreabilidade."""

    serialized = json.dumps(
        record,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def add_bronze_metadata(
    records: list[dict[str, Any]],
    *,
    catmat: str | int,
    endpoint: str,
    run_id: str,
    extract_type: str,
    page: int,
) -> list[dict[str, Any]]:
    """Adiciona metadados técnicos sem remover campos da origem."""

    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    output: list[dict[str, Any]] = []

    for record in records:
        enriched = dict(record)

        enriched["_ingestion_timestamp"] = ingestion_timestamp
        enriched["_source"] = "compras.gov.br"
        enriched["_source_endpoint"] = endpoint
        enriched["_pipeline_run_id"] = run_id
        enriched["_extract_type"] = extract_type
        enriched["_page"] = page
        enriched["_catmat"] = str(catmat)
        enriched["_record_hash"] = generate_record_hash(record)

        output.append(enriched)

    return output


# ============================================================
# MINIO
# ============================================================


def get_minio_client(settings: BronzeSettings) -> Minio:
    """Cria o cliente do MinIO."""

    return Minio(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Cria o bucket caso ele ainda não exista."""

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            LOGGER.info("Bucket criado: %s", bucket)

    except Exception as exc:
        raise StorageError(
            f"Erro ao validar/criar bucket {bucket}: {exc}"
        ) from exc


def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
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


def build_object_name(
    *,
    catmat: str | int,
    run_id: str,
    page: int,
    extract_type: str,
    ingestion_time: datetime | None = None,
) -> str:
    """Monta o caminho de armazenamento da camada Bronze."""

    ingestion_time = ingestion_time or datetime.now(timezone.utc)

    return (
        "compras_gov/precos_material/"
        f"catmat={catmat}/"
        f"ano_ingestao={ingestion_time:%Y}/"
        f"mes_ingestao={ingestion_time:%m}/"
        f"dia_ingestao={ingestion_time:%d}/"
        f"{extract_type.lower()}_"
        f"{run_id}_"
        f"page_{page:05d}.parquet"
    )


def save_records_to_minio(
    records: list[dict[str, Any]],
    *,
    settings: BronzeSettings,
    client: Minio,
    object_name: str,
) -> int:
    """
    Salva registros em Parquet no bucket Bronze.

    Retorna a quantidade de registros gravados.
    """

    if not records:
        LOGGER.warning(
            "Nenhum registro recebido para gravação: %s",
            object_name,
        )
        return 0

    try:
        df = pd.json_normalize(records, sep="__")

        parquet_data = dataframe_to_parquet_bytes(df)
        buffer = io.BytesIO(parquet_data)

        client.put_object(
            bucket_name=settings.bronze_bucket,
            object_name=object_name,
            data=buffer,
            length=len(parquet_data),
            content_type="application/octet-stream",
        )

        LOGGER.info(
            "Bronze salva | bucket=%s | objeto=%s | registros=%s",
            settings.bronze_bucket,
            object_name,
            len(df),
        )

        return len(df)

    except Exception as exc:
        raise StorageError(
            f"Erro ao salvar {object_name} no MinIO: {exc}"
        ) from exc


# ============================================================
# EXTRAÇÃO DE UM MATERIAL
# ============================================================


def extract_material_to_bronze(
    *,
    catmat: str | int,
    extract_type: str = "FULL",
    extra_params: dict[str, Any] | None = None,
    run_id: str | None = None,
    settings: BronzeSettings | None = None,
) -> dict[str, Any]:
    """
    Extrai todas as páginas de um CATMAT e grava na Bronze.

    Parameters
    ----------
    catmat:
        Código CATMAT.
    extract_type:
        FULL ou INCREMENTAL.
    extra_params:
        Parâmetros adicionais enviados ao endpoint.
    run_id:
        Identificador da execução.
    settings:
        Configurações da camada Bronze.
    """

    settings = settings or BronzeSettings.from_env()
    run_id = run_id or uuid.uuid4().hex

    extract_type = extract_type.upper()

    if extract_type not in {"FULL", "INCREMENTAL"}:
        raise ValueError(
            "extract_type deve ser FULL ou INCREMENTAL."
        )

    client = get_minio_client(settings)
    ensure_bucket(client, settings.bronze_bucket)

    session = build_http_session(settings.max_attempts)

    total_records = 0
    pages_processed = 0
    objects: list[str] = []

    started_at = datetime.now(timezone.utc)

    LOGGER.info(
        "Iniciando extração | catmat=%s | tipo=%s | run_id=%s",
        catmat,
        extract_type,
        run_id,
    )

    for page, records, _payload in iter_price_pages(
        catmat=catmat,
        settings=settings,
        extra_params=extra_params,
        session=session,
    ):
        enriched_records = add_bronze_metadata(
            records,
            catmat=catmat,
            endpoint=settings.compras_price_endpoint,
            run_id=run_id,
            extract_type=extract_type,
            page=page,
        )

        object_name = build_object_name(
            catmat=catmat,
            run_id=run_id,
            page=page,
            extract_type=extract_type,
        )

        written = save_records_to_minio(
            enriched_records,
            settings=settings,
            client=client,
            object_name=object_name,
        )

        total_records += written
        pages_processed += 1
        objects.append(object_name)

    finished_at = datetime.now(timezone.utc)

    result = {
        "run_id": run_id,
        "catmat": str(catmat),
        "extract_type": extract_type,
        "status": "SUCCESS",
        "pages_processed": pages_processed,
        "records_written": total_records,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": (
            finished_at - started_at
        ).total_seconds(),
        "objects": objects,
    }

    LOGGER.info(
        "Extração concluída | catmat=%s | paginas=%s | registros=%s",
        catmat,
        pages_processed,
        total_records,
    )

    return result


# ============================================================
# CARGA FULL
# ============================================================


def full_load(
    materials_file: str | Path = "config/materiais.yml",
    *,
    settings: BronzeSettings | None = None,
) -> list[dict[str, Any]]:
    """
    Executa a carga FULL dos materiais ativos.

    O filtro de início em 2020 deve ser enviado à API caso o endpoint
    suporte filtro temporal. Caso não suporte, a Silver deverá aplicar
    o recorte temporal mantendo a Bronze íntegra.
    """

    settings = settings or BronzeSettings.from_env()

    materials = load_materials(materials_file)

    results: list[dict[str, Any]] = []

    LOGGER.info(
        "Carga FULL iniciada | materiais=%s | inicio_desejado=%s",
        len(materials),
        settings.start_date,
    )

    for material in materials:
        catmat = (
            material.get("catmat")
            or material.get("codigo_catmat")
            or material.get("codigoItemCatalogo")
        )

        name = material.get("nome") or material.get("name") or catmat

        if not catmat:
            LOGGER.warning(
                "Material ignorado por não possuir CATMAT | material=%s",
                name,
            )
            continue

        try:
            result = extract_material_to_bronze(
                catmat=catmat,
                extract_type="FULL",
                settings=settings,
            )

        except Exception as exc:
            LOGGER.exception(
                "Falha na carga FULL | material=%s | catmat=%s",
                name,
                catmat,
            )

            result = {
                "material": name,
                "catmat": str(catmat),
                "extract_type": "FULL",
                "status": "FAILED",
                "error": str(exc),
            }

        results.append(result)

    return results


# ============================================================
# CARGA INCREMENTAL
# ============================================================


def incremental_load(
    materials_file: str | Path = "config/materiais.yml",
    *,
    watermark_by_catmat: dict[str, str] | None = None,
    settings: BronzeSettings | None = None,
) -> list[dict[str, Any]]:
    """
    Executa carga incremental dos materiais ativos.

    watermark_by_catmat:
        Dicionário opcional:
            {
                "123456": "2026-08-01",
                "654321": "2026-08-15"
            }

    Importante:
    O nome exato do filtro temporal depende do Swagger do endpoint.
    Neste módulo usamos 'dataInicial' somente como exemplo configurável.
    Ajuste o parâmetro conforme o endpoint real.
    """

    settings = settings or BronzeSettings.from_env()
    materials = load_materials(materials_file)

    watermark_by_catmat = watermark_by_catmat or {}

    results: list[dict[str, Any]] = []

    for material in materials:
        catmat = (
            material.get("catmat")
            or material.get("codigo_catmat")
            or material.get("codigoItemCatalogo")
        )

        name = material.get("nome") or material.get("name") or catmat

        if not catmat:
            LOGGER.warning(
                "Material ignorado por não possuir CATMAT | material=%s",
                name,
            )
            continue

        watermark = watermark_by_catmat.get(str(catmat))

        extra_params: dict[str, Any] = {}

        if watermark:
            extra_params["dataInicial"] = watermark

        try:
            result = extract_material_to_bronze(
                catmat=catmat,
                extract_type="INCREMENTAL",
                extra_params=extra_params,
                settings=settings,
            )

            result["watermark_used"] = watermark

        except Exception as exc:
            LOGGER.exception(
                "Falha na carga incremental | material=%s | catmat=%s",
                name,
                catmat,
            )

            result = {
                "material": name,
                "catmat": str(catmat),
                "extract_type": "INCREMENTAL",
                "status": "FAILED",
                "watermark_used": watermark,
                "error": str(exc),
            }

        results.append(result)

    return results


# ============================================================
# TESTE DE CONEXÃO
# ============================================================


def test_connections(
    settings: BronzeSettings | None = None,
) -> dict[str, bool]:
    """Testa conectividade básica com API e MinIO."""

    settings = settings or BronzeSettings.from_env()

    result = {
        "api": False,
        "minio": False,
    }

    # MinIO
    try:
        client = get_minio_client(settings)
        client.list_buckets()
        result["minio"] = True
    except Exception:
        LOGGER.exception("Falha ao testar conexão com MinIO.")

    # API
    try:
        session = build_http_session(settings.max_attempts)

        response = session.get(
            settings.compras_base_url,
            timeout=settings.request_timeout,
        )

        result["api"] = response.status_code < 500

    except Exception:
        LOGGER.exception("Falha ao testar conexão com a API.")

    return result


# ============================================================
# EXECUÇÃO MANUAL
# ============================================================


if __name__ == "__main__":
    try:
        config = BronzeSettings.from_env()

        LOGGER.info("Testando conexões...")

        status = test_connections(config)

        LOGGER.info("Status das conexões: %s", status)

        if not all(status.values()):
            raise BronzeError(
                "Uma ou mais conexões obrigatórias falharam."
            )

        results = full_load(
            materials_file="config/materiais.yml",
            settings=config,
        )

        print(
            json.dumps(
                results,
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )

    except Exception as exc:
        LOGGER.exception("Falha na execução da camada Bronze: %s", exc)
        raise
