# Arquitetura do ObraIndex

O **ObraIndex** é um projeto de Engenharia de Dados aplicado à análise de preços de materiais da construção civil. Sua arquitetura organiza o fluxo desde a extração dos dados públicos do **Compras.gov.br** até a disponibilização de indicadores analíticos no **Metabase**.

## Visão geral da arquitetura

<p align="center">
  <img src="docs/images/arquitetura-obraindex.png" alt="Arquitetura do ObraIndex" width="100%">
</p>

O fluxo principal da solução pode ser resumido como:

**Compras.gov.br → Airflow → Bronze / Silver / Gold → MinIO → PostgreSQL → Metabase**

A arquitetura utiliza os princípios da **Medallion Architecture**, separando o processamento em três níveis — Bronze, Silver e Gold — para aumentar a rastreabilidade, qualidade e capacidade analítica dos dados.

---

## 1.  Fonte de dados — Compras.gov.br

A origem dos dados é a **API pública do Compras.gov.br**.

O pipeline realiza requisições HTTP aos endpoints utilizados pelo projeto para coletar informações relacionadas aos materiais e aos registros de compras públicas.

A ingestão contempla mecanismos como:

- paginação;
- tratamento de erros HTTP;
- retries;
- exponential backoff;
- carga histórica;
- carga incremental;
- controle temporal por watermark.

Os dados recebidos da API constituem a entrada da camada Bronze.

---

## 2.  Orquestração e processamento — Apache Airflow

O **Apache Airflow** é responsável pela orquestração do pipeline.

A execução organiza sequencialmente as etapas de processamento:

```text
Validação do ambiente
        ↓
Validação das conexões
        ↓
Bronze — Extração
        ↓
Silver — Transformação
        ↓
Gold — Agregação
        ↓
Persistência dos watermarks
        ↓
Resumo da execução
```

Essa abordagem permite automatizar o pipeline, controlar dependências entre tarefas, acompanhar execuções e identificar eventuais falhas durante o processamento.

---

## 3.  Camada Bronze — Dados brutos

A camada **Bronze** representa o primeiro estágio da arquitetura Medallion.

Sua principal responsabilidade é realizar a ingestão e preservar os dados provenientes da fonte com o mínimo possível de transformação.

Principais responsabilidades:

- consumo da API do Compras.gov.br;
- extração dos registros;
- preservação dos dados de origem;
- armazenamento dos dados brutos;
- registro de metadados de ingestão;
- suporte às cargas FULL e incrementais.

Na arquitetura apresentada, os dados brutos são representados em **JSON** e armazenados no Data Lake.

---

## 4.  Camada Silver — Dados tratados

A camada **Silver** recebe os dados provenientes da Bronze e executa os tratamentos necessários para torná-los consistentes e adequados ao processamento analítico.

Entre as operações estão:

- padronização de nomes de colunas;
- tipagem dos dados;
- tratamento de valores numéricos;
- normalização de datas;
- normalização de códigos de materiais;
- deduplicação;
- validações de qualidade;
- identificação e tratamento de registros inválidos;
- tratamento analítico de outliers;
- enriquecimento dos dados.

Após o tratamento, os dados são persistidos em formato **Apache Parquet**.

---

## 5.  Camada Gold — Dados analíticos

A camada **Gold** transforma os dados tratados em estruturas voltadas para análise e consumo.

Ela concentra agregações e indicadores como:

- preço médio;
- preço mediano;
- preço mínimo e máximo;
- preço médio ponderado;
- percentis;
- desvio padrão;
- coeficiente de variação;
- quantidade de compras;
- quantidade de fornecedores;
- análises temporais;
- comparações regionais;
- variações mensais e anuais;
- volatilidade.

Os dados Gold são armazenados em **Parquet** no Data Lake e os resultados destinados ao consumo analítico são disponibilizados no PostgreSQL.

---

## 6.  Data Lake — MinIO

O **MinIO** funciona como Data Lake do ObraIndex.

Ele centraliza o armazenamento dos dados processados pelo pipeline e mantém a separação lógica entre as camadas da arquitetura:

```text
MinIO
│
├── Bronze
│   └── Dados brutos
│
├── Silver
│   └── Dados tratados
│
└── Gold
    └── Dados analíticos
```

O uso de armazenamento de objetos permite desacoplar processamento e armazenamento e manter os dados disponíveis para reprocessamentos e novas análises.

---

## 7.  Banco analítico — PostgreSQL

O **PostgreSQL** funciona como camada de dados para consumo analítico.

Enquanto o MinIO preserva os dados das diferentes etapas do Data Lake, o PostgreSQL disponibiliza estruturas da camada Gold de forma adequada para consultas SQL e ferramentas de Business Intelligence.

O fluxo é:

```text
Gold
  ↓
PostgreSQL
  ↓
Consultas SQL
  ↓
Metabase
```

Essa separação evita que a ferramenta de BI precise consultar diretamente os arquivos armazenados no Data Lake.

---

## 8.  Visualização e BI — Metabase

O **Metabase** representa a camada final de consumo.

Ele consulta os dados analíticos disponibilizados no PostgreSQL e permite construir dashboards para exploração das informações produzidas pelo pipeline.

Entre as análises previstas pela arquitetura estão:

- KPIs e indicadores;
- evolução histórica dos preços;
- análise temporal;
- comparação regional;
- comportamento dos materiais;
- identificação de variações relevantes.

---

## Fluxo completo

```text
API pública Compras.gov.br
            │
            ▼
      Apache Airflow
            │
            ▼
          BRONZE
       Dados brutos
            │
            ▼
           MinIO
            │
            ▼
          SILVER
      Dados tratados
            │
            ▼
           MinIO
            │
            ▼
           GOLD
     Dados analíticos
        │       │
        ▼       ▼
      MinIO  PostgreSQL
                │
                ▼
             Metabase
                │
                ▼
       Dashboards e KPIs
```

---

## Stack tecnológica

| Componente | Tecnologia | Responsabilidade |
|---|---|---|
| Linguagem | **Python** | Extração, transformação e processamento |
| Orquestração | **Apache Airflow** | Automação e gerenciamento do pipeline |
| Data Lake | **MinIO** | Armazenamento de objetos |
| Formato analítico | **Apache Parquet** | Persistência eficiente dos dados tratados |
| Banco analítico | **PostgreSQL** | Serving layer e consultas SQL |
| Business Intelligence | **Metabase** | Dashboards e indicadores |
| Infraestrutura | **Docker / Docker Compose** | Containerização dos serviços |
| Consulta | **SQL** | Exploração e consumo dos dados |

---

## Classificação arquitetural

O ObraIndex possui uma arquitetura de dados baseada em **Data Lake com padrão Medallion**, complementada por uma camada relacional de serving.

O **MinIO** mantém os dados nas diferentes etapas do pipeline, enquanto o **PostgreSQL** recebe os dados analíticos destinados ao consumo.

Essa organização combina:

**Data Lake + Medallion Architecture + Analytical Serving Layer**

e estabelece uma separação clara entre **ingestão, armazenamento, tratamento, agregação e visualização**.

---

## Objetivo da arquitetura

A arquitetura foi projetada para transformar dados públicos originalmente operacionais em informações estruturadas para análise dos preços de materiais da construção civil.

Com esse fluxo, o ObraIndex busca permitir análises como:

> **Como os preços dos materiais evoluem ao longo do tempo?**

> **Quais materiais apresentam maior volatilidade?**

> **Existem diferenças relevantes de preço entre estados e regiões?**

> **Quais materiais apresentaram os maiores aumentos em determinado período?**

O resultado é um pipeline reproduzível e organizado, conectando **Engenharia de Dados, análise de dados e Engenharia Civil**.
