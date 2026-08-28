# ObraIndex

**Inteligência de Dados para Custos da Construção Civil**  
*Da obra ao dado. Do dado à decisão.*

## Sobre o projeto

O **ObraIndex** é um projeto de Engenharia de Dados criado para analisar a evolução histórica dos custos da construção civil brasileira a partir de dados públicos.

A proposta é construir um pipeline completo de dados, desde a ingestão até a visualização, utilizando uma arquitetura em camadas **Bronze, Silver e Gold**, armazenamento em **Parquet**, cargas **FULL e incrementais**, orquestração com **Apache Airflow** e consumo final em **Power BI**.

O projeto conecta dois universos que se complementam: **Engenharia Civil** e **Engenharia de Dados**.

---

## Problema de negócio

Os custos da construção civil variam ao longo do tempo em função de fatores como:

- preços de materiais;
- custo de mão de obra;
- inflação;
- diferenças regionais;
- oferta e demanda;
- alterações econômicas.

O ObraIndex busca estruturar esses dados de forma histórica e analítica para responder perguntas como:

- Quais materiais tiveram maior aumento de preço?
- Como o custo da mão de obra evoluiu ao longo do tempo?
- Quais itens apresentaram maior volatilidade?
- Como os custos variam entre estados e regiões?
- Qual foi a variação percentual de determinado item entre dois períodos?

---

## Objetivo do MVP

Construir um pipeline de Engenharia de Dados capaz de:

- consumir dados públicos da construção civil;
- realizar extração via API;
- armazenar dados em formato Parquet;
- implementar as camadas Bronze, Silver e Gold;
- executar carga FULL inicial;
- executar cargas incrementais;
- controlar incrementalidade por watermark;
- orquestrar o pipeline com Apache Airflow;
- armazenar os dados em MinIO;
- gerar indicadores históricos de custos;
- disponibilizar os resultados para análise em Power BI.

---

## Arquitetura

```text
Fonte Pública / API
        |
        v
      Python
        |
        v
 Apache Airflow
        |
        v
      MinIO
        |
        v
     Bronze
 Dados Brutos
    Parquet
        |
        v
     Silver
 Dados Tratados
        |
        v
      Gold
 Indicadores e
  Agregações
        |
        v
     Metabase
```

---

## Camadas de dados

### Bronze

Responsável por armazenar os dados no formato mais próximo possível da origem.

Principais características:

- dados brutos;
- preservação do histórico;
- armazenamento em Parquet;
- possibilidade de reprocessamento;
- particionamento por data.

### Silver

Responsável pela limpeza, padronização e qualidade dos dados.

Principais tratamentos:

- tipagem;
- remoção de duplicidades;
- tratamento de valores nulos;
- padronização de datas;
- padronização de unidades;
- validação de dados;
- aplicação de regras de negócio.

### Gold

Responsável pela criação das estruturas analíticas utilizadas em dashboards e análises.

Exemplos de indicadores:

- preço médio por item;
- variação mensal;
- variação anual;
- evolução histórica;
- ranking de maiores aumentos;
- comparação regional;
- custo médio de mão de obra.

---

## Estratégia de carga

### Carga FULL

A primeira execução do pipeline deverá carregar todo o conjunto de dados disponível.

```text
API -> Python -> Parquet -> MinIO -> Bronze
```

### Carga incremental

Após a carga inicial, o pipeline deverá buscar apenas novos registros ou registros atualizados.

O controle será realizado através de um **watermark**, registrando até qual ponto os dados foram processados.

---

## Tecnologias

- **Python**
- **Pandas**
- **NumPy**
- **PyArrow**
- **Requests**
- **SQLAlchemy**
- **Apache Airflow**
- **MinIO**
- **Parquet**
- **Docker**
- **Docker Compose**
- **Power BI**
- **Git**
- **GitHub**
- **VS Code**
- **Jupyter Notebook**

---

## Roadmap do MVP

### Sprint 1 — Fundação + Ingestão + Bronze
- Estrutura do repositório;
- definição da fonte de dados;
- teste da API;
- criação do script de ingestão;
- configuração do MinIO;
- primeiro arquivo Parquet;
- primeira carga Bronze.

### Sprint 2 — Silver + Qualidade
- leitura da Bronze;
- limpeza e padronização;
- tipagem;
- tratamento de nulos;
- remoção de duplicidades;
- validações;
- gravação da Silver.

### Sprint 3  — Gold + Incremental
- definição dos indicadores;
- criação das agregações;
- implementação da Gold;
- carga incremental;
- watermark.

### Sprint 4  — Airflow + Power BI
- criação da DAG;
- automação do pipeline;
- testes de dependências;
- criação do dashboard inicial.

### Sprint 5  — Revisão
- revisão do pipeline;
- testes FULL e incremental;
- documentação;
- diagrama de arquitetura;
- preparação da apresentação.

---

## Evolução futura

Após a conclusão do MVP, o ObraIndex poderá evoluir para uma análise aplicada de orçamento de obras.

Exemplo:

> Quanto custaria hoje uma obra que foi orçada em determinado ano, utilizando preços históricos de materiais, serviços e mão de obra?

Possíveis aplicações futuras:

- UBS;
- UPA;
- escolas;
- habitações;
- edificações públicas;


---

## Motivação

O ObraIndex foi idealizado como um projeto que une conhecimento de **Engenharia Civil** com práticas modernas de **Engenharia de Dados**.

Mais do que construir um pipeline, o objetivo é demonstrar como dados públicos podem ser organizados, tratados e transformados em informação útil para análise de custos da construção civil.

---

## Status

Em desenvolvimento — MVP**

O repositório será desenvolvido inicialmente de forma privada e poderá ser disponibilizado publicamente após a conclusão e revisão da primeira versão.


