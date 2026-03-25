[![dbt_ci_pipeline](https://github.com/robertoBorgesJr/SPTrans/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/robertoBorgesJr/SPTrans/actions/workflows/dbt_ci.yml)

# 🚌 SPTrans GTFS Data Pipeline (Databricks + dbt)

Este projeto implementa um pipeline de dados moderno para processar e analisar dados de transporte público (GTFS) da SPTrans. O objetivo é transformar dados brutos em insights operacionais sobre a frequência de linhas e a densidade de paradas na cidade de São Paulo.

## 🛠️ Stack Tecnológica
- **Cloud:** Databricks (Lakehouse Architecture)
- **Linguagens:** SQL, Python (PySpark)
- **Transformação:** dbt-core (v1.11.6)
- **Orquestração de Metadados:** dbt Semantic Layer (Metrics & Saved Queries)
- **Gerenciamento de Pacotes:** `uv` (Fast Python package installer)
- **CI/CD:** GitHub Actions

## 🏗️ Arquitetura de Dados (Medallion)

O projeto segue a arquitetura de medalhão para garantir qualidade e linhagem:

1.  **Bronze:** Ingestão de dados brutos do GTFS no Delta Lake.
2.  **Silver (Spark Streaming):** Limpeza, padronização de tipos e deduplicação via PySpark. Utiliza `writeStream` com `trigger(availableNow=True)` para processamento eficiente.
3.  **Gold (dbt):** Modelagem dimensional, agregações de negócio e camada semântica.

## 📊 Inteligência de Dados & Camada Semântica

Diferente de pipelines tradicionais, este projeto utiliza a **dbt Semantic Layer** para garantir uma "Fonte Única da Verdade" (Single Source of Truth):

- **Métricas Governas:** Definição centralizada de `media_headway_global` e `total_viagens`.
- **Análise de Tendência:** Implementação de **Média Móvel de 7 dias** (`media_headway_7_dias`) para suavizar a sazonalidade dos dados de transporte.
- **Saved Queries:** Consultas pré-configuradas para consumo imediato por ferramentas de BI (Power BI/Looker).

## 🚦 Qualidade e Automação (CI/CD)

- **Testes de Dados:** Validação automática de chaves únicas, campos não nulos e regras de negócio (ex: intervalos de tempo positivos).
- **GitHub Actions:** Pipeline automatizado que executa `dbt build` a cada commit, garantindo que apenas códigos validados cheguem à branch principal.

## 🚀 Como Executar

### Pré-requisitos
- Python 3.10+ e `uv` instalado.
- Acesso a um SQL Warehouse no Databricks.

### Instalação
1. Clone o repositório:
   ```bash
   git clone [https://github.com/seu-usuario/sptrans-dbt-project.git](https://github.com/seu-usuario/sptrans-dbt-project.git)

2. Crie o ambiente virtual e instale as dependências:
   uv venv
   source .venv/bin/activate  # No Windows: .venv\Scripts\activate
   uv pip install dbt-databricks

3. Instale os pacotes do dbt:
   dbt deps

### Execução
- Para rodar os modelos da camada Gold e executar os testes de qualidade:
  dbt build --profiles-dir .

### Documentação
- Para visualizar o dicionário de dados e a linhagem do projeto:
  dbt docs generate
  dbt docs serve     

![Gráfico de Linhagem do dbt](img/dbt-lineage.png)


---
Desenvolvido por Roberto Elias Borges Jr - Foco em Data Science e Engenharia de Dados  