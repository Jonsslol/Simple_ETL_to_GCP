# **Projeto de ETL com Airflow e GCP**

Este projeto implementa um pipeline de ETL (Extract, Transform, Load) usando o **Apache Airflow** e o **Google Cloud Platform (GCP)**. O objetivo é coletar dados de uma API pública, armazená-los no **Google Cloud Storage (GCS)**, processá-los no **BigQuery** e exportar os dados transformados de volta para o GCS.

O projeto é composto por duas DAGs principais:
1. **`gcp_bronze_layer`**: Responsável pela coleta e armazenamento dos dados brutos.
2. **`gcp_silver_layer`**: Responsável pela transformação e exportação dos dados.
3. **`gcp_gold_layer`**: Responsável pela agregação dos dados transformados.
   
---

## **Funcionalidades**

### **DAG `gcp_bronze_layer`**
1. **Coleta de Dados:**
   - Coleta dados da API [Open Brewery DB](https://api.openbrewerydb.org/breweries).
   - Armazena os dados brutos no Google Cloud Storage em formato JSON.

2. **Formatação de Dados:**
   - Converte o JSON para o formato **newline-delimited** (um objeto por linha).

---

### **DAG `gcp_silver_layer`**
1. **Carregamento de Dados:**
   - Carrega os dados brutos do GCS para o BigQuery.

2. **Transformação de Dados:**
   - Remove duplicatas e trata valores nulos.
   - Cria uma tabela transformada no BigQuery.

3. **Exportação de Dados:**
   - Exporta os dados transformados para o GCS em formato **Parquet**.

---

## **Tecnologias Utilizadas**

- **Apache Airflow**: Orquestração do pipeline de ETL.
- **Google Cloud Platform (GCP)**:
  - **Google Cloud Storage (GCS)**: Armazenamento de dados brutos e transformados.
  - **BigQuery**: Processamento e transformação de dados.
- **Python**: Linguagem de programação usada para scripts e automação.

---

## **Configuração do Ambiente**

### **Pré-requisitos**

1. **Google Cloud Platform (GCP):**
   - Crie um projeto no GCP.
   - Ative as APIs:
     - **Cloud Storage**
     - **BigQuery**
   - Crie uma service account com as seguintes permissões:
     - `roles/storage.admin`
     - `roles/bigquery.dataEditor`

2. **Apache Airflow:**
   - Instale o Airflow em um ambiente local ou use o **Astronomer** para gerenciar o Airflow na nuvem.
   - Instale os providers necessários:
     ```bash
     pip install apache-airflow-providers-google
     ```

3. **Variáveis do Airflow:**
   - Defina as seguintes variáveis na UI do Airflow (**Admin > Variables**):
     | **Key**                     | **Value**                          |
     |-----------------------------|------------------------------------|
     | `gcp_bucket_name`           | `meu-bucket-exemplo`               |
     | `gcp_project_id`            | `meu-projeto-exemplo`              |
     | `gcp_dataset_name`          | `breweries_silver`                |
     | `gcp_table_raw`             | `breweries_raw`                   |
     | `gcp_table_transformed`     | `breweries_transformed`           |
     | `gcp_region`                | `us-central1`                     |

---

## **Estrutura do Projeto**

```
gcp_etl_pipeline/
├── dags/
│   ├── gcp_bronze_layer.py    # DAG para a camada Bronze
│   ├── gcp_silver_layer.py    # DAG para a camada Prata
│   └── gcp_gold_layer.py      # DAG para a camada Ouro
├── README.md                  # Documentação
└── requirements.txt           # Dependências do projeto
```

---

## **Como Executar**

### **1. Configuração do Airflow**

1. **Conexões do Airflow:**
   - Adicione uma conexão do tipo **Google Cloud** na UI do Airflow:
     - **Conn Id**: `google_cloud_default`
     - **Conn Type**: `Google Cloud`
     - **Keyfile JSON**: Cole o conteúdo do arquivo JSON da service account.

2. **Instale as Dependências:**
   - No ambiente do Airflow, instale as dependências necessárias:
     ```bash
     pip install -r requirements.txt
     ```

### **2. Executando as DAGs**

1. **Ative as DAGs:**
   - Na UI do Airflow, ative as DAGs `gcp_bronze_layer` e `gcp_silver_layer`.

2. **Execute as DAGs:**
   - Execute as DAGs manualmente ou aguarde a execução agendada.

3. **Verifique os Logs:**
   - Acompanhe a execução das DAGs pelos logs de cada tarefa.

---

## **Fluxo das DAGs**

### **DAG `gcp_bronze_layer`**
1. **Coleta de Dados:**
   - Faz uma requisição à API Open Brewery DB.
   - Armazena os dados brutos no GCS em formato JSON.

2. **Formatação de Dados:**
   - Converte o JSON para o formato **newline-delimited**.
   - Salva o arquivo corrigido no GCS.

---

### **DAG `gcp_silver_layer`**
1. **Carregamento de Dados:**
   - Carrega os dados do GCS para a tabela `breweries_raw` no BigQuery.

2. **Transformação de Dados:**
   - Remove duplicatas e trata valores nulos.
   - Cria a tabela `breweries_transformed` no BigQuery.

3. **Exportação de Dados:**
   - Exporta os dados transformados para o GCS em formato **Parquet**.

---

### **DAG `gcp_gold_layer`**
A DAG `gcp_gold_layer` cria uma visão agregada dos dados da camada Silver, respondendo à pergunta: "Quantas cervejarias existem por tipo (`brewery_type`) e localização (`state`)?".

#### **Funcionalidades**

- **Agregação de Dados:**
  - Agrupa os dados da tabela `breweries_transformed` (camada Silver) por `brewery_type` e `state`.
  - Conta o número de cervejarias para cada combinação de tipo e estado.

- **Armazenamento dos Dados Agregados:**
  - Salva o resultado em uma nova tabela no BigQuery chamada `breweries_aggregated`.

- **Exportação dos Dados:**
  - Exporta os dados agregados para o Google Cloud Storage em formato **Parquet**.

#### **Fluxo da DAG**

1. **Criar Tabela Agregada:**
   - Executa uma consulta SQL no BigQuery para agregar os dados.
   - Armazena o resultado na tabela `breweries_aggregated`.

2. **Exportar Dados Agregados:**
   - Exporta os dados da tabela `breweries_aggregated` para o GCS em formato **Parquet**.

#### **Exemplo de Consulta SQL**
A consulta SQL usada na DAG é a seguinte:

```sql
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.breweries_aggregated` AS
SELECT
    brewery_type,
    state,
    COUNT(*) AS total_breweries
FROM
    `{PROJECT_ID}.{DATASET_NAME}.breweries_transformed`
GROUP BY
    brewery_type, state
ORDER BY
    total_breweries DESC;
```

#### **Estrutura da Tabela Agregada**
A tabela `breweries_aggregated` terá a seguinte estrutura:

| brewery_type | state      | total_breweries |
|-------------|-----------|----------------|
| micro      | California | 150            |
| nano       | Texas      | 80             |
| brewpub    | New York   | 60             |


---

## **Exemplo de Variáveis de Ambiente**

```bash
export GCP_BUCKET_NAME="meu-bucket-exemplo"
export GCP_PROJECT_ID="meu-projeto-exemplo"
export GCP_DATASET_NAME="breweries_silver"
export GCP_TABLE_RAW="breweries_raw"
export GCP_TABLE_TRANSFORMED="breweries_transformed"
export GCP_REGION="us-central1"
```

---

## **Contato**

- **Nome**: João Quadros
- **Email**: jvquadroscontatos@hotmail.com
- **Linkedin**: www.linkedin.com/in/jquadros

