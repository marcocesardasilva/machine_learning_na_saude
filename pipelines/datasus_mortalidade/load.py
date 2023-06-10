# importar bibliotecas
from google.cloud import bigquery
from google.oauth2 import service_account
import os


def gcp_connection(file_key):
    ##########################################################################
    #                        Cria a conexão com o GCP                        #
    ##########################################################################

    print("##########################################################################")
    print("#                     Iniciando execução do programa                     #")
    print("##########################################################################")
    print("--------------------------------------------------------------------------")
    print("Criando conexão com o GCP...")
    try:
        current_directory = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_directory, file_key)
        credentials = service_account.Credentials.from_service_account_file(file_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        print(f"Conexão realizada com sucesso com o projeto {credentials.project_id}.")
        print("--------------------------------------------------------------------------")
    except Exception:
        print(f"Não foi possível efetivar a conexão com o GCP.")
        print("--------------------------------------------------------------------------")
    return client

def dataset_exist(client,dataset_name):
    ##########################################################################
    #                     Cria o dataset caso não exista                     #
    ##########################################################################
    print("--------------------------------------------------------------------------")
    print("Verificando a existência do dataset no GCP...")
    dataset_fonte = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_fonte)
        print(f"O conjunto de dados {dataset_fonte} já existe no GCP.")
        print("--------------------------------------------------------------------------")
    except Exception:
        print(f"Dataset {dataset_fonte} não foi encontrado no GCP, criando o dataset...")
        client.create_dataset(dataset_fonte)
        print(f"O conjunto de dados {dataset_fonte} foi criado no GCP com sucesso.")
        print("--------------------------------------------------------------------------")
    return dataset_fonte

def table_exist(client,dataset_fonte):
    ##########################################################################
    #                    Cria as tabelas caso não existam                    #
    ##########################################################################

    # Tabela e schema da tabela dim_indicador
    table_indicador = dataset_fonte.table("dim_indicador")

    schema_indicador = [
        bigquery.SchemaField("sk_indicador", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("nm_indicador", "STRING"),
        bigquery.SchemaField("de_indicador", "STRING")
    ]

    # Tabela e schema da tabela dim_equipe
    table_equipe = dataset_fonte.table("dim_equipe")

    schema_equipe = [
        bigquery.SchemaField("sk_equipe", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("nm_equipe", "STRING"),
        bigquery.SchemaField("de_equipe", "STRING")
    ]

    # Tabela e schema da tabela dim_localidade
    table_localidade = dataset_fonte.table("dim_localidade")

    schema_localidade = [
        bigquery.SchemaField("sk_localidade", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("uf", "STRING"),
        bigquery.SchemaField("municipio", "STRING")
    ]

    # Tabela e schema da tabela dim_periodo
    table_periodo = dataset_fonte.table("dim_periodo")

    schema_periodo = [
        bigquery.SchemaField("sk_periodo", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ano", "INTEGER"),
        bigquery.SchemaField("quadrimestre", "INTEGER")
    ]

    # Tabela e schema da tabela fato_sisab
    table_fato_sisab = dataset_fonte.table("fato_sisab")

    schema_fato_sisab = [
        bigquery.SchemaField("sk_indicador", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sk_equipe", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sk_periodo", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sk_localidade", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("numerador", "INTEGER"),
        bigquery.SchemaField("denominador_utilizado", "INTEGER"),
        bigquery.SchemaField("percentual_quadrimestre", "INTEGER"),
        bigquery.SchemaField("denominador_identificado", "INTEGER"),
        bigquery.SchemaField("denominador_estimado", "INTEGER"),
        bigquery.SchemaField("cadastro", "INTEGER"),
        bigquery.SchemaField("base_externa", "FLOAT64"),
        bigquery.SchemaField("percentual", "INTEGER"),
        bigquery.SchemaField("populacao", "INTEGER")
    ]

    tabelas = {
        table_indicador:schema_indicador,
        table_equipe:schema_equipe,
        table_localidade:schema_localidade,
        table_periodo:schema_periodo,
        table_fato_sisab:schema_fato_sisab
    }

    print("--------------------------------------------------------------------------")
    print("Verificando a existência das tabelas no GCP...")
    for tabela, schema in tabelas.items():
        try:
            client.get_table(tabela, timeout=30)
            print(f"A tabela {tabela} já existe!")
            print("--------------------------------------------------------------------------")
        except:
            print(f"Tabela {tabela} não encontrada! Criando tabela {tabela}...")
            client.create_table(bigquery.Table(tabela, schema=schema))
            print(f"A tabela {tabela} foi criada.")
            print("--------------------------------------------------------------------------")

    return table_indicador, table_equipe, table_localidade, table_periodo, table_fato_sisab

def load_data(tables_dfs,client,dataset_fonte):
    print("--------------------------------------------------------------------------")
    print("Carregando dados no GCP...")
    for tabela, df in tables_dfs.items():
        table_ref = client.dataset(dataset_fonte.dataset_id).table(tabela.table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Dados carregados na tabela {tabela}.")
    
    print("--------------------------------------------------------------------------")
    print("##########################################################################")
    print("#                       Fim da execução do programa                      #")
    print("##########################################################################")

