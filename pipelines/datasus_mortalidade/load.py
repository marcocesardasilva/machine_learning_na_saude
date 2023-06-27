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
    return client,credentials

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

    # Tabela e schema da table_mortalidade
    table_mortalidade = dataset_fonte.table("mortalidade")

    schema_mortalidade = [
        bigquery.SchemaField("pk_base_origem", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ano_arquivo", "STRING"),
        bigquery.SchemaField("dt_obito", "DATE"),
        bigquery.SchemaField("dt_nascimento", "DATE"),
        bigquery.SchemaField("idade", "INTEGER"),
        bigquery.SchemaField("tp_obito", "STRING"),
        bigquery.SchemaField("cd_municipio_residencia", "STRING"),
        bigquery.SchemaField("cid10", "STRING")
    ]

    print("--------------------------------------------------------------------------")
    print("Verificando a existência das tabelas no GCP...")
    try:
        client.get_table(table_mortalidade, timeout=30)
        print(f"A tabela {table_mortalidade} já existe!")
        print("--------------------------------------------------------------------------")
    except:
        print(f"Tabela {table_mortalidade} não encontrada! Criando tabela {table_mortalidade}...")
        client.create_table(bigquery.Table(table_mortalidade, schema=schema_mortalidade))
        print(f"A tabela {table_mortalidade} foi criada.")
        print("--------------------------------------------------------------------------")

    return table_mortalidade

def update_date(client,credentials,dataset_fonte,table_mortalidade):
    ##########################################################################
    #         Verifica a data de atualização para baixar os arquivos         #
    ##########################################################################
    # Construir a query
    query = f"""
        SELECT MAX(ano_arquivo) AS nu_ano
        FROM `{credentials.project_id}.{dataset_fonte.dataset_id}.{table_mortalidade.table_id}`
    """
    # Executar a query
    query_job = client.query(query)
    # Obter os resultados
    results = query_job.result()        
    # Iterar sobre os resultados
    for row in results:
        if row["nu_ano"] != None:
            proximo_ano = int(row["nu_ano"])
        else:
            proximo_ano = 2010
    if proximo_ano >= 2020:
        proximo_ano = None
        print('-------------------------------------------------')
        print('Todos os arquivos necessários já foram baixados!')
        print('-------------------------------------------------')
    else:
        print(f"Próximo ano à carregar é: {proximo_ano}")
    return proximo_ano

def load_data(tables_dfs,client,dataset_fonte):
    print("--------------------------------------------------------------------------")
    print("Carregando dados no GCP...")
    for tabela, df in tables_dfs.items():
        table_ref = client.dataset(dataset_fonte.dataset_id).table(tabela.table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Dados carregados na tabela {tabela}.")
    
    print("--------------------------------------------------------------------------")
    print("##########################################################################")
    print("#                         Dados carregados no GCP                        #")
    print("##########################################################################")

