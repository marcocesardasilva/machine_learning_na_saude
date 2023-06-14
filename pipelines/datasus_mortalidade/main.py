# importar de outro arquivo
from load import *
from extract import *
from transform import *

##########################################################################
#                            Definir variáveis                           #
##########################################################################
file_key = "keys\ml-na-saude-ed1fc3c1a83e.json"
dataset_name = "mortalidade"
data_folder = "dados"

##########################################################################
#               Criar conexão com o GCP, dataset e tabelas               #
##########################################################################
# Conexão com GCP
client, credentials = gcp_connection(file_key)
# Verificar se o dataset já existe, se não existe, cria
dataset_fonte = dataset_exist(client,dataset_name)
# Verifica se as tabelas já existem, se não existe, cria
table_fato_mortalidade = table_exist(client,dataset_fonte)

##########################################################################
#                              Executar ETL                              #
##########################################################################
while True:

    ##########################################################################
    #                             Extrair dados                              #
    ##########################################################################
    # Verificar próximo ano a baixar dados
    proximo_ano = update_date(client,credentials,dataset_fonte,table_fato_mortalidade)
    # Baixar arquivos
    atualizar = download_files(proximo_ano,data_folder)
    if atualizar == False:
        break

    ##########################################################################
    #                         Executar transformações                        #
    ##########################################################################
    # Ler todos os arquivos, tratar e agrupar em um único dataframe
    df_group = group_files(data_folder)
    # Criar dfs da fato e das dimensões
    fato_mortalidade = create_dfs(df_group)

    ##########################################################################
    #                          Carregar dados no GCP                         #
    ##########################################################################
    # Incluir tabelas e dfs em uma biblioteca
    tables_dfs = {table_fato_mortalidade:fato_mortalidade} 
    load_data(tables_dfs,client,dataset_fonte)
