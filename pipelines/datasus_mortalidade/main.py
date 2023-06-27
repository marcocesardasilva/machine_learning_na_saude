# importar de outro arquivo
from load import *
from extract import *
from transform import *

##########################################################################
#                            Definir variáveis                           #
##########################################################################
file_key = "..\..\keys\ml-na-saude-ed1fc3c1a83e.json"
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
table_mortalidade = table_exist(client,dataset_fonte)

##########################################################################
#                              Executar ETL                              #
##########################################################################
while True:

    ##########################################################################
    #                             Extrair dados                              #
    ##########################################################################
    # Verificar próximo ano a baixar dados
    proximo_ano = update_date(client,credentials,dataset_fonte,table_mortalidade)
    if proximo_ano == None:
        break
    # Baixar arquivos
    file = download_files(proximo_ano,data_folder)
    if file == None:
        break
    
    ##########################################################################
    #                         Executar transformações                        #
    ##########################################################################
    # Criar df_mortalidade
    df_mortalidade = create_df(data_folder,file)

    ##########################################################################
    #                          Carregar dados no GCP                         #
    ##########################################################################
    # Incluir tabelas e dfs em uma biblioteca
    tables_dfs = {table_mortalidade:df_mortalidade} 
    load_data(tables_dfs,client,dataset_fonte)
