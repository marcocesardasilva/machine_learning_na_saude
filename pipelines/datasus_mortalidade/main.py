# importar de outro arquivo
from load import *
from extract import *
from transform import *

##########################################################################
#                            Definir variáveis                           #
##########################################################################
file_key = "..\..\keys\ml-na-saude-ed1fc3c1a83e.json"
dataset_name = "yll_por_obito"
data_folder = "dados"

##########################################################################
#               Criar conexão com o GCP, dataset e tabelas               #
##########################################################################
# Conexão com GCP
client = gcp_connection(file_key)
# Verificar se o dataset já existe, se não existe, cria
dataset_fonte = dataset_exist(client,dataset_name)
# Verifica se as tabelas já existem, se não existe, cria
table_yll = table_exist(client,dataset_fonte)

##########################################################################
#                             Extrair dados                              #
##########################################################################
# Baixar arquivos
download_files(data_folder)

##########################################################################
#                         Executar transformações                        #
##########################################################################
# Criar df_mortalidade
df_yll = create_df(data_folder)

##########################################################################
#                          Carregar dados no GCP                         #
##########################################################################
# Incluir tabelas e dfs em uma biblioteca
tables_dfs = {table_yll:df_yll} 
load_data(tables_dfs,client,dataset_fonte)
