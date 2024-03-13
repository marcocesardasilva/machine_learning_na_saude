# importar de outro arquivo
from load import *
from extract import *
from transform import *

##########################################################################
#                            Definir variáveis                           #
##########################################################################
file_key = r"C:\Users\User\Documents\GitHub\machine_learning_na_saude\keys\ml-na-saude-ed1fc3c1a83e.json"
dataset_name = "mortalidade_infantil"
data_folder = r"C:\Users\User\Documents\GitHub\machine_learning_na_saude\pipelines\mortalidade_infantil\dados"

##########################################################################
#               Criar conexão com o GCP, dataset e tabelas               #
##########################################################################
# Conexão com GCP
client = gcp_connection(file_key)
# Verificar se o dataset já existe, se não existe, cria
dataset_fonte = dataset_exist(client,dataset_name)
# Verifica se as tabelas já existem, se não existe, cria
table_mortalidade = table_exist(client,dataset_fonte)

##########################################################################
#                             Extrair dados                              #
##########################################################################
# Baixar arquivos
download_files(data_folder)

##########################################################################
#                         Executar transformações                        #
##########################################################################
# Criar df_mortalidade
df_mortalidade = create_df(data_folder)
##########################################################################
#                          Carregar dados no GCP                         #
##########################################################################
# Incluir tabelas e dfs em uma biblioteca
tables_dfs = {table_mortalidade:df_mortalidade}
load_data(tables_dfs,client,dataset_fonte)
