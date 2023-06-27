# importar de outro arquivo
from load import *
from transform import *

##########################################################################
#                            Definir variáveis                           #
##########################################################################
file_key = "..\..\keys\ml-na-saude-ed1fc3c1a83e.json"
dataset_name = "indicadores_sisab"
data_folder = "dados"

##########################################################################
#               Criar conexão com o GCP, dataset e tabelas               #
##########################################################################
# Conexão com GCP
client = gcp_connection(file_key)
# Verificar se o dataset já existe, se não existe, cria
dataset_fonte = dataset_exist(client,dataset_name)
# Verifica se as tabelas já existem, se não existe, cria
table_indicador, table_equipe, table_localidade, table_periodo, table_fato_sisab = table_exist(client,dataset_fonte)

##########################################################################
#                         Executar transformações                        #
##########################################################################
# Ler todos os arquivos, tratar e agrupar em um único dataframe
df_group = group_files(data_folder)
# Criar dfs da fato e das dimensões
df_indicador, df_equipe, df_localidade, df_periodo, fato_sisab = create_dfs(df_group)

##########################################################################
#                          Carregar dados no GCP                         #
##########################################################################
# Incluir tabelas e dfs em uma biblioteca
tables_dfs = {table_indicador:df_indicador,table_equipe:df_equipe,table_localidade:df_localidade,table_periodo:df_periodo,table_fato_sisab:fato_sisab} 
load_data(tables_dfs,client,dataset_fonte)

