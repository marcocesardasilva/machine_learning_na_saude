# importar bibliotecas
import pandas as pd
import os


def group_files(data_folder):

    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, data_folder)
    # lista para armazenar os dataframes de cada arquivo CSV
    dfs = []

    print("--------------------------------------------------------------------------")
    print("Carregando os dados dos arquivos extraídos, tratando e concatenando...")
    # percorre todos os arquivos da pasta com a extensão .csv
    for arquivo in os.listdir(file_path):
        if arquivo.endswith('.csv'):
            # lê o arquivo CSV com o pandas
            df = pd.read_csv(os.path.join(file_path, arquivo), delimiter=';', skiprows=5, header=5, encoding='ISO-8859-1')
                    
            # adiciona as colunas de indicador e equipe
            df_colunas = pd.read_csv(os.path.join(file_path, arquivo), encoding='ISO-8859-1')
            indicador = df_colunas.iloc[5,0] # valor da linha 7 do arquivo CSV
            indicador = indicador.replace("Indicador: ", "")
            equipe = df_colunas.iloc[7, 0] # valor da linha 9 do arquivo CSV
            df['Indicador'] = indicador
            df['Equipe'] = equipe
            
            # adiciona a coluna com o nome do arquivo
            nome_arquivo = os.path.splitext(arquivo)[0] # remove a extensão do arquivo
            df['Arquivo'] = nome_arquivo

            # Definir Ano
            df['Ano'] = df.columns[5][:4]

            # Renomear coluna de % de quartil
            df = df.rename(columns={df.columns[5]: 'Percentual Quadrimestre'})

            # Definir Quadrimestre
            df['Quadrimestre'] = df['Arquivo'].str[20]
            
            # Remover a coluna vazia
            df = df.drop('Unnamed: 12', axis=1)
            df = df.drop('Arquivo', axis=1)

            # Remover as 5 últimas linas de rosto
            df = df.drop(df.tail(5).index)

            # adiciona o dataframe à lista de dataframes
            dfs.append(df)
    
    # concatena os dataframes em um único dataframe final
    df_group = pd.concat(dfs, ignore_index=True)

    # Incluir sks
    df_group['sk_indicador'] = df_group['Indicador'].map(df_group['Indicador'].drop_duplicates().reset_index(drop=True).reset_index().set_index('Indicador')['index'] + 1)
    df_group['sk_equipe'] = pd.factorize(df_group['Equipe'])[0] + 1
    df_group['sk_localidade'] = df_group['IBGE'].astype(int)
    df_group["sk_periodo"] = df_group["Ano"]+df_group["Quadrimestre"]
    print("Dataframe único criado com todos os dados.")
    print("--------------------------------------------------------------------------")

    return df_group

def create_dfs(df_group):
    print("--------------------------------------------------------------------------")
    print("Criando dataframes para geração de modelo dimensional star schema...")

    # mapeamento dos valores de de_indicador para nm_indicador
    map_indicador = {'Proporção de crianças de 1 (um) ano de idade vacinadas na APS contra Difteria, Tétano, Coqueluche, Hepatite B, infecções causadas por haemophilus influenzae tipo b e Poliomielite inativada': 'Crianças vacinadas',
                    'Proporção de gestantes com pelo menos 6 (seis) consultas pré-natal realizadas, sendo a 1ª (primeira) até a 12ª (décima segunda) semana de gestação': 'Gestantes pré-natal',
                    'Proporção de gestantes com atendimento odontológico realizado': 'Gestantes odontológico',
                    'Proporção de gestantes com realização de exames para sífilis e HIV': 'Gestantes exames DSTs',
                    'Proporção de mulheres com coleta de citopatológico na APS': 'Mulheres citopatológico',
                    'Proporção de pessoas com diabetes, com consulta e hemoglobina glicada solicitada no semestre': 'Pessoas diabetes',
                    'Proporção de pessoas com hipertensão, com consulta e pressão arterial aferida no semestre': 'Pessoas hipertensão'}

    # Criar df_indicador
    df_indicador =  df_group[['sk_indicador','Indicador']].drop_duplicates().reset_index(drop=True)
    df_indicador = df_indicador.rename(columns={'Indicador': 'de_indicador'})
    df_indicador['nm_indicador'] = df_indicador['de_indicador'].apply(lambda x: map_indicador[x])
    df_indicador = df_indicador.reindex(columns=['sk_indicador', 'nm_indicador', 'de_indicador'])
    df_indicador.name = 'df_indicador'
    print(f"Dataframe {df_indicador.name} criado.")

    # mapeamento dos valores de de_indicador para nm_indicador
    map_equipe = {'Considerado apenas (eSF e eAP) válidas para o componente de desempenho': 'Válidas',
                'Considerado apenas equipes (eSF e eAP) homologadas': 'Homologadas'}

    # Criar df_equipe
    df_equipe =  df_group[['sk_equipe','Equipe']].drop_duplicates().reset_index(drop=True)
    df_equipe = df_equipe.rename(columns={'Equipe': 'de_equipe'})
    df_equipe['nm_equipe'] = df_equipe['de_equipe'].apply(lambda x: map_equipe[x])
    df_equipe = df_equipe.reindex(columns=['sk_equipe', 'nm_equipe', 'de_equipe'])
    df_equipe.name = 'df_equipe'
    print(f"Dataframe {df_equipe.name} criado.")

    # Criar df_localidade
    df_localidade = df_group[['sk_localidade', 'UF', 'Munícipio']].drop_duplicates().reset_index(drop=True)
    df_localidade = df_localidade.rename(columns={'UF': 'uf', 'Munícipio': 'municipio'})
    df_localidade.name = 'df_localidade'
    print(f"Dataframe {df_localidade.name} criado.")

    # Criar df_periodo
    df_periodo = df_group[['sk_periodo','Ano','Quadrimestre']].drop_duplicates().reset_index(drop=True)
    df_periodo = df_periodo.rename(columns={'Ano': 'ano', 'Quadrimestre': 'quadrimestre'})
    df_periodo = df_periodo.reindex(columns=['sk_periodo', 'ano', 'quadrimestre'])
    df_periodo.name = 'df_periodo'
    print(f"Dataframe {df_periodo.name} criado.")

    # Criar fato_sisab
    fato_sisab = df_group[[
        'sk_indicador',
        'sk_equipe',
        'sk_periodo',
        'sk_localidade',
        'Numerador',
        'Denominador Utilizado',
        'Percentual Quadrimestre',
        'Denominador Identificado',
        'Denominador Estimado',
        'Cadastro',
        'Base Externa',
        'Percentual',
        'População'
        ]]
    fato_sisab = fato_sisab.rename(columns={
        'Numerador': 'numerador',
        'Denominador Utilizado': 'denominador_utilizado',
        'Percentual Quadrimestre': 'percentual_quadrimestre',
        'Denominador Identificado': 'denominador_identificado',
        'Denominador Estimado': 'denominador_estimado',
        'Cadastro': 'cadastro',
        'Base Externa': 'base_externa',
        'Percentual': 'percentual',
        'População': 'populacao'
        })
    fato_sisab['numerador'] = fato_sisab['numerador'].astype(int)
    fato_sisab['denominador_utilizado'] = fato_sisab['denominador_utilizado'].astype(int)
    fato_sisab['percentual_quadrimestre'] = fato_sisab['percentual_quadrimestre'].astype(int)
    fato_sisab['denominador_identificado'] = fato_sisab['denominador_identificado'].astype(int)
    fato_sisab['denominador_estimado'] = fato_sisab['denominador_estimado'].astype(int)
    fato_sisab['cadastro'] = fato_sisab['cadastro'].astype(int)
    fato_sisab['percentual'] = fato_sisab['percentual'].astype(int)
    fato_sisab['populacao'] = fato_sisab['populacao'].astype(int)
    fato_sisab.name = 'fato_sisab'
    print(f"Dataframe {fato_sisab.name} criado.")
    print("--------------------------------------------------------------------------")

    return df_indicador, df_equipe, df_localidade, df_periodo, fato_sisab

