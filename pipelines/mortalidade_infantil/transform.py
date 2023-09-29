# importar bibliotecas
import os
import pandas as pd
import basedosdados as bd


def create_df(data_folder):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, data_folder)

    print("--------------------------------------------------------------------------")
    print("Carregando os dados dos arquivos extraídos, tratando e concatenando...")

    # lista com os dataframes já tratados
    dfs = []

    # Função para gerar o dataframe
    for arquivo in os.listdir(file_path):
        if arquivo.endswith('.csv'):
            # Ler o arquivo CSV com o pandas
            df = pd.read_csv(os.path.join(file_path, arquivo), delimiter=';', encoding='ISO-8859-1', low_memory=False)
            # Realizar transformação das datas de nascimento e óbito
            df['dt_obito'] = pd.to_datetime(df['DTOBITO'], format='%d%m%Y', errors='coerce')
            df['dt_nasc'] = pd.to_datetime(df['DTNASC'], format='%d%m%Y', errors='coerce')
            # Excluir dados nulos para data de nascimento e de óbito
            df = df.dropna(subset=['dt_nasc'])
            df = df.dropna(subset=['dt_obito'])
            # Criar a coluna idade
            df['idade'] = ((df['dt_obito'] - df['dt_nasc']).dt.days / 365.25).round(2)
            # Manter apenas dados com idades válidas
            df = df[df['idade'] >= 0]
            # Manter apenas dados de Menores de 5 anos
            df = df[df['idade'] <= 5]
            # Criar as colunas ano_obito e quadrimestre_obito
            df['ano_obito'] = df['dt_obito'].dt.year.astype(float).astype(pd.Int64Dtype()).astype(str).where(df['dt_obito'].notna())
            df['quad_obito'] = pd.cut(df['dt_obito'].dt.month, bins=[1, 5, 9, 13], labels=[1, 2, 3], right=False)
            # Extrair os 6 primeiros dígitos da coluna CODMUNRES
            df['cd_mun_res'] = df['CODMUNRES'].astype(str).str.slice(stop=6)
            # Selecionar coluna desejadas
            df = df[['ano_obito','quad_obito','dt_obito','dt_nasc','idade','cd_mun_res']]
            # adiciona o dataframe à lista de dataframes
            dfs.append(df)

    # concatena os dataframes em um único dataframe final
    df_group = pd.concat(dfs, ignore_index=True)
    # Baixa a base de população por município
    df_download = bd.read_table(dataset_id='br_ms_populacao',table_id='municipio',billing_project_id="ml-na-saude")
    # Agrupa apenas as colunas desejadas
    df_populacao = df_download.groupby(['ano', 'id_municipio'])['populacao'].sum().reset_index()
    # Transformar id_municipio para apenas 6 digitos
    df_populacao['id_municipio'] = df_populacao['id_municipio'].astype(str).str[:6]
    # Alterar tipo de dados do ano
    df_populacao['ano'] = df_populacao['ano'].astype(str)
    # Merge os dataframes com base nas condições especificadas
    df_yll = df_group.merge(df_populacao, how='left', left_on=['ano_obito', 'cd_mun_res'], right_on=['ano', 'id_municipio'])
    # Drop das colunas desnecessárias após a junção
    df_yll.drop(['ano', 'id_municipio'], axis=1, inplace=True)

    return df_yll

