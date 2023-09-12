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

    # Lista com os códigos de cid-10 pertencentes aos ICSAPS
    COD_ICSAPS = ["A37","A36","A33","A34","A35","B26","B06","B05","A95","B16","G000","A170","A19","A150","A151","A152","A153","A160","A161","A162",
            "A154","A155","A156","A157","A158","A159","A163","A164","A165","A166","A167","A168","A169","A171","A172","A173","A174","A175","A176",
            "A177","A178","A179","A18","I00","I01","I02","A51","A52","A53","B50","B51","B52","B53","B54","B77","E86","A00","A01","A02","A03","A04",
            "A05","A06","A07","A08","A09","D50","E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E57","E58",
            "E59","E60","E61","E62","E63","E64","H66","J00","J01","J02","J03","J06","J31","J13","J14","J153","J154","J158","J159","J181","J45",
            "J46","J20","J21","J40","J41","J42","J43","J47","J44","I10","I11","I20","I50","J81","I63","I64","I65","I66","I67","I69","G45","G46",
            "E100","E101","E110","E111","E120","E121","E130","E131","E140","E141","E102","E103","E104","E105","E106","E107","E108","E112","E113",
            "E114","E115","E116","E117","E118","E122","E123","E124","E125","E126","E127","E128","E132","E133","E134","E135","E136","E137","E138",
            "E142","E143","E144","E145","E146","E147","E148","E109","E119","E129","E139","E149","G40","G41","N10","N11","N12","N30","N34","N390",
            "A46","L01","L02","L03","L04","L08","N70","N71","N72","N73","N75","N76","K25","K26","K27","K28","K920","K921","K922","O23","A50","P350"]

    # Função para calcular a expectativa de vida
    def calcular_expectativa_de_vida(idade):
        if idade < 0.08:
            return 89.99
        elif 0.08 <= idade < 1:
            return 89.55
        elif 1 <= idade < 5:
            return 89.07
        elif 5 <= idade < 10:
            return 82.58
        elif 10 <= idade < 15:
            return 77.58
        elif 15 <= idade < 20:
            return 72.60
        elif 20 <= idade < 25:
            return 67.62
        elif 25 <= idade < 30:
            return 62.66
        elif 30 <= idade < 35:
            return 57.71
        elif 35 <= idade < 40:
            return 52.76
        elif 40 <= idade < 45:
            return 47.83
        elif 45 <= idade < 50:
            return 42.94
        elif 50 <= idade < 55:
            return 38.10
        elif 55 <= idade < 60:
            return 33.33
        elif 60 <= idade < 65:
            return 28.66
        elif 65 <= idade < 70:
            return 24.12
        elif 70 <= idade < 75:
            return 19.76
        elif 75 <= idade < 80:
            return 15.65
        elif 80 <= idade < 85:
            return 11.69
        else:
            return 7.05

    # Função para gerar o dataframe
    for arquivo in os.listdir(file_path):
        if arquivo.endswith('.csv'):
            # Ler o arquivo CSV com o pandas
            df = pd.read_csv(os.path.join(file_path, arquivo), delimiter=';', encoding='ISO-8859-1', low_memory=False)
            # Analisar se é ou não icsaps
            df['icsaps'] = df['CAUSABAS'].apply(lambda x: 'Sim' if x in COD_ICSAPS else 'Não')
            # Manter apenas dados que são icsaps
            df = df[df['icsaps'] == 'Sim']
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
            # Criar coluna expectativa de vida e yll
            df['yll'] = df.apply(lambda row: calcular_expectativa_de_vida(row['idade']), axis=1)
            # Criar as colunas ano_obito e quadrimestre_obito
            df['ano_obito'] = df['dt_obito'].dt.year.astype(float).astype(pd.Int64Dtype()).astype(str).where(df['dt_obito'].notna())
            df['quad_obito'] = pd.cut(df['dt_obito'].dt.month, bins=[1, 5, 9, 13], labels=[1, 2, 3], right=False)
            # Extrair os 6 primeiros dígitos da coluna CODMUNRES
            df['cd_mun_res'] = df['CODMUNRES'].astype(str).str.slice(stop=6)
            # Renomear colunas
            df = df.rename(columns={'CAUSABAS':'cid10'})
            # Selecionar coluna desejadas
            df = df[['ano_obito','quad_obito','dt_obito','dt_nasc','idade','yll','cid10','cd_mun_res']]
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

