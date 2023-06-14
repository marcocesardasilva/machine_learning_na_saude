# importar bibliotecas
import os
from multiprocessing.resource_sharer import stop
from urllib import request

def download_files(proximo_ano,data_folder):
    # Verifica se existe o diretório para os dados, se não existe cria ele
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Declaração das variáveis próximo ano e próximo mês
    proximo_ano = proximo_ano
    ano_baixar = str(proximo_ano).zfill(4)

    print('-------------------------------------------------')
    print(f'Proximo Ano a carregar: {ano_baixar}')
    print('-------------------------------------------------')

    # Definir link e arquivo
    if proximo_ano <= 2020:
        file = f'Mortalidade_Geral_{ano_baixar}.csv'
        link = f'https://diaad.s3.sa-east-1.amazonaws.com/sim/{file}'
    elif proximo_ano == 2021:
        file = f'Mortalidade_Geral_{ano_baixar}.csv'
        link = f'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/{file}'
    else:
        ano_abreviado = ano_baixar[-2:]
        file = f'DO{ano_abreviado}OPEN.csv'
        link = f'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/{file}'

    # Verifica se o arquivo foi baixado
    if(os.path.exists(f'{data_folder}/{file}')):
        print(f'Arquivo {file} já foi baixado baixado')
        atualizar = True
    else:
    # Tenta baixar o primeiro arquivo
        try:
            print(f'Baixando o arquivo {file}')
            request.urlretrieve(f'{link}', f'{data_folder}/{file}')
            # Verifica se o arquivo foi baixado
            if(os.path.exists(f'{data_folder}/{file}')):
                print(f'Arquivo {file} baixado')
                atualizar = True
        except:
            print(f'Arquivos de {ano_baixar} ainda não disponibilizado')
            print('-------------------------------------------------')
            print('Todos os arquivos disponíveis foram baixados!')
            print('-------------------------------------------------')
            atualizar = False

    return atualizar
