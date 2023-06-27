# importar bibliotecas
import os
from multiprocessing.resource_sharer import stop
from urllib import request

def download_files(proximo_ano,data_folder):
    # Verifica se existe o diretório para os dados, se não existe cria ele
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Declaração da variável ano_baixar
    ano_baixar = str(proximo_ano).zfill(4)

    print('-------------------------------------------------')
    print(f'Proximo Ano a carregar: {ano_baixar}')
    print('-------------------------------------------------')

    # Definir link e arquivo
    file = f'Mortalidade_Geral_{ano_baixar}.csv'
    link = f'https://diaad.s3.sa-east-1.amazonaws.com/sim/{file}'

    # Verifica se o arquivo foi baixado
    if(os.path.exists(f'{data_folder}/{file}')):
        print(f'Arquivo {file} já foi baixado baixado')
    else:
    # Tenta baixar o arquivo
        try:
            print(f'Baixando o arquivo {file}')
            request.urlretrieve(f'{link}', f'{data_folder}/{file}')
            # Verifica se o arquivo foi baixado
            if(os.path.exists(f'{data_folder}/{file}')):
                print(f'Arquivo {file} baixado')
            else:
                print('-------------------------------------------------------')
                print('Não foi possível baixar o arquivo. Execução finalizada!')
                print('-------------------------------------------------------')
                file = None
        except:
            print(f'Arquivos de {ano_baixar} ainda não disponibilizado')
            print('-------------------------------------------------')
            print('Todos os arquivos disponíveis foram baixados!')
            print('-------------------------------------------------')
            file = None

    return file
