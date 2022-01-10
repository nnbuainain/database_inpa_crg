import pandas as pd
import numpy as np
import glob
import re

PATH = '../spreadsheets'
AVE = glob.glob(PATH + '/INPA_AVES*.xlsx')[0]
HERPS = glob.glob(PATH + '/BDHerpeto*.xlsx')[0]
FISH = glob.glob(PATH + '/Banco*.xlsx')[0]

ave = pd.read_excel('./spreadsheets/INPA_AVES Tecidos_Dezembro2021.xlsx')
herps = pd.read_excel('./spreadsheets/BDHerpeto atualizado 07.01.2022.xlsx')
# fish = pd.read_excel('./spreadsheets/Banco de dados de Peixes_2021_05_31.xlsx',header=1)

# Rename columns
ave = ave.drop(['LAT G', 'LAT M', 'LAT S', 'LAT_N/S', 'LON G', 'LON M', 'LON S', 'LON_E/W'],axis=1).rename(columns={'No TEC':'num_amostra', 'numero':'so_numero_amostra','Sigla prep':'sigla_preparador','Nº prepa':'numero_preparador', 'Nome preparador':'nome_coletor', 'Sigla campo':'sigla_num_campo', 'Nº campo':'so_numero_campo', 'ORDEM':'ordem', 'FAMILIA':'familia', 'GÊNERO':'genero', 'ESPÉCIE':'especie', 'GÊNERO ESPÉCIE':'genero_especie', 'SEXO':'sexo', 'EXPEDIÇÃO':'expedicao', 'PAÍS':'pais', 'EST':'estado', 'LOCALIDADE':'localidade', 'LAT_DEC':'latitude', 'LON_DEC':'longitude', 'TEMPO ATÉ CONSERVAR':'tempo_ate_conservar', 'MÉTODO DE COLETA':'metodo_coleta', 'MÚSCULO':'musculo', 'CORAÇÃO':'coracao', 'FÍGADO':'figado', 'SANGUE':'sangue', 'MEIO PRESERV. DEF.':'meio_preserv_def', 'DATA COLETA':'data_coleta', 'DATA PREP.':'data_preparacao', 'OBSERVAÇÕES':'observacao', 'EMPRESTADO':'emprestado', 'DATA':'data_emprestimo_1', 'GUIA N°':'num_guia_emprestimo_1', 'PARA':'pessoa_emprestimo_1', 'Obs':'obs_emprestimo_1', 'EMPRESTADO.1':'emprestado_2', 'DATA.1':'data_emprestimo_2', 'GUIA N°.1':'num_guia_emprestimo_2', 'PARA.1':'pessoa_emprestimo_2', 'EMPRESTADO.2':'emprestado_3', 'DATA.2':'data_emprestimo_3', 'GUIA N°.2':'num_guia_emprestimo_3', 'PARA.2':'pessoa_emprestimo_3', 'EMPRESTADO.3':'emprestado_4', 'DATA.3':'data_emprestimo_4', 'GUIA N°.3':'num_guia_emprestimo_4', 'PARA.3':'pessoa_emprestimo_4'})
herps = herps.drop(['LAT G', 'LAT M', 'LAT S', 'LAT_N/S', 'LON G', 'LON M', 'LON S', 'LON_E/W','COORDENADAS GEOGRÁFICAS', 'Unnamed: 26'],axis=1).rename(columns={'INPA - HT':'num_amostra', 'INPA-H':'num_voucher', 'No. CAMPO':'num_campo', 'DATA':'data_coleta', 'ORDEM':'ordem', 'FAMÍLIA':'familia', 'GÊNERO':'genero','ESPÉCIE':'genero_especie', 'LOCALIDADE':'localidade','MUNICÍPIO':'municipio', 'ESTADO':'estado', 'LAT_DEC':'latitude', 'LONG_DEC':'longitude', 'COLETOR':'nome_coletor', 'OBSERVAÇÕES':'observacao'})
# fish  = fish.drop(['Unnamed: 3'],axis=1).rename(columns={'STATUS':'status', 'CAIXA':'num_caixa', 'Número de DNA':'num_amostra', 'Número de CAMPO':'num_campo', 'VOUCHER':'num_voucher', 'ORDEM':'ordem', 'FAMÍLIA':'familia', 'GÊNERO':'genero', 'ESPÉCIE':'especie', 'NOME COMUM':'nome_comum', 'DATA DE COLETA':'data_coleta', 'País':'pais', 'ESTADO':'estado', 'MUNICÍPIO':'municipio', 'DRENAGEM':'drenagem', 'SUBDRENAGEM':'subdrenagem', 'LOCAL DE PESCA':'localidade_pesca', 'LOCAL DE COLETA':'localidade', 'LATITUDE':'latitude', 'LONGITUDE':'longitude', 'COLETORES':'nome_coletor', 'RESPONSÁVEL/PROJETOS':'responsavel_projetos', 'Comprimento Padrão (cm)':'comprimento_padrao', 'OBSERVAÇÕES':'observacao'})


## make column genero_especie in fish df

# fish['genero_especie'] = fish[['genero', 'especie']].apply(lambda x: str(x[0]) + ' ' + str(x[1]) if pd.isna(x[1]) == False else str(x[0]), axis=1)


for s in [ave, herps]:
    for i in list(s):
        s[i] = s[i].apply(lambda x: x.strip() if type(x) is str else x)
        s[i].replace('', np.nan, inplace=True)
    for i in ['ordem', 'familia', 'genero', 'genero_especie']:
        s[i] = s[i].apply(lambda x: x.capitalize() if type(x) is str else x)
    s[['latitude', 'longitude']] = s[['latitude', 'longitude']].round(3)


# Set NaNs to None, check if this is necessary because it is messing with the numbers

# fish = fish.astype(object).where(pd.notnull(fish), None)
# herps = herps.astype(object).where(pd.notnull(herps), None)
# ave = ave.astype(object).where(pd.notnull(ave), None)


# Standardize spreadsheets to have all necessary fields
# This is a temporary step until all attributes are properly corrected and inserted

herps['pais'] = 'Brasil'
herps['genero_especie'] = herps['genero_especie'].apply(lambda x: x.replace('  ',' ') if type(x) == str else x)

ave['municipio'] = None

## Replace collector's name in herpetology spreadsheet

# Import dictionary

herps_dict = pd.read_excel('./spreadsheets/dictionaries/coletores_herpeto_dict.xlsx')

# make df into dictionary
herps_dict = herps_dict.set_index('nome_original')['nome_final'].to_dict()
herps['nome_coletor'] = herps['nome_coletor'].replace(herps_dict,regex=True).str.strip()

# Spaces were showing as unicode \xa0, this will clean them up
herps['nome_coletor'] = herps['nome_coletor'].apply(lambda x: ' '.join(x.split()) if type(x) is str else x)


#Merge the three dataframes

data = pd.concat([ave,herps],keys=['ave','herps'])

# teste_filter = data_concat.filter(['num_amostra','num_voucher','num_campo','nome_coletor','ordem','familia','genero_especie','estado','localidade','observacao','data_coleta'])

