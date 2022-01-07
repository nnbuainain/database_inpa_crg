import pandas as pd
import numpy as np
import re

# Import herps and clean
herps = pd.read_excel('./spreadsheets/BDHerpeto atualizado 28.12.2021.xlsx')
herps = herps.rename(columns={'INPA - HT':'num_amostra', 'INPA-H':'num_voucher', 'No. CAMPO':'num_campo', 'DATA':'data_coleta', 'ORDEM':'ordem', 'FAMÍLIA':'familia', 'GÊNERO':'genero','ESPÉCIE':'genero_especie', 'LOCALIDADE':'localidade','MUNICÍPIO':'municipio', 'ESTADO':'estado', 'LAT_DEC':'latitude', 'LONG_DEC':'longitude', 'COLETOR':'nome_coletor', 'OBSERVAÇÕES':'observacao'})

for i in list(herps):
    herps[i] = herps[i].apply(lambda x: x.strip() if type(x) is str else x)

herps = herps.astype(object).where(pd.notnull(herps), None)

herps['pais'] = 'Brasil'
herps['genero_especie'] = herps['genero_especie'].apply(lambda x: x.replace('  ',' ') if type(x) == str else x)

for i in list(herps):
    herps[i] = herps[i].apply(lambda x: x.strip() if type(x) is str else x)
for i in ['ordem', 'familia', 'genero', 'genero_especie']:
    herps[i] = herps[i].apply(lambda x: x.capitalize() if type(x) is str else x)

## reduced for testing

herps_dict = pd.read_excel('./spreadsheets/dictionaries/coletores_herpeto_dict.xlsx')
herps_dict = herps_dict.set_index('nome_original')['nome_final'].to_dict()
herps_reduced = herps.filter(['nome_coletor']).drop_duplicates()

herps_reduced['nome_coletor_clean'] = herps_reduced['nome_coletor'].replace(herps_dict,regex=True).str.strip()
herps_reduced['nome_coletor_clean'] = herps_reduced['nome_coletor_clean'].apply(lambda x: ' '.join(x.split()) if type(x) is str else x)
herps_to_stack = herps_reduced['nome_coletor_clean']
herps_to_stack = herps_to_stack.str.split('/|,|;|&| e ', expand=True)
all_researchers = pd.DataFrame(
    {'Nome_pesquisador': herps_to_stack.stack().apply(lambda x: x.strip()).sort_values().unique()})

# Real deal

herps_dict = pd.read_excel('./spreadsheets/dictionaries/coletores_herpeto_dict.xlsx')
herps_dict = herps_dict.set_index('nome_original')['nome_final'].to_dict()

herps['nome_coletor'] = herps['nome_coletor'].replace(herps_dict,regex=True).str.strip()
herps['nome_coletor'] = herps_reduced['nome_coletor'].apply(lambda x: ' '.join(x.split()) if type(x) is str else x)

herps_to_stack = herps_reduced['nome_coletor_clean']
herps_to_stack = herps_to_stack.str.split('/|,|;|&| e ', expand=True)
all_researchers = pd.DataFrame(
    {'Nome_pesquisador': herps_to_stack.stack().apply(lambda x: x.strip()).sort_values().unique()})


# all_researchers = pd.DataFrame(
#     {'Nome_pesquisador': teste.stack().apply(lambda x: x.strip()).sort_values().unique()})
# all_researchers['Nome_pesquisador'] = all_researchers['Nome_pesquisador'].apply(lambda x: ' '.join(x.split()))



#original
herps_dict = pd.read_excel('./spreadsheets/dictionaries/coletores_herpeto_dict.xlsx')
herps_dict = herps_dict.set_index('nome_original')['nome_final'].to_dict()
teste = herps['nome_coletor'].replace(herps_dict,regex=True).str.strip()
teste = teste.str.split('/|,|;|&| e ', expand=True)
all_researchers = pd.DataFrame(
    {'Nome_pesquisador': teste.stack().apply(lambda x: x.strip()).sort_values().unique()})
all_researchers['Nome_pesquisador'] = all_researchers['Nome_pesquisador'].apply(lambda x: ' '.join(x.split()))