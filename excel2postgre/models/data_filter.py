import pandas as pd
import numpy as np

def filter_data_country(data):
    data_country = data.filter(['pais'], axis=1).dropna().sort_values('pais').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_country.index = data_country.index + 1

    # Turn index into a column with new column name
    data_country = data_country.reset_index().rename(columns={'index': 'id_pais'})

    return data_country

def filter_data_state(data):
    data_state = data.filter(['pais', 'estado'], axis=1).dropna(subset=['estado']).sort_values(
        'estado').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_state.index = data_state.index + 1

    # Turn index into a column
    data_state = data_state.reset_index().rename(columns={'index': 'id_estado'})

    # Get foreign key value from COUNTRY
    data_country = filter_data_country(data)
    data_merge = pd.merge(data_state, data_country, on='pais').drop('pais', axis=1)

    return data_merge


def filter_data_locality(data):
    data_locality = data.filter(['estado', 'localidade', 'latitude', 'longitude'], axis=1).dropna(
        subset=['localidade']).sort_values('localidade').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_locality.index = data_locality.index + 1

    # Turn index into a column
    data_locality = data_locality.reset_index().rename(columns={'index': 'id_localidade'})

    # Get foreign key value from STATE
    data_state = filter_data_state(data)
    data_merge = pd.merge(data_locality, data_state, on='estado').drop(['estado', 'id_pais'], axis=1)
    return data_merge


def filter_data_order(data):
    data_order = data.filter(['ordem'], axis=1).dropna().sort_values('ordem').drop_duplicates().reset_index(drop=True)

    # Standardize order writing and remove spaces
    data_order['ordem'] = data_order['ordem'].apply(lambda x: x.capitalize().strip())

    # Reset index to start from 1
    data_order.index = data_order.index + 1

    # Turn index into a column with new column name
    data_order = data_order.reset_index().rename(columns={'index': 'id_ordem'})

    return data_order


def filter_data_family(data):
    data_family = data.filter(['ordem', 'familia'], axis=1).dropna(subset=['familia']).sort_values(
        'familia').drop_duplicates(subset=['familia'], keep='first').reset_index(drop=True)

    # Standardize family writing and remove spaces
    data_family['familia'] = data_family['familia'].apply(lambda x: x.capitalize().strip()).drop_duplicates()

    # Reset index to start from 1
    data_family.index = data_family.index + 1

    # Turn index into a column
    data_family = data_family.reset_index().rename(columns={'index': 'id_familia'})

    # Get foreign key value from ORDER
    data_order = filter_data_order(data)
    data_merge = pd.merge(data_family, data_order, on='ordem').drop('ordem', axis=1)

    return data_merge


def filter_data_genus(data):
    data_genus = data.filter(['familia', 'genero'], axis=1).dropna(subset=['genero']).sort_values(
        'genero').drop_duplicates().reset_index(drop=True)

    # Standardize genus and species writing and remove spaces
    ## WARNING, REMOVE THIS IF I CHOOSE TO CLEAN ALL COLUMNS BEFORE IMPORTING DATA
    data_genus['genero'] = data_genus['genero'].apply(lambda x: x.capitalize().strip())

    # Refilter data after processing
    data_genus = data_genus.filter(['familia', 'genero'], axis=1).dropna(subset=['genero']).sort_values(
        'genero').drop_duplicates(subset=['genero'], keep='first').reset_index(drop=True)


    # Reset index to start from 1
    data_genus.index = data_genus.index + 1

    # Turn index into a column
    data_genus = data_genus.reset_index().rename(columns={'index': 'id_genero'})

    # Get foreign key value from FAMILY
    data_family = filter_data_family(data)
    data_merge = pd.merge(data_genus, data_family, on='familia').drop(['id_ordem', 'familia'], axis=1).sort_values(
        by='genero')

    return data_merge


def filter_data_species(data):
    data_species = data.filter(['genero', 'genero_especie'], axis=1).dropna().sort_values('genero_especie').drop_duplicates().reset_index(drop=True)

    # Standardize genus and species writing and remove spaces
    for i in ['genero', 'genero_especie']:
        data_species[i] = data_species[i].apply(lambda x: x.capitalize().strip())

    patterns = [r'sp[.]gr[.]', r'sp[.]aff[.]', r'sp[.] grupo ', r'sp\b(.*)', r'sp\d+\b(.*)',
    r'aff[.]', r'\baff\b', r'cf[.]', r'cf/', r'\bcf\b', r'gr[.]', r'\bgr\b', r'rod[.]', r'peq[.]',
    r'x[-]', r'femea[-]', r'[-].*', r'ou.*',r',']

    data_species['genero_especie'] = data_species['genero_especie'].replace(patterns,'',regex=True)

    data_species['genero_especie'] = data_species['genero_especie'].apply(lambda x: x.split()[0].strip().capitalize() + ' ' + x.split()[1].strip().lower() if len(x.split()) > 1 else x.split()[0].strip().capitalize() + ' sp.')

    #This should be included in the sample section and not the specie
    data_species['genero_especie_obs'] = data_species.genero_especie.str.extract(
        r'(sp\b.*|sp\d+\b.*|aff[.].*|cf[.].*|gr[.].*|rod[.].*|\baff\b|\bgr\b|\bcf\b|\bcf/.*)')

    # remove duplicates after processing
    data_species = data_species.filter(['genero', 'genero_especie'], axis=1).sort_values('genero_especie').drop_duplicates(
        subset=['genero_especie'], keep='first').reset_index(drop=True)

    # Reset index to start from 1
    data_species.index = data_species.index + 1

    # Turn index into a column
    data_species = data_species.reset_index().rename(columns={'index': 'id_especie'})

    # Get foreign key value from GENUS
    data_genus = filter_data_genus(data)
    data_merge = pd.merge(data_species, data_genus, on='genero').drop(['id_familia', 'genero'], axis=1)
    data_merge = data_merge.astype(object).where(pd.notnull(data_merge), None)

    return data_merge

def filter_data_sample(data):
    data_sample = data.filter(
        ['num_amostra', 'num_voucher', 'num_campo', 'genero_especie', 'localidade', 'observacao', 'latitude',
         'longitude'], axis=1)

    data_locality = filter_data_locality(data)

    data_locality['id_localidade'] = data_locality['id_localidade'].astype(pd.Int64Dtype())

    data_sample = pd.merge(data_sample, data_locality, how='left', on=['localidade', 'latitude', 'longitude'])

    ## TREAT SPECIES NAME

    data_sample = data_sample[
        (data_sample['genero_especie'].isnull() == False) | (data_sample['localidade'].isnull() == False)]

    data_sample['genero_especie'] = data_sample['genero_especie'].apply(
        lambda x: x.capitalize().strip() if type(x) is str else x)

    # remove 'cf.', '(sp. nov.)' and 'sp' from species name
    data_sample['genero_especie'] = data_sample['genero_especie'].apply(
        lambda x: x.replace(' (sp. nov.)', '') if type(x) is str else x)
    data_sample['genero_especie'] = data_sample['genero_especie'].apply(
        lambda x: x.replace(' cf.', '') if type(x) is str else x)

    # Eliminate trinominal nomenclature (subspecies) and set sp. to unidentified species
    # TRY TO REFACTOR THIS!
    def del_subspecies(name):
        if type(name) is not str:
            return name
        elif len(name.split()) == 1:
            return name + ' sp.'
        elif len(name.split()) > 1:
            return name.split()[0] + ' ' + name.split()[1].lower()
        else:
            return name

    data_sample['genero_especie'] = data_sample['genero_especie'].apply(del_subspecies)

    data_species = filter_data_species(data)
    data_species['id_especie'] = data_species['id_especie'].astype(pd.Int64Dtype())

    data_merge = pd.merge(data_sample, data_species, how='left', on=['genero_especie'])

    data_merge = data_merge.filter(
        ['num_amostra', 'num_campo', 'num_voucher', 'observacao', 'id_localidade', 'id_especie'], axis=1)

    # Replace empty spaces and NaN by None Type
    data_merge['num_campo'] = data_merge['num_campo'].apply(lambda x: x.strip() if type(x) is str else x)
    data_merge['num_campo'] = data_merge['num_campo'].replace('', np.nan)
    data_merge['num_campo'].replace(r'^\s+$', np.nan, regex=True, inplace=True)
    data_merge = data_merge.astype(object).where(pd.notnull(data_merge), None)

    return data_merge


def filter_data_researcher(data):
    # Split collectors
    preps_aves = data['nome_preparador'].str.split('/|,|;|&| e ', expand=True)
    collects_aves = data['nome_coletor_especime'].str.split('/|,|;|&| e ', expand=True)

    # add collectors of herps, fish and researcher from the loan spreadsheet below

    # Combine several columns of splitted collector into single column, strip and make unique
    frames = [preps_aves, collects_aves]
    all_researchers = pd.DataFrame(
        {'Nome_pesquisador': pd.concat(frames).stack().apply(lambda x: x.strip()).sort_values().unique(), })

    # Split first and last name
    all_researchers[['nome', 'sobrenome']] = all_researchers["Nome_pesquisador"].str.split(" ", n=1, expand=True)

    # Filter desired columns
    data_researcher = all_researchers.filter(['nome', 'sobrenome'], axis=1).reset_index(drop=True)

    # Reset index to start from 1
    data_researcher.index = data_researcher.index + 1

    # Turn index into a column with new column name
    data_researcher = data_researcher.reset_index().rename(columns={'index': 'id_pesquisador'})

    # add NaN for email and institution, figure out the best way to retrieve this info later
    data_researcher[['email', 'instituicao']] = None

    # Figure out how to deal with exceptions that are not names such as "doado por fulano"

    return data_researcher


def filter_data_ave(data):
    # Filter columns
    data_ave = data.filter(['num_amostra', 'sexo', 'expedicao', 'tempo_ate_conservar', 'metodo_coleta',
                            'meio_preserv_def', 'data_preparacao', 'musculo', 'sangue', 'figado', 'coracao',
                            'genero_especie', 'localidade', 'Sigla prep', 'Nº prepa'], axis=1)

    # GET SUBSPECIES
    data_ave = data_ave[(data_ave['genero_especie'].isnull() == False) | (data_ave['localidade'].isnull() == False)]
    data_ave['genero_especie'] = data_ave['genero_especie'].apply(
        lambda x: x.capitalize().strip() if type(x) is str else x)
    data_ave['genero_especie'] = data_ave['genero_especie'].apply(
        lambda x: x.replace(' (sp. nov.)', '') if type(x) is str else x)
    data_ave['genero_especie'] = data_ave['genero_especie'].apply(
        lambda x: x.replace(' cf.', '') if type(x) is str else x)

    def del_subspecies(name):
        if name is not float and len(name.split()) > 2:
            return name.split()[2]

    data_ave['subespecie'] = data_ave['genero_especie'].apply(
        lambda x: x.split()[2] if type(x) is str and len(x.split()) > 2 else None)

    # GET NUM_PREPARADOR AINDA NÃO TESTADO!
    data_ave['num_preparador'] = data_ave[['Sigla prep', 'Nº prepa']].apply(
        lambda x: str(x[0]) + ' ' + str(x[1]) if pd.isna(x[1]) == False else x[0], axis=1)

    # Convert sample type in boolean

    for i in ['musculo', 'sangue', 'figado', 'coracao']:
        data_ave[i] = data_ave[i].apply(lambda x: True if pd.isna(x) is False else False)

    data_ave = data_ave.drop(['localidade', 'genero_especie', 'Sigla prep', 'Nº prepa'], axis=1)

    data_ave = data_ave.astype(object).where(pd.notnull(data_ave), None)

    return data_ave


def filter_researcher_ave(data):
    data_researcher = filter_data_researcher(data)
    data_researcher['nome_completo'] = data_researcher[['nome', 'sobrenome']].apply(
        lambda x: x[0] + ' ' + x[1] if pd.isna(x[1]) == False else x[0], axis=1)

    data_samples = data.filter(['num_amostra', 'nome_coletor_especime', 'genero_especie', 'localidade'], axis=1)
    data_samples = data_samples[
        (data_samples['genero_especie'].isnull() == False) | (data_samples['localidade'].isnull() == False)]
    data_samples['nome_coletor_especime'] = data['nome_coletor_especime'].str.split('/|,|;|&| e ')
    data_samples = data_samples.explode('nome_coletor_especime')
    data_samples['nome_coletor_especime'] = data_samples['nome_coletor_especime'].str.strip()

    data_researcher_ave = \
    data_researcher.merge(data_samples, right_on='nome_coletor_especime', left_on='nome_completo')[
        ['id_pesquisador', 'num_amostra']].sort_values(by='num_amostra').reset_index(drop=True)

    data_researcher_ave.index = data_researcher_ave.index + 1
    data_researcher_ave = data_researcher_ave.reset_index().rename(
        columns={'index': 'id_pesq_ave', 'num_amostra': 'num_amostra'})

    return data_researcher_ave


def filter_collector(data):
    data_researcher = filter_data_researcher(data)
    data_researcher['nome_completo'] = data_researcher[['nome', 'sobrenome']].apply(
        lambda x: x[0] + ' ' + x[1] if pd.isna(x[1]) == False else x[0], axis=1)

    data_samples = data.filter(['num_amostra', 'nome_preparador', 'DATA COLETA', 'genero_especie', 'localidade'],
                               axis=1)

    data_samples = data_samples[
        (data_samples['genero_especie'].isnull() == False) | (data_samples['localidade'].isnull() == False)]
    data_samples['nome_preparador'] = data['nome_preparador'].str.split('/|,|;|&| e ')
    data_samples = data_samples.explode('nome_preparador')
    data_samples['nome_preparador'] = data_samples['nome_preparador'].str.strip()

    data_collector = data_researcher.merge(data_samples, right_on='nome_preparador', left_on='nome_completo')[
        ['DATA COLETA', 'num_amostra', 'id_pesquisador']].sort_values(by='num_amostra').reset_index(drop=True)

    data_collector.index = data_collector.index + 1
    data_collector = data_collector.reset_index().rename(
        columns={'index': 'id_coleta', 'num_amostra': 'num_amostra', 'DATA COLETA': 'data_coleta'})

    # WARNING!! Check out what is being done when day, month or year is missing
    data_collector['data_coleta'] = pd.to_datetime(data_collector['data_coleta'], format='%Y-%m-%d', errors='coerce')
    data_collector = data_collector.astype(object).where(pd.notnull(data_collector), None)

    return data_collector


####### AFAZERES

# Modificar SQL da base de dados de peixe para incluir as demais colunas que ficaram faltando
# Verificar todos os campos que estão faltando em comum por exemplo pais e municipio



### CONCATENATE ALL DATABASES
### IMPORT three databases, change column names, merge see what happens

# Load datasets
# ave = pd.read_excel('./spreadsheets/INPA_AVES Tecidos_Dezembro2021.xlsx')
# herps = pd.read_excel('./spreadsheets/BDHerpeto atualizado 28.12.2021.xlsx')
# fish = pd.read_excel('./spreadsheets/Banco de dados de Peixes_2021_05_31.xlsx',header=1)

#
# peixe['genero_especie'] = peixe[['genero', 'especie']].apply(lambda x: str(x[0]) + ' ' + str(x[1]) if pd.isna(x[1]) == False else str(x[0]), axis=1)

## Rename columns
# ave = ave.drop(['LAT G', 'LAT M', 'LAT S', 'LAT_N/S', 'LON G', 'LON M', 'LON S', 'LON_E/W'],axis=1).rename(columns={'No TEC':'num_amostra', 'numero':'so_numero_amostra','Sigla prep':'sigla_preparador','Nº prepa':'numero_preparador', 'Nome preparador':'nome_preparador', 'Sigla campo':'sigla_num_campo', 'Nº campo':'so_numero_campo', 'ORDEM':'ordem', 'FAMILIA':'familia', 'GÊNERO':'genero', 'ESPÉCIE':'especie', 'GÊNERO ESPÉCIE':'genero_especie', 'SEXO':'sexo', 'EXPEDIÇÃO':'expedicao', 'PAÍS':'pais', 'EST':'estado', 'LOCALIDADE':'localidade', 'LAT_DEC':'latitude', 'LON_DEC':'longitude', 'TEMPO ATÉ CONSERVAR':'tempo_ate_conservar', 'MÉTODO DE COLETA':'metodo_coleta', 'MÚSCULO':'musculo', 'CORAÇÃO':'coracao', 'FÍGADO':'figado', 'SANGUE':'sangue', 'MEIO PRESERV. DEF.':'meio_preserv_def', 'DATA COLETA':'data_coleta', 'DATA PREP.':'data_preparacao', 'OBSERVAÇÕES':'observacao', 'EMPRESTADO':'emprestado', 'DATA':'data_emprestimo_1', 'GUIA N°':'num_guia_emprestimo_1', 'PARA':'pessoa_emprestimo_1', 'Obs':'obs_emprestimo_1', 'EMPRESTADO.1':'emprestado_2', 'DATA.1':'data_emprestimo_2', 'GUIA N°.1':'num_guia_emprestimo_2', 'PARA.1':'pessoa_emprestimo_2', 'EMPRESTADO.2':'emprestado_3', 'DATA.2':'data_emprestimo_3', 'GUIA N°.2':'num_guia_emprestimo_3', 'PARA.2':'pessoa_emprestimo_3', 'EMPRESTADO.3':'emprestado_4', 'DATA.3':'data_emprestimo_4', 'GUIA N°.3':'num_guia_emprestimo_4', 'PARA.3':'pessoa_emprestimo_4'})
# herps = herps.rename(columns={'INPA - HT':'num_amostra', 'INPA-H':'num_voucher', 'No. CAMPO':'num_campo', 'DATA':'data_coleta', 'ORDEM':'ordem', 'FAMÍLIA':'familia', 'GÊNERO':'genero','ESPÉCIE':'genero_especie', 'LOCALIDADE':'localidade','MUNICÍPIO':'municipio', 'ESTADO':'estado', 'LAT_DEC':'latitude', 'LONG_DEC':'longitude', 'COLETOR':'nome_coletor', 'OBSERVAÇÕES':'observacao'})
# fish  = fish.drop(['Unnamed: 3'],axis=1).rename(columns={'STATUS':'status', 'CAIXA':'num_caixa', 'Número de DNA':'num_amostra', 'Número de CAMPO':'num_campo', 'VOUCHER':'num_voucher', 'ORDEM':'ordem', 'FAMÍLIA':'familia', 'GÊNERO':'genero', 'ESPÉCIE':'especie', 'NOME COMUM':'nome_comum', 'DATA DE COLETA':'data_coleta', 'País':'pais', 'ESTADO':'estado', 'MUNICÍPIO':'municipio', 'DRENAGEM':'drenagem', 'SUBDRENAGEM':'subdrenagem', 'LOCAL DE PESCA':'localidade_pesca', 'LOCAL DE COLETA':'localidade', 'LATITUDE':'latitude', 'LONGITUDE':'longitude', 'COLETORES':'nome_coletor', 'RESPONSÁVEL/PROJETOS':'responsavel_projetos', 'Comprimento Padrão (cm)':'comprimento_padrao', 'OBSERVAÇÕES':'observacao'})

# Set NaNs to None
# fish = fish.astype(object).where(pd.notnull(fish), None)
# herps = herps.astype(object).where(pd.notnull(herps), None)
# ave = ave.astype(object).where(pd.notnull(ave), None)


## Standardize spreadsheets to have all necessary fields
## This is a temporary step until all attributes are properly corrected and inserted

# herps['pais'] = 'Brasil'
# herps['genero_especie'] = herps['genero_especie'].apply(lambda x: x.replace('  ',' ') if type(x) == str else x)
# for i in ['ordem', 'familia', 'genero', 'genero_especie']:
#     herps[i] = herps[i].apply(lambda x: x.capitalize().strip() if type(x) is str else x)





# data_concat = pd.concat([ave,herps,fish],keys=['ave','herps','peixe'])
# teste_filter = data_concat.filter(['num_amostra','num_voucher','num_campo','nome_coletor','ordem','familia','genero_especie','estado','localidade','observacao','data_coleta'])


