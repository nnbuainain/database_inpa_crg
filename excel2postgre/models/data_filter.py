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
    data_order['ordem'] = data_order['ordem'].apply(lambda x: x.capitalize().strip()).drop_duplicates().dropna()


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
    r'x[-]', r'\W+femea.*', r'[-].*', r'ou.*',r',',r'[?]',r'\*',r'\Wjoelho.*',r'\Wpapo\b.*']

    data_species['genero_especie'] = data_species['genero_especie'].replace(patterns,'',regex=True)

    data_species['genero_especie'] = data_species['genero_especie'].apply(lambda x: x.split()[0].strip().capitalize() + ' ' + x.split()[1].strip().lower() if len(x.split()) > 1 else x.split()[0].strip().capitalize() + ' sp.')

    # remove duplicates after processing
    data_species = data_species.filter(['genero', 'genero_especie'], axis=1).sort_values('genero_especie').drop_duplicates(
        subset=['genero_especie'], keep='first').reset_index(drop=True)

    # Reset index to start from 1
    data_species.index = data_species.index + 1

    # Turn index into a column
    data_species = data_species.reset_index().rename(columns={'index': 'id_especie'})

    # Get foreign key value from GENUS
    data_genus = filter_data_genus(data)
    data_merge = pd.merge(data_species, data_genus,how='left', on='genero').drop(['id_familia', 'genero'], axis=1)
    data_merge = data_merge.astype(object).where(pd.notnull(data_merge), None)

    return data_merge

def filter_data_sample(data):
    data_sample = data.filter(['num_amostra', 'num_voucher', 'num_campo', 'genero_especie',
                               'localidade', 'observacao', 'latitude','longitude'], axis=1)

    # Drop samples with no infor for species and locality which are blanck spaces in the spreadsheet
    data_sample = data_sample[(data_sample['genero_especie'].isnull() == False) | (data_sample['localidade'].isnull() == False)]

    data_locality = filter_data_locality(data)

    # data_locality['id_localidade'] = data_locality['id_localidade'].astype(pd.Int64Dtype())

    ## Muito cuidade aqui, atente-se ao numero de linhas do df antes e depois do merge
    data_sample = pd.merge(data_sample, data_locality, how='left', on=['localidade', 'latitude', 'longitude'])

    ## TREAT SPECIES NAME
    patterns = [r'sp[.]gr[.]', r'sp[.]aff[.]', r'sp[.] grupo ', r'sp\b(.*)', r'sp\d+\b(.*)',
    r'aff[.]', r'\baff\b', r'cf[.]', r'cf/', r'\bcf\b', r'gr[.]', r'\bgr\b', r'rod[.]', r'peq[.]',
    r'x[-]', r'\W+femea.*', r'[-].*', r'ou.*',r',',r'[?]',r'\*',r'\Wjoelho.*',r'\Wpapo\b.*']

    data_sample['genero_especie'] = data_sample['genero_especie'].replace(patterns,'',regex=True)

    data_sample['genero_especie'] = data_sample['genero_especie'].replace('', None, regex=True)

    data_sample['genero_especie'] = data_sample['genero_especie'].apply(lambda x: x if type(x) != str else x.split()[0].strip().capitalize() + ' ' + x.split()[1].strip().lower() if len(x.split()) > 1 else x.split()[0].strip().capitalize() + ' sp.')

    # get observations about samples not fully identified
    # Adicionar regex papo, joelho, [?], conferir as de femeas
    data_sample['genero_especie_obs'] = data_sample.genero_especie.str.extract(
        r'(sp\b.*|sp\d+\b.*|aff[.].*|cf[.].*|gr[.].*|rod[.].*|\baff\b|\bgr\b|\bcf\b|\bcf/.*|\Wpapo\b.*|\Wjoelho.*|[?]|\W+femea.*|x[-]|ou.*)')

    data_species = filter_data_species(data)

    data_species['id_especie'] = data_species['id_especie'].astype(pd.Int64Dtype())

    data_merge = pd.merge(data_sample, data_species, how='left', on=['genero_especie'])

    data_merge = data_merge.filter(
        ['num_amostra', 'num_campo', 'num_voucher', 'observacao', 'id_localidade', 'id_especie','genero_especie_obs'], axis=1)

    # Delete this if this cleaning is done during importing of spreadsheet
    data_merge['num_campo'] = data_merge['num_campo'].apply(lambda x: x.strip() if type(x) is str else x)

    # Replace empty spaces and NaN by None Type
    data_merge['num_campo'] = data_merge['num_campo'].replace('', np.nan)
    data_merge['num_campo'].replace(r'^\s+$', np.nan, regex=True, inplace=True)
    data_merge = data_merge.astype(object).where(pd.notnull(data_merge), None)

    return data_merge


def filter_data_researcher(data):
    # Split collectors
    collector = data['nome_coletor'].str.split('/|,|;|&| e ', expand=True)
    collector_voucher_aves = data['nome_coletor_especime'].str.split('/|,|;|&| e ', expand=True)

    # add collectors of herps, fish and researcher from the loan spreadsheet below

    # Combine several columns of splitted collector into single column, strip and make unique
    frames = [collector, collector_voucher_aves]

    all_researchers_stacked = pd.DataFrame(
        {'nome_pesquisador': pd.concat(frames).stack().apply(lambda x: x.strip()).sort_values().unique(), })
    all_researchers_stacked['nome_pesquisador'].replace('',np.nan,inplace=True)
    all_researchers_stacked = all_researchers_stacked.dropna()

    # Split first and last name
    all_researchers_stacked[['nome_pesquisador', 'sobrenome_pesquisador']] = all_researchers_stacked["nome_pesquisador"].str.split(" ", n=1, expand=True)

    # Filter desired columns
    data_researcher = all_researchers_stacked.reset_index(drop=True)

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
    data_researcher['nome_completo_pesquisador'] = data_researcher[['nome_pesquisador', 'sobrenome_pesquisador']].apply(
        lambda x: x[0] + ' ' + x[1] if pd.isna(x[1]) == False else x[0], axis=1)

    data_sample = data.filter(['num_amostra', 'nome_coletor_especime', 'genero_especie', 'localidade'], axis=1)
    data_sample = data_sample[
        (data_sample['genero_especie'].isnull() == False) | (data_sample['localidade'].isnull() == False)]
    data_sample['nome_coletor_especime'] = data['nome_coletor_especime'].str.split('/|,|;|&| e ')
    data_sample = data_sample.explode('nome_coletor_especime')
    data_sample['nome_coletor_especime'] = data_sample['nome_coletor_especime'].str.strip()

    data_researcher_ave = data_researcher.merge(data_sample, right_on='nome_coletor_especime', left_on='nome_completo_pesquisador')[
        ['id_pesquisador', 'num_amostra']].sort_values(by='num_amostra').reset_index(drop=True)

    data_researcher_ave.index = data_researcher_ave.index + 1
    data_researcher_ave = data_researcher_ave.reset_index().rename(
        columns={'index': 'id_pesq_ave', 'num_amostra': 'num_amostra'})

    return data_researcher_ave


def filter_collector(data):
    data_researcher = filter_data_researcher(data)
    data_researcher['nome_completo'] = data_researcher[['nome_pesquisador', 'sobrenome_pesquisador']].apply(
        lambda x: x[0] + ' ' + x[1] if pd.isna(x[1]) == False else x[0], axis=1)

    data_sample = data.filter(['num_amostra', 'nome_coletor', 'data_coleta', 'genero_especie', 'localidade'],
                               axis=1)

    data_sample = data_sample[(data_sample['genero_especie'].isnull() == False) | (data_sample['localidade'].isnull() == False)]
    data_sample['nome_coletor'] = data['nome_coletor'].str.split('/|,|;|&| e ')
    data_sample = data_sample.explode('nome_coletor')
    data_sample['nome_coletor'] = data_sample['nome_coletor'].str.strip()

    data_collector = data_researcher.merge(data_sample, right_on='nome_coletor', left_on='nome_completo')[
        ['data_coleta', 'num_amostra', 'id_pesquisador']].sort_values(by='num_amostra').reset_index(drop=True)

    data_collector.index = data_collector.index + 1
    data_collector = data_collector.reset_index().rename(
        columns={'index': 'id_coleta', 'num_amostra': 'num_amostra', 'data_coleta': 'data_coleta'})

    # WARNING!! Check out what is being done when day, month or year is missing
    data_collector['data_coleta'] = pd.to_datetime(data_collector['data_coleta'], format='%Y-%m-%d', errors='coerce')
    data_collector = data_collector.astype(object).where(pd.notnull(data_collector), None)

    return data_collector


####### AFAZERES

# Modificar SQL da base de dados de peixe para incluir as demais colunas que ficaram faltando
# Verificar todos os campos que estão faltando em comum por exemplo pais e municipio
