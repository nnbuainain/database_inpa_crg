'''
    input -> dataframe with all data from excel file.
    return -> dataframe with data to be inserted into corresponding table.
        i.e:
               index   ordem
            0      1  ordem1
            1      2  ordem2
            2      3  ordem3
'''
import pandas as pd

def filter_data_order(data):
    data_order = data.filter(['ORDEM'], axis=1).dropna().sort_values('ORDEM').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_order.index = data_order.index + 1

    # Turn index into a column with new column name
    data_order = data_order.reset_index().rename(columns={'index':'id_ordem'})

    return data_order

def filter_data_family(data):
    data_family = data.filter(['ORDEM', 'FAMILIA'], axis=1).dropna(subset=['FAMILIA']).sort_values('FAMILIA').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_family.index = data_family.index + 1

    # Turn index into a column
    data_family = data_family.reset_index().rename(columns={'index':'id_familia'})

    # Get foreign key value from ORDER
    data_order = filter_data_order(data)
    data_merge = pd.merge(data_family, data_order, on='ORDEM').drop('ORDEM', axis=1)

    return data_merge

def filter_data_genus(data):
    data_genus = data.filter(['FAMILIA', 'GÊNERO'], axis=1).dropna(subset=['GÊNERO']).sort_values('GÊNERO').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_genus.index = data_genus.index + 1

    # Turn index into a column
    data_genus = data_genus.reset_index().rename(columns={'index': 'id_genero'})

    # Get foreign key value from FAMILY
    data_family = filter_data_family(data)
    data_merge = pd.merge(data_genus, data_family, on='FAMILIA').drop(['id_ordem', 'FAMILIA'], axis=1)

    return data_merge

def filter_data_species(data):
    data_species = data.filter(['GÊNERO', 'GÊNERO ESPÉCIE'], axis=1).dropna(subset=['GÊNERO ESPÉCIE']).sort_values('GÊNERO ESPÉCIE').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_species.index = data_species.index + 1

    # Turn index into a column
    data_species = data_species.reset_index().rename(columns={'index': 'id_species'})

    # Get foreign key value from GENUS
    data_genus = filter_data_genus(data)
    data_merge = pd.merge(data_species, data_genus, on='GÊNERO').drop(['id_familia', 'GÊNERO'], axis=1)

    return data_merge

def filter_data_country(data):
    data_country = data.filter(['PAÍS'], axis=1).dropna().sort_values('PAÍS').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_country.index = data_country.index + 1

    # Turn index into a column with new column name
    data_country = data_country.reset_index().rename(columns={'index':'id_pais'})

    return data_country

def filter_data_state(data):
    data_state = data.filter(['PAÍS', 'EST'], axis=1).dropna(subset=['EST']).sort_values('EST').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_state.index = data_state.index + 1

    # Turn index into a column
    data_state = data_state.reset_index().rename(columns={'index':'id_estado'})

    # Get foreign key value from COUNTRY
    data_country = filter_data_country(data)
    data_merge = pd.merge(data_state, data_country, on='PAÍS').drop('PAÍS', axis=1)

    return data_merge

def filter_data_locality(data):
    data_locality = data.filter(['EST', 'LOCALIDADE','LAT_DEC','LON_DEC'], axis=1).dropna(subset=['LOCALIDADE']).sort_values('LOCALIDADE').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_locality.index = data_locality.index + 1

    # Turn index into a column
    data_locality = data_locality.reset_index().rename(columns={'index':'id_localidade'})

    # Get foreign key value from STATE
    data_state = filter_data_state(data)
    data_merge = pd.merge(data_locality, data_state, on='EST').drop(['EST','id_pais'], axis=1)

    return data_merge