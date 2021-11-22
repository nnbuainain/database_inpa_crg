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

    # Get foreign key value from
    data_genus = filter_data_genus(data)
    data_merge = pd.merge(data_species, data_genus, on='GÊNERO').drop(['id_familia', 'GÊNERO'], axis=1)

    return data_merge