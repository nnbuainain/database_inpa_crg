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
    # Turn index into a column
    data_order.reset_index(level=0, inplace=True)

    return data_order

def filter_data_family(data):
    data_family = data.filter(['ORDEM', 'FAMILIA'], axis=1).dropna(subset=['FAMILIA']).sort_values('FAMILIA').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_family.index = data_family.index + 1
    # Turn index into a column
    data_family.reset_index(level=0, inplace=True)

    # Get foreign key value from ORDER
    data_order = filter_data_order(data)
    data_merge = pd.merge(data_family, data_order, on='ORDEM').drop('ORDEM', axis=1)

    return data_merge

def filter_data_gender(data):
    data_gender = data.filter(['FAMILIA', 'GÊNERO'], axis=1).dropna(subset=['GÊNERO']).sort_values('GÊNERO').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_gender.index = data_gender.index + 1
    # Turn index into a column
    data_gender.reset_index(level=0, inplace=True)

    # Get foreign key value from FAMILY
    data_family = filter_data_family(data)
    data_merge = pd.merge(data_gender, data_family, on='FAMILIA').drop(['index_y', 'FAMILIA'], axis=1)

    return data_merge