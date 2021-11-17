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
    data_order = data.filter(['ordem'], axis=1).sort_values('ordem').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_order.index = data_order.index + 1
    # Turn index into a column
    data_order.reset_index(level=0, inplace=True)

    return data_order

def filter_data_family(data):
    data_family = data.filter(['ordem', 'familia'], axis=1).sort_values('familia').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_family.index = data_family.index + 1
    # Turn index into a column
    data_family.reset_index(level=0, inplace=True)

    # Get foreign key value from ORDER
    data_order = filter_data_order(data)
    data_merge = pd.merge(data_family, data_order, on='ordem').drop('ordem', axis=1)

    return data_merge

def filter_data_gender(data):
    data_gender = data.filter(['familia', 'genero'], axis=1).sort_values('genero').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_gender.index = data_gender.index + 1
    # Turn index into a column
    data_gender.reset_index(level=0, inplace=True)

    # Get foreign key value from FAMILY
    data_family = filter_data_family(data)
    data_merge = pd.merge(data_gender, data_family, on='familia').drop(['index_y', 'familia'], axis=1)

    return data_merge