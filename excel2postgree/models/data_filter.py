'''
    input -> dataframe with all data from excel file.
    return -> dataframe with data to be inserted into corresponding table.
        i.e:
               index   ordem
            0      1  ordem1
            1      2  ordem2
            2      3  ordem3
            3      4  ordem4
            4      5  ordem5

'''

def filter_data_order(data):
    data_order = data.filter(['ordem'], axis=1).sort_values('ordem').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_order.index = data_order.index + 1
    # Turn index into a column
    data_order.reset_index(level=0, inplace=True)

    return data_order

def filter_data_family(data):
    #UNDER CONSTRUCTION...
    data_family = data
    
    return data_family