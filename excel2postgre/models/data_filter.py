import pandas as pd
import numpy as np

def filter_data_country(data):
    """Extract an indexed list of all countries in the DataFrame
    in order to create a Table 'PAIS' to export to the Database

    State indexes automatically start from 1

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'pais': Country's name -> str

    Returns
    -------
    data_country: A pandas DataFrame with processed data and
    the columns necessary to create table 'PAIS'
    """
    # Filter columns of interest, drop NaNs, sort values by country, drop duplicates, reset and drop indexes
    data_country = data.filter(['pais'], axis=1).dropna().sort_values('pais').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_country.index = data_country.index + 1

    # Turn index into a column with new column named 'index'
    data_country = data_country.reset_index().rename(columns={'index': 'id_pais'})

    return data_country

def filter_data_state(data):
    """Extract an indexed list of all states in the DataFrame
    in order to create a Table 'ESTADO' to export to the Database

    State index automatically start from 1
    Country index for each state is fetched from the 'PAIS' table
    with the filter_data_country(data)

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'pais': Country's name -> str
        b) 'estado': State's name -> str

    Returns
    -------
    data_merge: A pandas DataFrame with processed data and
    the columns necessary to create table 'ESTADO'
    """

    # Filter columns of interest, drop NaNs, sort values by state, drop duplicates, reset and drop indexes
    data_state = data.filter(['pais', 'estado'], axis=1).dropna(subset=['estado']).sort_values(
        'estado').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_state.index = data_state.index + 1

    # Turn index into a column named 'index'
    data_state = data_state.reset_index().rename(columns={'index': 'id_estado'})

    # Get foreign key value from PAIS Table
    data_country = filter_data_country(data)
    data_merge = pd.merge(data_state, data_country, on='pais').drop('pais', axis=1)

    return data_merge

def filter_data_locality(data):
    """Extract an indexed list of all localities in the DataFrame
    in order to create a Table 'LOCALIDADE' to export to the Database

    Locality index automatically start from 1

    Country and State index for each locality is fetched from the 'PAIS'
    and 'ESTADO' tables with filter_data_species(data)

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'estado': State's name -> str
        b) 'localidade': Locality's name -> str
        c) 'latitude': Latitude in decimal degrees format -> float
        d) 'longitute': Longitute in decimal degrees -> foat
    Returns
    -------
    data_merge: A pandas DataFrame with processed data and
    the columns necessary to create table 'LOCALIDADE'
    """

    # Filter columns of interest, drop NaNs, sort values by locality, drop duplicates, reset and drop indexes
    data_locality = data.filter(['estado', 'localidade', 'latitude', 'longitude','coordenadas_obs'], axis=1).dropna(
        subset=['localidade']).sort_values('localidade').drop_duplicates(['localidade', 'latitude', 'longitude']).reset_index(drop=True)

    # Reset index to start from 1
    data_locality.index = data_locality.index + 1

    # Turn index into a column
    data_locality = data_locality.reset_index().rename(columns={'index': 'id_localidade'})

    # Get foreign key value from 'ESTADO' table
    data_state = filter_data_state(data)
    data_merge = pd.merge(data_locality, data_state, on='estado').drop(['estado', 'id_pais'], axis=1).sort_values(by='id_localidade')

    # Convert NaN to None
    data_merge = data_merge.astype(object).where(pd.notnull(data_merge), None)

    return data_merge

def filter_data_order(data):
    """Extract an indexed list of all taxonomic rank Order in the DataFrame
    in order to create a Table 'ORDEM' to export to the Database

    Order index automatically start from 1

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'ordem' : Order's name -> str

    Returns
    -------
    data_order: A pandas DataFrame with processed data and
    the columns necessary to create table 'ORDEM'
    """

    # Filter columns of interest, drop NaNs, sort values by order, drop duplicates, reset and drop indexes
    data_order = data.filter(['ordem'], axis=1).dropna().sort_values('ordem').drop_duplicates().reset_index(drop=True)

    # Reset index to start from 1
    data_order.index = data_order.index + 1

    # Turn index into a column with new column named index
    data_order = data_order.reset_index().rename(columns={'index': 'id_ordem'})

    return data_order

def filter_data_family(data):
    """Extract an indexed list of all taxonomic rank Family in the DataFrame
    in order to create a Table 'FAMILIA' to export to the Database

    Family index automatically start from 1

    Order index for each Family is fetched from the 'ORDEM'
    Table with filter_data_order(data)

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'ordem' : Order's name -> str
        a) 'familia' : Family's name -> str

    Returns
    -------
    data_merge: A pandas DataFrame with processed data and
    the columns necessary to create table 'FAMILIA'
    """

    # Filter columns of interest, drop NaNs, sort values by family, drop duplicates, reset and drop indexes
    # argument keep='first' helps to avoid duplicates in case the same family is erroneously attributed to two orders
    data_family = data.filter(['ordem', 'familia'], axis=1).dropna(subset=['familia']).sort_values(
        'familia').drop_duplicates(subset=['familia'], keep='first').reset_index(drop=True)

    # Reset index to start from 1
    data_family.index = data_family.index + 1

    # Turn index into a column named 'index'
    data_family = data_family.reset_index().rename(columns={'index': 'id_familia'})

    # Get 'ORDEM' table
    data_order = filter_data_order(data)

    # Merge genus and family dataframes on key 'ordem' to get Order foreign key
    # drop unnecessary columns, sort values by 'familia'
    data_merge = pd.merge(data_family, data_order, on='ordem').drop('ordem', axis=1).sort_values(by='familia')

    return data_merge

def filter_data_genus(data):
    """Extract an indexed list of all taxonomic rank Genus in the DataFrame
    in order to create a Table 'GENERO' to export to the Database

    Genus index automatically start from 1

    Family index for each Genus is fetched from the Table 'FAMILIA'
    with filter_data_family(data)

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'familia' : Family's name in str format -> str
        a) 'genero' : Genus's name in str format -> str
    Returns
    -------
    data_merge: A pandas DataFrame with processed data and
    the columns necessary to create table 'GENERO'
    """

    # Filter columns of interest, drop NaNs, sort values by genus, drop duplicates, reset and drop indexes
    # argument keep='first' helps to avoid duplicates in case the same family is erroneously attributed to two orders
    data_genus = data.filter(['familia', 'genero'], axis=1).dropna(subset=['genero'])\
        .sort_values('genero').drop_duplicates(subset=['genero'], keep='first').reset_index(drop=True)

    # Reset index to start from 1
    data_genus.index = data_genus.index + 1

    # Turn index into a column named 'index'
    data_genus = data_genus.reset_index().rename(columns={'index': 'id_genero'})

    # Get family table
    data_family = filter_data_family(data)

    # Merge family and order dataframes on key 'familia' to get Family foreign key
    # drop unnecessary columns, sort values by 'genero'
    data_merge = pd.merge(data_genus, data_family, on='familia').drop(['id_ordem', 'familia'], axis=1)\
        .sort_values(by='genero')

    return data_merge

def filter_data_species(data):
    """Extract an indexed list of all taxonomic rank Species in the DataFrame
    in order to create a Table 'ESPECIE' to export to the Database

    Species index automatically start from 1

    Genus index for each species is fetched from the GENERO Table
    with filter_data_genus()

    If species name contains other names besides the genus and specific
    epithet such as abbreviations of identification uncertainty, author's name,
    subspecies, etc... these get cleaned out.

    Abbreviations of identification uncertainty such as 'cf.','gr.','aff',
    'sp. nov.', 'sp.' are removed but will be further inserted into a new column
    'genero_especie_obs' in the table 'AMOSTRA' with filter_data_sample()

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'genero' : Genus's name -> str
        a) 'genero_especie' : Species's name (Genus + specific epithet) -> str

    Returns
    -------
    data_merge: A pandas DataFrame with processed data and
    the columns necessary to create table 'ESPECIE'
    """

    # Filter columns of interest, drop NaNs, sort values by genero_species, drop duplicates, reset and drop indexes
    # Argument keep='first' avoid duplicates in case the same genus is erroneously attributed to two orders
    data_species = data.filter(['genero', 'genero_especie'], axis=1).dropna().sort_values('genero_especie')\
        .drop_duplicates(subset=['genero_especie'], keep='first').reset_index(drop=True)

    # Get regex patterns to clean all names that are not genus and specific epithet
    patterns = [r'sp[.]gr[.]', r'sp[.]aff[.]', r'sp[.] grupo ', r'sp\b(.*)', r'sp\d+\b(.*)',
    r'aff[.]', r'\baff\b', r'cf[.]', r'cf/', r'\bcf\b', r'gr[.]', r'\bgr\b', r'rod[.]', r'peq[.]',
    r'x[-]', r'\W+femea.*', r'[-].*', r'ou.*',r',', r'[?]', r'\*', r'\Wjoelho.*', r'\Wpapo\b.*']

    # Apply regex to clean species names by removing captured patterns
    data_species['genero_especie'] = data_species['genero_especie'].replace(patterns,'',regex=True)

    # Make sure species name is composed by genus + specific epithet
    # This helps to clean out author's name after the specific epithet and clean multi spaces between names
    data_species['genero_especie'] = data_species['genero_especie'].apply(lambda x: x.split()[0].strip().capitalize() + ' ' + x.split()[1].strip().lower() if len(x.split()) > 1 else x.split()[0].strip().capitalize() + ' sp.')

    # Remove duplicates after processing names, sort by 'genero_especie', reset and drop index
    data_species = data_species.filter(['genero', 'genero_especie'], axis=1).sort_values('genero_especie').drop_duplicates(
        subset=['genero_especie'], keep='first').reset_index(drop=True)

    # Reset index to start from 1
    data_species.index = data_species.index + 1

    # Turn index into a column named 'index'
    data_species = data_species.reset_index().rename(columns={'index': 'id_especie'})

    # Read genus table in order to get foreign key
    data_genus = filter_data_genus(data)

    # Merge genus and species dataframes to get genus's foreign key for each species
    data_merge = pd.merge(data_species, data_genus, how='left', on='genero').drop(['id_familia', 'genero'], axis=1)

    # Replace nans for None
    data_merge = data_merge.astype(object).where(pd.notnull(data_merge), None)

    # Convert float to int in 'id_genero'
    data_merge['id_genero'] = data_merge['id_genero'].astype(pd.Int64Dtype())

    return data_merge

def filter_data_sample(data):
    """Extract a list of all information necessary to create
    Table 'ESPECIE' in order to export to the Database

    Locality and species foreign keys for each sample are fetched from the
    ESPECIE and LOCALIDADE tables with filter_data_species() and filter_data_locality()


    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'num_amostra' : Genetic sample' Id number -> str
        b) 'num_voucher' : Voucher number of genetic sample -> str
        c) 'num_campo' : Field number of genetic sample -> str
        d) 'genero_especie' : Species name -> str
        e) 'municipio' : Municipality where sample was colllected -> str
        f) 'localidade' : locality where sample was colllected -> str
        g) 'observacao' : observations -> str
        h) 'latitude' : latitude of sample's locality in decimal degrees format -> float
        i) 'longitude' : longitude of sample's locality in decimal degrees format -> float

    Returns
    -------
    data_merge: A pandas DataFrame with processed data and
    the columns necessary to create table 'AMOSTRA'
    """

    # Filter columns of interest
    data_sample = data.filter(['num_amostra', 'num_voucher', 'num_campo', 'genero_especie', 'municipio',
                               'localidade', 'observacao', 'latitude', 'longitude'], axis=1)

    # Drop samples with no information for species and locality which are blank spaces in the spreadsheet
    data_sample = data_sample[(data_sample['genero_especie'].isnull() == False) | (data_sample['localidade'].isnull() == False)]

    data_sample['colecao'] = data_sample.index.get_level_values(0)

    # Replace NaNs by None, this will also make latitude and longitude pd.objects
    # This is necessary to match these fields in the data_locality df and merge
    data_sample = data_sample.astype(object).where(pd.notnull(data_sample), None)

    # Get list of localities with indexes to get localities' foreign key
    data_locality = filter_data_locality(data)

    # Merge sample and locality dataframes, make sure row numbers are the same before and after
    # If not, most likely the locality is duplicated in the locality table, ex: same locality, two different states

    data_sample = pd.merge(data_sample, data_locality, how='left', on=['localidade', 'latitude', 'longitude'])

    ## Clean species name

    # Get observations about samples not fully identified (ex: aff., cf., sp., etc...) and place into a new column
    data_sample['genero_especie_obs'] = data_sample.genero_especie.str.extract(
        r'(sp\b.*|sp\d+\b.*|aff[.].*|cf[.].*|gr[.].*|rod[.].*|\baff\b|\bgr\b|\bcf\b|\bcf/.*|\Wpapo\b.*|\Wjoelho.*|[?]|\W+femea.*|x[-]|\bou.*)')

    # Remove spaces before and after in genero_especie_obs column
    data_sample['genero_especie_obs'] = data_sample['genero_especie_obs'].apply(lambda x: x.strip() if type(x) == str else x)

    # Get regex patterns to clean all names that are not genus and specific epithet
    patterns = [r'sp[.]gr[.]', r'sp[.]aff[.]', r'sp[.] grupo ', r'sp\b(.*)', r'sp\d+\b(.*)',
    r'aff[.]', r'\baff\b', r'cf[.]', r'cf/', r'\bcf\b', r'gr[.]', r'\bgr\b', r'rod[.]', r'peq[.]',
    r'x[-]', r'\W+femea.*', r'[-].*', r'\bou.*', r',', r'[?]',r'\*', r'\Wjoelho.*', r'\Wpapo\b.*']

    # Apply regex to clean species names by removing captured patterns
    data_sample['genero_especie'] = data_sample['genero_especie'].replace(patterns, '', regex=True)

    # Remove samples with blank for species name
    data_sample['genero_especie'] = data_sample['genero_especie'].replace('', None, regex=True)

    # Make sure species name is composed by genus + specific epithet
    # This helps to clean out author's name after the specific epithet and clean multi spaces between names
    data_sample['genero_especie'] = data_sample['genero_especie'].apply(lambda x: x if type(x) != str else x.split()[0].strip().capitalize() + ' ' + x.split()[1].strip().lower() if len(x.split()) > 1 else x.split()[0].strip().capitalize() + ' sp.')

    # Get list of species with indexes to get localities' foreign key
    data_species = filter_data_species(data)

    # Merge sample and species dataframes to get species foreign key
    data_merge = pd.merge(data_sample, data_species, how='left', on=['genero_especie'])

    # Keep only relevant columns
    data_merge = data_merge.filter(
        ['num_amostra', 'num_campo', 'num_voucher', 'municipio', 'observacao', 'genero_especie_obs', 'colecao', 'id_localidade', 'id_especie'], axis=1)

    # Replace empty spaces to nan in column 'num_campo'
    data_merge['num_campo'] = data_merge['num_campo'].replace('', np.nan)
    data_merge['num_campo'].replace(r'^\s+$', np.nan, regex=True, inplace=True)

    # Return id_index to integer because it was converted erroneously to float during processing
    data_merge['id_localidade'] = data_merge['id_localidade'].astype(pd.Int64Dtype())

    # Convert NaNs to None
    data_merge = data_merge.astype(object).where(pd.notnull(data_merge), None)

    return data_merge

def filter_data_researcher(data):
    """Extract a list of all researchers associated to the collections
     to create Table 'COLETOR' and export to the Database

    These researchers may be collectors of genetic or voucher specimens
    or people that requested and/or were involved (responsible) in sample requests

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'nome_coletor' : A person or list of people who collected a genetic sample -> str
        b) 'nome_coletor_especime' : A person or list of people who collected a voucher specimen of
         a genetic sample -> str

    Returns
    -------
    data_researcher: A pandas DataFrame with processed data and
    the columns necessary to create table 'PESQUISADOR'
    """

    # Split lists of genetic sample ('nome_coletor') and voucher collectors ('nome_coletor_especime', exclusive of aves)
    # To get individual collectors. Make sure all delimiters are included in the expression
    collector = data['nome_coletor'].str.split('/|,|;|&| e ', expand=True)
    collector_voucher_aves = data['nome_coletor_especime'].str.split('/|,|;|&| e ', expand=True)

    # add researcher from the loan spreadsheet below when it is ready, this will need to be passed as new argument

    # list all dataframes with split collectors you want to combine
    frames = [collector, collector_voucher_aves]

    # Concat dataframes with split collectors and stack individual collectors from multiple to single column
    # Strip blank spaces, sort by name and drop duplicates
    all_researchers_stacked = pd.DataFrame(
        {'nome_pesquisador': pd.concat(frames).stack().apply(lambda x: x.strip()).sort_values().unique(), })

    # Replace empty space by np.nan and drop them
    all_researchers_stacked['nome_pesquisador'].replace('',np.nan,inplace=True)
    all_researchers_stacked = all_researchers_stacked.dropna()

    # Split first and last name
    all_researchers_stacked[['nome_pesquisador', 'sobrenome_pesquisador']] = all_researchers_stacked["nome_pesquisador"].str.split(" ", n=1, expand=True)

    # Reset and drop index
    data_researcher = all_researchers_stacked.reset_index(drop=True)

    # Reset index to start from 1
    data_researcher.index = data_researcher.index + 1

    # Turn index into a column with new column named 'id_pesquisador'
    data_researcher = data_researcher.reset_index().rename(columns={'index': 'id_pesquisador'})

    # add NaN for email and institution, figure out the best way to retrieve this information later
    data_researcher[['email', 'instituicao']] = None

    return data_researcher

def filter_data_ave(data):
    """Compile a list of all data exclusive to the bird spreadsheet, i. e.
    not shared with herps and fish, to create Table 'AVE' and export to the Database

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'num_amostra' : Unique genetic sample number -> str
        b) 'sex' : sex of the specimen in string format.
        Ideally should be M (male), F (female), I (undetermined) -> str
        c) 'expedicao': Expedition during which the sample was collected -> str
        d) 'tempo_ate_conservar': Description of how much time until preservation of samples -> str
        e) 'metodo_coleta': Method used to collect sample -> str
        f) 'meio_preserv_def: material used to preserve sample -> str
        g) 'data_preparacao': date when specimen was taxidermized -> datatime
        h) 'musculo': if sample is of type musculo (muscle) X for yes, blank for No -> str
        i) 'sangue': if sample is of type sangue (blood). X for yes, blank for No -> str
        j) 'figado': if sample is of type figado (liver). X for yes, blank for No -> str
        k) 'coracao': if sample is of type coracao (heart). X for yes, blank for No -> str
        l) 'genero_especie': Sample's species name with genus + specific epithet
        m) 'localidade': Locality where sample was collected
        n) 'sigla_preparador': Abbreviation of the person who prepared the specimen in str format
        o) 'numero_preparador': Sequential number of specimens from the person who
         prepared the specimen in str format

    Returns
    -------
    data_ave: A pandas DataFrame with processed data and
    the columns necessary to create table 'AVE'
    """
    # Filter rows from the bird collection and filter columns of interest
    data_ave = data.loc[['aves']].filter(['num_amostra', 'sexo', 'sexo_obs', 'expedicao', 'tempo_ate_conservar', 'metodo_coleta',
                            'meio_preserv_def', 'data_preparacao', 'data_preparacao_obs', 'musculo', 'sangue', 'figado', 'coracao',
                            'genero_especie', 'localidade', 'sigla_preparador', 'numero_preparador'], axis=1)

    # Exclude rows that are blank for species name and locality which are blank spaces in the spreadsheet
    data_ave = data_ave[(data_ave['genero_especie'].isnull() == False) | (data_ave['localidade'].isnull() == False)]

    # Get regex patterns to clean all names that are not genus and specific epithet
    patterns = [r'\s[(]sp. nov.[)]', r' cf[.]']

    # Apply regex to clean species names by removing captured patterns
    data_ave['genero_especie'] = data_ave['genero_especie'].replace(patterns, '', regex=True)

    data_ave['subespecie'] = data_ave['genero_especie'].apply(
        lambda x: None if type(x) != str else x.split()[2] if len(x.split()) > 2 else None)

    # Get only genus + specific epithet
    data_ave['genero_especie'] = data_ave['genero_especie'].apply(
        lambda x: x if type(x) != str else x.split()[0].strip().capitalize() + ' ' + x.split()[
            1].strip().lower() if len(x.split()) > 1 else x.split()[0].strip().capitalize() + ' sp.')

    # Get num_preparador by joining 'sigla_preparador' and 'numbero_preparador'
    data_ave['num_preparador'] = data_ave[['sigla_preparador', 'numero_preparador']].apply(
        lambda x: str(x[0]) + ' ' + str(x[1]) if pd.isna(x[1]) == False else x[0], axis=1)

    # Convert sample type in boolean
    for i in ['musculo', 'sangue', 'figado', 'coracao']:
        data_ave[i] = data_ave[i].apply(lambda x: True if pd.isna(x) is False else False)

    # Select only relevant columns
    data_ave = data_ave.drop(['localidade', 'genero_especie', 'sigla_preparador', 'numero_preparador'], axis=1)

    # Convert NaNs to None
    data_ave = data_ave.astype(object).where(pd.notnull(data_ave), None)

    return data_ave

def filter_data_researcher_ave(data):
    """Make a list of collecting records, associating every bird sample
    to their voucher collector individually to create Table 'PESQUISADOR_AVE'
    and export to the Database

    Voucher collectors are associated to samples individually to
    normalize the database by avoiding multiple values of collector
    in the same cell

    These are collectors of Voucher specimens of birds only, because in
    in this database genetic and voucher collectors may be different.
    For collectors of genetic samples see filter_collector()

    A initial list of all researchers in the database is retrieved with
    filter_data_researcher()

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'nome_coletor_especime' : A person or list of people who collected a voucher specimen -> str
        b) 'num_amostra' : Unique genetic sample number -> str
        c) 'genero_especie' : Species name -> str
        d) 'localidade' : locality where sample was colllected -> str
    Returns
    -------
    data_researcher_ave: A pandas DataFrame with processed data and
    the columns necessary to create table 'PESQUISADOR_AVE'
    """

    # Get general list of collectors
    data_researcher = filter_data_researcher(data)

    # Join collectors first and last names to form a single name
    data_researcher['nome_completo_pesquisador'] = data_researcher[['nome_pesquisador', 'sobrenome_pesquisador']].apply(
        lambda x: x[0] + ' ' + x[1] if pd.isna(x[1]) == False else x[0], axis=1)

    # Get columns of interest only in the bird ('ave') records
    data_sample = data.loc[['aves']].filter(['num_amostra', 'nome_coletor_especime', 'genero_especie', 'localidade'], axis=1)

    # Delete samples with no information for 'genero_especie' and 'localidade' which are blank spaces in the spreadsheet
    data_sample = data_sample[
        (data_sample['genero_especie'].isnull() == False) | (data_sample['localidade'].isnull() == False)]

    # Split voucher collectors name
    data_sample['nome_coletor_especime'] = data['nome_coletor_especime'].str.split('/|,|;|&| e ')

    # Explode voucher collector's name to make one row per sample-collector, where samples can repeat to associate
    # samples to all of its collectors in a separate row
    data_sample = data_sample.explode('nome_coletor_especime')

    # Delete spaces before and after names
    data_sample['nome_coletor_especime'] = data_sample['nome_coletor_especime'].str.strip()

    # Merge sample and researchers dataframes, keep only columns of interest, sort values and reset index
    data_researcher_ave = data_researcher.merge(data_sample, right_on='nome_coletor_especime', left_on='nome_completo_pesquisador')[
        ['id_pesquisador', 'num_amostra']].sort_values(by='num_amostra').reset_index(drop=True)

    # Reset index to start at 1
    data_researcher_ave.index = data_researcher_ave.index + 1

    # Rename columns
    data_researcher_ave = data_researcher_ave.reset_index().rename(
        columns={'index': 'id_pesq_ave', 'num_amostra': 'num_amostra'})

    return data_researcher_ave

def filter_data_collect(data):
    """Make a list of collecting records, associating every sample
    to their collector individually to create Table 'COLETOR'
    and export to the Database

    Collectors are assoaciated to sample individually to
    normalize the database by avoiding multiple values of collector
    in the same cell

    These are collectors of genetic samples only. For collectors of
    voucher specimens of birds see filter_researcher_ave()

    A initial list of all researchers in the database is retrieved with
    filter_data_researcher()

    Parameters
    ----------
    data : A pandas Dataframe with the named columns:
        a) 'nome_coletor' : A person or list of people who collected a genetic sample -> str
        b) 'num_amostra' : Unique genetic sample number -> str
        c) 'genero_especie' : Species name -> str
        d) 'localidade' : locality where sample was colllected -> str

    Returns
    -------
    data_merge: A pandas DataFrame with processed data and
    the columns necessary to create table 'COLETA'
    """

    # Get general list of collectors
    data_researcher = filter_data_researcher(data)

    # Join collectors first and last names to form a single name
    data_researcher['nome_completo_pesquisador'] = data_researcher[['nome_pesquisador', 'sobrenome_pesquisador']].apply(
        lambda x: x[0] + ' ' + x[1] if pd.isna(x[1]) == False else x[0], axis=1)

    # Get columns of interest
    data_sample = data.filter(['num_amostra', 'nome_coletor', 'data_coleta', 'data_coleta_obs', 'genero_especie', 'localidade'],
                               axis=1)
    # Drop samples with no information for species and locality which are blank spaces in the database
    data_sample = data_sample[(data_sample['genero_especie'].isnull() == False) | (data_sample['localidade'].isnull() == False)]

    # Split collector's name
    data_sample['nome_coletor'] = data['nome_coletor'].str.split('/|,|;|&| e ')

    # Explode collector's name to make one row per sample-collector, where samples can repeat to associate
    # every collector in a separate row
    data_sample = data_sample.explode('nome_coletor')

    # Remove spaces before and after from collector's name
    data_sample['nome_coletor'] = data_sample['nome_coletor'].str.strip()

    # Merge sample and research dataframes to get researcher foreign key, select columns of interest
    # sort values by 'num_amostra', reset index
    data_collect = data_researcher.merge(data_sample, right_on='nome_coletor', left_on='nome_completo_pesquisador')[
        ['data_coleta', 'data_coleta_obs', 'num_amostra', 'id_pesquisador']].sort_values(by='num_amostra').reset_index(drop=True)

    # Reset index to start in 1
    data_collect.index = data_collect.index + 1

    # Rename columns
    data_collect = data_collect.reset_index().rename(
        columns={'index': 'id_coleta', 'num_amostra': 'num_amostra', 'data_coleta': 'data_coleta'})

    # Make sure all dates are in datetime format, if not convert to NaN
    data_collect['data_coleta'] = pd.to_datetime(data_collect['data_coleta'], format='%Y-%m-%d', errors='coerce')

    # Convert NaNs to None
    data_collect = data_collect.astype(object).where(pd.notnull(data_collect), None)

    return data_collect

####### AFAZERES

## Quinta feira

# Modificar SQL:
# da base de dados de peixe para incluir as demais colunas que ficaram faltando
# Verificar todos os campos que estão faltando em comum por exemplo pais e municipio
# revisar modelo e SQL pra ver se está contemplando as mudanças feita aqui