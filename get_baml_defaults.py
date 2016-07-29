from get_defaulters import *
from get_baml_bonds import *


def get_baml_defaults(agency_folder_date = '2016 07 27', start_date = '2012-12-31', end_date = '2015-12-31', verbose = False):
    '''
    get a list of cusips/isins from the baml indices that have defaulted in a specified time window
    :param agency_folder_date: the folder date with the agency ratings
    :param start_date: flag if bond has defaulted after the start_date
    :param end_date:  and flag if the bond has defaulted prior to the end _ate
    :return: a dataframe with cusip and boolean default flag
    '''

    if verbose:
        print('start get_baml_defaults()...')

    # 1. get a list of all cusips/isins ever in BAML
    baml_bonds = get_baml_bonds(verbose = True)

    # 2. get all defaulted bond cusips/isins
    moodys_defaults, sp_defaults, fitch_defaults = get_defaulters(date = agency_folder_date, verbose = True)
    sp_defaults_by_cusip = sp_defaults[ ['Cusip', 'S&P Default Case']]
    sp_defaults_by_isin = sp_defaults[ ['Isin','S&P Default Case']]

    fitch_defaults_by_cusip = fitch_defaults[ ['Cusip','Fitch Default Case']]
    fitch_defaults_by_isin = fitch_defaults[ ['Isin','Fitch Default Case']]

    # 3. merge list of baml bonds with list of defaulted bonds
    if verbose:
        print('combine baml_bonds and sp_defaults and fitch_defaults...')
    baml_bonds = baml_bonds.merge(sp_defaults_by_isin, how = 'left',  left_on = 'Isin', right_on = 'Isin')
    baml_bonds = baml_bonds.merge(fitch_defaults_by_isin, how = 'left', left_on = 'Isin', right_on = 'Isin')

    baml_bonds = baml_bonds.merge(sp_defaults_by_cusip, how = 'left',  left_on = 'Cusip', right_on = 'Cusip', suffixes = ['',' by Cusip'])
    baml_bonds = baml_bonds.merge(fitch_defaults_by_cusip, how = 'left', left_on = 'Cusip', right_on = 'Cusip', suffixes = ['', ' by Cusip'])

    baml_bonds = baml_bonds.merge(moodys_defaults, how = 'left', left_on = ['BAML Ticker', 'BAML Name'], right_on = ['BAML Ticker', 'BAML Name'])

    if verbose:
        print('just keep bonds that have defaulted...')
    baml_bonds = baml_bonds[ pd.notnull(baml_bonds['S&P Default Case']) | pd.notnull(baml_bonds['Fitch Default Case']) |
                             pd.notnull(baml_bonds['S&P Default Case by Cusip']) | pd.notnull(baml_bonds['Fitch Default Case by Cusip']) |
                             pd.notnull(baml_bonds['Moodys Default Case']) ]
    def make_list(x):
        if isinstance(x, float):
            x = ()
        return x

    fields = ['S&P Default Case', 'Fitch Default Case', 'Moodys Default Case', 'S&P Default Case by Cusip', 'Fitch Default Case by Cusip']
    for f in fields:
        baml_bonds[f] = baml_bonds[f].map(make_list)

    # 4. combine the Moody's, S&P and Fitch default cases into a single list
    baml_bonds['Default Cases'] = baml_bonds['S&P Default Case'] + baml_bonds['Fitch Default Case'] + \
                                  baml_bonds['Moodys Default Case'] +  baml_bonds['S&P Default Case by Cusip'] + \
                                  baml_bonds['Fitch Default Case by Cusip']

    # 5. only keep unique default cases and save as list
    baml_bonds['Default Cases'] = (baml_bonds['Default Cases']).map(set)
    baml_bonds['Default Cases'] = (baml_bonds['Default Cases']).map(list)


    def mark_default(default_cases, begin_period, end_period):
        # determine if a default occurred between begin_date and end_date
        # default_cases is a list of default tuples where tuple is (agency, cusip, isin', rating, rating date)
        default_flag = False
        for case in default_cases:
            default_date = case[4]
            if default_date < end_date and default_date > start_date:
                default_flag = True
        return default_flag

    # 6. only keep cusip and default flag
    baml_defaults = baml_bonds.copy()
    baml_defaults['Default'] = baml_defaults['Default Cases'].apply(mark_default, args = (start_date, end_date))
    baml_defaults = baml_defaults[['Cusip', 'Default']]
    return baml_defaults