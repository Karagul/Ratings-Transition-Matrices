import pandas as pd
import numpy as np

def get_defaulters(date, verbose = False):
    '''
    :param date: a string in 'YYYY MM DD' format
    :params verbose: print to console?
    :return: two dataframes with all defaulted cusips/isins
    '''

    if verbose:
        print('start get_defaulters()...')

    path = 'Z:\\Ratings\\StataProduction\\Agency Composite Ratings\\Agency Data Input\\History\\' + date

    # 1. get the s&p d ratings
    if verbose:
        print('get s&p defaults...')
    sp_path = path + '\\spall_issue_level_for_given_input_ratings.csv'
    sp = pd.read_csv(sp_path, usecols = ['Cusips', 'Isins', 'SP_Rating', 'RatingDate', 'SP_RoleType'])
    sp.rename(columns = {'SP_Rating': 'S&P Rating', 'Cusips': 'Cusip', 'Isins': 'Isin',
                         'SP_RoleType': 'S&P Role Type', 'RatingDate': 'S&P Rating Date'}, inplace = True)

    # take just the first 8-digits. Exclude the check digit in cusip
    sp['Cusip'] = sp['Cusip'].map(lambda x: str(x))
    sp['Cusip'] = sp['Cusip'].map(lambda x: x[:8])

    # keep just the issuer of the defaulted bond
    sp = sp[ sp['S&P Role Type'] == 'Issuer']

    # replace missing cusip/isin with None string
    sp.sort_values(by=['Cusip', 'S&P Rating Date'], inplace=True)
    sp.ix[sp['Isin'].isnull(), 'Isin'] = 'None'
    sp.ix[sp['Cusip'] == 'nan', 'Cusip'] = 'None'    # note that cusip is a string (see above), so search for 'nan'

    # create a single column with tuple that summarizes default (cusip, isin, default_date, default_rating)
    sp['Agency'] = 'S&P'
    sp['S&P Default Case'] = sp[['Agency', 'Cusip', 'Isin', 'S&P Rating', 'S&P Rating Date']].apply(tuple, axis = 1)
    sp = sp[['Cusip', 'Isin', 'S&P Default Case']]

    # collect multiple defaults for same cusip into a list
    grouped = sp.groupby(by =  ['Cusip', 'Isin'])
    sp_defaults = grouped.aggregate(lambda x: tuple(x))

    # 2. get the fitch d ratings
    if verbose:
        print('get fitch defaults...')
    fitch_path = path + '\\fitch_issue_ratings.csv'
    fitch = pd.read_csv(fitch_path, sep = ',', usecols = ['Cusip', 'Isin', 'LongTermIssueRatingEffectiveDate', 'LongTermIssueRating'])

    # just keep D ratings
    d_rating = (fitch['LongTermIssueRating'] == 'D') | (fitch['LongTermIssueRating'] == 'DD') | (fitch['LongTermIssueRating'] == 'DDD')
    fitch = fitch[d_rating]
    fitch.rename(columns = {'LongTermIssueRating': 'Fitch Rating', 'LongTermIssueRatingEffectiveDate': 'Fitch Rating Date'}, inplace = True )

    # take just the first 8-digits. Exclude the check digit in cusip
    fitch['Cusip'] = fitch['Cusip'].map(lambda x: str(x))
    fitch['Cusip'] = fitch['Cusip'].map(lambda x: x[0:8])

    # replace missing cusip/isin with None string
    fitch.sort_values(by = ['Cusip', 'Fitch Rating Date'], inplace = True)
    fitch.loc[fitch['Isin'].isnull(), 'Isin'] = 'None'
    fitch.loc[fitch['Cusip'] == 'nan', 'Cusip'] = 'None' # note that cusip is a string (see above), so search for 'nan'

    # create a single column with tuple that summarizes default (cusip, isin, default_date, default_rating)
    fitch['Agency'] = 'Fitch'
    fitch['Fitch Default Case'] = fitch[['Agency', 'Cusip', 'Isin', 'Fitch Rating', 'Fitch Rating Date']].apply(tuple, axis=1)
    fitch = fitch[['Cusip', 'Isin', 'Fitch Default Case']]

    # collect multiple defaults for same cusip into a list
    grouped = fitch.groupby(by = ['Cusip', 'Isin'])
    fitch_defaults = grouped.aggregate(lambda x: tuple(x))

    # 3. get the moodys and manual defaults
    # read all sheets from excel file into a dictionary of dataframes
    if verbose:
        print('get moodys defaults...')
    excel = pd.read_excel(
        'Y:\\Joey\\Quantitative Strategy\\ML Index and Default Tracker\\Input\\Moodys and Manual Defaults - NEW.xlsx',
        sheetname=None, header=0)

    # append annual default events together into one dataframe
    moodys = pd.DataFrame()
    for key in [k for k in excel.keys() if k != 'Notes']:
        moodys = moodys.append(excel[key][['mlTicker', 'mlName', 'defaultDate']])
    moodys.rename(
        columns={'mlName': 'BAML Name', 'mlTicker': 'BAML Ticker', 'defaultDate': 'Moodys Rating Date'}, inplace=True)
    moodys.sort_values(by=['BAML Ticker', 'BAML Name', 'Moodys Rating Date'], inplace=True)

    # store dfault date as string
    moodys['Moodys Rating Date'] = moodys['Moodys Rating Date'].map(lambda x: str(x))

    # drop duplicate observations
    moodys.drop_duplicates(inplace=True)

    # drop where the default is not associated with a baml constituent member
    moodys = moodys[moodys['BAML Name'].notnull()]  # drop cases where there is no BAML company
    moodys = moodys[moodys['BAML Ticker'].notnull()]  # drop cases where there is no BAML company

    # collect multiple defaults for same ticker/name into a list
    moodys.sort_values(by=['BAML Ticker', 'BAML Name', 'Moodys Rating Date'], inplace=True)
    moodys['Agency'] = 'Moodys'
    moodys['Moodys Rating'] = 'Manual D'
    moodys['Moodys Default Case'] = moodys[['Agency', 'BAML Ticker', 'BAML Name', 'Moodys Rating', 'Moodys Rating Date']].apply(tuple, axis = 1)
    grouped = moodys.groupby(by = ['BAML Ticker', 'BAML Name'])
    moodys_defaults = grouped.aggregate(lambda x: tuple(x))

    # write and return the s&p and fitch defaulted bonds
    sp_defaults.reset_index(inplace=True)
    fitch_defaults.reset_index(inplace=True)
    moodys_defaults.reset_index(inplace=True)

    sp_defaults.to_csv('sp_defaults.csv')
    fitch_defaults.to_csv('fitch_defaults.csv')
    moodys_defaults.to_csv('moodys_defaults.csv')

    return moodys_defaults, sp_defaults, fitch_defaults
