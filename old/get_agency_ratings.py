import pandas as pd
import numpy as np

def get_agency_ratings(agency_file_date, most_recent_rating = True, ratings_date = None, verbose = False):
    '''
    :param agency_date: the YYYY MM DD folder date
    :return: a dataframe with latest moodys, sp, fitch rating per bond
    '''

    if verbose:
        print('start get_agency_ratings()...')

    agency_path = 'Z:\\Ratings\\StataProduction\\Agency Composite Ratings\\Agency Data Input\\History\\' + agency_file_date + '\\'

    # 1. get the latest fitch rating for each bond
    if verbose:
        print('get fitch rating...')
    keep_columns = ['Cusip', 'Isin', 'IssuerName', 'LongTermIssueRating', 'LongTermIssueRatingEffectiveDate']
    fitch = pd.read_csv(agency_path + 'fitch_issue_ratings.csv', sep = ',', usecols = keep_columns)
    fitch['Rating Date'] = pd.to_datetime(fitch['LongTermIssueRatingEffectiveDate'])
    fitch.rename(columns = {'IssuerName': 'Fitch Issuer Name', 'LongTermIssueRating': 'Fitch Rating'}, inplace = True)

    # drop if no cusip
    fitch = fitch[fitch['Cusip'].notnull()]
    fitch['Cusip'] = fitch['Cusip'].map(lambda x: x[:8])

    # keep the most recent rating action
    fitch.sort_values(by=['Cusip', 'Rating Date'], inplace=True)
    if most_recent_rating == False:
        fitch = fitch.ix[fitch['Rating Date'] <= ratings_date, :]

    fitch['Most Recent'] = fitch['Cusip'] != fitch['Cusip'].shift(-1)
    fitch = fitch[fitch['Most Recent']]
    fitch.reset_index(drop = True, inplace = True)

    # 2. get the latest s&p rating
    if verbose:
        print('get the most recent s&p rating...')
    #sp = pd.read_csv(agency_path + 'spissue_level_ratings_history_from_cusip_isins_file.csv', sep = ',')
    sp = pd.read_csv(agency_path + 'spissue_level_ratings_history_from_cusip_isins_file.csv', sep=',')
    sp = sp[sp['SP_RoleType'] == 'Issuer']
    sp.rename(columns={'Cusips': 'Cusip', 'SP_Rating': 'S&P Rating'}, inplace=True)
    sp['Rating Date'] = pd.to_datetime(sp['RatingDate'])
    sp.sort_values(by=['SP_SecuritySymbolID', 'Rating Date'], inplace=True)
    sp.reset_index(drop=True, inplace=True)

    # drop if no cusip
    sp = sp[sp['Cusip'].notnull()]
    sp['Cusip'] = sp['Cusip'].map(lambda x: x[0:8])

    # keep the most recent rating
    if most_recent_rating == False:
        sp = sp.ix[sp['Rating Date'] < ratings_date, :]
    sp['Most Recent'] = sp['Cusip'] != sp['Cusip'].shift(-1)
    sp = sp[sp['Most Recent']]
    sp.reset_index(drop = True, inplace = True)

    # 3. get the latest moodys rating
    if verbose:
        print('get moodys rating...')

    moodys = pd.read_csv(agency_path + 'MoodysInstrumentDesc.csv', sep = ',', usecols = ['InstrumentID', 'Cusip', 'ISIN', 'MoodysLegalName'])
    moodys2 = pd.read_csv(agency_path + 'MoodysInstrumentRatingHistory.csv', sep = ',', usecols = ['InstrumentID', 'RatingText', 'RatingLocalDate'])
    moodys = moodys.merge(moodys2, left_on = 'InstrumentID', right_on = 'InstrumentID')
    moodys.rename(columns = {'ISIN': 'Isin', 'RatingText': 'Moodys Rating'}, inplace = True)
    moodys['Rating Date'] = pd.to_datetime(moodys['RatingLocalDate'])
    del moodys['RatingLocalDate']

    # drop if no cusip
    moodys = moodys[moodys['Cusip'].notnull()]
    moodys['Cusip'] = moodys['Cusip'].map(lambda x: x[:8])

    # keep the most recent rating action
    moodys.sort_values(by=['Cusip', 'Rating Date'], inplace=True)
    if most_recent_rating == False:
        moodys = moodys.ix[ moodys['Rating Date'] < ratings_date, : ]
    moodys['Most Recent'] = moodys['Cusip'] != moodys['Cusip'].shift(-1)
    moodys = moodys[moodys['Most Recent']]
    moodys.reset_index(drop=True, inplace=True)


    # 4. combine moodys, s&p, fitch ratings
    agency_ratings = fitch.merge(moodys, how = 'outer', left_on = 'Cusip', right_on = 'Cusip')
    agency_ratings = agency_ratings.merge(sp, how='outer', left_on='Cusip', right_on='Cusip')

    # 5. add C agency rating flags
    agency_ratings = agency_ratings[['Cusip', 'Moodys Rating', 'S&P Rating', 'Fitch Rating']]
    agency_ratings['C Rating'] = 0
    ineligible = ['Caa1', 'Caa2', 'Caa3', 'Ca', 'C', 'CCC+', 'CCC', 'CCC-', 'CC', 'C', 'D' ]
    for r in ['Moodys Rating', 'S&P Rating', 'Fitch Rating']:
        agency_ratings.loc[agency_ratings[r].isin(ineligible), 'C Rating'] = 1

    # 6. add split rating flag
    ig_ratings = ['Aaa', 'Aa1', 'Aa2', 'Aa3', 'A1', 'A2', 'A3', 'Baa1', 'Baa2', 'Baa3',
          'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-']
    hy_ratings = ['Caa1', 'Caa2', 'Caa3', 'Ca', 'C',
          'CCC+', 'CCC', 'CCC-', 'CC', 'C', 'D',
          'B1', 'B2', 'B3', 'Ba1', 'Ba2', 'Ba3',
          'B-', 'B', 'B+', 'BB-', 'BB', 'BB+']


    agency_ratings['Sum'] = 0
    for r in ['Moodys Rating', 'S&P Rating', 'Fitch Rating']:
        agency_ratings.loc[agency_ratings[r].isin(ig_ratings), 'Sum'] += 10
        agency_ratings.loc[agency_ratings[r].isin(hy_ratings), 'Sum'] += 2

    agency_ratings['2 IG - 1 HY'] = 0
    agency_ratings['1 IG - 2 HY'] = 0
    agency_ratings['1 IG - 1 HY'] = 0
    agency_ratings['IG Portfolio Eligible'] = 0
    agency_ratings.loc[agency_ratings['Sum'] == 10, 'IG Portfolio Eligible'] = 1
    agency_ratings.loc[agency_ratings['Sum'] == 20, 'IG Portfolio Eligible'] = 1
    agency_ratings.loc[agency_ratings['Sum'] == 30, 'IG Portfolio Eligible'] = 1
    agency_ratings.loc[agency_ratings['Sum'] == 22, 'IG Portfolio Eligible'] = 1
    agency_ratings.loc[agency_ratings['Sum'] == 12, 'IG Portfolio Eligible'] = 1

    agency_ratings.loc[agency_ratings['Sum'] == 22, '2 IG - 1 HY'] = 1
    agency_ratings.loc[agency_ratings['Sum'] == 14, '1 IG - 2 HY'] = 1
    agency_ratings.loc[agency_ratings['Sum'] == 12, '1 IG - 1 HY'] = 1


    del agency_ratings['Sum']

    agency_ratings.to_csv('agency_ratings.csv')
    return agency_ratings
