import pandas as pd
import numpy as np
import timeit
import datetime
class AgencyRatings():

    '''
    Use this class to access bond-level agency rating data feeds:
    1. get the individual agency ratings
    2. generate agency composite ratings (ACR)

    This class pulls from Y:\QuantitativeStrategy\data-warehouse-exports

    To start, load the agency rating data feeds ingo self.moodys, self.sp, self.fitch by using load_agency_data.
    This is a lot of data! It is therfore slow to load but once it's loaded you have fast access to
    everything as it is stored in memory



    '''

    def __init__(self):
        self.moodys = None
        self.sp = None
        self.fitch = None

        # save baml constituents for use in backfill when we need to search through the baml bonds
        self.baml_constituents = None
        self.baml_constituents_loaded = False

    def load_agency_data(self, verbose = False):
        '''
        get the incremental agency ratings from data warehouse exports
        read in moodys, sp and fitch incremental data and store as attributes of class object
        save in self.moodys, self.sp, self.fitch
        '''

        start = timeit.default_timer()
        moodys = pd.read_csv('Y:\\QuantitativeStrategy\\data-warehouse-exports\\moodys_issue_rating_history.csv')
        #moodys = pd.read_csv('Y:\\QuantitativeStrategy\\staging-dw-exports\\moodys_issue_rating_history.csv')
        #moodys = pd.read_csv('moodys_issue_rating_history.csv')

        if verbose:
            print('moodys loaded in {} seconds'.format(timeit.default_timer() - start))

        start = timeit.default_timer()
        sp = pd.read_csv('Y:\\QuantitativeStrategy\\data-warehouse-exports\\s_p_issue_rating_history.csv')
        #sp = pd.read_csv('Y:\\QuantitativeStrategy\\staging-dw-exports\\s_p_issue_rating_history.csv')

        if verbose:
            print('sp loaded in {} seconds'.format(timeit.default_timer() - start))

        # get the csv files
        start = timeit.default_timer()
        fitch = pd.read_csv('Y:\\QuantitativeStrategy\\data-warehouse-exports\\fitch_issue_rating_history.csv')
        #fitch = pd.read_csv('Y:\\QuantitativeStrategy\\staging-dw-exports\\fitch_issue_rating_history.csv')
        #fitch = pd.read_csv('fitch_issue_rating_history.csv')

        if verbose:
            print('fitch loaded in {} seconds'.format(timeit.default_timer() - start))

        # the agency rating feed gives cusip as 9 digits
        # but many sources like baml might only give 8 cusips (no check digit)
        # for consistency, convert all cusips in the incremental agency rating data to 8 digits
        # (for isins, assume 12 digits)
        if verbose:
            print('converting to 8 digit cusip')

        mask = sp['id_type'].isin(['Cusip1', 'Cusip2', 'Cusip3', 'Cusip4', 'Cusip5', 'Cusip6'])
        sp.loc[mask, 'id_value'] = sp.loc[mask, 'id_value'].map(lambda x: x[:8])

        mask = fitch['id_type'].isin(['Cusip1', 'Cusip2', 'Cusip3', 'Cusip4', 'Cusip5', 'Cusip6'])
        fitch.loc[mask, 'id_value'] = fitch.loc[mask, 'id_value'].map(lambda x: x[:8])

        mask = moodys['id_type_text'].isin(['CUSIP',
                                            'CUSIP 3',
                                            'CUSIP 4',
                                            'CUSIP 5',
                                            'CUSIP - Previous',
                                            'CUSIP - Second',
                                            'CUSIP-2ndary Wrap Orig. CUSIP',
                                            'CUSIP-Deriv/Underlying Bond'])
        moodys.loc[mask, 'instrument_id_value'] = moodys.loc[mask, 'instrument_id_value'].map(lambda x: x[:8])

        # for moodys, only keep certain types of ratings
        # exclude ratings like bank credit facility, preferred stock
        # sometimes a bond can have multiples types of ratings, but we only want the 'regular bond rating'
        mask1 = moodys['security_class_short_description'] == 'REG'  # regular bond/debenture
        mask2 = moodys['security_class_short_description'] == 'MTN'  # medium term note
        mask3 = moodys['security_class_short_description'] == 'PRF'  # medium term note
        mask4 = moodys['security_class_short_description'] == 'CON'  # medium term note
        moodys = moodys[mask1 | mask2 | mask3 | mask4]

        # exclude LGD ratings
        mask = moodys['rating_class_text'].map(lambda x: 'LGD' in x)
        moodys = moodys[-mask]

        # save as attributes of class
        self.moodys = moodys
        self.sp = sp
        self.fitch = fitch

    def get_fitch_ratings(self, data, id_col, date = 'current'):

        '''
        attach a column with fitch ratings to a dataset

        pass in a dataframe with a group of bonds in the rows. we want to add a new column with the fitch rating

        :param data: a dataset that contains bonds that you want the rating for, as dataframe
        :id_col: the name of the column in the datset that contains either the cusip or isin, as string
        :param date: date(s) of the ratings you want, either 'current', a date in datetime.date, or 'incremental'
        :return: a dataset with an added fitch_rating column, as dataframe
        '''

        original = data.copy()

        # organize the bonds you want to get ratings for
        # keep just a dataframe with a single column of bond identifiers (cusip or isin)
        df = data.copy()
        df = df[[id_col]]

        # merge the entire incremental fitch ratings dataset and only keep bonds from the target group above
        df = df.merge(self.fitch, how = 'left', left_on = id_col, right_on = 'id_value')


        # data cleanup
        # set a datetime object and sort
        df['long_term_issue_rating_effective_date'] = pd.to_datetime(df['long_term_issue_rating_effective_date'])
        df.sort_values(by = [id_col, 'long_term_issue_rating_effective_date'], inplace = True)

        # get the ratings you want
        # if you just want the current ratings, then keep the last rating action for each bond
        if date == 'current':
            df.drop_duplicates(subset = id_col, keep = 'last', inplace = True)
        # but if you want a record of all ratings actions, don't drop anything
        elif date == 'incremental':
            pass
        # and if you want a rating on a specific historical date,
        # then keep the most recent rating prior to the historical date
        else:
            end_of_date = datetime.datetime(date.year, date.month, date.day, 23,59,59)
            df = df[df['long_term_issue_rating_effective_date'] <= end_of_date]
            df.drop_duplicates(subset = id_col, keep = 'last', inplace = True)

        # data cleanup
        fitch_fields = ['agent_common_id',
                        'issuer_name',
                        'fitch_issue_id_number',
                        'id_type',
                        'id_value',
                        'issue_description',
                        'long_term_issue_rating_effective_date']

        # delete columns that aren't needed
        for f in fitch_fields:

            # if incremental production, then keep everything
            if (date == 'incremental') & (f == 'long_term_issue_rating_effective_date'):
                pass
            # but if current or historical production, just keep the rating, rating date, and seniority
            else:
                del df[f]

        # data cleanup
        df.rename(columns = {'long_term_issue_rating': 'fitch_rating',
                            'long_term_issue_rating_effective_date': 'rating_date',
                            'issue_debt_level_code': 'fitch_seniority'}, inplace = True)

        return df

    def get_moodys_ratings(self, data, id_col, date):
        '''
        attach a column with moodys ratings to a dataset
        :param data: a dataset that contains bonds that you want the rating for, as dataframe
        :id_col: the name of the column in the datset that contains either the cusip or isin, as string
        :param date: date(s) of the ratings you want, either 'current', a date in datetime.date format, or 'incremental'
        :return: a dataset with an added moodys_rating column, as dataframe
        '''


        df = data.copy()
        df = df[[id_col]]


        df = df.merge(self.moodys, how = 'left', left_on = id_col, right_on = 'instrument_id_value')

        df['rating_date'] = pd.to_datetime(df['rating_date'])
        df.sort_values(by = [id_col, 'rating_date'], inplace = True)

        if date == 'current':
            df.drop_duplicates(subset = id_col, keep = 'last', inplace = True)
        elif date == 'incremental':
            pass
        else:
            end_of_date = datetime.datetime(date.year, date.month, date.day, 23,59,59)
            df = df[df['rating_date'] <= end_of_date]
            df.drop_duplicates(subset = id_col, keep = 'last', inplace = True)

        moodys_fields = ['instrument_id',
                         'moodys_rating_id',
                         'security_class_short_description',
                         'id_type_text',
                         'instrument_id_value',
                         'rating_date',
                         'rating_class_text',
                         'rating_direction_short_description',
                         'rating_type_short_description',
                         'rating_currency_iso_code']
        for f in moodys_fields:
            if (date == 'incremental') & (f == 'rating_date'):
                pass
            else:
                del df[f]
        df.rename(columns = {'rating_text': 'moodys_rating',
                            'seniority_short_description': 'moodys_seniority'}, inplace = True)

        return df

    def get_sp_ratings(self, data, id_col, date):
        '''
        attach a column with S&P ratings to a dataset
        :param data: a dataset that contains bonds that you want the rating for, as dataframe
        :id_col: the name of the column in the datset that contains either the cusip or isin, as string
        :param date: date(s) of the ratings you want, either 'current', a date in datetime.date format, or 'incremental'
        :return: a dataset with an added sp_rating column, as dataframe
        '''

        df = data.copy()
        df = df[[id_col]]

        df = df.merge(self.sp, how = 'left', left_on = id_col, right_on = 'id_value')

        df['rating_date'] = pd.to_datetime(df['rating_date'])
        df.sort_values(by = [id_col, 'rating_date'], inplace = True)

        if date == 'current':
            df.drop_duplicates(subset = id_col, keep = 'last', inplace = True)
        elif date == 'incremental':
            pass
        else:
            end_of_date = datetime.datetime(date.year, date.month, date.day, 23,59,59)
            df = df[df['rating_date'] <= end_of_date]
            df.drop_duplicates(subset = id_col, keep = 'last', inplace = True)

        sp_fields = ['security_id',
                     'security_symbol_value',
                     'id_type',
                     'id_value',
                     'rating_date']

        for f in sp_fields:
            # don't delete rating date if incremental
            if (date == 'incremental') & (f == 'rating_date'):
                pass
            else:
                del df[f]
        df.rename(columns = {'rating': 'sp_rating'}, inplace = True)

        return df

    def get_time_series_by_id(self, data, id_col, start_date, end_date, verbose = False):
        '''
        generate a daily time series of ratings given a set of bonds
        :param data: a dataset that contains bonds that you want the rating for, as dataframe
        :id_col: the name of the column in the datset that contains either the cusip or isin, as string
        :param start_date: start date of the time series in 'YYYY-MM-DD' format
        :param end_date: end date of the time series in 'YYYY-MM-DD' format
        :return: a time series dataset with an added moodys_rating, sp_rating, fitch_rating columns, as dataframe
        '''

        # get the incremental ratings from each agency for the given set of bonds
        if verbose:
            print('get incremental ratings for given bonds')
        moodys = self.get_moodys_ratings(data, id_col, date = 'incremental')
        sp = self.get_sp_ratings(data, id_col, date = 'incremental')
        fitch = self.get_fitch_ratings(data, id_col, date = 'incremental')

        if verbose:
            print('--process incremental ratings for combination with date template')
        for db in [moodys, sp , fitch]:

            # sort by date-time
            db.sort_values(by = [id_col, 'rating_date'], inplace = True)

            # convert date-time to just dates
            # (otherwise cannot properly merge into the date range below which are date types, not date-time types)
            db['rating_date'] = db['rating_date'].dt.date

            # keep the last action when multiple rating actions on the same date
            db.drop_duplicates(subset = [id_col, 'rating_date'], keep = 'last', inplace = True)

        # convert incremental data to daily time series
        # generate a series with daily dates
        if verbose:
            print('--create a date template')
        dates = pd.date_range(start = start_date, end = end_date, freq = 'D')
        dates = pd.DataFrame(dates)
        dates.rename(columns = {0: 'date'}, inplace = True)
        dates['date'] = dates['date'].dt.date    # convert datetime to just date
        dates['join'] = 1
        dates['from_date_template'] = 1

        # get a df with a columns of all cusips/isins
        bonds = data.copy()
        bonds = bonds[[id_col]]
        bonds.drop_duplicates(subset = id_col, inplace = True)
        bonds['join'] = 1

        # combine daily date range with the bonds
        # this will produce a dataframe with a daily observation for every bond
        # two columns: date, bond with rows:
        # bond1-day1
        # bond1-day2
        # ...
        # bond1-dayN
        # bond2-day1
        # bond2-day2
        # ...
        # bond2-dayN
        dates = dates.merge(bonds, how = 'left', left_on = 'join', right_on = 'join')
        del dates['join']

        # merge in the incremental moodys ratings to the date template
        dates = dates.merge(moodys, how = 'outer', left_on = [id_col, 'date'], right_on=[id_col, 'rating_date'])

        # find places where we have a rating_date but no date
        # ie the date_range was from 2000-2017 but the first rating date was 1995
        mask1 = (dates['date'].isnull()) & (dates['rating_date'].notnull())

        # set date to rating_date in the above cases
        dates.loc[mask1, 'date'] = dates.loc[mask1, 'rating_date']
        del dates['rating_date']

        # repeat for s&p ratings
        dates = dates.merge(sp, how = 'outer', left_on = [id_col, 'date'], right_on=[id_col, 'rating_date'])
        mask1 = (dates['date'].isnull()) & (dates['rating_date'].notnull())
        dates.loc[mask1, 'date'] = dates.loc[mask1, 'rating_date']
        del dates['rating_date']

        # repeat for fitch ratings
        dates = dates.merge(fitch, how = 'outer', left_on = [id_col, 'date'], right_on=[id_col, 'rating_date'])
        mask1 = (dates['date'].isnull()) & (dates['rating_date'].notnull())
        dates.loc[mask1, 'date'] = dates.loc[mask1, 'rating_date']
        del dates['rating_date']

        dates.loc[dates['from_date_template'].isnull(), 'from_date_template'] = 0

        dates.sort_values(by = [id_col, 'date'], inplace = True)

        # fill forward the ratings to convert from incremental to daily
        dates = dates.groupby(by = id_col, as_index = False).fillna(method='ffill')

        # drop cases that are not from the date template
        # ie cases where we instantiate the rating at 1/1/1900
        dates = dates[dates['from_date_template'] == 1]
        del dates['from_date_template']
        return dates



    def get_agency_ratings_by_id(self, data, id_col, date = 'current'):
        '''
        pass in a dataset that contains a column with cusips that you want to get agency ratings for
        get the agency rating for either the 'current' date or a specified historical date
        :param data: a dataset that contains bonds that you want the rating for, as dataframe
        :id_col: the name of the column in the datset that contains either the cusip or isin, as string
        :param date: date(s) of the ratings you want, either 'current', a date in datetime.date format, or 'incremental'
        :return: a dataset with an added moodys_rating column, as dataframe

        '''

        assert date != 'incremental', 'error: cannot use incremental ratings'
        assert id_col in data.columns, 'error: could not find the id column in data'
        if date != 'current':
            assert isinstance(date, datetime.date), 'error: for non current date values you must pass date as datetime.date'

        # get fitch ratings
        fitch = self.get_fitch_ratings(data, id_col, date)
        moodys = self.get_moodys_ratings(data, id_col, date)
        sp = self.get_sp_ratings(data, id_col, date)

        df = data.copy()

        df = df.merge(moodys, how = 'left', left_on = id_col, right_on = id_col)

        df = df.merge(sp, how = 'left', left_on = id_col, right_on = id_col)
        df = df.merge(fitch, how = 'left', left_on = id_col, right_on = id_col)

        for rating in ['moodys_rating', 'sp_rating', 'fitch_rating']:
            df.loc[df[rating].isnull(), rating] = 'NR'

        return df

    def get_average_ratings(self, data, require_two_agencies = True):
        '''
        calculate the average agency rating
        :param data: , a dataset with columns for moodys, sp and fitch alphanumeric ratings, as dataframe
        :param require_two_agencies: require at least two agency ratings in order to calculate average, as boolean,
        :return: the input dataset with new columns for average ratings
        '''

        for c in ['moodys_rating', 'sp_rating', 'fitch_rating']:
            assert c in data.columns, 'error: cannot find {} in data'.format(c)

        # mapping from alphanumeric to numeric rating
        self.numeric_dict = {'AAA': 21,
                            'AA1': 20, 'AA2': 19, 'AA3': 18,
                            'A1': 17, 'A2': 16, 'A3': 15,
                            'BBB1': 14, 'BBB2': 13, 'BBB3': 12,
                            'BB1': 11, 'BB2': 10, 'BB3': 9,
                            'B1': 8, 'B2': 7, 'B3': 6,
                            'CCC1': 5, 'CCC2': 4, 'CCC3': 3,
                            'CC': 2, 'C': 1, 'D': 0,

                            'Aaa': 21, 'Aa1': 20, 'Aa2': 19, 'Aa3': 18,
                            'A1': 17, 'A2': 16, 'A3': 15,
                            'Baa1': 14, 'Baa2': 13, 'Baa3': 12,
                            'Ba1': 11, 'Ba2': 10, 'Ba3': 9,
                            'Caa1': 5, 'Caa2': 4, 'Caa3': 3,
                            'Ca': 2, 'C': 1,

                            'AA+': 20, 'AA': 19, 'AA-': 18,
                            'A+': 17, 'A': 16, 'A-': 15,
                            'BBB+': 14, 'BBB': 13, 'BBB-': 12,
                            'BB+': 11, 'BB':10, 'BB-': 9,
                            'B+': 8, 'B':7, 'B-': 6,
                            'CCC+': 5, 'CCC': 4, 'CCC-': 3,

                            'SD': 0, 'RD': 0, 'WR': np.NaN, 'NR': np.NaN, 'WD': np.NaN
                             }

        # mapping from numeric to alphanumeric rating
        self.alphanumeric_dict = {21: 'AAA',
                                  20: 'AA1', 19: 'AA2', 18: 'AA3',
                                  17: 'A1', 16: 'A2', 15: 'A3',
                                  14: 'BBB1', 13: 'BBB2', 12: 'BBB3',
                                  11: 'BB1', 10: 'BB2', 9: 'BB3',
                                  8: 'B1', 7: 'B2', 6: 'B3',
                                  5: 'CCC1', 4: 'CCC2', 3: 'CCC3',
                                  2: 'CC', 1: 'C', 0: 'D',
                                  'NaN': 'NR'
                                  }

        df = data.copy()

        # map alphanumeric ratings to a number
        df['moodys_num'] = df['moodys_rating'].map(self.numeric_dict)
        df['sp_num'] = df['sp_rating'].map(self.numeric_dict)
        df['fitch_num'] = df['fitch_rating'].map(self.numeric_dict)

        # calculate average agency rating
        # offset numeric average by a small amount so that X.5 it gets rounded down to X and not rounded up to X + 1
        df['average_rating_num'] = df[ ['moodys_num', 'sp_num', 'fitch_num'] ].apply(np.mean, axis = 1) - 0.0002
        mask = df['average_rating_num'].notnull()
        df.loc[mask, 'average_rating_num'] = df.loc[mask, 'average_rating_num'].map(round)
        df['agency_rating_count'] = df[ ['moodys_num', 'sp_num', 'fitch_num'] ].count(axis = 1)

        del df['moodys_num']
        del df['sp_num']
        del df['fitch_num']

        # null average if less than two agency ratings
        if require_two_agencies == True:
            mask1 = df['agency_rating_count'] < 2
            df.loc[mask1, 'average_rating_num'] = np.NaN

        # notching based on seniority
        # TO DO

        # map numeric average to alphanumeric rating
        df['average_rating'] = df['average_rating_num'].map(self.alphanumeric_dict)
        df['average_rating'] = df['average_rating'].fillna('NR')

        return df