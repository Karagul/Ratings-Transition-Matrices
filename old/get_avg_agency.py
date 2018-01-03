import pandas as pd
import numpy as np
from get_agency_ratings import *


def get_avg_agency(agency_folder_date = '2016 07 20', ratings_date = '2012-12-31', verbose = False):
    '''
    query the agency rating data and get the agency ratings, avg agency rating for every bond on specified date
    :param agency_folder_date:  the date of the folder with the agency rating info
    :param ratings_date: the date on which you want the ratings for each bond
    :return: a df with cusip, average agency rating and individual agency ratings
    '''

    numeric_dict = {'AAA': 21, 'AA1': 20, 'AA2': 19, 'AA3': 18,
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

    alphanumeric_dict = {21: 'AAA', 20: 'AA1', 19: 'AA2', 18: 'AA3',
                       17: 'A1', 16: 'A2', 15: 'A3',
                       14: 'BBB1', 13: 'BBB2', 12: 'BBB3',
                       11: 'BB1', 10: 'BB2', 9: 'BB3',
                       8: 'B1', 7: 'B2', 6: 'B3',
                       5: 'CCC1', 4: 'CCC2', 3: 'CCC3',
                       2: 'CC', 1: 'C', 0: 'D', 'NaN': 'NR'
                      }


    ratings = get_agency_ratings(agency_folder_date, most_recent_rating = False, ratings_date = ratings_date, verbose = verbose)
    ratings = ratings[ ['Cusip', 'Moodys Rating', 'S&P Rating', 'Fitch Rating']]

    # convert alphanumeric ratings to numeric
    agency_ratings = ['Moodys Rating', 'S&P Rating', 'Fitch Rating']
    for r in agency_ratings:
        txt = r + ' Num'
        ratings[txt] = ratings[r].map(numeric_dict)

    # calculate average agency rating
    ratings['Average Agency Rating Num'] = ratings[ ['Moodys Rating Num', 'S&P Rating Num', 'Fitch Rating Num'] ].apply(np.mean, axis = 1) - 0.0001
    ratings['Average Agency Rating'] = ratings['Average Agency Rating Num'].map(round)
    ratings['Average Agency Rating'] = ratings['Average Agency Rating'].map(alphanumeric_dict)
    ratings['Average Agency Rating'] = ratings['Average Agency Rating'].fillna('NR')

    ratings = ratings[ ['Cusip', 'Average Agency Rating', 'Moodys Rating', 'S&P Rating', 'Fitch Rating' ]]
    return ratings

