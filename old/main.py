import pandas as pd
import numpy as np

from get_baml_defaults import *
from RatingsTransitionMatrix import *
from get_baml_db import *
from get_avg_agency import *

def main(agency_folder_date = '2016 07 27', start_date = '2013-12-31', end_date = '2014-12-31',
         region = 'NA'):

    # 1. get a list of defaulted cusips
    baml_defaults = get_baml_defaults(agency_folder_date = agency_folder_date,
                                      start_date = start_date, end_date = end_date)

    # 2. get the baml index constituents at the start date
    baml = get_baml_db(region = region, date = start_date)

    # 3. get the agency data for the above baml constituents at the start date
    acrs_1 = get_avg_agency(agency_folder_date = agency_folder_date, ratings_date = start_date)
    acrs_1.rename(columns={'Average Agency Rating': 'Average Agency Rating (t=0)'}, inplace = True)

    # 3. get the agency data for the above baml constituents at the end date
    acrs_2 = get_avg_agency(agency_folder_date = agency_folder_date, ratings_date = end_date)
    acrs_2.rename(columns={'Average Agency Rating': 'Average Agency Rating (t=1)'}, inplace = True)

    # 4. combine baml constituent data with acr info at start_date and end_date
    df = baml.merge(acrs_1, how='left', left_on='Cusip', right_on='Cusip')
    df = df.merge(acrs_2, how='left', left_on='Cusip', right_on='Cusip')
    df = df[['Cusip', 'Average Agency Rating (t=0)', 'Average Agency Rating (t=1)']]
    df.fillna('NR', inplace=True)

    # 5. add default flags to the baml constituent, acr data
    df = df.merge(baml_defaults, how='left', left_on='Cusip', right_on='Cusip')
    df['Default'].fillna(False, inplace=True)
    df.ix[df['Default'], 'Average Agency Rating (t=1)'] = 'D'
    df.to_csv('bond_ratings.csv')

    # 6. create RatingsTransitionMatrix and load with data
    rtm = RatingsTransitionMatrix()
    for i in range(df.shape[0]):
        r1 = df.ix[i, 'Average Agency Rating (t=0)']
        r2 = df.ix[i, 'Average Agency Rating (t=1)']
        if r1 != 'NR' and r2 != 'NR':
            rtm.load_case(r1, r2)

    # see RTM statistics
    print('# bonds that start at A2:', rtm.start_counts['A2'])
    print('transition probability from A2 to BBB1:', rtm.get_transition_prob('A2', 'BBB1'))
    print('downgrade probability of A2 bond:', rtm.get_dwngrade_prob('A2'))
    print('upgrade probability of A2 bond:', rtm.get_upgrade_prob('A2'))
    print('default probability of CC bond:', rtm.get_default_prob('CC'))
    print('expected notch change for B1 bond:', rtm.get_expctd_notch_chng('B1'))

    csv = rtm.get_transition_matrix()
    csv.to_csv('ratings_transition_matrix.csv')
    print('done with main()!')
    return rtm


if __name__ == '__main__':
    rtm = main()
