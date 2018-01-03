import collections
import pandas as pd
import numpy as np
import timeit
import datetime
import urllib.parse
from sqlalchemy import create_engine


class RatingsTransitionMatrix():
    def __init__(self):
        # dictionary from alphanumeric to numeric rating
        self.ratings_map = {'AAA': 21,
                            'AA1': 20, 'AA2': 19, 'AA3': 18,
                            'A1': 17, 'A2': 16, 'A3': 15,
                            'BBB1': 14, 'BBB2': 13, 'BBB3': 12,
                            'BB1': 11, 'BB2': 10, 'BB3': 9,
                            'B1': 8, 'B2': 7, 'B3': 6,
                            'CCC1': 5, 'CCC2': 4, 'CCC3': 3, 'CC': 2, 'C': 1,
                            'D': 0}
        # dictionary from numeric to alphanumeric rating
        self.ratings_map_inverse = {v: k for k, v in self.ratings_map.items()}

        # 1. track the number of issues transitioning from one rating to another
        # dictionary of dictionaries with rating transition **counts**.
        # Ex: dict['A1']['BBB1'] is the number of cases where ratings went from A1 to BBB1
        self.transition_dict = {r: collections.defaultdict(int) for r in self.ratings_map.keys()}

        # 2. track the sum of market value transitioning from one rating to another
        # dictionary of dictionary with rating transitions by market value

        # 3. track the weighted average oas
        self.oas_change_dict = {r: collections.defaultdict(float) for r in self.ratings_map.keys()}

        # dictionary with the number of times a bond started with rating X
        self.start_counts = {r: 0.0 for r in self.ratings_map.keys()}

    def load_case(self, start_rating, end_rating):

        # 1. add to the ratings transition matrix
        self.transition_dict[start_rating][end_rating] += 1

        # add to total count of cases that start with a given rating
        self.start_counts[start_rating] += 1

    def load_rtm(self, data):
        for i in range(data.shape[0]):
            r1 = data.loc[i, 'average_rating_0']
            r2 = data.loc[i, 'average_rating_1']
            if (r1 != 'NR') and (r2 != 'NR'):
                self.load_case(r1, r2)
        return None

    def load_oas_change_matrix(self, data):

        # make a copy of the data
        temp = data[['cusip', 'mkt_val', 'average_rating_0', 'average_rating_1', 'oas_0', 'oas_1', 'oas_change']].copy()
        mask1 = temp['average_rating_0'] != 'NR'
        mask2 = temp['average_rating_1'] != 'NR'
        mask3 = temp['oas_change'].notnull()
        temp = temp[mask1 & mask2 & mask3]

        # calc the weighted oas change for each rating transition
        temp['wghtd_oas_change'] = temp['mkt_val'] * temp['oas_change']
        top = temp.groupby(by=['average_rating_0', 'average_rating_1'])['wghtd_oas_change'].sum()
        bottom = temp.groupby(by=['average_rating_0', 'average_rating_1'])['mkt_val'].sum()
        wghtd_changes = top / bottom

        # convert to dataframe with columns average_rating_0, average_rating_1, wghtd_oas_change
        wghtd_changes = wghtd_changes.to_frame('wghtd_oas_change')
        wghtd_changes.reset_index(inplace=True, drop=False)
        wghtd_changes

        # load into a dictionary
        # iterate through each row of the dataframe and load info
        for i in range(wghtd_changes.shape[0]):
            r1 = wghtd_changes.loc[i, 'average_rating_0']
            r2 = wghtd_changes.loc[i, 'average_rating_1']
            val = wghtd_changes.loc[i, 'wghtd_oas_change']
            if (r1 != 'NR') and (r2 != 'NR'):
                self.oas_change_dict[r1][r2] = val
        return None

    def get_transition_prob(self, start_rating, end_rating):
        try:
            return self.transition_dict[start_rating][end_rating] / self.start_counts[start_rating]
        except ZeroDivisionError:
            # return 'Error - divide by zero error. There are no cases with a starting rating of {}'.format(start_rating)
            return np.NaN
        except:
            return 'unknown problem'

    def get_upgrade_prob(self, start_rating):
        if self.start_counts[start_rating] == 0:
            return "Sorry, can't calc upgrade prob. No cases with start rating of {}".format(start_rating)
        else:
            numeric_rating = self.ratings_map[start_rating]
            tot_prob = 0.0
            for i in range(numeric_rating + 1, 22):
                tot_prob += self.get_transition_prob(start_rating, self.ratings_map_inverse[i])
            return tot_prob

    def get_dwngrade_prob(self, start_rating):
        if self.start_counts[start_rating] == 0:
            return "Sorry, can't calc downgrade prob. No cases with start rating of {}".format(start_rating)
        else:
            numeric_rating = self.ratings_map[start_rating]
            tot_prob = 0.0
            for i in range(numeric_rating):
                tot_prob += self.get_transition_prob(start_rating, self.ratings_map_inverse[i])
            return tot_prob

    def get_default_prob(self, start_rating):
        return self.get_transition_prob(start_rating, 'D')

    def get_expctd_notch_chng(self, start_rating):
        if self.start_counts[start_rating] == 0:
            return "Sorry, can't calc expected notch change. No cases with start rating of {}".format(start_rating)
        else:
            numeric_rating = self.ratings_map[start_rating]
            wghtd_sum = 0.0  # the weighted average notch change

            # iterate over all possible end ratings
            for i in range(0, 22):
                end_rating = self.ratings_map_inverse[i]  # get the alphanumeric of the end rating
                notch_diff = i - numeric_rating  # get the notches diff between end and start rating
                end_rating_count = self.transition_dict[start_rating][
                    end_rating]  # get the number of times the rating transitioned from start to end
                wght = end_rating_count / self.start_counts[start_rating]
                wghtd_sum += (notch_diff * wght)

            return wghtd_sum

    def get_transition_matrix_1(self, csv=False):
        '''
        transition probabilities
        '''
        df = pd.DataFrame()
        df['Start'] = [self.ratings_map_inverse[x] for x in sorted(self.ratings_map_inverse.keys(), reverse=False)]
        df['Count'] = [self.start_counts[self.ratings_map_inverse[i]] for i in range(0, 22)]
        for end_rating_numeric in range(21, -1, -1):
            df[self.ratings_map_inverse[end_rating_numeric]] = \
                [self.get_transition_prob(self.ratings_map_inverse[i], self.ratings_map_inverse[end_rating_numeric]) for
                 i in range(0, 22)]
        df.sort_index(ascending=False, inplace=True)
        if csv:
            df.to_csv('ratings_transition_matrix.csv')
        return df

    def get_transition_matrix_2(self, csv=False):
        '''
        transitions by bond count
        '''
        df = pd.DataFrame()
        df['Start'] = [self.ratings_map_inverse[x] for x in sorted(self.ratings_map_inverse.keys(), reverse=False)]
        df['Count'] = [self.start_counts[self.ratings_map_inverse[i]] for i in range(0, 22)]
        for end_rating_numeric in range(21, -1, -1):
            df[self.ratings_map_inverse[end_rating_numeric]] = \
                [self.transition_dict[self.ratings_map_inverse[i]][self.ratings_map_inverse[end_rating_numeric]] for i
                 in range(0, 22)]
        df.sort_index(ascending=False, inplace=True)
        if csv:
            df.to_csv('ratings_transition_matrix.csv')
        return df

    def get_transition_matrix_3(self, csv=False):
        '''
        weighted-average oas changes
        '''
        df = pd.DataFrame()
        df['Start'] = [self.ratings_map_inverse[x] for x in sorted(self.ratings_map_inverse.keys(), reverse=False)]
        df['Count'] = [self.start_counts[self.ratings_map_inverse[i]] for i in range(0, 22)]
        for end_rating_numeric in range(21, -1, -1):
            df[self.ratings_map_inverse[end_rating_numeric]] = \
                [self.oas_change_dict[self.ratings_map_inverse[i]][self.ratings_map_inverse[end_rating_numeric]] for i
                 in range(0, 22)]
        df.sort_index(ascending=False, inplace=True)
        if csv:
            df.to_csv('ratings_transition_matrix.csv')
        return df


