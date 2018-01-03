import collections
import pandas as pd
import numpy as np

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
        self.ratings_map_inverse = {v: k for k,v in self.ratings_map.items()}

        # dictionary of dictionaries with rating transition counts. Ex: dict['A1']['BBB1'] is the number of cases where ratings went from A1 to BBB1
        self.transition_dict = { r: collections.defaultdict(int) for r in self.ratings_map.keys()}

        self.start_counts = { r: 0.0 for r in self.ratings_map.keys()}

    def load_case(self, start_rating, end_rating):

        # 1. add to the ratings transition matrix
        self.transition_dict[start_rating][end_rating] += 1

        # add to total count of cases that start with a given rating
        self.start_counts[start_rating] += 1

    def get_transition_prob(self, start_rating, end_rating):
        try:
            return self.transition_dict[start_rating][end_rating] / self.start_counts[start_rating]
        except ZeroDivisionError:
            #return 'Error - divide by zero error. There are no cases with a starting rating of {}'.format(start_rating)
            return np.NaN
        except:
            return 'unknown problem'

    def get_upgrade_prob(self, start_rating):
        if self.start_counts[start_rating] == 0:
            return "Sorry, can't calc upgrade prob. No cases with start rating of {}".format(start_rating)
        else:
            numeric_rating = self.ratings_map[start_rating]
            tot_prob = 0.0
            for i in range(numeric_rating +1, 22):
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

            #iterate over all possible end ratings
            for i in range(0,22):
                end_rating = self.ratings_map_inverse[i] # get the alphanumeric of the end rating
                notch_diff = i - numeric_rating          # get the notches diff between end and start rating
                end_rating_count = self.transition_dict[start_rating][end_rating]  # get the number of times the rating transitioned from start to end
                wght = end_rating_count / self.start_counts[start_rating]
                wghtd_sum += (notch_diff * wght)

            return wghtd_sum

    def get_transition_matrix(self):
        df = pd.DataFrame()
        df['Start']  = [self.ratings_map_inverse[x] for x in sorted(self.ratings_map_inverse.keys(), reverse = False) ]
        for end_rating_numeric in range(21,-1,-1):
            df[self.ratings_map_inverse[end_rating_numeric]] = [self.get_transition_prob(self.ratings_map_inverse[i], self.ratings_map_inverse[end_rating_numeric]) for i in range(0,22)]
        df.sort_index(ascending = False, inplace = True)
        return df

