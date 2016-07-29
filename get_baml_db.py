import pandas as pd
import numpy as np

def get_baml_db(db = 'Z:\BAML Database\From Philly\STRATEGYDB.db', region = 'NA', date = '2012-12-31', verbose = False):
    '''
    query an sqlite database for all bond cusip/isins  that belong to specified indices
    '''

    if region not in ['NA', 'EU']:
        return 'unrecognized region argument. Permissible values are NA and EUR'

    # connect to sqlite db
    import sqlite3
    import pandas as pd
    if verbose:
        print('connect to db...')
    conn = sqlite3.connect(db)
    c = conn.cursor()

    # set query
    query_region = ('C0A0', 'H0A0')
    if region == 'EU':
        query_region = ('ER00', 'HE00')

    #query = "SELECT COUNT(Composite_Rating) AS count, Composite_Rating FROM 'ALL' WHERE INDEX_ID IN {} AND Extraction_date2 = '2012-12-31' GROUP BY Composite_Rating;".format(query_region)
    #query = "SELECT Composite_Rating, COUNT(Composite_Rating) as count FROM 'ALL' WHERE INDEX_ID IN {} AND Extraction_date2 = '{}' GROUP BY Composite_Rating".format(query_region, date)
    #query = "SELECT AVG(Price) FROM 'ALL' WHERE INDEX_ID = 'H0A0'"
    query = "SELECT Cusip, ISIN FROM 'ALL' WHERE INDEX_ID IN {} AND Extraction_date2 = '{}'".format(query_region, date)
    if verbose:
        print('query is', query)

    # execute query
    if verbose:
        print('execute query...')
    c.execute(query)
    rows = c.fetchall()
    df = pd.DataFrame(rows)
    df.rename(columns = {0: 'Cusip', 1: 'ISIN'}, inplace = True)
    return df
