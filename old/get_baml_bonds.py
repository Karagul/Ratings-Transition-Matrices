
import pandas as pd
import numpy as np

def get_baml_bonds(verbose = False):

    if verbose:
        print('start get_baml_bonds()...')

    baml_bonds = pd.read_stata('D:\\Global Bank CSR\\Input\\all_baml_bonds.dta')

    # rename column headers
    baml_bonds.rename(columns = {'indexDate': 'BAML Index Date', 'indexName': 'BAML Index', 'ISIN': 'Isin',
                                'Description': 'BAML Name', 'Ticker': 'BAML Ticker', 'Country': 'BAML Country',
                                'SectorLevel3': 'BAML Sector 3', 'SectorLevel4': 'BAML Sector 4'}, inplace = True)

    # identify cusips that are in quotes
    quoted = baml_bonds['Cusip'].map(lambda x: x[0]) == "'"

    # clean cusips that are in quotes
    baml_bonds.loc[quoted, 'Cusip'] = baml_bonds['Cusip'].map(lambda x: x[1:-1])

    return baml_bonds