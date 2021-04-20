import os.path
import pandas as pd
import cvxpy as cvx

__all__ = ['fundTransactionCost']

DEFAULT_BUYCOST  = 0.12
DEFAULT_SELLCOST = 0.5
NORMAL_FACTOR    = 100.

defaultTxnCost = lambda _x : cvx.scalene(_x, DEFAULT_BUYCOST/NORMAL_FACTOR, DEFAULT_SELLCOST/NORMAL_FACTOR)

class FundTransactionCostLoader(object):

    def __init__(self, prefix = 'refData'):
        
        df = pd.read_csv( os.path.join( prefix, 'TxnCost.txt' ), sep = ' ', header = None, names = ['Code', 'BuyCost', 'SellCost'] )
        
        df[ 'Code' ] = df['Code'].astype(str).str.zfill(6)

        #print buyCosts, sellCosts
        self._txnCosts = dict()
        for fundCode, buyCost, sellCost in df.itertuples( index = False ):
            self._txnCosts[fundCode] = lambda _x, b=buyCost, s=sellCost : cvx.scalene(_x, b/NORMAL_FACTOR, s/NORMAL_FACTOR)

    def getTxnCost(self, fundCode):
        cost = self._txnCosts.get( fundCode )
        if cost is None:
            return defaultTxnCost
        else:
            return cost

# Only export this
fundTransactionCost = FundTransactionCostLoader()