import os.path
import numpy as np
import cvxpy as cvx
from collections import defaultdict

DEFAULT_BUYCOST  = 0.12
DEFAULT_SELLCOST = 0.5
NORMAL_FACTOR    = 100.

defaultTxnCost = lambda _x : cvx.scalene(_x, DEFAULT_BUYCOST/NORMAL_FACTOR, DEFAULT_SELLCOST/NORMAL_FACTOR)

class FundTransactionCostLoader(object):


    def __init__(self, prefix = 'refData'):
        fundCodes, buyCosts, sellCosts = [np.array(_) for _ in zip(*np.genfromtxt( os.path.join(prefix, 'TxnCost.txt'),
                                                        unpack=True,
                                                        dtype=[('code', 'S6'), ('buyRates', 'f8'), ('sellRates', 'f8')]
                                                        ))]
        buyCosts[np.isnan(buyCosts)]   = DEFAULT_BUYCOST
        sellCosts[np.isnan(sellCosts)] = DEFAULT_SELLCOST
        #print buyCosts, sellCosts
        self._txnCosts = defaultdict( lambda : defaultTxnCost )
        for fundCode, buyCost, sellCost in zip( fundCodes, buyCosts, sellCosts ):
            self._txnCosts[fundCode] = lambda _x : cvx.scalene(_x, buyCost/NORMAL_FACTOR, sellCost/NORMAL_FACTOR)

    def getTxnCost(self, fundCode):
        return self._txnCosts[fundCode]

if __name__ == "__main__":
    txnCostLoader = FundTransactionCostLoader()
    fundcode  = '001550'
    x = cvx.Variable()
    print( (txnCostLoader.getTxnCost(fundcode)(x)) <= 0 )