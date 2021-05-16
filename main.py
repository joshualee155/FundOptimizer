from   fundopt.fundopt import FundTargetRetMinCVaROptimiser, FundTargetRetMinVarianceOptimiser
import pandas   as pd
import numpy    as np
import datetime as dt
import logging

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 7)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))

    fund_returns.drop(['003064','502036', '512300', '510070'], axis=1, inplace=True)

    lookback = pd.bdate_range('2020-11-07', '2021-05-07')

    opt = FundTargetRetMinCVaROptimiser(
    # opt = FundTargetRetMinVarianceOptimiser(
        targetRet=0.12, 
        returns=fund_returns)

    currentPosition=pd.Series(dtype=float)
    currentPosition['163406'] = 117108.0
    currentPosition['020005'] = 44499.0

    opt.getOptimalPosition(currentPosition, lookbackPeriod=lookback, verbose=True)
