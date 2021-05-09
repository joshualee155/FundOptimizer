from   fundopt.fundopt import FundTargetRetMinCVaROptimiser, FundTargetRetMinVarianceOptimiser
import pandas   as pd
import numpy    as np
import datetime as dt
import logging

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2014, 3, 23)
    end   = dt.date(2021, 4, 22)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))

    lookback = pd.bdate_range('2020-10-22', '2021-04-22')

    opt = FundTargetRetMinCVaROptimiser(
    # opt = FundTargetRetMinVarianceOptimiser(
        targetRet=0.14, 
        returns=fund_returns)

    currentPosition=pd.Series(0.0, index=fund_returns.columns)
    currentPosition['163406'] = 117108.0
    currentPosition['020005'] = 44499.0

    opt.getOptimalPosition(currentPosition, lookbackPeriod=lookback, verbose=True)