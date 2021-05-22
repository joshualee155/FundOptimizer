from   fundopt.fundopt import FundTargetRetMinCVaROptimiser, FundTargetRetMinVarianceOptimiser
import pandas   as pd
import numpy    as np
import datetime as dt
import logging
import cvxpy as cvx

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 21)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))

    fund_returns.drop(['003064'], axis=1, inplace=True)

    lookback = pd.bdate_range('2020-11-21', '2021-05-21')

    print( f"Look back period from {lookback.min()} to {lookback.max()}" )
    print( "Top 5 high return funds:")
    print( fund_returns.reindex(lookback).fillna(0.0).mean(axis=0).sort_values(ascending=False).head() )

    opt = FundTargetRetMinCVaROptimiser(
        targetRet=0.05, 
        returns=fund_returns)

    currentPosition=pd.Series(dtype=float)
    currentPosition['163406'] = 117108.0
    currentPosition['020005'] = 44499.0

    solver_options = { 
        'verbose' : True,
        'solver'  : cvx.GLPK,
     }
    opt.getOptimalPosition(currentPosition, lookbackPeriod=lookback, solver_options=solver_options)
