from cvxpy.reductions.solvers import solver
from   fundopt.fundopt import FundTargetRetMinCVaROptimiser, FundTargetRetMinVarianceOptimiser
import pandas   as pd
import numpy    as np
import datetime as dt
import logging
import cvxpy as cvx

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 9, 30)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))

    fund_returns.drop(['001487'], axis=1, inplace=True)

    lookback = pd.bdate_range('2021-03-30', '2021-09-30')

    print( f"Look back period from {lookback.min()} to {lookback.max()}" )
    print( "Top 5 high return funds:")
    print( fund_returns.reindex(lookback).fillna(0.0).mean(axis=0).sort_values(ascending=False).head() )

    opt = FundTargetRetMinCVaROptimiser(
        targetRet=0.05, 
        returns=fund_returns)

    current=pd.Series(dtype=float)
    # current['001487'] = 68715.0
    current['001951'] = 66926.0
    current['502023'] = 41883.0
    current['000198'] = 120000.0

    solver_options = { 
        'verbose' : True,
        'solver'  : cvx.CBC,
        # 'scipy_options' : {
        #     'method' : 'highs',
        #     'disp' : True,
        # }
     }
    opt.getOptimalPosition(current, lookbackPeriod=lookback, solver_options=solver_options)

    print(opt.targetRet.value, opt.cvxProblem.value)
    
    # for target_ret in [0.055, 0.05, 0.04, 0.03, 0.02, 0.01]:
    #     opt.set_target_ret_and_rerun(target_ret, solver_options)
    #     print(opt.targetRet.value, opt.cvxProblem.value)

    # final = current.add(opt_trade, fill_value=0.0)
