from   fundopt.fundopt import FundTargetRetMinCVaROptimiser, FundTargetRetMinVarianceOptimiser
import pandas   as pd
import numpy    as np
import datetime as dt
import logging
import cvxpy as cvx
from collections import namedtuple

RunSpec = namedtuple('RunSpec', 'start end target_ret')

runSpecs = [
    # RunSpec('2020-05-07', '2021-05-07', 0.16),
    # RunSpec('2020-05-07', '2021-05-07', 0.14),
    # RunSpec('2020-05-07', '2021-05-07', 0.12),
    # RunSpec('2020-11-07', '2021-05-07', 0.16),
    # RunSpec('2020-11-07', '2021-05-07', 0.14),
    # RunSpec('2020-11-07', '2021-05-07', 0.12),
    # RunSpec('2020-11-07', '2021-05-07', 0.10),
    # RunSpec('2020-11-07', '2021-05-07', 0.08),
    RunSpec('2020-11-14', '2021-05-14', 0.06),
    RunSpec('2020-11-14', '2021-05-14', 0.05),
    RunSpec('2020-11-14', '2021-05-14', 0.04),
    RunSpec('2020-11-14', '2021-05-14', 0.03),
    RunSpec('2020-11-14', '2021-05-14', 0.02),
    RunSpec('2020-11-14', '2021-05-14', 0.01),
]

solver_options = {
    'verbose' : False,
    'solver'  : cvx.GLPK,
}

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 14)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))

    fund_returns = fund_returns.drop(['003064'], axis=1)

    currentPosition=pd.Series(dtype=float)
    currentPosition['163406'] = 117108.0
    currentPosition['020005'] = 44499.0

    for runSpec in reversed(runSpecs):

        lookback = pd.bdate_range(runSpec.start, runSpec.end)

        opt = FundTargetRetMinCVaROptimiser(
            targetRet=runSpec.target_ret, 
            returns=fund_returns)

        opt_trade = opt.getOptimalPosition(currentPosition, solver_options, lookbackPeriod=lookback)

        final = currentPosition.add(opt_trade, fill_value=0.0)

        print(runSpec.target_ret, opt.cvxProblem.value)
        print(final[final.abs()>0.1])
