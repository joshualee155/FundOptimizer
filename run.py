from   fundopt.fundopt import FundTargetRetMinCVaROptimiser, FundTargetRetMinVarianceOptimiser
import pandas   as pd
import numpy    as np
import datetime as dt
import logging
import cvxpy as cvx
from collections import namedtuple
import multiprocessing as mp
from functools import partial

RunSpec = namedtuple('RunSpec', 'start end target_ret')

runSpecs = [
    RunSpec('2020-07-09', '2021-07-09', 0.08),
    RunSpec('2020-07-09', '2021-07-09', 0.07),
    RunSpec('2020-07-09', '2021-07-09', 0.06),
    RunSpec('2020-07-09', '2021-07-09', 0.05),
    RunSpec('2020-07-09', '2021-07-09', 0.04),
    RunSpec('2020-07-09', '2021-07-09', 0.03),
    RunSpec('2020-07-09', '2021-07-09', 0.02),
    RunSpec('2020-07-09', '2021-07-09', 0.01),
    RunSpec('2021-01-01', '2021-07-09', 0.06),
    RunSpec('2021-01-01', '2021-07-09', 0.05),
    RunSpec('2021-01-01', '2021-07-09', 0.04),
    RunSpec('2021-01-01', '2021-07-09', 0.03),
    RunSpec('2021-01-01', '2021-07-09', 0.02),
    RunSpec('2021-01-01', '2021-07-09', 0.01),
]

solver_options = {
    'verbose' : False,
    'solver'  : cvx.SCIP,
}

_MULTIPROCESSING = True

def _run_with_spec(runSpec, fund_returns, current):

    lookback = pd.bdate_range(runSpec.start, runSpec.end)

    opt = FundTargetRetMinCVaROptimiser(
        targetRet=runSpec.target_ret, 
        returns=fund_returns)

    opt_trade = opt.getOptimalPosition(current, solver_options, lookbackPeriod=lookback)

    final = current.add(opt_trade, fill_value=0.0)

    print("-----------------------------------------")
    print(runSpec.start, runSpec.end, runSpec.target_ret, opt.cvxProblem.value)
    print("-----------------------------------------")
    print(final[final.abs()>1.0])
    print("-----------------------------------------")

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 7, 9)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))

    # fund_returns = fund_returns.drop(['003064'], axis=1)

    current=pd.Series(dtype=float)
    current['000198'] = 120000.0

    if _MULTIPROCESSING: 
        pool = mp.Pool(8)
        run_with_spec_bind = partial(_run_with_spec, fund_returns=fund_returns, current=current)
        r = pool.map(run_with_spec_bind, runSpecs)
    else:
        for runSpec in reversed(runSpecs):
            _run_with_spec(runSpec, fund_returns, current)
