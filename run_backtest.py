from   fundopt.fundopt import FundTargetRetMinCVaROptimiser
import pandas   as pd
import datetime as dt
import logging
from fundopt.backtester import Backtester
import multiprocessing as mp
from functools import partial

_MULTI_PROCESS = True

def _backtest_with_target_ret(target_ret, backtest_start, backtest_end, fund_returns, start_position):

    opt = FundTargetRetMinCVaROptimiser(
            targetRet=target_ret, 
            returns=fund_returns,
            )
        
    opt.targetRet.value = target_ret

    lookback_period = 120 # use 120 business days data (half year) to build optimization problem
    trading_period = 20 # rebalance every 20 business days
    backtester = Backtester(backtest_start, backtest_end, lookback_period, trading_period, opt)

    history = backtester.run(start_position)
    history.to_csv(f'backtest_{backtest_start}_{backtest_end}_{lookback_period}_{trading_period}_{target_ret}.csv')

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 9, 30)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))
    current=pd.Series(dtype=float)
    current['000198'] = 120000.0
    
    target_returns = [0.07, 0.08, 0.09, 0.10]

    if _MULTI_PROCESS:
        pool = mp.Pool(8)
        backtest_bind = partial(_backtest_with_target_ret, backtest_start=start, backtest_end=end, fund_returns=fund_returns, start_position=current)
        r = pool.map(backtest_bind, target_returns)
    else:
        for target_ret in reversed(target_returns):
            _backtest_with_target_ret(target_ret, start, end, fund_returns, current)
