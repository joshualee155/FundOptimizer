from   fundopt.fundopt import FundTargetRetMinCVaROptimiser
import pandas   as pd
import datetime as dt
import logging
from fundopt.backtester import Backtester

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 21)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))
    fund_returns = fund_returns.drop(['003064'], axis=1)

    target_ret = 0.01

    opt = FundTargetRetMinCVaROptimiser(
        targetRet=target_ret, 
        returns=fund_returns,
        )

    start_position=pd.Series(dtype=float)
    start_position['000198'] = 117108.0
    start_position['020005'] = 44499.0

    backtest_start = dt.date(2020, 1, 29)
    backtest_end = end
    lookback_period = 120 # use 120 business days data (half year) to build optimization problem
    trading_period = 20 # rebalance every 20 business days
    backtester = Backtester(backtest_start, backtest_end, lookback_period, trading_period, opt)

    history = backtester.run(start_position)
    
    history.to_csv(f'backtest_{backtest_start}_{backtest_end}_{lookback_period}_{trading_period}_{target_ret}.csv')