from   fundopt.fundopt import FundTargetRetMinCVaROptimiser
from   fundopt.fundtsloader import getTSLoader
from   dateutil.relativedelta import relativedelta
import pandas   as pd
import numpy    as np
import datetime as dt
import logging
import cvxpy as cvx
from collections import namedtuple

solver_options = {
    'verbose' : True, 
    'max_iters' : 400,
    'solver'  : cvx.GLPK,
}

def roll_position(position, start, end):
    """Roll fund position from start to end

    Args:
        position (pd.Series): fund position at start 
        start (datetime.date): start date
        end (datetime.date): end date
    """
    final_position = position.copy()
    for fund_code, value in position.iteritems():
        fund_ts_loader = getTSLoader(fund_code)
        fund_ts_loader.load(start, end)
        fund_ret = fund_ts_loader.getReturnByDate(start, end)
        final_position[fund_code] *= (1+fund_ret)

    return final_position

BacktestSpec = namedtuple('BacktestSpec', 'lookback_start lookback_end rolling_end')

def generate_backtest_sequence(start, end, lookback_period, trading_period):
    
    lookback_start = start
    lookback_end = lookback_start + pd.offsets.BDay(lookback_period)

    next_start = lookback_start + pd.offsets.BDay(trading_period)
    next_end = next_start + pd.offsets.BDay(lookback_period)

    sequence = []
    while next_end <= end:
        sequence.append( BacktestSpec( lookback_start, lookback_end, next_end ) )

        lookback_start = next_start
        lookback_end = lookback_start + pd.offsets.BDay(lookback_period)

        next_start = lookback_start + pd.offsets.BDay(trading_period)
        next_end = next_start + pd.offsets.BDay(lookback_period)

    return sequence        

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 7)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))
    fund_returns = fund_returns.drop(['003064','502036', '512300', '510070'], axis=1)

    target_ret = 0.06

    opt = FundTargetRetMinCVaROptimiser(
        targetRet=target_ret, 
        returns=fund_returns,
        )

    startPosition=pd.Series(dtype=float)
    startPosition['000198'] = 117108.0
    startPosition['020005'] = 44499.0
    currentPosition = startPosition

    backtest_start = dt.date(2020, 2, 7)
    backtest_end = end
    lookback_period = 120 # use 120 business days data (half year) to build optimization problem
    trading_period = 20 # rebalance every 20 business days
    sequence = generate_backtest_sequence(backtest_start, backtest_end, lookback_period, trading_period)

    positions = []
    for lookback_start, lookback_end, rolling_end in sequence:

        positions.append(currentPosition.to_frame().T.assign(date=lookback_end))
        
        lookback = pd.bdate_range(lookback_start, lookback_end)
        
        opt_trade = opt.getOptimalPosition(currentPosition, solver_options, lookbackPeriod=lookback)

        final = currentPosition.add(opt_trade, fill_value=0.0)
        final = final[final > 0.1]

        currentPosition = roll_position(final, lookback_end, rolling_end)
    positions.append(currentPosition.to_frame().T.assign(date=rolling_end))
    history = pd.concat(positions).set_index('date').fillna(0.0)
    
    history.to_csv(f'backtest_{backtest_start}_{backtest_end}_{lookback_period}_{trading_period}_{target_ret}.csv')
        