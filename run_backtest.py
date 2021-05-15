from   fundopt.fundopt import FundTargetRetMinCVaROptimiser
from   fundopt.fundtsloader import getTSLoader
from   dateutil.relativedelta import relativedelta
import pandas   as pd
import numpy    as np
import datetime as dt
import logging

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


if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 7)
    holding = 20
    
    fund_returns = pd.read_pickle('./{}_{}_{}.pkl'.format(start, end, holding))
    fund_returns = fund_returns.drop('003064', axis=1)

    target_ret = 0.12
    sample_months = 6
    roll_months = 1

    opt = FundTargetRetMinCVaROptimiser(
        targetRet=target_ret, 
        returns=fund_returns,
        )

    startPosition=pd.Series(dtype=float)
    startPosition['000198'] = 117108.0
    startPosition['020005'] = 44499.0
    currentPosition = startPosition

    lookback_start = dt.date(2020, 1, 7)
    lookback_end = lookback_start + pd.offsets.BDay(120)


    while lookback_end <= end:
        # lookback = pd.bdate_range(lookback_start, lookback_end)
        # opt_trade = opt.getOptimalPosition(currentPosition, lookbackPeriod=lookback, verbose=False)

        # final = currentPosition.add(opt_trade, fill_value=0.0)
        final = currentPosition
        final = final[final > 0.1]

        next_start = lookback_start + pd.offsets.BDay(20)
        next_end = next_start + pd.offsets.BDay(120)

        currentPosition = roll_position(final, lookback_end, next_end)

        lookback_start = next_start
        lookback_end = next_end

        