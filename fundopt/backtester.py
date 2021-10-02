from collections import namedtuple
from fundopt.fundtsloader import getTSLoader

import cvxpy
import pandas as pd

BacktestSpec = namedtuple('BacktestSpec', 'lookback_start lookback_end rolling_end')

class Backtester:

    solver_options = {
        'verbose' : False, 
        'max_iters' : 400,
        'solver'  : cvxpy.GLPK,
    }

    def __init__(self, backtest_start, backtest_end, lookback_period, trading_period, opt):
        self.backtest_start = backtest_start
        self.backtest_end = backtest_end
        self.lookback_period = lookback_period
        self.trading_period = trading_period
        self.opt = opt

    def generate_backtest_sequence(self):
        """Generate a list of back test specs from
            - lookback period
            - trading period
            - backtest start & end
            rolled backwards from end to start

        Returns:
            List[BacktestSpec]: list of backtest specs
        """
        rolling_end = self.backtest_end
        lookback_end = rolling_end - pd.offsets.BDay(self.trading_period)
        lookback_start = lookback_end - pd.offsets.BDay(self.lookback_period)

        sequence = []
        while lookback_start >= self.backtest_start:
            sequence.append( BacktestSpec( lookback_start, lookback_end, rolling_end ) )

            rolling_end = rolling_end - pd.offsets.BDay(self.trading_period)
            lookback_end = rolling_end - pd.offsets.BDay(self.trading_period)
            lookback_start = lookback_end - pd.offsets.BDay(self.lookback_period)

        return sequence[::-1]

    def roll_position(self, position, start, end):
        """Roll fund position from start to end

        Args:
            position (pd.Series): fund position at start 
            start (datetime.date): start date
            end (datetime.date): end date
        """
        roll_dates = pd.bdate_range(start+pd.offsets.BDay(1), end)
        final_position = pd.DataFrame(columns=position.index)
        for fund_code, value in position.iteritems():
            fund_ts_loader = getTSLoader(fund_code)
            fund_ts_loader.load(start, end)
            ret_ts = fund_ts_loader.getReturnTS(start, end, back_test_mode=True)
            ret_ts = ret_ts.reindex(roll_dates).fillna(0.0)
            final_position[fund_code] = value*(1+ret_ts).cumprod()

        return final_position

    def run(self, start_position):
        current = start_position

        sequence = self.generate_backtest_sequence()

        # positions = [current]
        positions = []
        for lookback_start, lookback_end, rolling_end in sequence:
            
            lookback = pd.bdate_range(lookback_start, lookback_end)
            
            opt_trade = self.opt.getOptimalPosition(current, self.solver_options, lookbackPeriod=lookback)

            final = current.add(opt_trade, fill_value=0.0)
            final = final[final > 0.1]
            # final = current

            rolled_positions = self.roll_position(final, lookback_end, rolling_end)

            positions.append(rolled_positions)

            current = rolled_positions.iloc[-1]

        history = pd.concat(positions).fillna(0.0)

        return history