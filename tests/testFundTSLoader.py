import unittest
from   fundtsloader import MMFundTimeSeriesLoader, OpenFundTimeSeriesLoader
import datetime as dt
import pandas as pd

class Test_testFundTSLoader(unittest.TestCase):
    def test_LoadFundData(self):

        StartDate       = dt.date(2017, 1, 26)
        EndDate         = dt.date(2018, 1, 26)
        HoldingPeriod   = 30
        DateIndex       = pd.date_range( StartDate, EndDate, feq = 'D' )
        #YuEBao          = MMFundTimeSeriesLoader('000198').load()
        #YERet           = YuEBao.getReturnTS( StartDate, EndDate, HoldingPeriod)
        Shen            = OpenFundTimeSeriesLoader('160922').load( prefix = 'temp', index_col = 1, date_cols = ['Date'] )
        ShenRet         = Shen.getReturnTS( StartDate, EndDate, HoldingPeriod )

if __name__ == '__main__':
    unittest.main()
