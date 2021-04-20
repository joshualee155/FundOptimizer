import unittest
from   fundopt.fundtsloader import MMFundTimeSeriesLoader, OpenFundTimeSeriesLoader
import datetime as dt
import pandas as pd

class Test_testFundTSLoader(unittest.TestCase):
    def test_LoadFundData(self):

        StartDate       = dt.date(2018, 10, 19)
        EndDate         = dt.date(2019, 10, 22)
        HoldingPeriod   = 30
        # DateIndex       = pd.date_range( StartDate, EndDate, feq = 'D' )
        #YuEBao          = MMFundTimeSeriesLoader('000198').load()
        #YERet           = YuEBao.getReturnTS( StartDate, EndDate, HoldingPeriod)
        Shen            = OpenFundTimeSeriesLoader('001884')
        Shen.load( StartDate - pd.Timedelta( days = HoldingPeriod ), EndDate )
        ShenRet         = Shen.getReturnTS( StartDate, EndDate, HoldingPeriod )

if __name__ == '__main__':
    unittest.main()
