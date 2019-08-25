import unittest
from   fundopt.fundopt import FundTargetRetMinCVaROptimiser
import numpy as np
import pandas as  pd
import datetime as dt
import os

class Test_testFundOptimiser(unittest.TestCase):

    def test_Optimise(self):
        startDate = dt.date(2017, 1, 26)
        endDate   = dt.date(2018, 1, 26)

        DateIndex = pd.date_range( startDate, endDate, freq='D' )
        fundList  = np.genfromtxt( './refData/AvailableFundList.txt', dtype = str )
        fundList  = fundList[:10]
        currentPosition = [0.0]*len(fundList)
        currentPosition[0] = 50000.0
        kwargs = { 'prefix' : 'temp', 'index_col' : 1, 'date_cols' : ['Date'] }
        optimiser = FundTargetRetMinCVaROptimiser(
                                    targetRet       = 0.005,
                                    startDate       = startDate,
                                    endDate         = endDate,
                                    holdingPeriod   = 30,
                                    longOnly        = True,
                                    fundList        = fundList,
                                    currentPosition = currentPosition, 
                                    **kwargs)
        optMovement = optimiser.getOptimalPosition(verbose = True)
        newPosition = np.array( currentPosition ) + optMovement

        currentDate  = dt.date(2018, 2, 1)
        initialValue = sum( newPosition )
        print( initialValue )
        currentValue = initialValue
        for fund, position in zip( fundList, newPosition ):
            currentValue += position * optimiser.tsLoader[fund].getReturnByDate( StartDate = endDate, EndDate = currentDate )
        print( currentValue )

if __name__ == '__main__':
    unittest.main()
