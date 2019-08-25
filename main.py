from   fundopt.fundopt import FundTargetRetMinCVaROptimiser
import pandas   as pd
import numpy    as np
import datetime as dt
import logging

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    startDate = dt.date(2017, 3, 1)
    endDate   = dt.date(2018, 3, 1)
    
    fundList  = np.genfromtxt( 'refData/AvailableFundList.txt', dtype = str )
    fundList  = fundList
    currentPosition = pd.Series( 0.0, index = fundList )
    currentPosition[ '000198' ] = 50000.0
    optimiser = FundTargetRetMinCVaROptimiser(
                                targetRet       = 0.10,
                                startDate       = startDate,
                                endDate         = endDate,
                                holdingPeriod   = 30,
                                longOnly        = True,
                                fundList        = currentPosition.index,
                                currentPosition = currentPosition, )
    optMovement = optimiser.getOptimalPosition(verbose = True)
    newPosition = np.squeeze( np.array( optimiser.currentPosition ) ) + optMovement

    currentDate  = dt.date(2018, 3, 9)
    initialValue = sum( newPosition )
    print( initialValue )
    currentValue = initialValue
    for fund, position in zip( optimiser.fundList, newPosition ):
        if position > 0.01:
            currentValue += position * optimiser.tsLoader[fund].getReturnByDate( StartDate = endDate, EndDate = currentDate )
    print( currentValue )
