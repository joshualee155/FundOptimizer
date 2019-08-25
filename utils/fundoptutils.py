import pandas as pd
import datetime as dt

class FundType( object ):
    
    OF   = 'Open Ended Fund'
    ETF  = 'Exchange Traded Fund'
    LOF  = 'Listed Open Ended Fund'
    MMF  = 'Money Market Fund'

def getFundType( fundCode ):
    fundTypeDf = pd.read_csv( 'refData/fund_list.csv', names = [ 'fundCode', 'fundType' ] )
    fundTypeDf[ 'fundCode' ] = fundTypeDf[ 'fundCode' ].apply( lambda x: str(x).zfill(6) )
    fundTypeDf.drop_duplicates( subset = [ 'fundCode' ], inplace = True )
    fundTypeDf.set_index( 'fundCode', drop = True, inplace = True )
    try:
        sType = fundTypeDf[ 'fundType' ][ fundCode ]
        if sType == 'OF':
            return FundType.OF
        elif sType == 'ETF':
            return FundType.ETF
        elif sType == 'LOF':
            return FundType.LOF
        elif sType == 'MMF':
            return FundType.MMF
        else:
            raise NameError( "Unknown fund type %s" % sType )
    except KeyError:
        return FundType.OF

def str2date( sDate ):
    """
    Convert a string date to datetime.date
    """
    try:
        dateTime = dt.datetime.strptime( sDate, "%Y%m%d" )
    except ValueError:
        dateTime = dt.datetime.strptime( sDate, "%Y-%m-%d" )
    return dateTime.date()

def getHolidays( startDate, endDate ):
    """
    Return China exchange holidays ( non-trading days ) from `startDate` to `endDate`
    """

    with open( 'refData/holidays.txt', 'r' ) as f:
        holidays = f.read().strip().split('\n')

    holidays = [ date for date in map( str2date, holidays ) if date >= startDate and date <= endDate ]

    return holidays

