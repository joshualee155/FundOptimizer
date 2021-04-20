import numpy as np
import pandas as pd
import datetime as dt
import os.path
from   abc import ABCMeta, abstractmethod
from   utils.fundoptutils import getFundType, FundType
from functools import partial
import multiprocessing as mp
import logging
from arctic import Arctic

a = Arctic( 'localhost' )
lib = a['fund']

class FundTimeSeriesLoader(object):


    __metaclass__ = ABCMeta

    _rawData  = None
    
    def __init__( self, fundCode ):
        self.fundCode = fundCode
        
    def load( self, startDate, endDate ):
        self._rawData   = self.getData( startDate, endDate )

        self._postProcess()
        # self._rawData   = self._rawData.resample('D').pad()

        self.IsAvailableForTrading = (self._rawData.index[-1].date() >= endDate)
        self.IsAvailableForOptimisation &= (self._rawData.index[0].date() <= startDate)

    def getData( self, startDate, endDate ):

        requestDates = pd.bdate_range( startDate, endDate )
        cachedDf, cachedDates = self.getLocalData( requestDates )

        # missingDates     = list( set( requestDates ).difference( cachedDates ) )
        # missingDfFromWeb = self.getDataFromWeb( missingDates )

        # aggregatedDf = pd.concat( ( cachedDf, missingDfFromWeb ) )
        # aggregatedDf = aggregatedDf[ ~aggregatedDf.index.duplicated() ]
        # aggregatedDf.sort_index( inplace = True )
        # if not missingDfFromWeb.empty:
        #     self.writeLocalData( aggregatedDf )

        self.IsAvailableForOptimisation = (len( cachedDates ) / len( requestDates )) > 0.9

        requestDf = cachedDf.reindex( requestDates, method = 'ffill' )
        requestDf.dropna( axis = 0, how = 'any', inplace = True )

        return requestDf

    def _postProcess(self):
        # if 'Unnamed: 0' in self._rawData:
        #     self._rawData.drop( columns = [ 'Unnamed: 0' ], inplace = True )
        self._rawData = self._rawData[ ~self._rawData.index.duplicated() ]
        self._rawData.sort_index( inplace = True )

    @abstractmethod
    def getReturnTS(self):
        pass

    def getLocalData(self, dates):
        """
        Read from local cache, return the dataframe and available dates in the cache
        """

        # localData = pd.read_csv( os.path.join(self.localPrefix, '%s.csv' % self.fundCode), 
        #                         index_col = 0, parse_dates = True,
        #                         )
        localData = lib.read( self.fundCode, chunk_range = dates)
        localData = localData[ ~localData.index.duplicated() ]
        dates     = localData.index.tolist()

        return localData, dates

    @abstractmethod
    def writeLocalData(self):
        pass

    

class MMFundTimeSeriesLoader(FundTimeSeriesLoader):
    
    
    def __init__( self, fundCode, prefix = 'temp' ):
        super(MMFundTimeSeriesLoader, self).__init__(fundCode)
        self.localPrefix = prefix

    def getReturnByDate( self, StartDate, EndDate ):
        
        if StartDate not in self._rawData or EndDate not in self._rawData:
            self.load( StartDate, EndDate )
        return sum( self._rawData['dailyProfit'][StartDate:EndDate] )/10000.

    def getReturnTS( self, offset = 1 ):
        if self.IsAvailableForTrading:
            return self._rawData['dailyProfit'] \
                        .rolling(window = offset, center = False).sum()/10000.
        else:
            return None


class OpenFundTimeSeriesLoader(FundTimeSeriesLoader):
    
    def __init__(self, fundCode, prefix = 'temp'):
        super(OpenFundTimeSeriesLoader, self).__init__(fundCode)
        self.localPrefix = prefix
        
    def getReturnByDate( self, StartDate = None, EndDate = None ):
        if StartDate not in self._rawData or EndDate not in self._rawData:
            self.load( StartDate, EndDate )
        startNAV, startAccNAV = self._rawData[ ['NAV', 'ACC_NAV'] ].loc[StartDate]
        endAccNAV             = self._rawData[ 'ACC_NAV' ][EndDate]
        
        return (endAccNAV - startAccNAV)/startNAV
    
    def getReturnTS( self, offset = 1  ):
        if self.IsAvailableForTrading: 
            startNAV    = self._rawData[ 'NAV' ].shift(offset)
            startAccNAV = self._rawData[ 'ACC_NAV' ].shift(offset)
            endAccNAV   = self._rawData[ 'ACC_NAV' ]
        
            return (endAccNAV - startAccNAV)/startNAV
        else:
            return None

    def _postProcess( self ):
        super( OpenFundTimeSeriesLoader, self )._postProcess()
        self._fillNaNinAccNAV()

    def _fillNaNinAccNAV( self ):
        '''
        Ugly tweaks to fill the nan hole in ACCUM_NAV. sometimes nan in ACCUM_NAV, no clue for reasons
        :return:
        '''
        inds = self._rawData['ACC_NAV'].isnull().to_numpy().nonzero()[0]

        for ind in inds:
            self._rawData.loc[ self._rawData.index[ind], 'ACC_NAV'] = self._rawData['NAV'].iloc[ind] \
                                                    - self._rawData['NAV'].iloc[ind-1] \
                                                    + self._rawData['ACC_NAV'].iloc[ind-1]
        return

def fundTSLoaderResolver( fundCode ):
    fundType = getFundType(fundCode)
    if fundType == FundType.MMF:
        return MMFundTimeSeriesLoader(fundCode)
    else:
        return OpenFundTimeSeriesLoader(fundCode)

def _load_data(fund, start, end, holdingPeriod=1):
    logging.debug( "%s:Loading time series...", fund )
    try:
        tsLoader = fundTSLoaderResolver(fund)
        tsLoader.load( start, end )
        ret = tsLoader.getReturnTS(holdingPeriod)
        if ret is not None:
            ret.name = fund
            return ret
    except Exception as e:
        logging.warning( "%s:Cannot load time series. Error: %s", fund, e )


def load_funds(funds, start, end, holdingPeriod=1):

    to_map = partial( _load_data, start=start, end=end, holdingPeriod=holdingPeriod )
    pool = mp.Pool(8)
    res = pool.map( to_map, funds )
    # pool.close()
    # pool.join()

    df = pd.concat(res, axis=1)
    return df.dropna(how='all')

if __name__ == '__main__':
    import matplotlib.pyplot as plt

    StartDate       = dt.date(2018, 10, 19)
    EndDate         = dt.date(2019, 10, 22)
    HoldingPeriod   = 30
    DateIndex       = pd.date_range( StartDate, EndDate, feq = 'D' )
    #YuEBao          = MMFundTimeSeriesLoader('000198').load( index_col = 4, date_cols = ['endDate', 'publishDate'] )
    #YERet           = YuEBao.getReturnTS( StartDate, EndDate, HoldingPeriod)
    import os
    fundList = [filename.split('.')[0] for filename in os.listdir( './temp' )]
    #with open( './refData/AvailableFundList.txt', 'w' ) as f:
    for fund in [ '001884' ]:
        Shen    = fundTSLoaderResolver( fund )
        ShenRet = Shen.getReturnTS( StartDate, EndDate, HoldingPeriod )
        if Shen.IsAvailableForTrading:
            print( fund )
    
    #plt.plot( DateIndex, ShenRet )
    #plt.show()