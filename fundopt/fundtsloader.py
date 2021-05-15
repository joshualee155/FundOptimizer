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

        requestDates = self.generate_request_dates(startDate, endDate)
        cachedDf, cachedDates = self.getLocalData( requestDates )

        self.IsAvailableForOptimisation = (len( cachedDates ) / len( requestDates )) > 0.9

        requestDf = cachedDf.reindex( requestDates, method = 'ffill' )
        requestDf.dropna( axis = 0, how = 'any', inplace = True )

        return requestDf

    def generate_request_dates(self, startDate, endDate):
        requestDates = pd.bdate_range( startDate, endDate )
        return requestDates

    def _postProcess(self):
        # if 'Unnamed: 0' in self._rawData:
        #     self._rawData.drop( columns = [ 'Unnamed: 0' ], inplace = True )
        self._rawData = self._rawData[ ~self._rawData.index.duplicated() ]
        self._rawData.sort_index( inplace = True )

    @abstractmethod
    def getReturnTS(self, start, end, offset):
        pass

    @abstractmethod
    def getReturnByDate(self, start, end):
        pass

    def getLocalData(self, dates):
        """
        Read from local cache, return the dataframe and available dates in the cache
        """

        localData = lib.read( self.fundCode, chunk_range = dates)
        localData = localData[ ~localData.index.duplicated() ]
        dates     = localData.index.tolist()

        return localData, dates

    @abstractmethod
    def writeLocalData(self):
        pass

    

class MMFundTimeSeriesLoader(FundTimeSeriesLoader):
        
    def generate_request_dates(self, startDate, endDate):
        requestDates = pd.date_range( startDate, endDate )
        return requestDates

    def getReturnByDate( self, start, end ):
        """get single return from start to end

        Args:
            start ([type]): start date, assumption: trading day
            end ([type]): end date: assumption: trading day

        Returns:
            float: return in decimal, e.g., 
                   if the daily profit (per 10000 units) is 1.5 flat, 
                   then return from day 1 to day 10 will be
                   1.5 x 10 / 10000 = 0.0015
        """
        if start > self._rawData.index.min() or end < self._rawData.index.max():
            self.load( start, end )
        return sum( self._rawData['dailyProfit'][start:end] )/10000.

    def getReturnTS( self, start, end, offset = 1 ):
        """Generate (overlapping) returns

        Args:
            start (datetime.date): start date, assumption: trading day
            end (datetime.date): end date, assumption: trading day
            offset (int, optional): holding period. Defaults to 1.

        Returns:
            pd.Series: return time series
        """
        if start > self._rawData.index.min() or end < self._rawData.index.max():
            self.load( start, end )
        
        if self.IsAvailableForTrading:
            daily = self._rawData['dailyProfit'].loc[start:end]
        
            starts = pd.bdate_range(start, end)
            ends = starts + pd.offsets.BDay(offset)

            return_calc = pd.DataFrame()
            return_calc['start'] = starts
            return_calc['end'] = ends
            return_calc = return_calc[return_calc['end'].dt.date<=end]

            results = []
            for row in return_calc.itertuples(index=False):
                results.append( daily.loc[row.start:row.end].sum()/10000. )

            return_calc['return'] = results
            return return_calc.set_index('end')['return'].reindex(starts)
        else:
            return None


class OpenFundTimeSeriesLoader(FundTimeSeriesLoader):
    
    def getReturnByDate( self, start, end ):
        if start > self._rawData.index.min() or end < self._rawData.index.max():
            self.load( start, end )
        startNAV, startAccNAV = self._rawData[ ['NAV', 'ACC_NAV'] ].loc[start]
        endAccNAV             = self._rawData[ 'ACC_NAV' ][end]
        
        return (endAccNAV - startAccNAV)/startNAV
    
    def getReturnTS( self, start, end, offset = 1  ):
        if start > self._rawData.index.min() or end < self._rawData.index.max():
            self.load( start, end )
        nav = self._rawData.loc[start:end]
        if self.IsAvailableForTrading: 
            startNAV    = nav[ 'NAV' ].shift(offset)
            startAccNAV = nav[ 'ACC_NAV' ].shift(offset)
            endAccNAV   = nav[ 'ACC_NAV' ]
        
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

def getTSLoader( fundCode ):
    fundType = getFundType(fundCode)
    if fundType == FundType.MMF:
        return MMFundTimeSeriesLoader(fundCode)
    else:
        return OpenFundTimeSeriesLoader(fundCode)

def _load_data(fund, start, end, holdingPeriod=1):
    logging.debug( "%s:Loading time series...", fund )
    try:
        tsLoader = getTSLoader(fund)
        tsLoader.load( start, end )
        ret = tsLoader.getReturnTS(start, end, holdingPeriod)
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