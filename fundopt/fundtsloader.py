import numpy as np
import pandas as pd
import datetime as dt
import os.path
from   abc import ABCMeta, abstractmethod
from   utils.fundoptutils import getFundType, FundType
from functools import partial
import multiprocessing as mp
import logging
from arctic import Arctic # pyright: reportMissingImports=false

a = Arctic( 'localhost' )
lib = a['fund']
lib_adj = a['fund_adj']

class FundTimeSeriesLoader(object):


    __metaclass__ = ABCMeta

    fund_nav  = None
    
    def __init__( self, fundCode ):
        self.fundCode = fundCode
        
    def load( self, startDate, endDate ):
        self.fund_nav   = self.getData( startDate, endDate )

        self._postProcess()
        # self._rawData   = self._rawData.resample('D').pad()

        self.is_available_for_trading = (self.fund_nav.index[-1].date() >= endDate)
        self.IsAvailableForOptimisation &= (self.fund_nav.index[0].date() <= startDate)

    def getData( self, startDate, endDate ):

        requestDates = self.generate_request_dates(startDate, endDate)
        cachedDf, cachedDates = self.getLocalData( requestDates )

        self.IsAvailableForOptimisation = (len( cachedDates ) / len( requestDates )) > 0.9

        # Do a forward fill but not the NaNs in the end
        requestDf = cachedDf.reindex( requestDates ).apply(lambda x: x.loc[:x.last_valid_index()].ffill())
        requestDf.dropna( axis = 0, how = 'any', inplace = True )

        return requestDf

    def generate_request_dates(self, startDate, endDate):
        requestDates = pd.bdate_range( startDate, endDate )
        return requestDates

    def _postProcess(self):
        # if 'Unnamed: 0' in self._rawData:
        #     self._rawData.drop( columns = [ 'Unnamed: 0' ], inplace = True )
        self.fund_nav = self.fund_nav[ ~self.fund_nav.index.duplicated() ]
        self.fund_nav.sort_index( inplace = True )

    @abstractmethod
    def getReturnTS(self, start, end, offset, back_test_mode):
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
        if start > self.fund_nav.index.min() or end < self.fund_nav.index.max():
            self.load( start, end )
        return sum( self.fund_nav['dailyProfit'][start:end] )/10000.

    def getReturnTS(self, start, end, offset=1, back_test_mode=False):
        """Generate (overlapping) returns

        Args:
            start (datetime.date): start date, assumption: trading day
            end (datetime.date): end date, assumption: trading day
            offset (int, optional): holding period. Defaults to 1.

        Returns:
            pd.Series: return time series
        """
        if self.is_available_for_trading or back_test_mode:
            if start > self.fund_nav.index.min() or end < self.fund_nav.index.max():
                self.load( start, end )
        
            daily = self.fund_nav['dailyProfit'].loc[start:end]
        
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

    fund_adj = None

    def load(self, start, end):
        super(OpenFundTimeSeriesLoader, self).load(start, end)
        self.load_fund_adj(start, end)

    def load_fund_adj(self, start, end):
        if lib_adj.has_symbol(self.fundCode):
            dates = self.generate_request_dates(start, end)
            self.fund_adj = lib_adj.read(self.fundCode, chunk_range=dates)
        else:
            self.fund_adj = pd.DataFrame()

    def getReturnByDate( self, start, end ):
        if start > self.fund_nav.index.min() or end < self.fund_nav.index.max() or self.fund_adj is None:
            self.load( start, end )

        adj = self.fund_adj.loc[start:end]
        nav = self.fund_nav.loc[start:end, 'NAV']
        for row in adj.itertuples(index=True):
            if row.type == 'div':
                nav[row.Index:] += row.amount
            elif row.type == 'split':
                nav[row.Index:] *= row.amount
            else:
                logging.error(f'Unknown adjustment type: {row.type}. Ingored.')

        ret = nav[end] / nav[start] - 1.0
        
        return ret

    def getReturnTS( self, start, end, offset=1, back_test_mode=False  ):

        if self.is_available_for_trading or back_test_mode:
            if start < self.fund_nav.index.min() or end > self.fund_nav.index.max():
                self.load( start, end )
            adj = self.fund_adj.loc[start:end]
            nav = self.fund_nav.loc[start:end, 'NAV']

            for row in adj.itertuples(index=True):
                if row.type == 'div':
                    nav[row.Index:] += row.amount
                elif row.type == 'split':
                    nav[row.Index:] *= row.amount
                else:
                    logging.error(f'Unknown adjustment type: {row.type}. Ingored.')

            start_nav = nav.shift(offset)
            end_nav = nav
            
            ret = (end_nav/start_nav) - 1
            
            return ret
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
        inds = self.fund_nav['ACC_NAV'].isnull().to_numpy().nonzero()[0]

        for ind in inds:
            self.fund_nav.loc[ self.fund_nav.index[ind], 'ACC_NAV'] = self.fund_nav['NAV'].iloc[ind] \
                                                    - self.fund_nav['NAV'].iloc[ind-1] \
                                                    + self.fund_nav['ACC_NAV'].iloc[ind-1]
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