import numpy as np
import pandas as pd
import datetime as dt
import os.path
from   abc import ABCMeta, abstractmethod
from   utils.fundoptutils import getFundType, FundType, getHolidays
from   fundopt.openfundtsloadermixin import OpenFundTSLoaderSinaMixin
from   fundopt.moneymarketfundtsloadermixin import MoneyMarketFundTSLoaderSinaMixin

class FundTimeSeriesLoader(object):


    __metaclass__ = ABCMeta

    _rawData  = None
    
    def __init__( self, fundCode ):
        self.fundCode = fundCode
        
    def load( self, startDate, endDate ):
        self._rawData   = self.getData( startDate, endDate )

        self._postProcess()
        self._rawData   = self._rawData.resample('D').pad()

        self.IsAvailableForTrading = (self._rawData.index[-1].date() >= endDate)
        self.IsAvailableForOptimisation = (self._rawData.index[0].date() <= startDate)

    def getData( self, startDate, endDate ):

        requestDates = pd.date_range( startDate, endDate ).tolist()
        cachedDf, cachedDates = self.getLocalData()

        missingDates     = list( set( requestDates ).difference( cachedDates ) )
        missingDfFromWeb = self.getDataFromWeb( missingDates )

        aggregatedDf = pd.concat( ( cachedDf, missingDfFromWeb ) )
        aggregatedDf = aggregatedDf[ ~aggregatedDf.index.duplicated() ]
        aggregatedDf.sort_index( inplace = True )
        if not missingDfFromWeb.empty:
            self.writeLocalData( aggregatedDf )
        requestDf = aggregatedDf.reindex( requestDates, method = 'ffill' )
        requestDf.dropna( axis = 0, how = 'any', inplace = True )

        return requestDf

    def _postProcess(self):
        if 'Unnamed: 0' in self._rawData:
            self._rawData.drop( columns = [ 'Unnamed: 0' ], inplace = True )
        self._rawData = self._rawData[ ~self._rawData.index.duplicated() ]
        self._rawData.sort_index( inplace = True )

    @abstractmethod
    def getReturnTS(self):
        pass

class MMFundTimeSeriesLoader(FundTimeSeriesLoader, MoneyMarketFundTSLoaderSinaMixin):
    
    
    def __init__( self, fundCode, prefix = 'temp' ):
        super(MMFundTimeSeriesLoader, self).__init__(fundCode)
        self.localPrefix = prefix

    def getReturnByDate( self, StartDate = None, EndDate = None ):
        
        if StartDate not in self._rawData or EndDate not in self._rawData:
            self.load( StartDate, EndDate )
        return sum( self._rawData['dailyProfit'][StartDate:EndDate] )/10000.

    def getReturnTS( self, StartDate = None, EndDate = None, offset = 1 ):
        delta = pd.Timedelta(days = offset)
        if self.IsAvailableForTrading:
            return self._rawData['dailyProfit'][StartDate - delta:EndDate] \
                        .rolling(window = offset, center = False).sum().iloc[offset:].values/10000.
        else:
            return None


class OpenFundTimeSeriesLoader(FundTimeSeriesLoader, OpenFundTSLoaderSinaMixin):
    
    def __init__(self, fundCode, prefix = 'temp'):
        super(OpenFundTimeSeriesLoader, self).__init__(fundCode)
        self.localPrefix = prefix
        
    def getReturnByDate( self, StartDate = None, EndDate = None ):
        if StartDate not in self._rawData or EndDate not in self._rawData:
            self.load( StartDate, EndDate )
        startNAV, startAccNAV = self._rawData[ ['NAV', 'ACC_NAV'] ].loc[StartDate]
        endAccNAV             = self._rawData[ 'ACC_NAV' ][EndDate]
        
        return (endAccNAV - startAccNAV)/startNAV
    
    def getReturnTS( self, StartDate = None, EndDate = None, offset = 1  ):
        delta       = pd.Timedelta(days = offset)
        if self.IsAvailableForTrading: 
            startNAV    = self._rawData[ 'NAV' ][StartDate-delta:EndDate-delta].values
            startAccNAV = self._rawData[ 'ACC_NAV' ][StartDate-delta:EndDate-delta].values
            endAccNAV   = self._rawData[ 'ACC_NAV' ][StartDate:EndDate].values
        
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
        inds = self._rawData['ACC_NAV'].isnull().nonzero()[0]

        for ind in inds:
            self._rawData.loc[ self._rawData.index[ind], 'ACC_NAV'] = self._rawData['NAV'].iloc[ind] \
                                                    - self._rawData['NAV'].iloc[ind] \
                                                    + self._rawData['ACC_NAV'].iloc[ind-1]
        return

def fundTSLoaderResolver( fundCode ):
    fundType = getFundType(fundCode)
    if fundType == FundType.MMF:
        return MMFundTimeSeriesLoader(fundCode)
    else:
        return OpenFundTimeSeriesLoader(fundCode)

if __name__ == '__main__':
    import matplotlib.pyplot as plt

    StartDate       = dt.date(2017, 2, 12)
    EndDate         = dt.date(2018, 2, 14)
    HoldingPeriod   = 30
    DateIndex       = pd.date_range( StartDate, EndDate, feq = 'D' )
    #YuEBao          = MMFundTimeSeriesLoader('000198').load( index_col = 4, date_cols = ['endDate', 'publishDate'] )
    #YERet           = YuEBao.getReturnTS( StartDate, EndDate, HoldingPeriod)
    import os
    fundList = [filename.split('.')[0] for filename in os.listdir( './temp' )]
    #with open( './refData/AvailableFundList.txt', 'w' ) as f:
    for fund in [ '000624' ]:
        Shen    = fundTSLoaderResolver( fund )
        ShenRet = Shen.getReturnTS( StartDate, EndDate, HoldingPeriod )
        if Shen.IsAvailableForTrading:
            print( fund )
    
    #plt.plot( DateIndex, ShenRet )
    #plt.show()