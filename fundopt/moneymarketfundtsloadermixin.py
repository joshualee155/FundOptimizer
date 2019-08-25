import requests
import pandas as pd
import os.path
import logging as logger

url_mmf = 'http://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNavCur'

class MoneyMarketFundTSLoaderSinaMixin( object ):

    def getLocalData( self ):
        """
        Read from local cache, return the dataframe and available dates in the cache
        """

        localData = pd.read_csv( os.path.join(self.localPrefix, '%s.csv' % self.fundCode), 
                                index_col = 0, parse_dates = True,
                                )
        dates     = localData.index.tolist()

        return localData, dates

    def writeLocalData( self, dataDf ):
        """
        Write to local cache in CSV format
        """

        dataDf.to_csv( os.path.join(self.localPrefix, '%s.csv' % self.fundCode ) )

    def getDataFromWeb( self, missingDates ):

        if not missingDates:
            return pd.DataFrame()

        startDate = str( min( missingDates ).date() )
        endDate   = str( max( missingDates ).date() )

        res = requests.post( url_mmf, data = { 'symbol'   : self.fundCode, 
                                               'datefrom' : startDate, 
                                               'dateto'   : endDate, 
                                               } )
        if res.ok:
            logger.info( "Start downloading %s....", self.fundCode )
            dataJson = res.json().get( 'result' ).get( 'data' )
            totalLen = int( dataJson.get( 'total_num' ) )
            if totalLen == 0:
                logger.debug( "No data found for %s, Skip.", self.fundCode )
                return pd.DataFrame()
            data     = dataJson.get( 'data' ).values()
            currLen  = len( data )

            pageNum  = 2
            while currLen < totalLen:
                res = requests.post( url_mmf, data = { 'symbol'   : self.fundCode, 
                                                       'datefrom' : startDate, 
                                                       'dateto'   : endDate, 
                                                       'page'     : str( pageNum )
                                                       } )
                dataJson  = res.json().get( 'result' ).get( 'data' )
                if dataJson.get('data') is not None:
                    data     += dataJson.get( 'data' ).values()
                    currLen   = len(data)
                    pageNum  += 1
                else:
                    break

            dataDf = pd.DataFrame.from_dict( data ).astype( { 'fbrq' : str, 'nhsyl' : float, 'dwsy' : float } )
            dataDf.rename( columns = { 'fbrq' : 'Date', 'nhsyl' : 'weeklyReturn', 'dwsy' : 'dailyProfit' }, inplace = True )
            dataDf.drop( columns = ["NAV_CUR1"], inplace = True )
            dataDf[ 'Date' ] = pd.to_datetime( dataDf[ 'Date' ] )
            dataDf.drop_duplicates( subset = [ 'Date' ], inplace = True )
            dataDf = dataDf[ dataDf[ 'Date' ].isin( missingDates ) ]
            dataDf.set_index( 'Date', inplace = True )
            dataDf.sort_index( inplace = True )            
            return dataDf

        else:
            return pd.DataFrame()