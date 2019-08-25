import requests
import pandas as pd
import os.path
import logging

url_of  = 'http://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNav'

class OpenFundTSLoaderSinaMixin( object ):

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
        """
        Download fund NAV data from Sina Finance via HTTP request
        """

        if not missingDates:
            return pd.DataFrame()

        firstDate = str( min(missingDates).date() )
        lastDate  = str( max(missingDates).date() )


        res = requests.post( url_of, data = { 'symbol'   : self.fundCode, 
                                              'datefrom' : firstDate, 
                                              'dateto'   : lastDate, 
                                              } )
        if res.ok:
            logging.debug( "%s:Start downloading fund data ", self.fundCode )
            dataJson = res.json().get( 'result' ).get( 'data' )
            totalLen = int( dataJson.get( 'total_num' ) )
            if totalLen == 0:
                logging.debug( "No data found for %s, Skip.", self.fundCode )
                return pd.DataFrame()
            data     = dataJson.get( 'data' )
            currLen  = len( data )

            pageNum  = 2
            while currLen < totalLen:
                res = requests.post( url_of, data = { 'symbol'   : self.fundCode, 
                                                      'datefrom' : firstDate, 
                                                      'dateto'   : lastDate, 
                                                      'page'     : str( pageNum )
                                                       } )
                dataJson  = res.json().get( 'result' ).get( 'data' )
                data     += dataJson.get( 'data' )

                currLen   = len(data)
                pageNum  += 1

            dataDf = pd.DataFrame.from_dict( data ).astype( { 'jjjz' : float, 'ljjz' : float } )
            dataDf[ 'fbrq' ] = pd.to_datetime( dataDf[ 'fbrq' ] )
            dataDf.rename( columns = { 'fbrq' : 'Date', 'jjjz' : 'NAV', 'ljjz' : 'ACC_NAV' }, inplace = True )
            dataDf[ 'Date' ] = dataDf[ 'Date' ].apply( pd.Timestamp )
            dataDf = dataDf[ dataDf[ 'Date' ].isin( missingDates ) ]
            dataDf.set_index( 'Date', inplace = True )
            logging.debug( "%s:Downloaded %d records of fund NAVs.", self.fundCode, len( dataDf ) )
            return dataDf
        else:
            return pd.DataFrame()
