import requests
import pandas as pd
import numpy as np
from   datetime import date
import multiprocessing as mp
import os

url_of  = 'http://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNav'
url_mmf = 'http://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNavCur'
endDate   = '%s' % date.today()
startDate = '2014-01-01'

def download_of( fundCode ):
    """
    Download the daily NAV for Open Ended Fund. 

    Paremeter: 
    ==========
        fundCode: str, 6-digit fund code
    """

    #if os.path.exists( './temp/%s.csv' % fundCode ):
    #    return 1
    requestDates = pd.date_range( startDate, endDate )
    res = requests.post( url_of, data = { 'symbol'   : fundCode, 
                                       'datefrom' : startDate, 
                                       'dateto'   : endDate, 
                                       } )
    if res.ok:
        print( "Start downloading %s...." % fundCode )
        dataJson = res.json().get( 'result' ).get( 'data' )
        totalLen = int( dataJson.get( 'total_num' ) )
        if totalLen == 0:
            print( "No data found for %s, skip" % fundCode )
            return ""
        data     = dataJson.get( 'data' )
        currLen  = len( data )

        pageNum  = 2
        while currLen < totalLen:
            res = requests.post( url_of, data = { 'symbol' : fundCode, 
                                               'datefrom' : startDate, 
                                               'dateto' : endDate, 
                                               'page' : str( pageNum )
                                               } )
            dataJson  = res.json().get( 'result' ).get( 'data' )
            data     += dataJson.get( 'data' )

            currLen   = len(data)
            pageNum  += 1

        dataDf = pd.DataFrame.from_dict( data ).astype( { 'fbrq' : pd.Timestamp, 'jjjz' : float, 'ljjz' : float } )
        dataDf.rename( columns = { 'fbrq' : 'Date', 'jjjz' : 'NAV', 'ljjz' : 'ACC_NAV' }, inplace = True )
        dataDf[ 'Date' ] = dataDf[ 'Date' ].apply( pd.Timestamp )
        dataDf.set_index( 'Date', inplace = True )
        dataDf = dataDf[ ~dataDf.index.duplicated() ]
        dataDf.sort_index( inplace = True )
        dataDf = dataDf.reindex( requestDates, method = 'ffill' )
        dataDf.dropna( inplace = True )
        dataDf.to_csv( './temp/%s.csv' % fundCode )
        return fundCode
    return ""

def download_mmf( fundCode ):
    """
    Download the daily profit and weekly return for Money Market Fund. 

    Paremeter: 
    ==========
        fundCode: str, 6-digit fund code
    """

    res = requests.post( url_mmf, data = { 'symbol'   : fundCode, 
                                       'datefrom' : startDate, 
                                       'dateto'   : endDate, 
                                       } )
    if res.ok:
        print( "Start downloading %s...." % fundCode )
        dataJson = res.json().get( 'result' ).get( 'data' )
        totalLen = int( dataJson.get( 'total_num' ) )
        if totalLen == 0:
            print( "No data found, skip" )
            return 0
        data     = dataJson.get( 'data' ).values()
        currLen  = len( data )

        pageNum  = 2
        while currLen < totalLen:
            res = requests.post( url_mmf, data = { 'symbol' : fundCode, 
                                               'datefrom' : startDate, 
                                               'dateto' : endDate, 
                                               'page' : str( pageNum )
                                               } )
            dataJson  = res.json().get( 'result' ).get( 'data' )
            dataSlice = dataJson.get('data')
            if dataSlice is not None:
                if isinstance( dataSlice, list ):
                    data += dataSlice
                else:
                    data += dataSlice.values()
                currLen   = len(data)
                pageNum  += 1
            else:
                break

        dataDf = pd.DataFrame.from_dict( data ).astype( { 'fbrq' : pd.Timestamp, 'nhsyl' : float, 'dwsy' : float } )
        dataDf.rename( columns = { 'fbrq' : 'Date', 'nhsyl' : 'weeklyReturn', 'dwsy' : 'dailyProfit' }, inplace = True )
        dataDf.drop( columns = ["NAV_CUR1"], inplace = True )
        dataDf[ 'Date' ] = pd.to_datetime( dataDf[ 'Date' ] )
        dataDf.drop_duplicates( subset = [ 'Date' ], inplace = True )
        dataDf.set_index( 'Date', inplace = True )
        dataDf.sort_index( inplace = True )

        dataDf.to_csv( './temp/%s.csv' % fundCode )
        return 1
    return 0


if __name__ == '__main__':

    fundList, fundTypes  = np.genfromtxt( './refData/fund_list.csv', dtype = str, delimiter = ',', unpack = True  )
    fundList  = [ fund.zfill(6) for fund, fundType in zip(fundList, fundTypes) if fundType  == 'MMF' ]

    fundAvail = [filename.split('.')[0] for filename in os.listdir( './temp/' ) ]
    
    fundList  = list( set( fundList ).difference( fundAvail ) )
    fundList = [ '000198' ]
    #pool = mp.Pool( 16 )
    #r = pool.map_async( download_mmf, fundList )
    #r.wait()
    download_mmf( fundList[0] )
    print( "Done!" )