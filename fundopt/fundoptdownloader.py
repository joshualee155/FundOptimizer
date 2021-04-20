import requests
import pandas as pd
import numpy as np
from   datetime import date
import multiprocessing as mp
import os
import time

url_of  = 'http://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNav'
url_mmf = 'http://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNavCur'

folder = './temp'

os.makedirs(folder, exist_ok=True)

def _try_request( url, data, max_tries = 3, pause = 0.01 ):

    res = None

    n_tries = 1
    while True:
        try:
            res = requests.post( url, data = data )
            break
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            time.sleep(pause)
            if n_tries < max_tries:
                print( f"Trying {n_tries}-th time." )
                n_tries += 1
            else:
                raise

    return res


def download_of( fundCode, startDate, endDate ):
    """
    Download the daily NAV for Open Ended Fund. 

    Paremeter: 
    ==========
        fundCode: str, 6-digit fund code
    """

    #if os.path.exists( './temp/%s.csv' % fundCode ):
    #    return 1
    requestDates = pd.bdate_range( startDate, endDate )
    res = _try_request( url_of, data = { 'symbol'   : fundCode, 
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
        newLen = currLen
        while currLen < totalLen and newLen > 0:
            res = _try_request( url_of, data = { 'symbol' : fundCode, 
                                               'datefrom' : startDate, 
                                               'dateto' : endDate, 
                                               'page' : str( pageNum )
                                               } )
            dataJson  = res.json().get( 'result' ).get( 'data' )
            data     += dataJson.get( 'data' )
            newLen    = len( dataJson.get( 'data' ) )
            currLen   = len(data)
            pageNum  += 1

        dataDf = pd.DataFrame( data ).astype( { 'jjjz' : float, 'ljjz' : float } )
        dataDf.rename( columns = { 'fbrq' : 'Date', 'jjjz' : 'NAV', 'ljjz' : 'ACC_NAV' }, inplace = True )
        dataDf[ 'Date' ] = pd.to_datetime( dataDf[ 'Date' ] )
        dataDf.set_index( 'Date', inplace = True )
        dataDf = dataDf[ ~dataDf.index.duplicated() ]
        dataDf.sort_index( inplace = True )
        dataDf = dataDf.reindex( requestDates, method = 'ffill' )
        dataDf.dropna( inplace = True )
        dataDf.to_csv( folder + '/%s.csv' % fundCode )
        return fundCode
    return ""

def download_mmf( fundCode, startDate, endDate ):
    """
    Download the daily profit and weekly return for Money Market Fund. 

    Paremeter: 
    ==========
        fundCode: str, 6-digit fund code
    """

    res = _try_request( url_mmf, data = { 'symbol'   : fundCode, 
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
        data     = list( dataJson.get( 'data' ).values() )
        currLen  = len( data )

        pageNum  = 2
        newLen = currLen
        while currLen < totalLen and newLen > 0:
            res = _try_request( url_mmf, data = { 'symbol' : fundCode, 
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
                    data += list( dataSlice.values() )
                currLen   = len(data)
                newLen = len( dataSlice )
                pageNum  += 1
            else:
                break

        dataDf = pd.DataFrame.from_dict( data ).astype( {'nhsyl' : float, 'dwsy' : float } )
        dataDf.rename( columns = { 'fbrq' : 'Date', 'nhsyl' : 'weeklyReturn', 'dwsy' : 'dailyProfit' }, inplace = True )
        dataDf.drop( columns = ["NAV_CUR1"], inplace = True )
        dataDf[ 'Date' ] = pd.to_datetime( dataDf[ 'Date' ] )
        dataDf.drop_duplicates( subset = [ 'Date' ], inplace = True )
        dataDf.set_index( 'Date', inplace = True )
        dataDf.sort_index( inplace = True )

        dataDf.to_csv( folder + '/%s.csv' % fundCode )
        return 1
    return 0


if __name__ == '__main__':

    from functools import partial
    # import os
    # os.makedirs(folder)

    fundList, fundTypes  = np.genfromtxt( './refData/fund_list.csv', dtype = str, delimiter = ',', unpack = True  )
    fundList  = [ fund.zfill(6) for fund, fundType in zip(fundList, fundTypes) if fundType == 'MMF' ]

    endDate   = '%s' % date.today()
    startDate = '2021-02-27'

    # fundAvail = [filename.split('.')[0] for filename in os.listdir( './temp/' ) ]
    
    # fundList  = list( set( fundList ).difference( fundAvail ) )
    # fundList = [ '000198' ]
    # pool = mp.Pool( 8 )
    # to_apply = partial( download_of, startDate = startDate, endDate = endDate )
    # r = pool.map( to_apply , fundList )
    
    for fund in fundList:
        time.sleep(0.01)
        download_mmf( fund, startDate, endDate )
    print("Done!")

    # download_of( fundList[0] )

    # for fund in fundList:
    #     download_mmf( fund )
    # print( "Done!" )