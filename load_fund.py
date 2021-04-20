from   fundopt.fundopt import FundTargetRetMinCVaROptimiser
from   fundopt.fundtsloader import fundTSLoaderResolver
import multiprocessing as mp
from functools import partial
import pandas   as pd
import numpy    as np
import datetime as dt
import logging

def _load_data(fund, start, end):
    logging.debug( "%s:Loading time series...", fund )
    try:
        tsLoader = fundTSLoaderResolver(fund)
        tsLoader.load( start, end )
        ret = tsLoader.getReturnTS()
        if ret is not None:
            ret.name = fund
            return ret
    except Exception as e:
        logging.warning( "%s:Cannot load time series. Error: %s", fund, e )

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    startDate = dt.date(2019, 9, 23)
    endDate   = dt.date(2020, 3, 24)
    
    fundList  = np.genfromtxt( 'refData/AvailableFundList.txt', dtype = str ).tolist()
    fundList.remove('003254')
    fundList.remove('003255')

    # to_map = partial( _load_data, start = startDate, end = endDate )
    # pool = mp.Pool(8)
    # res = pool.map( to_map, fundList[:100] )
    # pool.close()
    # pool.join()

    res = _load_data( '165705', startDate, endDate )
    df = pd.concat(res, axis=1)
    print( res )