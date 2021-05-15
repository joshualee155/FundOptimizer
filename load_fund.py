from   fundopt.fundtsloader import load_funds
import pandas   as pd
import numpy    as np
import datetime as dt
import logging
from arctic import CHUNK_STORE, Arctic

a = Arctic('localhost')
a.initialize_library('fund', lib_type=CHUNK_STORE)

lib = a['fund']

if __name__ == "__main__":
    logging.basicConfig( level = logging.INFO )

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 7)
    holding = 20
    
    funds = lib.list_symbols()
    funds.remove('003254') # data issue
    funds.remove('003255') # data issue
    funds.remove('001481') # QDII funds, not sold on Ant Financial

    fund_returns=load_funds(funds, start, end, holding)
    fund_returns.to_pickle('./{}_{}_{}.pkl'.format(start, end, holding))
