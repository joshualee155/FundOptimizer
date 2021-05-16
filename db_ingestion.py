from arctic import Arctic
import os 
import pandas as pd

if __name__ == "__main__":

    a = Arctic( 'localhost' )
    fund = a['fund']
    fund_adj = a['fund_adj']

    print( "Start appending symbols to NAV db..." )
    nav_path = './temp/'
    for csv in os.listdir( nav_path ):
        symbol = csv.split('.')[0]
        print( 'Appending symbol {} to db'.format( symbol ) )
        df = pd.read_csv(  os.path.join( nav_path, csv ), index_col=0, parse_dates=True )
        df.index.name = 'date'
        fund.append( symbol, df, upsert=True, chunk_size = 'M' )

    print( "Start appending symbols to ADJ db..." )
    adj_path = './adj_temp/'
    for csv in os.listdir( adj_path ):
        symbol = csv.split('.')[0]
        print( 'Appending symbol {} to db'.format( symbol ) )
        df = pd.read_csv(  os.path.join( adj_path, csv ), index_col=0, parse_dates=True )
        df.index.name = 'date'
        fund_adj.append( symbol, df, upsert=True, chunk_size = 'M' )

    
