from arctic import Arctic
import os 
import pandas as pd

if __name__ == "__main__":

    a = Arctic( 'localhost' )
    fund = a['fund']
    fund_adj = a['fund_adj']

    nav_path = './temp/'
    if os.path.exists(nav_path):
        print( "Start appending symbols to NAV db..." )
        for csv in os.listdir( nav_path ):
            symbol = csv.split('.')[0]
            print( 'Appending symbol {} to NAV db'.format( symbol ) )
            df = pd.read_csv(  os.path.join( nav_path, csv ), index_col=0, parse_dates=True )
            df.index.name = 'date'
            fund.append( symbol, df, upsert=True, chunk_size = 'M' )

    adj_path = './adj_temp/'
    if os.path.exists(adj_path):
        print( "Start appending symbols to ADJ db..." )
        for csv in os.listdir( adj_path ):
            symbol = csv.split('.')[0]
            if fund_adj.has_symbol(symbol):
                fund_adj.delete(symbol)
            print( 'Appending symbol {} to ADJ db'.format( symbol ) )
            df = pd.read_csv(  os.path.join( adj_path, csv ), index_col=0, parse_dates=True )
            df.index.name = 'date'
            if not df.empty:
                fund_adj.append( symbol, df, upsert=True, chunk_size = 'Y' )

        
