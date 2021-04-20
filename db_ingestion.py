from arctic import Arctic
import os 
import pandas as pd

if __name__ == "__main__":

    a = Arctic( 'localhost' )
    fund = a['fund']
    
    # print( "Start deleting symbols..." )
    # for symbol in fund.list_symbols():
    #     fund.delete( symbol )

    # print( "Start re-ingesting symbols..." )
    # path1 = './temp/'
    # for file_name in os.listdir( path1 ):
    #     symbol = file_name.split('.')[0]
    #     print( 'Ingesting symbol {} to db'.format( symbol ) )
    #     df = pd.read_csv(  os.path.join( path1, file_name ), index_col=0, parse_dates=True )
    #     df.index.name = 'date'
    #     fund.write( symbol, df, chunk_size = 'M' )

    print( "Start appending symbols..." )
    path2 = './temp/'
    for file_name in os.listdir( path2 ):
    # for file_name in ['000198.csv']:
        symbol = file_name.split('.')[0]
        print( 'Appending symbol {} to db'.format( symbol ) )
        df = pd.read_csv(  os.path.join( path2, file_name ), index_col=0, parse_dates=True )
        df.index.name = 'date'
        fund.append( symbol, df, upsert=True, chunk_size = 'M' )

    
