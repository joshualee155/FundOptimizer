from arctic import Arctic, CHUNK_STORE # pyright: reportMissingImports=false
import os
import pandas as pd
from pymongo import MongoClient
import keyring
import ssl

if __name__ == "__main__":

    # client = MongoClient("localhost")
    client = MongoClient(keyring.get_password('atlas', 'connection_string'), ssl_cert_reqs=ssl.CERT_NONE)

    a = Arctic(client)

    if a.library_exists('fund'):
        a.delete_library('fund')
    if a.library_exists('fund_adj'):
        a.delete_library('fund_adj')

    fund = a.initialize_library('fund', CHUNK_STORE)
    fund_adj = a.initialize_library('fund_adj', CHUNK_STORE)

    fund = a['fund']
    fund_adj = a['fund_adj']

    local = Arctic('localhost')

    fund_local = local['fund']
    fund_adj_local = local['fund_adj']
    
    for symbol in fund_local.list_symbols():
        data = fund_local.read(symbol)
        fund.write(symbol, data, chunk_size='M')
    
    for symbol in fund_adj_local.list_symbols():
        data = fund_adj_local.read(symbol)
        fund_adj.write(symbol, data, chunk_size='Y')
    