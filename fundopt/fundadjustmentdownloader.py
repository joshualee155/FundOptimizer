import requests
from bs4 import BeautifulSoup
import multiprocessing as mp
import os 
import pandas as pd
from arctic import Arctic

lib = Arctic('localhost')
fund = lib['fund']

folder = './adj_temp'
os.makedirs(folder, exist_ok=True)

def get_fund_split_data(symbol):
    """Download fund split data from Tiantian Jijinwang

    Args:
        symbol (str): fund symbol

    Returns:
        pd.DataFrame: fund split data, indexed by date, with one column `amount`
    """
    url = f'http://fundf10.eastmoney.com/fhsp_{symbol}.html'
    res = requests.get(url)
    html = BeautifulSoup(res.text)
    split_data = html.find('table', attrs={'class':'w782 comm fhxq'})
    split = pd.read_html(str(split_data))[0]
    
    if split.iloc[0,0] == '暂无拆分信息!':
        return pd.DataFrame()
    
    rename = {'年份' : 'year', 
              '拆分折算日' : 'date', 
              '拆分类型' : 'splitType', 
              '拆分折算比例' : 'amount'}

    split = split.rename(columns=rename)
    
    split['date'] = pd.to_datetime(split['date'])
    split['amount'] = split['amount'].str.split(':', expand=True)[1].astype(float)

    split = split.drop(['year', 'splitType'], axis=1).set_index('date').sort_index()
    
    return split

def get_fund_div_data(symbol):
    """Download fund dividend data from Sina Finance

    Args:
        symbol (str): fund symbol

    Returns:
        pd.DataFrame: fund dividend data, indexed by date, with one column `amount`
    """
    url = 'https://stock.finance.sina.com.cn/fundInfo/api/openapi.php/FundPageInfoService.tabfh'

    data_input = {
        'symbol' : symbol, 
        'format' : 'json',
    }

    resp = requests.get(url, params=data_input)
    data = resp.json()
    fhdata = data['result']['data']['fhdata']
    
    if len(fhdata)==0:
        return pd.DataFrame()
    
    div = pd.DataFrame(fhdata).astype({'mffh':float})
    
    div = div[div['mffh'] > 0.0]
    
    div = div.drop('fhr', axis=1)

    RENAME = {
        'djr' : 'date',
        'mffh' : 'amount',
    }

    div = div.rename(columns = RENAME)

    div['date'] = pd.to_datetime(div['date'])
   
    # sort div data frame from old to new
    div = div.set_index('date').sort_index()
    
    return div

def get_fund_adj_data(symbol):
    """Get fund adjustment data by merging fund split with fund div

    Args:
        symbol (str): fund symbol

    Returns:
        pd.DataFrame: fund adjustment data, indexed by date, with columns `amount`, `type`
                      where `type` can be either `split`, or `div`
    """
    split = get_fund_split_data(symbol).assign(type='split')
    div = get_fund_div_data(symbol).assign(type='div')
    adj = pd.concat([split, div]).sort_index()

    return adj

def save_fund_adj_data(symbol):
    """Save fund adjustment data to local folder

    Args:
        symbol (str): fund symbol
    """
    adj = get_fund_adj_data(symbol)
    adj.to_csv(f'{folder}/{symbol}.csv')

def load_funds(symbols, multiprocessing=True):

    if multiprocessing:
        pool = mp.Pool(8)
        r = pool.map(save_fund_adj_data,symbols)
    else:
        for symbol in symbols:
            save_fund_adj_data(symbol)

if __name__ == "__main__":
    symbols = fund.list_symbols()

    load_funds(symbols)

