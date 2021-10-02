import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
from tqdm import tqdm

def get_fund_holding(symbol):

    url = 'http://finance.sina.com.cn/fund/quotes/{}/bc.shtml'.format(symbol)
    html = requests.get(url)

    bs = BeautifulSoup(html.content, features="lxml")
    
    tbl = bs.find('table', {'id':'fund_sdzc_table'})
    if tbl is None or tbl.tbody.text=='\n':
        return 

    pat = re.compile('\d\d\d\d-\d\d-\d\d')
    report_date = pd.to_datetime( pat.findall(bs.find('div', {'class':'zqx_zcpz_date'}).text)[0] )

    stocks = tbl.attrs['codelist'].split(',')

    ts_codes = [ s[2:]+'.'+s[:2].upper() for s in stocks]

    holding = pd.read_html(tbl.prettify())[0]

    data_dict = dict(zip(ts_codes, holding[('占净值比例（%）',  '持股比例')].str[:-1].astype(float)))

    data = pd.DataFrame.from_dict(data_dict, 'index', columns=['holding'])

    # lib_fund_holding.write(fund, data, metadata={'report_date':report_date})
    return data


if __name__ == "__main__":
    import numpy as np

    symbols, _  = np.genfromtxt( './refData/fund_list.csv', dtype = str, delimiter = ',', unpack = True  )
    symbols = [ symbol.zfill(6) for symbol in symbols ]

    res = []
    for symbol in tqdm(symbols):
        df = get_fund_holding(symbol)
        if df is not None:
            res.append( df.reset_index().assign(fund=symbol) )

    fund_holding = pd.concat(res, ignore_index=True)
    fund_holding.to_csv('fund_holding.csv')