from fundopt.fundtsloader import getTSLoader
import datetime as dt
import matplotlib.pyplot as plt

def test_mm_fund(start, end, holding):
    loader = getTSLoader('000198')
    loader.load(start, end)
    fund_ret = loader.getReturnTS(start, end, holding)
    print(fund_ret)

def test_open_fund(start, end, holding):
    loader = getTSLoader('512310')
    loader.load(start, end)
    fund_ret = loader.getReturnTS(start, end, holding)
    print(fund_ret)

if __name__ == "__main__":

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 7)
    holding = 20

    # test_mm_fund(start, end, holding)

    test_open_fund(start, end, holding)
