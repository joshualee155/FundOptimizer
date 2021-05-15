from fundopt.fundtsloader import getTSLoader
import datetime as dt

if __name__ == "__main__":

    start = dt.date(2020, 1, 1)
    end   = dt.date(2021, 5, 7)
    holding = 20
    # test for MM funds
    loader = getTSLoader('000198')
    loader.load(start, end)

    fund_ret = loader.getReturnTS(start, end, holding)

    print(fund_ret)
