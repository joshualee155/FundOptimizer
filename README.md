# FundOptimizer
Portfolio optimization using cvxpy. 

Trading set includes all open ended funds and money market funds with data available in Sina Finance. 

Generally they are tradeable via Ant Finance.

## Optimization process
1. Fetch daily NAV and accumulated NAV data from Sina Finance
    * [Caution] This is simply web scrap, may not work in the future
2. Building optimization problem
    * Specify target return
    * Specify look back period via start date and end date
    * Specify holding period
    * Currently only support CVaR (Conditional VaR), also known as ES (Expected Shortfall) as risk metric