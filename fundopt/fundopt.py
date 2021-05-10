import numpy as np
import cvxpy as cvx
import pandas as pd
import datetime as dt
from abc import ABCMeta, abstractmethod
from .fundtcloader import fundTransactionCost as ftc
import os
import logging


class BaseFundOptimizer(object):

    __metaclass__ = ABCMeta

    cvxConstraints = []
    cvxObjective = None
    cvxProblem   = None
    cvxTrades    = None

    def __init__(self, returns, longOnly=True):
        """        
        Arguments:
            returns {pd.DataFrame} -- DataFrame of fund returns with index being dates and columns being fund codes
        """
        self.returns = returns
        self.fundList = returns.columns
        self.longOnly = longOnly

    def _genConstraints(self, currentPosition, **kwargs):
        # Self-financing constrains
        self.cvxConstraints = [ cvx.sum(self.cvxTrades)
                                + sum( [ ftc.getTxnCost(fund)(self.cvxTrades[ii]) for ii, fund in enumerate(self.fundList) ] ) <= 0.0 ]
        if self.longOnly:
            self.cvxConstraints += [ currentPosition + self.cvxTrades >= 0.0 ]

    def _genProblem(self, currentPosition, **kwargs):
        self._genConstraints( currentPosition, **kwargs)
        self._genObjectiveFunc( currentPosition, **kwargs)
        self.cvxProblem = cvx.Problem(self.cvxObjective, self.cvxConstraints)

    @abstractmethod
    def _genObjectiveFunc(self, currentPosition, **kwargs):
        pass

    def _interpretResult(self, optPos):
        sellPos = optPos < -1.
        buyPos  = optPos > 1.
        resultStr = ''
        fundArray = np.array( self.fundList, dtype = str )
        for sellFund, sellQuant in zip(fundArray[sellPos], optPos[sellPos]):
            resultStr += 'Sell fund %s for %.2f Yuan\n' % (sellFund, abs(sellQuant))
        for buyFund,  buyQuant  in zip(fundArray[buyPos],  optPos[buyPos]):
            resultStr += 'Buy  fund %s for %.2f Yuan\n' % (buyFund, buyQuant)

        return resultStr

    def getOptimalPosition(self, currentPosition, verbose = False, **kwargs):
        """Get optimal trades at given current position
        
        Arguments:
            currentPosition {pd.Series} -- Current fund position with index being fund codes and values being invested notional 
            current fund position has to be the same universe as the returns, extra investment will be discarded.
        
        Keyword Arguments:
            verbose {bool} -- Whether to print the optimization steps and final trade suggestions (default: {False})
        
        Returns:
            np.array -- Optimal trades to execute
        """
        currentPosition = currentPosition.reindex(self.fundList).fillna(0.0).values
        self.cvxTrades = cvx.Variable( currentPosition.shape )
        self._genProblem(currentPosition, **kwargs)
        try:
            self.cvxProblem.solve(verbose = verbose, solver = cvx.ECOS)
        except:
            optPos = np.zeros_like(currentPosition)
        if self.cvxProblem.status in [ cvx.OPTIMAL, cvx.OPTIMAL_INACCURATE ]:
            optPos = np.round(np.squeeze(np.array(self.cvxTrades.value)), decimals = 2)
        else:
            optPos = np.zeros_like(currentPosition)

        if verbose:
            if self.cvxProblem.status in [ cvx.OPTIMAL, cvx.OPTIMAL_INACCURATE ]:
                print( self._interpretResult(optPos) )
            else:
                print( 'Optimisation failed. Keep current position' )

        return optPos


class FundTargetRetOptimiser(BaseFundOptimizer):

    def __init__(self, targetRet = 0.01, *args, **kwargs):
        super(FundTargetRetOptimiser, self).__init__( *args, **kwargs )
        self.targetRet = targetRet

    def _genConstraints(self, currentPosition, lookbackPeriod=None):
        super(FundTargetRetOptimiser, self)._genConstraints(currentPosition)
        if lookbackPeriod is not None:
            returns = self.returns.loc[lookbackPeriod].fillna(0.0).values
        else:
            returns = self.returns.fillna(0.0).values
        Mu = np.mean(returns, axis = 0)
        self.cvxConstraints += [ Mu.T@( currentPosition + self.cvxTrades ) >= self.targetRet*np.sum( currentPosition ) ]

class FundTargetRetMinCVaROptimiser(FundTargetRetOptimiser):

    def __init__(self, cvarConf = 0.975, *args, **kwargs):
        super(FundTargetRetMinCVaROptimiser, self).__init__( *args, **kwargs )
        self.cvarConf = cvarConf
        self.aux      = cvx.Variable()

    def _genObjectiveFunc(self, currentPosition, lookbackPeriod=None):
        if lookbackPeriod is not None:
            returns = self.returns.loc[lookbackPeriod].fillna(0.0).values
        else:
            returns = self.returns.fillna(0.0).values
        T, _ = returns.shape
        CVaR = self.aux + cvx.sum( cvx.pos( - (currentPosition+self.cvxTrades)@returns.T - self.aux ) )/T/(1-self.cvarConf)
        self.cvxObjective =  cvx.Minimize( CVaR )

class FundTargetRetMinVarianceOptimiser(FundTargetRetOptimiser):

    def __init__(self, *args, **kwargs):
        super(FundTargetRetMinVarianceOptimiser, self).__init__( *args, **kwargs )

    def _genObjectiveFunc(self, currentPosition, lookbackPeriod=None):
        if lookbackPeriod is not None:
            returns = self.returns.loc[lookbackPeriod]
        else:
            returns = self.returns
        sigma = returns.cov().values
        variance = cvx.quad_form( currentPosition+self.cvxTrades, sigma )
        self.cvxObjective = cvx.Minimize( variance )