import numpy as np
import cvxpy as cvx
import pandas as pd
import datetime as dt
from abc import ABCMeta, abstractmethod
from fundopt.fundtsloader import fundTSLoaderResolver
from fundopt.fundtcloader import FundTransactionCostLoader
import os
import logging


class FundOptimizer(object):


    __metaclass__ = ABCMeta

    cvxConstraints   = []
    cvxObjectiveFunc = None
    cvxProblem       = None
    cvxPosition      = None

    Ret              = None

    def __init__(self,
                 startDate       = dt.date(2015, 11, 4),
                 endDate         = dt.date(2016, 11, 4),
                 holdingPeriod   = 30,
                 longOnly        = True,
                 fundList        = [],
                 currentPosition = [], 
                 **kwargs):
        assert(len(fundList) == len(currentPosition))
        self.startDate       = startDate
        self.endDate         = endDate
        self.holdingPeriod   = holdingPeriod
        self.longOnly        = longOnly
        self.fundList        = fundList
        self.currentPosition = currentPosition
        #self.cvxPosition     = cvx.Variable(len(currentPosition))
        self.tsLoader        = { }
        self.fundTxnCosts    = []
        self._loadFund()
        self._populateRet()

    def _loadFund( self ):
        txnCostLoader = FundTransactionCostLoader()
        availableFundList = []
        for fund in self.fundList:
            try:
                logging.debug( "%s:Loading time series...", fund )
                self.tsLoader[fund] = fundTSLoaderResolver(fund)
                self.tsLoader[fund].load( self.startDate - pd.Timedelta( days = self.holdingPeriod ), self.endDate )
                if not self.tsLoader[fund].IsAvailableForOptimisation:
                    del self.tsLoader[fund]
                    raise RuntimeError( "Fund %s is not available for optimisation" % fund  )
                self.fundTxnCosts.append( txnCostLoader.getTxnCost(fund) )
                availableFundList.append( fund )
                logging.debug( "%s:Successfully loaded fund data.", fund )
            except Exception as e:
                logging.warn( "%s:Cannot read fund data. Reason: %s", fund, e )

        logging.info( "FundOptimiser._loadFund:Read successfully %d funds data. Total fund list size: %d",  len( self.tsLoader ), len( self.fundList ) )
        self.fundList        = availableFundList
        availablePosition    = self.currentPosition[ availableFundList ]
        self.currentPosition = np.matrix(availablePosition).T
        self.cvxPosition      = cvx.Variable( len( self.currentPosition ) )

    def _populateRet(self):
        retTS = []
        for fund in self.fundList:
            try:
                retTS.append(self.tsLoader[fund].getReturnTS(self.startDate, self.endDate, self.holdingPeriod))
            except Exception as e:
                logging.warn( "Cannot calculate returns for %s, Reason: %s", fund, e )

        self.Ret = np.matrix(retTS)
        logging.info( "FundOptimiser._populateRet:Calculated returns for %d funds and %d dates",  self.Ret.shape[0], self.Ret.shape[1])

    def _genConstraints(self):
        # Self-financing constrains
        self.cvxConstraints = [ cvx.sum(self.cvxPosition)
                                + sum( [ txnCost(self.cvxPosition[ii]) for ii, txnCost in enumerate(self.fundTxnCosts) ] ) <= 0.0 ]
        if self.longOnly:
            self.cvxConstraints += [ self.currentPosition + self.cvxPosition >= 0.0 ]

    def _genProblem(self):
        self.cvxProblem = cvx.Problem(self.cvxObjectiveFunc, self.cvxConstraints)

    @abstractmethod
    def _genObjectiveFunc(self):
        pass

    def _interpretResult(self, optPos):
        sellPos = optPos < -0.01
        buyPos  = optPos > 0.01
        resultStr = ''
        fundArray = np.array( self.fundList, dtype = str )
        for sellFund, sellQuant in zip(fundArray[sellPos], optPos[sellPos]):
            resultStr += 'Sell fund %s for %.2f Yuan\n' % (sellFund, abs(sellQuant))
        for buyFund,  buyQuant  in zip(fundArray[buyPos],  optPos[buyPos]):
            resultStr += 'Buy  fund %s for %.2f Yuan\n' % (buyFund, buyQuant)

        return resultStr

    def getOptimalPosition(self, verbose = False):
        self._genObjectiveFunc()
        self._genConstraints()
        self._genProblem()
        try:
            self.cvxProblem.solve(verbose = verbose)
        except:
            optPos = np.zeros_like(self.currentPosition)
        if self.cvxProblem.status in [ cvx.OPTIMAL, cvx.OPTIMAL_INACCURATE ]:
            optPos = np.round(np.squeeze(np.array(self.cvxPosition.value)), decimals = 2)
        else:
            optPos = np.zeros_like(self.currentPosition)

        if verbose:
            if self.cvxProblem.status in [ cvx.OPTIMAL, cvx.OPTIMAL_INACCURATE ]:
                print( self._interpretResult(optPos) )
            else:
                print( 'Optimisation failed. Keep current position' )

        return optPos


class FundTargetRetOptimiser(FundOptimizer):

    def __init__(self, targetRet = 0.01, *args, **kwargs):
        super(FundTargetRetOptimiser, self).__init__( *args, **kwargs )
        self.targetRet = targetRet

    def _genConstraints(self):
        super(FundTargetRetOptimiser, self)._genConstraints()
        Mu = np.matrix(np.mean(self.Ret, axis=1))
        self.cvxConstraints += [ Mu.T*( self.currentPosition + self.cvxPosition ) >= self.targetRet*np.sum( self.currentPosition ) ]

class FundTargetRetMinCVaROptimiser(FundTargetRetOptimiser):

    def __init__(self, cvarConf = 0.975, *args, **kwargs):
        super(FundTargetRetMinCVaROptimiser, self).__init__( *args, **kwargs )
        self.cvarConf = cvarConf
        self.aux      = cvx.Variable()

    def _genObjectiveFunc(self):
        _, T = self.Ret.shape
        CVaR = self.aux + cvx.sum( cvx.pos( - (self.currentPosition+self.cvxPosition).T*self.Ret - self.aux ) )/T/(1-self.cvarConf)
        self.cvxObjectiveFunc = cvx.Minimize( CVaR )