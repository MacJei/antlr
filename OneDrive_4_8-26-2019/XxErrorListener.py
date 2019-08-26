from antlr4 import *
import sys
from SqlServer.parser.TSqlLexer import TSqlLexer
from SqlServer.parser.TSqlListener import TSqlListener
from antlr4.error.ErrorListener import ErrorListener

class TSqlErrorListener( ErrorListener ):
    def __init__(self):
        super(TSqlErrorListener, self).__init__()
    
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise Exception("Oh no!!")
        
    # def reportAmbiguity(self, recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs):
        # raise Exception("Oh no!!")

    # def reportAttemptingFullContext(self, recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs):
        # raise Exception("Oh no!!")

    # def reportContextSensitivity(self, recognizer, dfa, startIndex, stopIndex, prediction, configs):
        # raise Exception("Oh no!!")      