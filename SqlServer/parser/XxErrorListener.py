from antlr4 import *
import sys
# from SqlServer.parser.TSqlLexer import TSqlLexer
# from SqlServer.parser.TSqlListener import TSqlListener
# from SqlServer.parser.FullTSqlAntlrLexer import FullTSqlAntlrLexer
# from SqlServer.parser.FullTSqlAntlrParser import FullTSqlAntlrParser
from antlr4.error.ErrorListener import ErrorListener

class TSqlErrorListener( ErrorListener ):
    def __init__(self):
        super(TSqlErrorListener, self).__init__()
    
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise Exception("syntaxError : " + msg)
        
    # def reportAmbiguity(self, recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs):
        # raise Exception(msg)

    # def reportAttemptingFullContext(self, recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs):
        # raise Exception(msg)

    # def reportContextSensitivity(self, recognizer, dfa, startIndex, stopIndex, prediction, configs):
        # raise Exception(msg) 