from antlr4 import *
import sys
from Teradata.parser.TDantlrLexer import TDantlrLexer
from Teradata.parser.TDantlrListener import TDantlrListener
from antlr4.error.ErrorListener import ErrorListener

class TDErrorListener( ErrorListener ):
    def __init__(self):
        super(TDErrorListener, self).__init__()
    
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise Exception("Oh no!!")
        
    # def reportAmbiguity(self, recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs):
        # raise Exception("Oh no!!")

    # def reportAttemptingFullContext(self, recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs):
        # raise Exception("Oh no!!")

    # def reportContextSensitivity(self, recognizer, dfa, startIndex, stopIndex, prediction, configs):
        # raise Exception("Oh no!!")      