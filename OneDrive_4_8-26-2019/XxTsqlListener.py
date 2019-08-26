from antlr4 import *
from SqlServer.parser.TSqlLexer import TSqlLexer
from SqlServer.parser.TSqlListener import TSqlListener
from SqlServer.parser.TSqlParser import TSqlParser
from antlr4.tree.Tree import TerminalNodeImpl
import SqlServer.parser.XxfunctionConverter as TSql2Spark
import datetime
import sys
import re


class XxTsqlListener(TSqlListener):
    def __init__(self, tokens:CommonTokenStream, logger):
        #logging
        self.logger = logger

        #function
        self.fn_nest_cnt = 0
        self.fn_tokens = []
        self.fn_spaces = []
        self.fn_exprns = [] ##
        self.fn_conv_txt = {}        
        
        #alias
        self.alias_scope = 0
        self.alias_order_by = False  
        self.alias_scope_txt = [{}]
        
        #interval
        self.intv_nest_cnt = 0
        self.intv_tokens = []
        self.intv_spaces = []
        self.intv_conv_txt = {}

        #field mode cast
        self.fld_mode_cast_start = 0
        self.fld_mode_cast_end = 0
        self.fld_mode_cast = False
        #self.fld_mode_cast_txt = {}

        #concat
        self.cnct_nest_cnt = 0
        self.cnct_tokens = []
        self.cnct_spaces = []
        self.cnct_conv_txt = {}

        #case specific
        self.case_spf = False

        #qualify
        self.qlfy_expr = False
        self.qlfy_str = ''
        self.qlfy_scope = 0
        self.qlfy_scope_stat = []
        self.qlfy_sub_txt = {}
        self.qlfy_al_cntr = 0

        #common
        self.last_visited_token = ''  #Holds the last encountered token by DFS traversal of parse tree.
        self.token_stream = tokens.tokens
        self.out_sql = ''


    #OVERLOADED: Generic Terminal Handler
    def visitTerminal(self, ctx:TerminalNodeImpl):
        if self.fn_nest_cnt == 0:   #Tokens under FUNCTION calls are not to be handled by this method
            tok_txt = ctx.getText()
            if tok_txt.upper() in ['__LOAD__','__BLOCK__']:
                tok_txt = tok_txt.strip('_')
            hdn_tok = self.getLeftHiddenToken(ctx)
            if tok_txt == '<EOF>':
                pass
            else:
                self.out_sql += hdn_tok + tok_txt
                self.last_visited_token = tok_txt

    #def enterEveryRule(self,ctx):


    #OVERLOADED: Function Enter Exit Handlers
    def enterRANKING_WINDOWED_FUNC(self, ctx:TSqlParser.RANKING_WINDOWED_FUNCContext):
        self.enterFunction(ctx)
    def exitRANKING_WINDOWED_FUNC(self, ctx:TSqlParser.RANKING_WINDOWED_FUNCContext):
        self.exitFunction(ctx)

    def enterAGGREGATE_WINDOWED_FUNC(self, ctx:TSqlParser.AGGREGATE_WINDOWED_FUNCContext):
        self.enterFunction(ctx)
    def exitAGGREGATE_WINDOWED_FUNC(self, ctx:TSqlParser.AGGREGATE_WINDOWED_FUNCContext):
        self.exitFunction(ctx)

    def enterANALYTIC_WINDOWED_FUNC(self, ctx:TSqlParser.ANALYTIC_WINDOWED_FUNCContext):
        self.enterFunction(ctx)
    def exitANALYTIC_WINDOWED_FUNC(self, ctx:TSqlParser.ANALYTIC_WINDOWED_FUNCContext):
        self.exitFunction(ctx)

    def enterSCALAR_FUNCTION(self, ctx:TSqlParser.SCALAR_FUNCTIONContext):
        self.enterFunction(ctx)
    def exitSCALAR_FUNCTION(self, ctx:TSqlParser.SCALAR_FUNCTIONContext):
        self.exitFunction(ctx)

    def enterCAST(self, ctx:TSqlParser.CASTContext):
        self.enterFunction(ctx)
    def exitCAST(self, ctx:TSqlParser.CASTContext):
        self.exitFunction(ctx)

    def enterCONVERT(self, ctx:TSqlParser.CONVERTContext):
        self.enterFunction(ctx)
    def exitCONVERT(self, ctx:TSqlParser.CONVERTContext):
        self.exitFunction(ctx)

    def enterCOALESCE(self, ctx:TSqlParser.COALESCEContext):
        self.enterFunction(ctx)
    def exitCOALESCE(self, ctx:TSqlParser.COALESCEContext):
        self.exitFunction(ctx)

    def enterCURRENT_TIMESTAMP(self, ctx:TSqlParser.CURRENT_TIMESTAMPContext):
        self.enterFunction(ctx)
    def exitCURRENT_TIMESTAMP(self, ctx:TSqlParser.CURRENT_TIMESTAMPContext):
        self.exitFunction(ctx)

    def enterDATEADD(self, ctx:TSqlParser.DATEADDContext):
        self.enterFunction(ctx)
    def exitDATEADD(self, ctx:TSqlParser.DATEADDContext):
        self.exitFunction(ctx)

    def enterDATEDIFF(self, ctx:TSqlParser.DATEDIFFContext):
        self.enterFunction(ctx)
    def exitDATEDIFF(self, ctx:TSqlParser.DATEDIFFContext):
        self.exitFunction(ctx)

    def enterDATENAME(self, ctx:TSqlParser.DATENAMEContext):
        self.enterFunction(ctx)
    def exitDATENAME(self, ctx:TSqlParser.DATENAMEContext):
        self.exitFunction(ctx)

    def enterDATEPART(self, ctx:TSqlParser.DATEPARTContext):
        self.enterFunction(ctx)
    def exitDATEPART(self, ctx:TSqlParser.DATEPARTContext):
        self.exitFunction(ctx)

    def enterNULLIF(self, ctx:TSqlParser.NULLIFContext):
        self.enterFunction(ctx)
    def exitNULLIF(self, ctx:TSqlParser.NULLIFContext):
        self.exitFunction(ctx)

    def enterSTUFF(self, ctx:TSqlParser.STUFFContext):
        self.enterFunction(ctx)
    def exitSTUFF(self, ctx:TSqlParser.STUFFContext):
        self.exitFunction(ctx)

    def enterISNULL(self, ctx:TSqlParser.ISNULLContext):
        self.enterFunction(ctx)
    def exitISNULL(self, ctx:TSqlParser.ISNULLContext):
        self.exitFunction(ctx)

    def enterIFF(self, ctx:TSqlParser.IFFContext):
        self.enterFunction(ctx)
    def exitIFF(self, ctx:TSqlParser.IFFContext):
        self.exitFunction(ctx)

    #LOCAL: Generic Tasks on Function entry/Exit
    def enterFunction(self, ctx):
        self.fn_nest_cnt += 1

    def exitFunction(self, ctx):
        self.fn_nest_cnt -= 1

        self.fn_tokens = []
        self.fn_spaces = []
        self.walkFunctionExpr(ctx)

        fn_new_str = TSql2Spark.convert(self.fn_tokens, self.fn_spaces, self.logger)

        #print("Converted Fun Str:",fn_new_str,self.fn_spaces)

        if self.fn_nest_cnt == 0 and self.intv_nest_cnt == 0 and self.cnct_nest_cnt == 0 and not(self.qlfy_expr):
            self.out_sql += fn_new_str
            self.fn_conv_txt[str(ctx.getSourceInterval()[0])] = fn_new_str.strip()
        else:
            self.fn_conv_txt[str(ctx.getSourceInterval()[0])] = fn_new_str.strip()

    def walkFunctionExpr(self, ctx):
        for child in ctx.getChildren():
            if isinstance(child, TerminalNodeImpl):
                tok_txt = child.getText()
                self.fn_tokens.append(tok_txt)
                hdn_tok = self.getLeftHiddenToken(child)
                self.fn_spaces.append(hdn_tok)
                #return hdn_tok+tok_txt##
            else:
                self.walkFunctionExpr(child)

    #LOCAL: Generic Task
    def getLeftHiddenToken(self,ctx):
        tok_pos = ctx.getSourceInterval()[0]
        hdn_tok = ''
        while tok_pos > 0 and \
                self.token_stream[tok_pos - 1].channel == 1 and \
                self.token_stream[tok_pos - 1].type == 787:  #This checks the condition TOKEN = SPACE
            hdn_tok = self.token_stream[tok_pos - 1].text + hdn_tok
            tok_pos -= 1
        return hdn_tok