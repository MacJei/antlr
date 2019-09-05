from antlr4 import *
from SqlServer.parser.FullTSqlAntlrLexer import FullTSqlAntlrLexer
from SqlServer.parser.FullTSqlAntlrListener import FullTSqlAntlrListener
from SqlServer.parser.FullTSqlAntlrParser import FullTSqlAntlrParser
from antlr4.tree.Tree import TerminalNodeImpl
import datetime
import sys
import re
import os

class TSqlScriptParse(FullTSqlAntlrListener):
    def __init__(self, tokens:CommonTokenStream):
        self.out_script = ''
        self.last_token = ''    #to make sure not to add extra semi-colon if statement already ending with it
        self.upd_tbl_alias = []
        self.token_stream = tokens.tokens

    def exitDml_clause(self, ctx:FullTSqlAntlrParser.Dml_clauseContext):
        if self.last_token != ';':
            self.out_script += ';'
            self.last_token = ';'

    def exitDdl_clause(self, ctx:FullTSqlAntlrParser.Ddl_clauseContext):
        if self.last_token != ';':
            self.out_script += ';'
            self.last_token = ';'

    def exitCfl_statement(self, ctx:FullTSqlAntlrParser.Cfl_statementContext):
        if self.last_token != ';':
            self.out_script += ';'
            self.last_token = ';'
            
    def exitDbcc_clause(self, ctx:FullTSqlAntlrParser.Dbcc_clauseContext):
        if self.last_token != ';':
            self.out_script += ';'  
            self.last_token = ';'            
    
    def exitAnother_statement(self, ctx:FullTSqlAntlrParser.Another_statementContext):
        if self.last_token != ';':
            self.out_script += ';'
            self.last_token = ';'

    def exitBackup_statement(self, ctx:FullTSqlAntlrParser.Backup_statementContext):
        if self.last_token != ';':
            self.out_script += ';'
            self.last_token = ';'

    def exitGo_statement(self, ctx:FullTSqlAntlrParser.Go_statementContext):
        if self.last_token != ';':
            self.out_script += ';'
            self.last_token = ';'
            
    #def exitWith_expression(self, ctx:FullTSqlAntlrParser.With_expressionContext):
    #    if self.last_token != ';':
    #        self.out_script += ';'
    #        self.last_token = ';'

    def enterCommon_table_expression(self, ctx:FullTSqlAntlrParser.Common_table_expressionContext):
        if self.last_token == ',':
            self.out_script = self.out_script[:-1]
        
        if self.out_script[-4:].upper() != 'WITH':
            self.out_script += ' WITH '
    
    def exitCommon_table_expression(self, ctx:FullTSqlAntlrParser.Common_table_expressionContext):
        if self.last_token != ';':
            self.out_script += ';'
            self.last_token = ';'
      
    def exitSearch_condition(self, ctx:FullTSqlAntlrParser.Search_conditionContext):
        parent = ctx.parentCtx
        if (isinstance(parent, FullTSqlAntlrParser.If_statementContext) or isinstance(parent, FullTSqlAntlrParser.While_statementContext)):
            if self.last_token != ';':
                self.out_script += ';'
                self.last_token = ';' 
                
    def exitCreate_or_alter_broker_priority(self, ctx:FullTSqlAntlrParser.Create_or_alter_broker_priorityContext):  
        self.out_script += ' SP_END;'

    def exitIf_statement(self, ctx:FullTSqlAntlrParser.If_statementContext):
        self.out_script += ' END IF; '
            
    def exitWhile_statement(self, ctx:FullTSqlAntlrParser.While_statementContext):
        self.out_script += ' END WHILE; '

    def enterAs_table_alias(self, ctx:FullTSqlAntlrParser.As_table_aliasContext):
        alias = ctx.getText()
        self.upd_tbl_alias.append(alias)
    
    def getLeftHiddenToken(self,ctx):
        tok_pos = ctx.getSourceInterval()[0]
        hdn_tok = ''
        while tok_pos > 0 and self.token_stream[tok_pos - 1].channel == 1: # and self.token_stream[tok_pos - 1].text.strip() == '':
            hdn_tok = self.token_stream[tok_pos - 1].text + hdn_tok
            tok_pos -= 1
        return hdn_tok

    def visitTerminal(self, ctx:TerminalNodeImpl):
            tok_txt = ctx.getText()
            #if tok_txt.upper() in ['__LOAD__','__BLOCK__']:
            #    tok_txt = tok_txt.strip('_')
 
            if tok_txt != '<EOF>':
                hdn_tok = self.getLeftHiddenToken(ctx)    
                self.out_script += (hdn_tok + tok_txt)
                self.last_token = tok_txt
            
                if tok_txt.upper() in ['BEGIN', 'TRY', 'CATCH', 'TRANSACTION'] \
                    or (tok_txt.upper() in ['ELSE', 'END'] and not(isinstance(ctx.parentCtx, FullTSqlAntlrParser.Case_expressionContext))):
                    self.out_script += ';'
                    self.last_token = ';'
                    self.out_script = re.sub(r'(BEGIN|END)\s*;(\s+)(TRY|CATCH)\s*;',r'\1\2\3;', self.out_script, flags=re.S|re.I)
                    self.out_script = re.sub(r'(BEGIN|END|COMMIT|ROLLBACK)\s*;?(\s+TRANSACTION)\s*;(\s+\w+)\s*;?',r'\1\2\3;', self.out_script, flags=re.S|re.I)

