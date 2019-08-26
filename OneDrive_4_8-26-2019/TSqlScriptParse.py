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
        self.last_token = ''
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
            
    def exitWith_expression(self, ctx:FullTSqlAntlrParser.With_expressionContext):
        if self.last_token != ';':
            self.out_script += ';'
            self.last_token = ';'    
            
    def exitSearch_condition(self, ctx:FullTSqlAntlrParser.Search_conditionContext):
        parent = ctx.parentCtx
        if (isinstance(parent, FullTSqlAntlrParser.If_statementContext) or isinstance(parent, FullTSqlAntlrParser.While_statementContext)):
            if self.last_token != ';':
                self.out_script += ';'
                self.last_token = ';' 

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
            if tok_txt.upper() in ['__LOAD__','__BLOCK__']:
                tok_txt = tok_txt.strip('_')
 
            hdn_tok = self.getLeftHiddenToken(ctx)    
            self.out_script += (hdn_tok + tok_txt)
            self.last_token = tok_txt
            
            if tok_txt.upper() in ['BEGIN', 'TRY', 'CATCH', 'TRANSACTION'] or (tok_txt.upper() == 'END' and not(isinstance(ctx.parentCtx, FullTSqlAntlrParser.Case_expressionContext))):
                self.out_script += ';'
                self.last_token = ';'
                self.out_script = re.sub(r'(BEGIN|END)\s*;(\s+)(TRY|CATCH)\s*;',r'\1\2\3;', self.out_script, flags=re.S|re.I)
                self.out_script = re.sub(r'(BEGIN|END|COMMIT|ROLLBACK)\s*;?(\s+TRANSACTION)\s*;(\s+\w+)\s*;?',r'\1\2\3;', self.out_script, flags=re.S|re.I)

##------------------------------------------------------------------------------------------------------------------------------------------##                 
                
#Main

# if __name__ == '__main__':
    # for file in os.listdir('./test/'):
    
        # with open('./test/'+file, 'r') as fr:
            # in_script = fr.read()
        
        # in_script = re.sub(r'(\bLOAD\b|\bBLOCK\b)', r'__\1__', in_script, re.S|re.I)        
    
        # start_time = str(datetime.datetime.now())    
        # #print('start:',start_time)
        
        # lexer = FullTSqlAntlrLexer(InputStream(in_script))
        # stream = CommonTokenStream(lexer)
        # parser = FullTSqlAntlrParser(stream)
        # tree = parser.tsql_file()
        # conv = TSqlScriptParse(stream)
        # walker = ParseTreeWalker()
        # walker.walk(conv, tree)
        
        # end_time = str(datetime.datetime.now())    
        # #print('start:',end_time)
        
        # print(conv.out_script)
        # print(conv.upd_tbl_alias)

    
##------------------------------------------------------------------------------------------------------------------------------------------##    
    
    #Report Logic
    #in_script = in_script.strip()
    #line_count = in_script.count("\n") + 1
    #print(f"{file},{line_count},{start_time},{end_time}")
    
#data = ['usp_MergePremiseEnrollDetails_ODS.sql,170,2019-08-08 17:36:25.224517,2019-08-08 17:37:34.062975',
#'usp_LoadPremiseDetailsAgg.sql,238,2019-08-08 17:39:07.555375,2019-08-08 17:40:26.064921',
#'usp_Stage_TX_Build_PUC_GRT_Rates.sql,94,2019-08-08 17:54:20.997331,2019-08-08 17:55:31.626084',
#'usp_MergePremiseAddressDetails_ODS.sql,312,2019-08-08 17:57:45.007794,2019-08-08 18:00:15.385129',
#'usp_load_lt_esiid_level.sql,398,2019-08-08 18:02:19.981125,2019-08-08 18:06:30.729697',
#'P_ManageForecastdata.sql,631,2019-08-08 18:07:25.063079,2019-08-08 18:17:06.099263',
#'usp_MergePremiseDetails_ODS.sql,1333,2019-08-08 18:36:51.830720,2019-08-08 18:42:01.800207'] 
#
#for ele in data:
#    arr = ele.split(',')
#    st = datetime.datetime.strptime(arr[2],'%Y-%m-%d %H:%M:%S.%f')
#    et = datetime.datetime.strptime(arr[3],'%Y-%m-%d %H:%M:%S.%f')
#    diff = et - st
#    sec = diff.seconds
#    min = int(sec / 60)
#    sec = sec % 60
#    print(arr[0],arr[1],arr[2],arr[3],f"{min}:{sec}", sep = ',')
#
#    
#usp_MergePremiseEnrollDetails_ODS.sql,170,2019-08-08 17:36:25.224517,2019-08-08 17:37:34.062975,1:8
#usp_LoadPremiseDetailsAgg.sql,238,2019-08-08 17:39:07.555375,2019-08-08 17:40:26.064921,1:18
#usp_Stage_TX_Build_PUC_GRT_Rates.sql,94,2019-08-08 17:54:20.997331,2019-08-08 17:55:31.626084,1:10
#usp_MergePremiseAddressDetails_ODS.sql,312,2019-08-08 17:57:45.007794,2019-08-08 18:00:15.385129,2:30
#usp_load_lt_esiid_level.sql,398,2019-08-08 18:02:19.981125,2019-08-08 18:06:30.729697,4:10
#P_ManageForecastdata.sql,631,2019-08-08 18:07:25.063079,2019-08-08 18:17:06.099263,9:41
#usp_MergePremiseDetails_ODS.sql,1333,2019-08-08 18:36:51.830720,2019-08-08 18:42:01.800207,5:9    
