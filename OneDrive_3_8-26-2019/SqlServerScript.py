import re
import util
from antlr4 import *
from SqlServer.parser.FullTSqlAntlrLexer import FullTSqlAntlrLexer
from SqlServer.parser.FullTSqlAntlrListener import FullTSqlAntlrListener
from SqlServer.parser.FullTSqlAntlrParser import FullTSqlAntlrParser
from SqlServer.parser.TSqlScriptParse import TSqlScriptParse
import SqlServer.SqlServerStatements as sss

class TSQL():

    def __init__(self, cntx, fname):
        self.cntx = cntx
        self.fname = fname
        self.upd_tbl_alias = []
        self.text = self.readScript()
        self.original_text = self.text        
        self.statements = self.loadStatements()
    
    def readScript(self):
        self.cntx.logger.add_log('INFO', 'Reading file content')
        file_path = self.cntx.source_dir+'\\'+self.fname
        with open(file_path,'r') as f:
            script_text = f.read()
        self.cntx.logger.add_log('INFO', 'Number of lines are ' + str(script_text.count('\n')))
            
        self.cntx.logger.add_log('INFO', 'Removing Comment lines from script text')
        script_text = re.sub(r'/\*.*?\*/', '', script_text, flags = re.S)               #remove comment with /* */
        script_text = re.sub(r'--.*?(\r?\n)', r'\1', script_text)                       #remove comment with --
        script_text = re.sub(r'((\r?\n)[ \t]*){2,}', r'\1', script_text)                #remove blank lines
        
        self.cntx.logger.add_log('INFO', 'Changing Script text to lower case. ')
        script_text = util.newLower(script_text)
        return script_text
    
    
    def loadStatements(self):
        statement_objs = []
        
        self.cntx.logger.add_log('INFO', 'Starting process to split script content to individual statements.')
        
        #Antlr Parse
        try:        
            script_text = re.sub(r'(\bLOAD\b|\bBLOCK\b)', r'__\1__', self.text, re.S|re.I)
            lexer = FullTSqlAntlrLexer(InputStream(script_text))
            stream = CommonTokenStream(lexer)
            parser = FullTSqlAntlrParser(stream)
            tree = parser.tsql_file()
            conv = TSqlScriptParse(stream)
            walker = ParseTreeWalker()
            walker.walk(conv, tree)
        except Exception as e:
            self.cntx.logger.add_log('ERROR', 'Failed to parse script content')
            self.cntx.logger.add_log_details(e.__str__())
            self.cntx.logger.add_log('WARN', 'Using unparsed script content. Result may be iscosistant.')
            raise
        else:
            self.cntx.logger.add_log('INFO', 'File contect parse completed.')
            script_text = conv.out_script
            self.upd_tbl_alias = conv.upd_tbl_alias
        
        #split file content    
        self.cntx.logger.add_log('INFO', 'Splitting script content into individual statements based on semi-colon.')
        script_statements = util.newSplit(script_text, ';')  
    
        for stmt in script_statements:
            stmt = stmt.strip() 
            
            if len(stmt) > 0:   

                if re.search(r'^(?:\bCREATE\b|\bALTER\b)\s+\bPROCEDURE\b', stmt, re.S|re.I):
                    stmt_obj = sss.SPheader(stmt)
                    statement_objs.append(stmt_obj)
    
                elif re.search(r'^DECLARE\s+@', stmt, re.S|re.I):
                    stmt_obj = sss.Declare(stmt)
                    statement_objs.append(stmt_obj) 
                    
                elif re.search(r'^(?:\bSET|\bSELECT)\s+@\w+\s*=', stmt, re.S|re.I):
                    stmt_obj = sss.SetVar(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.search(r'^DECLARE\s+\w+\s+CURSOR|OPEN\s+\w+|^FETCH\s+\w+\s+FROM|^CLOSE\s+\w+', stmt, re.S|re.I):
                    stmt_obj = sss.Cursor(stmt)
                    statement_objs.append(stmt_obj)

                elif re.search(r'^(BEGIN|END)\s+(TRY|CATCH)', stmt, re.S|re.I):
                    stmt_obj = sss.ErrorHadling(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.search(r'^IF\s*\(?|^END IF', stmt, re.S|re.I):
                    stmt_obj = sss.IfCondition(stmt)
                    statement_objs.append(stmt_obj) 

                elif re.search(r'^WHILE\s*\(?|^END WHILE', stmt, re.S|re.I):
                    stmt_obj = sss.WhileLoop(stmt)
                    statement_objs.append(stmt_obj)                    
            
                elif re.search(r'^SELECT\s+|^WITH\s+', stmt, re.S|re.I):
                    stmt_obj = sss.Select(stmt)
                    statement_objs.append(stmt_obj)

                elif re.search(r'^INSERT\s+(?:INTO\s+)?', stmt, re.S|re.I):   
                    stmt_obj = sss.Insert(stmt)
                    statement_objs.append(stmt_obj)

                elif re.search(r'^UPDATE\s+', stmt, re.S|re.I):
                    stmt_obj = sss.Update(stmt)
                    statement_objs.append(stmt_obj)

                elif re.search(r'^DELETE\s+|^TRUNCATE\s+TABLE\s', stmt, re.S|re.I): 
                    stmt_obj = sss.Delete(stmt)
                    statement_objs.append(stmt_obj)

                elif re.search(r'^MERGE\s+', stmt, re.S|re.I):
                    stmt_obj = sss.Merge(stmt)
                    statement_objs.append(stmt_obj)

                elif re.search(r'^EXEC(?:UTE)?\s+|^CALL \s+', stmt, re.S|re.I):
                    stmt_obj = sss.Execute(stmt)
                    statement_objs.append(stmt_obj)

                else:
                    self.cntx.logger.add_log('WARN','Statement Not Supported.')
                    self.cntx.logger.add_log_details(stmt)
                    
        return statement_objs
        
        