import re
import util
from antlr4 import *
from SqlServer.parser.FullTSqlAntlrLexer import FullTSqlAntlrLexer
from SqlServer.parser.FullTSqlAntlrListener import FullTSqlAntlrListener
from SqlServer.parser.FullTSqlAntlrParser import FullTSqlAntlrParser
from SqlServer.parser.XxErrorListener import TSqlErrorListener
from antlr4.error.ErrorListener import ErrorListener
from SqlServer.parser.FullTSqlScriptParse import TSqlScriptParse
import SqlServer.SqlServerStatements as sss


#TSql script class
class TSQL():

    def __init__(self, cntx, fname):
        self.cntx = cntx
        self.fname = fname
        self.upd_tbl_alias = []
        try:
            self.text = self.readScript()
            self.statements = self.loadStatements()
        except:
            raise
    
    #reads script content and prepare for parsing
    def readScript(self):
        
        #reading script contect
        self.cntx.logger.add_log('INFO', 'Reading script content')
        file_path = self.cntx.source_dir+'\\'+self.fname
                
        with open(file_path,'r') as f:
            script_text = f.read()
        self.cntx.logger.add_log('INFO', 'Number of lines are ' + str(script_text.count('\n')))
        
        #removing comment lines    
        self.cntx.logger.add_log('INFO', 'Removing comment lines from script content')
        script_text = re.sub(r'/\*.*?\*/', '', script_text, flags = re.S)               #remove comment with /* */
        script_text = re.sub(r'--.*?(\r?\n)', r'\1', script_text)                       #remove comment with --
        script_text = re.sub(r'((\r?\n)[ \t]*){2,}', r'\1', script_text)                #remove blank lines
        #check for error
        if ('/*' in script_text) or ('*/' in script_text) or ('--' in script_text):
            self.cntx.logger.add_log('ERROR', 'Unable to remove comments from script content')
        
        #separatte alias after ]
        script_text = re.sub(r'\](\w+)', r'] \1', script_text)
        
        #changing script content to lower case
        try:
            self.cntx.logger.add_log('INFO', 'Changing script content to lower case.')
            script_text = util.newLower(script_text)
        except Exception as e:
            self.cntx.logger.add_log('ERROR', 'Encountered error while changing script content to lower case.')
            self.cntx.logger.add_log_details(str(e))
        
        return script_text
    
    #parse script content and 
    def loadStatements(self):
        is_sp_stmt = False
        statement_objs = []
        
        self.cntx.logger.add_log('INFO', 'Starting process to split script content to individual statements.')
        #Antlr Parse
        try:        
            script_text = re.sub(r'\bLOAD\b', r'__LOAD__', \
                          re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                          re.sub(r'\bPLATFORM\b', r'__PLATFORM__', self.text, flags=re.S|re.I), \
                          flags=re.S|re.I), \
                          flags=re.S|re.I)       
            lexer = FullTSqlAntlrLexer(InputStream(script_text))
            lexer.removeErrorListeners()
            lexer.addErrorListener(TSqlErrorListener())
            stream = CommonTokenStream(lexer)
            parser = FullTSqlAntlrParser(stream)
            parser.removeErrorListeners()
            parser.addErrorListener(TSqlErrorListener())
            tree = parser.tsql_file()
            conv = TSqlScriptParse(stream)
            walker = ParseTreeWalker()
            walker.walk(conv, tree)
        except Exception as e:
            self.cntx.logger.add_log('ERROR', 'Failed to parse script content.')
            self.cntx.logger.add_log_details('Syntax error: ' + str(e))
            self.cntx.logger.add_log('WARN', 'Using unparsed script content. Result may be iscosistant.')
        else:
            self.cntx.logger.add_log('INFO', 'Script contect parse completed.')
            script_text = conv.out_script
            self.upd_tbl_alias = conv.upd_tbl_alias
        finally:
            script_text = script_text.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
         
        #split file content
        try:    
            self.cntx.logger.add_log('INFO', 'Splitting script content into individual statements based on semi-colon.')
            script_statements = util.newSplit(script_text, ';')
        except:
            raise Exception('Splitting script content into statement list failed.')
    
        self.cntx.logger.add_log('INFO', 'Identiying statements in scope of the converter')
        for stmt in script_statements:
            stmt = stmt.strip()
            if len(stmt) > 0:   
                #print('['+stmt+']\n')

                if re.search(r'^(?:\bCREATE\b|\bALTER\b)\s+\bPROC(?:EDURE)?\b', stmt, re.S|re.I):
                    is_sp_stmt = True
                    stmt_obj = sss.SPheader(stmt)
                    statement_objs.append(stmt_obj)
                    
                if stmt == 'SP_END':
                    is_sp_stmt = False

                elif re.search(r'^DECLARE\s+@', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.Declare(stmt)
                    statement_objs.append(stmt_obj) 
                    
                elif re.search(r'^(?:\bSET|\bSELECT)\s+@\w+\s*=', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.SetVar(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.search(r'^DECLARE\s+\w+\s+CURSOR|OPEN\s+\w+|^FETCH\s+\w+\s+FROM|^CLOSE\s+\w+', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.Cursor(stmt)
                    statement_objs.append(stmt_obj)
            
                elif re.search(r'^(BEGIN|END)\s+(TRY|CATCH)', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.ErrorHadling(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.search(r'^IF\s*\(?|^ELSE|^END IF', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.IfCondition(stmt)
                    statement_objs.append(stmt_obj) 
            
                elif re.search(r'^WHILE\s*\(?|^END WHILE', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.WhileLoop(stmt)
                    statement_objs.append(stmt_obj)                    
            
                elif re.search(r'^SELECT\s+|^WITH\s+', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.Select(stmt)
                    statement_objs.append(stmt_obj)
            
                elif re.search(r'^INSERT\s+(?:INTO\s+)?', stmt, re.S|re.I) and is_sp_stmt:   
                    stmt_obj = sss.Insert(stmt)
                    statement_objs.append(stmt_obj)
            
                elif re.search(r'^UPDATE\s+', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.Update(stmt)
                    statement_objs.append(stmt_obj)
            
                elif re.search(r'^DELETE\s+|^TRUNCATE\s+TABLE\s', stmt, re.S|re.I) and is_sp_stmt: 
                    stmt_obj = sss.Delete(stmt)
                    statement_objs.append(stmt_obj)
            
                elif re.search(r'^MERGE\s+', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.Merge(stmt)
                    statement_objs.append(stmt_obj)
            
                elif re.search(r'^EXEC(?:UTE)?\s+|^CALL \s+', stmt, re.S|re.I) and is_sp_stmt:
                    stmt_obj = sss.Execute(stmt)
                    statement_objs.append(stmt_obj)

                else:
                    if is_sp_stmt:
                        self.cntx.logger.add_log('WARN','Statement skipped. Not Supported.')
                    else:
                        self.cntx.logger.add_log('WARN','Statement skipped. Outside stored procedure definition.')
                    
                    self.cntx.logger.add_log_details(stmt)
                    
        return statement_objs
        
        