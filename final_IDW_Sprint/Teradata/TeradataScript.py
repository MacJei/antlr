import re
import util
import Teradata.TeradataStatements as TS

class BTEQ():

    def __init__(self, cntx, fname):
        self.cntx = cntx
        self.fname = fname
        
        self.text = self.readScript()
        self.statements = self.loadStatements()        
    
    def readScript(self):
        file_path = self.cntx.source_dir+'\\'+self.fname
        with open(file_path,'r') as f:
            script_text = f.read()
            
        #if here doc exists
        if re.search(r'\bbteq\s*<<\s*(\w+)', script_text, flags = re.S|re.I):
            here_doc = re.findall(r'\bbteq\s*<<\s*(\w+).*?\n(.*?)\1',script_text, flags = re.S|re.I)
            script_text = here_doc[0][1]
        
        script_text = re.sub(r'/\*.*?\*/', '', script_text, flags = re.S)               #remove comment with /* */
        script_text = re.sub(r'--.*?(\r?\n)', r'\1', script_text)                       #remove comment with --
        script_text = re.sub(r'((\r?\n)[ \t]*){2,}', r'\1', script_text)                #remove blank lines
        script_text = re.sub(r'(^[ \t]*\.[^;]*?$)', r'\1;', script_text, flags = re.M)  #add semi-colon at at the end of BTEQ control statements 
        
        #remove locking for access string
        script_text = re.sub(r'\bLOCKING\b.*?\bFOR\b\s+ACCESS\b', '', script_text, flags=re.S|re.I)
        
        script_text = util.newLower(script_text)
        return script_text
    
    
    def loadStatements(self):
        statement_objs = []
        script_statements = util.newSplit(self.text, ';')   #split file content
    
        for stmt in script_statements:
            stmt = stmt.strip()
            if len(stmt) > 0:
                if re.match(r'^\s*\.[a-z]+\s+', stmt, re.S|re.I):
                    stmt_obj = TS.CtrlStmt(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.match(r'^SELECT', stmt, re.S|re.I):  
                    stmt_obj = TS.Select(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.match(r'CREATE\s+\w*\s*VOLATILE\s+TABLE\s+', stmt, re.S|re.I):
                    stmt_obj = TS.VolTbl(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.match(r'^INS(?:ERT)?\s+INTO\s+', stmt, re.S|re.I):   
                    stmt_obj = TS.Insert(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.match(r'^UPD(?:ATE)?\s+.*?\s+SET\s+', stmt, re.S|re.I):
                    stmt_obj = TS.Update(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.match(r'^DEL(?:ETE)?\s+(?:FROM)?\s*', stmt, re.S|re.I): 
                    stmt_obj = TS.Delete(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.match(r'^MERGE\s+.*?\s+USING\s+', stmt, re.S|re.I):
                    stmt_obj = TS.Merge(stmt)
                    statement_objs.append(stmt_obj)
                    
                elif re.match(r'^CALL\s+', stmt, re.S|re.I):
                    stmt_obj = TS.Call(stmt)
                    statement_objs.append(stmt_obj)
                    
                else:
                    #print('Not supported --- [', stmt, ']', sep= '')
                    pass
                
        return statement_objs
        
        
class PLSQL():
    pass
    
class JNIDX():
    pass