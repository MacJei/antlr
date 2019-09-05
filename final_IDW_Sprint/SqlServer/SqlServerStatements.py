import re
import util
from antlr4 import *
from SqlServer.parser.TSqlLexer import TSqlLexer
from SqlServer.parser.TSqlListener import TSqlListener
from SqlServer.parser.TSqlParser import TSqlParser
from SqlServer.parser.XxTsqlListener import XxTsqlListener
from SqlServer.parser.XxErrorListener import TSqlErrorListener


class SqlServerStatements():
    def __init__(self, text):
        self.original_text = text
        self.text = text
        self.cnv_code = ''

    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'sqlserver':
            if cnv_ds.cntx.in_code_type == 'tsql':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    sig = self.scopeCheckChange(cnv_ds)
                    if sig == 'go':
                        cnv_code = self.toPyspark(cnv_ds)
                    if sig == 'nogo':
                        cnv_ds.cntx.logger.add_log('INFO', 'Skipping out of scope statement')
                        cnv_ds.cntx.logger.add_log_details(self.text)
                        cnv_code = ''
        return cnv_code  
        
    def replaceOperator(self, cnv_ds, cond_str):
        m_cond_str = re.sub(r'=', r'==', cond_str, flags=re.S|re.I)
        m_cond_str = re.sub(r'<>', r'!=', m_cond_str, flags=re.S|re.I)      
        m_cond_str = re.sub(r'\bIS\s+NULL\b', r'== None', m_cond_str, flags=re.S|re.I) 
        m_cond_str = re.sub(r'\bIS\s+NOT\s+NULL\b', r'!= None', m_cond_str, flags=re.S|re.I) 
        m_cond_str = re.sub(r'\bOR\b', r'or', m_cond_str, flags=re.S|re.I)    
        m_cond_str = re.sub(r'\bAND\b', r'and', m_cond_str, flags=re.S|re.I)
        m_cond_str = re.sub(r'@@FETCH_STATUS', r'fetch_status', m_cond_str, flags=re.S|re.I)
        cnv_ds.cntx.logger.add_log('INFO', 'Modified below conditinal expression from')
        cnv_ds.cntx.logger.add_log_details(cond_str + '\nto\n' + m_cond_str)
        return m_cond_str
    
    def replaceVariables(self, cnv_ds, stmt, quote = True):
        stmt = stmt.strip().strip(';')

        #remove square bracker from column names
        stmt = re.sub(r'\[(\w+)\]', r'\1', stmt, flags=re.S|re.I)
        
        #replace variables
        vars = re.findall(r'@\w+', stmt)
        if len(vars) > 0:
            cnv_ds.cntx.logger.add_log('INFO', 'Replacing variables found in statement ' + ','.join(set(vars)))
            fmt = '.format('
            for var in vars:
                if re.search(r'CHAR', cnv_ds.var_datatype[var], re.I):
                    stmt = stmt.replace(var, "'{}'")
                    fmt += var[1:] + ', '
                elif  re.search(r'TIME', cnv_ds.var_datatype[var], re.I):  
                    stmt = stmt.replace(var, "to_timestamp('{}')")
                    fmt += var[1:] + ', '  
                elif  re.search(r'DATE', cnv_ds.var_datatype[var], re.I):  
                    stmt = stmt.replace(var, "to_date('{}')")
                    fmt += var[1:] + ', '
                else:
                    stmt = stmt.replace(var, "{}")
                    fmt += var[1:] + ', '
            if quote:
                return '"""' + stmt + '"""' + fmt.strip()[:-1] + ')'
            else:
                return stmt + fmt.strip()[:-1] + ')'
        else:
            if quote:
                return '"""' + stmt + '"""' 
            else:
                return stmt     
            
    def scopeCheckChange(self, cnv_ds):
        stmt = self.text
                
        #check for CROSS/OUTER APPLY
        if re.search(r'(CROSS|OUTER)\s+APPLY', stmt, re.S|re.I):
            line = re.search(r'(CROSS|OUTER)\s+APPLY.*?\r?\n.*?\r?\n', stmt, re.S|re.I).group()
            cnv_ds.cntx.logger.add_log('WARN', 'CROSS/OUTER APPLY not in conversion scope')
            cnv_ds.cntx.logger.add_log_details(line)

        #[TODO] check for error functions
            
        #check for + operator
        if re.search(r'\+', stmt, re.S|re.I):
            plus_used = re.findall(r'[\),\s\(@\w\']+\+[\),\s\(@\w\']+', stmt, re.S|re.I)
            cnv_ds.cntx.logger.add_log('WARN', 'Check if + operator used for string concatination or date arithmetic.')
            cnv_ds.cntx.logger.add_log_details('\n'.join(plus_used))
            
            cnv_ds.cntx.logger.add_log('INFO', "Checking for hard coded string and converting to string contination operator '||'")
            stmt = re.sub(r'(\'\s*)\+', r'\1\|\|', stmt, flags=re.S).replace('\\','')
            stmt = re.sub(r'\+(\s*\')', r'\|\|\1', stmt, flags=re.S).replace('\\','')
            
        #check for WITH LOCK
        re_match = re.search(r'WITH?\s*\((NOLOCK|TABLOCK).*?\)', stmt, re.S|re.I)
        if re_match:
            str = re_match.group()
            cnv_ds.cntx.logger.add_log('WARN', f'{str} not supported. Removing from statement.')
            stmt = stmt.replace(str, '')
        
        #check for ( NOLOCK )
        re_match = re.search(r'\(\s*NOLOCK\s*\)', stmt, re.S|re.I)
        if re_match:
            str = re_match.group()
            cnv_ds.cntx.logger.add_log('WARN', f'{str} not supported. Removing from statement.')
            stmt = stmt.replace(str, '')
            
        #change truncate/drop to delete
        re_match = re.match(r'^(TRUNCATE)\s+TABLE\b', stmt, re.S|re.I)
        if re_match:
            cnv_ds.cntx.logger.add_log('INFO', f'Converting {re_match.group(1)} statement to DELETE statement')
            stmt = re.sub(r'\bTRUNCATE\s+TABLE\b', 'DELETE FROM', stmt, flags=re.S|re.I)
            cnv_ds.cntx.logger.add_log_details(self.text + '\nto\n' + stmt)
                        
        self.text = stmt
        return 'go'
            
#END class SqlServerStatements            

        
class Select(SqlServerStatements):
    def __init__(self, text):
        super().__init__(text)
    
    def toPyspark(self, cnv_ds):        
        cnv_code = ''
        stmt = self.text

        try:
            with_table_grp = re.match(r'WITH\s+(\w+)\s+AS\s*\((.*)\)$', stmt, re.S|re.I)
            into_grp = re.search(r'SELECT\s+(.*?)\bINTO\s+(.*?)\bFROM\s+(.*)', stmt, flags=re.S|re.I)
                        
            if with_table_grp:
                with_table = with_table_grp.group(1).strip()
                with_table_sql = with_table_grp.group(2).strip()
                cnv_ds.cntx.logger.add_log('INFO', f'Processing WITH clause Statement for {with_table}')
                                                
                #get table df name
                if with_table not in cnv_ds.table_df_map.keys():
                    with_table_df = re.sub(r'\.', '__', re.sub(r'\]|\[|#', '', with_table)) + '__df'
                    cnv_ds.table_df_map[with_table] = with_table_df
                else:
                    with_table_df = cnv_ds.table_df_map[with_table]
                
                stmt = with_table_sql
                select_df = with_table_df
            
            elif into_grp:
                into_tbl = into_grp.group(2).strip()
                cnv_ds.cntx.logger.add_log('INFO', f'Processing SELECT statement to poplate {into_tbl}')

                #get table df name
                if into_tbl not in cnv_ds.table_df_map.keys():
                    into_tbl_df = re.sub(r'\.', '__', re.sub(r'\]|\[|#', '', into_tbl)) + '__df'                
                    cnv_ds.table_df_map[into_tbl] = into_tbl_df
                else:
                    into_tbl_df = cnv_ds.table_df_map[into_tbl]
                    
                #get the SELECT query
                stmt = 'select ' + into_grp.group(1).strip() + ' from ' + into_grp.group(3).strip()
                select_df = into_tbl_df
            
            else:
                select_df = 'select__df'
            
            #Antlr Parse
            try:
                stmt = re.sub(r'\bLOAD\b', r'__LOAD__', \
                       re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                       re.sub(r'\bPLATFORM\b', r'__PLATFORM__', stmt, flags=re.S|re.I), \
                       flags=re.S|re.I), \
                       flags=re.S|re.I)            
                #stmt = re.sub(r'\bLOAD\b', r'__LOAD__', re.sub(r'\bBLOCK\b', r'__BLOCK__', stmt, flags=re.S|re.I), flags=re.S|re.I)
                lexer = TSqlLexer(InputStream(stmt))
                lexer.removeErrorListeners()
                lexer.addErrorListener(TSqlErrorListener())            
                stream = CommonTokenStream(lexer)
                parser = TSqlParser(stream)
                parser.removeErrorListeners()
                parser.addErrorListener(TSqlErrorListener())
                tree = parser.tsql_file()
                lsnr = XxTsqlListener(stream, cnv_ds.cntx.logger)
                walker = ParseTreeWalker()
                walker.walk(lsnr, tree)
            except Exception as e:
                cnv_ds.cntx.logger.add_log('ERROR', 'Failed to parse below statement')
                stmt = stmt.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                cnv_ds.cntx.logger.add_log_details(stmt)
                cnv_ds.cntx.logger.add_log_details('Syntax error: ' + str(e))
                cnv_ds.cntx.logger.add_log('WARN', 'Using unparsed statement. Result may be iscosistant.')                
            else:    
                stmt = lsnr.out_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')      
                cnv_ds.cntx.logger.add_log('INFO', 'Statement parse completed.')
                
            #table to dataframe and replace variables
            stmt = util.replaceTableWithDF(cnv_ds, stmt)
            stmt = self.replaceVariables(cnv_ds, stmt)
            
            cnv_code += "#Creating dataframe for select statement\n"
            cnv_code += f'{select_df} = spark.sql({stmt})\n'
            cnv_code += f"{select_df}.createOrReplaceTempView('{select_df}')\n"
                                
            if select_df == 'select__df':
                if re.search(r'\bFROM\b', stmt, re.S|re.I):
                    cnv_ds.cntx.logger.add_log('INFO', f'Processing SELECT statement')
                    cnv_code += f"rowcount_df = {select_df}\n"
                else:
                    cnv_ds.cntx.logger.add_log('ERROR', 'Select statement not supported')
                    cnv_ds.cntx.logger.add_log_details(self.original_text)
            
        except Exception as e:
            cnv_ds.cntx.logger.add_log('ERROR','Error occurred while converting the following SELECT statement,')
            cnv_ds.cntx.logger.add_log_details(self.original_text)
            cnv_ds.cntx.logger.add_log_details(str(e))
            cnv_code = ''
            #raise

        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)

#END class Select        


class Insert(SqlServerStatements):
    def __init__(self, text):
        super().__init__(text)
               
    def toPyspark(self, cnv_ds):
        stmt = self.text
        cnv_code = ''
        
        #Antlr Parse
        try:
            stmt = re.sub(r'\bLOAD\b', r'__LOAD__', \
                   re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                   re.sub(r'\bPLATFORM\b', r'__PLATFORM__', stmt, flags=re.S|re.I), \
                   flags=re.S|re.I), \
                   flags=re.S|re.I)
            #stmt = re.sub(r'\bLOAD\b', r'__LOAD__', re.sub(r'\bBLOCK\b', r'__BLOCK__', stmt, flags=re.S|re.I), flags=re.S|re.I)
            lexer = TSqlLexer(InputStream(stmt))
            lexer.removeErrorListeners()
            lexer.addErrorListener(TSqlErrorListener())            
            stream = CommonTokenStream(lexer)
            parser = TSqlParser(stream)
            parser.removeErrorListeners()
            parser.addErrorListener(TSqlErrorListener())
            tree = parser.tsql_file()
            lsnr = XxTsqlListener(stream, cnv_ds.cntx.logger)
            walker = ParseTreeWalker()
            walker.walk(lsnr, tree)
        except Exception as e:
            cnv_ds.cntx.logger.add_log('ERROR', 'Failed to parse below statement')
            stmt = stmt.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform') + ';'
            cnv_ds.cntx.logger.add_log_details(stmt)
            cnv_ds.cntx.logger.add_log_details('Syntax error: ' + str(e))
            cnv_ds.cntx.logger.add_log('WARN', 'Using unparsed statement. Result may be iscosistant.')                
        else:    
            stmt = lsnr.out_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform') + ';'     
            cnv_ds.cntx.logger.add_log('INFO', 'Statement parse completed.')
                        
        #get table & DB name where records will be inserted
        ins_sel = re.match(r'^\bINSERT\s+(?:INTO\s+)?([\w\[\]#\.]+)\s*', stmt, re.S|re.I)
        ins_val = re.match(r'^\bINSERT\s+(?:INTO\s+)?([\w\[\]#\.]+).*?\bVALUES\s*\(', stmt, re.S|re.I)
        
        if ins_sel:         
            try:
                dest_tbl = ins_sel.group(1).strip()
                cnv_ds.cntx.logger.add_log('INFO', f'Converting INSERT INTO statement by SELECT. Inserting into {dest_tbl}')
                if dest_tbl:
                    #get table df name
                    table_df = cnv_ds.table_df_map[dest_tbl]
                
                #Check if Select is present in Source of the Insert statement 
                src_select = re.search(dest_tbl + r'(.*)[\(]?\s*\bSELECT\s+(.)*\bFROM\s+((\w*)[\.]?(\w*)[\.]?(\w*))', stmt, re.S|re.I)
                if src_select:
                    #Check if no column name is specified , only Select * is present
                    select_star = re.search(dest_tbl + r'\s*[\(]?\s*\bSELECT\s+[\*\s]?\s*\bFROM\s+((\w*)[\.]?(\w*)[\.]?(\w*))', stmt, re.S|re.I)
                    if select_star:
                        #get the select * string
                        select_str = re.split("SELECT",stmt,1, re.I)     
                        if select_str.strip()[-1] == ';':
                            select_str = select_str.strip()[:-1]
                        select_str = ('select' + (select_str[1])).strip().strip(')')                           
                    else:
                        #when column names are specified in the select list
                        if re.search(r'(.)\s*SELECT\b', stmt, re.S|re.I).group(1) == '(':
                        #replace starting bracket with special string
                            stmt = re.sub(r'\((\s*)(SELECT\b)', r'‹\1\2', stmt, 1, flags=re.S|re.I)
                        
                        #replace FROM keyword with special character 
                        stmt = re.sub(r"\bFROM\b", 'ƒ', stmt, flags=re.S|re.I)
                        
                        #custom split insert statement 
                        stmt_part = util.newSplit(stmt, 'ƒ')
                      
                        #Keeping the first part of stmt_part[0] and appending rest of the array strings in stmt_part[1]
                        for i in range(2,len(stmt_part)):
                             stmt_part[1]=stmt_part[1]+' ƒ '+stmt_part[i]
                    
                        #Replacing special character to from in first part of the split
                        stmt_part_1 = re.sub(r'ƒ', 'from', re.sub(r'‹', r'(', stmt_part[0], flags=re.S|re.I), flags=re.S|re.I)
                        #Replacing special character to from in second part of the split
                        stmt_part_2 = re.sub(r'ƒ', 'from', re.sub(r'‹', r'(', stmt_part[1], flags=re.S|re.I), flags=re.S|re.I)
                    
                        #get Destination table column list
                        col_list_str = re.search(dest_tbl + r'\s*\((.*?)\)[\(\s]*\bSELECT\b', stmt_part_1, re.S|re.I).group(1)
                        col_list = util.newSplit(col_list_str, ',')
                    
                        #get insert values from the select list
                        val_list_str = re.search(r'\bSELECT\b\s+(.*)', stmt_part_1, re.S|re.I).group(1)
                        val_list = util.newSplit(val_list_str, ',')
                    
                        cnct_str = ''
                        for i in range(len(col_list)):
                            col = col_list[i].strip()
                            val = val_list[i].strip()
                            cnct_str += val + ' as ' + col + ',\n'    
                        cnct_str = cnct_str[:-2]
                        select_str = 'select ' + cnct_str + "\nfrom " + stmt_part_2
                    
                    #table to dataframe and replace variables
                    select_str = util.replaceTableWithDF(cnv_ds, select_str)
                    select_str = self.replaceVariables(cnv_ds, select_str)
                    
                    table_df_tmp = table_df + '_1'
                    #pyspark code to remove deleted records from data frame
                    cnv_code += f"{table_df_tmp} = spark.sql({select_str})\n"
                    cnv_code += f"mod_df['{table_df}'] = mod_df['{table_df}'].union({table_df_tmp})\n"
                    cnv_code += f"mod_df['{table_df}'].createOrReplaceTempView('{table_df}')\n" 
                    cnv_code += f"rowcount_df = {table_df_tmp}\n\n"                    

            except Exception as e:
                cnv_ds.cntx.logger.add_log('ERROR','Error occurred while converting below INSERT statement,')
                cnv_ds.cntx.logger.add_log_details(self.original_text)  
                cnv_ds.cntx.logger.add_log_details(str(e))                 
                cnv_code = ''
        
        if ins_val:
            try:
                #get tablename, column and value list
                match_grp = re.match(r'INSERT\s+(?:INTO\s+)?([\w\[\]#\.]+)\s+\(?(.*?)\)?\s*VALUES\s*\((.*?)\);', stmt, re.S|re.I)
                table = match_grp.group(1).strip()
                cols = match_grp.group(2).strip()
                vals = match_grp.group(3).strip()
                
                cnv_ds.cntx.logger.add_log('INFO',f'Converting INSERT INTO statement by VALUE. Inserting into {table}')
                
                #get table df name
                table_df = cnv_ds.table_df_map[table]
    
                #create insert column list
                if cols:
                    cols = re.sub(r'\s', '', cols, flags=re.S|re.I)
                    col_list = util.newSplit(cols, ',')
                else:
                    cnv_ds.cntx.logger.add_log('ERROR','Can not convert insert statement if column list not specified')
    
                #create insert value list
                vals = re.sub(r'[\r\n]+', '', vals, flags=re.S|re.I) 
                val_list = util.newSplit(vals, ',')
    
                #create select statement
                select_str = 'select '
                for i in range(len(col_list)):
                    select_str += val_list[i] + ' as ' + col_list[i] + ',\n'
                select_str = select_str[:-2]
                
                #replace db table names with corresponding data frame name
                select_str = util.replaceTableWithDF(cnv_ds, select_str)
                select_str = self.replaceVariables(cnv_ds, select_str)
                    
                table_df_tmp = table_df + '_1'
                #pyspark code to remove deleted records from data frame
                cnv_code += f"{table_df_tmp} = spark.sql({select_str})\n"
                cnv_code += f"mod_df['{table_df}'] = mod_df['{table_df}'].union({table_df_tmp})\n"
                cnv_code += f"mod_df['{table_df}'].createOrReplaceTempView('{table_df}')\n" 
                cnv_code += f"rowcount_df = {table_df_tmp}\n\n"

            except Exception as e:
                cnv_ds.cntx.logger.add_log('ERROR','Error occurred while converting below INSERT statement,')
                cnv_ds.cntx.logger.add_log_details(self.original_text)
                cnv_ds.cntx.logger.add_log_details(str(e))                
                cnv_code = ''
            
        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)    
        
#END class Insert

        
class Delete(SqlServerStatements):
    def __init__(self, text):
        super().__init__(text)
        
    def toPyspark(self, cnv_ds):  
        cnv_code = '' 
        stmt = self.text
        try:
            #add keyword FROM if not present
            if not(re.search(r'\bDELETE\s+\bFROM\b', stmt, re.S|re.I)):
                stmt = re.sub(r'(\bDELETE\s+)', r'\1from ', stmt, flags=re.S|re.I)            
            
            #Antlr Parse
            try:
                stmt = re.sub(r'\bLOAD\b', r'__LOAD__', \
                       re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                       re.sub(r'\bPLATFORM\b', r'__PLATFORM__', stmt, flags=re.S|re.I), \
                       flags=re.S|re.I), \
                       flags=re.S|re.I)              
                #stmt = re.sub(r'\bLOAD\b', r'__LOAD__', re.sub(r'\bBLOCK\b', r'__BLOCK__', stmt, flags=re.S|re.I), flags=re.S|re.I)
                lexer = TSqlLexer(InputStream(stmt))
                lexer.removeErrorListeners()
                lexer.addErrorListener(TSqlErrorListener())            
                stream = CommonTokenStream(lexer)
                parser = TSqlParser(stream)
                parser.removeErrorListeners()
                parser.addErrorListener(TSqlErrorListener())
                tree = parser.tsql_file()
                lsnr = XxTsqlListener(stream, cnv_ds.cntx.logger)
                walker = ParseTreeWalker()
                walker.walk(lsnr, tree)
            except Exception as e:
                cnv_ds.cntx.logger.add_log('ERROR', 'Failed to parse below statement')
                stmt = stmt.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                cnv_ds.cntx.logger.add_log_details(stmt)
                cnv_ds.cntx.logger.add_log_details('Syntax error: ' + str(e))
                cnv_ds.cntx.logger.add_log('WARN', 'Using unparsed statement. Result may be iscosistant.')                
            else:    
                stmt = lsnr.out_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                cnv_ds.cntx.logger.add_log('INFO', 'Statement parse completed.')            
            
            #get table name where records will be delete
            table = re.search(r'\bDELETE\s+FROM\s+([\w\[\]#\.]+)', stmt, re.S|re.I).group(1)
            table_re = re.sub(r'(\]|\.|\[)', r'\\\1', table)
            #get data frame name of the table
            table_df = cnv_ds.table_df_map[table]
            
            cnv_ds.cntx.logger.add_log('INFO',f'Converting DELETE statement on {table}')
    
            from_str = ''
            where_str = ''
            subtract_sql = ''
            
            #check if delete statement has where clause
            if re.search(r'\bWHERE\b', stmt, re.S|re.I):
                #get the where condition
                where_str = re.search(r'WHERE(.*)', stmt, re.S|re.I).group(1).strip()
                #get from clause statement
                from_str = re.search(r'(?<=FROM)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I).group(1).strip()
            else:
                #in case of no where clause in delete statement
                where_str = ''
                #get from clause statement
                from_str = re.search(r'(?<=FROM)\s+(.*)', stmt, re.S|re.I).group(1).strip()
                        
            #check if from clause has multiple tables
            if re.search(r'\,', from_str, re.S|re.I):
                #break from clause in 2 parts. part 1: first table. part 2: rest of the from clause
                first_table, other_table = re.search(r'^(.*?)(\,.*?$)', from_str, re.S|re.I).groups()
                first_table = first_table.strip()
                other_table = other_table.strip()
                #get alias of the table
                alias_match = re.search(r'{}\s*(?:AS\s+)?(\w*)\s*'.format(table_re), from_str, re.S|re.I)
                if alias_match is None or alias_match.group(1).upper() == 'WHERE': 
                    alias = ''
                else:
                    alias = alias_match.group(1).strip()
                #select statement to get records to be deleted
                subtract_sql = f'select {alias}.* \nfrom {from_str} \nwhere {where_str}'
            else:
                #get alias of the table
                alias_match = re.search(r'{}\s*(?:AS\s+)?(\w*)\s*'.format(table_re), from_str, re.S|re.I)
                if alias_match is None or alias_match.group(1).upper() == 'WHERE': 
                    alias = ''
                else:
                    alias = alias_match.group(1).strip()
                    
                #select statement to get records to be deleted.
                subtract_sql = 'select ' + (f'{alias}.*' if alias else '*') + f'\nfrom {from_str}' + (f'\nwhere {where_str}' if where_str else '')
                
            #replace db table names with corresponding data frame name
            subtract_sql = util.replaceTableWithDF(cnv_ds, subtract_sql)
            subtract_sql = self.replaceVariables(cnv_ds, subtract_sql)
                
            table_df_tmp = table_df + '_1'
            #pyspark code to remove deleted records from data frame
            cnv_code += f"{table_df_tmp} = spark.sql({subtract_sql})\n"
            cnv_code += f"mod_df[\'{table_df}\'] = mod_df[\'{table_df}\'].subtract({table_df_tmp})\n"
            cnv_code += f"mod_df[\'{table_df}\'].createOrReplaceTempView('{table_df}')\n"
            cnv_code += f"rowcount_df = {table_df_tmp}\n\n"

        except Exception as e:
            cnv_ds.cntx.logger.add_log('ERROR','Error occurred while converting below DELETE statement,')
            cnv_ds.cntx.logger.add_log_details(self.original_text)  
            cnv_ds.cntx.logger.add_log_details(str(e))           
            cnv_code = ''
            raise
            
        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)

#END class Delete

        
class Update(SqlServerStatements):
    def __init__(self, text):
        super().__init__(text)
       
    def toPyspark(self, cnv_ds):
        cnv_code = ''
        stmt = self.text
        
        #Antlr Parse
        try:
            stmt = re.sub(r'\bLOAD\b', r'__LOAD__', \
                   re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                   re.sub(r'\bPLATFORM\b', r'__PLATFORM__', stmt, flags=re.S|re.I), \
                   flags=re.S|re.I), \
                   flags=re.S|re.I)            
            #stmt = re.sub(r'\bLOAD\b', r'__LOAD__', re.sub(r'\bBLOCK\b', r'__BLOCK__', stmt, flags=re.S|re.I), flags=re.S|re.I)
            lexer = TSqlLexer(InputStream(stmt))
            lexer.removeErrorListeners()
            lexer.addErrorListener(TSqlErrorListener())            
            stream = CommonTokenStream(lexer)
            parser = TSqlParser(stream)
            parser.removeErrorListeners()
            parser.addErrorListener(TSqlErrorListener())
            tree = parser.tsql_file()
            lsnr = XxTsqlListener(stream, cnv_ds.cntx.logger)
            walker = ParseTreeWalker()
            walker.walk(lsnr, tree)
        except Exception as e:
            cnv_ds.cntx.logger.add_log('ERROR', 'Failed to parse below statement')
            stmt = stmt.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform') + ';'
            cnv_ds.cntx.logger.add_log_details(stmt)
            cnv_ds.cntx.logger.add_log_details('Syntax error: ' + str(e))
            cnv_ds.cntx.logger.add_log('WARN', 'Using unparsed statement. Result may be iscosistant.')                
        else:    
            stmt = lsnr.out_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform') + ';'    
            cnv_ds.cntx.logger.add_log('INFO', 'Statement parse completed.')        
        
        cnv_ds.cntx.logger.add_log("INFO","Update Statement conversion started") 
        
        ###
        try:  
            tbl_alias = re.search(r'\bUPDATE\s+(.*?)\s+(?=SET)', stmt, re.S|re.I).group(1).strip()
            cnv_ds.cntx.logger.add_log("INFO","Table alias name to be referenced :")
            cnv_ds.cntx.logger.add_log_details(tbl_alias)
            try:
                upd_set = re.search(r'(?<=SET)\s+(.*?);', stmt, re.S|re.I).group(1).strip()
                cnv_ds.cntx.logger.add_log("INFO","Statement after 'SET' clause :")
                cnv_ds.cntx.logger.add_log_details(upd_set)
        
                # Searching for the table alias we earlier stored, after "SET"
                if tbl_alias in upd_set:
                    cnv_ds.cntx.logger.add_log( "INFO","Table alias present after the 'SET' clause , indicating possibility of 'FROM' clause  ")
        
                    # Checking if "FROM" exists before "SELECT" :
                    if re.search(r'SELECT|FROM', upd_set, re.S|re.I).group().strip() in ('FROM', 'from'):
                        cnv_ds.cntx.logger.add_log( "INFO", "'FROM' clause exists before 'SELECT'")
                        
                        # get table ALIAS name that will be updated
                        upd_table_alias = re.search(r'\bUPDATE\s+(.*?)\s+(?=SET)', stmt, re.S|re.I).group(1).strip()
                        cnv_ds.cntx.logger.add_log( "INFO","Table ALIAS name that will be updated :")
                        cnv_ds.cntx.logger.add_log_details(upd_table_alias)
        
                        # get list of tables involved in update statement
                        if re.search(r'(?<=FROM)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I):
                            upd_from_str = re.search(r'(?<=FROM)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I).group(1).strip()
                        else:
                            upd_from_str = re.search(r'(?<=FROM)\s+(.*?);', stmt, re.S|re.I).group(1).strip()
                        cnv_ds.cntx.logger.add_log( "INFO", "List of tables involved in update statement :")
                        cnv_ds.cntx.logger.add_log_details(upd_from_str)
        
                        # get table name that will be updated
                        upd_table = re.search(r'([\w\[\]#\.]+)\s+(?:AS\s+)?{}'.format(upd_table_alias), upd_from_str, re.S|re.I).group(1).strip()
                        cnv_ds.cntx.logger.add_log( "INFO", "Table name that will be updated :")
                        cnv_ds.cntx.logger.add_log_details(upd_table)
        
                        # get the set section of update statement
                        upd_set_str = re.search(r'(?<=SET)\s+(.*?)\s+(?=FROM)', stmt, re.S|re.I).group(1).strip()
        
                        # get the where section of update statement
                        if re.search(r'(?<=FROM)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I):        
                            upd_where_str = re.search(r'\bFROM\b.*?\bWHERE\b(.*?);', stmt, re.S|re.I).group(1).strip()
                        else:
                            upd_where_str = ''
                        cnv_ds.cntx.logger.add_log("INFO", "Statements preset after 'WHERE Clause:")
                        cnv_ds.cntx.logger.add_log_details(upd_where_str)
        
                        # make sql to select records that will be updated
                        if upd_where_str == '':        
                            subtract_sql = f"select {upd_table_alias}.* \n from {upd_from_str}"
                        else:
                            subtract_sql = f"select {upd_table_alias}.* \n from {upd_from_str} \n where {upd_where_str}"
                        cnv_ds.cntx.logger.add_log("INFO","Subtract SQL Statement :")
                        cnv_ds.cntx.logger.add_log_details(subtract_sql)
        
                    # Checking if "WHERE" exists before SELECT :
                    elif re.search(r'WHERE|SELECT', upd_set, re.S|re.I).group().strip() in ('WHERE', 'where'):

                    #-o-# getting the table name        
                        upd_table = re.search(r'(?<=UPDATE)\s+([\w\[\]#\.]+)', stmt, re.S|re.I).group().strip()
                        cnv_ds.cntx.logger.add_log("INFO", "Table name that will be updated : ")
                        cnv_ds.cntx.logger.add_log_details(upd_table)
                        
                        #get table alias
                        upd_table_alias = upd_table.split(']')[-1]
                        if upd_table_alias:
                            upd_table_alias = upd_table_alias.split()[-1]
                            if upd_table == upd_table_alias:
                                upd_table_alias = ''                                
                        else:    
                            upd_table_alias = upd_table_alias.replace('as', '').strip()
                        cnv_ds.cntx.logger.add_log( "INFO","Table ALIAS name that will be updated :")
                        cnv_ds.cntx.logger.add_log_details(upd_table_alias)

                        upd_from_str = upd_table
        
                        # get where part
                        upd_where_str = re.search(r'\bSET\b.*?\bWHERE\b(.*)', stmt, re.S|re.I).group(1).strip()
                        cnv_ds.cntx.logger.add_log("INFO", "Statements preset after 'WHERE Clause:")
                        cnv_ds.cntx.logger.add_log_details(upd_where_str)
        
                        # get the set section of update        
                        upd_set_str = re.search(r'(?<=SET)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I).group(1).strip()
                        cnv_ds.cntx.logger.add_log("INFO", "SET section of update :")
                        cnv_ds.cntx.logger.add_log_details(upd_set_str)
        
                        # make sql to select records that will be updated
                        subtract_sql = f"select * \n from {upd_from_str} \n where {upd_where_str}"
                        cnv_ds.cntx.logger.add_log("INFO", "Subtract SQL Statement :")
                        cnv_ds.cntx.logger.add_log_details(subtract_sql)
                    else:        
                        sel_part = re.search(r'\s.*?\w.*?(?=SELECT)', stmt, re.S|re.I).group().strip()

                        # checking for number of select statements        
                        var_sel = 'SELECT'
                        num_sel_part = re.findall(r'SELECT', stmt, re.S|re.I)
                        upd_sel_index = stmt.index(var_sel)
        
                        # extracting part after first select
                        upd_sel_from_str = stmt[upd_sel_index + 8:]
                        var_1 = 'FROM'
                        var_2 = 'WHERE'
        
                        # Getting the index for "FROM"
                        upd_frm_index = stmt.index(var_1)
                        upd_from_str_index = stmt[upd_frm_index + 5:]
        
                        # Getting the index for "WHERE"
                        upd_where_index = stmt.index(var_2)
                        where_str = stmt[upd_where_index + 6:]
        
                        # Checking if number of selects >=2
                        if len(num_sel_part) >= 2:
                            frm_part = re.findall(r'FROM', upd_sel_from_str, re.S|re.I)
                            whr_part = re.findall(r'WHERE', upd_sel_from_str, re.S|re.I)
        
                            # checking first for FROM ,if null then  WHERE and finally if both not present then "SELECT":
                            if re.search(r'FROM|WHERE|SELECT', upd_from_str_index, re.S|re.I).group().strip() in ('from', 'FROM'):
                                # get table alias which will be updated        
                                upd_table_alias = re.search(r'\bUPDATE\s+(.*?)\s+(?=SET)', stmt, re.S|re.I).group(1).strip()
                                cnv_ds.cntx.logger.add_log("INFO","Table ALIAS name that will be updated :")
                                cnv_ds.cntx.logger.add_log_details(upd_table_alias)
        
                                # get list of tables involved in update statement
                                if re.search(r'(?<=FROM)\s+(.*?)\s+(?=WHERE)', upd_from_str_index, re.S|re.I):        
                                    upd_from_str = re.search(r'(?<=FROM)\s+(.*?)\s+(?=WHERE)', upd_from_str_index, re.S|re.I).group(1).strip()
                                else:
                                    upd_from_str = re.search(r'(?<=FROM)\s+(.*?);', stmt, re.S|re.I).group(1).strip()
                                cnv_ds.cntx.logger.add_log("INFO","List of tables involved in update statement :")
                                cnv_ds.cntx.logger.add_log_details(upd_from_str)
        
                                # get table name that will be updated
                                upd_table = re.search(r'([\w\[\]#\.]+)\s*(?:AS\s+)?{}'.format(upd_table_alias), upd_from_str, re.S|re.I).group(1).strip()
                                cnv_ds.cntx.logger.add_log("INFO","Table name that will be updated :")
                                cnv_ds.cntx.logger.add_log_details(upd_table)
        
                                # after from part excluding the select part present in SET
                                upd_after_frm = re.search('(?<=FROM)\s.*\w.*', upd_from_str_index, re.S|re.I).group().strip()
        
                                # Only keeping the part of update statement before "from" :        
                                upd_set_str_rep = stmt.replace(upd_after_frm, ';')
        
                                # get the set section of update statement
                                upd_set_str = re.search(r'(?<=SET)\s+(.*)\s+(?=FROM)', upd_set_str_rep, re.S|re.I).group(1).strip()
                                cnv_ds.cntx.logger.add_log("INFO","SET section of update :")
                                cnv_ds.cntx.logger.add_log_details(upd_set_str)
        
                                # get the where section of update statement
                                if re.search(r'(?<=FROM)\s+(.*?)\s+(?=WHERE)', upd_from_str_index, re.S|re.I):
                                    upd_where_str = re.search(r'\bFROM\b.*?\bWHERE\b(.*?);', upd_from_str_index, re.S|re.I).group(1).strip()
                                else:
                                    upd_where_str = ''
                                cnv_ds.cntx.logger.add_log("INFO","Statements preset after 'WHERE Clause:")
                                cnv_ds.cntx.logger.add_log_details(upd_where_str)
        
                                # make sql to select records that will be updated
                                if upd_where_str == '':
                                    subtract_sql = f"select {upd_table_alias}.* \n from {upd_from_str}"        
                                else:
                                    subtract_sql = f"select {upd_table_alias}.* \n from {upd_from_str} \n where {upd_where_str}"        
                                cnv_ds.cntx.logger.add_log("INFO","Subtract SQL Statement :")
                                cnv_ds.cntx.logger.add_log_details(subtract_sql)
        
                            # Checking if WHERE exists before Select after the first select  present in SET BLOCK :
                            else:
                                # get table name that will be updated
                                upd_table = re.search(r'(?<=UPDATE)\s+([\w\[\]#\.]+)', stmt, re.S|re.I).group().strip()
                                cnv_ds.cntx.logger.add_log("INFO","Table name that will be updated :")
                                cnv_ds.cntx.logger.add_log_details(upd_table)
                                upd_table_alias = upd_table
                                upd_from_str = upd_table
                
                                # get the set section of update
                                upd_set_str = re.search(r'(?<=SET)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I).group(1).strip()
                                cnv_ds.cntx.logger.add_log("INFO","SET section of update :")
                                cnv_ds.cntx.logger.add_log_details(upd_set_str)
        
                                # get the where section of update statement
                                upd_where_str = re.search(r'\bSET\b.*?\bWHERE\b(.*)', stmt, re.S|re.I).group(1).strip()
                                cnv_ds.cntx.logger.add_log("INFO","Statements preset after 'WHERE Clause:")
                                cnv_ds.cntx.logger.add_log_details(upd_where_str)
                                upd_from_str = upd_table_alias
        
                                # make sql to select records that will be updated
                                subtract_sql = f"select {upd_table_alias}.* \n from {upd_from_str} \n where {upd_where_str}"
                                cnv_ds.cntx.logger.add_log("INFO","Subtract SQL Statement :")
                                cnv_ds.cntx.logger.add_log_details(subtract_sql)
        
                        # SELECT only exists in the column section
                        else:
                            upd_table = re.search(r'\bUPDATE?\s+([\w\[\]#\.]+)', stmt, re.S|re.I).group(1).strip()
                            cnv_ds.cntx.logger.add_log("INFO","Table name that will be updated :")
                            cnv_ds.cntx.logger.add_log_details(upd_table)
        
                            upd_table_alias = upd_table
                            upd_from_str = upd_table
        
                            upd_where_str = ''
                            cnv_ds.cntx.logger.add_log("INFO", f"Statements preset after 'WHERE Clause:")
                            cnv_ds.cntx.logger.add_log_details(upd_where_str)
        
                            upd_set_str = re.search(r'(?<=SET)\s+(\w.*);', stmt, re.S|re.I).group(1).strip()
                            cnv_ds.cntx.logger.add_log("INFO","SET section of update :")
                            cnv_ds.cntx.logger.add_log_details(upd_set_str)
        
                            subtract_sql = f"select {upd_table_alias}.* \n from {upd_from_str}"
                            cnv_ds.cntx.logger.add_log("INFO","Subtract SQL Statement :")
                            cnv_ds.cntx.logger.add_log_details(subtract_sql)
        
                        # make sql to select records that will be updated
                        if upd_where_str == '':
                            subtract_sql = f"select {upd_table_alias}.* \n from {upd_from_str}"
                        else:
                            subtract_sql = f"select {upd_table_alias}.* \n from {upd_from_str} \n where {upd_where_str}"
                        cnv_ds.cntx.logger.add_log("INFO","Subtract SQL Statement :")
                        cnv_ds.cntx.logger.add_log_details(subtract_sql)
        
                    #>-o-<   
                    subtract_sql = util.replaceTableWithDF(cnv_ds, subtract_sql)
                    subtract_sql = self.replaceVariables(cnv_ds, subtract_sql)        
                
                    # get data frame name of the table
                    try:
                        upd_table_df = cnv_ds.table_df_map[upd_table]
                        upd_table_df_tmp_1 = upd_table_df + '_1'
                        upd_table_df_tmp_2 = upd_table_df + '_2'
                    except:
                        cnv_ds.cntx.logger.add_log("ERROR","Table to be updated not present in table_df_map")
                        cnv_ds.cntx.logger.add_log_details(f"table_df_map : {cnv_ds.table_df_map} \n update table : {upd_table}")
                    else:
                        # create python dictionary with key as column to be updated and value as update value 
                        upd_col_dict = {}
            
                        # split set section of update statement to get each column assignment
                        set_fields = util.newSplit(upd_set_str, ',')
                        cnv_ds.cntx.logger.add_log("INFO","Columns Splitting begins ")
                        # for each column assignment get update column and updating value
                        if set_fields:
                            # for each column assignment get update column and updating value
                            for field in set_fields:
                                side = field.split('=')
                                side[0] = side[0].strip()
                                side[1] = self.replaceVariables(cnv_ds, side[1].strip(), False)
                                if re.match(r'\w+\.\w+', side[0], re.S|re.I):
                                    upd_col_dict[side[0]] = f'{side[1]} as {side[0]}'
                                else:
                                    upd_col_dict[f'{upd_table_alias}.{side[0]}'.strip('.')]= f'{side[1]} as {side[0]}'
                        else:
                            side = upd_set_str.split('=')
                            side[0] = side[0].strip()
                            side[1] = self.replaceVariables(cnv_ds, side[1].strip(), False)
                            if re.match(r'\w+\.\w+', side[0], re.S|re.I):
                                upd_col_dict[side[0]] = f'{side[1]} as {side[0]}'
                            else:
                                upd_col_dict[f'{upd_table_alias}.{side[0]}'.strip('.')]= f'{side[1]} as {side[0]}'
                        cnv_ds.cntx.logger.add_log("INFO","Splitting Completed ")
            
                        # pyspark code for update value select statement
                        cnv_code += f"df_col_list = mod_df['{upd_table_df}'].columns\n"
                        if upd_table_alias:
                            cnv_code += f"df_col_list_str = ('{upd_table_alias}.') + (',{upd_table_alias}.').join(df_col_list)\n"
                        else:                   
                            cnv_code += f"df_col_list_str = ','.join(df_col_list)\n"
                        cnv_code += f"upd_col_dict = {upd_col_dict}\n\n"        
                        cnv_code += "for col in upd_col_dict.keys():\n"
                        cnv_code += ' '*4 + "df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])\n\n"
                        
                        # final update value select statement
                        upd_from = f' from {upd_from_str} \n' + (f'where {upd_where_str}' if upd_where_str else '')
                        upd_from = util.replaceTableWithDF(cnv_ds, upd_from)
                        upd_from = self.replaceVariables(cnv_ds, upd_from) 
                        update_sql = '"select " + df_col_list_str + ' + upd_from                  
                        cnv_ds.cntx.logger.add_log("INFO","Update SQL Statement :")
                        cnv_ds.cntx.logger.add_log_details(update_sql)
                else:
                #    cnv_log+='Final Else statement executed\n'
        
                    # get table name which will be updated
                    upd_table = re.search(r'\bUPDATE?\s+([\w\[\]#\.]+)', stmt, re.S|re.I).group(1).strip()
                    cnv_ds.cntx.logger.add_log("INFO","Table name that will be updated :")
                    cnv_ds.cntx.logger.add_log_details(upd_table)
        
                    # get corresponding data frame name for the table
                    try :
                        upd_table_df = cnv_ds.table_df_map[upd_table]
                        upd_table_df_tmp_1 = upd_table_df + '_1'
                        upd_table_df_tmp_2 = upd_table_df + '_2'
                    except:
                        cnv_ds.cntx.logger.add_log("ERROR","Table to be updated not present in table_df_map")
                        cnv_ds.cntx.logger.add_log_details(f"table_df_map : {cnv_ds.table_df_map} \n update table : {upd_table}")
                    else:
                        # check if update statement has where condition
                        if re.search(r'\bWHERE\b', stmt, re.S|re.I):
                            # get the set section of update
                            upd_set_str = re.search(r'(?<=SET)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I).group(1).strip()
                            # get the where section of update statement
                            upd_where_str = re.search(r'\bSET\b.*?\bWHERE\b(.*)', stmt, re.S|re.I).group(1).strip()
                        else:
                            # get the set section of update
                            upd_set_str = re.search(r'(?<=SET)\s+(.*?);', stmt, re.S|re.I).group(1).strip()
                            upd_where_str = ''
                        cnv_ds.cntx.logger.add_log("INFO","SET section of update :\n")
                        cnv_ds.cntx.logger.add_log_details(upd_set_str)
                        cnv_ds.cntx.logger.add_log("INFO","Statements preset after 'WHERE' Clause:")
                        cnv_ds.cntx.logger.add_log_details(upd_where_str)
            
                        # make sql to select records that will be updated
                        if upd_where_str == '':
                            subtract_sql = f"select * \n from {upd_table_df}"
                        else:
                            subtract_sql = f"select * \n from {upd_table_df} " + (f"\n where {upd_where_str}" if {upd_where_str} else '')
                        cnv_ds.cntx.logger.add_log("INFO", f"Subtract SQL Statement :")
                        cnv_ds.cntx.logger.add_log_details(subtract_sql)
            
                        # remove semi-colon at the end
                        if subtract_sql.strip()[-1] == ';':
                            subtract_sql = subtract_sql.strip()[:-1]
            
                        #>-o-< replace db table names with corresponding dataframe name
                        subtract_sql = util.replaceTableWithDF(cnv_ds, subtract_sql)
                        subtract_sql = self.replaceVariables(cnv_ds, subtract_sql)                     
            
                        # create python dictionary with key as column to be updated and value as update value
                        upd_col_dict = {}        
                        # split set section of update statement to get each column assignment
                        set_fields = util.newSplit(upd_set_str, ',')
                        cnv_ds.cntx.logger.add_log("INFO","Columns Splitting begins")
                        # for each column assignment get update column and updating value
                        if set_fields:
                            # for each column assignment get update column and updating value
                            for field in set_fields:
                                side = field.split('=')
                                side[0] = side[0].strip()
                                side[1] = self.replaceVariables(cnv_ds, side[1].strip(), False)
                                if re.match(r'\w+\.\w+', side[0], re.S|re.I):
                                    upd_col_dict[side[0]] = f'{side[1]} as {side[0]}'
                                else:
                                    upd_col_dict[f'{upd_table_alias}.{side[0]}'.strip('.')]= f'{side[1]} as {side[0]}'
                        else:
                            side = upd_set_str.split('=')
                            side[0] = side[0].strip()
                            side[1] = self.replaceVariables(cnv_ds, side[1].strip(), False)
                            if re.match(r'\w+\.\w+', side[0], re.S|re.I):
                                upd_col_dict[side[0]] = f'{side[1]} as {side[0]}'
                            else:
                                upd_col_dict[f'{upd_table_alias}.{side[0]}'.strip('.')]= f'{side[1]} as {side[0]}'
                        cnv_ds.cntx.logger.add_log("INFO","Splitting Completed ")
            
                        # pyspark code for update value select statement
                        cnv_code += f"df_col_list = mod_df['{upd_table_df}'].columns\n"
                        cnv_code += f"df_col_list_str = ','.join({df_col_list})\n"
                        cnv_ds.cntx.logger.add_log("INFO",f"df_col_list_str = {','.join(df_col_list)}")
                        cnv_code += f"upd_col_dict = {upd_col_dict}\n\n"
                        cnv_code += "for col in upd_col_dict.keys():\n"
                        cnv_code += ' '*4 + "df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])\n\n"
            
                        # final update value select statement
                        upd_from = f' from {upd_table_df} \n' + (f'where {upd_where_str}' if upd_where_str else '')
                        upd_from = util.replaceTableWithDF(cnv_ds, upd_from)
                        upd_from = self.replaceVariables(cnv_ds, upd_from) 
                        update_sql = '"select " + df_col_list_str + ' + upd_from
                        cnv_ds.cntx.logger.add_log("INFO",f"Update SQL Statement :")
                        cnv_ds.cntx.logger.add_log_details(update_sql)
                try:
                    # pyspark code to update records in dataframe
                    cnv_code += f"{upd_table_df_tmp_1} = spark.sql({subtract_sql})\n\n"
                    cnv_code += f"{upd_table_df_tmp_2} = spark.sql({update_sql})\n\n"
                    cnv_code += f"mod_df[\'{upd_table_df}\'] = mod_df[\'{upd_table_df}\'].subtract({upd_table_df_tmp_1}).union({upd_table_df_tmp_2})\n"
                    cnv_code += f"mod_df[\'{upd_table_df}\'].createOrReplaceTempView('{upd_table_df}')\n"
                    cnv_code += f"rowcount_df = {upd_table_df_tmp_2}\n"
                    cnv_ds.cntx.logger.add_log("INFO","Update Statement Conversion Completed Successfully")
                except:
                    cnv_ds.cntx.logger.add_log("ERROR","No corresponding date frame for the update table.")
        
            except:
                cnv_ds.cntx.logger.add_log("ERROR","Exception : The statement doesn't end with ';' ")
                #raise
        except Exception as e:
            cnv_ds.cntx.logger.add_log("ERROR","Exception : Error occurred while converting below UPDATE statement ")
            cnv_ds.cntx.logger.add_log_details(self.original_text)
            cnv_ds.cntx.logger.add_log_details(str(e))
            cnv_code = ''
        ###
       
        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)

#END class Update
        
        
class Merge(SqlServerStatements):
    def __init__(self, text):
        super().__init__(text)
       
    def toPyspark(self, cnv_ds):
        try:    
            cnv_code = ''
            
            #Antlr Parse
            try:
                stmt = re.sub(r'\bLOAD\b', r'__LOAD__', \
                       re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                       re.sub(r'\bPLATFORM\b', r'__PLATFORM__', self.text, flags=re.S|re.I), \
                       flags=re.S|re.I), \
                       flags=re.S|re.I)                  
                #stmt = re.sub(r'\bLOAD\b', r'__LOAD__', re.sub(r'\bBLOCK\b', r'__BLOCK__', self.text, flags=re.S|re.I), flags=re.S|re.I)
                lexer = TSqlLexer(InputStream(stmt))
                lexer.removeErrorListeners()
                lexer.addErrorListener(TSqlErrorListener())            
                stream = CommonTokenStream(lexer)
                parser = TSqlParser(stream)
                parser.removeErrorListeners()
                parser.addErrorListener(TSqlErrorListener())
                tree = parser.tsql_file()
                lsnr = XxTsqlListener(stream, cnv_ds.cntx.logger)
                walker = ParseTreeWalker()
                walker.walk(lsnr, tree)
            except Exception as e:
                cnv_ds.cntx.logger.add_log('ERROR', 'Failed to parse below statement')
                stmt = stmt.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform') + ';'
                cnv_ds.cntx.logger.add_log_details(stmt)
                cnv_ds.cntx.logger.add_log_details('Syntax error: ' + str(e))
                cnv_ds.cntx.logger.add_log('WARN', 'Using unparsed statement. Result may be iscosistant.')                
            else:    
                stmt = lsnr.out_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform') + ';'     
                cnv_ds.cntx.logger.add_log('INFO', 'Statement parse completed.')
            
            #get primary table name and alias
            p_table, p_table_alias = re.search(r'\bMERGE\s+([\w\[\]#\.]+)\s+(?:AS\s+)?(\w*)\s+USING\b', stmt, re.S|re.I).groups()
            cnv_ds.cntx.logger.add_log("INFO",f"Processing MERGE statement into table {p_table}")
            
            if p_table_alias == '':
                p_table_alias = p_table.split(']')[-1].replace('as ', '').strip()
            
            #get corresponding data frame name for the primary table
            p_table_df = cnv_ds.table_df_map[p_table]
            p_table_df_tmp = p_table_df + '_tmp'
            
            #check if secondary table is a subquery
            if re.search(r'\bUSING\s*\(\s*SELECT', stmt, re.S|re.I):
                #get select statement of secondary table
                s_table_sql, s_table_alias = re.search(r'\bUSING\s*\((.*?)\)\s*(?:AS\s+)?(\w*)\s+ON\b', stmt, re.S|re.I).groups()        
                cnv_ds.cntx.logger.add_log("INFO","Merge from table is below subquery")
                cnv_ds.cntx.logger.add_log_details(s_table_sql)
    
                #make dataframe name for secondary table select statement
                s_table = s_table_alias + '__df'       
    
                #table name to dataframe and replace variables
                s_table_sql = util.replaceTableWithDF(cnv_ds, s_table_sql)
                s_table_sql = self.replaceVariables(cnv_ds, s_table_sql)
    
                #pyspark code to create df for secondary table
                cnv_code += f'{s_table} = spark.sql({s_table_sql})\n'
                cnv_code += f'{s_table}.createOrReplaceTempView("{s_table}")\n\n'     
    
            else:
                #get secondary table name and alias
                s_table, s_table_alias = re.search(r'\bUSING\s+([\w\[\]#\.]+)\s*(?:AS\s+)?(\w*)\s+ON\b', stmt, re.S|re.I).groups() 
                cnv_ds.cntx.logger.add_log("INFO",f"Merge from table is {s_table}")
                if s_table_alias == '':
                    s_table_alias = s_table.split(']')[-1].replace('as ', '').strip()
                            
            #get merge join condition
            join_cond_str = re.search(r'\bON\s+\(?(.*?)\)?\s+WHEN\b', stmt, re.S|re.I).group(1).strip()
            
            #get merge insert section
            insert_str_match = re.search(r'\bINSERT\b?\s+(.*?)(?:(?:WHEN\b)|;)', stmt, re.S|re.I)           
            if insert_str_match:
                insert_str = insert_str_match.group(1).strip()
                
                #get merge insert section columns
                if insert_str.split()[0].upper() == 'VALUES':
                    insert_col_str = ''
                else:
                    insert_col_str = re.search(r'\(?(.*?)\)?\s+VALUES\b', insert_str, re.S|re.I).group(1).strip()
                    
                #get merge insert section values
                insert_val_str = re.search(r'\bVALUES\s*\(?(.*?)\)?$', insert_str, re.S|re.I).group(1).strip()
            else:
                insert_str = ''
                insert_col_str = ''
                insert_val_str = ''
    
            if insert_str:
                if insert_col_str:
                    insert_cols = util.newSplit(insert_col_str, ',')
                    insert_vals = util.newSplit(insert_val_str, ',')
                    #make select column string for merge insert
                    select_str = ''
                    for i in range(len(insert_cols)):
                        col = insert_cols[i].strip()
                        val = insert_vals[i].strip()
                        select_str += f"{val} as {col},\n"
                    select_str = select_str[:-2]                
                else:
                    select_str = '*'    #[TODO] add logic for select_str            
            
                #make sql to select records to be inserted by merge insert
                merge_join = f'select {select_str} \n from {s_table} as {s_table_alias} \n left outer join {p_table} as {p_table_alias} \n on {join_cond_str} \n'
                
                #table name to dataframe and replace variables
                merge_join = util.replaceTableWithDF(cnv_ds, merge_join)            
                merge_join = self.replaceVariables(cnv_ds, merge_join)
                
                insert_sql = merge_join + ' + check_col + " is null"'
                        
                #pyspark code to get first column of update table
                cnv_code += f"#Get first column of db table {p_table}\n"
                cnv_code += f"check_col = mod_df['{p_table_df}'].columns[0]\n"            
                #pyspark code to load temp df with merge insert records
                cnv_code += "#Load temporary dataframe merge insert records\n"
                cnv_code += f"{p_table_df_tmp} = spark.sql({insert_sql})\n\n"        
    
            else:
                insert_sql = ''
    
            #pyspark code to remove old records from db table dataframe and insert merge update and insert records
            cnv_code += (f"mod_df['{p_table_df}'] = mod_df['{p_table_df}']") + (f".union({p_table_df_tmp})\n" if insert_sql else '')
            cnv_code += f"mod_df['{p_table_df}'].createOrReplaceTempView('{p_table_df}')\n"
            cnv_code += f"rowcount_df = {p_table_df_tmp}\n\n"
          
        except Exception as e:
            cnv_ds.cntx.logger.add_log("ERROR","Exception : Error occurred while converting the following MERGE statement")
            cnv_ds.cntx.logger.add_log_details(self.original_text)
            cnv_ds.cntx.logger.add_log_details(str(e))
            cnv_code = ''            
        
        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)

#END class Merge
        
   
class Execute(SqlServerStatements):
    def __init__(self, text):
        super().__init__(text)
        
    def toPyspark(self, cnv_ds):

        try:
            #remove 'WITH EXECUTE AS CALLER'
            stmt = re.sub(r'\bWITH\s*EXECUTE\s*AS\s*CALLER\b', '', self.text, flags=re.S|re.I)
            
            re_match = re.search(r'(?:\bEXECUTE|\bCALL)\s+([\]\[\.\w]+)\s*(.*)', stmt, re.S|re.I)
            sp_name = re_match.group(1)
            sp_parm = re_match.group(2).strip()
            
            cnv_ds.cntx.logger.add_log("INFO", f"Converting EXEC statement calling {sp_name}")
            
            sp_name = re.sub(r'\[|\]', r'', sp_name)
            sp_name = sp_name.split('.')[-1]
            
            in_var = ''
            out_var = ''
            
            if bool(sp_parm):
                sp_parm_list = util.newSplit(sp_parm, ',')
                for parm in sp_parm_list:
                    if ' out' in parm:
                        parm = parm.replace('out', '').replace('@', '').strip()
                        out_var += parm + ', '
                    else:
                        parm = parm.replace('@', '')
                        in_var += parm + ', '
                        
                in_var = in_var.strip()[:-1]            
                out_var = out_var.strip()[:-1] 
                        
            if bool(out_var):
                out_var += ' = '
    
            cnv_code = out_var + sp_name + '(' + in_var + ')'
            
            #add import
            cnv_ds.output.code[0] += f"from {sp_name} import {sp_name}\n"              

        except:
            cnv_ds.cntx.logger.add_log("ERROR","Exception : Error occurred while converting below EXEC statement ")
            cnv_ds.cntx.logger.add_log_details(self.original_text)
            cnv_code = ''             
        
        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)

#END class Execute        
        
        
class IfCondition(SqlServerStatements):
    def __init__(self, text):
        super().__init__(text)
        
    def toPyspark(self, cnv_ds):
        try:
            cnv_ds.cntx.logger.add_log("INFO", "Converting IF condition statement")
            cnv_code = ''
            if re.search(r'^IF\s*\(?', self.text, re.S|re.I):
                re_match = re.search(r'^IF\s*(\(?.*)', self.text, re.S|re.I)
                if_cond = re_match.group(1)
    
                if_cond = if_cond.replace('@', '')
                if_cond = self.replaceOperator(cnv_ds, if_cond)  
            
                cnv_code = 'if ' + if_cond.strip() + ':'
                cnv_code = re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)
                cnv_ds.tabs += 1
                
            #if re.search(r'^ELSE', self.text, re.S|re.I):
            if self.text.upper() == 'ELSE':
                cnv_code = ' '*4*(cnv_ds.tabs -1) + 'else:'
            
            #if re.search(r'^END IF', self.text, re.S|re.I):
            if self.text.upper() == 'END IF':
                prev_code = cnv_ds.output.code[cnv_ds.cd_idx - 1]
                if re.search(r'\s+IF\b', prev_code, re.I):
                    cnv_ds.output.code[cnv_ds.cd_idx - 1] = '#' + cnv_ds.output.code[cnv_ds.cd_idx - 1] + '  #None of the statements under IF can be converted'
                cnv_ds.tabs -= 1
        except Exception as e:
            cnv_ds.cntx.logger.add_log("ERROR","Exception : Error occurred while converting the following IF statement ")
            cnv_ds.cntx.logger.add_log_details(self.original_text)
            cnv_ds.cntx.logger.add_log_details(str(e))
            cnv_code = '' 
            raise    
            
        return cnv_code

#END class IfCondition
        
 
class WhileLoop(SqlServerStatements):
    def __init__(self, text):
       super().__init__(text)
       
    def toPyspark(self, cnv_ds):

        try:
            cnv_ds.cntx.logger.add_log("INFO", "Converting WHILE statement")
            cnv_code = ''
            if re.search(r'^WHILE\s*\(?', self.text, re.S|re.I):
                re_match = re.search(r'^WHILE\s+(.*)', self.text, re.S|re.I)
                loop_cond = re_match.group(1)
    
                loop_cond = loop_cond.replace('@', '')
                loop_cond = self.replaceOperator(cnv_ds, loop_cond)        
            
                cnv_code = 'while (' + loop_cond.strip() + '):'
                cnv_code = re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)
                cnv_ds.tabs += 1
            
            if re.search(r'^END WHILE', self.text, re.S|re.I):
                prev_code = cnv_ds.output.code[cnv_ds.cd_idx - 1]
                if re.search(r'\s+WHILE\b', prev_code, re.I):
                    #cnv_ds.output.code[cnv_ds.cd_idx - 1] += '\n' + ' '*4*cnv_ds.tabs + 'pass\n'
                    cnv_ds.output.code[cnv_ds.cd_idx - 1] = '#' + cnv_ds.output.code[cnv_ds.cd_idx - 1] + '  #None of the statements under WHILE can be converted'        
                cnv_ds.tabs -= 1
        except Exception as e:
            cnv_ds.cntx.logger.add_log("ERROR","Exception : Error occurred while converting the following WHILE statement ")
            cnv_ds.cntx.logger.add_log_details(self.original_text)
            cnv_ds.cntx.logger.add_log_details(str(e))
            cnv_code = ''                
            
        return cnv_code

#END class WhileLoop        
        

class Cursor(SqlServerStatements):
    def __init__(self, text):
       super().__init__(text)
       
    def toPyspark(self, cnv_ds):
        cnv_code = ''
        stmt = self.text

        try:
            # check if declare cursor statemennt
            if re.search(r'^DECLARE\s+\w+\s+CURSOR\b', stmt, re.S|re.I):
                # get cursor name
                cur_name = re.search(r'^DECLARE\s+(\w+)\s+CURSOR\b', stmt, re.S|re.I).group(1).strip()
                cnv_ds.cntx.logger.add_log("INFO", f"Converting declare statement for cursor {cur_name}")
                # get cursor select query
                if re.search(r'\bFOR\s+SELECT\b',stmt,re.S|re.I):
                    cur_sel_stmt = re.search(r'\bFOR\s+(.*)', stmt, re.S | re.I).group(1).strip()

                    #Antlr Parse
                    try:
                        cur_sel_stmt = re.sub(r'\bLOAD\b', r'__LOAD__', \
                                       re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                                       re.sub(r'\bPLATFORM\b', r'__PLATFORM__', cur_sel_stmt, flags=re.S|re.I), \
                                       flags=re.S|re.I), \
                                       flags=re.S|re.I)                         
                        #cur_sel_stmt = re.sub(r'\bLOAD\b', r'__LOAD__', re.sub(r'\bBLOCK\b', r'__BLOCK__', cur_sel_stmt, flags=re.S|re.I), flags=re.S|re.I)
                        lexer = TSqlLexer(InputStream(cur_sel_stmt))
                        lexer.removeErrorListeners()
                        lexer.addErrorListener(TSqlErrorListener())
                        stream = CommonTokenStream(lexer)
                        parser = TSqlParser(stream)
                        parser.removeErrorListeners()
                        parser.addErrorListener(TSqlErrorListener())
                        tree = parser.tsql_file()
                        lsnr = XxTsqlListener(stream, cnv_ds.cntx.logger)
                        walker = ParseTreeWalker()
                        walker.walk(lsnr, tree)
                    except Exception as e:
                        cnv_ds.cntx.logger.add_log('ERROR', 'Failed to parse below statement')
                        cur_sel_stmt = cur_sel_stmt.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                        cnv_ds.cntx.logger.add_log_details(cur_sel_stmt)
                        cnv_ds.cntx.logger.add_log_details('Syntax error: ' + str(e))
                        cnv_ds.cntx.logger.add_log('WARN', 'Using unparsed script content. Result may be iscosistant.')
                    else:
                        cnv_ds.cntx.logger.add_log('INFO', 'Script contect parse completed.')
                        cur_sel_stmt = lsnr.out_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                    
                    #replace db table names with corresponding data frame name
                    cur_sel_stmt = util.replaceTableWithDF(cnv_ds, cur_sel_stmt)
                    cur_sel_stmt = self.replaceVariables(cnv_ds, cur_sel_stmt)                
                else:
                    cnv_ds.cntx.logger.add_log("INFO", 'FOR select statement not found or not in order')
                # Pyspark code for cursor select statement 
                cnv_code += f"{cur_name}__df = spark.sql({cur_sel_stmt})\n"        
            
            # check if open cursor statemennt
            if re.search(r'^OPEN\s+\w+', stmt, re.S|re.I):
                # get cursor name
                cur_name = re.search(r'^OPEN\s+(\w+)',stmt,re.S|re.I).group(1).strip()
                cnv_ds.cntx.logger.add_log("INFO", f"Converting open statemennt for cursor {cur_name}")
                #create iterator
                cnv_code += f"{cur_name}__df_iter = iter({cur_name}__df).collect()\n"
                cnv_code += f"fetch_status = 0\n"
                
            # Fetch from cursor statement
            if re.search(r'^FETCH\s+\w+\s+FROM\s+\w+', stmt, re.S|re.I):
                # get cursor name
                cur_name = re.search(r'^FETCH\s+\w+\s+FROM\s+(\w+)',stmt,re.S|re.I).group(1).strip()  
                cnv_ds.cntx.logger.add_log("INFO", f"Converting fetch statemennt on cursor {cur_name}")                
                # get fetch into vars
                ftch_vars = re.search(r'\bINTO\s+(.*)',stmt,re.S|re.I).group(1).strip()
                # replacing @ from input variable
                ftch_vars = ftch_vars.replace('@','')
                
                cnv_code += f"{cur_name}__df_cols = {cur_name}__df.columns\n"
                cnv_code += "try:\n";
                cnv_code += ' '*4 + f"{cur_name}__df_row = next({cur_name}__df_iter).asDict()\n"
                cnv_code += ' '*4 + f"{ftch_vars} = list({cur_name}__df_row[col] for col in {cur_name}__df_cols)\n"
                cnv_code += "except StopIteration:\n"
                cnv_code += ' '*4 + f"fetch_status = 1\n"
                
            # Close cursor statements
            if re.search(r'^CLOSEs+\w+', stmt, re.S|re.I):
                cur_name = re.search(r'^CLOSE\s+(\w+)',stmt,re.S|re.I).group(1).strip()
                cnv_ds.cntx.logger.add_log("INFO", f"Converting close statemennt on cursor {cur_name}")  
                cnv_code += f"del {cur_name}\n"

        except Exception as e:
            cnv_ds.cntx.logger.add_log("ERROR","Error occurred while converting below statement ")
            cnv_ds.cntx.logger.add_log_details(self.original_text)
            cnv_ds.cntx.logger.add_log_details(str(e))
            cnv_code = ''  
            
        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M) 

#END class Cursor        
       
       
class ErrorHadling(SqlServerStatements):
    def __init__(self, text):
       super().__init__(text)
       
    def toPyspark(self, cnv_ds):
        cnv_code = ''

        if re.search(r'\bBEGIN\s+TRY\b', self.text, re.S|re.I):
            cnv_code = re.sub(r'^', ' '*4*cnv_ds.tabs, '\ntry:\n', flags=re.M)
            cnv_ds.tabs += 1
            
        if re.search(r'\bEND\s+TRY\b', self.text, re.S|re.I):
            cnv_ds.tabs -= 1
            
        if re.search(r'\bBEGIN\s+CATCH\b', self.text, re.S|re.I):
            cnv_code = re.sub(r'^', ' '*4*cnv_ds.tabs, "\nexcept Exception as e:\n    raise\n", flags=re.M)
            cnv_ds.tabs += 1
            
        if re.search(r'\bEND\s+CATCH\b', self.text, re.S|re.I):
            cnv_ds.tabs -= 1            
                
        return cnv_code

#END class ErrorHadling
        
     
class SPheader(SqlServerStatements):
    def __init__(self, text):
       super().__init__(text)
       
    def toPyspark(self, cnv_ds):
        sp_name = ''
        sp_parm_str = ''
        rtn_parm_str = ''        
        cnv_ds.cntx.logger.add_log("INFO", "Processing Srored Procedure create/alter statement")
        try:
            if re.search(r'(?:\bCREATE\b|\bALTER\b)\s+\bPROC(?:EDURE)?\b', self.text, re.S|re.I):
                re_match = re.search(r'\bPROC(?:EDURE)?\s+(.*?)\s+(@.+?\bAS\b|\bAS\b)', self.text, re.S|re.I)
                
                if re_match.group(1).strip():
                    #get sp name
                    sp_name = re_match.group(1).strip()
                    sp_name = re.sub(r'\[|\]', r'', sp_name)
                    sp_name = sp_name.split('.')[-1].rstrip().rstrip('(')
                    cnv_ds.sp_name = sp_name
                    cnv_ds.output.out_fname = sp_name + '.py'
                    cnv_ds.cntx.logger.add_log("INFO", f"Processing stored procedure definition for {sp_name}")
                else:
                    cnv_ds.cntx.logger.add_log("ERROR", "Unable to extract SP name")
                
                if re_match.group(2).strip():
                    #get parameter list
                    sp_param = util.newSplit(re_match.group(2).replace('as', ''), ',')
                    for var in sp_param:
                        var = var.strip()
                        if var:
                            re_match = re.search(r'(@\w+)\s*(?:AS)?\s+\[?([\w\s]+)\]?', var, re.I)
                            if re_match:
                                v_name = re_match.group(1)
                                v_type = re_match.group(2).strip()
                                                                
                                #check parameter type
                                if ' out' in var:
                                    rtn_parm_str += v_name.replace('@', '') + ', '
                                else:
                                    cnv_ds.var_datatype[v_name] = v_type
                                    sp_parm_str += v_name.replace('@', '') + ', '
                            else:
                                cnv_ds.cntx.logger.add_log("ERROR", f"Could not parse string {var} in sp {sp_name}")
                    
                    sp_parm_str = sp_parm_str.strip()[:-1]
                    rtn_parm_str = rtn_parm_str.strip()[:-1]
                    
                else:
                    cnv_ds.cntx.logger.add_log("ERROR", "Unable to extract SP parameters")
                
                #set return string
                cnv_ds.rtn_parm_str = rtn_parm_str
                #add python function definition
                sp_header = 'def ' + sp_name + '(' + sp_parm_str + '):\n'
                cnv_ds.output.code[1] = cnv_ds.output.code[1].replace('#SPDEF#', sp_header)
                cnv_ds.tabs += 1

        except Exception as e:
            cnv_ds.cntx.logger.add_log("ERROR","Unable to convert below statement")
            cnv_ds.cntx.logger.add_log_details(self.original_text)
            cnv_ds.cntx.logger.add_log_details(str(e))
               
        return ''

#END class SPheader        


class Declare(SqlServerStatements):
    def __init__(self, text):
       super().__init__(text)
       
    def toPyspark(self, cnv_ds):
        cnv_code = ''
        cnv_ds.cntx.logger.add_log("INFO","Processing DECLARE variable statement")
        re_match = re.search(r'\bDECLARE\b\s+(@.+)', self.text, re.S|re.I)
        if re_match:
            dlr_var = util.newSplit(re_match.group(1), ',')
            for var in dlr_var:
                var = var.strip()
                re_match = re.search(r'(@\w+)\s*(?:AS)?\s+(\w+)', var, re.I)
                if re_match:
                    v_name = re_match.group(1)
                    v_type = re_match.group(2)
                    py_var = v_name.strip('@')
                    cnv_ds.var_datatype[v_name] = v_type
                else:
                    cnv_ds.cntx.logger.add_log("ERROR","Unable to parse below expression")
                    cnv_ds.cntx.logger.add_log_details(var)                    
                
                #process var asgn
                if '=' in var:
                    var_asgn = var.split('=')[1].strip()            
                    if (var_asgn.count("'") == 2 and var_asgn[0] == "'" and var_asgn[-1] == "'") or re.match(r'[\-\+\d\.]+', var_asgn) or re.match(r'@{1,2}\w+', var_asgn):
                        cnv_code += py_var + ' = ' + var_asgn.strip('@') + "\n"
                    else:
                        if re.search(r'^\(\s*SELECT\s+', var_asgn, re.S|re.I):
                            var_asgn = var_asgn.strip()[1:-1].strip()
                            var_asgn_sql = var_asgn
                        else:
                            var_asgn_sql = 'select ' + var_asgn 
                        
                        #Antlr Parse
                        try:
                            var_asgn_sql = re.sub(r'\bLOAD\b', r'__LOAD__', \
                                           re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                                           re.sub(r'\bPLATFORM\b', r'__PLATFORM__', var_asgn_sql, flags=re.S|re.I), \
                                           flags=re.S|re.I), \
                                           flags=re.S|re.I)                              
                            #var_asgn_sql = re.sub(r'\bLOAD\b', r'__LOAD__', re.sub(r'\bBLOCK\b', r'__BLOCK__', var_asgn_sql, flags=re.S|re.I), flags=re.S|re.I)
                            lexer = TSqlLexer(InputStream(var_asgn_sql))
                            lexer.removeErrorListeners()
                            lexer.addErrorListener(TSqlErrorListener())
                            stream = CommonTokenStream(lexer)
                            parser = TSqlParser(stream)
                            parser.removeErrorListeners()
                            parser.addErrorListener(TSqlErrorListener())
                            tree = parser.tsql_file()
                            lsnr = XxTsqlListener(stream, cnv_ds.cntx.logger)
                            walker = ParseTreeWalker()
                            walker.walk(lsnr, tree)
                        except Exception as e:
                            cnv_ds.cntx.logger.add_log('ERROR', 'Failed to parse below statement')
                            var_asgn_sql = var_asgn_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                            cnv_ds.cntx.logger.add_log_details(var_asgn_sql)
                            cnv_ds.cntx.logger.add_log_details('Syntax error: ' + str(e))
                            cnv_ds.cntx.logger.add_log('WARN', 'Using unparsed statement. Result may be iscosistant.')
                        else:
                            cnv_ds.cntx.logger.add_log('INFO', 'Statement parse completed.')
                            var_asgn_sql = lsnr.out_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                        
                        #table to df and variable repalce
                        var_asgn_sql = util.replaceTableWithDF(cnv_ds, var_asgn_sql)
                        var_asgn_sql = self.replaceVariables(cnv_ds, var_asgn_sql)  
                        
                        if re.search(r'(CHAR|TIME|DATE)', cnv_ds.var_datatype[v_name], re.I):
                            cnv_code += py_var + ' = str(spark.sql(' + var_asgn_sql + ')' + '.collect()[0][0])\n' 
                        else:
                            cnv_code += py_var + ' = spark.sql(' + var_asgn_sql + ')' + '.collect()[0][0]\n'
        else:
            cnv_ds.cntx.logger.add_log("ERROR","Unable to parse below expression")
            cnv_ds.cntx.logger.add_log_details(self.original_text)              
                    
        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)

#END class Declare        


class SetVar(SqlServerStatements):
    def __init__(self, text):
       super().__init__(text)
       
    def toPyspark(self, cnv_ds):
        cnv_code = ''
        cnv_ds.cntx.logger.add_log("INFO","Processing SET statement")
        re_match = re.search(r'@(\w+)\s*=\s*(.*)', self.text, re.S|re.I)
        if re_match:
            var = re_match.group(1)
            var_asgn = re_match.group(2).strip()
            
            if (var_asgn.count("'") == 2 and var_asgn[0] == "'" and var_asgn[-1] == "'") or re.match(r'[\-\+\d\.]+', var_asgn) or re.match(r'@{1,2}\w+', var_asgn):
                #cnv_code = var + ' = ' + var_asgn.strip('@')
                var_asgn = var_asgn.replace('@', '')
                var_asgn = re.sub(r'\browcount\b', r'rowcount_df.count()', var_asgn, flags=re.S|re.I)
                cnv_code = var + ' = ' + var_asgn
                
            else:
                if re.search(r'^\(\s*SELECT\s+', var_asgn, re.S|re.I):
                    var_asgn = var_asgn.strip()[1:-1].strip()
                    var_asgn_sql = var_asgn
                else:
                    var_asgn_sql = 'select ' + var_asgn
                
                #Antlr Parser
                try:
                    var_asgn_sql = re.sub(r'\bLOAD\b', r'__LOAD__', \
                                   re.sub(r'\bBLOCK\b', r'__BLOCK__', \
                                   re.sub(r'\bPLATFORM\b', r'__PLATFORM__', var_asgn_sql, flags=re.S|re.I), \
                                   flags=re.S|re.I), \
                                   flags=re.S|re.I)                    
                    #var_asgn_sql = re.sub(r'\bLOAD\b', r'__LOAD__', re.sub(r'\bBLOCK\b', r'__BLOCK__', var_asgn_sql, flags=re.S|re.I), flags=re.S|re.I)
                    lexer = TSqlLexer(InputStream(var_asgn_sql))
                    lexer.removeErrorListeners()
                    lexer.addErrorListener(TSqlErrorListener())
                    stream = CommonTokenStream(lexer)
                    parser = TSqlParser(stream)
                    parser.removeErrorListeners()
                    parser.addErrorListener(TSqlErrorListener())
                    tree = parser.tsql_file()
                    lsnr = XxTsqlListener(stream, cnv_ds.cntx.logger)
                    walker = ParseTreeWalker()
                    walker.walk(lsnr, tree)
                except Exception as e:
                    cnv_ds.cntx.logger.add_log('ERROR', 'Failed to parse below statement')
                    var_asgn_sql = var_asgn_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                    cnv_ds.cntx.logger.add_log_details(var_asgn_sql)
                    cnv_ds.cntx.logger.add_log_details('Syntax error: ' + str(e))
                    cnv_ds.cntx.logger.add_log('WARN', 'Using unparsed staement. Result may be iscosistant.')
                else:
                    cnv_ds.cntx.logger.add_log('INFO', 'Statement parse completed.')
                    var_asgn_sql = lsnr.out_sql.replace('__LOAD__', 'load').replace('__BLOCK__', 'block').replace('__PLATFORM__', 'platform')
                
                #tab2df and rpl var
                var_asgn_sql = util.replaceTableWithDF(cnv_ds, var_asgn_sql)
                var_asgn_sql = self.replaceVariables(cnv_ds, var_asgn_sql) 
                
                if re.search(r'(CHAR|TIME|DATE)', cnv_ds.var_datatype['@' + var], re.I):
                    cnv_code += var + ' = str(spark.sql(' + var_asgn_sql + ')' + '.collect()[0][0])'
                else:
                    cnv_code += var + ' = spark.sql(' + var_asgn_sql + ')' + '.collect()[0][0]'  
        else:
            cnv_ds.cntx.logger.add_log("ERROR","Unable to parse below expression")
            cnv_ds.cntx.logger.add_log_details(self.original_text)
                
        return re.sub(r'^', ' '*4*cnv_ds.tabs, cnv_code, flags=re.M)
        
#END class SetVar