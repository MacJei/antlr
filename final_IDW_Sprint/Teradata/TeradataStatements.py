import re
import util
from antlr4 import *
from Teradata.parser.TDantlrLexer import TDantlrLexer
from Teradata.parser.TDantlrListener import TDantlrListener
from Teradata.parser.TDantlrParser import TDantlrParser
from Teradata.parser.PySparkParse import PySparkParse
from Teradata.parser.TDErrorListener import TDErrorListener


class TeradataStatement():
    def __init__(self, text):
        self.text = text
        self.cnv_code = ''

    def simpleParse(self, stmt = None):
        if stmt == None:
            stmt = self.text
        
        log = '' #log string
        #conversion for EXTRACT statemant
        match = re.search(r'\bEXTRACT\s*\((\w+)\s+FROM\s+(.*?)\)', stmt, re.S|re.I)
        if match:
            log += f"match[0] --> {match[1]}({match[2]})\n"
            stmt = re.sub(r'\bEXTRACT\s*\((\w+)\s+FROM\s+(.*?)\)', r'\1(\2)', stmt, flags=re.S|re.I)    
            
        #conversion for current timestamp       
        match = re.search(r'(CURRENT_TIMESTAMP)\(\d+\)', stmt, re.S|re.I)
        if match:
            log += f"{match[0]} --> {match[1]}\n"        
            stmt = re.sub(r'(CURRENT_TIMESTAMP)\(\d+\)', r'\1', stmt, flags=re.S|re.I)    
        
        #conversion for timestamp
        match = re.search(r'(\sTIMESTAMP)\(\d+\)', stmt, re.S|re.I)
        if match:
            log += f"{match[0]} --> {match[1]}\n"           
            stmt = re.sub(r'(\sTIMESTAMP)\(\d+\)', r'\1', stmt, flags=re.S|re.I)    

        #conversion for CAST timestamp
        match = re.search(r'\bCAST\s*\(\s*(.*?)\s+AS\s+TIMESTAMP(?:\(\d+\))?\s+FORMAT\s+(.*?)\s*\)', stmt, re.S|re.I)
        if match:
            log += f"{match[0]} --> to_timestamp({match[1]},{match[2]})"
            stmt = re.sub(r'\bCAST\s*\(\s*(.*?)\s+AS\s+TIMESTAMP(?:\(\d+\))?\s+FORMAT\s+(.*?)\s*\)', r'to_timestamp(\1,\2)', stmt, flags=re.S|re.I)    

        #conversion for CAST date
        match = re.search(r'\bCAST\s*\(\s*(.*?)\s+AS\s+DATE\s+FORMAT\s+(.*?)\s*\)', stmt, re.S|re.I)
        if match:
            log += f"{match[0]} --> to_date({match[1]},{match[2]})"
            stmt = re.sub(r'\bCAST\s*\(\s*(.*?)\s+AS\s+DATE\s+FORMAT\s+(.*?)\s*\)', r'to_date(\1,\2)', stmt, flags=re.S|re.I)    

        #conversion for substring
        match = re.search(r'(\bSUBSTRING\s*\(.*?)\bFROM\b(.*?)\bFOR\b(.*?\))', stmt, re.S|re.I)
        if match:
            log += f"{match[0]} --> {match[1]},{match[2]},{match[3]}"
            stmt = re.sub(r'(\bSUBSTRING\s*\(.*?)\bFROM\b(.*?)\bFOR\b(.*?\))', r'\1,\2,\3', stmt, flags=re.S|re.I)    

        return stmt

        
class Select(TeradataStatement):
    def __init__(self, text):
        super().__init__(text)
        
    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'teradata':
            if cnv_ds.cntx.in_code_type == 'bteq':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    cnv_code, cnv_log = self.toPyspark(cnv_ds)
                    #cnv_code = cnv_log = ''
        
        return cnv_code, cnv_log            
    
    def toPyspark(self, cnv_ds):
        try:
            lexer = TDantlrLexer(InputStream(self.text))
            stream = CommonTokenStream(lexer)
            parser = TDantlrParser(stream)
            parser.addErrorListener(TDErrorListener())
            tree = parser.start()
            psp = PySparkParse(stream)
            walker = ParseTreeWalker()
            walker.walk(psp, tree)
            stmt = psp.out_sql
        except Exception as e:
            stmt = self.simpleParse() + ';'
            
        cnv_code = ''
        cnv_log = ''        
        
        #replace table with dataframe
        stmt = util.replaceTableWithDF(stmt, cnv_ds, 1)
        
        #remove semi-colon at the end
        if stmt.strip()[-1] == ';':
            stmt = stmt.strip()[:-1]        
        
        cnv_code += "#cereating dataframe for select statement\n"
        cnv_code += f'select__df = spark.sql("""{stmt}""")\n'
        cnv_code += "last_activity__df = select__df\n"
        
        return cnv_code, cnv_log
        
        
class Insert(TeradataStatement):
    def __init__(self, text):
        super().__init__(text)
        
    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'teradata':
            if cnv_ds.cntx.in_code_type == 'bteq':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    cnv_code, cnv_log = self.toPyspark(cnv_ds)
                    #cnv_code = cnv_log = ''
        
        return cnv_code, cnv_log
        
    def toPyspark(self, cnv_ds):
        try:
            lexer = TDantlrLexer(InputStream(self.text))
            stream = CommonTokenStream(lexer)
            parser = TDantlrParser(stream)
            parser.addErrorListener(TDErrorListener())
            tree = parser.start()
            psp = PySparkParse(stream)
            walker = ParseTreeWalker()
            walker.walk(psp, tree)
            stmt = psp.out_sql
        except Exception as e:
            stmt = self.simpleParse() + ';'

        cnv_code = ''
        cnv_log = ''
            
        #get table name where records will be inserted
        table = re.match(r'\bINS(?:ERT)?\s+INTO\s+(\w*\.?\w+)', stmt, re.S|re.I).group(1).strip()
        #get table df name
        table_df = cnv_ds.table_df_map[table]
        
        #check if insert statement with select
        if re.match(r'\bINSERT\s+INTO(.*?)\(?.*?\)?\s+\(?\bSEL(?:ECT)?\b', stmt, re.S|re.I):
        
            #check if select statement has target table column list specified
            if re.search(table+r'\s+\(.*?\)[\s\(]+\bSEL(?:ECT)?\b', stmt, re.S|re.I):
        
                #check if select is in circular bracket
                if re.search(r'(.)\s*SEL(?:ECT)?\b', stmt, re.S|re.I).group(1) == '(':
                    #replace starting bracker with spaecial string
                    stmt = re.sub(r'\((\s*)(SEL(?:ECT)?\b)', r'‹\1\2', stmt, 1, flags=re.S|re.I)
                
                #replace FROM keyword with spaecial character 
                stmt = re.sub(r"\bFROM\b", 'ƒ', stmt, flags=re.S|re.I)
                #custom split insert statement 
                stmt_part = util.newSplit(stmt, 'ƒ')
                
                #first part of the split
                stmt_part_1 = re.sub(r'ƒ', 'from', re.sub(r'‹', r'(', stmt_part[0], flags=re.S|re.I), flags=re.S|re.I)
                #second part of the split
                stmt_part_2 = re.sub(r'ƒ', 'from', re.sub(r'‹', r'(', stmt_part[1], flags=re.S|re.I), flags=re.S|re.I)
                
                #get target table column list
                col_list_str = re.search(table+r'\s+\((.*?)\)[\(\s]*\bSEL(?:ECT)?\b', stmt_part_1, re.S|re.I).group(1)
                col_list = util.newSplit(col_list_str, ',')
                
                #get target insert values
                val_list_str = re.search(r'\bSEL(?:ECT)?\b\s+(.*)', stmt_part_1, re.S|re.I).group(1)
                val_list = util.newSplit(val_list_str, ',')
                
                #create select column list
                cnct_str = ''
                for i in range(len(col_list)):
                    col = col_list[i].strip()
                    val = val_list[i].strip()
                    
                    #remove alias from column value
                    if re.match(r'^\bCASE\b', val, re.S|re.I):
                        #ignore END keyword as alias for column value with CASE statement
                        val = re.sub(r'(?<=END)\s*(AS)?[ \t]+\w+$', '', val, flags=re.S|re.I)
                    else:
                        val = re.sub(r'(\s*AS)?[ \t]+[a-zA-Z]\w+$', '', val, flags=re.S|re.I)
                        
                    cnct_str += val + ' as ' + col + ',\n'    
                
                cnct_str = cnct_str[:-2]
                select_str = 'select ' + cnct_str + "\nfrom " + stmt_part_2;
   
            else:
                #get select statement in insert into in case of target table column list not specified
                select_str = re.search(r'([\(\s]*\bSEL(?:ECT)?\b.*)', stmt, re.S|re.I).group(1)
        
            #replace table with dataframe
            select_str = util.replaceTableWithDF(select_str, cnv_ds, 1)
            
            #remove semi-colon at the end
            if select_str.strip()[-1] == ';':
                select_str = select_str.strip()[:-1]

                #enclose modified sql in triple quote
            select_str = '"""' + select_str + '"""'
            
            #[TODO] check if modified statement using variable
                        
        #check if insert statement with vlaues
        if re.search(r'INSERT\s+INTO\s+\w*\.?\w.*?\bVALUES\s*\(', stmt, re.S|re.I):
            #get tablename, column and value list
            match_grp = re.match(r'INSERT\s+INTO\s+(.*?)\s+\(?(.*?)\)?\s*VALUES\s*\((.*?)\)\s*;$', stmt, re.S|re.I)
            table = match_grp.group(1).strip()
            cols = match_grp.group(2).strip()
            vals = match_grp.group(3).strip()

            #if column list present then create python list
            if cols:
                cols = re.sub(r'\s', '', cols, flags=re.S|re.I)
                cols = re.sub(r',', '","', cols, flags=re.S|re.I)
                cols = '"' + cols + '"'
                cnv_code += "ins_cols = [" + cols + "]\n"
            
            else:
                #pyspark code to get column list from df
                cnv_code += "ins_cols = " + table_df + ".columns\n"
        
            #remove colon and new line from column value string
            vals = re.sub(r':', '', vals, flags=re.S|re.I)
            vals = re.sub(r'[\r\n]+', '', vals, flags=re.S|re.I)
            
            #custom split column value string
            val_list = util.newSplit(vals, ',')
            
            #create python list for column values
            val_str = ''
            for val in val_list:
                val = val.strip()
                #if column value is in single quote
                if re.search(r"^'", val):
                    val_str += '"' + val + '",'
                #if column value is function or CASE statement    
                elif re.search(r'\w+\s*\(|^CASE\s+', val, re.S|re.I ):
                    vals = re.sub(r'\s+', ' ', vals, flags=re.S|re.I)
                    val_str += '"' + val + '",'
                else:
                    val_str += val + ','
            val_str = val_str[:-1]

            #pyspark code to create select string
            cnv_code += "ins_vals = [" + val_str + "]\n"
            cnv_code += "col_val_str = ''\n"        
            cnv_code += "for idx in (range(len(ins_cols))):\n"
            cnv_code += "\tcol_val_str = col_val_str + str(ins_vals[idx]) + ' as ' + ins_cols[idx] + ','\n"
            cnv_code += "else:\n"
            cnv_code += "\tcol_val_str = col_val_str[:-1]\n"
            #final select statement
            select_str = "'select ' + col_val_str"
            
        #if table present in union check list
        if table in cnv_ds.union_chklist:
            #create temporary data frame
            table_df_tmp = table_df + '_1'
            
            #pyspark code to load data in temporary data frame
            cnv_code += f"#Create temporary dataframe with records to be inserted into {table}\n"
            cnv_code += f"{table_df_tmp} = spark.sql({select_str})\n"
            
            #pyspark code to insert data into db table dataframe from temorary data frame
            cnv_code += f"#Load records into {table} dataframe \n"
            cnv_code += f"mod_df[\'{table_df}\'] = mod_df[\'{table_df}\'].union({table_df_tmp})\n"
            cnv_code += f"mod_df[\'{table_df}\'].createOrReplaceTempView('{table_df}')\n\n"
        else:
            #add table in union check list
            cnv_ds.union_chklist.append(table)
            #pyspark code to load data into db table dataframe
            cnv_code += f"#Load records into {table} dataframe \n"
            cnv_code += f"mod_df[\'{table_df}\'] = spark.sql({select_str})\n"
            cnv_code += f"mod_df[\'{table_df}\'].createOrReplaceTempView('{table_df}')\n\n"
            
        return cnv_code, cnv_log    
        
        
class Delete(TeradataStatement):
    def __init__(self, text):
        super().__init__(text)

    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'teradata':
            if cnv_ds.cntx.in_code_type == 'bteq':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    cnv_code, cnv_log = self.toPyspark(cnv_ds)
                    #cnv_code = cnv_log = ''
        
        return cnv_code, cnv_log
        
    def toPyspark(self, cnv_ds):
        #check if deleting all records from a table
        del_mtch = re.match(r'\bDELETE\s+(?:\bFROM\s*)?(\w*\.?\w+)\s*(?:ALL)?\s*;?', self.text, re.S|re.I)
        if del_mtch:
            table = del_mtch.group(1)
            table_df = cnv_ds.table_df_map[table]
            if table_df not in cnv_ds.trnc_tables:
                cnv_ds.trnc_tables += f"'{table_df}', "
        
        try:
            lexer = TDantlrLexer(InputStream(self.text))
            stream = CommonTokenStream(lexer)
            parser = TDantlrParser(stream)
            parser.addErrorListener(TDErrorListener())
            tree = parser.start()
            psp = PySparkParse(stream)
            walker = ParseTreeWalker()
            walker.walk(psp, tree)
            stmt = psp.out_sql
        except Exception as e:
            stmt = self.simpleParse() + ';'

        cnv_code = ''
        cnv_log = ''
            
        #remove keyword ALL
        stmt = re.sub(r'\s*\bALL\b', '', stmt, flags=re.S|re.I)
        #add keyword FROM if not present
        if not(re.search(r'\bDELETE\s+\bFROM\b', stmt, re.S|re.I)):
            stmt = re.sub(r'(\bDELETE\s+)', r'\1from ', stmt, flags=re.S|re.I)
            
        #get table name where records will be delete
        table = re.search(r'\bDEL(?:ETE)?\b\s+FROM\s+(\w*\.?\w+)', stmt, re.S|re.I).group(1)
        #get data frame name of the table
        table_df = cnv_ds.table_df_map[table]
        
        #prcess delete statemanet if target db table or records inserted before
        if table in cnv_ds.union_chklist:
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
                from_str = re.search(r'(?<=FROM)\s+(.*?)\;', stmt, re.S|re.I).group(1).strip()
            
            #check if from clause has multiple tables
            if re.search(r'\,', from_str, re.S|re.I):
                #break from clause in 2 parts. part 1: first table. part 2: rest of the from clause
                first_table, other_table = re.search(r'^(.*?)(\,.*?$)', from_str, re.S|re.I).groups()
                first_table = first_table.strip()
                other_table = other_table.strip()
                #get alias of the first table, in case of no alias table name will be returned
                alias = re.search(r'{}\s+(?:AS\s+)?(\w+)'.format(table), first_table, re.S|re.I).group(1).strip()
                #select statement to get records to be deleted
                subtract_sql = f'select {alias}.* \nfrom {from_str} \nwhere {where_str}'
            else:
                #get alias of the table, in case of no alias table name will be returned
                alias = re.search(r'{}\s*(?:AS\s+)?(\w*)'.format(table), from_str, re.S|re.I).group(1).strip()
                #select statement to get records to be deleted.
                subtract_sql = 'select ' + (f'{alias}.*' if alias else '*') + f'\nfrom {from_str}' + (f'\nwhere {where_str}' if where_str else '')

            #remove semi-colon at the end
            if subtract_sql.strip()[-1] == ';':
                subtract_sql = subtract_sql.strip()[:-1]                
            #replace db table names with corresponding data frame name
            subtract_sql = util.replaceTableWithDF(subtract_sql, cnv_ds, 1)
            #enclose modified sql in triple quote
            subtract_sql = '"""' + subtract_sql + '"""'
            #[TODO] check if modified statement using variable
                
            table_df_tmp = table_df + '_1'
            #pyspark code to remove deleted records from data frame
            cnv_code += f"{table_df_tmp} = spark.sql({subtract_sql})\n"
            cnv_code += f"mod_df[\'{table_df}\'] = mod_df[\'{table_df}\'].subtract({table_df_tmp})\n"
            cnv_code += f"mod_df[\'{table_df}\'].createOrReplaceTempView('{table_df}')\n\n"

            return cnv_code, cnv_log

            
class Update(TeradataStatement):
    def __init__(self, text):
        super().__init__(text)

    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'teradata':
            if cnv_ds.cntx.in_code_type == 'bteq':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    cnv_code, cnv_log = self.toPyspark(cnv_ds)
                    #cnv_code = cnv_log = ''
        
        return cnv_code, cnv_log
        
    def toPyspark(self, cnv_ds):
        try:
            lexer = TDantlrLexer(InputStream(self.text))
            stream = CommonTokenStream(lexer)
            parser = TDantlrParser(stream)
            parser.addErrorListener(TDErrorListener())
            tree = parser.start()
            psp = PySparkParse(stream)
            walker = ParseTreeWalker()
            walker.walk(psp, tree)
            stmt = psp.out_sql
        except Exception as e:
            stmt = self.simpleParse() + ';'        
        
        cnv_code = ''
        cnv_log = ''
        
        #if update statement has FROM keyword
        if re.search(r'\bUPD(?:ATE)?[^\(]*?\bFROM\b', stmt, re.S|re.I):
            #get table alias which will be updated
            upd_table_alias = re.search(r'\bUPD(?:ATE)?\s+(.*?)\s+(?=FROM)', stmt, re.S|re.I).group(1).strip()
            #get list of tables involved in update statement
            upd_from_str = re.search(r'(?<=FROM)\s+(.*?)\s+(?=SET)', stmt, re.S|re.I).group(1).strip()
            #get table name that will be updated
            upd_table = re.search(r'(\w*\.?\w+)\s+(?:AS\s+)?{}'.format(upd_table_alias), upd_from_str, re.S|re.I).group(1).strip()
            
            #get data frame name of the table
            upd_table_df = cnv_ds.table_df_map[upd_table]
            upd_table_df_tmp_1 = upd_table_df + '_1'
            upd_table_df_tmp_2 = upd_table_df + '_1'
            
            #get the set section of update statement
            upd_set_str = re.search(r'(?<=SET)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I).group(1).strip()
            #get the where section of update statement
            upd_where_str = re.search(r'\bSET\b.*?\bWHERE\b(.*?);', stmt, re.S|re.I).group(1).strip()
            
            #make sql to select records that will be updated
            subtract_sql = f"select {upd_table_alias}.* \nfrom {upd_from_str} \nwhere {upd_where_str}"
            
            #replace db table names with corresponding dataframe name
            subtract_sql = util.replaceTableWithDF(subtract_sql, cnv_ds, 1)
            #[TODO] replace variable name

            #enclose modified sql in triple quote
            subtract_sql = '"""' + subtract_sql + '"""'
            
            
            #create python dictionary with kay as column to be updated and value as update value
            upd_col_dict = '{'
            #split set sction of update statement to get each column assignment
            set_fields = util.newSplit(upd_set_str, ',') 
            #for each column assignment get update column and updating value
            for field in set_fields:
                side = field.split('=')
                side[0] = side[0].strip()
                side[1] = side[1].strip()
                #if update column has table reference
                if re.match(r'\w+\.\w+', side[0], re.S|re.I):
                    upd_col_dict += f'"{side[0]}":"{side[1]} as{side[0]}"' + ",\n"
                else:
                    upd_col_dict += f'"{upd_table_alias}.{side[0]}":"{side[1]} as {side[0]}"' + ",\n"
            
            upd_col_dict = upd_col_dict[:-2] + '}'    
                
            #pyspark code for update value select statement
            cnv_code += f"df_col_list = mod_df['{upd_table_df}'].columns\n"
            cnv_code += f"df_col_list_str = '{upd_table_alias}.'+',{upd_table_alias}.'.join(df_col_list)\n"
            cnv_code += f"upd_col_dict = {upd_col_dict}\n\n"
            cnv_code += "for col in upd_col_dict.keys():\n"
            cnv_code += "\tdf_col_list_str = re.sub(col,upd_col_dict[col],df_col_list_str, flags=re.I)\n\n"
            
            #final update value select statement
            update_sql = f'"select " + df_col_list_str + """\nfrom {upd_from_str} \nwhere {upd_where_str}"""'
   
        else:
            #get table name which will be updated
            upd_table = re.search(r'\bUPD(?:ATE)?\s+([\.\w]+)', stmt, re.S|re.I).group(1).strip()
            #get corresponding data frame name for the table
            upd_table_df = cnv_ds.table_df_map[upd_table]
            upd_table_df_tmp_1 = upd_table_df + '_1'
            upd_table_df_tmp_2 = upd_table_df + '_2'
            
            #check if update statement has where condition
            if re.search(r'\bWHERE\b', stmt, re.S|re.I):
                #get the set section of update 
                upd_set_str = re.search(r'(?<=SET)\s+(.*?)\s+(?=WHERE)', stmt, re.S|re.I).group(1).strip()
                #get the where section of update statement
                upd_where_str = re.search(r'\bSET\b.*?\bWHERE\b(.*)', stmt, re.S|re.I).group(1).strip()
            else:
                #get the set section of update
                upd_set_str = re.search(r'(?<=SET)\s+(.*?);', stmt, re.S|re.I).group(1).strip()
                upd_where_str = ''
                
            #make sql to select records that will be updated
            subtract_sql = f"select * \nfrom {upd_table_df} "+ (f"\nwhere {upd_where_str}" if upd_where_str else '')
            #remove semi-colon at the end
            if subtract_sql.strip()[-1] == ';':
                subtract_sql = subtract_sql.strip()[:-1]  
            #replace db table names with corresponding data frame name
            subtract_sql = util.replaceTableWithDF(subtract_sql, cnv_ds, 1)
            #enclose modified sql in triple quote
            subtract_sql = '"""' + subtract_sql + '"""'
            #[TODO]check if modified statement using variable

            
            #create python dictionary with kay as column to be updated and value as update value
            upd_col_dict = '{'
            #split set sction of update statement to get each column assignment
            set_fields = util.newSplit(upd_set_str, ',') 
            #for each column assignment get update column and updating value
            for field in set_fields:
                side = field.split('=')
                side[0] = side[0].strip()
                side[1] = side[1].strip()
                upd_col_dict += f'"{side[0]}":"{side[1]} as {side[0]}",\n'
            upd_col_dict = upd_col_dict[:-2] + '}'   
            
            #pyspark code for update value select statement
            cnv_code += f"df_col_list = mod_df['{upd_table_df}'].columns\n"    
            cnv_code += "df_col_list_str = ','.join(df_col_list)\n"    
            cnv_code += f"upd_col_dict = {upd_col_dict}\n\n"    
            cnv_code += "for col in upd_col_dict.keys():\n"    
            cnv_code += "\tdf_col_list_str = re.sub(col,upd_col_dict[col],df_col_list_str, flags=re.I)\n\n"    
            #final update value select statement    
            update_sql = f'"select " + df_col_list_str + """\nfrom {upd_table}' + (f'\nwhere {upd_where_str}"""' if upd_where_str else '')    

            #remove semi-colon at the end
            if update_sql[-4:] == ';"""':
                update_sql = update_sql[:-4] + '"""'
        
        #replace db table names with corresponding data frame name
        update_sql = util.replaceTableWithDF(update_sql, cnv_ds, 1)
        #[TODO] check if modified statement using variable

        #pyspark code to update records in dataframe
        cnv_code += f"{upd_table_df_tmp_1} = spark.sql({subtract_sql})\n\n"
        cnv_code += f"{upd_table_df_tmp_2} = spark.sql({update_sql})\n\n"
        cnv_code += f"mod_df[\'{upd_table_df}\'] = mod_df[\'{upd_table_df}\'].subtract({upd_table_df_tmp_1}).union({upd_table_df_tmp_2})\n"
        cnv_code += f"mod_df[\'{upd_table_df}\'].createOrReplaceTempView('{upd_table_df}')\n\n"

        return cnv_code, cnv_log
        
        
class Merge(TeradataStatement):
    def __init__(self, text):
        super().__init__(text)

    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'teradata':
            if cnv_ds.cntx.in_code_type == 'bteq':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    cnv_code, cnv_log = self.toPyspark(cnv_ds)
                    #cnv_code = cnv_log = ''
        
        return cnv_code, cnv_log
        
    def toPyspark(self, cnv_ds):    
        try:
            lexer = TDantlrLexer(InputStream(self.text))
            stream = CommonTokenStream(lexer)
            parser = TDantlrParser(stream)
            parser.addErrorListener(TDErrorListener())
            tree = parser.start()
            psp = PySparkParse(stream)
            walker = ParseTreeWalker()
            walker.walk(psp, tree)
            stmt = psp.out_sql
        except Exception as e:
            stmt = self.simpleParse() + ';'
        
        #[TEMP_FIX] for db reference in columns
        stmt = re.sub(r'(\w+)\.(\w+)\.(\w+)', r'\2.\3', stmt, flags = re.S|re.I)

        cnv_code = ''
        cnv_log = ''
        
        #get primary table name and alias
        p_table, p_table_alias = re.search(r'\bMERGE\s+INTO\s+([\w\.]+)\s+(?:AS\s+)?(\w*)\s*USING\b', stmt, re.S|re.I).groups()

        #get corresponding data frame name for the primary table
        p_table_df = cnv_ds.table_df_map[p_table]
        
        #[TODO] if p_table_alias is missing
        #[TEMP_FIX]
        if not(bool(p_table_alias)):
            if '.' in p_table:
                p_table_alias = p_table.split('.')[-1]
            else:
                p_table_alias = p_table
        
        #check if secondary table is a subquery
        if re.search(r'\bUSING\s*\(\s*SELECT', stmt, re.S|re.I):
            #get select statement of secondary table
            s_table_sql, s_table_alias = re.search(r'\bUSING\s*\((.*?)\)\s*(?:AS\s+)?(\w*)\s+ON\b', stmt, re.S|re.I).groups()
            
            #make dataframe name for secondary table select statement
            if s_table_alias:
                s_table = s_table_alias + '__df'
            else:
                #[TODO] if p_table_alias is missing
                s_table = 'temp__df'
                
            #replace table name with dataframe name
            s_table_sql = util.replaceTableWithDF(s_table_sql, cnv_ds, 1)
            #[TODO] check if modified statement using variable
            #pyspark code to create df for secondary table
            cnv_code += f'{s_table} = spark.sql("""{s_table_sql}""")\n'
            cnv_code += f'{s_table}.createOrReplaceTempView("{s_table}")\n\n'
        else:
            #get secondary table name and alias
            s_table, s_table_alias = re.search(r'\bUSING\s+([\w\.]+)\s*(?:AS\s+)?(\w*)\s+ON\b', stmt, re.S|re.I).groups()

            #[TODO] if s_table_alias is missing
            #[TEMP_FIX]
            if not(bool(s_table_alias)):
                if '.' in s_table:
                    s_table_alias = s_table.split('.')[-1]
                else:
                    s_table_alias = s_table
                    
        #get merge join condition
        join_cond_str = re.search(r'\bON\s+\(?(.*?)\)?\s+WHEN', stmt, re.S|re.I).group(1).strip()
        
        #get merge update set section
        upd_set_str_match = re.search(r'\bUPD(?:ATE)?\s+SET\s+(.*?)(?:(?:WHEN\b)|;)', stmt, re.S|re.I)
        if upd_set_str_match:
            upd_set_str = upd_set_str_match.group(1).strip()
        else:
            upd_set_str = ''

        if upd_set_str:
            #make sql to select records that will be updated
            subtract_sql = f'select {p_table_alias}.* \n from {p_table} as {p_table_alias} \n inner join {s_table} as {s_table_alias} \n on {join_cond_str}'
            #replace db table names with corresponding data frame name
            subtract_sql = util.replaceTableWithDF(subtract_sql, cnv_ds, 1)
            #[TODO] check if modified statement using variable
            
            p_table_df_tmp_1 = p_table_df + '_1'
            #pyspark code to load temp df with records to be updated
            cnv_code += '#Load records to be updated temporary dataframe\n'
            cnv_code += f'{p_table_df_tmp_1} = spark.sql("""{subtract_sql}""")\n'
            
            #create python dictionary with kay as column to be updated and value as update value
            upd_col_dict = '{'        
            #custom split set sction of UPDATE statement to get each column assignment
            set_fields = util.newSplit(upd_set_str, ',')
            #for each column assignment get update column and updating value
            for field in set_fields:
                side = field.split('=')
                side[0] = side[0].strip()
                side[1] = side[1].strip()
                #if update column has table reference
                if re.match(r'\w+\.\w+', side[0], re.S|re.I):
                    upd_col_dict += f'"{side[0]}":"{side[1]} as{side[0]}"' + ",\n"
                else:
                    upd_col_dict += f'"{p_table_alias}.{side[0]}":"{side[1]} as {side[0]}"' + ",\n"
            
            upd_col_dict = upd_col_dict[:-2] + '}'    

            #pyspark code to get colummn list of update table
            cnv_code += f"#Get column list of db table {p_table}\n"
            cnv_code += f"df_col_list = mod_df['{p_table_df}'].columns\n"
            cnv_code += "#Convert column list to string with db table alias\n"
            cnv_code += f"df_col_list_str = '{p_table_alias}.'+',{p_table_alias}.'.join(df_col_list)\n"
            #pyspark code to get first column of update table
            cnv_code += f"#Get first column of db table {p_table}\n"
            cnv_code += "check_col = df_col_list_str[0:df_col_list_str.index(',')]\n"
            #pyspark code to replace update columns with updating values
            cnv_code += "#Create dictionary with update column and udating value\n"
            cnv_code += f"upd_col_dict = {upd_col_dict}\n"
            cnv_code += "#Substitute update columns in column string with updating value\n"
            cnv_code += "for col in upd_col_dict.keys():\n"
            cnv_code += "\tdf_col_list_str = re.sub(col,upd_col_dict[col],df_col_list_str, flags=re.I)\n"

            #make sql to select records with updated column value
            update_sql = f'"select " + df_col_list_str + """\n from {p_table} as {p_table_alias} \n inner join {s_table} as {s_table_alias} \n on {join_cond_str}"""'
            #replace db table names with corresponding data frame name
            update_sql = util.replaceTableWithDF(update_sql, cnv_ds, 1)
            
            p_table_df_tmp_2 = p_table_df + '_2'
            #pyspark code to load temp df with updated records
            cnv_code += "#Load temporary dataframe with updated records\n"
            cnv_code += f"{p_table_df_tmp_2} = spark.sql({update_sql})\n"
        else:
            update_sql = ''
            
        #get merge insert section
        insert_str_match = re.search(r'\bINS(?:ERT)?\s+(.*?)(?:(?:WHEN\b)|;)', stmt, re.S|re.I)    
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
            insert_sql = f'"""select {select_str} \n from {s_table} as {s_table_alias} \n left outer join {p_table} as {p_table_alias} \n on {join_cond_str} \n where """ + check_col + " is null"'
            #replace table name with dataframe name
            insert_sql = util.replaceTableWithDF(insert_sql, cnv_ds, 1)
            #[TODO] check if modified statement using variable
        
            p_table_df_tmp_3 = p_table_df + '_3'
            #pyspark code to load temp df with merge insert records
            cnv_code += "#Load temporary dataframe merge insert records\n"
            cnv_code += f"{p_table_df_tmp_3} = spark.sql({insert_sql})\n\n"
        
        else:
            insert_sql = ''
        
        #pyspark code to remove old records from db table dataframe and insert merge update and insert records
        cnv_code += "#Remove old records from db table dataframe and insert merge update and insert records\n"
        
        cnv_code += (f"mod_df['{p_table_df}'] = mod_df['{p_table_df}']") + (f".subtract({p_table_df_tmp_1}).union({p_table_df_tmp_2})" if update_sql else '') + (f".union({p_table_df_tmp_3})\n" if insert_sql else '')
        cnv_code += f"mod_df['{p_table_df}'].createOrReplaceTempView('{p_table_df}')\n"
        
        return cnv_code, cnv_log

   
class Call(TeradataStatement):
    def __init__(self, text):
        super().__init__(text)

    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'teradata':
            if cnv_ds.cntx.in_code_type == 'bteq':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    cnv_code, cnv_log = self.toPyspark(cnv_ds)
                    #cnv_code = cnv_log = ''
                    
        return cnv_code, cnv_log
        
    def toPyspark(self, cnv_ds):
        stmt = self.text + ';'
        stmt = self.simpleParse(stmt)
        cnv_code = ''
        cnv_log = ''           
        
        #read sp out parameter position file
        with open('.\\Teradata\\sp_out_param.pos', 'r') as f:
            pos_txt = f.read()
        pos_txt_sps = re.findall('(\w+) \[', pos_txt)
        
        #get sp name and parameter list
        sp_name, sp_params = re.search(r'CALL\s+(?:\w+(?=\.))?\.?\s*(\w+)\s*\((.*?)\)\s*;', stmt, re.S|re.I).groups()
        sp_params = re.sub(r'[\s\:]', '', sp_params, re.S)
        sp_param_list = util.newSplit(sp_params, ',')
        
        #if sp name present in out parameter position file
        if sp_name in pos_txt_sps:
            #get out parameter indexes
            out_param_pos = re.search(r'{} \[(.*?)\]'.format(sp_name), pos_txt, re.S|re.I).group(1)
            
            if out_param_pos:
                #make out parameter string
                out_param_idxs = [int(idx) for idx in out_param_pos.split(',')]
                out_param_str = ''
                for idx in out_param_idxs:
                    out_param_str += sp_param_list[idx] + ','
                out_param_str = out_param_str[:-1]
                
                #make in parameter string
                param_idxs = [idx for idx in range(len(sp_param_list))]
                in_param_idxs = list(set(param_idxs) - set(out_param_idxs))
                in_param_str = ''
                for idx in in_param_idxs:
                    in_param_str += sp_param_list[idx] + ','
                in_param_str = in_param_str[:-1]
                
                cnv_code += f"{out_param_str} = sp_name({in_param_str})"
            else:
                cnv_code += f"{sp_name}({sp_params})"
                
            #add import code
            cnv_ds.output.code[0] += f"from {sp_name} import {sp_name}\n"   
        
            return cnv_code, cnv_log
        
        else:
            #Error
            return '', f'Statement Spipped \n\t {self.text}'
            
            
class VolTbl(TeradataStatement):
    def __init__(self, text):
        super().__init__(text)

    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'teradata':
            if cnv_ds.cntx.in_code_type == 'bteq':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    cnv_code, cnv_log = self.toPyspark(cnv_ds)
                    #cnv_code = cnv_log = ''
        
        return cnv_code, cnv_log
        
    def toPyspark(self, cnv_ds):
        stmt = self.text + ';'
        cnv_code = ''
        cnv_log = ''
        
        #get volatile table name
        table = re.search(r'\bVOLATILE\s+TABLE\s+(\w+).*?;', stmt, re.S|re.I).group(1).strip()
        #get table dataframe name
        table_df = cnv_ds.table_df_map[table]
        #get SELECT statement populating volatile table
        sel_srch = re.search(r'(\bSEL(?:ECT)?\b.*?)\)?\s*WITH', stmt, re.S|re.I)
        
        #if select sql found
        if sel_srch:
            stmt = sel_srch.group(1).strip()
            try:
                lexer = TDantlrLexer(InputStream(stmt))
                stream = CommonTokenStream(lexer)
                parser = TDantlrParser(stream)
                parser.addErrorListener(TDErrorListener())
                tree = parser.start()
                psp = PySparkParse(stream)
                walker = ParseTreeWalker()
                walker.walk(psp, tree)
                stmt = psp.out_sql
            except Exception as e:
                print(e)
                stmt = self.simpleParse(stmt)
            
            #remove semi-colon at the end
            if stmt.strip()[-1] == ';':
                stmt = stmt.strip()[:-1]
            
            #replace db table names with corresponding data frame name
            stmt = util.replaceTableWithDF(stmt, cnv_ds, 1)   

            #enclose modified sql in triple quote
            stmt = '"""' + stmt + '"""'            
        
            #add table in union check list
            cnv_ds.union_chklist.append(table)
        
            #pyspark code to load data into volatile table dataframe
            cnv_code += f"#Load records into {table} dataframe\n"
            cnv_code += f"mod_df[\'{table_df}\'] = spark.sql({stmt})\n"
            cnv_code += f"#Create local temporary view for dataframe {table_df}\n"
            cnv_code += f"mod_df[\'{table_df}\'].createOrReplaceTempView(\'{table_df}\')\n"

        return cnv_code, cnv_log    

        
class CtrlStmt(TeradataStatement):
    def __init__(self, text):
        super().__init__(text)

    def convStatement(self, cnv_ds):
        #check conversion context
        if cnv_ds.cntx.source_db == 'teradata':
            if cnv_ds.cntx.in_code_type == 'bteq':
                if cnv_ds.cntx.out_code_type == 'pyspark':
                    cnv_code, cnv_log = self.toPyspark(cnv_ds)
                    #cnv_code = cnv_log = ''
        
        return cnv_code, cnv_log
        
    def toPyspark(self, cnv_ds):
        stmt = self.text
        #parse BTEQ control statement
        rs = self.parseCtrlStatement(stmt)
        
        #for error then quit statement
        if rs == 'if_error_quit':
            #put last pyspark code in try...except block if not enclosed already
            last_out_code = cnv_ds.output.code[cnv_ds.cd_idx - 1]
            if not bool(re.search(r'^try:.*?\bexcept:', last_out_code, re.S|re.I)):
                #align and add converted code in pyspark_code_array
                last_out_code = re.sub(r'^', r'\t', last_out_code, flags=re.M).strip()
                #wrap into try...except
                ts = "\t"*cnv_ds.tabs
                new_last_out_code = ts + "try:\n\t" + last_out_code + "\n" + ts + "except:\n" + ts + "\traise\n" + ts + "\t#quit()\n"
                #update last out code
                cnv_ds.output.code[cnv_ds.cd_idx - 1] = new_last_out_code
        
        #for goto based on activity count statement
        elif 'if_activity_goto' in rs:
            #get label name and condition
            cnd = rs.split(';')[1]
            lbl = rs.split(';')[2]
            #store goto label
            cnv_ds.goto_label.append(lbl)
            #align and add converted code
            ts = "\t"*cnv_ds.tabs
            cnv_ds.output.addCode(ts + cnd, '')
            cnv_ds.tabs += 1
            cnv_ds.cd_idx += 1
            
        #for label statement
        elif 'label_' in rs:
            #get label name
            lbl = re.search(r'label_(\w+)', rs).group(1)
            #remove label
            if lbl in cnv_ds.goto_label:
                cnv_ds.goto_label.remove(lbl)
                cnv_ds.tabs -= 1
                cnv_ds.output.code[cnv_ds.cd_idx - 1] += "\n"
            
        #for remark statement
        elif 'remark_txt' in rs:
            #get code string
            remark = re.search(r'^remark_txt;(.*)', rs).group(1)
            #align and add converted code in pyspark_code_array
            ts = "\t"*cnv_ds.tabs
            cnv_ds.output.addCode(ts + remark, '')
            cnv_ds.cd_idx += 1
        
        #for quit statement
        if rs == 'quit':
            ts = "\t"*cnv_ds.tabs
            cnv_ds.output.addCode(ts + 'quit()', '')
            cnv_ds.cd_idx += 1

        #for statements not supported
        else:
            cnv_ds.output.addCode('', f'[WARN]:"Statement skipped\n{stmt}\n')
        
        return None, None
        
    def parseCtrlStatement(self, stmt):
        #check if statement has GOTO
        if re.search(r'\.GOTO\b', stmt, re.S|re.I):
            #if statement has condition on activity count
            if re.search(r'^\.IF\s+ACTIVITYCOUNT\b.*?\bTHEN\s+', stmt, re.S|re.I):
                #get goto label name
                goto_label = re.search(r'\.GOTO\s+(\w+)', stmt, re.S|re.I).group(1).strip()
                
                #convert if condition and operators
                if_condition = re.sub(r'\s+\bTHEN.*?$', r'):', stmt, flags=re.S|re.I)
                if_condition = re.sub(r'\.IF\s+ACTIVITYCOUNT\s+', r'if not(last_activity__df.count() ', if_condition, flags=re.S|re.I)    
                if_condition = re.sub(r'=', r'==', if_condition, flags=re.S|re.I)
                if_condition = re.sub(r'<>', r'!=', if_condition, flags=re.S|re.I)

                return f"if_activity_goto;{if_condition};{goto_label}"
            else:
                return 'warn_log:Statement not supported'
        else:
            if re.search(r'^\.IF\b', stmt, re.S|re.I):
                #for sql error then quit statement
                if re.search(r'^\.IF\s+ERROR.*?\bTHEN\s+\.?\bQUIT\b', stmt, re.S|re.I):
                    return 'if_error_quit'
                else:
                    return 'warn_log:Statement not supported'
            else:
                #for label statatement
                lbl_srch = re.search(r'^\.LABEL\s+(\w+)', stmt, re.S|re.I)
                if lbl_srch:
                    label = lbl_srch.group(1)
                    return f"label_{label}"
                #for remark statement
                rmk_srch = re.search(r'^\.REMARK\s+\'(.*?)\'', stmt, re.S|re.I)
                if rmk_srch:
                    remark = rmk_srch.group(1)
                    return f"remark_txt;print('{remark}')"
                #for quit statement
                quit_srch = re.search(r'^\.QUIT', stmt, re.S|re.I)
                if quit_srch:
                    return 'quit'
                #if none of the above
                return 'warn_log:Statement not supported'
