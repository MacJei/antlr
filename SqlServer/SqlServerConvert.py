from OutputScript import OutputScript
import re
import util

class SqlServerConvert():

    class ConvDataStore():
        def __init__(self, cntx, output):
            self.cntx = cntx
            self.output = output
            self.table_df_map = {}
            self.var_datatype = {}
            self.union_chklist = []
            self.with_tables = []
            self.cd_idx = 0
            self.tabs = 0
            self.trnc_tables = ''
            self.rtn_parm_str = ''
            self.sp_name = ''
            
            
    def __init__(self, cntx, input):
        self.cntx = cntx
        self.input = input

        #make output filename
        if '.' in input.fname:
            out_fname = input.fname[::-1].split('.',1)[1][::-1] + '.py'
        else:
            out_fname = input.fname + '.py'
        #make output script object
        self.output = OutputScript(cntx, out_fname)
        
        #create data store for conversion
        self.cnv_ds = SqlServerConvert.ConvDataStore(self.cntx, self.output)
        self.cnv_ds.table_df_map, self.cnv_ds.union_chklist = self.scanScript()        

        
    #create list of all participating tables in the script and derive data frame name    
    def scanScript(self):
        table_df_map = {}
        union_chklist = []
        
        self.cntx.logger.add_log('INFO', 'Scanning script contect for all the tables used')
        
        insert_db_tables = re.findall(r'\bINSERT\s+(?:INTO\s+)?([\w\[\]#\.]+)', self.input.text, re.S|re.I)
        insert_db_tables = list(tbl.strip() for tbl in insert_db_tables)
        #print(insert_db_tables)
        
        delete_db_tables = re.findall(r'\bDELETE\b\s*(?:FROM)?\s+([\w\[\]#\.]+)', self.input.text, re.S|re.I)
        delete_db_tables = list(tbl.strip() for tbl in delete_db_tables)
        truncate_db_tables = re.findall(r'\bTRUNCATE\s+TABLE\s+([\w\[\]#\.]+)', self.input.text, re.S|re.I)
        delete_db_tables += list(tbl.strip() for tbl in truncate_db_tables)        
        #print(delete_db_tables)
        
        update_db_tables = re.findall(r'\bUPDATE\s+([\w\[\]#\.]+)', self.input.text, re.S|re.I)
        update_db_tables = list(filter((lambda tbl: tbl not in self.input.upd_tbl_alias), update_db_tables))
        update_db_tables = list(tbl.strip() for tbl in update_db_tables)
        #print(update_db_tables)
        
        merge_db_tables1 = re.findall(r'\bMERGE\s+([\w\[\]#\.]+)\s+', self.input.text, re.S|re.I)
        merge_db_tables2 = re.findall(r'\bUSING\s+([\w\[\]#\.]+)\s+', self.input.text, re.S|re.I)
        merge_db_tables = list(tbl.strip() for tbl in (merge_db_tables1 + merge_db_tables2)) 
        #print(merge_db_tables)
        
        from_db_tables_init = re.findall(r'\bFROM\s+([\w\[\]#\.\s\,]+?)(?:\bAS\b|\bWHERE\b|\bGROUP\b|\bINNER\b|\bOUTER\b|\bLEFT\b|\bRIGHT\b|\bFULL\b|\bJOIN\b|\bORDER\b|\bWITH\b|\bINTERSECT\b|\bUNION\b|\bEXCEPT\b|\bMINUS\b|\(|\)|;)', \
                                         self.input.text, re.S|re.I)
        from_db_tables = []
        for tbl in from_db_tables_init:
            if ',' in tbl:
                tables = tbl.split(',')
                from_db_tables.extend(tables)
            else:
                from_db_tables.append(tbl.strip())
        #print(from_db_tables)
        
        join_db_tables = re.findall(r'\bJOIN\s+([\w\[\]#\.]+)', self.input.text, re.S|re.I)
        join_db_tables = list(tbl.strip() for tbl in join_db_tables)
        #print(join_db_tables)
        
        all_db_tables = insert_db_tables + delete_db_tables + update_db_tables + merge_db_tables + from_db_tables + join_db_tables
        #print(insert_db_tables , delete_db_tables , update_db_tables , merge_db_tables , from_db_tables , join_db_tables)
        
        if all_db_tables:
            for tbl in all_db_tables:
                tbl = tbl.strip()
                if tbl:
                    #remove unwanted chars
                    tbl = re.sub(r'[ ]*(?:\bAS\b)?[ ]+\w+\s*$', '', tbl, flags=re.I)
                    tbl = re.sub(r'\]\s*\w+\s*$', '\]', tbl).replace('\\', '')

                    #make df name
                    if tbl not in ['set']:                    
                        tbl = tbl.strip()
                        tbl = re.sub(r'[\.]+', '.', tbl)
                        df = tbl.replace(' ', '_')
                        df = df.replace('#', '')
                        df = df.replace('[', '')
                        df = df.replace(']', '')
                        df = df.replace('.', '__')
                        df += '__df'
                        table_df_map[tbl] = df
                        #print(tbl, ' : ', df)
                    #populate union check list
                    if tbl in (insert_db_tables + delete_db_tables + update_db_tables + merge_db_tables) and  not(tbl == '#' or '.#' in tbl.replace(' ','') or '].#' in tbl.replace(' ','')):
                        union_chklist.append(df)
                else:
                    self.cntx.logger.add_log('WARN', 'Found no db tables. Could be an issue.')
        
        return table_df_map, list(set(union_chklist))
    
    #adds code to create initial dataframes based on execution platform and target database
    def addPlatformRead(self):
        self.cntx.logger.add_log('INFO', 'Adding dataframe read code based on conversion context')
        self.cntx.logger.add_log_details('output code type: ' + self.cntx.out_code_type)
        self.cntx.logger.add_log_details('execution platform: ' + self.cntx.platform)  
    
        if self.cntx.out_code_type == 'pyspark':
            #import based on execution platform
            if self.cntx.platform == 'glue':
                py_import = "#Import packages\n"
                py_import += "import sys\n"
                py_import += "import re\n"
                py_import += "from conn_param import *\n"
                py_import += "from ss_inblt_func import *\n"
                py_import += "from awsglue.transforms import *\n"
                py_import += "from awsglue.utils import getResolvedOptions\n"
                py_import += "from pyspark.context import SparkContext\n"
                py_import += "from awsglue.context import GlueContext\n"
                py_import += "from awsglue.job import Job\n"
                py_import += "from awsglue.dynamicframe import DynamicFrame\n"
                py_import += "from pyspark.sql import SparkSession\n"

                #add import code to output
                self.output.addCode(py_import)
                self.cnv_ds.cd_idx += 1      

                #spark session creation code based on paltform
                code_txt = "#SPDEF#\n"
                code_txt += ' '*4 + "sc = SparkContext.getOrCreate()\n"    
                code_txt += ' '*4 + "glueContext = GlueContext(sc)\n"    
                code_txt += ' '*4 + "spark = glueContext.spark_session\n\n"

                if len(self.cnv_ds.table_df_map) > 0:
                    #pyspark create table dataframe code based on target db
                    code_txt += ' '*4 + "try:\n"
                    code_txt += ' '*4*2 + "#db_tables_dict#"
                    #read dataframe based on target db
                    code_txt += ' '*4*2 + "for table,df in db_tables_dict.items():\n"
                    code_txt += ' '*4*3 + "if df not in mod_df.keys():\n"
                    code_txt += ' '*4*4 + "dym__df = glueContext.create_dynamic_frame.from_catalog(database = catalog_db, table_name = table)\n"
                    code_txt += ' '*4*4 + "org_df[df] = dym__df.toDF()\n"
                    code_txt += ' '*4*4 + "mod_df[df] = dym__df.toDF()\n"
                    code_txt += ' '*4*4 + "mod_df[df].createOrReplaceTempView(df)\n"
                    code_txt += ' '*4 + "except:\n"
                    code_txt += ' '*4*2 + "raise\n"
                    code_txt += ' '*4*2 + "#quit()\n"
                    
                #add code to output
                self.output.addCode(code_txt)
                self.cnv_ds.cd_idx += 1
                
    #adds code to create initial dataframes based on execution platform and target database
    def addPlatformWrite(self):
        self.cntx.logger.add_log('INFO', 'Adding dataframe write code based on conversion context')
        self.cntx.logger.add_log_details('output code type: ' + self.cntx.out_code_type)
        self.cntx.logger.add_log_details('execution platform: ' + self.cntx.platform) 
        
        code_txt = ''
        if self.cntx.out_code_type == 'pyspark':
            if self.cntx.platform == 'glue':
            
                #if returning
                if self.cnv_ds.rtn_parm_str.strip():
                    code_txt += ' '*4 + 'return ' + self.cnv_ds.rtn_parm_str
                    
                #pyspark code to write datafreames to target database
                code_txt += "\n\n#Write modified data frames to target\n"
                code_txt += "if __name__ == '__main__':\n"
                if self.cnv_ds.rtn_parm_str.strip():
                    code_txt += ' '*4 + self.cnv_ds.rtn_parm_str + ' = ' + self.cnv_ds.sp_name + '(*sys.argv[1:])\n'
                else:
                    code_txt += ' '*4 + self.cnv_ds.sp_name + '(*sys.argv[1:])\n'
                
                #final write code
                code_txt += ' '*4 + "try:\n"; 
                code_txt += ' '*4*2 + "for tab_df in mod_df.keys():\n"
                code_txt += ' '*4*3 + "if mod_df[tab_df] == org_df[tab_df]:\n"
                code_txt += ' '*4*4 + "continue\n" 
                code_txt += ' '*4*3 + "target_table = re.search(r'\w*\.?\w+$', tab_df.replace('__df','').replace('__', '.'), re.M).group()\n"
                code_txt += ' '*4*3 + "dym__trans__df = DynamicFrame.fromDF(mod_df[tab_df],glueContext,'dym__trans__df')\n"
                code_txt += ' '*4*3 + "glueContext.write_dynamic_frame.from_options(frame = dym__trans__df, connection_type = 's3', connection_options = {'path': s3_target_dir + target_table}, format = target_file_format)\n"
                code_txt += ' '*4 + "except:\n"; 
                code_txt += ' '*4*2 + "raise\n";

        #add code to output
        self.output.addCode(code_txt)
        self.cnv_ds.cd_idx += 1
            
            
    #starts conversion process for given context
    def convert(self):
        
        self.addPlatformRead()
        
        for stmt in self.input.statements:
            cnv_code = stmt.convStatement(self.cnv_ds)
            if len(cnv_code) > 0:
                self.output.addCode(cnv_code)
                self.cnv_ds.cd_idx += 1

        #make final db table dictionary string         
        db_tables_dict = "db_tables_dict = {"
        for table, df in self.cnv_ds.table_df_map.items():
            if (table[0] != '#') and ('.#' not in table.replace(' ','')) and ('.[#' not in table.replace(' ','')) and table not in self.cnv_ds.with_tables:
                dot_count = table.count('.')
                if dot_count == 0:
                    tab = 'dbo.' + table
                if dot_count > 1: 
                    tab = table.split('.')[-1].strip()
                    sch = table.split('.')[-2].strip()
                    if sch:
                        tab = sch + '.' + tab
                    else:
                        tab = 'dbo.' + tab
                tab = tab.replace('[', '').replace(']', '')
                db_tables_dict += f"'{tab}':'{df}',\n" + ' '*4*2
        db_tables_dict = db_tables_dict.rstrip().strip(',') + '}\n\n'
        #put db table dictionary string in output code 
        self.output.code[1] = self.output.code[1].replace('#db_tables_dict#', db_tables_dict)
                
        self.addPlatformWrite()
        
        return self.output
        