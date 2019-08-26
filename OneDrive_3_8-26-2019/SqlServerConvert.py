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
        
        from_db_tables_init = re.findall(r'\bFROM\s+([\w\]\[#\.\s\,]+?)(?:WHERE|GROUP|JOIN|ORDER|\)|\(|;)', self.input.text, re.S|re.I)
        from_db_tables = []
        for tbl in from_db_tables_init:
            if ',' in tbl:
                tables = tbl.split(',')
                from_db_tables.extend(tables)
            else:
                from_db_tables.append(tbl)
        #print(from_db_tables)
        
        join_db_tables = re.findall(r'\bJOIN\s+([\w\[\]#\.]+)', self.input.text, re.S|re.I)
        join_db_tables = list(tbl.strip() for tbl in join_db_tables)
        #print(join_db_tables)
        
        all_db_tables = insert_db_tables + delete_db_tables + update_db_tables + merge_db_tables + from_db_tables + join_db_tables
        
        tbl_df_map = {}
        for tbl in all_db_tables:
            tbl = tbl.strip()
            if tbl:                
                tbl = re.sub(r'[ ]*(?:AS)?[ ]+\w+$', '', tbl, flags=re.I)
                tbl = re.sub(r'(\]\s*\w+)', '\]', tbl).replace('\\', '')
                
                df = re.sub(r' ', '_', tbl)
                df = re.sub(r'#|\[|\]', '', df)
                df = re.sub(r'\.', '__', df)
                df += '__df'
            
                table_df_map[tbl] = df
                
                if tbl in (insert_db_tables + delete_db_tables + update_db_tables + merge_db_tables) and  \
                   not(tbl == '#' or '.#' in tbl.replace(' ','') or '].#' in tbl.replace(' ','')):
                    
                    union_chklist.append(df)
        
        return table_df_map, list(set(union_chklist))
    
    #adds code to create initial dataframes based on execution platform and target database
    def addPlatformRead(self):
        if self.cntx.out_code_type == 'pyspark':
            #import based on execution platform
            if self.cntx.platform == 'glue':
                py_import = "#Import packages\n"
                py_import += "import sys\n"
                py_import += "import re\n"
                py_import += "from rs_conn_param import *\n"
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

             #   #spark session creation code based on paltform
             #   code_txt = ">>>SP-Header<<<\n"
             #   code_txt += ' '*4 + "try:\n"
             #   code_txt += ' '*4*2 + "sc = SparkContext.getOrCreate()\n"    
             #   code_txt += ' '*4*2 + "glueContext = GlueContext(sc)\n"    
             #   code_txt += ' '*4*2 + "spark = glueContext.spark_session\n\n"
             #   #pyspark create table dataframe code based on target db
             #   code_txt += ' '*4*2 + "try:\n"
             #   code_txt += ' '*4*3 + "db_tables_dict = {"
             #   for key,val in self.cnv_ds.table_df_map.items():
             #       if not(key[1] == '#' or '.#' in key.replace(' ','') or '].#' in key.replace(' ','')) :
             #           code_txt += f"'{key}':'{val}',\n"
             #   code_txt += ' '*4*3 + '}\n\n'
             #   
             #   #read dataframe based on target db
             #   code_txt += ' '*4*3 + "for table,df in db_tables_dict.items():\n"
             #   code_txt += ' '*4*4 + "if df not in mod_df.keys():\n"
             #   code_txt += ' '*4*5 + "dym__df = glueContext.create_dynamic_frame.from_catalog(database=cat_db, table_name=table)\n"
             #   code_txt += ' '*4*5 + "org_df[df] = dym__df.toDF()\n"
             #   code_txt += ' '*4*5 + "mod_df[df] = dym__df.toDF()\n"
             #   code_txt += ' '*4*5 + "mod_df[df].createOrReplaceTempView(df)\n"
             #   code_txt += ' '*4*2 + "except:\n"
             #   code_txt += ' '*4*3 + "raise\n" + ' '*4*3 + "#quit()\n"
             #   #add code to output
             #   self.output.addCode(code_txt)
             #   self.cnv_ds.cd_idx += 1
             #
             #   self.cnv_ds.tabs = 1 
             
                #spark session creation code based on paltform
                code_txt = ">>>SP-Header<<<\n"
                #code_txt += ' '*4 + "try:\n"
                code_txt += ' '*4 + "sc = SparkContext.getOrCreate()\n"    
                code_txt += ' '*4 + "glueContext = GlueContext(sc)\n"    
                code_txt += ' '*4 + "spark = glueContext.spark_session\n\n"
                #pyspark create table dataframe code based on target db
                code_txt += ' '*4 + "try:\n"
                code_txt += ' '*4*2 + "db_tables_dict = {"
                for key,val in self.cnv_ds.table_df_map.items():
                    if not(key[1] == '#' or '.#' in key.replace(' ','') or '].#' in key.replace(' ','')) :
                        code_txt += f"'{key}':'{val}',\n"
                code_txt += ' '*4*2 + '}\n\n'
                
                #read dataframe based on target db
                code_txt += ' '*4*2 + "for table,df in db_tables_dict.items():\n"
                code_txt += ' '*4*3 + "if df not in mod_df.keys():\n"
                code_txt += ' '*4*4 + "dym__df = glueContext.create_dynamic_frame.from_catalog(database=cat_db, table_name=table)\n"
                code_txt += ' '*4*4 + "org_df[df] = dym__df.toDF()\n"
                code_txt += ' '*4*4 + "mod_df[df] = dym__df.toDF()\n"
                code_txt += ' '*4*4 + "mod_df[df].createOrReplaceTempView(df)\n"
                code_txt += ' '*4 + "except:\n"
                code_txt += ' '*4*2 + "raise\n" + ' '*4*2 + "#quit()\n"
                #add code to output
                self.output.addCode(code_txt)
                self.cnv_ds.cd_idx += 1
                
    #adds code to create initial dataframes based on execution platform and target database
    def addPlatformWrite(self):
        code_txt = ''
        if self.cntx.out_code_type == 'pyspark':
            if self.cntx.platform == 'glue':
                
            #    #close try block and return
            #    code_txt += ' '*4   + "except:\n"
            #    code_txt += ' '*4*2   + "raise\n"
            #    
            #    #if returning
            #    if self.cnv_ds.rtn_parm_str.strip():
            #        code_txt += ' '*4   + "else:\n"
            #        code_txt += ' '*4*2 + 'return ' + self.cnv_ds.rtn_parm_str + "\n\n"
                
                #pyspark code to write datafreames to target database
                code_txt += "#Write modified data frames to target\n";
                code_txt += "if __name__ == '__main__':\n"
                code_txt += ' '*4 + self.cnv_ds.rtn_parm_str + ' = ' + self.cnv_ds.sp_name + '(*sys.argv[1:])\n'                
                code_txt += ' '*4 + "try:\n"; 
                code_txt += ' '*4*2 + "for tab_df in mod_df.keys():\n"
                code_txt += ' '*4*3 + "if mod_df[tab_df] == org_df[tab_df]:\n"
                code_txt += ' '*4*4 + "continue\n" 
                code_txt += ' '*4*3 + "dym__trans__df = DynamicFrame.fromDF(mod_df[tab_df],glueContext,'dym__trans__df')\n"
                code_txt += ' '*4*3 + "glueContext.write_dynamic_frame.from_options(frame = dym__trans__df, connection_type = 's3', connection_options = {'path': 's3://target/s3tables'}, format = 'csv')\n"
                code_txt += ' '*4   + "except:\n"; 
                code_txt += ' '*4*2   + "raise\n"; 

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
                
        self.addPlatformWrite()
        
        return self.output
        