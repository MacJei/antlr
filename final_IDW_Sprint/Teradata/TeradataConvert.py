from OutputScript import OutputScript
import re
import util

class TeradataConvert():

    class ConvDataStore():
        def __init__(self, cntx, output):
            self.cntx = cntx
            self.output = output
            self.table_df_map = {}
            self.union_chklist = []
            self.goto_label = []
            self.cd_idx = 0
            self.tabs = 0
            self.trnc_tables = ''

    def __init__(self, cntx, input):
        self.cntx = cntx
        self.input = input
                
        #self.goto_label = []
        #self.trnc_tables = ''
        
        #make output filename
        if '.' in input.fname:
            out_fname = input.fname[::-1].split('.',1)[1][::-1] + '.py'
        else:
            out_fname = input.fname + '.py'
        #make output script object
        self.output = OutputScript(cntx, out_fname)
        
        #create data store for convesion
        self.cnv_ds = TeradataConvert.ConvDataStore(self.cntx, self.output)
        self.cnv_ds.table_df_map, self.cnv_ds.union_chklist = self.scanScript()        
    
    #create list of all participating tables in the script and derive data frame name    
    def scanScript(self):
        table_df_map = {}
        union_chklist = []

        #list tables with DELETE only
        delete_db_tables = re.findall(r'\bDELETE\s+(?!(?:\bFROM\b))(\w*\.?\w+)',self.input.text, re.S|re.I)

        #list tables after FROM
        from_db_tables_int = re.findall(r'\bFROM\s+([\w\s\.?\,]+)\s+WHERE\b|\bFROM\s+(\w*\.?\w+)\b', self.input.text, re.S|re.I)
        #prepare the final list by unpacking the tuple items returned by regular expression 
        from_db_tables = []
        for item in from_db_tables_int:
            item1,item2=item
            if item1 != '' and ',' not in item1:
                from_db_tables.append(item1)
            if item2 != '' and ',' not in item2:
                from_db_tables.append(item2)
            #handle multiple table names in a single string seperated by comma
            if ',' in item1:
                from_db_tables_tmp1 = item1.split(',')
                from_db_tables = from_db_tables + from_db_tables_tmp1
            if ',' in item2:
                from_db_tables_tmp2 = item2.split(',')
                from_db_tables = from_db_tables + from_db_tables_tmp2  

        #list tables after JOIN
        join_db_tables = re.findall(r'\bJOIN\s+(\w*\.?\w+)\b', self.input.text, re.S|re.I)

        #list tables after INSERT
        insert_db_tables = re.findall(r'\bINS(?:ERT)?\s+INTO\s+(\w*\.?\w+)\b', self.input.text, re.S|re.I)

        #list tables after MERGE and/or USING
        merge_db_tables = []
        merge_db_tables1 = re.findall(r'\bMERGE\s+INTO\s+([\w\.]+)\s+(?:AS\s+)?\w*\s*USING\s*(\w*\.?\w+)', self.input.text, re.S|re.I)
        merge_db_tables2 = re.findall(r'\bMERGE\s+INTO\s+([\w\.]+)\s+(?:AS\s+)?\w*\s*USING\s*\(', self.input.text, re.S|re.I)
        #prepare the final list of tables by combining the above two list
        for item in merge_db_tables1:
            item1,item2 = item
            merge_db_tables.append(item1)
            merge_db_tables.append(item2)
        for item in merge_db_tables2:
            merge_db_tables.append(item)
        
        #list tables after UPDATE
        update_db_tables = re.findall(r'\bUPD(?:ATE)?\s+(\w*\.?\w+)\s+(?=SET)(?!\s+FROM)', self.input.text, re.S|re.I)

        #list volatile tables
        volatile_db_tables = re.findall(r'\bVOLATILE\s+TABLE\s+(\w+)', self.input.text, re.S|re.I)

        #consolidate into single list
        all_db_tables = delete_db_tables + from_db_tables + join_db_tables + insert_db_tables + merge_db_tables + update_db_tables + volatile_db_tables

        #format table name list
        for item in all_db_tables:
            #remove leading and trailing spaces
            item = item.strip()
            if item:
                #remove alias if any
                if ' ' in item: 
                    item = item[0:item.index(' ')]
    
                #create dataframe name
                df_name = item.replace('.','__')+'__df'
    
                #add to table_df_mapping hash if not added before
                if item not in table_df_map.keys() and item != '':
                    table_df_map[item] = df_name
    
                #if not volatile table add to union checklist
                if item not in volatile_db_tables:
                    union_chklist.append(item)
                    
        return table_df_map, union_chklist      
    
    #adds code to create initial dataframes based on execution platform and target database
    def addPlatformRead(self):
        if self.cntx.out_code_type == 'pyspark':
            #import based on execution platform
            if self.cntx.platform == 'glue':
                py_import = "#Import packages\n"
                py_import += "import sys\n"
                py_import += "import re\n"
                py_import += "from awsglue.transforms import *\n"
                py_import += "from awsglue.utils import getResolvedOptions\n"
                py_import += "from pyspark.context import SparkContext\n"
                py_import += "from awsglue.context import GlueContext\n"
                py_import += "from awsglue.job import Job\n"
                py_import += "from awsglue.dynamicframe import DynamicFrame\n"
                py_import += "from pyspark.sql import SparkSession\n"
                #import based on target db
                if self.cntx.target_db == 'redshift':            
                    py_import += "import pg8000\n"
                    py_import += "from rs_conn_param import *\n"
                #add import code to output
                self.output.addCode(py_import, '')
                self.cnv_ds.cd_idx += 1
            
            #spark session creation code based on paltform
            code_txt = "sc = SparkContext()\n"    
            code_txt += "glueContext = GlueContext(sc)\n"    
            code_txt += "spark = glueContext.spark_session\n\n"
            #pyspark create table dataframe code based on target db
            code_txt += "try:\n\tdb_tables_dict = {"
            for key,val in self.cnv_ds.table_df_map.items():
                code_txt += f"'{key}':'{val}',\n\t"
            code_txt += '}\n'    
            #read dataframe based on target db
            if self.cntx.target_db == 'redshift':
                code_txt += "\n\tfor tab,df in db_tables_dict.items():\n"
                code_txt += "\t"*2 + "schema,table = tab.split('.')\n"
                code_txt += "\t"*2 + "dym__df = glueContext.create_dynamic_frame.from_catalog(database=cat_db, table_name=rs_db+'_'+schema+'_'+table,redshift_tmp_dir=temp_dir)\n"
                code_txt += "\t"*2 + "org_df[df] = dym__df.toDF()\n"
                code_txt += "\t"*2 + "mod_df[df] = dym__df.toDF()\n"
                code_txt += "\t"*2 + "mod_df[df].createOrReplaceTempView(df)\n"
                code_txt += "except:\n\traise\n\t#quit()\n"
            #add code to output
            self.output.addCode(code_txt, '')
            self.cnv_ds.cd_idx += 1         
        
    #adds code to create initial dataframes based on execution platform and target database
    def addPlatformWrite(self):
        code_txt = ''
        if self.cntx.out_code_type == 'pyspark':
            if self.cntx.platform == 'glue':
                if self.cntx.target_db == 'redshift':
                    if self.cnv_ds.trnc_tables:
                        code_txt += "#truncate table df list\n"
                        code_txt += "truncate_dfs = [" + self.cnv_ds.trnc_tables.strip()[:-1] + "]\n\n"
                    code_txt += "#sql to get PK columns of Redshift table\n"
                    
                    get_pk_sql = '''"""select
                    f.attname as column_name
                    from pg_attribute f
                    join pg_class c on c.oid = f.attrelid
                    join pg_type t on t.oid = f.atttypid
                    left join pg_attrdef d on d.adrelid = c.oid and d.adnum = f.attnum
                    left join pg_namespace n on n.oid = c.relnamespace
                    left join pg_constraint p on p.conrelid = c.oid and f.attnum = any (p.conkey)
                    left join pg_class as g on p.confrelid = g.oid
                    where c.relkind = 'r'::char
                    and n.nspname = lower('#schema#')
                    and c.relname = lower('#table#')
                    and p.contype = 'p'
                    and f.attnum > 0;"""'''
                    get_pk_sql = re.sub(r'^[ \t]+', '', get_pk_sql, flags=re.M)
                    code_txt += f'''pk_sql = {get_pk_sql}\n\n'''

                    #pyspark code to write datafreames to target database
                    code_txt += "#Capture changes in target tables and modify them accordingly\n";
                    code_txt += "try:\n";                    
                    #create conntion to database
                    code_txt += "\t#Create connection to database\n"
                    code_txt += "\tconn = pg8000.connect(host=hostname, port=port, database=rs_db, user=username, password=password)\n"
                    code_txt += "\tcur = conn.cursor()\n\n"
                    #check each dataframe
                    code_txt += "\tfor tab_df in mod_df.keys():\n"
                    code_txt += "\t"*2 + "if mod_df[tab_df] == org_df[tab_df]:\n"
                    code_txt += "\t"*3 + "continue\n\n"
                    code_txt += "\t"*2 + "df = tab_df.replace('__df','')\n"
                    code_txt += "\t"*2 + "df_spt = df.split('__')\n"
                    code_txt += "\t"*2 + "if len(df_spt) == 2:\n"
                    code_txt += "\t"*3 + "schema = df_spt[0]\n"
                    code_txt += "\t"*3 + "table = df_spt[1]\n"
                    code_txt += "\t"*2 + "else:\n"
                    code_txt += "\t"*3 + "continue\n\n"
                    #derive records to be inserted into target table
                    code_txt += "\t"*2 + "#Derive records to be inserted into target table\n"
                    code_txt += "\t"*2 + "insert__df = mod_df[tab_df].subtract(org_df[tab_df])\n\n"
                    #check if table is truncated
                    code_txt += "\t"*2 + "#check if table is truncated\n"
                    code_txt += "\t"*2 + "if tab_df in truncate_dfs:\n"
                    code_txt += "\t"*3 + "insert__df.cache()\n"	
                    code_txt += "\t"*3 + "insert__df.count()\n"
                    code_txt += "\t"*3 + "cur.execute('truncate table '+schema+'.'+table+';')\n"
                    #derive dataframe for records to be deleted from target table
                    code_txt += "\t"*2 + "else:\n"
                    code_txt += "\t"*3 + "#Derive dataframe for records to be deleted from target table\n"
                    code_txt += "\t"*3 + "delete__df = org_df[tab_df].subtract(mod_df[tab_df])\n\n"
                    code_txt += "\t"*3 + "#Check if delete data frame is empty\n"
                    code_txt += "\t"*3 + "if not(delete__df.rdd.isEmpty()):\n"
                    code_txt += "\t"*4 + "#Cache records to be inserted into target table\n"
                    code_txt += "\t"*4 + "insert__df.cache()\n"
                    code_txt += "\t"*4 + "insert__df.count()\n\n"
                    #Get PK columns of target table
                    code_txt += "\t"*4 + "#Get PK columns of target table\n"
                    code_txt += "\t"*4 + "itr_pk_sql = pk_sql.replace('#schema#',schema).replace('#table#',table)\n"
                    code_txt += "\t"*4 + "cur.execute(itr_pk_sql)\n"
                    code_txt += "\t"*4 + "pk_col_str = ','.join(cur.fetchall()[0])\n"
                    code_txt += "\t"*4 + "if(pk_col_str):\n"
                    code_txt += "\t"*5 + "#Select PK columns from delete record dataframe\n"
                    code_txt += "\t"*5 + "delete__df.createOrReplaceTempView('del__'+tab_df)\n"
                    code_txt += "\t"*5 + "delete__df = spark.sql('select '+pk_col_str+' from del__'+tab_df)\n"
                    code_txt += "\t"*4 + "else:\n"
                    code_txt += "\t"*5 + "pk_col_str = ','.join(delete__df.columns)\n\n"
                    #create temporary table to store PK columns of deleted rows
                    code_txt += "\t"*4 + "#Create temporary table\n"
                    code_txt += "\t"*4 + "cur.execute('create table '+table+'_tmp as select '+pk_col_str+' from '+schema+'.'+table+' where 1 = 2;')\n"
                    code_txt += "\t"*4 + "conn.commit()\n\n"
                    #write rows to be deleted in temp table
                    code_txt += "\t"*4 + "#Write PK columns of deleted records in temporary table\n";
                    code_txt += "\t"*4 + "dym__delete__df = DynamicFrame.fromDF(delete__df,glueContext,'dym__delete__df')\n"
                    code_txt += "\t"*4 + "glueContext.write_dynamic_frame.from_jdbc_conf(frame = dym__delete__df, catalog_connection = cat_conn, \\\n"
                    code_txt += "\t"*4 + "connection_options = {'dbtable':table+'_tmp', 'database':rs_db, 'user':username, 'password':password}, \\\n"
                    code_txt += "\t"*4 + "redshift_tmp_dir = temp_dir)\n\n";
                    #execute delete statemant to delete records from target table
                    code_txt += "\t"*4 + "#Delete records from target tabl\n"
                    code_txt += "\t"*4 + "delete_sql = 'delete from '+schema+'.'+table+' where('+pk_col_str+') in(select '+pk_col_str+' from '+table+'_tmp);'\n"
                    code_txt += "\t"*4 + "cur.execute(delete_sql)\n"
                    code_txt += "\t"*4 + "conn.commit()\n\n"
                    #delete temp table and close connection
                    code_txt += "\t"*4 + "#Delete temp table\n"
                    code_txt += "\t"*4 + "cur.execute('drop table '+table+'_tmp;')\n"
                    code_txt += "\t"*4 + "conn.commit()\n\n"
                    #inset records in target table
                    code_txt += "\t"*2 + "#Insert new/updated records in target table\n"
                    code_txt += "\t"*2 + "if not(insert__df.rdd.isEmpty()):\n"
                    code_txt += "\t"*3 + "dym__insert__df = DynamicFrame.fromDF(insert__df,glueContext,'dym__insert__df')\n"
                    code_txt += "\t"*3 + "glueContext.write_dynamic_frame.from_jdbc_conf(frame = dym__insert__df, catalog_connection = cat_conn, \\\n"
                    code_txt += "\t"*3 + "connection_options = {'dbtable':schema+'.'+table, 'database':rs_db, 'user':username, 'password':password}, \\\n"
                    code_txt += "\t"*3 + "redshift_tmp_dir = temp_dir)\n\n"
                    #close connection
                    code_txt += "\tcur.close()\n"
                    code_txt += "except:\n\traise\n\t#quit()\n"
        #add code to output
        self.output.addCode(code_txt, '')
        self.cnv_ds.cd_idx += 1
            
    #starts conversion process for given context
    def convert(self):
        #scan input script
        self.scanScript()
        
        self.addPlatformRead()
        
        for stmt in self.input.statements:
            cnv_code, cnv_log = stmt.convStatement(self.cnv_ds)
            if not(cnv_code == None and cnv_log == None):
                self.output.addCode(cnv_code, cnv_log)
                self.cnv_ds.cd_idx += 1
                
        self.addPlatformWrite()
        
        return self.output
        