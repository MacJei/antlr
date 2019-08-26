from optparse import OptionParser
import re

#table_df_map = {}
#union_chklist = []

#
def parseInputArgs():
    usage = """\npython imwProcOffload.py [-s --srcdb] <source db> 
                            [-t --tgtdb] <target db> 
                            [-p --platform] <platform> 
                            [-i --incdtyp] <input code type> 
                            [-o --outcdtyp] <output code type> 
                            [-O --tgtdir] <output dir>
                            [-I --srcdir] <source dir>"""
    
    p = OptionParser(usage)
    
    p.add_option('-s', '--srcdb', action='store', type='string', dest='source_db', help="source script database name (Oracle, Netezza, Teradata)")
    p.add_option('-t', '--tgtdb', action='store', type='string', dest='target_db', help="target database name (Redshift, SQLDW, Snowflake)")
    p.add_option('-p', '--platform', action='store', type='string', dest='platform', help="platforn on output code will run (Glue, EMR, Azure_Databricks ...)")
    p.add_option('-i', '--incdtyp', action='store', type='string', dest='in_code_type', help="source database scripting language ('bteq','tdsp','jnidx','nzsql,'plsql')")
    p.add_option('-o', '--outcdtyp', action='store', type='string', dest='out_code_type', help="scripting language of output script ('python', 'pyspark')")
    p.add_option('-O', '--tgtdir', action='store', type='string', dest='target_dir', help="Loaction where output scripts will be stored")
    p.add_option('-I', '--srcdir', action='store', type='string', dest='source_dir', help="Loaction where input scripts are stored")
                
    options, args = p.parse_args()
    
    if options.source_db == None:
        p.error('Source database is required')
        return -1
    
    if options.target_db == None:
        p.error('Target database is required')  
        return -1
    
    if options.platform == None:
        p.error('Platform is required')
        return -1
    
    if options.in_code_type == None:
        p.error('Input code type is required')
        return -1
    
    if options.out_code_type == None:
        p.error('Output code type is required')   
        return -1
    
    if options.target_dir == None:
        p.error('Output location is required')     
        return -1
    
    if options.source_dir == None:
        p.error('Output location is required')     
        return -1
        
    if len(args) > 0:
        p.error('expecting zero argument, {} given'.format(len(args)))
        return -1    
    
    argv = {'source_db'     : options.source_db.lower(),
            'target_db'     : options.target_db.lower(),
            'platform'      : options.platform.lower(),
            'in_code_type'  : options.in_code_type.lower(),
            'out_code_type' : options.out_code_type.lower(),
            'target_dir'    : options.target_dir,
            'source_dir'    : options.source_dir
        }
    
    return argv
    
    
#custom split function that ignores if given delimeter is within () or ''
def newSplit(sql_str, dl):    
    in_bracket = 0        #indicates if current caracter is within circular bracket
    in_quote = 0          #indicates if current caracter is within quote
    split_cache = ''      #current split element cache
    split_arr = []        #result array

    for ch in sql_str:
        if ch == '(' :      #update in_bracket value if '(' not in quote
            if in_quote == 0:
                in_bracket = in_bracket + 1
            split_cache = split_cache + ch #add character into split cache
        elif ch == ')':     #update in_bracket value if ')' not in quote
            if in_quote == 0:
                in_bracket = in_bracket - 1
            #add character into split cache
            split_cache = split_cache + ch
        #update in_quote value based on whether single quote found before
        elif ch == "'":
            if in_quote == 0:
                in_quote = in_quote + 1
            else:
                in_quote = in_quote - 1
            #add character into split cache
            split_cache = split_cache + ch
        #if given delimeter found
        elif ch == dl:
            #if delimeter not in circular bracket or single quote
            if in_bracket == 0 and in_quote == 0:
                #move split cache content to split array
                split_arr.append(split_cache)
                #reset split cache
                split_cache = ''
            else:
                #add character into split cache
                split_cache = split_cache + ch
        else:
            #add character into split cache
            split_cache = split_cache + ch
    #add remaining characters in output list
    split_arr.append(split_cache)
    return split_arr  


def newLower(in_str):
    qt_str=[]
    #make list substrings with in quotes
    qt_str=re.findall(r'\'(.*?)\'',in_str)
    #replace the quoted substrings with special string
    new_str = re.sub(r'\'(.*?)\'', r'>-o-<', in_str)
    #apply lowercase function on replaced string
    new_str = new_str.lower()
    #replace spaecial string with corresponding quoted substring
    for element in qt_str:
        new_str = re.sub(r'>-o-<', "'" + element + "'", new_str, 1)
    return new_str

    
#subroutine to replace table name with corresponding dataframe name
def replaceTableWithDF(stmt, cnv_ds, flag = 0):
    #replace db table name in select statement with corresponding dataframe name
    for table, df in cnv_ds.table_df_map.items():
        table_re = table.replace(r'[',r'\[').replace('.','\.').replace(']','\]').replace('#','\#')
        if flag == 1:   #flag to check column reference and table alias same as table name
            #if db.table is present in sql
            if re.search(r'{}\b'.format(table_re), stmt, re.S|re.I):
                #get only table name 
                if "." in table:
                    tab = table.split(".")[1]
                else:
                    tab = table

                #check if db.table has alias same as table name
                if re.search(r'\b{}\s*(?:AS)?\s+{}\b'.format(table, tab), stmt, re.S|re.I):
                    #change db.table with dataframe name. alias rename not required.
                    stmt = re.sub(r'\b{}\b'.format(table), df, stmt, flags = re.S|re.I)
                #db.table has either no alias or alias other than table name    
                else:
                    #check if table name to reference column
                    if re.search(r'\b{}\.\w+'.format(tab), stmt, re.S|re.I):
                        #replace db.table and table name in column reference with dataframe name
                        stmt = re.sub(r'\b{}\.'.format(tab), r'{}\.'.format(df), stmt, flags = re.S|re.I)
                        stmt = re.sub(r'\b{}\b'.format(table), df, stmt, flags = re.S|re.I)
                    #table name not used to refer column
                    else:
                        #normal replace db.table with dataframe name
                        stmt = re.sub(r'\b{}\b'.format(table), df, stmt, flags = re.S|re.I)
        else:
            #normal replace db.table with dataframe name
            stmt = re.sub(r'{}'.format(table_re), df + ' ', stmt, flags = re.S|re.I)
            
    return stmt        