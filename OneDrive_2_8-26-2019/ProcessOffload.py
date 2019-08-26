import os
import re
import util
from ConvertContext import ConvertContext

def startConversion(cntx):
    for file in os.listdir(cntx.source_dir):
        in_file_path = cntx.source_dir + '\\' + file
        cntx.logger.add_log('INFO', 'Starting conversion for file ' + in_file_path)
        
        if not os.path.isfile(in_file_path):
            cntx.logger.add_log('INFO', in_file_path + ' is not a file. Skipped.')
            continue
       
        try:
            #Teradata
            if  cntx.source_db == 'teradata':
                if cntx.in_code_type == 'bteq':
                    in_script = TeradataScript.BTEQ(cntx, file)
                    cnv = TeradataConvert(cntx, in_script)
                    out_script = cnv.convert()
                    out_script.printCode()
                    
                if cntx.in_code_type == 'sp':
                    pass
                if cntx.in_code_type == 'jnidx':
                    pass

            #TSql
            if  cntx.source_db == 'sqlserver':
                if cntx.in_code_type == 'tsql':
                    in_script = SqlServerScript.TSQL(cntx, file)
                    cnv = SqlServerConvert(cntx, in_script)
                    out_script = cnv.convert()
                    out_script.printCode() 
                    cntx.logger.print_log()

        except Exception as e:
            cntx.logger.add_log('ERROR', 'Unable to process file ' + in_file_path)
            cntx.logger.add_log_details(e.__str__())
            raise

#Main

input = util.parseInputArgs()
cntx = ConvertContext(input)

cntx.logger.add_log('INFO', 'Conversion context :\n' + cntx.showContext())

if cntx.source_db == 'teradata':
    from Teradata import *
    startConversion(cntx)
    
if cntx.source_db == 'sqlserver':
    from SqlServer import *
    startConversion(cntx)
