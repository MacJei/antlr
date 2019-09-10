import re

def convert(tknList = [], spsList = [], logger = None):
    
    td_fnctn_map = {
        #Change Functions
        'CAST': CAST,
        'CONVERT': CONVERT,
        'CURRENT_TIMESTAMP': CURRENT_TIMESTAMP,
        'DATEADD': DATEADD,
        'DATEDIFF': DATEDIFF,
        'GETDATE': GETDATE,
        'ISNULL': ISNULL,
        'LTRIM': LTRIM,
        'RTRIM': RTRIM,
        'TRIM': TRIM,
        'SUBSTRING': SUBSTRING,
        'FORMAT': FORMAT, #Blanket error
        'PRINT': 'SAME', #Not a Function, but a statement
        'ISDATE': ISDATE, #Blanket error
        'YEAR': 'SAME',
        'MONTH': 'SAME',
        'DATE': DATE,
        #No Change Functions
        'AVG'                   : 'SAME',
        'CORR'                  : 'SAME',
        'COUNT'                 : 'SAME',
        'COVAR_POP'             : 'SAME',
        'COVAR_SAMP'            : 'SAME',
        'GROUPING'              : 'SAME',
        'KURTOSIS'              : 'SAME',
        'MAXIMUM'               : 'SAME',
        'MAX'                   : 'SAME',
        'MINIMUM'               : 'SAME',
        'MIN'                   : 'SAME',
        'STDDEV_POP'            : 'SAME',
        'STDDEV_SAMP'           : 'SAME',
        'SUM'                   : 'SAME',
        'VAR_POP'               : 'SAME',
        'VAR_SAMP'              : 'SAME',
        'ABS'                   : 'SAME',
        'CEILING'               : 'SAME',
        'CEIL'                  : 'SAME',
        'EXP'                   : 'SAME',
        'FLOOR'                 : 'SAME',
        'LN'                    : 'SAME',
        'POWER'                 : 'SAME',
        'ROUND'                 : 'SAME',
        'SIGN'                  : 'SAME',
        'SQRT'                  : 'SAME',
        'COS'                   : 'SAME',
        'SIN'                   : 'SAME',
        'TAN'                   : 'SAME',
        'ACOS'                  : 'SAME',
        'ASIN'                  : 'SAME',
        'ATAN'                  : 'SAME',
        'DEGREES'               : 'SAME',
        'RADIANS'               : 'SAME',
        'COSH'                  : 'SAME',
        'SINH'                  : 'SAME',
        'TANH'                  : 'SAME',
        'MOD'                   : 'SAME',
        'COALESCE'              : 'SAME',
        'OCTET_LENGTH'          : 'SAME',
        'SHIFTLEFT'             : 'SAME',
        'SHIFTRIGHT'            : 'SAME',
        'CURRENT_DATE'          : 'SAME',
        'GREATEST'              : 'SAME',
        'LEAST'                 : 'SAME',
        'LAST_DAY'              : 'SAME',
        'NEXT_DAY'              : 'SAME',
        'MONTHS_BETWEEN'        : 'SAME',
        'CUME_DIST'             : 'SAME',
        'DENSE_RANK'            : 'SAME',
        'FIRST_VALUE'           : 'SAME',
        'LAST_VALUE'            : 'SAME',
        'PERCENT_RANK'          : 'SAME',
        'RANK'                  : 'SAME',
        'ROW_NUMBER'            : 'SAME',
        'ASCII'                 : 'SAME',
        'CHR'                   : 'SAME',
        'INITCAP'               : 'SAME',
        'LENGTH'                : 'SAME',
        'LEFT'                  : 'SAME',
        'LOWER'                 : 'SAME',
        'LPAD'                  : 'SAME',
        'POSITION'              : 'SAME',
        'REVERSE'               : 'SAME',
        'RPAD'                  : 'SAME',
        'RIGHT'                 : 'SAME',
        'SOUNDEX'               : 'SAME',
        'UPPER'                 : 'SAME',
        'NULLIF'                : 'SAME'
    }

    #[TEMP_FIX] to add spaces in CASE statement 
    tknList = alterTknList(tknList)
    
    out_str = ''
    if td_fnctn_map.get(tknList[0].upper()) is None:  #####Out of scope - function call
        for i in range(len(tknList)):
            out_str += spsList[i] + tknList[i]
        #Log WARN for out of scope functions
        logger.add_log('WARN',tknList[0] + ' Function is not covered in scope and has been kept AS IS in target code. Consider manual reconstruction if necessary.')
        logger.add_log_details(out_str.strip())
        return out_str
    else:
        fnctn = td_fnctn_map[tknList[0].upper()]
        if fnctn == 'SAME':                            #####AS IS in target - function call
            for i in range(len(tknList)):
                out_str += spsList[i] + tknList[i]
            # Log INFO for AS IS functions
            logger.add_log('INFO', tknList[0] + ' Function call is same in Target language, hence not converted.')
            logger.add_log_details(out_str.strip())
            return out_str
        else:                                           #####Requires conversion - function call
            ret_result = fnctn(tknList, spsList, logger)
            if ret_result[0:5].lower() == 'error':
                logger.add_log('ERROR',tknList[0] + ' Function could not be converted. Consider manual reconstruction.' + ret_result[6:])
                for i in range(len(tknList)):
                    out_str += spsList[i] + tknList[i]
                logger.add_log_details(out_str.strip())
                return out_str
            else:
                lpad = spsList[0]
                return lpad + ret_result
 

def alterTknList(tknList):
    alt_tknList = []
    for tkn in tknList:
        if tkn.upper() in ['CASE','WHEN','THEN','ELSE','END']:
            alt_tkn = ' ' + tkn + ' '
            alt_tknList.append(alt_tkn)
        else:
            alt_tknList.append(tkn)
    return alt_tknList


def CAST(tknList, spsList, logger):
    tknList_upr = [tkn.upper() for tkn in tknList]

    idx_as = tknList_upr.index('AS')
    cast_expr = ''.join(tknList[2:idx_as])

    idx_typ = idx_as + 1;
    cast_type = tknList[idx_typ]

    if cast_type.upper() not in ['FORMAT', 'TIMESTAMP', 'DECIMAL', 'VARCHAR', 'CHAR', 'INT', 'INTEGER', 'DATE', 'BIGINT', 'FLOAT']:
        return 'Error'

    if cast_type.upper() == 'FORMAT':
        cast_fmt = tknList[idx_typ + 1]
        cast_type = ''
    else:
        cast_type_len = ''
        if cast_type.upper() in ['TIMESTAMP', 'DECIMAL', 'VARCHAR', 'CHAR'] and tknList[idx_typ+1] == '(':
            idx_typ_end = tknList_upr.index(')', idx_typ + 1)
            cast_type_len = ''.join(tknList[idx_typ+1 : idx_typ_end+1])

        if cast_type.upper() != 'TIMESTAMP':
            cast_type = cast_type + cast_type_len

        try:
            idx_fmt = tknList_upr.index('FORMAT')
            cast_fmt = tknList[idx_fmt + 1]
        except ValueError:
            cast_fmt = ''

    if cast_fmt:
        cast_fmt = changeDateFormat(cast_fmt)

    if cast_type:
        if cast_type.upper() == 'DATE':
            if cast_fmt:
                out_str = 'to_date(' + cast_expr  + ',' + cast_fmt + ')'
            else:
                cast_expr = re.sub(r"(\d+)[-/'.](\d+)[-/'.](\d+)", r'\1-\2-\3', cast_expr)
                out_str = 'to_date(' + cast_expr + ',\'MM/dd/yyyy\')'
                logger.add_log('WARN','Using default date format \'MM/dd/yyyy\' as it is not specified. Consider rewriting function if necessary.')
                logger.add_log_details(out_str)

        elif cast_type.upper() == 'TIMESTAMP':
            if cast_fmt:
                out_str = 'to_timestamp(' + cast_expr  + ',' + cast_fmt + ')'
            else:
                cast_expr = re.sub(r"(\d+)[-/'.](\d+)[-/'.](\d+) (\d*)[:.-]?(\d*)[:.-]?(\d*)([.]?\d*)", r'\1-\2-\3 \4:\5:\6\7', cast_expr)
                out_str = 'to_timestamp(' + cast_expr + ')'

        else:
            out_str = 'cast(' + cast_expr  + ' as ' + cast_type + ')'
    else:
        if cast_fmt:
            out_str = 'date_format(' + cast_expr  + ',' + cast_fmt + ')'
        else:
            out_str = 'Error'
    return out_str


def CONVERT(tknList, spsList, logger):
    #print(tknList)
    x = newSplitArr(tknList[2:-1],',')
    if len(x) > 2:
        return 'Error:Format String not supported'
    tknList[2:-1] = x[1] + ['AS'] + x[0]
    return CAST(tknList,spsList,logger)


def ISNULL(tknList, spsList, logger):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'IFNULL(' + expr + ')'

def FORMAT(tknList, spsList, logger):
    #print('FORMAT function is not supported in Saprk SQL. Need reconstructing')
    return 'Error'

def ISDATE(tknList, spsList, logger):
    return 'Error'

def DATEADD(tknList, spsList, logger):
    datepart_conv_dict = {
        'year': ['year($2) - year($1)', 'add_months($2 , $1 *12)'],
        'yy': ['year($2) - year($1)', 'add_months($2 , $1 *12)'],
        'yyyy': ['year($2) - year($1)', 'add_months($2 , $1 *12)'],
        'quarter': ['quarter($2) - quarter($1)', 'add_months($2 , $1 *3)'],
        'q': ['quarter($2) - quarter($1)', 'add_months($2 , $1 *3)'],
        'qq': ['quarter($2) - quarter($1)', 'add_months($2 , $1 *3)'],
        'month': ['months_between($2 ,  $1)', 'add_months($2 , $1)'],
        'mm': ['months_between($2 ,  $1)', 'add_months($2 , $1)'],
        'm': ['months_between($2 ,  $1)', 'add_months($2 , $1)'],
        'dayofyear': ['dayofyear($2) - dayofyear($1)', 'date_add($2 , $1)'],
        'dy': ['dayofyear($2) - dayofyear($1)', 'date_add($2 , $1)'],
        'y': ['dayofyear($2) - dayofyear($1)', 'date_add($2 , $1)'],
        'day': ['datediff($2 ,  $1)', 'date_add($2 , $1)'],
        'dd': ['datediff($2 ,  $1)', 'date_add($2 , $1)'],
        'd': ['datediff($2 ,  $1)', 'date_add($2 , $1)'],
        'week': ['weekofyear($2) - weekofyear($1)', 'date_add($2 , $1 *7)'],
        'wk': ['weekofyear($2) - weekofyear($1)', 'date_add($2 , $1 *7)'],
        'ww': ['weekofyear($2) - weekofyear($1)', 'date_add($2 , $1 *7)'],
        'weekday': ['not supported', 'date_add($2 , $1)'],
        'dw': ['not supported', 'date_add($2 , $1)'],
        'w': ['not supported', 'date_add($2 , $1)'],
        'hour': ['hour($2) - hour($1)', 'not supported'],
        'hh': ['hour($2) - hour($1)', 'not supported'],
        'minute': ['minute($2) - minute($1)', 'not supported'],
        'mi': ['minute($2) - minute($1)', 'not supported'],
        'n': ['minute($2) - minute($1)', 'not supported'],
        'second': ['second($2) - second($1)', 'from_unixtime(to_unix_timestamp($2) + ($1))'],
        'ss': ['second($2) - second($1)', 'from_unixtime(to_unix_timestamp($2) + ($1))'],
        's': ['second($2) - second($1)', 'from_unixtime(to_unix_timestamp($2) + ($1))'],
        'millisecond': ['not supported', 'not supported'],
        'ms': ['not supported', 'not supported'],
        'microsecond': ['not supported', 'not supported'],
        'mcs': ['not supported', 'not supported'],
        'nanosecond': ['not supported', 'not supported'],
        'ns': ['not supported', 'not supported']
    }
    x = newSplitArr(tknList[2:-1], ',')
    if len(x) > 3:
        return 'Error: >3 parameters not supported.'
    if tknList[0].lower() == 'datediff':
        datepart, startdate, enddate = x[0][0], ''.join(x[1]), ''.join(x[2])
        enddate = 'to_date(\'1900-01-01\')' if enddate == '0' else enddate
        startdate = 'to_date(\'1900-01-01\')' if startdate == '0' else startdate
        op = datepart_conv_dict.get(datepart.lower())[0].replace('$1', startdate).replace('$2', enddate)
        if op == 'not supported':
            return 'Error:Datepart \'' + datepart + '\' not supported'
        else:
            return op
    if tknList[0].lower() == 'dateadd':
        datepart, delta, date_seed = x[0][0], ''.join(x[1]), ''.join(x[2])
        date_seed = 'to_date(\'1900-01-01\')' if date_seed == '0' else date_seed
        op = datepart_conv_dict.get(datepart.lower())[1].replace('$1', delta).replace('$2', date_seed)
        if op == 'not supported':
            return 'Error:Datepart \'' + datepart + '\' not supported'
        else:
            return op


def DATEDIFF(tknList, spsList, logger):
    #Redirect to DATEADD function. They are handled together.
    return DATEADD(tknList,spsList,logger)

def INTERVAL(tknList, spsList, logger):
    if tknList[2].lower() == 'year' and tknList[3] in ['+','-']:
        if tknList[3] == '-':
            return "add_months({}, {}{}*12)".format(tknList[4],tknList[3],tknList[1].strip("'"))
        else:
            return "add_months({}, {}*12)".format(tknList[4],tknList[1].strip("'"))

    elif tknList[2].lower() == 'month' and tknList[3] in ['+','-']:
        if tknList[3] == '-':
            return "add_months({}, {}{})".format(tknList[4],tknList[3],tknList[1].strip("'"))
        else:
            return "add_months({}, {})".format(tknList[4],tknList[1].strip("'"))

    elif tknList[2].lower() == 'day' and tknList[3] in ['+','-']:
        if tknList[3] == '-':
            return "date_add({}, {}{})".format(tknList[4],tknList[3],tknList[1].strip("'"))
        else:
            return "date_add({}, {})".format(tknList[4],tknList[1].strip("'"))

    elif tknList[2].lower() == 'hour' and tknList[3] in ['+','-']:
        return "from_unixtime(to_unix_timestamp({}){}{}*60*60)".format(tknList[4],tknList[3],tknList[1].strip("'"))

    elif tknList[2].lower() == 'minute' and tknList[3] in ['+','-']:
        return "from_unixtime(to_unix_timestamp({}){}{}*60)".format(tknList[4],tknList[3],tknList[1].strip("'"))

    elif tknList[2].lower() == 'second' and tknList[3] in ['+','-']:
            return "from_unixtime(to_unix_timestamp({}){}{})".format(tknList[4],tknList[3],tknList[1].strip("'"))

    else:
        return 'Error'


def TO_DATE(tknList, spsList, logger):
    dt_exp = ''
    cm_idx = tknList.index(',')

    for i in range(2,cm_idx):
        dt_exp += spsList[i] + tknList[i]

    dt_fmt = tknList[cm_idx + 1]

    dt_fmt = changeDateFormat(dt_fmt)
    return 'to_date(' + dt_exp  + ',' + dt_fmt + ')'


def TO_TIMESTAMP(tknList, spsList, logger):
    try:
        cm_idx = tknList.index(',')
    except ValueError:
        cm_idx = 0

    if cm_idx:
        ts_exp = ''
        for i in range(2,cm_idx):
            ts_exp += spsList[i] + tknList[i]

        ts_fmt = tknList[cm_idx + 1]
        ts_fmt = changeDateFormat(ts_fmt)
        return 'to_timestamp(' + ts_exp  + ',' + ts_fmt + ')'
    else:
        ts_exp = ''
        for i in range(2,len(tknList) - 1):
            ts_exp += spsList[i] + tknList[i]

        return 'from_unixtime('+ ts_exp + ')'

def CHAR_LENGTH(tknList, spsList, logger):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'char_length(' + expr + ')'

def SKEW(tknList, spsList, logger):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'skewness(' + expr + ')'

def LOG(tknList, spsList, logger):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'log10(' + expr + ')'

def DATABASE(tknList, spsList, logger):
    return 'current_dateabse()'

def DATE(tknList, spsList, logger):
    if len(tknList[2:-1]) > 0:
        return 'Error'
    else:
        return 'current_date()'

def TO_NUMBER(tknList, spsList, logger):
    try:
        cm_idx = tknList.index(',')
    except ValueError:
        cm_idx = 0

    if cm_idx:
        return 'Error'
    else:
        expr = ''
        for i in range(2,cm_idx):
            expr += spsList[i] + tknList[i]

        return 'cast(' + expr + ' as float)'

def ADD_MONTHS(tknList, spsList, logger):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]

    return 'add_months(' + l_expr + ',' + r_expr + ')'

def INDEX(tknList, spsList, logger):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]

    return 'instr(' + l_expr + ',' + r_expr + ')'

def INSTR(tknList, spsList, logger):
    expr = ''
    prv_idx = 2
    dlm_cnt = tknList.count(',')
    
    if dlm_cnt == 3:
        return 'Error'

    for i in range(dlm_cnt):
        nxt_idx = tknList.index(',', prv_idx)
        for j in range(prv_idx, nxt_idx):
            expr += spsList[j] + tknList[j]
        expr += ','
        prv_idx = nxt_idx + 1
    expr = expr[:-2]

    return 'position(' + expr + ')'

def OREPLACE(tknList, spsList, logger):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'replace(' + expr + ')'

def OTRANSLATE(tknList, spsList, logger):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'translate(' + expr + ')'

def TRIM(tknList, spsList, logger):
    if tknList[2].upper() in ['BOTH', 'LEADING', 'TRAILING'] and tknList[3].upper() == 'FROM':
        tknList[2] += r" ' \t\r\n'"
        
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'trim(' + expr + ')'
    
def LTRIM(tknList, spsList, logger):
    dlm_cnt = tknList.count(',')
    if dlm_cnt == 0:
        expr = ''
        for i in range(2,len(tknList) - 1):
            expr += spsList[i] + tknList[i]
        return 'ltrim(' + expr + ')'    
    else:
        cm_idx = tknList.index(',')   
        
        l_expr = ''
        for i in range(2,cm_idx):
            l_expr += spsList[i] + tknList[i]
    
        r_expr = ''
        for i in range(cm_idx+1, len(tknList) - 1):
            r_expr += spsList[i] + tknList[i] 

        return 'ltrim(' + r_expr + ',' + l_expr + ')' 

def RTRIM(tknList, spsList, logger):
    dlm_cnt = tknList.count(',')
    
    if dlm_cnt == 0:
        expr = ''
        for i in range(2,len(tknList) - 1):
            expr += spsList[i] + tknList[i]
        return 'ltrim(' + expr + ')'    
    else:
        cm_idx = tknList.index(',')   
        
        l_expr = ''
        for i in range(2,cm_idx):
            l_expr += spsList[i] + tknList[i]
    
        r_expr = ''
        for i in range(cm_idx+1, len(tknList) - 1):
            r_expr += spsList[i] + tknList[i] 

        return 'rtrim(' + r_expr + ',' + l_expr + ')'

def EXTRACT(tknList, spsList, logger):
    extr_fn = tknList[2].lower()
    extr_from = ''
    for i in range(4,len(tknList) - 1):
        extr_from += spsList[i] + tknList[i]

    return extr_fn + '(' + extr_from + ')'

def RANDOM(tknList, spsList, logger):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]

    return 'cast(rand()*(' + l_expr + '-' + r_expr + ')+' + r_expr + ' as integer)'   

def TRUNC(tknList, spsList, logger):    
    dlm_cnt = tknList.count(',')
    if dlm_cnt == 0:
        expr = ''
        for i in range(2,len(tknList) - 1):
            expr += spsList[i] + tknList[i]

        return 'int(' + expr + '* power(10,0))/power(10,0)'
    else:
        cm_idx = tknList.index(',')
    
        l_expr = ''
        for i in range(2,cm_idx):
            l_expr += spsList[i] + tknList[i]
    
        r_expr = ''
        for i in range(cm_idx+1, len(tknList) - 1):
            r_expr += spsList[i] + tknList[i]

        return 'int(' + l_expr + '* power(10,' + r_expr + '))/power(10,' + r_expr + ')'
            
    
def TO_CHAR(tknList, spsList, logger):
    dlm_cnt = tknList.count(',')
    if dlm_cnt == 0:
        expr = ''
        for i in range(2,len(tknList) - 1):
            expr += spsList[i] + tknList[i]

        return 'string(' + expr + ')'
    elif dlm_cnt == 1:
        l_expr = ''
        cm_idx = tknList.index(',')
        for i in range(2,cm_idx):
            l_expr += spsList[i] + tknList[i]    

        r_expr = ''
        for i in range(cm_idx+1, len(tknList) - 1):
            r_expr += spsList[i] + tknList[i]
            
        if '9' in r_expr:    
            r_expr.replace('9','#')
            return 'format_number(' + l_expr + ',' + r_expr + ')' 
        else:
            r_expr = changeDateFormat(r_expr)
            return 'date_format(' + l_expr + ',' + r_expr + ')' 
    else:
        return 'Error'

def ZEROIFNULL(tknList, spsList, logger):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'nvl(' + expr + ', 0)'

def NULLIFZERO(tknList, spsList, logger):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'nullif(' + expr + ', 0)'

def CURRENT_TIMESTAMP(tknList, spsList, logger):
    return 'current_timestamp()'

def GETDATE(tknList, spsList, logger):
    return 'current_timestamp()'

def SUBSTRING(tknList, spsList, logger):
    out_str = ''
    for i in range(len(tknList)):
        if tknList[i].upper() in ('FROM','FOR'):
            out_str += ','
        else:
            out_str += tknList[i]
    return out_str

def BITAND(tknList, spsList, logger):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]
        
    return '(' + l_expr + ' & ' + r_expr + ')'    

def BITNOT(tknList, spsList, logger):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return '(~' + expr + ')'

def BITOR(tknList, spsList, logger):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]
        
    return '(' + l_expr + ' | ' + r_expr + ')' 

def BITXOR(tknList, spsList, logger):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]

    return '((' + l_expr + '&(~' + r_expr + '))|((~' + l_expr + ')&' + r_expr + '))'

def ATAN2(tknList, spsList, logger):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i] 
        
    return 'atan2(' + r_expr + ',' + l_expr + ')'

    
def changeDateFormat(cast_fmt):
    cast_fmt = cast_fmt.lower()

    cast_fmt = cast_fmt.replace('month','[:1:]')
    cast_fmt = cast_fmt.replace('mon','[:2:]')
    cast_fmt = cast_fmt.replace('mi','[:3:]')    

    cast_fmt = cast_fmt.replace('t','a')
    cast_fmt = cast_fmt.replace('h','H')
    cast_fmt = cast_fmt.replace('b',' ')
    cast_fmt = cast_fmt.replace('e','E')
    cast_fmt = cast_fmt.replace('m','M')
    cast_fmt = cast_fmt.replace('[:3:]','mm')

    cast_fmt = cast_fmt.replace('m4','MMMM')
    cast_fmt = cast_fmt.replace('m3','MMM')
    cast_fmt = cast_fmt.replace('[:1:]','MMMM')
    cast_fmt = cast_fmt.replace('[:2:]','MMM')
    cast_fmt = cast_fmt.replace('d3','ddd')
    cast_fmt = cast_fmt.replace('y4','yyyy')
    cast_fmt = cast_fmt.replace('e4','EEEE')
    cast_fmt = cast_fmt.replace('e3','EEE')

    cast_fmt = re.sub(r's\(\d+\)','SSS', cast_fmt)

    return cast_fmt

#GENERIC utility Functions
#custom split function that ignores if given delimeter is within () or ''
def newSplitArr(sql_str, dl):
    in_bracket = 0        #indicates if current caracter is within circular bracket
    in_quote = 0          #indicates if current caracter is within quote
    split_cache = []      #current split element cache
    split_arr = []       #result array

    for ch in sql_str:
        if ch == '(' :      #update in_bracket value if '(' not in quote
            if in_quote == 0:
                in_bracket = in_bracket + 1
            split_cache.append(ch) #add character into split cache
        elif ch == ')':     #update in_bracket value if ')' not in quote
            if in_quote == 0:
                in_bracket = in_bracket - 1
            #add character into split cache
            split_cache.append(ch)
        #update in_quote value based on whether single quote found before
        elif ch == "'":
            if in_quote == 0:
                in_quote = in_quote + 1
            else:
                in_quote = in_quote - 1
            #add character into split cache
            split_cache.append(ch)
        #if given delimeter found
        elif ch == dl:
            #if delimeter not in circular bracket or single quote
            if in_bracket == 0 and in_quote == 0:
                #move split cache content to split array
                split_arr.append(split_cache)
                #reset split cache
                split_cache = []
            else:
                #add character into split cache
                split_cache.append(ch)
        else:
            #add character into split cache
            split_cache.append(ch)
    #add remaining characters in output list
    split_arr.append(split_cache)
    return split_arr
