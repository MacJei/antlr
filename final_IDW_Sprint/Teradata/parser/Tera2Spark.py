import re

def convert(tknList = [], spsList = []):
    
    td_fnctn_map = {
        #Change Functions
        'EXTRACT'               : EXTRACT,
        'RANDOM'                : RANDOM,
        'TO_CHAR'               : TO_CHAR,
        'ZEROIFNULL'            : ZEROIFNULL,
        'NULLIFZERO'            : NULLIFZERO,
        'CURRENT_TIMESTAMP'     : CURRENT_TIMESTAMP,
        'SUBSTR'                : SUBSTR,
        'SUBSTRING'             : SUBSTR,
        'CHARACTERS'            : CHAR_LENGTH,
        'CHARS'                 : CHAR_LENGTH,
        'CHAR'                  : CHAR_LENGTH,
        'CHARACTER_LENGTH'      : CHAR_LENGTH,
        'CHAR_LENGTH'           : CHAR_LENGTH,
        'INTERVAL'              : INTERVAL,
        'CAST'                  : CAST,
        'TRIM'                  : TRIM,
        'LTRIM'                 : LTRIM,
        'RTRIM'                 : RTRIM,        
        'TO_DATE'               : TO_DATE,
        'TO_TIMESTAMP'          : TO_TIMESTAMP,
        'SKEW'                  : SKEW,
        'LOG'                   : LOG,
        'DATABASE'              : DATABASE,
        'DATE'                  : DATE,
        'TO_NUMBER'             : TO_NUMBER,
        'ADD_MONTHS'            : ADD_MONTHS,
        'OADD_MONTHS'           : ADD_MONTHS,
        'INDEX'                 : INDEX,
        'OREPLACE'              : OREPLACE,
        'OTRANSLATE'            : OTRANSLATE,
        'INSTR'                 : INSTR,
        'TD_DAY_OF_WEEK'        : TD_DAY_OF_WEEK,
        'TD_DAY_OF_MONTH'       : TD_DAY_OF_MONTH,
        'TD_DAY_OF_YEAR'        : TD_DAY_OF_YEAR,
        'TD_WEEK_OF_YEAR'       : TD_WEEK_OF_YEAR,
        'TD_MONTH_OF_QUARTER'   : TD_MONTH_OF_QUARTER,
        'TD_MONTH_OF_YEAR'      : TD_MONTH_OF_YEAR,
        'TD_QUARTER_OF_YEAR'    : TD_QUARTER_OF_YEAR,
        'TO_CHAR'               : TO_CHAR,
        'BITAND'                : BITAND,
        'BITNOT'                : BITNOT,
        'BITOR'                 : BITOR,
        'BITXOR'                : BITXOR,
        'ATAN2'                 : ATAN2,
        'TRUNC'                 : TRUNC,
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
    if td_fnctn_map.get(tknList[0].upper()) is None:
        for i in range(len(tknList)):
            out_str += spsList[i] + tknList[i]
        return out_str
    else:
        fnctn = td_fnctn_map[tknList[0].upper()]
        if fnctn == 'SAME':
            for i in range(len(tknList)):
                out_str += spsList[i] + tknList[i]
            return out_str
        else:
            out_str = fnctn(tknList, spsList)
            if out_str == 'Error':
                for i in range(len(tknList)):
                    out_str += spsList[i] + tknList[i]
                return out_str
            else:
                lpad = spsList[0]
                return lpad + out_str
 

def alterTknList(tknList):
    alt_tknList = []
    for tkn in tknList:
        if tkn.upper() in ['CASE','WHEN','THEN','ELSE','END']:
            alt_tkn = ' ' + tkn + ' '
            alt_tknList.append(alt_tkn)
        else:
            alt_tknList.append(tkn)
    return alt_tknList
 
def CAST(tknList, spsList):
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
                out_str = 'to_date(' + cast_expr + ')'

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

def INTERVAL(tknList, spsList):
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

def TO_DATE(tknList, spsList):
    dt_exp = ''
    cm_idx = tknList.index(',')

    for i in range(2,cm_idx):
        dt_exp += spsList[i] + tknList[i]

    dt_fmt = tknList[cm_idx + 1]

    dt_fmt = changeDateFormat(dt_fmt)
    return 'to_date(' + dt_exp  + ',' + dt_fmt + ')'

def TO_TIMESTAMP(tknList, spsList):
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

def CHAR_LENGTH(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'char_length(' + expr + ')'

def SKEW(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'skewness(' + expr + ')'

def LOG(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'log10(' + expr + ')'

def DATABASE(tknList, spsList):
    return 'current_dateabse()'

def DATE(tknList, spsList):
    if len(tknList[2:-1]) > 0:
        return 'Error'
    else:
        return 'current_date()'

def TO_NUMBER(tknList, spsList):
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

def ADD_MONTHS(tknList, spsList):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]

    return 'add_months(' + l_expr + ',' + r_expr + ')'

def INDEX(tknList, spsList):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]

    return 'instr(' + l_expr + ',' + r_expr + ')'

def INSTR(tknList, spsList):
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

def OREPLACE(tknList, spsList):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'replace(' + expr + ')'

def OTRANSLATE(tknList, spsList):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'translate(' + expr + ')'

def TRIM(tknList, spsList):
    if tknList[2].upper() in ['BOTH', 'LEADING', 'TRAILING'] and tknList[3].upper() == 'FROM':
        tknList[2] += r" ' \t\r\n'"
        
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'trim(' + expr + ')'
    
def LTRIM(tknList, spsList):
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

def RTRIM(tknList, spsList):
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

def EXTRACT(tknList, spsList):
    extr_fn = tknList[2].lower()
    extr_from = ''
    for i in range(4,len(tknList) - 1):
        extr_from += spsList[i] + tknList[i]

    return extr_fn + '(' + extr_from + ')'

def RANDOM(tknList, spsList):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]

    return 'cast(rand()*(' + l_expr + '-' + r_expr + ')+' + r_expr + ' as integer)'   

def TRUNC(tknList, spsList):    
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
            
    
def TO_CHAR(tknList, spsList):
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

def ZEROIFNULL(tknList, spsList):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'nvl(' + expr + ', 0)'

def NULLIFZERO(tknList, spsList):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return 'nullif(' + expr + ', 0)'

def CURRENT_TIMESTAMP(tknList, spsList):
    return 'current_timestamp()'

def SUBSTR(tknList, spsList):
    out_str = ''
    for i in range(len(tknList)):
        if tknList[i].upper() in ('FROM','FOR'):
            out_str += ','
        else:
            out_str += tknList[i]
    return out_str

def BITAND(tknList, spsList):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]
        
    return '(' + l_expr + ' & ' + r_expr + ')'    

def BITNOT(tknList, spsList):
    expr = ''
    for i in range(2,len(tknList) - 1):
        expr += spsList[i] + tknList[i]

    return '(~' + expr + ')'

def BITOR(tknList, spsList):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]
        
    return '(' + l_expr + ' | ' + r_expr + ')' 

def BITXOR(tknList, spsList):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i]

    return '((' + l_expr + '&(~' + r_expr + '))|((~' + l_expr + ')&' + r_expr + '))'

def ATAN2(tknList, spsList):
    cm_idx = tknList.index(',')

    l_expr = ''
    for i in range(2,cm_idx):
        l_expr += spsList[i] + tknList[i]

    r_expr = ''
    for i in range(cm_idx+1, len(tknList) - 1):
        r_expr += spsList[i] + tknList[i] 
        
    return 'atan2(' + r_expr + ',' + l_expr + ')'
    
def TD_DAY_OF_WEEK(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'dayofweek(' + expr + ')'
     
def TD_DAY_OF_MONTH(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'dayofmonth(' + expr + ')'
    
def TD_DAY_OF_YEAR(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'dayofyear(' + expr + ')'
     
def TD_WEEK_OF_YEAR(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'weekofyear(' + expr + ')'
    
def TD_MONTH_OF_QUARTER(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'mod(month(' + expr + '), 3)'   

def TD_MONTH_OF_YEAR(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'month(' + expr + ')'
   
def TD_QUARTER_OF_YEAR(tknList, spsList):
    expr = ''
    for i in range(2, len(tknList) - 1):
        expr = spsList[i] + tknList[i]
    return 'quarter(' + expr + ')'   
    
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