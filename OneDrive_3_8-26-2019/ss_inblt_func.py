from datetime import datetime
from dateutil.parser import parse


#LTRIM ( character_expression ) 
def ltrim (expr):
    return expr.lstrip()

#RTRIM ( character_expression )    
def rtrim(expr):
    return expr.rstrip()

#CURRENT_TIMESTAMP      
def current_timestamp():
    getdate()

#GETDATE()    
def getdate():
    return str(datetime.now())

#COALESCE ( expression [ ,...n ] )
def coalesce(*exprs)
    for expr in exprs:
        if expr != None:
            return expr

#ISDATE ( expression )            
def isdate(expr):
    try:
        parse(expr)
        isdate = True
    except ValueError:
        isdate = False
    return isdate    

#SUBSTRING ( expression ,start , length ) 
def substring(expr, st, ln):
    st -= 1
    return expr[st:st+ln]
    
#YEAR ( date )
def year(dt):
    if dt == 0:
        dt = '1900-01-01 00:00:00.000000'
    py_dt = parse(dt)
    return datetime(py_dt, '%Y')
    
#MONTH ( date )   
def month(dt):
    if dt == 0:
        dt = '1900-01-01 00:00:00.000000'
    py_dt = parse(dt)
    return datetime(py_dt, '%m')
    
#ROUND ( numeric_expression , length [ ,function ] )
#same symtax in python

#ISNULL ( check_expression , replacement_value )  
def isnull(expr, rval):
    if expr is None:
        return rval
    else:
        expr
        
#LEFT ( character_expression , integer_expression )
def left(expr, ln):
    return expr[ln:]

#RIGHT ( character_expression , integer_expression )      
def right():
    return expr[:-ln]


#OBJECT_ID('tempdb..#[object name]')
def object_id(str):
    if 'tempdb..#' in str:
        return str.replace('tempdb..', '')
    else:
        return None
        

#TODO
#dateadd
#datediff
