from datetime import datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


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
        

def dateadd(abb, arb, dt):
	if dt == 0:
		new_date = datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
	else:
		new_date = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
	if abb == 0:        #year
		mod_date = add_year(new_date, arb)
		return mod_date
	elif abb == 1:  	#quarter 
		mod_date = add_months(new_date, arb*3)
		return mod_date
	elif abb == 2:		#month
		mod_date = add_months(new_date, arb)
		return mod_date
	elif abb == 3:		#dayofyear
		day_of_year = new_date.timetuple().tm_yday
		mod_date = new_date + datetime.timedelta(days = day_of_year)
		return datetime.strftime(mod_date, "%Y-%m-%d %H:%M:%S")
	elif abb == 4:		#days
		mod_date = new_date + datetime.timedelta(days = arb)
		return datetime.strftime(mod_date, "%Y-%m-%d %H:%M:%S")
	elif abb == 5:		#weeks
		mod_date = new_date + datetime.timedelta(weeks = arb)
		return datetime.strftime(mod_date, "%Y-%m-%d %H:%M:%S")
	elif abb == 6:		#weekday
		week_day = new_date.weekday()
		mod_date = new_date + datetime.timedelta(days = week_day)
		return datetime.strftime(mod_date, "%Y-%m-%d %H:%M:%S")
	elif abb == 7:		#hours
		mod_date = new_date + datetime.timedelta(hours = arb)
		return datetime.strftime(mod_date, "%Y-%m-%d %H:%M:%S")
	elif abb == 8:		#minutes
		mod_date = new_date + datetime.timedelta(minutes = arb)
		return datetime.strftime(mod_date, "%Y-%m-%d %H:%M:%S")
	elif abb == 9:		#seconds
		mod_date = new_date + datetime.timedelta(seconds = arb)
		return datetime.strftime(mod_date, "%Y-%m-%d %H:%M:%S")
		

def datediff(abb, s_dt, ed_dt):
	if s_dt == 0:
		st_dt = datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
		end_dt = datetime.strptime(ed_dt, "%Y-%m-%d %H:%M:%S")
	elif ed_dt== 0:
		st_dt = datetime.strptime(s_dt, "%Y-%m-%d %H:%M:%S")
		end_dt = datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
	elif s_dt == 0 and ed_dt == 0:
		return 0
	else:
		st_dt = datetime.strptime(s_dt, "%Y-%m-%d %H:%M:%S")
		end_dt = datetime.strptime(ed_dt, "%Y-%m-%d %H:%M:%S")
	if abb == 0:        #year
		difference_in_years = relativedelta(end_dt, st_dt).years
		return difference_in_years
	elif abb == 1:  	#quarter 
		difference_in_months = dif_months(end_dt, st_dt)
		difference_in_quarters = difference_in_months/3
		return difference_in_quarters
	elif abb == 2:		#month
		difference_in_months = dif_months(end_dt, st_dt)
		return difference_in_months
	elif abb == 3:		#days
		days = dif_dhms(0,end_dt, st_dt)
		return days
	elif abb == 4:		#weeks
		weeks = dif_dhms(1,end_dt, st_dt)
		return weeks
	elif abb == 5:		#hours
		hours = dif_dhms(2,end_dt, st_dt)
		return hours
	elif abb == 6:		#minutes
		minutes = dif_dhms(3,end_dt, st_dt)
		return minutes
	elif abb == 7:		#seconds
		seconds = dif_dhms(4,end_dt, st_dt)
		return seconds

#---------- functions used by datediff and dateadd ----------       
def add_year(dt, years):     
    new_dt = dt.replace(year = dt.year + years)
    mod_dt = datetime.strftime(dt, "%Y-%m-%d  %H:%M:%S")
    return mod_dt
	
def add_months(inp_date, mnth_ad): #
	month = inp_date.month - 1 + mnth_ad
	year = inp_date.year + month // 12
	month = month % 12 + 1
	day = inp_date.day
	hour = inp_date.hour
	minute = inp_date.minute
	second = inp_date.second
	out_date = datetime(year,month,day,hour,minute,second) 
	output = datetime.strftime(out_date, "%Y-%m-%d %H:%M:%S")
	return output
	
def dif_months(s_dt, e_dt):
    month_dif = (s_dt.year - e_dt.year)*12 + s_dt.month - e_dt.month
    return month_dif
	
def dif_dhms(abb, f_dt, l_dt):
    difference = f_dt - l_dt
    days = difference.days
    secs = difference.seconds
    if abb == 0:    #days
        return days
    if abb == 1:    #weeks
        weeks = days//7
        return weeks
    if abb == 2:    #hours
        hours = (secs//3600) + (days*24)
        return hours
    if abb == 3:
        minutes = (secs//60) + (days*24*60)
        return minutes
    if abb == 4:
        seconds = secs + (days*24*3600)
        return seconds  
#------------------------------------------------------------        