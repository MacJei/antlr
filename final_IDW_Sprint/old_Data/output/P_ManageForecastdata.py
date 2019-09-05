#Import packages
import sys
import re
from rs_conn_param import *
from ss_inblt_func import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession

def p_manageforecastdata(imode):

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    try:
        db_tables_dict = {'ams.iw_growth_lt_12x12':'ams__iw_growth_lt_12x12__df',
        'ams.iw_growth_stnorm_hourly':'ams__iw_growth_stnorm_hourly__df',
        'dbo.salesinputfile':'salesinputfile__df',
        'dbo.salesinputfile':'dbo__salesinputfile__df',
        'dbo.st_calculation':'st_calculation__df',
        'ams.iw_hourly_breakout':'ams__iw_hourly_breakout__df',
        'usnorth_release.activelist':'derlff__usnorth_release__activelist__df',
        'dbo.cte':'cte__df',
        'dbo.lt_cal':'lt_cal__df',
        'ams.iw_cluster_st':'ams__iw_cluster_st__df',
        'ams.iw_growth_stnorm_hourly':'ams__iw_growth_stnorm_hourly__df',
        'ams.iw_cluster_normal':'ams__iw_cluster_normal__df',
        'ams.iw_distr_hourly_loss':'ams__iw_distr_hourly_loss__df',
        'ams.iw_trans_hourly_loss':'ams__iw_trans_hourly_loss__df',
        'ams.iw_deration_hourly_loss':'ams__iw_deration_hourly_loss__df'}

        for table,df in db_tables_dict.items():
            if df not in mod_df.keys():
                dym__df = glueContext.create_dynamic_frame.from_catalog(database=cat_db, table_name=table)
                org_df[df] = dym__df.toDF()
                mod_df[df] = dym__df.toDF()
                mod_df[df].createOrReplaceTempView(df)
    except:
        raise
        #quit()

    
    try:
    
        load_profile_cd = '-1'
        dtnow = str(spark.sql("""select current_timestamp()""").collect()[0][0])
        
        ststartdate = str(spark.sql("""select add_months(to_date('1900-01-01') , datediff(m,0,to_timestamp('{}')))""".format(dtnow)).collect()[0][0])
        stenddate = str(spark.sql("""select to_date(dateadd(s,-1,dateadd(mm,datediff(m,0,to_timestamp('{}')+1)+2,0)),'MM/dd/yyyy')""".format(dtnow)).collect()[0][0])
        ltstartdate = str(spark.sql("""select add_months(to_date('1900-01-01') , datediff(m,0,to_timestamp('{}'))+2)""".format(dtnow)).collect()[0][0])
        ltusnstartdate = str(spark.sql("""select date_add(to_timestamp('{}') , 2 *7)""".format(dtnow)).collect()[0][0])
        ltenddate = str(spark.sql("""select to_date(max(flow_stop_dt),'MM/dd/yyyy') from salesinputfile__df""").collect()[0][0])
        if (imode==1):
            ams__iw_growth_lt_12x12__df_1 = spark.sql("""select *
            from ams__iw_growth_lt_12x12__df""")
            mod_df['ams__iw_growth_lt_12x12__df'] = mod_df['ams__iw_growth_lt_12x12__df'].subtract(ams__iw_growth_lt_12x12__df_1)
            mod_df['ams__iw_growth_lt_12x12__df'].createOrReplaceTempView('ams__iw_growth_lt_12x12__df')
            rowcount_df = ams__iw_growth_lt_12x12__df_1
            
            
            
            st_loop__df = spark.sql("""select salesinputfile__dfid,load_profile_cd,flow_start_dt,flow_stop_dt from dbo__salesinputfile__df__df""")
            
            st_loop__df_iter = iter(st_loop__df).collect()
            fetch_status = 0
            
            st_loop__df_cols = st_loop__df.columns
            try:
                st_loop__df_row = next(st_loop__df_iter).asDict()
                lid,liload_profile_cd ,liflow_start_dt,liflow_stop_dt = list(st_loop__df_row[col] for col in st_loop__df_cols)
            except StopIteration:
                fetch_status = 1
            
            while (fetch_status == 0):
                #Creating dataframe for select statement
                st_calculation__df = spark.sql("""select  salesinputfile__dfid,
                						case substring(product_cd,1,3) when 'DER' then 'ERCOT'
                						when 'PJM' then 'PJM' when 'NEP' then 'NEPOOL' when 'MIS' then 'MISO'else 'NYISO' end as market_cd,
                						'NOTOU' as tou_schedule_type_cd,'U1' as ufe_zone_cd,'NWS' as weather_sensitivity_cd,
                						s.load_profile_cd,s.load_profile_cd as forecast_profile_cd,s.weather_zone_cd,s.meter_type_cd,s.book_cd,
                						s.lse_cd,s.loss_cd,s.utility_cd,s.congestion_zone_cd,s.prov_state_cd,s.product_type_cd,s.component_type_cd,
                						'A' as temp_band_cd,'N' as source_cd,null as forecast_date,null hournum,avg_uf as usage_factor,s.count esiid_cnt,
                						00.0 as unadj_load, 000  as distrib_loss_load,00   as transmission_loss_load,0    as ufe_loss_load,0    as ancillary_loss_load,
                						0    as deration_loss_load,0 as cap_ob, 0 as tran_ob,to_timestamp('{}') as	crdt,to_timestamp('{}') as	batch_dt,0 as	batch_hr
                						,weather_station,cluster_cd,flow_start_dt ,flow_stop_dt 
                						from dbo__salesinputfile__df__df s 
                						where salesinputfile__dfid={} and ( (substring(product_cd,1,3) = 'DER' and flow_start_dt > to_timestamp('{}'))
                						 or (substring(product_cd,1,3) <> 'DER' and flow_start_dt > to_date('{}')))""".format(dtnow, dtnow, lid, dtnow, ltusnstartdate))
                st_calculation__df.createOrReplaceTempView('st_calculation__df')
                
                
                st_loop__df_cols = st_loop__df.columns
                try:
                    st_loop__df_row = next(st_loop__df_iter).asDict()
                    lid,liload_profile_cd ,liflow_start_dt,liflow_stop_dt = list(st_loop__df_row[col] for col in st_loop__df_cols)
                except StopIteration:
                    fetch_status = 1
                
            
            df_col_list = mod_df['lt_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.unadj_load': 'IFNULL((c.esiid_cnt*isnull(cst.load,cn.load)/1000),0) as unadj_load'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            lt_calculation_temp__df_1 = spark.sql("""select c.* 
             from #lt_cal__dfculation_temp c 
            			left join ams__iw_cluster_st__df cst on   (c.congestion_zone_cd=cst.zone and c.weather_station=cst.weather_station and c.cluster_cd=cst.cluster_cd and c.forecast_dt=cst.forecast_dt and c.hour_num=cst.hour_num)
            			left join ams__iw_cluster_normal__df cn on (c.congestion_zone_cd=cn.zone and c.weather_station=cn.weather_station and c.cluster_cd=cn.cluster_cd   and c.forecast_dt=cn.forecast_dt and c.hour_num=cn.hour_num)""")
            
            lt_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #lt_cal__dfculation_temp c 
            			left join ams__iw_cluster_st__df cst on   (c.congestion_zone_cd=cst.zone and c.weather_station=cst.weather_station and c.cluster_cd=cst.cluster_cd and c.forecast_dt=cst.forecast_dt and c.hour_num=cst.hour_num)
            			left join ams__iw_cluster_normal__df cn on (c.congestion_zone_cd=cn.zone and c.weather_station=cn.weather_station and c.cluster_cd=cn.cluster_cd   and c.forecast_dt=cn.forecast_dt and c.hour_num=cn.hour_num)""")
            
            mod_df['lt_calculation_temp__df'] = mod_df['lt_calculation_temp__df'].subtract(lt_calculation_temp__df_1).union(lt_calculation_temp__df_2)
            mod_df['lt_calculation_temp__df'].createOrReplaceTempView('lt_calculation_temp__df')
            rowcount_df = lt_calculation_temp__df_2
            
            df_col_list = mod_df['lt_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.distrib_loss_load': 'IFNULL(unadj_load*dhl.distrib_loss_pct,0) as distrib_loss_load'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            lt_calculation_temp__df_1 = spark.sql("""select c.* 
             from #lt_cal__dfculation_temp c 
            			left join ams__iw_distr_hourly_loss__df dhl on (c.market_cd=dhl.market_cd and c.utility_cd=dhl.utility_cd and c.loss_cd=dhl.loss_cd and c.forecast_dt=dhl.forecast_dt and c.hour_num=dhl.hour_num )""")
            
            lt_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #lt_cal__dfculation_temp c 
            			left join ams__iw_distr_hourly_loss__df dhl on (c.market_cd=dhl.market_cd and c.utility_cd=dhl.utility_cd and c.loss_cd=dhl.loss_cd and c.forecast_dt=dhl.forecast_dt and c.hour_num=dhl.hour_num )""")
            
            mod_df['lt_calculation_temp__df'] = mod_df['lt_calculation_temp__df'].subtract(lt_calculation_temp__df_1).union(lt_calculation_temp__df_2)
            mod_df['lt_calculation_temp__df'].createOrReplaceTempView('lt_calculation_temp__df')
            rowcount_df = lt_calculation_temp__df_2
            
            df_col_list = mod_df['lt_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.transmission_loss_load': 'IFNULL((unadj_load*thl.transmission_loss_pct)/100,0) as transmission_loss_load'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            lt_calculation_temp__df_1 = spark.sql("""select c.* 
             from #lt_cal__dfculation_temp c 
            			left join ams__iw_trans_hourly_loss__df thl on (c.market_cd=thl.market_cd and c.forecast_dt=thl.forecast_dt and c.hour_num=thl.hour_num)""")
            
            lt_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #lt_cal__dfculation_temp c 
            			left join ams__iw_trans_hourly_loss__df thl on (c.market_cd=thl.market_cd and c.forecast_dt=thl.forecast_dt and c.hour_num=thl.hour_num)""")
            
            mod_df['lt_calculation_temp__df'] = mod_df['lt_calculation_temp__df'].subtract(lt_calculation_temp__df_1).union(lt_calculation_temp__df_2)
            mod_df['lt_calculation_temp__df'].createOrReplaceTempView('lt_calculation_temp__df')
            rowcount_df = lt_calculation_temp__df_2
            
            df_col_list = mod_df['lt_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.deration_loss_load': 'IFNULL((unadj_load*thl.deration_loss_pct*-1)/100,0) as deration_loss_load'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            lt_calculation_temp__df_1 = spark.sql("""select c.* 
             from #lt_cal__dfculation_temp c 
            			left join ams__iw_deration_hourly_loss__df thl on (c.market_cd=thl.market_cd and c.forecast_dt=thl.forecast_dt and c.hour_num=thl.hour_num)""")
            
            lt_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #lt_cal__dfculation_temp c 
            			left join ams__iw_deration_hourly_loss__df thl on (c.market_cd=thl.market_cd and c.forecast_dt=thl.forecast_dt and c.hour_num=thl.hour_num)""")
            
            mod_df['lt_calculation_temp__df'] = mod_df['lt_calculation_temp__df'].subtract(lt_calculation_temp__df_1).union(lt_calculation_temp__df_2)
            mod_df['lt_calculation_temp__df'].createOrReplaceTempView('lt_calculation_temp__df')
            rowcount_df = lt_calculation_temp__df_2
            
            #Creating dataframe for select statement
            defaulttags__df = spark.sql("""select market_cd, utility_cd, congestion_zone_cd, load_profile_cd, avg(cast(capacity_plc_tag as float)/1000) as capacity_tag_avg, 
            			IFNULL( avg(cast(transmis_plc_tag as float)/1000) , avg(cast(capacity_plc_tag as float)/1000)) as tranmission_tag_avg from derlff__usnorth_release__activelist__df
            			where book_cd = 'org_ne' 
            			group by market_cd, utility_cd, congestion_zone_cd, load_profile_cd
            			order by market_cd, utility_cd, congestion_zone_cd, load_profile_cd""")
            defaulttags__df.createOrReplaceTempView('defaulttags__df')
            
            df_col_list = mod_df['lt_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.cap_ob': '(IFNULL(capacity_tag_avg ,0) ) as cap_ob', 'c.tran_ob': '(IFNULL(tranmission_tag_avg ,0) ) as tran_ob'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            lt_calculation_temp__df_1 = spark.sql("""select c.* 
             from #lt_cal__dfculation_temp c 
            			inner join #defaulttags  a on 
            			a.utility_cd=c.utility_cd and a.load_profile_cd=c.load_profile_cd
            			 and a.congestion_zone_cd=c.congestion_zone_cd and a.market_cd = c.market_cd""")
            
            lt_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #lt_cal__dfculation_temp c 
            			inner join #defaulttags  a on 
            			a.utility_cd=c.utility_cd and a.load_profile_cd=c.load_profile_cd
            			 and a.congestion_zone_cd=c.congestion_zone_cd and a.market_cd = c.market_cd""")
            
            mod_df['lt_calculation_temp__df'] = mod_df['lt_calculation_temp__df'].subtract(lt_calculation_temp__df_1).union(lt_calculation_temp__df_2)
            mod_df['lt_calculation_temp__df'].createOrReplaceTempView('lt_calculation_temp__df')
            rowcount_df = lt_calculation_temp__df_2
            
            #Creating dataframe for select statement
            cte__df = spark.sql("""select load_profile_cd,t.cluster_cd,t.loss_cd,t.utility_cd,
            			   t.weather_zone_cd,t.weather_station,t.congestion_zone_cd,t.product_type_cd,t.component_type_cd,
            				t.book_cd,t.lse_cd,t.meter_type_cd,t.prov_state_cd ,year(t.date) y ,month(t.date) m  ,sum(distinct esiid_cnt_cal)cnt ,t.flow_start_dt,t.flow_stop_dt,date
            		from  #lt_cal__dfculation_temp  t 
            		group by 
            		load_profile_cd,t.cluster_cd,t.loss_cd,t.utility_cd,t.weather_zone_cd,
            		t.weather_station,t.congestion_zone_cd,t.product_type_cd,t.component_type_cd,
            		t.book_cd,t.lse_cd,t.meter_type_cd,t.prov_state_cd ,year(date),month(date) ,t.flow_start_dt,t.flow_stop_dt,date""")
            cte__df.createOrReplaceTempView('cte__df')
            
            tempessiid__df_1 = spark.sql("""select load_profile_cd as load_profile_cd,
            cluster_cd as cluster_cd,
            loss_cd as loss_cd,
            utility_cd as utility_cd,
            weather_zone_cd as weather_zone_cd,
            weather_station as weather_station,
            congestion_zone_cd as congestion_zone_cd,
            product_type_cd as product_type_cd,
            component_type_cd as component_type_cd,
            book_cd as book_cd,
            lse_cd as lse_cd,
            meter_type_cd as meter_type_cd,
            prov_state_cd as prov_state_cd,
            date as edate,
            c.m as emonth,
            c.y as eyear,
            sum(cnt) as esiidcnt
            from   cte__df  c
            		 group by load_profile_cd,cluster_cd,loss_cd,utility_cd,
            			   weather_zone_cd,weather_station,congestion_zone_cd,product_type_cd,component_type_cd,
            				book_cd,lse_cd,meter_type_cd,prov_state_cd ,date, c.y,c.m
            		 order by  date, c.y,c.m""")
            mod_df['tempessiid__df'] = mod_df['tempessiid__df'].union(tempessiid__df_1)
            mod_df['tempessiid__df'].createOrReplaceTempView('tempessiid__df')
            rowcount_df = tempessiid__df_1
            
            
            #Creating dataframe for select statement
            lt_cal__df = spark.sql("""select  market_cd,tou_schedule_type_cd,ufe_zone_cd,
            							weather_sensitivity_cd,load_profile_cd ,forecast_profile_cd,weather_zone_cd,
            							meter_type_cd,book_cd,lse_cd,loss_cd,utility_cd,congestion_zone_cd,
            							prov_state_cd,product_type_cd, component_type_cd,temp_band_cd,source_cd,forecast_dt,
            							hour_num,usage_factor,
            							(esiid_cnt)as esiid_cnt,
            							sum(unadj_load)as unadj_load,
            							sum(distrib_loss_load) as distrib_loss_load,
            							sum(transmission_loss_load)as transmission_loss_load,
            							sum(ufe_loss_load) as ufe_loss_load,
            							sum(ancillary_loss_load) as ancillary_loss_load,
            							sum(deration_loss_load) as deration_loss_load,
            							max(cast(cap_ob as numeric(10,6))) as cap_ob,
            							max(cast(tran_ob as numeric(10,6))) as tran_ob,
            							crdt,
            							batch_dt,
            							batch_hr,block_12x12_desc,
            							weather_station,cluster_cd
            					from #lt_cal__dfculation_temp c where esiid_cnt > 0
            					group by 
            						market_cd,tou_schedule_type_cd,ufe_zone_cd,
            						weather_sensitivity_cd,load_profile_cd,forecast_profile_cd,
            						weather_zone_cd,meter_type_cd,book_cd,
            						lse_cd,loss_cd,utility_cd,congestion_zone_cd,
            						prov_state_cd,product_type_cd,component_type_cd,temp_band_cd,source_cd,
            						forecast_dt,hour_num,usage_factor,
            						crdt,batch_dt,batch_hr,block_12x12_desc,
            						weather_station,cluster_cd,esiid_cnt""")
            lt_cal__df.createOrReplaceTempView('lt_cal__df')
            
            
        if (imode==2):
            ams__iw_growth_stnorm_hourly__df_1 = spark.sql("""select *
            from ams__iw_growth_stnorm_hourly__df""")
            mod_df['ams__iw_growth_stnorm_hourly__df'] = mod_df['ams__iw_growth_stnorm_hourly__df'].subtract(ams__iw_growth_stnorm_hourly__df_1)
            mod_df['ams__iw_growth_stnorm_hourly__df'].createOrReplaceTempView('ams__iw_growth_stnorm_hourly__df')
            rowcount_df = ams__iw_growth_stnorm_hourly__df_1
            
            
            
            st_loop__df = spark.sql("""select salesinputfile__dfid, load_profile_cd,flow_start_dt,flow_stop_dt from dbo__salesinputfile__df__df""")
            
            st_loop__df_iter = iter(st_loop__df).collect()
            fetch_status = 0
            
            st_loop__df_cols = st_loop__df.columns
            try:
                st_loop__df_row = next(st_loop__df_iter).asDict()
                id, iload_profile_cd ,iflow_start_dt,iflow_stop_dt = list(st_loop__df_row[col] for col in st_loop__df_cols)
            except StopIteration:
                fetch_status = 1
            
            while (fetch_status == 0):
                #Creating dataframe for select statement
                st_calculation__df = spark.sql("""select  
                						salesinputfile__dfid,  case substring(product_cd,1,3) when 'DER' then 'ERCOT'
                						when 'PJM' then 'PJM' when 'NEP' then 'NEPOOL' when 'MIS' then 'MISO'else 'NYISO' end as market_cd,
                						'NOTOU' as tou_schedule_type_cd,'U1' as ufe_zone_cd,'NWS' as weather_sensitivity_cd,
                						s.load_profile_cd,s.load_profile_cd as forecast_profile_cd,s.weather_zone_cd,s.meter_type_cd,
                						s.book_cd,s.lse_cd,s.loss_cd,s.utility_cd,s.congestion_zone_cd,s.prov_state_cd,s.product_type_cd,
                						s.component_type_cd,'A' as temp_band_cd,'N' as source_cd,null as forecast_date,null hournum,avg_uf as usage_factor,
                						s.count esiid_cnt,00.0 as unadj_load, 000  as distrib_loss_load,00 as transmission_loss_load,
                						0 as ufe_loss_load, 0 as ancillary_loss_load,0 as deration_loss_load,null as cap_ob, null as tran_ob,
                						to_timestamp('{}') as	crdt,to_timestamp('{}') as	batch_dt,null as	batch_hr
                						,weather_station,cluster_cd,flow_start_dt,flow_stop_dt 
                						from dbo__salesinputfile__df__df s 
                						where salesinputfile__dfid={} 
                						and ( (substring(product_cd,1,3) = 'DER' and flow_start_dt > to_timestamp('{}'))
                						 or (substring(product_cd,1,3) <> 'DER' and flow_start_dt > to_date('{}')))""".format(dtnow, dtnow, id, dtnow, ltusnstartdate))
                st_calculation__df.createOrReplaceTempView('st_calculation__df')
                
                st_calculation_temp__df_1 = spark.sql("""select {} as saleinputid,
                s.market_cd as market_cd,
                tou_schedule_type_cd as tou_schedule_type_cd,
                ufe_zone_cd as ufe_zone_cd,
                weather_sensitivity_cd as weather_sensitivity_cd,
                load_profile_cd as load_profile_cd,
                concat(load_profile_cd,'_',weather_zone_cd)  as forecast_profile_cd as forecast_profile_cd,
                weather_zone_cd as weather_zone_cd,
                meter_type_cd as meter_type_cd,
                book_cd as book_cd,
                lse_cd as lse_cd,
                loss_cd as loss_cd,
                utility_cd as utility_cd,
                congestion_zone_cd as congestion_zone_cd,
                prov_state_cd as prov_state_cd,
                product_type_cd as product_type_cd,
                component_type_cd as component_type_cd,
                temp_band_cd as temp_band_cd,
                source_cd as source_cd,
                d.hb_date forecast_date as forecast_dt,
                d.hour_ending hournum as hour_num,
                usage_factor as usage_factor,
                case  when d.hb_date > to_date('{}') and  d.hb_date <=to_date('{}')
                									  then s.esiid_cnt*usage_factor
                									  else 0	
                								 end esiid_cnt as esiid_cnt,
                unadj_load as unadj_load,
                distrib_loss_load as distrib_loss_load,
                transmission_loss_load as transmission_loss_load,
                ufe_loss_load as ufe_loss_load,
                ancillary_loss_load as ancillary_loss_load,
                deration_loss_load as deration_loss_load,
                cap_ob as cap_ob,
                tran_ob as tran_ob,
                crdt as crdt,
                batch_dt as batch_dt,
                batch_hr as batch_hr,
                d.block_12x12_desc as block_12x12_desc,
                weather_station as weather_station,
                cluster_cd as cluster_cd,
                flow_start_dt as flow_start_dt,
                flow_stop_dt as flow_stop_dt
                from  st_calculation__df s 
                			outer apply (
                						select hb_date,h.hour_ending,h.block_12x12_desc  from ams__iw_hourly_breakout__df h 
                						where  
                						 to_date(h.hb_date,'MM/dd/yyyy')>= to_date(to_timestamp('{}'),'MM/dd/yyyy')
                						and to_date(h.hb_date,'MM/dd/yyyy')<=to_date(to_timestamp('{}'),'MM/dd/yyyy')
                						and h.hour_ending is not null
                						) d""".format(id, ststartdate, stenddate, iflow_start_dt, iflow_stop_dt))
                mod_df['st_calculation_temp__df'] = mod_df['st_calculation_temp__df'].union(st_calculation_temp__df_1)
                mod_df['st_calculation_temp__df'].createOrReplaceTempView('st_calculation_temp__df')
                rowcount_df = st_calculation_temp__df_1
                
                
                st_loop__df_cols = st_loop__df.columns
                try:
                    st_loop__df_row = next(st_loop__df_iter).asDict()
                    id, iload_profile_cd ,iflow_start_dt,iflow_stop_dt = list(st_loop__df_row[col] for col in st_loop__df_cols)
                except StopIteration:
                    fetch_status = 1
                
            
            df_col_list = mod_df['st_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.unadj_load': 'IFNULL((c.esiid_cnt*isnull(cst.load,cn.load)/1000),0) as unadj_load'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            st_calculation_temp__df_1 = spark.sql("""select c.* 
             from #st_calculation__df_temp c 
            			left join( select cst.zone, cst.weather_station,cst.cluster_cd,cst.forecast_dt,cst.hour_num,cst.load
            						 from  ams__iw_cluster_st__df cst 
            						 inner join (
            						    select cs.zone, cs.weather_station,cs.cluster_cd,cs.forecast_dt,cs.hour_num ,max(batch_dt) batchdate
            							 from ams__iw_cluster_st__df cs
            							 group by 
            							cs.zone, cs.weather_station,cs.cluster_cd,cs.forecast_dt,cs.hour_num
            							) t on cst.zone = t.zone and t.weather_station=cst.weather_station and cst.cluster_cd=t.cluster_cd and cst.forecast_dt=t.forecast_dt and cst.hour_num=t.hour_num and t.batchdate=cst.batch_dt
            						) cst on   (c.congestion_zone_cd=cst.zone and c.weather_station=cst.weather_station and c.cluster_cd=cst.cluster_cd and c.forecast_dt=cst.forecast_dt and c.hour_num=cst.hour_num)
            			left join ams__iw_cluster_normal__df cn on (c.congestion_zone_cd=cn.zone and c.weather_station=cn.weather_station and c.cluster_cd=cn.cluster_cd   and c.forecast_dt=cn.forecast_dt and c.hour_num=cn.hour_num)""")
            
            st_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #st_calculation__df_temp c 
            			left join( select cst.zone, cst.weather_station,cst.cluster_cd,cst.forecast_dt,cst.hour_num,cst.load
            						 from  ams__iw_cluster_st__df cst 
            						 inner join (
            						    select cs.zone, cs.weather_station,cs.cluster_cd,cs.forecast_dt,cs.hour_num ,max(batch_dt) batchdate
            							 from ams__iw_cluster_st__df cs
            							 group by 
            							cs.zone, cs.weather_station,cs.cluster_cd,cs.forecast_dt,cs.hour_num
            							) t on cst.zone = t.zone and t.weather_station=cst.weather_station and cst.cluster_cd=t.cluster_cd and cst.forecast_dt=t.forecast_dt and cst.hour_num=t.hour_num and t.batchdate=cst.batch_dt
            						) cst on   (c.congestion_zone_cd=cst.zone and c.weather_station=cst.weather_station and c.cluster_cd=cst.cluster_cd and c.forecast_dt=cst.forecast_dt and c.hour_num=cst.hour_num)
            			left join ams__iw_cluster_normal__df cn on (c.congestion_zone_cd=cn.zone and c.weather_station=cn.weather_station and c.cluster_cd=cn.cluster_cd   and c.forecast_dt=cn.forecast_dt and c.hour_num=cn.hour_num)""")
            
            mod_df['st_calculation_temp__df'] = mod_df['st_calculation_temp__df'].subtract(st_calculation_temp__df_1).union(st_calculation_temp__df_2)
            mod_df['st_calculation_temp__df'].createOrReplaceTempView('st_calculation_temp__df')
            rowcount_df = st_calculation_temp__df_2
            
            df_col_list = mod_df['st_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.distrib_loss_load': 'IFNULL(unadj_load*dhl.distrib_loss_pct,0) as distrib_loss_load'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            st_calculation_temp__df_1 = spark.sql("""select c.* 
             from #st_calculation__df_temp c 
            			left join ams__iw_distr_hourly_loss__df dhl on (c.market_cd=dhl.market_cd and c.utility_cd=dhl.utility_cd and c.loss_cd=dhl.loss_cd and c.forecast_dt=dhl.forecast_dt and c.hour_num=dhl.hour_num )""")
            
            st_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #st_calculation__df_temp c 
            			left join ams__iw_distr_hourly_loss__df dhl on (c.market_cd=dhl.market_cd and c.utility_cd=dhl.utility_cd and c.loss_cd=dhl.loss_cd and c.forecast_dt=dhl.forecast_dt and c.hour_num=dhl.hour_num )""")
            
            mod_df['st_calculation_temp__df'] = mod_df['st_calculation_temp__df'].subtract(st_calculation_temp__df_1).union(st_calculation_temp__df_2)
            mod_df['st_calculation_temp__df'].createOrReplaceTempView('st_calculation_temp__df')
            rowcount_df = st_calculation_temp__df_2
            
            df_col_list = mod_df['st_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.transmission_loss_load': 'IFNULL((unadj_load*thl.transmission_loss_pct)/100,0) as transmission_loss_load'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            st_calculation_temp__df_1 = spark.sql("""select c.* 
             from #st_calculation__df_temp c 
            			left join ams__iw_trans_hourly_loss__df thl on (c.market_cd=thl.market_cd and c.forecast_dt=thl.forecast_dt and c.hour_num=thl.hour_num)""")
            
            st_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #st_calculation__df_temp c 
            			left join ams__iw_trans_hourly_loss__df thl on (c.market_cd=thl.market_cd and c.forecast_dt=thl.forecast_dt and c.hour_num=thl.hour_num)""")
            
            mod_df['st_calculation_temp__df'] = mod_df['st_calculation_temp__df'].subtract(st_calculation_temp__df_1).union(st_calculation_temp__df_2)
            mod_df['st_calculation_temp__df'].createOrReplaceTempView('st_calculation_temp__df')
            rowcount_df = st_calculation_temp__df_2
            
            df_col_list = mod_df['st_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.deration_loss_load': 'IFNULL((unadj_load*thl.deration_loss_pct*-1)/100,0) as deration_loss_load'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            st_calculation_temp__df_1 = spark.sql("""select c.* 
             from #st_calculation__df_temp c 
            			left join ams__iw_deration_hourly_loss__df thl on (c.market_cd=thl.market_cd and c.forecast_dt=thl.forecast_dt and c.hour_num=thl.hour_num)""")
            
            st_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #st_calculation__df_temp c 
            			left join ams__iw_deration_hourly_loss__df thl on (c.market_cd=thl.market_cd and c.forecast_dt=thl.forecast_dt and c.hour_num=thl.hour_num)""")
            
            mod_df['st_calculation_temp__df'] = mod_df['st_calculation_temp__df'].subtract(st_calculation_temp__df_1).union(st_calculation_temp__df_2)
            mod_df['st_calculation_temp__df'].createOrReplaceTempView('st_calculation_temp__df')
            rowcount_df = st_calculation_temp__df_2
            
            #Creating dataframe for select statement
            defaulttags2__df = spark.sql("""select market_cd, utility_cd, congestion_zone_cd, load_profile_cd, avg(cast(capacity_plc_tag as float)/1000) as capacity_tag_avg, 
            			IFNULL( avg(cast(transmis_plc_tag as float)/1000) , avg(cast(capacity_plc_tag as float)/1000)) as tranmission_tag_avg from derlff__usnorth_release__activelist__df
            			where book_cd = 'org_ne' 
            			group by market_cd, utility_cd, congestion_zone_cd, load_profile_cd
            			order by market_cd, utility_cd, congestion_zone_cd, load_profile_cd""")
            defaulttags2__df.createOrReplaceTempView('defaulttags2__df')
            
            df_col_list = mod_df['st_calculation_temp__df'].columns
            df_col_list_str = ('c.') + (',c.').join(df_col_list)
            upd_col_dict = {'c.cap_ob': '(IFNULL(capacity_tag_avg ,0) ) as cap_ob', 'c.tran_ob': '(IFNULL(tranmission_tag_avg ,0) ) as tran_ob'}
            
            for col in upd_col_dict.keys():
                df_col_list_str = df_col_list_str.replace(col,upd_col_dict[col])
            
            st_calculation_temp__df_1 = spark.sql("""select c.* 
             from #st_calculation__df_temp c 
            			inner join #defaulttags2  a on 
            			a.utility_cd=c.utility_cd and a.load_profile_cd=c.load_profile_cd and a.congestion_zone_cd=c.congestion_zone_cd""")
            
            st_calculation_temp__df_2 = spark.sql("select " + df_col_list_str + """from #st_calculation__df_temp c 
            			inner join #defaulttags2  a on 
            			a.utility_cd=c.utility_cd and a.load_profile_cd=c.load_profile_cd and a.congestion_zone_cd=c.congestion_zone_cd""")
            
            mod_df['st_calculation_temp__df'] = mod_df['st_calculation_temp__df'].subtract(st_calculation_temp__df_1).union(st_calculation_temp__df_2)
            mod_df['st_calculation_temp__df'].createOrReplaceTempView('st_calculation_temp__df')
            rowcount_df = st_calculation_temp__df_2
            
            #Creating dataframe for select statement
            cte__df = spark.sql("""select market_cd,tou_schedule_type_cd,ufe_zone_cd,weather_sensitivity_cd,load_profile_cd,
            							concat(load_profile_cd,'_',weather_zone_cd) as forecast_profile_cd,
            							weather_zone_cd,meter_type_cd,book_cd,
            							lse_cd,loss_cd,utility_cd,congestion_zone_cd,prov_state_cd,product_type_cd,
            							component_type_cd,temp_band_cd,source_cd,forecast_dt,hour_num,usage_factor,
            							sum(esiid_cnt)as esiid_cnt,sum(unadj_load)as unadj_load,
            							sum(distrib_loss_load) as distrib_loss_load,sum(transmission_loss_load)as transmission_loss_load,
            							sum(ufe_loss_load)as ufe_loss_load,sum(ancillary_loss_load)as ancillary_loss_load,
            							sum(deration_loss_load)as deration_loss_load,max(cast(cap_ob as numeric(10,6))) as cap_ob,
            							max(cast(tran_ob as numeric(10,6))) as tran_ob,crdt,batch_dt,batch_hr,block_12x12_desc
            							,weather_station,cluster_cd
            					from #st_calculation__df_temp c
            					group by 
            					market_cd,tou_schedule_type_cd,ufe_zone_cd,weather_sensitivity_cd,load_profile_cd,forecast_profile_cd,
            					weather_zone_cd,meter_type_cd,book_cd,lse_cd,loss_cd,utility_cd,congestion_zone_cd,
            					prov_state_cd,product_type_cd,component_type_cd,temp_band_cd,source_cd,forecast_dt,hour_num,
            					usage_factor,
            					crdt,batch_dt,batch_hr,
            					block_12x12_desc,weather_station,cluster_cd""")
            cte__df.createOrReplaceTempView('cte__df')
            
            ams__iw_growth_stnorm_hourly__df_1 = spark.sql("""select market_cd as market_cd,
            tou_schedule_type_cd as tou_schedule_type_cd,
            ufe_zone_cd as ufe_zone_cd,
            weather_sensitivity_cd as weather_sensitivity_cd,
            load_profile_cd as load_profile_cd,
            forecast_profile_cd as forecast_profile_cd,
            weather_zone_cd as weather_zone_cd,
            meter_type_cd as meter_type_cd,
            book_cd as book_cd,
            lse_cd as lse_cd,
            loss_cd as loss_cd,
            utility_cd as utility_cd,
            congestion_zone_cd as congestion_zone_cd,
            prov_state_cd as prov_state_cd,
            product_type_cd as product_type_cd,
            component_type_cd as component_type_cd,
            temp_band_cd as temp_band_cd,
            source_cd as source_cd,
            forecast_dt as forecast_dt,
            hour_num as hour_num,
            usage_factor as usage_factor,
            case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then esiid_cnt else 0 end esiid_cnt as esiid_cnt,
            case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then unadj_load else 0 end unadj_load as unadj_load,
            case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then distrib_loss_load else 0 end distrib_loss_load as distrib_loss_load,
            case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then transmission_loss_load else 0 end transmission_loss_load as transmission_loss_load,
            case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then ufe_loss_load else 0 end ufe_loss_load as ufe_loss_load,
            case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then ancillary_loss_load else 0 end ancillary_loss_load as ancillary_loss_load,
            case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then deration_loss_load else 0 end deration_loss_load as deration_loss_load,
            cap_ob*(case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then esiid_cnt else 0 end) as cap_ob as cap_ob,
            tran_ob*(case when to_date(forecast_dt,'MM/dd/yyyy') >=  to_date(getdate(),'MM/dd/yyyy') then esiid_cnt else 0 end) as tran_ob as tran_ob,
            crdt as crdt,
            batch_dt as batch_dt,
            batch_hr as batch_hr
            from   cte__df 
            			where esiid_cnt>0""")
            mod_df['ams__iw_growth_stnorm_hourly__df'] = mod_df['ams__iw_growth_stnorm_hourly__df'].union(ams__iw_growth_stnorm_hourly__df_1)
            mod_df['ams__iw_growth_stnorm_hourly__df'].createOrReplaceTempView('ams__iw_growth_stnorm_hourly__df')
            rowcount_df = ams__iw_growth_stnorm_hourly__df_1
            
            
        if (imode==3):
            #Creating dataframe for select statement
            select__df = spark.sql("""select market_cd, tou_schedule_type_cd, ufe_zone_cd, weather_sensitivity_cd, load_profile_cd, 
            		forecast_profile_cd, weather_zone_cd, meter_type_cd, book_cd, lse_cd, loss_cd, utility_cd,
            		congestion_zone_cd, prov_state_cd, product_type_cd, component_type_cd, temp_band_cd, source_cd, 
            		forecast_month, block_12x12_desc, usage_factor, unadj_load, esiid_cnt, distrib_loss_load, 
            		transmission_loss_load, ufe_loss_load, ancillary_loss_load, deration_loss_load,
            		cap_ob, tran_ob, crdt, batch_dt, batch_hr
            		from ams__iw_growth_lt_12x12__df
            		order by market_cd, tou_schedule_type_cd, ufe_zone_cd, weather_sensitivity_cd, load_profile_cd, 
            		forecast_profile_cd, weather_zone_cd, meter_type_cd, book_cd, lse_cd, loss_cd, utility_cd,
            		congestion_zone_cd, prov_state_cd, product_type_cd, component_type_cd, temp_band_cd, source_cd, 
            		forecast_month, block_12x12_desc""")
            select__df.createOrReplaceTempView('select__df')
            rowcount_df = select__df
            
        if (imode==4):
            #Creating dataframe for select statement
            select__df = spark.sql("""select market_cd, tou_schedule_type_cd, ufe_zone_cd, weather_sensitivity_cd, load_profile_cd,
            		forecast_profile_cd, weather_zone_cd, meter_type_cd, book_cd, lse_cd, loss_cd, utility_cd, 
            		congestion_zone_cd, prov_state_cd, product_type_cd, component_type_cd, temp_band_cd, 
            		source_cd, forecast_dt, hour_num, usage_factor, esiid_cnt, unadj_load, distrib_loss_load, 
            		transmission_loss_load, ufe_loss_load, ancillary_loss_load, deration_loss_load, cap_ob, 
            		tran_ob, crdt, batch_dt, batch_hr
            		from ams__iw_growth_stnorm_hourly__df
            		order by forecast_dt ,hour_num""")
            select__df.createOrReplaceTempView('select__df')
            rowcount_df = select__df
            
    
    except Exception as e:
        raise
    
        
        errormessage = str(spark.sql("""select error_message()
              ,{} = error_severity()
              ,{} = error_state()""".format(errorseverity, errorstate)).collect()[0][0])


#Write modified data frames to target
if __name__ == '__main__':
    p_manageforecastdata(*sys.argv[1:])
    try:
        for tab_df in mod_df.keys():
            if mod_df[tab_df] == org_df[tab_df]:
                continue
            dym__trans__df = DynamicFrame.fromDF(mod_df[tab_df],glueContext,'dym__trans__df')
            glueContext.write_dynamic_frame.from_options(frame = dym__trans__df, connection_type = 's3', connection_options = {'path': 's3://target/s3tables'}, format = 'csv')
    except:
        raise

