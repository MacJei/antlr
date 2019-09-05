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

#SPDEF#
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    try:
        db_tables_dict = {'icm_ods.prem_addr_dtls':'icm_ods__icm_ods__prem_addr_dtls__df',
        'icm_ods.prem_dtls':'icm_ods__icm_ods__prem_dtls__df',
        'icm_ods.prem_enrol_dtls':'icm_ods__icm_ods__prem_enrol_dtls__df',
        'icm_trans.peacetx_prem_enrol_chanl_dtls':'icm_stage__icm_trans__peacetx_prem_enrol_chanl_dtls__df'}

        for table,df in db_tables_dict.items():
            if df not in mod_df.keys():
                dym__df = glueContext.create_dynamic_frame.from_catalog(database=cat_db, table_name=table)
                org_df[df] = dym__df.toDF()
                mod_df[df] = dym__df.toDF()
                mod_df[df].createOrReplaceTempView(df)
    except:
        raise
        #quit()



#Write modified data frames to target
if __name__ == '__main__':
    (*sys.argv[1:])
    try:
        for tab_df in mod_df.keys():
            if mod_df[tab_df] == org_df[tab_df]:
                continue
            dym__trans__df = DynamicFrame.fromDF(mod_df[tab_df],glueContext,'dym__trans__df')
            glueContext.write_dynamic_frame.from_options(frame = dym__trans__df, connection_type = 's3', connection_options = {'path': 's3://target/s3tables'}, format = 'csv')
    except:
        raise

