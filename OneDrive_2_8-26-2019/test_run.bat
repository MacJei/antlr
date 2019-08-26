@echo off
rem python ProcessOffload.py -s Teradata -t Redshift -p Glue -i bteq -o pyspark -O D:\idw_code_test\tgt -I D:\idw_code_test\src_scripts\teradata\bteq

python ProcessOffload.py -s SqlServer -t Redshift -p Glue -i tsql -o pyspark -O D:\idw_code_test\tgt -I D:\idw_code_test\src_scripts\tsql