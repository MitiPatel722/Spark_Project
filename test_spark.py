#Dynamically take the table name from text file and clean the data for all tables
from pyspark.sql.functions import col, count, isnan, lit, sum
import sys
import os
import subprocess
#function to run hdfs command and check whether directory alrady exist in hdfs or not
def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode
#function to run any hdfs commands in pyspark
def run_cmd1(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    (output, errors) = proc.communicate()
    if proc.returncode:
        raise RuntimeError(
            'Error running command: %s. Return code: %d, Error: %s' % (
                ' '.join(args_list), proc.returncode, errors))
    return (output, errors)
#function to count total number null for each column
def count_not_null(c, nan_as_null=False):
    """Use conversion between boolean and integer
    - False -> 0
    - True ->  1
    """
    pred = col(c).isNull() & (~isnan(c) if nan_as_null else lit(True))
    return sum(pred.cast("integer")).alias(c)
with open('tablename.txt') as f:
	 for i in f:
			i  = i.rstrip('\n')
			print("table name: "+ i);
               		storeprint = 0
			tables_df = spark.sql("select * from miti_db.{}".format(i))
			print('count of {0}  {1}'.format(i,tables_df.count()))
			dropduplicate_tables_df =tables_df.dropDuplicates()
              		print('count of dinstinct {0} {1}'.format(i,dropduplicate_tables_df.count()))
	   		dropnull=tables_df.dropna() 
                        print('count after droping null {0} {1}'.format(i,dropnull.count()))
                        dfschemafordistinct=tables_df.select([ col_name for col_name in tables_df.columns if tables_df.count()== dropduplicate_tables_df.count() ])
                        dfschemafordistinctstr=str(dfschemafordistinct)
			columns = [ column for column in tables_df.columns if len(tables_df.select(column).distinct().collect()) == len(tables_df.select(column).collect()) ]
		#	df.select(columns).show()
			printschema = tables_df.select(columns)
			printschemastr = str(printschema)
			print(printschemastr)
			print(dfschemafordistinctstr)
                        #print(dfschemafordistinctstr)
			#dfschemafordistinct.show(1)
                        #print(suppliers.select([ col_name for col_name in suppliers.columns if suppliers.count()== dropduplicatsupplier.count() ]))
                        import re
			strprimarykey=re.findall(r'\[(\w+)',printschemastr)
			if not strprimarykey:
				 print ('There is no primary key in this table')
                                 dropduplicate_tables_df = tables_df.dropDuplicates()
                                 dropnul_thresh3 = dropduplicate_tables_df.dropna(thresh=3)
				 dropnul_thresh3.show(3)
                                 print('final count in table which has no primary key {0}'.format(dropnul_thresh3.count()))
				 path = str("/user/miti/project_shellscript/alltables_cleansed/")+i
				 print(path)

    				 cmd = ['hadoop', 'fs', '-test', '-e',path]
				 code = run_cmd(cmd)
				 print(code)
				 if code == 1:
				# dropnul_thresh3.write.csv("/user/miti/testcsv/{0}".format(i))
			 		 dropnul_thresh3.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(path)
					 print("File has been save")
			 	 else: 
					 print("CSV File is already written")
				 
			else: 
                       		 print(strprimarykey[0]) 
                       		 print('Count of distinct_tables_df from subset, excluding primarykey: {0}'.format(
    tables_df.select([
        col_name for col_name in tables_df.columns if col_name != strprimarykey[0]
    ]).distinct().count()))
        			 tables_df_dropduplicate_expk = tables_df.dropDuplicates(subset=[
    col_name for col_name in tables_df.columns if col_name != strprimarykey[0]])
  				 drop_null_thres3 = tables_df_dropduplicate_expk.dropna(thresh=3)
				 drop_null_thres3.show(3)
				 print('final count after dropping row with three nulls {0}'.format(drop_null_thres3.count()))
				 path = str("/user/miti/project_shellscript/alltables_cleansed/")+i
				 print(path)
                                # drop_null_thres3.write.csv("/user/miti/testcsv/{0}".format(i))
				 cmd = ['hadoop', 'fs', '-test', '-e',path]
				 code = run_cmd(cmd)
				 print(code)
				 if code == 1:
			   	 	drop_null_thres3.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(path)	
				 else:
					print("CSV File is already written")	
			print("total null counts in Columns")	
			tables_df.agg(*[count_not_null(c) for c in tables_df.columns]).show()                
    		     

quit()
