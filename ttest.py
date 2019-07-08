from __future__ import print_function
import sys
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from scipy.stats import ttest_ind
from pyspark.sql.functions import lit
import numpy as np
sqlContext = SQLContext(SparkContext())
spark = SparkSession.builder.appName('ttest2.py').getOrCreate()


#Function for calculating mean
def fun_mean(data):
	n = fun_length(data)
	if n==0.0:
		return 0.0
	else:
		sum = 0
		for x in data:
			print(x)
			print(type(x))
			sum += x
		mn = sum/n
		return float(mn)

#Function for getting variance
def fun_var(data):
	n= fun_length(data)
	if n<=1.0:
		return 0.0
	else:
		mean_a = fun_mean(data)
		sqr_sum = 0
		for x in data:
			sqr_sum += (((x-mean_a)*(x-mean_a)))
		var_a = sqr_sum/(n-1)
		return float(var_a)

#function for calculating length
def fun_length(data):
	return float(len(data))

#function for T-test
#Refer wiki (Equal or unequal sample sizes, unequal variances): https://en.wikipedia.org/wiki/Student%27s_t-test
def fun_t_test(data1,data2):
	import math
	var_a = fun_var(data1)
	mean_a = fun_mean(data1)
	n_a = fun_length(data1)
	n_b = fun_length(data2)
	var_b = fun_var(data2) 
	mean_b = fun_mean(data2)
	if((n_a==0.0)|(n_b==0.0)):
		ttest=0.0
	else:
		n_b = fun_length(data1)
		ttest = (mean_a-mean_b)/(math.sqrt((var_a/n_a)+(var_b/n_b)))
	return float(ttest)

#function to claculate degree of freedom
#Refer wiki (Equal or unequal sample sizes, unequal variances): https://en.wikipedia.org/wiki/Student%27s_t-test
def fun_t_df(data1,data2):
	n_a = fun_length(data1)
	n_b = fun_length(data2)
	df = n_a+n_b-2
	return float(df)

# p-value UDF - my function
def fun_p_value(t_value,df):
	from scipy import stats	
	if(df<=2.0):
		p=0.0
	else:
		p = (1 - stats.t.cdf(abs(t_value),df=df))*2
	return float(p)

# python standard library for p-value
def fun_python_lib(a, b):
	t, pvalue = ttest_ind(a, b, equal_var=False)
	return float(pvalue)

import pandas as pd
#Reading the two text files- groupA.txt and groupB.txt (Two arguments passed through the command)
biosetpath = sys.argv[1]
grpA = pd.read_csv(sys.argv[2],header=None)
grpB = pd.read_csv(sys.argv[3],header=None)
grpAlist = grpA[0].tolist()
grpBlist = grpB[0].tolist()

#create full directory path
inputpathA=[]
inputpathB=[]
for i in grpAlist:
	inputpathA.append(biosetpath+'/'+str(i))
for j in grpBlist:
	inputpathB.append(biosetpath+'/'+str(j))

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType

emps_schema = StructType([StructField("gene_id", IntegerType(), True),StructField("reference", StringType(), True),StructField("gene_value", FloatType(), True)])

df1 = spark.read.csv(inputpathA,schema=emps_schema)
df2 = spark.read.csv(inputpathB,schema=emps_schema )

#Renaming the columns
#df1 = df1.withColumnRenamed('_c0','gene_id').withColumnRenamed('_c1','reference').withColumnRenamed('_c2','gene_value')
#df2 = df2.withColumnRenamed('_c0','gene_id').withColumnRenamed('_c1','reference').withColumnRenamed('_c2','gene_value')

#Clean the data (Filter out records in which any of the column is null) 
#RULE-5: if a record does not proper fields,then that record must be dropped
NonNullDF1 = df1.filter((col("gene_id").isNull()==False) | (col("reference").isNull()==False)|(col("gene_value").isNull()==False))
NonNullDF2 = df2.filter((col("gene_id").isNull()==False) | (col("reference").isNull()==False)|(col("gene_value").isNull()==False))

#RULE-4: all records with reference value of "r2" MUST be dropped
df1_r1 = NonNullDF1.filter((col('reference')=='r1'))
df2_r1 = NonNullDF2.filter((col('reference')=='r1'))

#groupby gene_id
df1_id_value =df1_r1.groupBy('gene_id').agg(F.collect_list('gene_value'))
df2_id_value =df2_r1.groupBy('gene_id').agg(F.collect_list('gene_value'))
df1_c = df1_id_value.withColumnRenamed('collect_list(gene_value)','df1_gene_values')
df2_c = df2_id_value.withColumnRenamed('collect_list(gene_value)','df2_gene_values')

#UDF's
function_mean_udf = udf(fun_mean,FloatType())
function_var_udf = udf(fun_var,FloatType())
function_length_udf = udf(fun_length,FloatType())
function_t_test_udf = udf(fun_t_test,FloatType())
function_t_df_udf = udf(fun_t_df,FloatType())
function_p_value_udf = udf(fun_p_value,FloatType())
function_python_lib_udf = udf(fun_python_lib,FloatType())

#JOIN two dataframe
full_outer_join = df1_c.join(df2_c, df1_c.gene_id == df2_c.gene_id, how='full').drop(df2_c.gene_id)

#T-test
full_outer_join_ttest = full_outer_join.withColumn('mean-A',function_mean_udf(full_outer_join['df1_gene_values'])).withColumn('a_length',function_length_udf(full_outer_join['df1_gene_values'])).withColumn('b_length',function_length_udf(full_outer_join['df2_gene_values'])).withColumn('mean-B',function_mean_udf(full_outer_join['df2_gene_values'])).withColumn('ttest',function_t_test_udf(full_outer_join['df1_gene_values'],full_outer_join['df2_gene_values'])).withColumn('degree_of_freedom',function_t_df_udf(full_outer_join['df1_gene_values'],full_outer_join['df2_gene_values']))

# P-value (using my function)
full_outer_join_ttest_p = full_outer_join_ttest.withColumn('my-p-value',function_p_value_udf(full_outer_join_ttest['ttest'],full_outer_join_ttest['degree_of_freedom']))

#P-value using library
newDf = full_outer_join_ttest_p.withColumn("python-p-value",function_python_lib_udf(full_outer_join_ttest_p['df1_gene_values'],full_outer_join_ttest_p['df2_gene_values']))

select_DF = newDf.select('gene_id','my-p-value',"python-p-value",'mean-A','mean-B')
select_DF.show()
spark.stop()


