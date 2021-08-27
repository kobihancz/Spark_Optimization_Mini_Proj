'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''

#Import pyspark spark SQL modules, as well is OS in order to obtain file path
import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os

#use SparkSession entry into Spark and create "Optimize I" application
spark = SparkSession.builder.appName('Optimize I').getOrCreate()

#base path is equal to the current working directory '/Users/kobihancz/Downloads/Springboard_Data_Engineering/Spark_Optimization_Mini_Project'
base_path = os.getcwd()

# project path
# project_path = ('/').join(base_path.split('/')[0:-3]) 

# answers path
# answers_input_path = os.path.join(project_path, 'data/answers')

# questions path
# questions_input_path = os.path.join(project_path, 'output/questions-transformed')


# Q/A via alternate method
project_path = "/Users/kobihancz/Downloads/Springboard_Data_Engineering/Spark_Optimization_Mini_Project/"
answers_path = project_path + "data/answers"
questions_path = project_path + "data/questions"

# read answers data into answers data frame
answersDF = spark.read.option('path', answers_path).load()

# read questions data into questions data frame 
questionsDF = spark.read.option('path', questions_path).load()

'''
Answers aggregation

Here we : get number of answers per question per month
'''
#use time object to see how long the queary takes
start_time = time.time()

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

# print the time the quary takes 
print("-- %s seconds to process --" % (time.time() - start_time))

resultDF.explain()

'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''

start_time = time.time()

spark.conf.set("spark.sql.adaptive.enabled","true")

answers_month = answersDF.withColumn('month', month('creation_date'))

#repartition by month in order to reduce shuffling

answers_month = answers_month.repartition(col("month"))

answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

print("-- %s seconds to process --" % (time.time() - start_time))

resultDF.explain()