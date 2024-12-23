#!/usr/bin/env python3

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 1")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")


lines = temperature_file.map(lambda line: line.split(";"))

table = lines.map(lambda p: Row(station = p[0], date = p[1], year = p[1].split("-")[0], month = p[1].split("-")[1],
                                day = p[1].split("-")[2], temp = float(p[3])))


tempDf = sqlContext.createDataFrame(table)
tempDf.registerTempTable("table")

# filter years to period 1960-2014
temps = tempDf.where((tempDf['year'] >= 1960) & (tempDf['year'] <= 2014))

tempsMin = temps.groupBy('station', 'year', 'month', 'day').agg(F.min('temp')).alias('dailymin')
tempsMax = temps.groupBy('station', 'year', 'month', 'day').agg(F.max('temp')).alias('dailymax')

# Join max and min into same tuple
join_temp = tempsMin.join(tempsMax, ['station', 'year', 'month', 'day'], 'inner')


max_days = join_temp.withColumn('dailysum', join_temp['min(temp)'] + join_temp['max(temp)']).groupBy('station', 'year', 'month').agg(F.sum('dailysum').alias('dailysum'))


max_date = join_temp.groupBy('station', 'year', 'month').agg((F.max('day') * 2).alias('maxdays'))


avg_temp = max_date.join(max_days, ['station', 'year', 'month'], 'inner').groupBy('station', 'year', 'month')

# Calculate the sum of dailysum and maxdays for each group
avg_temp_sum = avg_temp.agg(F.sum('dailysum').alias('total_dailysum'), F.sum('maxdays').alias('total_maxdays'))

# Calculate the average temperature
avg_temp = avg_temp_sum.withColumn("AvgTemp", avg_temp_sum['total_dailysum'] / avg_temp_sum['total_maxdays'])

# Select the necessary columns for the final output
avg_temp = avg_temp.select('station', 'year', 'month', 'AvgTemp').orderBy('AvgTemp', ascending = 0)

# Save the result
avg_temp.rdd.saveAsTextFile("BDA/output")



# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avg_temp.rdd.saveAsTextFile("BDA/output")
