#!/usr/bin/env python3

"""
Find the average monthly temperature for each available station in Sweden. 
Your result should include average temperature for each station for each month in the period of 1960-2014. 
Bear in mind that not every station has the readings for each month in this timeframe.
In this exercise you will use the temperature-readings.csv file.
The output should contain the following information:

Year, month, station number, average monthly temperature

ID; Year; Time; Temp; Quality

102170;2014-12-31;18:00:00;-8.7;G
"""

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

"""
102170;
2014-12-31;
18:00:00;
-8.7;
G"""

month_temp = lines.map(lambda x: ((x[0], x[1][0:4], x[1][5:7], x[1][8:10]), float(x[3])))

"""
(102170, 2014, 12, 31: -8.7)
"""

# filter years to period 1960-2014

month_temp = month_temp.filter(lambda x: int(x[0][1]) >= 1960 and int(x[0][1]) <= 2014)


"""

(102170, 2014, 12, 31: -8.7)
"""

# get max and min temperature for each day and divide by (days in month)*2 for each station

# Get max and min
max_temperatures = month_temp.map(lambda x: (x[0],x[1])).reduceByKey(max)
min_temperatures = month_temp.map(lambda x: (x[0],x[1])).reduceByKey(min)

# Join max and min into same tuple
join_temp = min_temperatures.join(max_temperatures)
join_temp = join_temp.map(lambda x: (x[0], x[1][0]+x[1][1]))

"""
((102170, 2014, 12, 31), -8.7+10.5)
"""

max_date = join_temp.map(lambda x: ( (x[0][0], x[0][1], x[0][2]), x[0][3])).reduceByKey(max)

"""
((102170, 2014, 12), max(D))
"""

max_date = max_date.map(lambda x: (x[0], int(x[1])+int(x[1]) ))

"""
((102170, 2014, 12), 2*max(D))
"""

join_temp = join_temp.map(lambda x : ( (x[0][0], x[0][1], x[0][2]), x[1])).reduceByKey(lambda x, y: x + y)

"""
((102170, 2014, 12), Summed(-8.7+10.5)*2)
"""


avg_temp = max_date.join(join_temp)

"""
((102170, 2014, 12), max(D), summed(-8.7+10.5)*2)
"""

avg_temp = avg_temp.map(lambda x: ((x[0]), x[1][1]/x[1][0]))




# Sorting
avg_temp = avg_temp.sortBy(ascending = False, keyfunc=lambda k: k[0])



# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avg_temp.saveAsTextFile("BDA/output")
