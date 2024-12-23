#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 2")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))
lines_pre = precipitation_file.map(lambda line: line.split(";"))

# (station) = (temperature)
temperature = lines.map(lambda x: ((x[0]),(float(x[3]))))
precipitation = lines_pre.map(lambda x:((x[0]+x[1]),(float(x[3]))))

# filter out max temp for each station
temperature = temperature.reduceByKey(lambda a,b: a if a >= b else b)

# Sum of rain for a station / day
precipitation = precipitation.reduceByKey(lambda x,y: x+y)

# mapping precipitation to get only station as key
precipitation = precipitation.map(lambda x: (x[0][0:5],x[1]))

# filtering out max for each station
precipitation = precipitation.reduceByKey(lambda a,b: a if a >= b else b)


#filter out temps that arent between 25-30.
temperature = temperature.filter(lambda x: x[1]>=25 and x[1]<=30)

precipitation = precipitation.filter(lambda x: x[1]>=100 and x[1]<=200)


# join the datasets on station
joined = precipitation.join(temperature)


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
joined.saveAsTextFile("BDA/output/ex3")
